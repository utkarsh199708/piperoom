package owners

import (
	"slices"
	"strings"
	"testing"
)

// k8sConns is conns(prefix) plus the Kubernetes keys in extra.
func k8sConns(extra string) string {
	return "pipelines:\n  - id: p1\n    topic_prefix: pg\n    source_tables: [public.t]\n    connectors: [c1]" + conns("A") + extra + "\n"
}

// The Kubernetes and Strimzi keys are pipeline-only: no process-wide
// variable, no staging, no ProcessRefs entry, but accepted in connections:.
func TestPipelineOnlyKeys(t *testing.T) {
	want := []string{"kubernetes_api_url", "kubernetes_ca_file", "kubernetes_token", "strimzi_namespace", "strimzi_operator_deployment", "strimzi_operator_namespace"}
	if got := PipelineOnlyKeys(); !slices.Equal(got, want) {
		t.Fatalf("PipelineOnlyKeys = %v", got)
	}
	for _, k := range want {
		if slices.Contains(ConnectionKeys(), k) || slices.Contains(StagingKeys(), k) {
			t.Errorf("%s is listed as a process-wide or staging key", k)
		}
		if !slices.Contains(AllConnectionKeys(), k) || !IsPipelineOnlyKey(k) {
			t.Errorf("%s missing from AllConnectionKeys", k)
		}
		for _, r := range ProcessRefs() {
			if strings.Contains(r, strings.ToUpper(k)) {
				t.Errorf("ProcessRefs names %s", r)
			}
		}
	}
	if !IsCAKey("kubernetes_ca_file") || !IsPathKey("kubernetes_ca_file") {
		t.Error("kubernetes_ca_file is a CA path")
	}
	if IsLiteralKey("kubernetes_token") || IsPathKey("kubernetes_token") {
		t.Error("kubernetes_token is a secret, neither literal nor path")
	}
	if len(AllConnectionKeys()) != len(connKeys) {
		t.Error("AllConnectionKeys misses a key")
	}
}

func TestK8sKeysValidation(t *testing.T) {
	full := `
      kubernetes_api_url: {value: "https://kube.example.com:6443"}
      kubernetes_token: {file: /var/run/secrets/kubernetes.io/serviceaccount/token}
      kubernetes_ca_file: {file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt}
      strimzi_namespace: {value: kafka}`
	o, err := Load(writeTemp(t, k8sConns(full+"\n      strimzi_operator_deployment: {value: strimzi-cluster-operator}\n      strimzi_operator_namespace: {value: strimzi}")))
	if err != nil {
		t.Fatalf("full block: %v", err)
	}
	p, _ := o.Pipeline("p1")
	if p.Connections["strimzi_namespace"].Value != "kafka" {
		t.Fatalf("connections %v", p.Connections)
	}
	// The token file is a reference for the shared-reference rule; the CA is not.
	refs := p.ConnRefs()
	if !slices.Contains(refs, "file:/var/run/secrets/kubernetes.io/serviceaccount/token") || slices.Contains(refs, "file:/var/run/secrets/kubernetes.io/serviceaccount/ca.crt") {
		t.Fatalf("refs %v", refs)
	}
	for name, tc := range map[string]struct{ extra, want string }{
		"token as a literal": {`
      kubernetes_api_url: {value: "https://kube.example.com"}
      kubernetes_token: {value: eyJhbGciOi}
      strimzi_namespace: {value: kafka}`, "only for non-secret"},
		"namespace without url": {`
      strimzi_namespace: {value: kafka}`, "kubernetes_api_url is required"},
		"url without token": {`
      kubernetes_api_url: {value: "https://kube.example.com"}
      strimzi_namespace: {value: kafka}`, "kubernetes_token is required"},
		"url without namespace": {`
      kubernetes_api_url: {value: "https://kube.example.com"}
      kubernetes_token: {env: KUBE_TOKEN}`, "strimzi_namespace is required"},
		"operator alone": {`
      strimzi_operator_deployment: {value: op}`, "is required with any kubernetes_"},
		"relative token file": {`
      kubernetes_api_url: {value: "https://kube.example.com"}
      kubernetes_token: {file: token}
      strimzi_namespace: {value: kafka}`, "absolute path"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeTemp(t, k8sConns(tc.extra)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "eyJhbGciOi") {
				t.Fatalf("error echoes the token: %v", err)
			}
		})
	}
}

// A batch pipeline has no Strimzi Connect: the keys are cdc only.
func TestK8sKeysAreCDCOnly(t *testing.T) {
	y := "pipelines:\n  - id: b1\n    kind: batch\n    dags: [d]\n    connections:\n      warehouse_db_url: {env: WH}\n      kubernetes_api_url: {value: \"https://kube.example.com\"}\n"
	if _, err := Load(writeTemp(t, y)); err == nil || !strings.Contains(err.Error(), "is a cdc setting") {
		t.Fatalf("err = %v", err)
	}
}
