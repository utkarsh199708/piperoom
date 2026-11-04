package wiring

import (
	"context"
	"errors"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/clients/kubernetes"
	"piperoom/internal/config"
	"piperoom/internal/tools/k8stools"
)

// testStrimzi is a Kubernetes reader that is never called (building one
// sends no request).
func testStrimzi(t *testing.T) strimziSet {
	t.Helper()
	c, err := kubernetes.New(kubernetes.Config{BaseURL: "https://kube.invalid:6443", Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	return strimziSet{kube: c, settings: k8stools.Settings{Namespace: "kafka"}}
}

// strimziYAML is twoTenantYAML with pa's block extended by k8s.
func strimziYAML(k8s string) string {
	y := twoTenantYAML("")
	// pa's block is the first; its last key is A's Kafka password.
	return strings.Replace(y, "kafka_sasl_password: {env: A_KAFKA_PASSWORD}", "kafka_sasl_password: {env: A_KAFKA_PASSWORD}"+k8s, 1)
}

const paK8s = `
      kubernetes_api_url: {value: "https://kube.example.com:6443"}
      kubernetes_token: {env: A_KUBE_TOKEN}
      strimzi_namespace: {value: kafka}
      strimzi_operator_namespace: {value: strimzi}`

func TestStrimziToolsOnlyWithTheKeys(t *testing.T) {
	own := loadOwners(t, strimziYAML(paK8s))
	e := twoTenantEnv()
	e["A_KUBE_TOKEN"] = "secret-kube-token"
	f := newFakeOpener()
	st, err := openStacks(context.Background(), config.Config{}, nil, own, StackOptions{}, f.deps(env(e)))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	pa, pb := regNames(st.For("pa").Tools), regNames(st.For("pb").Tools)
	for _, n := range k8stools.Names {
		if !slices.Contains(pa, n) {
			t.Errorf("pa misses %s", n)
		}
		if slices.Contains(pb, n) {
			t.Errorf("pb (no Kubernetes keys) has %s", n)
		}
		if kinds, known := agent.ToolPipelineKinds(n); !known || !slices.Equal(kinds, []string{"cdc"}) {
			t.Errorf("%s: kinds %v known %v", n, kinds, known)
		}
	}
	// The Kubernetes keys never reach the set's config.Config (they have no
	// field): the A set's settings are the plain connections.
	for _, c := range f.cfgs {
		if strings.Contains(c.SourceDBURL, "kube") {
			t.Fatalf("config %+v", c)
		}
	}
	got := st.For("pa").Clients.strimzi.settings
	if got.Namespace != "kafka" || got.OperatorNamespace != "strimzi" {
		t.Fatalf("settings %+v", got)
	}
}

func TestPipelineKubeErrorsHideValues(t *testing.T) {
	for name, tc := range map[string]struct {
		k8s  string
		env  map[string]string
		want string
	}{
		"url from env is a DSN": {`
      kubernetes_api_url: {env: KURL}
      kubernetes_token: {env: KTOK}
      strimzi_namespace: {value: kafka}`, map[string]string{"KURL": "postgres://u:secret-pw@h/db", "KTOK": "t"}, "an https://host[:port] URL"},
		"http url": {`
      kubernetes_api_url: {value: "http://kube.example.com"}
      kubernetes_token: {env: KTOK}
      strimzi_namespace: {value: kafka}`, map[string]string{"KTOK": "t"}, "an https://host[:port] URL"},
		"token unset": {`
      kubernetes_api_url: {value: "https://kube.example.com"}
      kubernetes_token: {env: KTOK}
      strimzi_namespace: {value: kafka}`, nil, "KTOK is not set"},
		"token file missing": {`
      kubernetes_api_url: {value: "https://kube.example.com"}
      kubernetes_token: {file: /var/run/secrets/kubernetes.io/serviceaccount/token}
      strimzi_namespace: {value: kafka}`, nil, "open file"},
		"namespace not a label": {`
      kubernetes_api_url: {value: "https://kube.example.com"}
      kubernetes_token: {env: KTOK}
      strimzi_namespace: {value: "Kafka_NS"}`, map[string]string{"KTOK": "t"}, "a namespace"},
		"operator deployment bad": {`
      kubernetes_api_url: {value: "https://kube.example.com"}
      kubernetes_token: {env: KTOK}
      strimzi_namespace: {value: kafka}
      strimzi_operator_deployment: {value: "op/x"}`, map[string]string{"KTOK": "t"}, "a Deployment name"},
	} {
		t.Run(name, func(t *testing.T) {
			own := loadOwners(t, strimziYAML(tc.k8s))
			p, _ := own.Pipeline("pa")
			_, _, err := pipelineKube(p, env(tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "secret-pw") || strings.Contains(err.Error(), "Kafka_NS") {
				t.Fatalf("error shows the value: %v", err)
			}
			if !errors.Is(err, errConnRef) && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("err does not wrap errConnRef: %v", err)
			}
		})
	}
}

func TestPipelineKubeTokenFile(t *testing.T) {
	own := loadOwners(t, strimziYAML(`
      kubernetes_api_url: {value: "https://kube.example.com"}
      kubernetes_token: {file: /var/run/secrets/kubernetes.io/serviceaccount/token}
      kubernetes_ca_file: {file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt}
      strimzi_namespace: {value: kafka}`))
	p, _ := own.Pipeline("pa")
	src := env(nil)
	src.canRead = func(string) error { return nil }
	src.readFile = func(string) ([]byte, error) {
		t.Fatal("the token file must not be read at start")
		return nil, nil
	}
	kc, ok, err := pipelineKube(p, src)
	if err != nil || !ok {
		t.Fatalf("ok %v err %v", ok, err)
	}
	if kc.tokenFile != "/var/run/secrets/kubernetes.io/serviceaccount/token" || kc.token != "" || kc.caFile == "" {
		t.Fatalf("config %+v", kc)
	}
	// No Kubernetes keys: not configured.
	pb, _ := own.Pipeline("pb")
	if _, ok, err := pipelineKube(pb, src); ok || err != nil {
		t.Fatalf("pb: ok %v err %v", ok, err)
	}
}
