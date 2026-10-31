package strimzi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

func TestRollbackFloors(t *testing.T) {
	h := newHarness(t, "", nil)
	for op, tier := range map[string]string{
		"k8s_delete": protocol.TierT2, "k8s_unapply": protocol.TierT2, "preview_k8s_secret_apply": protocol.TierT0,
	} {
		if got, ok := h.reg.Floor(op); !ok || got != tier {
			t.Errorf("%s floor = %q (registered %v), want %q", op, got, ok, tier)
		}
	}
}

func TestK8sDelete(t *testing.T) {
	for _, tc := range []struct {
		kind, name string
		spec       map[string]any
	}{{"KafkaConnector", "orders-src", map[string]any{}}, {"KafkaTopic", "orders-canary", map[string]any{"topicName": "pg.public.piperoom_canary"}}} {
		h := newHarness(t, "", nil)
		path := h.shell(tc.kind, tc.name, tc.spec)
		res, err := run(t, h, "k8s_delete", map[string]any{"kind": tc.kind, "name": tc.name})
		if err != nil {
			t.Fatalf("%s: k8s_delete: %v", tc.kind, err)
		}
		if h.api.get(path) != nil {
			t.Fatalf("%s: the object is still there", tc.kind)
		}
		if res.Receipt.Target != "DELETE "+path {
			t.Fatalf("%s: receipt = %+v", tc.kind, res.Receipt)
		}
		if got := strings.Join(h.api.requests, ","); got != "GET "+path+"?,DELETE "+path+"?" {
			t.Fatalf("%s: requests = %s, want the ownership GET and one DELETE", tc.kind, got)
		}
		// Again: already absent is the state a rollback wants.
		if res, err := run(t, h, "k8s_delete", map[string]any{"kind": tc.kind, "name": tc.name}); err != nil || !strings.Contains(res.Summary, "already absent") {
			t.Fatalf("%s: a second delete = %q, %v", tc.kind, res.Summary, err)
		}
	}
}

// Anything but the chart's KafkaConnector and KafkaTopic names is refused
// before any request.
func TestK8sDeleteRefusals(t *testing.T) {
	h := newHarness(t, "", nil)
	for name, p := range map[string]map[string]any{
		"the Kafka CR":            {"kind": "Kafka", "name": testKafka},
		"the KafkaConnect":        {"kind": "KafkaConnect", "name": "orders-connect"},
		"a KafkaUser":             {"kind": "KafkaUser", "name": "orders-connect"},
		"a Secret":                {"kind": "Secret", "name": "orders-db-credentials"},
		"a Pod":                   {"kind": "Pod", "name": "orders-connect-connect-0"},
		"another connector":       {"kind": "KafkaConnector", "name": "payments-src"},
		"another topic":           {"kind": "KafkaTopic", "name": "orders-other"},
		"no name":                 {"kind": "KafkaConnector"},
		"an unknown field":        {"kind": "KafkaConnector", "name": "orders-src", "force": true},
		"a path in the name":      {"kind": "KafkaConnector", "name": "../orders-src"},
		"a name of the right set": {"kind": "KafkaTopic", "name": "orders-src"},
	} {
		if _, err := run(t, h, "k8s_delete", p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := run(t, h, "k8s_delete", map[string]any{"kind": "Kafka", "name": testKafka}); !errors.Is(err, ErrKind) {
		t.Errorf("Kafka: err = %v, want ErrKind", err)
	}
	if len(h.api.requests) != 0 {
		t.Fatalf("refused deletes reached the API: %v", h.api.requests)
	}
}

// On production both kinds are refused before any request: the topic is
// kept (§7, §8), and a connector delete is a person's change, as
// connect_delete_connector is refused there by the guard.
func TestK8sDeleteProduction(t *testing.T) {
	h := newHarness(t, protocol.ClassProduction, nil)
	h.shell("KafkaTopic", "orders-canary", map[string]any{})
	path := h.shell("KafkaConnector", "orders-src", map[string]any{})
	for _, p := range []map[string]any{
		{"kind": "KafkaTopic", "name": "orders-canary"},
		{"kind": "KafkaConnector", "name": "orders-src"},
		{"kind": "KafkaConnector", "name": "orders-src", "admission_policy_confirmed": true},
	} {
		if _, err := run(t, h, "k8s_delete", p); !errors.Is(err, ErrProduction) {
			t.Errorf("%v on production: %v, want ErrProduction", p, err)
		}
	}
	if len(h.api.requests) != 0 {
		t.Fatalf("a refused production delete reached the API: %v", h.api.requests)
	}
	if h.api.get(path) == nil {
		t.Fatal("the connector was deleted on production")
	}
}

// An object of a chart name that the pipeline release does not own (no
// instance label, or another release's annotation) is never deleted or
// patched back.
func TestRollbackRefusesUnownedObjects(t *testing.T) {
	for name, md := range map[string]map[string]any{
		"no labels":        {"labels": map[string]any{}},
		"another instance": {"labels": map[string]any{"app.kubernetes.io/instance": "someone-else"}},
		"another release": {"labels": map[string]any{"app.kubernetes.io/instance": "piperoom-pipeline-orders"},
			"annotations": map[string]any{"meta.helm.sh/release-name": "other"}},
	} {
		h := newHarness(t, "", nil)
		conn := objectPath(testNS, "KafkaConnector", "orders-src")
		h.api.put(conn, map[string]any{"kind": "KafkaConnector", "metadata": cloneMap(md), "spec": map[string]any{}})
		if _, err := run(t, h, "k8s_delete", map[string]any{"kind": "KafkaConnector", "name": "orders-src"}); !errors.Is(err, ErrNotOwned) {
			t.Errorf("%s: k8s_delete err = %v, want ErrNotOwned", name, err)
		}
		if h.api.get(conn) == nil {
			t.Errorf("%s: an unowned connector was deleted", name)
		}
		user := objectPath(testNS, "KafkaUser", "orders-connect")
		h.api.put(user, map[string]any{"kind": "KafkaUser", "metadata": cloneMap(md), "spec": map[string]any{"authentication": map[string]any{"type": "tls"}}})
		if _, err := run(t, h, "k8s_unapply", map[string]any{"kind": "KafkaUser", "name": "orders-connect"}); !errors.Is(err, ErrNotOwned) {
			t.Errorf("%s: k8s_unapply err = %v, want ErrNotOwned", name, err)
		}
		for _, r := range h.api.requests {
			if !strings.HasPrefix(r, "GET ") {
				t.Errorf("%s: a write reached the API: %s", name, r)
			}
		}
	}
}

// Only a canary KafkaTopic is deleted: the topic its spec names (or its
// metadata.name, when the spec names none) must be a canary name exactly,
// and the topic its status reports the same one. Anything that differs
// keeps the CR; a paused shell that never named a topic is kept as is.
func TestK8sDeleteKeepsANonCanaryTopic(t *testing.T) {
	for name, tc := range map[string]struct {
		spec, status map[string]any
		paused       bool
	}{
		"status names a pipeline topic":    {map[string]any{"topicName": "pg.public.piperoom_canary"}, map[string]any{"topicName": "pg.public.orders"}, false},
		"status names a canary suffix":     {map[string]any{"topicName": "pg.public.piperoom_canary"}, map[string]any{"topicName": "x.pg.public.piperoom_canary"}, false},
		"spec names a pipeline topic":      {map[string]any{"topicName": "pg.public.orders"}, nil, false},
		"an unpaused CR with no topicName": {map[string]any{}, nil, false},
		"status only, spec empty":          {map[string]any{}, map[string]any{"topicName": "pg.public.piperoom_canary"}, false},
	} {
		h := newHarness(t, "", nil)
		path := h.shell("KafkaTopic", "orders-canary", tc.spec)
		obj := h.api.get(path)
		if tc.status != nil {
			obj["status"] = tc.status
		}
		if !tc.paused {
			delete(obj["metadata"].(map[string]any)["annotations"].(map[string]any), PauseAnnotation)
		}
		h.api.put(path, obj)
		if _, err := run(t, h, "k8s_delete", map[string]any{"kind": "KafkaTopic", "name": "orders-canary"}); !errors.Is(err, ErrField) {
			t.Errorf("%s: err = %v, want ErrField", name, err)
		}
		if h.api.get(path) == nil {
			t.Errorf("%s: a non-canary topic was deleted", name)
		}
	}
	h := newHarness(t, "", nil)
	path := h.shell("KafkaTopic", "orders-canary", map[string]any{})
	res, err := run(t, h, "k8s_delete", map[string]any{"kind": "KafkaTopic", "name": "orders-canary"})
	if err != nil || !strings.Contains(res.Summary, "never applied") || h.api.get(path) == nil {
		t.Fatalf("an unapplied shell: %q, %v (deleted %v)", res.Summary, err, h.api.get(path) == nil)
	}
	for _, r := range h.api.requests {
		if strings.HasPrefix(r, "DELETE") {
			t.Fatalf("an unapplied shell was deleted: %s", r)
		}
	}
}

// The ownership check needs Helm's release annotation: the instance label
// alone, which anyone who can create the object can copy, is not enough.
func TestCheckOwnedNeedsTheHelmAnnotation(t *testing.T) {
	h := newHarness(t, "", nil)
	label := map[string]any{"app.kubernetes.io/instance": "piperoom-pipeline-" + testPipeline}
	for name, md := range map[string]map[string]any{
		"the label alone":      {"labels": label},
		"another namespace":    {"namespace": testNS, "labels": label, "annotations": map[string]any{helmReleaseName: "piperoom-pipeline-" + testPipeline, helmReleaseNamespace: "other"}},
		"a non-string release": {"labels": label, "annotations": map[string]any{helmReleaseName: 1}},
	} {
		if err := h.d.checkOwned(map[string]any{"metadata": md}, "KafkaConnector", "orders-src"); !errors.Is(err, ErrNotOwned) {
			t.Errorf("%s: err = %v, want ErrNotOwned", name, err)
		}
	}
	ok := map[string]any{"namespace": testNS, "labels": label, "annotations": map[string]any{helmReleaseName: "piperoom-pipeline-" + testPipeline, helmReleaseNamespace: testNS}}
	if err := h.d.checkOwned(map[string]any{"metadata": ok}, "KafkaConnector", "orders-src"); err != nil {
		t.Fatalf("the release's object was refused: %v", err)
	}
}

func cloneMap(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

// k8s_unapply of a CR: one JSON patch under the resourceVersion test that
// re-pauses it and puts the shell's spec back.
func TestK8sUnapplyCR(t *testing.T) {
	h := newHarness(t, "", nil)
	path := objectPath(testNS, "KafkaConnect", "orders-connect")
	h.api.put(path, map[string]any{
		"kind":     "KafkaConnect",
		"metadata": map[string]any{"name": "orders-connect", "annotations": map[string]any{"strimzi.io/use-connector-resources": "true"}},
		"spec":     map[string]any{"replicas": 3, "build": map[string]any{"output": map[string]any{"type": "docker"}}, "config": map[string]any{"group.id": "x"}},
	})
	res, err := run(t, h, "k8s_unapply", map[string]any{"kind": "KafkaConnect"})
	if err != nil {
		t.Fatalf("k8s_unapply: %v (%v)", err, h.api.requests)
	}
	obj := h.api.get(path)
	ann := obj["metadata"].(map[string]any)["annotations"].(map[string]any)
	if ann[PauseAnnotation] != "true" || ann["strimzi.io/use-connector-resources"] != "true" {
		t.Fatalf("annotations = %v", ann)
	}
	want := map[string]any{"replicas": float64(1), "bootstrapServers": testKafka + "-kafka-bootstrap:9092"}
	if !reflect.DeepEqual(obj["spec"], want) {
		t.Fatalf("spec = %v, want the shell's %v", obj["spec"], want)
	}
	if !strings.Contains(res.Summary, "re-paused") || res.Receipt.Target != "PATCH "+path {
		t.Fatalf("result = %q %+v", res.Summary, res.Receipt)
	}
	var seq []string
	for _, r := range h.api.requests {
		seq = append(seq, strings.SplitN(r, " ", 2)[0])
	}
	if got := strings.Join(seq, ","); got != "GET,PATCH" {
		t.Fatalf("request sequence = %s", got)
	}

	// A KafkaUser whose annotations map is gone gets the map added whole.
	user := objectPath(testNS, "KafkaUser", "orders-piperoom-ro")
	h.api.put(user, map[string]any{"kind": "KafkaUser", "metadata": map[string]any{"name": "orders-piperoom-ro"},
		"spec": map[string]any{"authentication": map[string]any{"type": "tls"}}})
	if _, err := run(t, h, "k8s_unapply", map[string]any{"kind": "KafkaUser", "name": "orders-piperoom-ro"}); err != nil {
		t.Fatalf("k8s_unapply KafkaUser: %v", err)
	}
	obj = h.api.get(user)
	if obj["metadata"].(map[string]any)["annotations"].(map[string]any)[PauseAnnotation] != "true" || len(obj["spec"].(map[string]any)) != 0 {
		t.Fatalf("KafkaUser after unapply = %v", obj)
	}
}

func TestK8sUnapplySecretAndConfigMap(t *testing.T) {
	h := newHarness(t, "", nil)
	secret := coreObjectPath(testNS, "secrets", "orders-db-credentials")
	h.api.put(secret, map[string]any{"kind": "Secret", "type": "Opaque", "metadata": map[string]any{"name": "orders-db-credentials"},
		"data": map[string]any{"password": "czNjcjN0"}})
	res, err := run(t, h, "k8s_unapply", map[string]any{"kind": "Secret"})
	if err != nil {
		t.Fatalf("k8s_unapply Secret: %v", err)
	}
	if data, _ := h.api.get(secret)["data"].(map[string]any); len(data) != 0 {
		t.Fatalf("the Secret still holds %v", data)
	}
	b, _ := json.Marshal(res)
	if strings.Contains(string(b), "czNjcjN0") {
		t.Fatal("a stored value reached the result")
	}
	cm := coreObjectPath(testNS, "configmaps", "orders-connect-logging")
	h.api.put(cm, map[string]any{"kind": "ConfigMap", "metadata": map[string]any{"name": "orders-connect-logging"},
		"data": map[string]any{"log4j.properties": "log4j.rootLogger=DEBUG"}})
	if _, err := run(t, h, "k8s_unapply", map[string]any{"kind": "ConfigMap", "name": "orders-connect-logging"}); err != nil {
		t.Fatalf("k8s_unapply ConfigMap: %v", err)
	}
	if data, _ := h.api.get(cm)["data"].(map[string]any); len(data) != 0 {
		t.Fatalf("the ConfigMap still holds %v", data)
	}
}

func TestK8sUnapplyRefusals(t *testing.T) {
	h := newHarness(t, "", nil)
	for name, p := range map[string]map[string]any{
		"the Kafka CR":             {"kind": "Kafka"},
		"a KafkaConnector":         {"kind": "KafkaConnector", "name": "orders-src"},
		"a KafkaTopic":             {"kind": "KafkaTopic"},
		"a KafkaUser without name": {"kind": "KafkaUser"},
		"another KafkaUser":        {"kind": "KafkaUser", "name": "admin"},
		"another KafkaConnect":     {"kind": "KafkaConnect", "name": "other-connect"},
		"another Secret":           {"kind": "Secret", "name": "orders-registry-push"},
		"the metrics ConfigMap":    {"kind": "ConfigMap", "name": "orders-connect-metrics"},
		"an unknown field":         {"kind": "Secret", "data": map[string]any{}},
	} {
		if _, err := run(t, h, "k8s_unapply", p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if len(h.api.requests) != 0 {
		t.Fatalf("refused unapplies reached the API: %v", h.api.requests)
	}

	prod := newHarness(t, protocol.ClassProduction, nil)
	for _, p := range []map[string]any{
		{"kind": "KafkaConnect"},
		{"kind": "Secret"},
		{"kind": "Secret", "admission_policy_confirmed": true},
	} {
		if _, err := run(t, prod, "k8s_unapply", p); !errors.Is(err, ErrProduction) {
			t.Fatalf("%v on production: %v, want ErrProduction", p, err)
		}
	}
	if len(prod.api.requests) != 0 {
		t.Fatalf("a refused production unapply reached the API: %v", prod.api.requests)
	}
	user := prod.shell("KafkaUser", "orders-connect", map[string]any{"authentication": map[string]any{"type": "tls"}})
	if _, err := run(t, prod, "k8s_unapply", map[string]any{"kind": "KafkaUser", "name": "orders-connect"}); err == nil || !strings.Contains(err.Error(), "card must confirm") {
		t.Fatalf("KafkaUser on production without the card: %v", err)
	}
	if _, err := run(t, prod, "k8s_unapply", map[string]any{"kind": "KafkaUser", "name": "orders-connect", "admission_policy_confirmed": true}); err != nil {
		t.Fatalf("KafkaUser on production with the card: %v", err)
	}
	if len(prod.api.get(user)["spec"].(map[string]any)) != 0 {
		t.Fatal("the KafkaUser keeps its spec")
	}
}

// A 409 on the rollback patch (the object changed since the GET) fails the
// step; nothing is written.
func TestK8sUnapplyConflict(t *testing.T) {
	h := newHarness(t, "", nil)
	path := h.shell("KafkaUser", "orders-connect", map[string]any{"authentication": map[string]any{"type": "tls"}})
	h.api.status["PATCH "+path] = 409
	if _, err := run(t, h, "k8s_unapply", map[string]any{"kind": "KafkaUser", "name": "orders-connect"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	if len(h.api.get(path)["spec"].(map[string]any)) == 0 {
		t.Fatal("a conflicting patch was applied")
	}
}

// The rollback a k8s_apply preview records parses as "<op> {params}" and is
// accepted by that op's Validate; the two production refusals are recorded
// as not reversible.
func TestApplyPreviewRollbackRunsAsRecorded(t *testing.T) {
	for _, class := range []string{"", protocol.ClassProduction} {
		h := newHarness(t, class, nil)
		for kind, name := range map[string]string{"KafkaTopic": "orders-canary", "KafkaConnect": "orders-connect", "KafkaUser": "orders-connect", "ConfigMap": "orders-connect-logging", "KafkaConnector": "orders-src"} {
			r := rendered{kind: kind, name: name}
			reversible, record := h.d.applyRollback(r)
			refusedOnProd := class == protocol.ClassProduction && (kind == "KafkaTopic" || kind == "KafkaConnect" || kind == "KafkaConnector")
			if reversible == refusedOnProd {
				t.Errorf("%s %s on %q: reversible = %v", kind, name, class, reversible)
				continue
			}
			if !reversible {
				continue
			}
			op, params, ok := strings.Cut(record, " ")
			if !ok {
				t.Fatalf("%s: rollback %q is not \"<op> {params}\"", kind, record)
			}
			o, found := h.reg.Get(op)
			if !found {
				t.Fatalf("%s: rollback op %q is not registered", kind, op)
			}
			if err := o.Validate(json.RawMessage(params), h.env); err != nil {
				t.Errorf("%s: %s refuses its own recorded params %s: %v", kind, op, params, err)
			}
		}
	}
}

func TestPreviewK8sSecretApply(t *testing.T) {
	h := newHarness(t, "", map[string]string{"source_password": "s3cr3t-value"})
	path := coreObjectPath(testNS, "secrets", "orders-db-credentials")
	h.api.put(path, map[string]any{"kind": "Secret", "type": "Opaque", "metadata": map[string]any{"name": "orders-db-credentials"},
		"data": map[string]any{"stale": "c3RhbGUtdmFsdWU="}})
	res, err := run(t, h, "preview_k8s_secret_apply", map[string]any{"keys": map[string]string{"password": "${secret:source_password}"}})
	if err != nil {
		t.Fatalf("preview_k8s_secret_apply: %v", err)
	}
	pr, ok := res.Data.(events.PreviewResult)
	if !ok {
		t.Fatalf("Data is %T, want events.PreviewResult", res.Data)
	}
	if pr.Current != "keys stale" || !strings.HasPrefix(pr.Expected, "keys password") || !pr.Reversible {
		t.Fatalf("preview = %+v", pr)
	}
	b, _ := json.Marshal(res)
	if strings.Contains(string(b), "c3RhbGU") || strings.Contains(string(b), "s3cr3t") {
		t.Fatal("a value reached the preview")
	}
	op, params, _ := strings.Cut(pr.Rollback, " ")
	o, _ := h.reg.Get(op)
	if op != "k8s_unapply" || o.Validate(json.RawMessage(params), h.env) != nil {
		t.Fatalf("rollback = %q", pr.Rollback)
	}
	for _, r := range h.api.requests {
		if !strings.HasPrefix(r, "GET ") {
			t.Fatalf("a preview sent %s", r)
		}
	}
}

// k8s_unapply puts back exactly the spec the chart gives a shell: the golden
// render of the pipeline release pins it.
func TestShellSpecIsTheCharts(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(chartGolden, "pipeline.golden.yaml"))
	if err != nil {
		t.Fatalf("reading the chart's golden render: %v", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	seen := map[string]bool{}
	for {
		var o struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec map[string]any `yaml:"spec"`
		}
		err := dec.Decode(&o)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if o.Kind != "KafkaConnect" && o.Kind != "KafkaUser" {
			continue
		}
		seen[o.Kind] = true
		if o.Spec == nil {
			o.Spec = map[string]any{}
		}
		got, _ := json.Marshal(shellSpec(o.Kind, testKafka))
		want, _ := json.Marshal(o.Spec)
		if string(got) != string(want) {
			t.Errorf("%s %s: shellSpec = %s, the chart's = %s", o.Kind, o.Metadata.Name, got, want)
		}
	}
	if !seen["KafkaConnect"] || !seen["KafkaUser"] {
		t.Fatalf("the golden render has no KafkaConnect or KafkaUser shell (%v)", seen)
	}
}

// On production the Secret's rollback is refused (k8s_unapply), so its
// preview records it as not reversible, and k8s_secret_apply passes the D11
// gate as k8s_apply does.
func TestSecretApplyOnProduction(t *testing.T) {
	h := newHarness(t, protocol.ClassProduction, map[string]string{"source_password": "s3cr3t-value"})
	path := coreObjectPath(testNS, "secrets", "orders-db-credentials")
	h.api.put(path, map[string]any{"kind": "Secret", "type": "Opaque", "metadata": map[string]any{"name": "orders-db-credentials"}})
	res, err := run(t, h, "preview_k8s_secret_apply", map[string]any{"keys": map[string]string{"password": "${secret:source_password}"}})
	if err != nil {
		t.Fatal(err)
	}
	if pr := res.Data.(events.PreviewResult); pr.Reversible || strings.HasPrefix(pr.Rollback, "k8s_unapply") {
		t.Fatalf("production preview = %+v, want not reversible", pr)
	}
	keys := map[string]any{"keys": map[string]string{"password": "${secret:source_password}"}}
	if _, err := run(t, h, "k8s_secret_apply", keys); err == nil || !strings.Contains(err.Error(), "card must confirm") {
		t.Fatalf("k8s_secret_apply on production without the card: %v", err)
	}
	if data, _ := h.api.get(path)["data"].(map[string]any); len(data) != 0 {
		t.Fatal("the gate let the write through")
	}
	keys["admission_policy_confirmed"] = true
	if _, err := run(t, h, "k8s_secret_apply", keys); err != nil {
		t.Fatalf("k8s_secret_apply on production with the card: %v", err)
	}
}

// The driver's class follows the runner's: a target registered shared and
// raised to production later is guarded as production at once. The runner's
// guard refuses k8s_delete and the Strimzi ops' ClassGuarder raises the
// driver's class, so the production refusals Execute holds (an unapply of
// the KafkaConnect or the Secret, the card for the rest) follow the raise. A
// lower class never lowers it.
func TestClassGuardFollowsRaises(t *testing.T) {
	h := newHarness(t, "", nil)
	if h.reg.GuardForbidden("k8s_delete", protocol.ClassShared) {
		t.Fatal("k8s_delete refused on shared")
	}
	if _, err := run(t, h, "k8s_unapply", map[string]any{"kind": "Secret"}); errors.Is(err, ErrProduction) {
		t.Fatalf("a shared Secret unapply was refused as production: %v", err)
	}
	h.api.requests = nil
	if !h.reg.GuardForbidden("k8s_delete", protocol.ClassProduction) {
		t.Fatal("k8s_delete allowed on production")
	}
	if h.reg.GuardForbidden("k8s_unapply", protocol.ClassProduction) {
		t.Fatal("k8s_unapply refused outright: its production row depends on the kind")
	}
	// A later request on a lower class does not lower the driver's.
	h.reg.GuardForbidden("k8s_get", protocol.ClassShared)
	for _, p := range []map[string]any{{"kind": "KafkaConnect"}, {"kind": "Secret"}} {
		if _, err := run(t, h, "k8s_unapply", p); !errors.Is(err, ErrProduction) {
			t.Errorf("%v after the raise: %v, want ErrProduction", p, err)
		}
	}
	if _, err := run(t, h, "k8s_delete", map[string]any{"kind": "KafkaConnector", "name": "orders-src"}); !errors.Is(err, ErrProduction) {
		t.Errorf("k8s_delete after the raise: %v, want ErrProduction", err)
	}
	for _, r := range h.api.requests {
		if !strings.HasPrefix(r, "GET ") {
			t.Errorf("a write reached the API after the raise: %s", r)
		}
	}
}
