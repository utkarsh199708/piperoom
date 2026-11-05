package strimzi

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// previewOf runs a preview op and returns its PreviewResult.
func previewOf(t *testing.T, h *harness, op string, params any) (events.PreviewResult, error) {
	t.Helper()
	res, err := run(t, h, op, params)
	if err != nil {
		return events.PreviewResult{}, err
	}
	pr, ok := res.Data.(events.PreviewResult)
	if !ok {
		t.Fatalf("%s: Data is %T, want events.PreviewResult", op, res.Data)
	}
	return pr, nil
}

// A rollback's own preview records no rollback: Reversible is false and
// Rollback is prose, never "<op> {params}", so the room's walk takes no step
// from it.
func checkNotReversible(t *testing.T, name string, pr events.PreviewResult) {
	t.Helper()
	if pr.Reversible || strings.TrimSpace(pr.Rollback) == "" {
		t.Errorf("%s: preview reversible %v, rollback %q; want not reversible with a reason", name, pr.Reversible, pr.Rollback)
	}
	if op, rest, _ := strings.Cut(pr.Rollback, " "); strings.HasPrefix(strings.TrimSpace(rest), "{") {
		t.Errorf("%s: rollback %q reads as the op %s with params", name, pr.Rollback, op)
	}
}

func TestPreviewRollbackFloors(t *testing.T) {
	h := newHarness(t, "", nil)
	for _, op := range []string{"preview_k8s_delete", "preview_k8s_unapply"} {
		if got, ok := h.reg.Floor(op); !ok || got != protocol.TierT0 {
			t.Errorf("%s floor = %q (registered %v), want T0", op, got, ok)
		}
	}
}

// preview_k8s_delete reads with the delete's checks and sends GETs only.
func TestPreviewK8sDelete(t *testing.T) {
	for _, tc := range []struct {
		kind, name, want string
		spec             map[string]any
	}{
		{"KafkaConnector", "orders-src", "deleted by its uid and resourceVersion with foreground propagation", map[string]any{}},
		{"KafkaTopic", "orders-canary", "deletes the Kafka topic pg.public.piperoom_canary", map[string]any{"topicName": "pg.public.piperoom_canary"}},
	} {
		h := newHarness(t, "", nil)
		path := h.shell(tc.kind, tc.name, tc.spec)
		pr, err := previewOf(t, h, "preview_k8s_delete", delParams(tc.kind, tc.name))
		if err != nil {
			t.Fatalf("%s: preview_k8s_delete: %v", tc.kind, err)
		}
		if !strings.Contains(pr.Expected, tc.want) || !strings.Contains(pr.Current, "paused") || pr.Target != tc.kind+" "+testNS+"/"+tc.name {
			t.Errorf("%s: preview = %+v", tc.kind, pr)
		}
		checkNotReversible(t, tc.kind, pr)
		if h.api.get(path) == nil {
			t.Fatalf("%s: the preview deleted the object", tc.kind)
		}
		for _, r := range h.api.requests {
			if !strings.HasPrefix(r, "GET ") {
				t.Fatalf("%s: a preview sent %s", tc.kind, r)
			}
		}
	}

	// Absent: nothing to delete, as the delete reports.
	h := newHarness(t, "", nil)
	h.api.allow(objectPath(testNS, "KafkaConnector", "orders-src"))
	pr, err := previewOf(t, h, "preview_k8s_delete", delParams("KafkaConnector", "orders-src"))
	if err != nil || !strings.Contains(pr.Expected, "already absent") {
		t.Fatalf("absent: %+v, %v", pr, err)
	}

	// The paused shell of the canary is kept, as the delete keeps it.
	h = newHarness(t, "", nil)
	h.shell("KafkaTopic", "orders-canary", map[string]any{})
	if pr, err := previewOf(t, h, "preview_k8s_delete", delParams("KafkaTopic", "orders-canary")); err != nil || !strings.HasPrefix(pr.Expected, "kept") {
		t.Fatalf("never applied: %+v, %v", pr, err)
	}
}

// The preview refuses what the delete refuses, so a card never shows a
// passed preview for a step the runner then refuses.
func TestPreviewK8sDeleteRefusesWhatTheDeleteRefuses(t *testing.T) {
	h := newHarness(t, "", nil)
	h.shell("KafkaTopic", "orders-canary", map[string]any{"topicName": "payments.public.piperoom_canary"})
	if _, err := previewOf(t, h, "preview_k8s_delete", delParams("KafkaTopic", "orders-canary")); err == nil {
		t.Error("another prefix's canary: the preview passed")
	}
	h = newHarness(t, "", nil)
	path := objectPath(testNS, "KafkaConnector", "orders-src")
	h.api.put(path, map[string]any{"kind": "KafkaConnector", "metadata": map[string]any{"name": "orders-src",
		"annotations": map[string]any{"meta.helm.sh/release-name": "someone-else"}}, "spec": map[string]any{}})
	if _, err := previewOf(t, h, "preview_k8s_delete", delParams("KafkaConnector", "orders-src")); err == nil {
		t.Error("another release's connector: the preview passed")
	}
	if _, err := previewOf(t, h, "preview_k8s_delete", map[string]any{"kind": "KafkaUser", "name": "orders-connect"}); !errors.Is(err, ErrKind) {
		t.Errorf("a kind k8s_delete refuses: err = %v", err)
	}
	h = newHarness(t, "production", nil)
	if _, err := previewOf(t, h, "preview_k8s_delete", delParams("KafkaConnector", "orders-src")); !errors.Is(err, ErrProduction) {
		t.Errorf("production: err = %v, want ErrProduction", err)
	}
}

// preview_k8s_unapply sends the rollback patch with dryRun=All and leaves
// the object as it was; for a Secret it names keys, never a value.
func TestPreviewK8sUnapply(t *testing.T) {
	h := newHarness(t, "", nil)
	path := objectPath(testNS, "KafkaConnect", "orders-connect")
	before := map[string]any{
		"kind":     "KafkaConnect",
		"metadata": map[string]any{"name": "orders-connect", "annotations": map[string]any{"strimzi.io/use-connector-resources": "true"}},
		"spec":     map[string]any{"replicas": float64(3)},
	}
	h.api.put(path, before)
	pr, err := previewOf(t, h, "preview_k8s_unapply", map[string]any{"kind": "KafkaConnect"})
	if err != nil {
		t.Fatalf("preview_k8s_unapply: %v (%v)", err, h.api.requests)
	}
	if !strings.Contains(pr.Expected, "re-paused") || pr.DryRun == nil || !pr.DryRun.OK || pr.DryRun.Source != "kubernetes dryRun=All" {
		t.Fatalf("preview = %+v (dry run %+v)", pr, pr.DryRun)
	}
	checkNotReversible(t, "KafkaConnect", pr)
	if got := h.api.get(path); !reflect.DeepEqual(got["spec"], before["spec"]) {
		t.Fatalf("the preview changed the spec: %v", got["spec"])
	}
	if got := strings.Join(h.api.requests, ","); got != "GET "+path+"?,PATCH "+path+"?dryRun=All" {
		t.Fatalf("requests = %s, want a GET and a dry-run PATCH", got)
	}

	secret := coreObjectPath(testNS, "secrets", "orders-db-credentials")
	h.api.put(secret, map[string]any{"kind": "Secret", "type": "Opaque", "metadata": map[string]any{"name": "orders-db-credentials"},
		"data": map[string]any{"password": "czNjcjN0", "user": "dXNlcg=="}})
	h.api.requests = nil
	res, err := run(t, h, "preview_k8s_unapply", map[string]any{"kind": "Secret"})
	if err != nil {
		t.Fatalf("preview_k8s_unapply Secret: %v", err)
	}
	pr = res.Data.(events.PreviewResult)
	if pr.Current != "Secret holds keys password, user" || !pr.DryRun.OK {
		t.Fatalf("Secret preview = %+v", pr)
	}
	b, _ := json.Marshal(res)
	if strings.Contains(string(b), "czNjcjN0") || strings.Contains(string(b), "dXNlcg") {
		t.Fatal("a stored value reached the preview")
	}
	if data, _ := h.api.get(secret)["data"].(map[string]any); len(data) != 2 {
		t.Fatalf("the preview emptied the Secret: %v", data)
	}
	for _, r := range h.api.requests {
		if strings.HasPrefix(r, "PATCH ") && !strings.HasSuffix(r, "?dryRun=All") {
			t.Fatalf("a preview sent %s", r)
		}
	}
}

// A dry run the API server refuses is a preview that says so, not an error:
// the card shows why the rollback would fail.
func TestPreviewK8sUnapplyDryRunRefused(t *testing.T) {
	h := newHarness(t, "", nil)
	cm := coreObjectPath(testNS, "configmaps", "orders-connect-logging")
	h.api.put(cm, map[string]any{"kind": "ConfigMap", "metadata": map[string]any{"name": "orders-connect-logging"},
		"data": map[string]any{"log4j.properties": "x"}})
	h.api.status["PATCH "+cm] = 422
	pr, err := previewOf(t, h, "preview_k8s_unapply", map[string]any{"kind": "ConfigMap"})
	if err != nil {
		t.Fatalf("preview_k8s_unapply: %v", err)
	}
	if pr.DryRun == nil || pr.DryRun.OK || pr.DryRun.Text == "" || pr.Current != "ConfigMap holds keys log4j.properties" {
		t.Fatalf("preview = %+v (dry run %+v)", pr, pr.DryRun)
	}
}

// Every Strimzi preview runs D11's production gate as its write does (a read
// of the policy binding), so a preview never passes for a step the write
// refuses on production.
func TestPreviewsRunTheProductionGate(t *testing.T) {
	for _, tc := range []struct {
		op     string
		params map[string]any
	}{
		// delete is refused on production before the gate (design §7).
		{"preview_k8s_apply", topicApply()},
		{"preview_k8s_unapply", map[string]any{"kind": "KafkaUser", "name": "orders-connect"}},
		{"preview_k8s_secret_apply", map[string]any{"keys": map[string]string{"password": "${secret:source_password}"}}},
		{"preview_k8s_delete", map[string]any{"kind": "KafkaConnector", "name": "orders-src"}},
	} {
		h := newHarness(t, protocol.ClassProduction, map[string]string{"source_password": "s3cr3t-value"})
		h.shell("KafkaTopic", "orders-canary", map[string]any{})
		h.shell("KafkaConnector", "orders-src", map[string]any{})
		h.shell("KafkaUser", "orders-connect", map[string]any{})
		_, err := run(t, h, tc.op, tc.params)
		want := "card must confirm"
		if tc.op == "preview_k8s_delete" {
			want = "refused on a production target"
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s on production without the card: %v, want %q", tc.op, err, want)
		}
		for _, r := range h.api.requests {
			if !strings.HasPrefix(r, "GET ") {
				t.Errorf("%s sent %s", tc.op, r)
			}
		}
	}
}
