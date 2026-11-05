//go:build integration

package strimzi

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"piperoom/internal/setup/ops"
)

// TestKindRunnerWrites runs the driver against a real API server with the
// runner ServiceAccount's own token (row U12c's nightly kind job,
// .github/workflows/setup-strimzi-nightly.yml): the chart's two releases are
// installed in namespace data, the Strimzi operator watches it, and the test
// fills the canary topic and the two KafkaUser shells, unpauses them and
// waits for the operators to report Ready. It also asserts that the Role
// refuses an object outside the chart's set (403), which the unit tests can
// only fake.
//
// PIPEROOM_IT_K8S_API (https://...), PIPEROOM_IT_K8S_CA_FILE and
// PIPEROOM_IT_K8S_TOKEN_FILE name the cluster; without them the test skips.
func TestKindRunnerWrites(t *testing.T) {
	base, ca, token := os.Getenv("PIPEROOM_IT_K8S_API"), os.Getenv("PIPEROOM_IT_K8S_CA_FILE"), os.Getenv("PIPEROOM_IT_K8S_TOKEN_FILE")
	if base == "" || ca == "" || token == "" {
		t.Skip("PIPEROOM_IT_K8S_API, _CA_FILE and _TOKEN_FILE are not set (the kind nightly sets them)")
	}
	api, err := NewAPI(APIConfig{BaseURL: base, CAFile: ca, TokenFile: token})
	if err != nil {
		t.Fatal(err)
	}
	const ns, kafka, pipeline = "data", "prod-kafka", "orders"
	targets := targetsOf("kind", ns, kafka)
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: targets})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	if _, err := Register(reg, Options{Pipeline: pipeline, Connectors: []string{"orders-src", "orders-sink"}, API: api}); err != nil {
		t.Fatal(err)
	}
	env := ops.Env{Targets: targets}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	exec := func(op string, params any) ops.Result {
		t.Helper()
		o, ok := reg.Get(op)
		if !ok {
			t.Fatalf("%s is not registered", op)
		}
		raw, _ := json.Marshal(params)
		if err := o.Validate(raw, env); err != nil {
			t.Fatalf("%s: Validate: %v", op, err)
		}
		res, err := o.Execute(ctx, raw, env)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res
	}
	applies := []map[string]any{
		{"kind": "KafkaTopic", "template": "canary-topic", "params": map[string]any{"topic_prefix": "pg", "schema": "public", "table": "piperoom_canary"}},
		{"kind": "KafkaUser", "template": "connect-user", "params": map[string]any{"topic_prefix": "pg", "auth": "scram-sha-512"}},
		{"kind": "KafkaUser", "template": "read-user", "params": map[string]any{"topic_prefix": "pg", "auth": "scram-sha-512"}},
	}
	for _, a := range applies {
		res := exec("k8s_apply", a)
		if !strings.Contains(res.Summary, "unpaused") {
			t.Fatalf("%v: %s", a["template"], res.Summary)
		}
	}
	for _, obj := range []struct{ kind, name string }{
		{"KafkaTopic", pipeline + "-canary"}, {"KafkaUser", pipeline + "-connect"}, {"KafkaUser", pipeline + "-piperoom-ro"},
	} {
		for {
			res := exec("k8s_get", map[string]any{"kind": obj.kind, "name": obj.name})
			if strings.Contains(res.Summary, "Ready=True") {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("%s %s never became Ready: %s", obj.kind, obj.name, res.Summary)
			case <-time.After(5 * time.Second):
			}
		}
	}
	// The Role names objects, so a patch of anything else is the API server's
	// 403, not the validator's refusal.
	_, err = api.Patch(ctx, objectPath(ns, "KafkaTopic", "not-the-charts"), []PatchOp{{Op: "add", Path: "/metadata/labels", Value: map[string]any{}}}, true)
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("patching an object outside the chart's set: err = %v, want ErrForbidden", err)
	}
}
