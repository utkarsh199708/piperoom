package demo

// The scripted agent and the demo fakes must keep working with the REAL
// actions and the owners pipeline from dev/owners.yaml: if either drifts, the
// --demo flow breaks while production stays green.

import (
	"context"
	"encoding/json"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/owners"
)

func TestRealActionsOverFakesPreventDemoDrift(t *testing.T) {
	reg := actions.NewRegistry()
	if err := actions.Register(reg, FakeActionDeps()); err != nil {
		t.Fatalf("register real actions: %v", err)
	}
	// The pipeline as dev/owners.yaml defines it (id "orders").
	pipeline := owners.Pipeline{
		ID:           "orders",
		Title:        "Orders CDC",
		TopicPrefix:  "pg",
		SourceTables: []string{"public.orders"},
		Connectors:   []string{"pg-orders-connector"},
		SinkTables:   []string{"analytics.orders"},
		SinkTSColumn: "updated_at",
	}
	ctx := context.Background()
	steps := []struct {
		action string
		params string
		want   string
	}{
		{
			action: ActionSchemaSetCompatibility,
			params: `{"subject":"pg.public.orders-value","level":"NONE"}`,
			want:   "pg.public.orders-value compatibility now NONE",
		},
		{
			action: ActionConnectRestartTask,
			params: `{"connector":"pg-orders-connector","task":0}`,
			want:   "restarted pg-orders-connector task 0 (state now RUNNING)",
		},
	}
	for _, st := range steps {
		a, ok := reg.Get(st.action)
		if !ok {
			t.Fatalf("action %q not registered", st.action)
		}
		params := json.RawMessage(st.params)
		if err := a.Validate(params); err != nil {
			t.Fatalf("%s: validate: %v", st.action, err)
		}
		sc, ok := a.(actions.Scoped)
		if !ok {
			t.Fatalf("%s: not scoped", st.action)
		}
		if err := sc.CheckScope(pipeline, params); err != nil {
			t.Fatalf("%s: scope: %v", st.action, err)
		}
		got, err := a.Execute(ctx, params)
		if err != nil {
			t.Fatalf("%s: execute: %v", st.action, err)
		}
		if got != st.want {
			t.Fatalf("%s: got %q, want %q", st.action, got, st.want)
		}
	}

	// The real scope check still refuses a resource outside the pipeline.
	a, _ := reg.Get(ActionConnectRestartTask)
	if err := a.(actions.Scoped).CheckScope(pipeline, json.RawMessage(`{"connector":"other-connector","task":0}`)); err == nil {
		t.Fatal("out-of-pipeline connector passed the scope check")
	}
}
