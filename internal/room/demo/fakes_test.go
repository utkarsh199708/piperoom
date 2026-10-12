package demo

// The scripted agent and the demo fakes must keep working with the REAL
// actions and the owners pipeline from dev/owners.yaml: if either drifts, the
// --demo flow breaks while production stays green.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/owners"
	"piperoom/internal/topology"
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
			want:   "pg.public.orders-value compatibility BACKWARD → NONE",
		},
		{
			action: ActionConnectRestartTask,
			params: `{"connector":"pg-orders-connector","task":0}`,
			want:   "restarted pg-orders-connector task 0 (RUNNING)",
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

// TestRealResumeConnectorOverFakes runs the real connect_resume_connector action
// over the fake Connect. The demo connector starts RUNNING, which the real
// Validate refuses, so the test starts from PAUSED and checks the state the
// fake reports after the resume.
func TestRealResumeConnectorOverFakes(t *testing.T) {
	connect := NewFakeConnect()
	connect.connState = "PAUSED"
	reg := actions.NewRegistry()
	if err := actions.Register(reg, actions.Deps{Connect: connect, Registry: NewFakeRegistry()}); err != nil {
		t.Fatalf("register real actions: %v", err)
	}
	pipeline := owners.Pipeline{
		ID:           "orders",
		Title:        "Orders CDC",
		TopicPrefix:  "pg",
		SourceTables: []string{"public.orders"},
		Connectors:   []string{"pg-orders-connector"},
		SinkTables:   []string{"analytics.orders"},
		SinkTSColumn: "updated_at",
	}
	a, ok := reg.Get("connect_resume_connector")
	if !ok {
		t.Fatal("connect_resume_connector not registered")
	}
	params := json.RawMessage(`{"connector":"pg-orders-connector"}`)
	if err := a.Validate(params); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if err := a.(actions.Scoped).CheckScope(pipeline, params); err != nil {
		t.Fatalf("scope: %v", err)
	}
	got, err := a.Execute(context.Background(), params)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if want := "resumed pg-orders-connector (RUNNING)"; got != want {
		t.Fatalf("execute: got %q, want %q", got, want)
	}
	st, err := connect.Status(context.Background(), "pg-orders-connector")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.State != "RUNNING" || len(st.Tasks) != 1 || st.Tasks[0].State != "RUNNING" {
		t.Fatalf("after resume: connector %s, tasks %+v; want RUNNING and task 0 RUNNING", st.State, st.Tasks)
	}
}

// TestBuilderTraceRejectsOutOfPipelineTable pins review item 7 for the demo
// mode: the demo builder rejects a table outside the orders pipeline with
// topology.ErrBadInput so the room service maps it to 400, exactly like the
// real builder.
func TestBuilderTraceRejectsOutOfPipelineTable(t *testing.T) {
	var b Builder
	if _, err := b.Trace(context.Background(), "orders", "public.payments", "1"); !errors.Is(err, topology.ErrBadInput) {
		t.Fatalf("out-of-pipeline table: err = %v, want topology.ErrBadInput", err)
	}
	if _, err := b.Trace(context.Background(), "nope", "public.orders", "1"); !errors.Is(err, topology.ErrBadInput) {
		t.Fatalf("unknown pipeline: err = %v, want topology.ErrBadInput", err)
	}
	if _, err := b.Trace(context.Background(), "orders", "public.orders", "1"); err != nil {
		t.Fatalf("in-pipeline table: %v", err)
	}
}
