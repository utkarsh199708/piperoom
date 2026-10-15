package room

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store/memstore"
	"piperoom/internal/topology"
)

// previewStub is an action with a preview; block makes Preview ignore its
// context and hang until the test ends (a client with no deadline of its own).
type previewStub struct {
	name  string
	block chan struct{}
}

func (a *previewStub) Name() string                                             { return a.name }
func (a *previewStub) Layer() string                                            { return "kafka-connect" }
func (a *previewStub) Describe() string                                         { return a.name }
func (a *previewStub) ParamsSchema() map[string]any                             { return map[string]any{"type": "object"} }
func (a *previewStub) Validate(json.RawMessage) error                           { return nil }
func (a *previewStub) Execute(context.Context, json.RawMessage) (string, error) { return "ok", nil }
func (a *previewStub) CheckScope(owners.Pipeline, json.RawMessage) error        { return nil }
func (a *previewStub) Preview(ctx context.Context, _ json.RawMessage) (actions.PreviewResult, error) {
	if a.block != nil {
		<-a.block
	}
	return actions.PreviewResult{Target: a.name, Current: "FAILED", Expected: "RUNNING"}, nil
}

// slowTopology blocks until its context ends, like a topology build against
// an unreachable cluster.
type slowTopology struct{ noopTopology }

func (slowTopology) Build(ctx context.Context, _ string) (topology.Graph, error) {
	<-ctx.Done()
	return topology.Graph{}, ctx.Err()
}

// QA X2: the web shows "Preview unavailable" when no proposal.previewed
// arrives within 20 s. A slow topology build and a step that hangs must not
// hold the event back: the topology runs beside the steps, every step is
// bounded even when the action ignores its context, and the hung step gets
// its own error.
func TestPreviewIsWrittenPromptlyWhenStepsHang(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	reg := actions.NewRegistry()
	for _, a := range []actions.Action{&previewStub{name: "connect_restart_task"}, &previewStub{name: "hangs", block: block}} {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	svc := New(Deps{Store: memstore.New(), Actions: reg, Users: DefaultUsers(), Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders"}}}, Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
		<-ctx.Done()
		return nil
	}, Topology: slowTopology{}})
	t.Cleanup(svc.Close)
	svc.preview = previewBudget{total: 600 * time.Millisecond, step: 300 * time.Millisecond, topo: 200 * time.Millisecond}

	ctx := context.Background()
	u, ok := svc.users.Get("arjun")
	if !ok {
		t.Fatal("no arjun")
	}
	r, err := svc.CreateRoom(ctx, u, "connector down", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	pid, err := svc.CreateProposal(ctx, r.ID, events.ProposalCreatedPayload{Title: "restart", Risk: "the task restarts", Rollback: "nothing to undo: a restart has no rollback", Steps: []events.ProposalStep{
		{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"c","task":0}`)},
		{Action: "hangs", Layer: "kafka-connect", Params: json.RawMessage(`{}`)},
	}}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	var got *events.ProposalPreviewedPayload
	for deadline := time.Now().Add(3 * time.Second); got == nil && time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		evs, err := svc.store.Since(ctx, r.ID, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range evs {
			if p, ok := decode[events.ProposalPreviewedPayload](e); ok && e.Type == events.ProposalPreviewed && p.ProposalID == pid {
				got = &p
			}
		}
	}
	if got == nil {
		t.Fatal("no proposal.previewed within 3 s")
	}
	if took := time.Since(start); took > 1500*time.Millisecond {
		t.Fatalf("preview took %s, want within the budget", took)
	}
	if len(got.Steps) != 2 || got.Steps[0].Preview == nil || got.Steps[1].Preview != nil || got.Steps[1].Error == "" {
		t.Fatalf("steps = %+v, want a preview for step 0 and an error for the hung step 1", got.Steps)
	}
}
