package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

type fakeAction struct {
	name   string
	layer  string
	params map[string]any
}

func (a *fakeAction) Name() string                   { return a.name }
func (a *fakeAction) Layer() string                  { return a.layer }
func (a *fakeAction) Describe() string               { return a.name + " description" }
func (a *fakeAction) ParamsSchema() map[string]any   { return a.params }
func (a *fakeAction) Validate(json.RawMessage) error { return nil }
func (a *fakeAction) Execute(context.Context, json.RawMessage) (string, error) {
	return "done", nil
}

type fakeProposals struct {
	created events.ProposalCreatedPayload
	id      string
}

func (f *fakeProposals) Create(_ context.Context, p events.ProposalCreatedPayload) (string, error) {
	f.created = p
	return f.id, nil
}

func TestUpdateHypothesis(t *testing.T) {
	sink := &fakeSink{}
	tool := &updateHypothesisTool{sink: sink}

	res, err := tool.Run(context.Background(), json.RawMessage(`{"text":"amount type changed","based_on_steps":["s_1"],"confidence":"high"}`))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Summary == "" {
		t.Fatal("expected summary")
	}
	got := sink.ofType(events.AgentHypothesis)
	if len(got) != 1 {
		t.Fatalf("expected 1 hypothesis event, got %d", len(got))
	}
	var p events.AgentHypothesisPayload
	if err := json.Unmarshal(got[0].payload, &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.Confirmed {
		t.Fatal("hypothesis must start unconfirmed")
	}
	if p.Confidence != "high" || p.Text != "amount type changed" {
		t.Fatalf("unexpected payload: %+v", p)
	}
}

func TestUpdateHypothesisBadConfidence(t *testing.T) {
	sink := &fakeSink{}
	tool := &updateHypothesisTool{sink: sink}
	_, err := tool.Run(context.Background(), json.RawMessage(`{"text":"x","based_on_steps":[],"confidence":"sure"}`))
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("expected ErrBadInput, got %v", err)
	}
}

func TestPostMessage(t *testing.T) {
	sink := &fakeSink{}
	tool := &postMessageTool{sink: sink}
	_, err := tool.Run(context.Background(), json.RawMessage(`{"text":"wrong service?","reply_to":"c_1"}`))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	msgs := sink.ofType(events.AgentMessage)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	var p events.AgentMessagePayload
	if err := json.Unmarshal(msgs[0].payload, &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.ReplyTo != "c_1" || p.Text != "wrong service?" {
		t.Fatalf("unexpected payload: %+v", p)
	}
}

func TestProposeFix(t *testing.T) {
	reg := actions.NewRegistry()
	if err := reg.Register(&fakeAction{name: "connect_restart_task", layer: "kafka-connect"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(&fakeAction{name: "schema_set_compatibility", layer: "schema-registry"}); err != nil {
		t.Fatal(err)
	}
	proposals := &fakeProposals{id: "p_1"}
	tool := &proposeFixTool{actions: reg, proposals: proposals, sink: &fakeSink{}}

	raw := json.RawMessage(`{
		"title": "set compatibility and restart task",
		"steps": [
			{"action":"schema_set_compatibility","params":{"subject":"pg.public.orders-value","level":"NONE"},"description":"allow the new schema"},
			{"action":"connect_restart_task","params":{"connector":"pg-orders-connector","task":0},"description":"restart task 0"}
		],
		"risk": "low",
		"rollback": "restore BACKWARD"
	}`)
	res, err := tool.Run(context.Background(), raw)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if proposals.created.Title != "set compatibility and restart task" {
		t.Fatalf("proposal not created: %+v", proposals.created)
	}
	if len(proposals.created.RequiredLayers) != 2 {
		t.Fatalf("required layers = %v, want 2 unique", proposals.created.RequiredLayers)
	}
	if proposals.created.RequiredLayers[0] != "schema-registry" || proposals.created.RequiredLayers[1] != "kafka-connect" {
		t.Fatalf("unexpected layer order: %v", proposals.created.RequiredLayers)
	}
	if !strings.Contains(res.Summary, "p_1") {
		t.Fatalf("result should name the proposal id: %q", res.Summary)
	}
}

func TestProposeFixUnknownAction(t *testing.T) {
	reg := actions.NewRegistry()
	tool := &proposeFixTool{actions: reg, proposals: &fakeProposals{id: "p_1"}}
	raw := json.RawMessage(`{"title":"x","steps":[{"action":"nope","params":{},"description":"d"}],"risk":"","rollback":""}`)
	_, err := tool.Run(context.Background(), raw)
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("expected ErrBadInput, got %v", err)
	}
}
