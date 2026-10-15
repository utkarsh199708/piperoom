package agent

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
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

func (a *fakeAction) Name() string                                    { return a.name }
func (a *fakeAction) Layer() string                                   { return a.layer }
func (a *fakeAction) Describe() string                                { return a.name + " description" }
func (a *fakeAction) ParamsSchema() map[string]any                    { return a.params }
func (a *fakeAction) Validate(context.Context, json.RawMessage) error { return nil }
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
	raw := json.RawMessage(`{"title":"x","steps":[{"action":"nope","params":{},"description":"d"}],"risk":"low","rollback":"restore the old task"}`)
	_, err := tool.Run(context.Background(), raw)
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("expected ErrBadInput, got %v", err)
	}
}

// TestProposeFixRejectsEmptyOrPlaceholderRiskRollback: a model that sends an
// empty or placeholder risk or rollback gets an error tool result (is_error) so
// it retries. Nothing is stored, and the text "<no value>" never reaches the
// approvers.
func TestProposeFixRejectsEmptyOrPlaceholderRiskRollback(t *testing.T) {
	cases := []struct {
		name     string
		risk     string
		rollback string
	}{
		{"empty risk", "", "restore BACKWARD"},
		{"whitespace risk", "   ", "restore BACKWARD"},
		{"no value risk", "<no value>", "restore BACKWARD"},
		{"n/a risk", "N/A", "restore BACKWARD"},
		{"none risk", "None", "restore BACKWARD"},
		{"dash risk", "-", "restore BACKWARD"},
		{"tbd risk", " TBD ", "restore BACKWARD"},
		{"empty rollback", "low", ""},
		{"whitespace rollback", "low", "\t\n"},
		{"no value rollback", "low", "<NO VALUE>"},
		{"n/a rollback", "low", "n/a"},
		{"tbd rollback", "low", "tbd"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reg := actions.NewRegistry()
			if err := reg.Register(&fakeAction{name: "connect_restart_task", layer: "kafka-connect"}); err != nil {
				t.Fatal(err)
			}
			proposals := &fakeProposals{id: "p_1"}
			r, err := newRunner(Alert{}, Deps{
				Tools:     tools.NewRegistry(),
				Actions:   reg,
				Inbox:     &fakeInbox{},
				Sink:      &fakeSink{},
				Proposals: proposals,
				Logger:    discardLogger(),
			}, Options{})
			if err != nil {
				t.Fatalf("newRunner: %v", err)
			}
			r.toolList = buildToolList(r.deps.Tools, newRoomTools(r.deps, r.proposals))

			raw, err := json.Marshal(map[string]any{
				"title":    "restart task",
				"steps":    []map[string]any{{"action": "connect_restart_task", "params": map[string]any{}, "description": "restart"}},
				"risk":     c.risk,
				"rollback": c.rollback,
			})
			if err != nil {
				t.Fatal(err)
			}
			res := r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "propose_fix", input: raw})
			if !res.isError {
				t.Fatalf("expected an error tool result, got %q", res.content)
			}
			if proposals.created.Title != "" {
				t.Fatal("a proposal with an empty or placeholder risk/rollback must not be created")
			}
		})
	}
}

// TestRoomToolNamesMatchRoomTools keeps the static roomToolNames list — what
// RoomToolNames() serves without constructing any tool — in sync with the
// tools newRoomTools really builds. Add, rename or drop a room tool without
// touching the list and this fails. The list holds every room tool, so the
// deps here give page_owner and withdraw_proposal their collaborators; what a
// run without them builds is TestRoomToolsOfferedByDeps.
func TestRoomToolNamesMatchRoomTools(t *testing.T) {
	built := newRoomTools(Deps{Pager: &fakePager{}, Proposals: &withdrawingProposals{}}, nil)
	names := make([]string, 0, len(built))
	for _, tl := range built {
		names = append(names, tl.Name())
	}
	if got := RoomToolNames(); !slices.Equal(got, names) {
		t.Fatalf("RoomToolNames() = %v, but newRoomTools builds %v", got, names)
	}
	// The returned slice must be a copy: callers cannot corrupt the list.
	got := RoomToolNames()
	got[0] = "mutated"
	if RoomToolNames()[0] != names[0] {
		t.Fatal("RoomToolNames() must return a copy of the static list")
	}
}
