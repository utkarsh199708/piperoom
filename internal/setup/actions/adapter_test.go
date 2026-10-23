package setupactions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

// call is one request a fake runner received.
type call struct {
	roomID string
	req    protocol.OpRequest
}

// fakeRunner records every request and answers from answer.
type fakeRunner struct {
	mu     sync.Mutex
	calls  []call
	answer func(protocol.OpRequest) (protocol.OpResult, error)
}

func (f *fakeRunner) Execute(_ context.Context, roomID string, req protocol.OpRequest) (protocol.OpResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, call{roomID: roomID, req: req})
	answer := f.answer
	f.mu.Unlock()
	if answer != nil {
		return answer(req)
	}
	return protocol.OpResult{ID: req.ID, OK: true, Summary: "done"}, nil
}

func (f *fakeRunner) recorded() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

// testReceipt is the runner's receipt a fake returns.
var testReceipt = events.SetupReceipt{
	Kind:     "exec",
	Argv:     []string{"docker", "compose", "up", "-d"},
	Target:   "piperoom-orders",
	ExitCode: 0,
	At:       time.Date(2026, 10, 20, 10, 30, 0, 0, time.UTC),
	TookMS:   1234,
	OutMAC:   "a1b2c3",
}

// testSpec is one write op with a table-list scope and a T0 preview op.
func testSpec() Spec {
	return Spec{
		Name:     "source_grant_select",
		Tier:     plan.TierT2,
		Layer:    "postgres",
		Describe: "Grant SELECT on the named tables.",
		Schema: objectSchema(map[string]any{
			"role":   map[string]any{"type": "string", "minLength": 1},
			"tables": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		}, "role", "tables"),
		Scope:     ScopeListField("tables", func(p owners.Pipeline) []string { return p.SourceTables }),
		PreviewOp: PreviewOpName("source_grant_select"),
	}
}

// testPipeline is the room's provisional pipeline: two tables and one
// connector.
func testPipeline() owners.Pipeline {
	return owners.Pipeline{
		ID:           "orders",
		TopicPrefix:  "orders",
		SourceTables: []string{"public.orders", "public.order_items"},
		Connectors:   []string{"pg-orders"},
	}
}

// testContext is an action call's context as the room builds it: the pipeline
// (tools.WithPipeline), the room id and the frozen proposal step.
func testContext(t *testing.T, params json.RawMessage) context.Context {
	t.Helper()
	ctx := tools.WithPipeline(context.Background(), testPipeline())
	ctx = room.WithRoomID(ctx, "r_setup")
	idx := 1
	return room.WithSetupStep(ctx, room.SetupStep{
		ProposalID: "p_1",
		StepIndex:  idx,
		ParamsHash: "hash_abc",
		Tier:       plan.TierT2,
		Class:      protocol.ClassShared,
		Action:     "source_grant_select",
		Params:     params,
	})
}

func newTestAdapter(t *testing.T, f *fakeRunner) *Adapter {
	t.Helper()
	a, err := New(testSpec(), Deps{Runner: f, RoomID: "r_setup"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a
}

var grantParams = json.RawMessage(`{"role":"piperoom_reader","tables":["public.orders"]}`)

func TestAdapterImplementsTheRoomInterfaces(t *testing.T) {
	a := newTestAdapter(t, &fakeRunner{})
	var action actions.Action = a
	var _ actions.Scoped = a
	var _ actions.Preview = a
	var _ actions.Tiered = a
	var _ actions.Receipted = a
	if action.Name() != "source_grant_select" || action.Layer() != "postgres" {
		t.Fatalf("action = %s/%s", action.Name(), action.Layer())
	}
	if got := actions.ClassOf(action); got != actions.ClassFix {
		t.Fatalf("class = %q, want a plain fix", got)
	}
}

func TestNewRefusesABadSpec(t *testing.T) {
	good := testSpec()
	cases := map[string]func(*Spec){
		"empty name":   func(s *Spec) { s.Name = "" },
		"bad name":     func(s *Spec) { s.Name = "Source Grant" },
		"unknown tier": func(s *Spec) { s.Tier = "T9" },
		"no layer":     func(s *Spec) { s.Layer = "" },
		"no describe":  func(s *Spec) { s.Describe = "" },
		"nil schema":   func(s *Spec) { s.Schema = nil },
		"open schema": func(s *Spec) {
			s.Schema = map[string]any{"type": "object", "additionalProperties": true, "required": []string{}}
		},
		"not an object": func(s *Spec) {
			s.Schema = map[string]any{"type": "string", "additionalProperties": false, "required": []string{}}
		},
		"no required list": func(s *Spec) { s.Schema = map[string]any{"type": "object", "additionalProperties": false} },
		"no scope":         func(s *Spec) { s.Scope = nil },
		"preview is a write": func(s *Spec) {
			s.PreviewOp = s.Name
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			spec := good
			mutate(&spec)
			if _, err := New(spec, Deps{}); !errors.Is(err, ErrBadSpec) {
				t.Fatalf("New = %v, want ErrBadSpec", err)
			}
		})
	}
}

func TestExecuteReceiptReturnsTheRunnersReceipt(t *testing.T) {
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: true, Summary: "granted SELECT on 1 table", Receipt: testReceipt}, nil
	}}
	a := newTestAdapter(t, f)
	ctx := testContext(t, grantParams)
	out, receipt, err := a.ExecuteReceipt(ctx, grantParams)
	if err != nil {
		t.Fatalf("ExecuteReceipt: %v", err)
	}
	if out != "granted SELECT on 1 table" {
		t.Fatalf("out = %q", out)
	}
	if !sameReceipt(receipt, testReceipt) {
		t.Fatalf("receipt = %+v, want the runner's %+v", receipt, testReceipt)
	}
	calls := f.recorded()
	if len(calls) != 1 {
		t.Fatalf("sent %d ops, want 1", len(calls))
	}
	req := calls[0].req
	if calls[0].roomID != "r_setup" {
		t.Fatalf("room = %q", calls[0].roomID)
	}
	if req.Op != "source_grant_select" {
		t.Fatalf("op = %q", req.Op)
	}
	if req.Tier != plan.TierT2 || req.Class != protocol.ClassShared {
		t.Fatalf("tier/class = %q/%q, want T2/shared", req.Tier, req.Class)
	}
	if req.ProposalID != "p_1" || req.ParamsHash != "hash_abc" || req.StepIndex == nil || *req.StepIndex != 1 {
		t.Fatalf("proposal fields = %+v", req)
	}
	if string(req.Params) != string(grantParams) {
		t.Fatalf("params = %s, want the step's %s", req.Params, grantParams)
	}
	if req.Deadline.IsZero() || req.ID == "" {
		t.Fatalf("request has no deadline or id: %+v", req)
	}
}

// sameReceipt compares two receipts field by field (Argv is a slice).
func sameReceipt(a, b events.SetupReceipt) bool {
	if a.Kind != b.Kind || a.Target != b.Target || a.ExitCode != b.ExitCode || !a.At.Equal(b.At) || a.TookMS != b.TookMS || a.OutMAC != b.OutMAC {
		return false
	}
	if len(a.Argv) != len(b.Argv) {
		return false
	}
	for i := range a.Argv {
		if a.Argv[i] != b.Argv[i] {
			return false
		}
	}
	return true
}

func TestExecuteReturnsTheRunnersSummary(t *testing.T) {
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: true, Summary: "granted"}, nil
	}}
	a := newTestAdapter(t, f)
	out, err := a.Execute(testContext(t, grantParams), grantParams)
	if err != nil || out != "granted" {
		t.Fatalf("Execute = %q, %v", out, err)
	}
}

func TestExecuteRaisesATierTheRoomRaised(t *testing.T) {
	f := &fakeRunner{}
	a := newTestAdapter(t, f)
	ctx := testContext(t, grantParams)
	ctx = room.WithSetupStep(ctx, room.SetupStep{
		ProposalID: "p_1", StepIndex: 0, ParamsHash: "h", Tier: plan.TierT3,
		Class: protocol.ClassProduction, Action: "source_grant_select", Params: grantParams,
	})
	if _, _, err := a.ExecuteReceipt(ctx, grantParams); err != nil {
		t.Fatalf("ExecuteReceipt: %v", err)
	}
	req := f.recorded()[0].req
	if req.Tier != plan.TierT3 {
		t.Fatalf("tier = %q, want the step's T3", req.Tier)
	}
}

func TestExecuteRefusesWithoutAStep(t *testing.T) {
	f := &fakeRunner{}
	a := newTestAdapter(t, f)
	ctx := tools.WithPipeline(context.Background(), testPipeline())
	_, _, err := a.ExecuteReceipt(ctx, grantParams)
	if !errors.Is(err, ErrNoStep) {
		t.Fatalf("ExecuteReceipt without a step = %v, want ErrNoStep", err)
	}
	if got := len(f.recorded()); got != 0 {
		t.Fatalf("a stepless write sent %d ops", got)
	}
}

func TestExecuteRefusesWithoutAPipeline(t *testing.T) {
	f := &fakeRunner{}
	a := newTestAdapter(t, f)
	ctx := testContext(t, grantParams)
	ctx = tools.WithPipelineID(ctx, "") // clear the pipeline, keep the step
	_, _, err := a.ExecuteReceipt(ctx, grantParams)
	if !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("ExecuteReceipt without a pipeline = %v, want ErrNoScope", err)
	}
	if got := len(f.recorded()); got != 0 {
		t.Fatalf("an unscoped write sent %d ops", got)
	}
}

func TestExecuteRefusesAStepThatIsNotThisWrite(t *testing.T) {
	cases := []struct {
		name string
		step func(room.SetupStep) room.SetupStep
	}{
		{"another action", func(s room.SetupStep) room.SetupStep { s.Action = "compose_up"; return s }},
		{"other params", func(s room.SetupStep) room.SetupStep {
			s.Params = json.RawMessage(`{"role":"piperoom_reader","tables":["public.customers"]}`)
			return s
		}},
		{"no proposal", func(s room.SetupStep) room.SetupStep { s.ProposalID = ""; return s }},
		{"no params hash", func(s room.SetupStep) room.SetupStep { s.ParamsHash = ""; return s }},
		{"negative index", func(s room.SetupStep) room.SetupStep { s.StepIndex = -1; return s }},
		{"unknown class", func(s room.SetupStep) room.SetupStep { s.Class = "prod"; return s }},
		{"unknown tier", func(s room.SetupStep) room.SetupStep { s.Tier = "T9"; return s }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeRunner{}
			a := newTestAdapter(t, f)
			ctx := testContext(t, grantParams)
			step, _ := room.SetupStepFrom(ctx)
			ctx = room.WithSetupStep(ctx, tc.step(step))
			_, _, err := a.ExecuteReceipt(ctx, grantParams)
			if !errors.Is(err, ErrStepMismatch) {
				t.Fatalf("ExecuteReceipt = %v, want ErrStepMismatch", err)
			}
			if got := len(f.recorded()); got != 0 {
				t.Fatalf("a mismatched step sent %d ops", got)
			}
		})
	}
}

func TestExecuteAcceptsEquivalentParams(t *testing.T) {
	f := &fakeRunner{}
	a := newTestAdapter(t, f)
	// Same value, different key order: the step binding compares canonical JSON.
	reordered := json.RawMessage(`{"tables":["public.orders"],"role":"piperoom_reader"}`)
	ctx := testContext(t, reordered)
	if _, _, err := a.ExecuteReceipt(ctx, grantParams); err != nil {
		t.Fatalf("ExecuteReceipt with reordered params: %v", err)
	}
	if got := len(f.recorded()); got != 1 {
		t.Fatalf("sent %d ops, want 1", got)
	}
}

func TestValidateRefusesAParamsArray(t *testing.T) {
	a := newTestAdapter(t, &fakeRunner{})
	if err := a.Validate(context.Background(), json.RawMessage(`[]`)); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("Validate(array) = %v, want ErrBadInput", err)
	}
	if err := a.Validate(context.Background(), json.RawMessage(`{"role":`)); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("Validate(broken JSON) = %v, want ErrBadInput", err)
	}
}

func TestExecuteTurnsARunnerRefusalIntoASentinel(t *testing.T) {
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: false, Summary: "table is outside the pipeline", Data: json.RawMessage(`{"error_code":"out_of_scope"}`)}, nil
	}}
	a := newTestAdapter(t, f)
	_, _, err := a.ExecuteReceipt(testContext(t, grantParams), grantParams)
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("ExecuteReceipt = %v, want ErrOutOfScope", err)
	}
	if !strings.Contains(err.Error(), "outside the pipeline") {
		t.Fatalf("the error lost the runner's summary: %v", err)
	}
}

func TestExecuteWithoutARunner(t *testing.T) {
	a, err := New(testSpec(), Deps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, _, err := a.ExecuteReceipt(testContext(t, grantParams), grantParams); !errors.Is(err, ErrNoRunner) {
		t.Fatalf("ExecuteReceipt without a runner = %v, want ErrNoRunner", err)
	}
}

func TestExecuteRefusesAnotherRoomsCall(t *testing.T) {
	f := &fakeRunner{}
	a := newTestAdapter(t, f)
	ctx := tools.WithPipeline(context.Background(), testPipeline())
	ctx = room.WithRoomID(ctx, "r_other")
	idx := 0
	ctx = room.WithSetupStep(ctx, room.SetupStep{
		ProposalID: "p_1", StepIndex: idx, ParamsHash: "h", Tier: plan.TierT2,
		Class: protocol.ClassShared, Action: "source_grant_select", Params: grantParams,
	})
	if _, _, err := a.ExecuteReceipt(ctx, grantParams); !errors.Is(err, ErrNoRunner) {
		t.Fatalf("ExecuteReceipt for another room = %v, want ErrNoRunner", err)
	}
	if got := len(f.recorded()); got != 0 {
		t.Fatalf("a cross-room write sent %d ops", got)
	}
}

func TestParamsSchemaIsACopy(t *testing.T) {
	a := newTestAdapter(t, &fakeRunner{})
	schema := a.ParamsSchema()
	req := schema["required"].([]string)
	req[0] = "changed"
	if again := a.ParamsSchema()["required"].([]string); again[0] != "role" {
		t.Fatalf("the returned schema edits the adapter's: %v", again)
	}
}
