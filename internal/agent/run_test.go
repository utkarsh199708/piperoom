package agent

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// ---- fakes ----

// scriptedModel returns a fixed sequence of assistant turns and records the
// messages each request sent, so tests can assert history is append-only.
type scriptedModel struct {
	turns []anthropic.BetaMessage
	calls [][]anthropic.BetaMessageParam
}

func (m *scriptedModel) Turn(_ context.Context, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	m.calls = append(m.calls, cloneMessages(p.Messages))
	i := len(m.calls) - 1
	if i < len(m.turns) {
		return m.turns[i], nil
	}
	return anthropic.BetaMessage{
		StopReason: anthropic.BetaStopReasonEndTurn,
		Content:    []anthropic.BetaContentBlockUnion{{Type: "text", Text: "done"}},
	}, nil
}

// cloneMessages deep-copies via JSON so the captured history is immune to any
// later append reallocating the backing array.
func cloneMessages(ms []anthropic.BetaMessageParam) []anthropic.BetaMessageParam {
	b, err := json.Marshal(ms)
	if err != nil {
		return nil
	}
	var out []anthropic.BetaMessageParam
	if err := json.Unmarshal(b, &out); err != nil {
		return nil
	}
	return out
}

func mustMessage(t *testing.T, s string) anthropic.BetaMessage {
	t.Helper()
	var m anthropic.BetaMessage
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("bad scripted message: %v", err)
	}
	return m
}

type fakeInbox struct {
	mu         sync.Mutex
	drains     [][]HumanInput
	drainCalls int
	waitErr    error

	// drainErr, when set, is returned by the next drainErrTimes calls to
	// Drain (consumed before drains), so tests can script a flaky store.
	drainErr      error
	drainErrTimes int
	// waitImmediate, when set, makes Wait return nil at once instead of
	// blocking on ctx.Done() — a "wait says something changed" signal that
	// carries no information of its own, used to test that a Drain error
	// does not spin the loop.
	waitImmediate bool
}

func (f *fakeInbox) Drain(_ context.Context) ([]HumanInput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.drainErrTimes > 0 {
		f.drainErrTimes--
		return nil, f.drainErr
	}
	if f.drainCalls < len(f.drains) {
		out := f.drains[f.drainCalls]
		f.drainCalls++
		return out, nil
	}
	return nil, nil
}

func (f *fakeInbox) Wait(ctx context.Context) error {
	if f.waitErr != nil {
		return f.waitErr
	}
	if f.waitImmediate {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

type sinkEvent struct {
	typ     events.Type
	payload json.RawMessage
}

type fakeSink struct {
	mu     sync.Mutex
	events []sinkEvent
}

func (s *fakeSink) Emit(_ context.Context, typ events.Type, payload any) error {
	b, _ := json.Marshal(payload)
	s.mu.Lock()
	s.events = append(s.events, sinkEvent{typ: typ, payload: b})
	s.mu.Unlock()
	return nil
}

func (s *fakeSink) ofType(typ events.Type) []sinkEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []sinkEvent
	for _, e := range s.events {
		if e.typ == typ {
			out = append(out, e)
		}
	}
	return out
}

// recordingTool captures every input it is run with, so tests can assert the
// injected "why" property was stripped before Tool.Run.
type recordingTool struct {
	name   string
	schema map[string]any
	result tools.Result
	err    error

	mu     sync.Mutex
	inputs []json.RawMessage
}

func (t *recordingTool) Name() string                { return t.name }
func (t *recordingTool) Description() string         { return "recording test tool" }
func (t *recordingTool) InputSchema() map[string]any { return t.schema }
func (t *recordingTool) Kind() tools.Kind            { return tools.KindRead }
func (t *recordingTool) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	t.mu.Lock()
	t.inputs = append(t.inputs, append([]byte(nil), input...))
	t.mu.Unlock()
	return t.result, t.err
}

func (t *recordingTool) inputsAsJSON() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, len(t.inputs))
	for i, in := range t.inputs {
		out[i] = string(in)
	}
	return out
}

func testSchema(required ...string) map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string"},
		},
		"additionalProperties": false,
		"required":             required,
	}
}

func newTestRunner(t *testing.T, model model, inbox Inbox, sink Sink, tool tools.Tool, opts Options) *runner {
	t.Helper()
	reg := tools.NewRegistry()
	if err := reg.Register(tool); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	r, err := newRunner(Alert{Title: "orders stale", PipelineID: "orders", PipelineMap: "pg -> connect -> topic -> consumer -> warehouse"}, Deps{
		Tools:  reg,
		Inbox:  inbox,
		Sink:   sink,
		Logger: discardLogger(),
	}, opts)
	if err != nil {
		t.Fatalf("newRunner: %v", err)
	}
	r.model = model
	return r
}

// ---- helpers ----

func messagesJSON(ms []anthropic.BetaMessageParam) string {
	b, _ := json.Marshal(ms)
	return string(b)
}

func countToolResults(m anthropic.BetaMessageParam) int {
	n := 0
	for _, b := range m.Content {
		if b.OfToolResult != nil {
			n++
		}
	}
	return n
}

func lastMessage(calls [][]anthropic.BetaMessageParam) anthropic.BetaMessageParam {
	last := calls[len(calls)-1]
	return last[len(last)-1]
}

// ---- tests ----

func TestRunParallelToolCallsProduceOneUserMessage(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"check connect","connector":"pg-orders-connector"}},
			{"type":"tool_use","id":"tu2","name":"connect_status","input":{"why":"check again","connector":"other"}}
		]}`),
	}}
	sink := &fakeSink{}
	inbox := &fakeInbox{waitErr: context.Canceled}
	r := newTestRunner(t, model, inbox, sink, rec, Options{MaxTurns: 3})

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(model.calls) < 2 {
		t.Fatalf("expected at least 2 model calls, got %d", len(model.calls))
	}
	last := lastMessage(model.calls)
	if got := countToolResults(last); got != 2 {
		t.Fatalf("expected 2 tool results in one user message, got %d", got)
	}
	if len(rec.inputsAsJSON()) != 2 {
		t.Fatalf("expected 2 tool executions, got %d", len(rec.inputsAsJSON()))
	}
}

func TestRunWhyIsStripped(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"check connect","connector":"pg-orders-connector"}}
		]}`),
	}}
	sink := &fakeSink{}
	inbox := &fakeInbox{waitErr: context.Canceled}
	r := newTestRunner(t, model, inbox, sink, rec, Options{MaxTurns: 3})

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := rec.inputsAsJSON()
	if len(got) != 1 {
		t.Fatalf("expected 1 tool execution, got %d", len(got))
	}
	if strings.Contains(got[0], `"why"`) {
		t.Fatalf("why was not stripped: %s", got[0])
	}
	if !strings.Contains(got[0], `"connector"`) {
		t.Fatalf("connector missing from stripped input: %s", got[0])
	}
	// The step title should come from the why text.
	steps := sink.ofType(events.AgentStepStart)
	if len(steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(steps))
	}
	var p events.AgentStepStartPayload
	if err := json.Unmarshal(steps[0].payload, &p); err != nil {
		t.Fatalf("decode step: %v", err)
	}
	if p.Title != "check connect" {
		t.Fatalf("step title = %q, want %q", p.Title, "check connect")
	}
}

func TestRunUnknownToolIsError(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"no_such_tool","input":{"why":"probe"}}
		]}`),
	}}
	sink := &fakeSink{}
	inbox := &fakeInbox{waitErr: context.Canceled}
	r := newTestRunner(t, model, inbox, sink, rec, Options{MaxTurns: 3})

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	results := sink.ofType(events.AgentToolResult)
	if len(results) != 1 {
		t.Fatalf("expected 1 tool result, got %d", len(results))
	}
	var p events.AgentToolResultPayload
	if err := json.Unmarshal(results[0].payload, &p); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if !p.IsError {
		t.Fatalf("expected is_error=true, got false")
	}
	if len(rec.inputsAsJSON()) != 0 {
		t.Fatalf("unknown tool must not execute a real tool, got %v", rec.inputsAsJSON())
	}
}

func TestRunInboxInputLandsInNextUserMessage(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"check connect","connector":"pg-orders-connector"}}
		]}`),
		mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}`),
	}}
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil, // before turn 0
			{{Kind: "steer", Actor: events.Actor{Name: "Arjun"}, Text: "also check order_items"}},
		},
		waitErr: context.Canceled,
	}
	r := newTestRunner(t, model, inbox, sink, rec, Options{MaxTurns: 5})

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(model.calls) < 2 {
		t.Fatalf("expected at least 2 model calls, got %d", len(model.calls))
	}
	last := lastMessage(model.calls)
	js := messagesJSON([]anthropic.BetaMessageParam{last})
	if !strings.Contains(js, "also check order_items") {
		t.Fatalf("human input missing from next user message: %s", js)
	}
}

func TestRunHistoryNeverMutated(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"check connect","connector":"pg-orders-connector"}}
		]}`),
		mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}`),
	}}
	sink := &fakeSink{}
	inbox := &fakeInbox{waitErr: context.Canceled}
	r := newTestRunner(t, model, inbox, sink, rec, Options{MaxTurns: 5})

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(model.calls) < 2 {
		t.Fatalf("expected at least 2 model calls, got %d", len(model.calls))
	}
	for i := 1; i < len(model.calls); i++ {
		prev := model.calls[i-1]
		if len(model.calls[i]) < len(prev) {
			t.Fatalf("history shrank at call %d", i)
		}
		prefix := messagesJSON(model.calls[i][:len(prev)])
		full := messagesJSON(prev)
		if prefix != full {
			t.Fatalf("earlier messages changed at call %d\nbefore: %s\nafter:  %s", i, full, prefix)
		}
	}
}

func TestRunRefusalGoesIdleAndWaits(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"refusal","content":[],"stop_details":{"category":"general_harms","explanation":"cannot do this"}}`),
	}}
	sink := &fakeSink{}
	inbox := &fakeInbox{waitErr: context.Canceled}
	r := newTestRunner(t, model, inbox, sink, rec, Options{MaxTurns: 3})

	// A refusal no longer ends the investigation: it goes idle and waits. The
	// cancelled inbox ends the run cleanly.
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(model.calls) != 1 {
		t.Fatalf("refused turn must not be appended; expected 1 model call, got %d", len(model.calls))
	}
	statuses := sink.ofType(events.AgentStatus)
	var errDetail string
	var sawIdle bool
	for _, s := range statuses {
		var p events.AgentStatusPayload
		if json.Unmarshal(s.payload, &p) != nil {
			continue
		}
		if p.State == "error" {
			errDetail = p.Detail
		}
		if p.State == "idle" {
			sawIdle = true
		}
	}
	if errDetail == "" {
		t.Fatalf("expected agent.status error, got %v", statuses)
	}
	if !strings.Contains(errDetail, "general_harms") || !strings.Contains(errDetail, "cannot do this") {
		t.Fatalf("error detail should use stop_details, got %q", errDetail)
	}
	if !sawIdle {
		t.Fatalf("expected agent.status idle after refusal, got %v", statuses)
	}
	if msgs := sink.ofType(events.AgentMessage); len(msgs) == 0 {
		t.Fatalf("expected an agent.message after refusal")
	}
}

func TestRunMaxTurnsPausesAndResetsOnInput(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"check connect","connector":"pg-orders-connector"}}
		]}`),
	}}
	sink := &fakeSink{}
	inbox := &stagedInbox{inputOnDrain: 4, text: "keep going"}
	r := newTestRunner(t, model, inbox, sink, rec, Options{MaxTurns: 1})

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	// After hitting the limit the agent pauses, receives a steer, and runs one
	// more turn (the scripted model's end_turn default) before the inbox ends.
	if len(model.calls) != 2 {
		t.Fatalf("expected 2 model calls, got %d", len(model.calls))
	}
	statuses := sink.ofType(events.AgentStatus)
	for _, s := range statuses {
		var p events.AgentStatusPayload
		if json.Unmarshal(s.payload, &p) == nil && p.State == "done" {
			t.Fatalf("run must not end with done while humans are steering; got %v", statuses)
		}
	}
	var sawPause bool
	for _, s := range statuses {
		var p events.AgentStatusPayload
		if json.Unmarshal(s.payload, &p) == nil && p.State == "idle" && strings.Contains(p.Detail, "max turns") {
			sawPause = true
		}
	}
	if !sawPause {
		t.Fatalf("expected an idle status noting max turns, got %v", statuses)
	}
	last := lastMessage(model.calls)
	if !strings.Contains(messagesJSON([]anthropic.BetaMessageParam{last}), "keep going") {
		t.Fatalf("resumed request should carry the human steer: %s", messagesJSON([]anthropic.BetaMessageParam{last}))
	}
}

// TestRunToolResultCarriesStepID covers C13 P1: the step id is emitted to the
// room, but the model only sees the tool_result block, so the wrapper must
// carry the same step id — the system prompt tells the model to cite it.
func TestRunToolResultCarriesStepID(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "task 0 FAILED"}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"check connect","connector":"pg-orders-connector"}}
		]}`),
	}}
	sink := &fakeSink{}
	inbox := &fakeInbox{waitErr: context.Canceled}
	r := newTestRunner(t, model, inbox, sink, rec, Options{MaxTurns: 3})

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	steps := sink.ofType(events.AgentStepStart)
	if len(steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(steps))
	}
	var sp events.AgentStepStartPayload
	if err := json.Unmarshal(steps[0].payload, &sp); err != nil {
		t.Fatalf("decode step: %v", err)
	}
	if !strings.HasPrefix(sp.StepID, "s_") {
		t.Fatalf("step id %q must use the s_ prefix", sp.StepID)
	}

	results := sink.ofType(events.AgentToolResult)
	if len(results) != 1 {
		t.Fatalf("expected 1 tool result, got %d", len(results))
	}
	var rp events.AgentToolResultPayload
	if err := json.Unmarshal(results[0].payload, &rp); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if rp.StepID != sp.StepID {
		t.Fatalf("tool result step id %q != step start step id %q", rp.StepID, sp.StepID)
	}

	// The model-visible tool result must contain the emitted step id, both as
	// the wrapper attribute and as the raw id the model can quote.
	if len(model.calls) < 2 {
		t.Fatalf("expected at least 2 model calls, got %d", len(model.calls))
	}
	js := messagesJSON([]anthropic.BetaMessageParam{lastMessage(model.calls)})
	if !strings.Contains(js, sp.StepID) {
		t.Fatalf("step id %q missing from the model-visible tool result: %s", sp.StepID, js)
	}
	if !strings.Contains(js, `step=\"`+sp.StepID+`\"`) {
		t.Fatalf("wrapper must carry step=%q on the tool result: %s", sp.StepID, js)
	}
}
