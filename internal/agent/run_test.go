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
	turns []anthropic.Message
	calls [][]anthropic.MessageParam
}

func (m *scriptedModel) Turn(_ context.Context, p anthropic.MessageNewParams) (anthropic.Message, error) {
	m.calls = append(m.calls, cloneMessages(p.Messages))
	i := len(m.calls) - 1
	if i < len(m.turns) {
		return m.turns[i], nil
	}
	return anthropic.Message{
		StopReason: anthropic.StopReasonEndTurn,
		Content:    []anthropic.ContentBlockUnion{{Type: "text", Text: "done"}},
	}, nil
}

// cloneMessages deep-copies via JSON so the captured history is immune to any
// later append reallocating the backing array.
func cloneMessages(ms []anthropic.MessageParam) []anthropic.MessageParam {
	b, err := json.Marshal(ms)
	if err != nil {
		return nil
	}
	var out []anthropic.MessageParam
	if err := json.Unmarshal(b, &out); err != nil {
		return nil
	}
	return out
}

func mustMessage(t *testing.T, s string) anthropic.Message {
	t.Helper()
	var m anthropic.Message
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
}

func (f *fakeInbox) Drain(_ context.Context) ([]HumanInput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	return t.result, nil
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

func messagesJSON(ms []anthropic.MessageParam) string {
	b, _ := json.Marshal(ms)
	return string(b)
}

func countToolResults(m anthropic.MessageParam) int {
	n := 0
	for _, b := range m.Content {
		if b.OfToolResult != nil {
			n++
		}
	}
	return n
}

func lastMessage(calls [][]anthropic.MessageParam) anthropic.MessageParam {
	last := calls[len(calls)-1]
	return last[len(last)-1]
}

// ---- tests ----

func TestRunParallelToolCallsProduceOneUserMessage(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.Message{
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
	model := &scriptedModel{turns: []anthropic.Message{
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
	model := &scriptedModel{turns: []anthropic.Message{
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
	model := &scriptedModel{turns: []anthropic.Message{
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
	js := messagesJSON([]anthropic.MessageParam{last})
	if !strings.Contains(js, "also check order_items") {
		t.Fatalf("human input missing from next user message: %s", js)
	}
}

func TestRunHistoryNeverMutated(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.Message{
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

func TestRunRefusalEmitsErrorStatus(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.Message{
		mustMessage(t, `{"stop_reason":"refusal","content":[]}`),
	}}
	sink := &fakeSink{}
	inbox := &fakeInbox{}
	r := newTestRunner(t, model, inbox, sink, rec, Options{MaxTurns: 3})

	err := r.run(context.Background())
	if err == nil {
		t.Fatal("expected an error on refusal")
	}
	statuses := sink.ofType(events.AgentStatus)
	found := false
	for _, s := range statuses {
		var p events.AgentStatusPayload
		if json.Unmarshal(s.payload, &p) == nil && p.State == "error" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected agent.status error, got %v", statuses)
	}
}

func TestRunMaxTurnsStops(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.Message{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"check connect","connector":"pg-orders-connector"}}
		]}`),
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu2","name":"connect_status","input":{"why":"check again","connector":"other"}}
		]}`),
	}}
	sink := &fakeSink{}
	inbox := &fakeInbox{}
	r := newTestRunner(t, model, inbox, sink, rec, Options{MaxTurns: 1})

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(model.calls) != 1 {
		t.Fatalf("expected exactly 1 model call, got %d", len(model.calls))
	}
	statuses := sink.ofType(events.AgentStatus)
	found := false
	for _, s := range statuses {
		var p events.AgentStatusPayload
		if json.Unmarshal(s.payload, &p) == nil && p.State == "done" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected agent.status done, got %v", statuses)
	}
}
