package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// panicTool is a read tool that panics, standing in for a bug in a tool.
type panicTool struct {
	name  string
	value any
}

func (t *panicTool) Name() string                { return t.name }
func (t *panicTool) Description() string         { return "panicking test tool" }
func (t *panicTool) InputSchema() map[string]any { return testSchema("connector") }
func (t *panicTool) Kind() tools.Kind            { return tools.KindRead }
func (t *panicTool) Run(context.Context, json.RawMessage) (tools.Result, error) {
	panic(t.value)
}

// panicOnSink panics when the given event type is emitted, which is how a
// room tool (they emit through the sink) is made to panic.
type panicOnSink struct {
	fakeSink
	on events.Type
}

func (s *panicOnSink) Emit(ctx context.Context, typ events.Type, payload any) error {
	if typ == s.on {
		panic("room tool boom")
	}
	return s.fakeSink.Emit(ctx, typ, payload)
}

// expectPanic runs f and returns what it panicked with, failing if it did not.
func expectPanic(t *testing.T, f func()) (got any) {
	t.Helper()
	defer func() {
		got = recover()
		if got == nil {
			t.Fatal("call returned; want a panic")
		}
	}()
	f()
	return nil
}

func toolTurnJSON(name, input string) string {
	return `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu1","name":"` + name + `","input":` + input + `}],"usage":{"input_tokens":60,"output_tokens":10}}`
}

func TestReadToolPanicReachesCallerAndUsageRunsOnce(t *testing.T) {
	reg := tools.NewRegistry()
	if err := reg.Register(&panicTool{name: "connect_status", value: "tool boom"}); err != nil {
		t.Fatal(err)
	}
	turn := mustMessage(t, toolTurnJSON("connect_status", `{"why":"check","connector":"c"}`))
	ep := testEndpoint(&scriptedModel{turns: []anthropic.BetaMessage{turn}}, 0)
	u := &usageRecorder{}
	got := expectPanic(t, func() {
		_ = runEndpoint(ep, "r", Deps{Tools: reg, Usage: u.fn}, Options{})
	})
	if got != "tool boom" {
		t.Fatalf("recovered %v, want the tool's own panic value", got)
	}
	u.once(t, 70)
}

func TestReadToolPanicKeepsOneOfSeveral(t *testing.T) {
	reg := tools.NewRegistry()
	for _, n := range []string{"tool_a", "tool_b", "tool_c"} {
		if err := reg.Register(&panicTool{name: n, value: "boom " + n}); err != nil {
			t.Fatal(err)
		}
	}
	turn := mustMessage(t, `{"stop_reason":"tool_use","content":[
		{"type":"tool_use","id":"a","name":"tool_a","input":{"why":"x","connector":"c"}},
		{"type":"tool_use","id":"b","name":"tool_b","input":{"why":"x","connector":"c"}},
		{"type":"tool_use","id":"c","name":"tool_c","input":{"why":"x","connector":"c"}}
	],"usage":{"input_tokens":60,"output_tokens":10}}`)
	ep := testEndpoint(&scriptedModel{turns: []anthropic.BetaMessage{turn}}, 0)
	u := &usageRecorder{}
	got := expectPanic(t, func() {
		_ = runEndpoint(ep, "r", Deps{Tools: reg, Usage: u.fn}, Options{})
	})
	s, _ := got.(string)
	if s != "boom tool_a" && s != "boom tool_b" && s != "boom tool_c" {
		t.Fatalf("recovered %v, want one of the tools' panic values", got)
	}
	u.once(t, 70)
}

func TestRoomToolPanicReachesCallerAndUsageRunsOnce(t *testing.T) {
	turn := mustMessage(t, toolTurnJSON("update_hypothesis", `{"text":"x","based_on_steps":[],"confidence":"high"}`))
	ep := testEndpoint(&scriptedModel{turns: []anthropic.BetaMessage{turn}}, 0)
	u := &usageRecorder{}
	sink := &panicOnSink{on: events.AgentHypothesis}
	got := expectPanic(t, func() {
		_ = runEndpoint(ep, "r", Deps{Sink: sink, Usage: u.fn}, Options{})
	})
	if got != "room tool boom" {
		t.Fatalf("recovered %v, want the room tool's panic value", got)
	}
	u.once(t, 70)
}

func TestNormalToolRunIsUnaffectedByPanicRecovery(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	reg := tools.NewRegistry()
	if err := reg.Register(rec); err != nil {
		t.Fatal(err)
	}
	turn := mustMessage(t, toolTurnJSON("connect_status", `{"why":"check","connector":"c"}`))
	ep := testEndpoint(&scriptedModel{turns: []anthropic.BetaMessage{turn}}, 0)
	u := &usageRecorder{}
	if err := runEndpoint(ep, "r", Deps{Tools: reg, Usage: u.fn}, Options{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := len(rec.inputsAsJSON()); n != 1 {
		t.Fatalf("tool ran %d times, want 1", n)
	}
	u.once(t, 70)
}
