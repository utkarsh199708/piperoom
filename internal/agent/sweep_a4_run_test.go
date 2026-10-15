package agent

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// A steer that wakes an idle agent puts it back to working before the model
// writes any step: the room's failover and the hand-off check read the last
// status, and an idle agent writing steps looks finished.
func TestSteerAfterIdleEmitsWorkingBeforeSteps(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"Waiting for a steer."}]}`),
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"check connect","connector":"pg-orders-connector"}}
		]}`),
	}}
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains:  [][]HumanInput{nil, {{Kind: "steer", Text: "check the connector", Actor: events.Actor{Name: "arjun"}}}},
		waitErr: ErrInboxClosed,
	}
	r := newTestRunner(t, model, inbox, sink, rec, Options{MaxTurns: 5})
	_ = r.run(context.Background())

	last := ""
	for _, e := range sink.events {
		switch e.typ {
		case events.AgentStatus:
			var p events.AgentStatusPayload
			if json.Unmarshal(e.payload, &p) == nil {
				last = p.State
			}
		case events.AgentStepStart:
			if last != "working" {
				t.Fatalf("a step started while the last status was %q", last)
			}
			return
		}
	}
	t.Fatal("the steered turn wrote no step")
}

// deadSink refuses events on a done context, as the room stores do.
type deadSink struct{ fakeSink }

func (s *deadSink) Emit(ctx context.Context, typ events.Type, payload any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.fakeSink.Emit(ctx, typ, payload)
}

// blockTool blocks until its context ends.
type blockTool struct{}

func (blockTool) Name() string                { return "connect_status" }
func (blockTool) Description() string         { return "blocks" }
func (blockTool) InputSchema() map[string]any { return testSchema("connector") }
func (blockTool) Kind() tools.Kind            { return tools.KindRead }
func (blockTool) Run(ctx context.Context, _ json.RawMessage) (tools.Result, error) {
	<-ctx.Done()
	return tools.Result{}, ctx.Err()
}

// A read cut off by the caller's deadline (the first-look pass deadline) still
// stores its tool_result, so no step is left open.
func TestReadCutByDeadlineStillStoresItsResult(t *testing.T) {
	sink := &deadSink{}
	r := newTestRunner(t, &scriptedModel{}, &fakeInbox{}, sink, blockTool{}, Options{MaxTurns: 3})
	r.toolList = buildToolList(r.deps.Tools, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	r.runReadTool(ctx, toolUse{id: "tu1", name: "connect_status", input: json.RawMessage(`{"why":"check","connector":"c"}`)})
	calls, results := len(sink.ofType(events.AgentToolCall)), len(sink.ofType(events.AgentToolResult))
	if calls != 1 || results != 1 {
		t.Fatalf("tool_call events %d, tool_result events %d: want 1 and 1", calls, results)
	}
}
