package agent

import (
	"context"
	"encoding/json"
	"testing"

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
