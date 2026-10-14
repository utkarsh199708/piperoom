package room

import (
	"encoding/json"
	"testing"

	"piperoom/internal/events"
)

// An action result whose outcome is unknown reaches the agent with OK nil, so
// it claims neither recovery nor failure; known outcomes keep their OK.
func TestActionResultUnknownOutcome(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload events.ActionExecutedPayload
		want    *bool
	}{
		{"ok", events.ActionExecutedPayload{ProposalID: "p", OK: true}, ptr(true)},
		{"failed", events.ActionExecutedPayload{ProposalID: "p", OK: false}, ptr(false)},
		{"unknown", events.ActionExecutedPayload{ProposalID: "p", OK: false, OutcomeUnknown: true}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(tc.payload)
			in := inputFor(events.Event{Seq: 7, Type: events.ActionExecuted, Actor: executorActor, Payload: raw})
			if in == nil || in.Kind != "action_result" {
				t.Fatalf("input %+v", in)
			}
			switch {
			case tc.want == nil && in.OK != nil:
				t.Fatalf("OK = %v, want nil (unknown)", *in.OK)
			case tc.want != nil && (in.OK == nil || *in.OK != *tc.want):
				t.Fatalf("OK = %v, want %v", in.OK, *tc.want)
			}
		})
	}
}

func ptr(b bool) *bool { return &b }

// trackPending decides "the agent's own event" by actor kind only: an agent
// event whose actor differs in any other field (stored before a field was
// added, or with another display name) still ends the pending input.
func TestTrackPendingMatchesAgentByKind(t *testing.T) {
	steer, _ := json.Marshal(events.SteerSentPayload{Text: "look at the connector"})
	msg, _ := json.Marshal(events.AgentMessagePayload{Text: "on it"})
	status, _ := json.Marshal(events.AgentStatusPayload{State: "working"})
	human := events.Actor{Kind: events.ActorHuman, ID: "u1", Name: "Ana"}
	for _, tc := range []struct {
		name  string
		agent events.Actor
	}{
		{"agent actor", events.AgentActor},
		{"agent kind, other name", events.Actor{Kind: events.ActorAgent, ID: "agent", Name: "Claude"}},
		{"agent kind, other id", events.Actor{Kind: events.ActorAgent, ID: "agent-v2", Name: "Agent"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &roomState{}
			trackPending(st, events.Event{Seq: 1, Type: events.SteerSent, Actor: human, Payload: steer})
			if len(st.pending) != 1 {
				t.Fatalf("pending after steer = %d, want 1", len(st.pending))
			}
			trackPending(st, events.Event{Seq: 2, Type: events.AgentStatus, Actor: tc.agent, Payload: status})
			if len(st.pending) != 1 {
				t.Fatalf("a status cleared the pending input")
			}
			trackPending(st, events.Event{Seq: 3, Type: events.AgentMessage, Actor: tc.agent, Payload: msg})
			if len(st.pending) != 0 {
				t.Fatalf("pending after the agent's message = %d, want 0", len(st.pending))
			}
		})
	}
}
