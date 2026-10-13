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
