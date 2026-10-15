package room

import (
	"encoding/json"
	"testing"

	"piperoom/internal/events"
)

// TestInputForSteerDeclined pins "the agent never sees a declined suggestion":
// neither the suggestion nor its decline becomes agent input.
func TestInputForSteerDeclined(t *testing.T) {
	for _, tc := range []struct {
		typ     events.Type
		payload any
	}{
		{events.SteerDeclined, events.SteerDeclinedPayload{SuggestionID: "g_1", Reason: "no"}},
		{events.SteerSuggested, events.SteerSuggestedPayload{SuggestionID: "g_1", Text: "restart it"}},
	} {
		raw, err := json.Marshal(tc.payload)
		if err != nil {
			t.Fatal(err)
		}
		e := events.Event{Seq: 1, Type: tc.typ, Actor: events.Actor{Kind: events.ActorHuman, ID: "arjun"}, Payload: raw}
		if in := inputFor(e); in != nil {
			t.Fatalf("%s must not reach the agent: %+v", tc.typ, in)
		}
	}
}

// F5b: the text the agent reads carries no ids and counts steps from 1.
func TestInputForActionResultIsOneBasedAndIdFree(t *testing.T) {
	raw, _ := json.Marshal(events.ActionExecutedPayload{ProposalID: "p_01M4ZZZZZZZZZZZZZZZZZZZZZZ", StepIndex: 0, OK: true, Output: "task restarted"})
	in := inputFor(events.Event{Seq: 1, Type: events.ActionExecuted, Actor: executorActor, Payload: raw})
	if in == nil || in.Text != "step 1: task restarted" {
		t.Fatalf("input = %+v, want text %q", in, "step 1: task restarted")
	}
	if in.ProposalID == "" || in.StepIndex == nil || *in.StepIndex != 0 {
		t.Fatalf("structured fields changed: %+v", in)
	}
}

func TestInputForApprovalResultIsIdFree(t *testing.T) {
	for _, tc := range []struct {
		typ     events.Type
		payload any
		want    string
	}{
		{events.ApprovalGiven, events.ApprovalGivenPayload{ProposalID: "p_1", Layer: "kafka-connect"}, `layer "kafka-connect" approved your proposal`},
		{events.ApprovalRejected, events.ApprovalRejectedPayload{ProposalID: "p_1", Layer: "kafka-connect", Reason: "too risky"}, `layer "kafka-connect" rejected your proposal: too risky`},
	} {
		raw, _ := json.Marshal(tc.payload)
		in := inputFor(events.Event{Seq: 1, Type: tc.typ, Actor: events.Actor{Kind: events.ActorHuman, ID: "a"}, Payload: raw})
		if in == nil || in.Text != tc.want || in.ProposalID != "p_1" {
			t.Fatalf("%s input = %+v, want text %q and proposal id kept", tc.typ, in, tc.want)
		}
	}
}
