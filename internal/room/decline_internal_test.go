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
