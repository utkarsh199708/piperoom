package room_test

import (
	"encoding/json"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

func TestApplySteerDeclined(t *testing.T) {
	arjun := human("arjun", "Arjun")
	priya := human("priya", "Priya")
	base := room.NewSnapshot()
	base = room.Apply(base, ev(events.SteerSuggested, 1, priya, events.SteerSuggestedPayload{SuggestionID: "g_1", Text: "a"}))
	base = room.Apply(base, ev(events.SteerSuggested, 2, priya, events.SteerSuggestedPayload{SuggestionID: "g_2", Text: "b"}))

	cases := []struct {
		name    string
		payload any
		want    []string
	}{
		{"declines one", events.SteerDeclinedPayload{SuggestionID: "g_1", Reason: "no"}, []string{"g_2"}},
		{"declines the other, no reason", events.SteerDeclinedPayload{SuggestionID: "g_2"}, []string{"g_1"}},
		{"unknown id leaves pending", events.SteerDeclinedPayload{SuggestionID: "g_9"}, []string{"g_1", "g_2"}},
		{"empty id leaves pending", events.SteerDeclinedPayload{}, []string{"g_1", "g_2"}},
		{"undecodable payload leaves pending", json.RawMessage(`"not an object"`), []string{"g_1", "g_2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := room.Apply(base.Clone(), ev(events.SteerDeclined, 3, arjun, tc.payload))
			var got []string
			for _, sg := range s.PendingSuggestions {
				got = append(got, sg.SuggestionID)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("pending = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("pending = %v, want %v", got, tc.want)
				}
			}
			if s.LastSeq != 3 {
				t.Fatalf("last seq = %d, want 3", s.LastSeq)
			}
			// The base snapshot is not changed by Apply on its clone.
			if len(base.PendingSuggestions) != 2 {
				t.Fatalf("base mutated: %+v", base.PendingSuggestions)
			}
		})
	}
}
