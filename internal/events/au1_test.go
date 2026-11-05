package events_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"piperoom/internal/events"
)

// AU1-0 TC1: autonomy.granted and autonomy.revoked round-trip, every revoke
// reason, with by only for a human revoke.
func TestAutonomyEventsRoundTrip(t *testing.T) {
	granted := []events.AutonomyGrantedPayload{
		{Action: "connect_resume_connector", Streak: 10, Window: "30d", Evidence: []string{"p_1", "p_2", "p_3"}},
		{Action: "connect_restart_task", Streak: 3, Window: "1d", Evidence: []string{}},
	}
	for _, v := range granted {
		if got, raw := strictRoundTrip(t, v); !reflect.DeepEqual(got, v) {
			t.Errorf("granted round trip lost data: %s", raw)
		}
	}
	priya := &events.Actor{Kind: events.ActorHuman, ID: "priya", Name: "Priya", Verified: true}
	for _, reason := range []string{events.AutonomyRevokedVerifyFailed, events.AutonomyRevokedHuman, events.AutonomyRevokedConfigChanged, events.AutonomyRevokedExpired} {
		v := events.AutonomyRevokedPayload{Action: "connect_resume_connector", Reason: reason}
		if reason == events.AutonomyRevokedHuman {
			v.By = priya
		}
		if got, raw := strictRoundTrip(t, v); !reflect.DeepEqual(got, v) {
			t.Errorf("revoked %s round trip lost data: %s", reason, raw)
		}
	}
}

func TestAutonomyWireNames(t *testing.T) {
	if events.AutonomyGranted != "autonomy.granted" || events.AutonomyRevoked != "autonomy.revoked" || events.PolicyEarned != "earned_autonomy" {
		t.Fatalf("wire names changed: %q %q %q", events.AutonomyGranted, events.AutonomyRevoked, events.PolicyEarned)
	}
	if _, raw := strictRoundTrip(t, events.AutonomyRevokedPayload{Action: "a", Reason: events.AutonomyRevokedExpired}); raw != `{"action":"a","reason":"expired"}` {
		t.Errorf("system revoke = %s; want no by", raw)
	}
}

// AU1-0 TC3, contract half: payloads of a log written before AU1 decode and
// marshal back to the same bytes. A5's snapshot golden test (AU1b) covers the
// folded snapshot.
func TestPayloadsBeforeAU1MarshalUnchanged(t *testing.T) {
	tests := []struct {
		typ events.Type
		raw string
		dec func(events.Event) (any, error)
	}{
		{events.ApprovalGiven, `{"proposal_id":"p_1","layer":"kafka-connect","grant":"policy","policy":"auto_approve"}`,
			func(e events.Event) (any, error) { return events.Decode[events.ApprovalGivenPayload](e) }},
		{events.ProposalScheduled, `{"proposal_id":"p_1","run_at":"2026-10-22T10:00:00Z","policy":"auto_approve","cancel_until":"2026-10-22T10:00:00Z"}`,
			func(e events.Event) (any, error) { return events.Decode[events.ProposalScheduledPayload](e) }},
	}
	for _, tt := range tests {
		v, err := tt.dec(events.Event{Type: tt.typ, Payload: json.RawMessage(tt.raw)})
		if err != nil {
			t.Fatalf("%s: %v", tt.typ, err)
		}
		got, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tt.raw {
			t.Errorf("%s re-marshalled\n got %s\nwant %s", tt.typ, got, tt.raw)
		}
	}
}
