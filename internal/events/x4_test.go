package events_test

import (
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// TC1: every scope and the team page survive a JSON round trip; an
// unrecognised scope decodes as unknown.
func TestX4HypothesisScopeRoundTrip(t *testing.T) {
	for _, s := range []events.HypothesisScope{events.ScopePipeline, events.ScopeUpstreamApp, events.ScopeVendor} {
		in := events.AgentHypothesisPayload{Text: "t", BasedOnSteps: []string{"s_1"}, Confidence: "high", Scope: s, Team: "checkout", ChangeID: 812}
		raw, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), `"scope":"`+string(s)+`"`) {
			t.Fatalf("%s: marshal = %s", s, raw)
		}
		out, err := events.Decode[events.AgentHypothesisPayload](events.Event{Type: events.AgentHypothesis, Payload: raw})
		if err != nil {
			t.Fatal(err)
		}
		if out.Scope != s || out.Team != "checkout" || out.ChangeID != 812 || out.Text != "t" || len(out.BasedOnSteps) != 1 {
			t.Fatalf("%s: round trip = %+v", s, out)
		}
	}

	for _, body := range []string{
		`{"text":"t","scope":"application"}`,
		`{"text":"t","scope":"UPSTREAM_APP"}`,
		`{"text":"t","scope":""}`,
		`{"text":"t","scope":null}`,
		`{"text":"t","scope":"unknown"}`,
	} {
		var p events.AgentHypothesisPayload
		if err := json.Unmarshal([]byte(body), &p); err != nil {
			t.Fatalf("%s: %v", body, err)
		}
		if p.Scope != events.ScopeUnknown || p.Text != "t" {
			t.Fatalf("%s: scope = %q, want unknown", body, p.Scope)
		}
	}

	var p events.AgentHypothesisPayload
	if err := json.Unmarshal([]byte(`{"text":"t","scope":3}`), &p); err == nil {
		t.Fatal("a non-string scope decoded without error")
	}

	page := events.TeamPagedPayload{Team: "checkout", HypothesisSeq: 17, ChangeID: 812}
	raw, _ := json.Marshal(page)
	if string(raw) != `{"team":"checkout","hypothesis_seq":17,"change_id":812}` {
		t.Fatalf("team page = %s", raw)
	}
	got, err := events.Decode[events.TeamPagedPayload](events.Event{Type: events.RoomTeamPaged, Payload: raw})
	if err != nil || got != page {
		t.Fatalf("team page round trip = %+v, %v", got, err)
	}
	if raw, _ := json.Marshal(events.TeamPagedPayload{Team: "x", HypothesisSeq: 1}); strings.Contains(string(raw), "change_id") {
		t.Fatalf("change_id 0 must be omitted: %s", raw)
	}
	if events.RoomTeamPaged != "room.team_paged" {
		t.Fatalf("RoomTeamPaged = %q", events.RoomTeamPaged)
	}
}

// TC2: a hypothesis written before X4 decodes as unknown, and one that has
// no scope (or an unknown one) marshals byte for byte as before X4, so
// replay and export hashes do not change.
func TestX4OldHypothesisReplay(t *testing.T) {
	old := `{"text":"connector task failed","based_on_steps":["s_3","s_4"],"confidence":"high","confirmed":false,"unbacked_terms":["order_id"]}`
	p, err := events.Decode[events.AgentHypothesisPayload](events.Event{Type: events.AgentHypothesis, Payload: json.RawMessage(old)})
	if err != nil {
		t.Fatal(err)
	}
	if p.Scope != events.ScopeUnknown || p.Team != "" || p.ChangeID != 0 {
		t.Fatalf("old hypothesis = %+v, want scope unknown, no team, no change", p)
	}
	again, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != old {
		t.Fatalf("re-marshal changed the bytes:\n got %s\nwant %s", again, old)
	}
	for _, s := range []events.HypothesisScope{"", events.ScopeUnknown, "bogus"} {
		raw, _ := json.Marshal(events.AgentHypothesisPayload{Text: "x", Scope: s})
		if strings.Contains(string(raw), "scope") {
			t.Fatalf("scope %q must be omitted: %s", s, raw)
		}
	}
	// A zero-value payload decodes with Scope unknown, not "".
	var z events.AgentHypothesisPayload
	if err := json.Unmarshal([]byte(`{}`), &z); err != nil || z.Scope != events.ScopeUnknown {
		t.Fatalf("empty object: scope %q, %v", z.Scope, err)
	}
}

func TestX4ScopeNormalize(t *testing.T) {
	tests := []struct {
		in, want events.HypothesisScope
	}{
		{"pipeline", events.ScopePipeline},
		{"upstream_app", events.ScopeUpstreamApp},
		{"vendor", events.ScopeVendor},
		{"unknown", events.ScopeUnknown},
		{"", events.ScopeUnknown},
		{"Vendor", events.ScopeUnknown},
		{" pipeline", events.ScopeUnknown},
	}
	for _, tt := range tests {
		if got := tt.in.Normalize(); got != tt.want {
			t.Errorf("Normalize(%q) = %q, want %q", tt.in, got, tt.want)
		}
		if got := tt.in.IsZero(); got != (tt.want == events.ScopeUnknown) {
			t.Errorf("IsZero(%q) = %v", tt.in, got)
		}
	}
}
