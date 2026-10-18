package events_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
)

func TestLayersOf(t *testing.T) {
	cdc := []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse"}
	tests := []struct {
		kind string
		want []string
	}{
		{"", cdc},
		{events.PipelineKindCDC, cdc},
		{events.PipelineKindBatch, []string{"orchestrator", "transform", "warehouse"}},
		{"stream", nil},
		{"CDC", nil}, // kinds are exact; owners.Load normalises before it asks
	}
	for _, tt := range tests {
		got := events.LayersOf(tt.kind)
		if !slices.Equal(got, tt.want) {
			t.Errorf("LayersOf(%q) = %v, want %v", tt.kind, got, tt.want)
		}
		for _, l := range got {
			if !slices.Contains(events.Layers, l) {
				t.Errorf("LayersOf(%q) has %q, which is not in events.Layers", tt.kind, l)
			}
		}
	}
}

// A caller may keep and change what LayersOf returns (rule 5: no shared
// mutable state), so each call must hand out a new slice.
func TestLayersOfReturnsAFreshSlice(t *testing.T) {
	a := events.LayersOf(events.PipelineKindCDC)
	a[0] = "changed"
	if b := events.LayersOf(events.PipelineKindCDC); b[0] != "postgres" {
		t.Fatalf("LayersOf shares its slice: second call starts with %q", b[0])
	}
	if events.Layers[0] != "postgres" {
		t.Fatalf("LayersOf aliases events.Layers: %v", events.Layers)
	}
}

// The P-wave fields on payloads that already exist are omitempty, so an event
// written before P0 marshals exactly as before (export hashes).
func TestPWaveFieldsAreOmittedWhenEmpty(t *testing.T) {
	tests := []struct {
		name   string
		v      any
		absent []string
	}{
		{"approval grant and policy", events.ApprovalGivenPayload{ProposalID: "p_1", Layer: "kafka"}, []string{"grant", "policy"}},
		{"control reason", events.ControlGrantedPayload{To: events.Actor{ID: "a"}}, []string{"reason"}},
		{"proposal origin and fallback layers", events.ProposalCreatedPayload{ProposalID: "p_1"}, []string{"origin", "fallback_layers"}},
	}
	for _, tt := range tests {
		raw, err := json.Marshal(tt.v)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		for _, k := range tt.absent {
			if strings.Contains(string(raw), `"`+k+`"`) {
				t.Errorf("%s: %s has %q when empty", tt.name, raw, k)
			}
		}
	}
}

func TestPWavePayloadShapes(t *testing.T) {
	raw, err := json.Marshal(events.ApprovalGivenPayload{ProposalID: "p_1", Layer: "kafka", Grant: events.GrantPolicy, Policy: events.PolicyAutoApprove})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"proposal_id":"p_1","layer":"kafka","grant":"policy","policy":"auto_approve"}`; string(raw) != want {
		t.Errorf("policy approval = %s, want %s", raw, want)
	}
	var sched events.ProposalScheduledPayload
	if err := json.Unmarshal([]byte(`{"proposal_id":"p_1","run_at":"2026-10-18T10:00:00Z","policy":"auto_approve","cancel_until":"2026-10-18T10:00:00Z"}`), &sched); err != nil {
		t.Fatal(err)
	}
	if sched.ProposalID != "p_1" || sched.Policy != events.PolicyAutoApprove || sched.RunAt.IsZero() || !sched.CancelUntil.Equal(sched.RunAt) {
		t.Errorf("proposal.scheduled decoded as %+v", sched)
	}
	raw, err = json.Marshal(events.ProposalCancelledPayload{ProposalID: "p_1", By: events.Actor{Kind: events.ActorHuman, ID: "priya", Name: "Priya"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"by":{"kind":"human","id":"priya","name":"Priya"}`) || !strings.Contains(string(raw), `"reason":""`) {
		t.Errorf("proposal.cancelled = %s", raw)
	}
}
