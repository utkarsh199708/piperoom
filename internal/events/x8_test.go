package events_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
)

// TC2: proposal.staged survives a JSON round trip, field for field, through
// events.Decode and through a strict decoder; proposal.stage_started too.
func TestX8ProposalStagedRoundTrip(t *testing.T) {
	at := time.Date(2026, 10, 22, 9, 30, 0, 0, time.UTC)
	in := events.ProposalStagedPayload{
		ProposalID: "p_01J",
		ParamsHash: "ab12",
		At:         at,
		Steps: []events.StepStage{
			{StepIndex: 0, Result: &events.StageResult{
				Target: "connector orders-src", Applied: true, Healthy: true,
				Detail: "RUNNING, 3 of 3 tasks RUNNING for 60 s", DurationMS: 61250,
			}},
			{StepIndex: 2, Result: &events.StageResult{
				Target: "subject orders.public.orders-value",
				DryRun: &events.DryRunResult{OK: false, Source: "registry compatibility check", Text: "READER_FIELD_MISSING_DEFAULT_VALUE"},
			}},
			{StepIndex: 3, Error: "staging connect: dial tcp: i/o timeout"},
		},
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := events.Decode[events.ProposalStagedPayload](events.Event{Type: events.ProposalStaged, Payload: raw})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip:\n got %+v\nwant %+v", out, in)
	}
	var strict events.ProposalStagedPayload
	if err := decodeStrict(raw, &strict); err != nil || !reflect.DeepEqual(strict, in) {
		t.Fatalf("strict round trip = %+v, %v", strict, err)
	}
	// An unknown key (a writer that invented a field) is refused by a strict decoder.
	bad := strings.Replace(string(raw), `"applied":true`, `"applied":true,"config":{"password":"x"}`, 1)
	var junk events.ProposalStagedPayload
	if err := decodeStrict([]byte(bad), &junk); err == nil {
		t.Fatal("strict decoder accepted an unknown field in a stage result")
	}

	started := events.ProposalStageStartedPayload{ProposalID: "p_01J", ParamsHash: "ab12", Steps: []int{0, 2, 3}, ExpiresAt: at.Add(7 * time.Minute)}
	raw, _ = json.Marshal(started)
	if want := `{"proposal_id":"p_01J","params_hash":"ab12","steps":[0,2,3],"expires_at":"2026-10-22T09:37:00Z"}`; string(raw) != want {
		t.Fatalf("stage_started = %s, want %s", raw, want)
	}
	var got events.ProposalStageStartedPayload
	if err := decodeStrict(raw, &got); err != nil || !reflect.DeepEqual(got, started) {
		t.Fatalf("stage_started round trip = %+v, %v", got, err)
	}

	if events.ProposalStageStarted != "proposal.stage_started" || events.ProposalStaged != "proposal.staged" {
		t.Fatalf("type names = %q, %q", events.ProposalStageStarted, events.ProposalStaged)
	}
}

// E1: zero values are kept on the wire (false is evidence), and every
// optional field is omitted when empty.
func TestX8StageResultZeroValues(t *testing.T) {
	raw, _ := json.Marshal(events.StageResult{})
	if want := `{"applied":false,"healthy":false,"duration_ms":0}`; string(raw) != want {
		t.Fatalf("zero StageResult = %s, want %s", raw, want)
	}
	raw, _ = json.Marshal(events.StepStage{StepIndex: 0})
	if want := `{"step_index":0}`; string(raw) != want {
		t.Fatalf("empty StepStage = %s, want %s", raw, want)
	}
}

// The one outcome rule: healthy needs applied; a staging pre-check that ran
// and said no is a failure; a skipped pre-check or nothing tried is
// not_applied.
func TestX8StageOutcome(t *testing.T) {
	for _, tc := range []struct {
		name string
		r    events.StageResult
		want string
	}{
		{"applied and healthy", events.StageResult{Applied: true, Healthy: true}, events.StageHealthy},
		{"applied, task failed", events.StageResult{Applied: true, Detail: "task 0 FAILED"}, events.StageFailed},
		{"healthy without applied is not healthy", events.StageResult{Healthy: true}, events.StageNotApplied},
		{"no staging", events.StageResult{Detail: events.StageNoStaging}, events.StageNotApplied},
		{"pre-check refused", events.StageResult{DryRun: &events.DryRunResult{OK: false, Source: "registry compatibility check"}}, events.StageFailed},
		{"pre-check skipped", events.StageResult{DryRun: &events.DryRunResult{Skipped: "no compatibility endpoint"}}, events.StageNotApplied},
		{"pre-check ok, not applied", events.StageResult{DryRun: &events.DryRunResult{OK: true}}, events.StageNotApplied},
	} {
		if got := tc.r.Outcome(); got != tc.want {
			t.Errorf("%s: Outcome = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// Healthy and StepHealthy: what require_staging and the card's verdict use.
func TestX8StagedHealthy(t *testing.T) {
	ok := &events.StageResult{Applied: true, Healthy: true}
	bad := &events.StageResult{Applied: true}
	for _, tc := range []struct {
		name    string
		steps   []events.StepStage
		healthy bool
		step0   bool
		step1   bool
	}{
		{"none staged", nil, false, false, false},
		{"all healthy", []events.StepStage{{StepIndex: 0, Result: ok}, {StepIndex: 1, Result: ok}}, true, true, true},
		{"one failed", []events.StepStage{{StepIndex: 0, Result: ok}, {StepIndex: 1, Result: bad}}, false, true, false},
		{"one error", []events.StepStage{{StepIndex: 0, Result: ok}, {StepIndex: 1, Error: "timeout"}}, false, true, false},
		{"error beside a result never counts", []events.StepStage{{StepIndex: 0, Result: ok, Error: "x"}}, false, false, false},
		{"step 1 not staged", []events.StepStage{{StepIndex: 0, Result: ok}}, true, true, false},
	} {
		p := events.ProposalStagedPayload{Steps: tc.steps}
		if got := p.Healthy(); got != tc.healthy {
			t.Errorf("%s: Healthy = %v, want %v", tc.name, got, tc.healthy)
		}
		if got := p.StepHealthy(0); got != tc.step0 {
			t.Errorf("%s: StepHealthy(0) = %v, want %v", tc.name, got, tc.step0)
		}
		if got := p.StepHealthy(1); got != tc.step1 {
			t.Errorf("%s: StepHealthy(1) = %v, want %v", tc.name, got, tc.step1)
		}
	}
}
