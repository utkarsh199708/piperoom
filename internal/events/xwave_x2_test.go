package events_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/events"
)

func i64(v int64) *int64 { return &v }

// strictRoundTrip marshals v, decodes it back into a new T with unknown fields
// rejected, and returns the JSON.
func strictRoundTrip[T any](t *testing.T, v T) (T, string) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var got T
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("strict decode of %s: %v", raw, err)
	}
	return got, string(raw)
}

// TC4, E1: agent.verification round-trips with and without the X2 counts; a
// recovered After of 0 survives, and a payload without them marshals as before.
func TestAgentVerificationRecoveryRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		p       events.AgentVerificationPayload
		has     []string
		hasNot  []string
		outcome string
	}{
		{
			name:   "fix verification, no counts",
			p:      events.AgentVerificationPayload{ProposalID: "p_1", Checks: []events.VerificationCheck{}, Recovered: true, Verdict: "recovered"},
			hasNot: []string{"outcome", "before", "after", "unit"},
		},
		{
			name: "recovered, after zero kept",
			p: events.AgentVerificationPayload{ProposalID: "p_2", Checks: []events.VerificationCheck{}, Recovered: true, Verdict: "412 missing, now 0",
				Outcome: events.RecoveryRecovered, Before: i64(412), After: i64(0), Unit: events.RecoveryUnitRows},
			has: []string{`"before":412`, `"after":0`, `"outcome":"recovered"`, `"unit":"rows"`},
		},
		{
			name: "partial",
			p: events.AgentVerificationPayload{ProposalID: "p_3", Checks: []events.VerificationCheck{}, Verdict: "12 of 412 still missing",
				Outcome: events.RecoveryPartial, Before: i64(412), After: i64(12), Unit: events.RecoveryUnitRecords},
			has: []string{`"after":12`, `"outcome":"partial"`},
		},
		{
			name: "unverifiable has no counts",
			p: events.AgentVerificationPayload{ProposalID: "p_4", Checks: []events.VerificationCheck{}, Verdict: "the warehouse cannot count",
				Outcome: events.RecoveryUnverifiable},
			has:    []string{`"outcome":"unverifiable"`},
			hasNot: []string{"before", "after"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, raw := strictRoundTrip(t, tt.p)
			if !reflect.DeepEqual(got, tt.p) {
				t.Fatalf("round trip changed it:\n got %+v\nwant %+v", got, tt.p)
			}
			for _, s := range tt.has {
				if !strings.Contains(raw, s) {
					t.Errorf("%s lacks %s", raw, s)
				}
			}
			for _, k := range tt.hasNot {
				if strings.Contains(raw, `"`+k+`"`) {
					t.Errorf("%s has %q", raw, k)
				}
			}
		})
	}
	// E1: the strict decoder rejects a field the contract does not have.
	dec := json.NewDecoder(strings.NewReader(`{"proposal_id":"p","checks":[],"recovered":false,"verdict":"","before":1,"remaining":3}`))
	dec.DisallowUnknownFields()
	var p events.AgentVerificationPayload
	if err := dec.Decode(&p); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestRecoveryCheckAndPreviewCountRoundTrip(t *testing.T) {
	chk := events.AgentRecoveryCheckPayload{ProposalID: "p_1", StepIndex: 0, Poll: 2, MaxPolls: 14, Before: 412, After: 0, Unit: events.RecoveryUnitRows}
	got, raw := strictRoundTrip(t, chk)
	if got != chk {
		t.Fatalf("got %+v", got)
	}
	for _, k := range []string{`"step_index":0`, `"poll":2`, `"max_polls":14`, `"after":0`} {
		if !strings.Contains(raw, k) {
			t.Errorf("%s lacks %s (zero values must be kept)", raw, k)
		}
	}

	fix := events.PreviewResult{Target: "connector orders", Current: "FAILED", Expected: "RUNNING"}
	if _, raw := strictRoundTrip(t, fix); strings.Contains(raw, `"count"`) || strings.Contains(raw, `"unit"`) {
		t.Fatalf("a fix preview carries recovery fields: %s", raw)
	}
	rec := events.PreviewResult{Target: "public.orders", Current: "412 rows match", Expected: "will re-emit 412 rows", Count: i64(412), Unit: events.RecoveryUnitRows}
	if got, _ := strictRoundTrip(t, rec); *got.Count != 412 || got.Unit != "rows" {
		t.Fatalf("got %+v", got)
	}
	zero := events.PreviewResult{Target: "t", Count: i64(0), Unit: events.RecoveryUnitRows}
	if _, raw := strictRoundTrip(t, zero); !strings.Contains(raw, `"count":0`) {
		t.Fatalf("a zero count is dropped: %s", raw)
	}
}

func TestProposalClass(t *testing.T) {
	if _, raw := strictRoundTrip(t, events.ProposalCreatedPayload{ProposalID: "p_1"}); strings.Contains(raw, `"class"`) {
		t.Fatalf("a fix proposal carries class: %s", raw)
	}
	p := events.ProposalCreatedPayload{ProposalID: "p_2", Class: events.ActionClassRecovery}
	if got, raw := strictRoundTrip(t, p); got.Class != "recovery" || !strings.Contains(raw, `"class":"recovery"`) {
		t.Fatalf("got %s", raw)
	}
	if events.AgentRecoveryCheck != "agent.recovery_check" || !strings.HasPrefix(string(events.AgentRecoveryCheck), "agent.") {
		t.Fatalf("type %q", events.AgentRecoveryCheck)
	}
}

func TestRecoveryOutcomeFor(t *testing.T) {
	tests := []struct {
		before, after int64
		want          string
	}{
		{412, 0, events.RecoveryRecovered},
		{0, 0, events.RecoveryRecovered},
		{412, 12, events.RecoveryPartial},
		{412, 411, events.RecoveryPartial},
		{412, 412, events.RecoveryNotRecovered},
		{412, 500, events.RecoveryNotRecovered},
		{0, 3, events.RecoveryNotRecovered},
	}
	for _, tt := range tests {
		if got := events.RecoveryOutcomeFor(tt.before, tt.after); got != tt.want {
			t.Errorf("RecoveryOutcomeFor(%d, %d) = %q, want %q", tt.before, tt.after, got, tt.want)
		}
	}
}
