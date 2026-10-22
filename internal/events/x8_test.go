package events_test

import (
	"reflect"
	"testing"
	"time"

	"piperoom/internal/events"
)

// X8-0 TC2: proposal.staged round-trips, with a result, an error, and a
// zero result whose false and 0 are verdicts that stay on the wire.
func TestProposalStagedRoundTrip(t *testing.T) {
	at := time.Date(2026, 10, 22, 11, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		v    events.ProposalStagedPayload
	}{
		{"applied and healthy", events.ProposalStagedPayload{ProposalID: "p_1", ParamsHash: "9f2c", At: at, Steps: []events.StepStage{
			{StepIndex: 0, Result: &events.StageResult{Applied: true, Healthy: true, Detail: "task 0 RUNNING after 4 s", DurationMS: 4100}},
		}}},
		{"applied, unhealthy; second step not stageable", events.ProposalStagedPayload{ProposalID: "p_2", At: at, Steps: []events.StepStage{
			{StepIndex: 0, Result: &events.StageResult{Applied: true, Detail: "task 0 FAILED", DurationMS: 60000}},
			{StepIndex: 1, Error: "not stageable"},
		}}},
		{"not applied, zero duration", events.ProposalStagedPayload{ProposalID: "p_3", At: at, Steps: []events.StepStage{
			{StepIndex: 0, Result: &events.StageResult{}},
		}}},
	}
	for _, tt := range tests {
		got, raw := strictRoundTrip(t, tt.v)
		if !reflect.DeepEqual(got, tt.v) {
			t.Errorf("%s: round trip lost data:\n got %+v\nwant %+v\njson %s", tt.name, got, tt.v, raw)
		}
	}
}

func TestStageResultWireShape(t *testing.T) {
	_, raw := strictRoundTrip(t, events.StageResult{})
	if raw != `{"applied":false,"healthy":false,"detail":"","duration_ms":0}` {
		t.Errorf("StageResult{} = %s", raw)
	}
	if events.ProposalStaged != "proposal.staged" {
		t.Errorf("ProposalStaged = %q", events.ProposalStaged)
	}
}
