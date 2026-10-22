package events_test

import (
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// X6c TC3 and the lag rule: one table for events.PerformanceOutcomeFor.
func TestPerformanceOutcomeFor(t *testing.T) {
	tests := []struct {
		name          string
		before, after int64
		lb, la        *int64
		want          string
	}{
		{"+15 % improved", 1000, 1150, nil, nil, events.PerformanceImproved},
		{"+10 % exactly improved", 1000, 1100, nil, nil, events.PerformanceImproved},
		{"+2 % no change", 1000, 1020, nil, nil, events.PerformanceNoChange},
		{"-2 % no change", 1000, 980, nil, nil, events.PerformanceNoChange},
		{"-10 % exactly worse", 1000, 900, nil, nil, events.PerformanceWorse},
		{"-20 % worse", 1000, 800, nil, nil, events.PerformanceWorse},
		{"from zero improved", 0, 5, nil, nil, events.PerformanceImproved},
		{"zero both no change", 0, 0, nil, nil, events.PerformanceNoChange},
		{"to zero worse", 600, 0, nil, nil, events.PerformanceWorse},
		{"lag stops growing improved", 1000, 1010, i64(300), i64(0), events.PerformanceImproved},
		{"lag shrinks improved", 1000, 1000, i64(300), i64(-50), events.PerformanceImproved},
		{"lag slope -10 % improved", 1000, 1000, i64(300), i64(270), events.PerformanceImproved},
		{"lag slope -5 % no change", 1000, 1000, i64(300), i64(285), events.PerformanceNoChange},
		{"lag starts growing worse", 1000, 1000, i64(0), i64(40), events.PerformanceWorse},
		{"lag slope +10 % worse", 1000, 1000, i64(300), i64(330), events.PerformanceWorse},
		{"lag shrinking faster no change", 1000, 1000, i64(-100), i64(-300), events.PerformanceNoChange},
		{"rate up wins over lag up", 1000, 1200, i64(100), i64(500), events.PerformanceImproved},
		{"rate down wins over lag down", 1000, 700, i64(100), i64(-10), events.PerformanceWorse},
		{"rate flat, lag down improved", 1000, 950, i64(100), i64(-10), events.PerformanceImproved},
		{"one slope missing ignores lag", 1000, 1000, i64(300), nil, events.PerformanceNoChange},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := events.PerformanceOutcomeFor(tt.before, tt.after, tt.lb, tt.la); got != tt.want {
				t.Fatalf("PerformanceOutcomeFor(%d, %d) = %q, want %q", tt.before, tt.after, got, tt.want)
			}
		})
	}
	if events.PerformanceUnverifiable != events.RecoveryUnverifiable {
		t.Fatal("unverifiable must be one value for both classes")
	}
}

// E1: every X6 field round-trips under the strict decoder, zero values that
// matter are kept, and payloads without them marshal as before.
func TestX6RoundTrip(t *testing.T) {
	perf := events.AgentVerificationPayload{ProposalID: "p_1", Checks: []events.VerificationCheck{}, Recovered: true,
		Verdict: "consume rate 7 200/min to 8 400/min", Outcome: events.PerformanceImproved,
		Before: i64(7200), After: i64(8400), Unit: events.PerformanceUnitRecordsPerMinute,
		LagSlopeBefore: i64(1500), LagSlopeAfter: i64(0)}
	got, raw := strictRoundTrip(t, perf)
	if !reflect.DeepEqual(got, perf) {
		t.Fatalf("round trip changed it:\n got %+v\nwant %+v", got, perf)
	}
	for _, s := range []string{`"outcome":"improved"`, `"unit":"records_per_min"`, `"lag_slope_before":1500`, `"lag_slope_after":0`} {
		if !strings.Contains(raw, s) {
			t.Errorf("%s lacks %s", raw, s)
		}
	}
	rec := events.AgentVerificationPayload{ProposalID: "p_2", Checks: []events.VerificationCheck{}, Verdict: "v",
		Outcome: events.RecoveryRecovered, Before: i64(3), After: i64(0), Unit: events.RecoveryUnitRows}
	if _, raw := strictRoundTrip(t, rec); strings.Contains(raw, "lag_slope") {
		t.Fatalf("a recovery verification carries X6 fields: %s", raw)
	}

	pv := events.PreviewResult{Target: "connector orders-sink", Current: "tasks.max 1", Expected: "tasks.max 3", Reversible: true,
		Tuning: []events.TuningChange{{Key: "tasks.max", Current: "1", Proposed: "3"}, {Key: "consumer.override.max.poll.records", Current: "", Proposed: "1000"}}}
	gotPV, raw := strictRoundTrip(t, pv)
	if !reflect.DeepEqual(gotPV, pv) {
		t.Fatalf("preview round trip: %+v", gotPV)
	}
	if !strings.Contains(raw, `"current":""`) {
		t.Fatalf("an unset current value is dropped: %s", raw)
	}
	if _, raw := strictRoundTrip(t, events.PreviewResult{Target: "t"}); strings.Contains(raw, "tuning") {
		t.Fatalf("a fix preview carries tuning: %s", raw)
	}

	pc := events.ProposalCreatedPayload{ProposalID: "p_3", Title: "raise tasks.max", Steps: []events.ProposalStep{},
		RequiredLayers: []string{"kafka-connect"}, Class: events.ActionClassPerformance}
	if got, raw := strictRoundTrip(t, pc); got.Class != "performance" || !strings.Contains(raw, `"class":"performance"`) {
		t.Fatalf("class: %s", raw)
	}
}
