package httpapi

import (
	"encoding/json"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/room"
)

var infraT0 = time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)

// series returns one sample every step from start for d, all of value v.
func series(start time.Time, d, step time.Duration, v float64) []clients.SamplePoint {
	var out []clients.SamplePoint
	for at := start; at.Before(start.Add(d)); at = at.Add(step) {
		out = append(out, clients.SamplePoint{At: at, Value: v})
	}
	return out
}

func pt(h float64, v float64) clients.SamplePoint {
	return clients.SamplePoint{At: infraT0.Add(time.Duration(h * float64(time.Hour))), Value: v}
}

func TestInfraGBDays(t *testing.T) {
	const gb = 1e9
	// TC1: 10 GB for 24 h then 20 GB for 24 h is 30 GB-days. The newest sample
	// (at 48 h) holds nothing.
	tc1 := func(step time.Duration) []clients.SamplePoint {
		wal := series(infraT0, 24*time.Hour, step, 10*gb)
		wal = append(wal, series(infraT0.Add(24*time.Hour), 24*time.Hour, step, 20*gb)...)
		return append(wal, pt(48, 20*gb))
	}
	cases := []struct {
		name string
		wal  []clients.SamplePoint
		want float64
	}{
		{"TC1 every 15 min", tc1(15 * time.Minute), 30},
		{"TC1 hourly", tc1(time.Hour), 30},
		{"one sample holds nothing", []clients.SamplePoint{pt(0, 10*gb)}, 0},
		// A 5 h gap counts one hour: 10 GB × 1/24 d.
		{"gap counts at most an hour", []clients.SamplePoint{pt(0, 10*gb), pt(5, 10*gb)}, 0.417},
		// Out of order: the backwards pair counts nothing, never negative.
		{"out of order", []clients.SamplePoint{pt(1, 24*gb), pt(0, 24*gb), pt(1, 24*gb)}, 1},
		{"negative value counts nothing", []clients.SamplePoint{pt(0, -24*gb), pt(1, 0)}, 0},
		// GB, not GiB: 1 GiB for 24 h.
		{"decimal GB", append(series(infraT0, 24*time.Hour, time.Hour, 1<<30), pt(24, 0)), 1.074},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := infraFromSamples(c.wal, nil, nil)
			if got == nil || got.WALRetainedGBDays == nil {
				t.Fatalf("infra = %+v", got)
			}
			if *got.WALRetainedGBDays != c.want {
				t.Fatalf("GB-days = %v, want %v", *got.WALRetainedGBDays, c.want)
			}
			if got.Samples != len(c.wal) || got.DLQRecords != nil || got.OldestUnreadAgeS != nil {
				t.Fatalf("infra = %+v", got)
			}
		})
	}
}

func TestInfraNewestAndWindow(t *testing.T) {
	// TC2: no sample of any metric is nil (null), never zeros.
	if got := infraFromSamples(nil, []clients.SamplePoint{}, nil); got != nil {
		t.Fatalf("no samples: %+v", got)
	}
	dlq := []clients.SamplePoint{pt(1, 5), pt(3, 1200.4)}
	age := []clients.SamplePoint{pt(0.5, 60), pt(2, 5399.6)}
	got := infraFromSamples(nil, dlq, age)
	if got == nil || got.WALRetainedGBDays != nil {
		t.Fatalf("no WAL samples must leave GB-days null: %+v", got)
	}
	if got.DLQRecords == nil || *got.DLQRecords != 1200 || got.OldestUnreadAgeS == nil || *got.OldestUnreadAgeS != 5400 {
		t.Fatalf("newest values: %+v", got)
	}
	if got.Samples != 4 || !got.From.Equal(infraT0.Add(30*time.Minute)) || !got.To.Equal(infraT0.Add(3*time.Hour)) {
		t.Fatalf("window: %+v", got)
	}
	// A negative gauge (never written, but not trusted) shows 0, not a negative count.
	if got := infraFromSamples(nil, []clients.SamplePoint{pt(0, -3)}, nil); *got.DLQRecords != 0 {
		t.Fatalf("negative dlq: %d", *got.DLQRecords)
	}
}

// TC3: the F11 fields keep their names, order and values; infra is added last
// and is null without history.
func TestAgentStatsGoldenJSON(t *testing.T) {
	st := room.AgentStats{
		PipelineID: "orders", WindowDays: 90, GeneratedAt: infraT0,
		Rooms: 23, Hypothesised: 21, Labelled: 17, Graded: 17, LayerCorrect: 15,
		RootCause:                    room.ShadowRootCause{Yes: 11, Partial: 4, No: 2},
		ByConfidence:                 []room.AgentStatsBucket{{Confidence: "high", Runs: 12, Correct: 11, Accuracy: 0.917}},
		MedianTimeToFirstHypothesisS: 48, MedianHumanMTTRS: 720, MedianCostUSD: 0.42,
		CostAvailable: true, EnoughData: true,
		Shadow: room.AgentStatsShadow{Rooms: 6, Graded: 5, LayerCorrect: 4},
	}
	const f11 = `{"pipeline_id":"orders","window_days":90,"generated_at":"2026-10-20T00:00:00Z",` +
		`"rooms":23,"hypothesised":21,"labelled":17,"graded":17,"layer_correct":15,` +
		`"root_cause":{"yes":11,"partial":4,"no":2},` +
		`"by_confidence":[{"confidence":"high","runs":12,"correct":11,"accuracy":0.917}],` +
		`"median_time_to_first_hypothesis_s":48,"median_human_mttr_s":720,"median_cost_usd":0.42,` +
		`"cost_available":true,"enough_data":true,"shadow":{"rooms":6,"graded":5,"layer_correct":4}`
	plain, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != f11+"}" {
		t.Fatalf("F11 body changed:\n%s", plain)
	}
	b, err := json.Marshal(agentStatsBody{AgentStats: st})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != f11+`,"infra":null}` {
		t.Fatalf("no history:\n%s", b)
	}
	b, err = json.Marshal(agentStatsBody{AgentStats: st, Infra: infraFromSamples(
		[]clients.SamplePoint{pt(0, 24e9), pt(1, 24e9)}, nil, []clients.SamplePoint{pt(1, 90)})})
	if err != nil {
		t.Fatal(err)
	}
	want := f11 + `,"infra":{"from":"2026-10-20T00:00:00Z","to":"2026-10-20T01:00:00Z","samples":3,` +
		`"wal_retained_gb_days":1,"dlq_records":null,"oldest_unread_age_s":90}}`
	if string(b) != want {
		t.Fatalf("with history:\n%s\nwant\n%s", b, want)
	}
}
