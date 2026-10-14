package evals

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestPercentileOnFixedInputs(t *testing.T) {
	xs := []float64{50, 15, 40, 20, 35} // unsorted on purpose
	for _, c := range []struct{ p, want float64 }{
		{0, 15}, {50, 35}, {90, 46}, {99, 49.6}, {100, 50}, {25, 20}, {-5, 15}, {150, 50},
	} {
		if got := Percentile(xs, c.p); !near(got, c.want) {
			t.Errorf("p%v = %v, want %v", c.p, got, c.want)
		}
	}
	if !reflect.DeepEqual(xs, []float64{50, 15, 40, 20, 35}) {
		t.Fatal("Percentile reordered its input")
	}
	ten := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if got := Percentile(ten, 90); !near(got, 9.1) {
		t.Errorf("p90 of 1..10 = %v, want 9.1", got)
	}
	if got := Percentile([]float64{7}, 99); got != 7 {
		t.Errorf("one value: %v", got)
	}
	if got := Percentile(nil, 50); got != -1 {
		t.Errorf("no data = %v, want -1", got)
	}
	if d := distOf(nil); d != (Dist{-1, -1, -1}) {
		t.Errorf("empty dist = %+v", d)
	}
	if m := meanP90(nil); m != (MeanP90{-1, -1}) {
		t.Errorf("empty mean = %+v", m)
	}
	if m := meanP90([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); !near(m.Mean, 5.5) || !near(m.P90, 9.1) {
		t.Errorf("mean/p90 = %+v", m)
	}
}

func usageEvent(t *testing.T, p events.AgentUsagePayload) RecordedEvent {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return RecordedEvent{Type: events.AgentUsage, Payload: b}
}

// fixedTranscript has a triage turn, a main turn with a compaction pass, a
// hand-off, a malformed usage event and an unrelated event.
func fixedTranscript(t *testing.T, mainModel string) Transcript {
	return Transcript{Events: []RecordedEvent{
		usageEvent(t, events.AgentUsagePayload{Model: "tri", Role: "triage", InputTokens: 10, OutputTokens: 5, LatencyMS: 100}),
		{Type: events.AgentMessage, Payload: json.RawMessage(`{"text":"hi"}`)},
		usageEvent(t, events.AgentUsagePayload{Model: mainModel, Role: "main", CacheReadInputTokens: 90, OutputTokens: 10, LatencyMS: 300}),
		usageEvent(t, events.AgentUsagePayload{Model: mainModel, Role: "compaction", InputTokens: 50}),
		usageEvent(t, events.AgentUsagePayload{Model: mainModel, Role: "handoff", InputTokens: 7}),
		{Type: events.AgentUsage, Payload: json.RawMessage(`not json`)},
	}}
}

var testPrices = agent.Prices{
	"tri":  {Input: 1, Output: 1, CacheWrite: 1, CacheRead: 1},
	"main": {Input: 10, Output: 100, CacheWrite: 10, CacheRead: 1},
}

func TestRunUsageOf(t *testing.T) {
	u := RunUsageOf(RunResult{Scenario: "s", Run: 2, DurationMS: 5000}, fixedTranscript(t, "main"), testPrices)
	if u.Scenario != "s" || u.Run != 2 || u.DurationMS != 5000 || u.Turns != 2 || u.TriageTurns != 1 || u.MainTurns != 1 {
		t.Fatalf("usage = %+v", u)
	}
	if u.TurnLatencyMS.P50 != 200 || !near(u.TurnLatencyMS.P90, 280) {
		t.Fatalf("turn latency = %+v, want p50 200, p90 280", u.TurnLatencyMS)
	}
	// Turns: 15 tokens; 100 + the 50 of its compaction pass.
	if !near(u.TokensPerTurn.Mean, 82.5) || !near(u.TokensPerTurn.P90, 136.5) || u.Tokens != 15+100+50+7 {
		t.Fatalf("tokens = %+v / %d", u.TokensPerTurn, u.Tokens)
	}
	// tri: 15 * 1; main: 90*1 + 10*100 + 50*10 + 7*10 = 1660 millionths.
	if u.CostUSD == nil || !near(*u.CostUSD, 0.001675) {
		t.Fatalf("cost = %v, want 0.001675", u.CostUSD)
	}
	if u := RunUsageOf(RunResult{}, fixedTranscript(t, "unpriced"), testPrices); u.CostUSD != nil {
		t.Fatalf("a request on an unpriced model priced the run: %v", *u.CostUSD)
	}
	if u := RunUsageOf(RunResult{}, fixedTranscript(t, "main"), nil); u.CostUSD != nil {
		t.Fatalf("no price table priced the run: %v", *u.CostUSD)
	}
	if u := RunUsageOf(RunResult{}, Transcript{}, testPrices); u.CostUSD != nil || u.Turns != 0 || u.TurnLatencyMS.P50 != -1 {
		t.Fatalf("a run with no usage events = %+v, want no data", u)
	}
}

func TestUsageCollectorApply(t *testing.T) {
	c := NewUsageCollector(testPrices)
	add := func(sc string, run int, ms int64, tr Transcript) {
		c.Add(RunResult{Scenario: sc, Run: run, DurationMS: ms}, tr)
	}
	mainOnly := Transcript{Events: []RecordedEvent{usageEvent(t, events.AgentUsagePayload{Model: "main", Role: "main", InputTokens: 20, LatencyMS: 1000})}}
	add("b", 1, 3000, mainOnly)
	add("a", 2, 2000, fixedTranscript(t, "main"))
	add("a", 1, 1000, fixedTranscript(t, "unpriced"))
	rep := Report{Scenarios: []ScenarioSummary{{Name: "a"}, {Name: "b"}, {Name: "none"}}}
	c.Apply(&rep)

	var order []string
	for _, r := range rep.RunUsage {
		order = append(order, r.Scenario+"#"+string(rune('0'+r.Run)))
	}
	if !reflect.DeepEqual(order, []string{"a#1", "a#2", "b#1"}) {
		t.Fatalf("run usage order = %v", order)
	}
	a := rep.Scenarios[0].Usage
	if a == nil || a.Runs != 2 || a.DurationMS.P50 != 1500 || a.Turns != 4 || a.TriageTurns != 2 || !near(a.TriageShare, 0.5) {
		t.Fatalf("scenario a = %+v", a)
	}
	if a.PricedRuns != 1 || a.CostPerRunUSD == nil || !near(a.CostPerRunUSD.Mean, 0.001675) {
		t.Fatalf("scenario a cost = %+v, want the one priced run", a)
	}
	if rep.Scenarios[2].Usage != nil {
		t.Fatal("a scenario with no runs got usage")
	}
	o := rep.Overall.Usage
	// Pooled: durations 1000, 2000, 3000; turn latencies 100, 300, 100, 300, 1000.
	if o == nil || o.Runs != 3 || o.DurationMS.P50 != 2000 || !near(o.DurationMS.P99, 2980) || o.Turns != 5 ||
		o.TurnLatencyMS.P50 != 300 || !near(o.TriageShare, 0.4) || o.PricedRuns != 2 {
		t.Fatalf("overall = %+v", o)
	}

	var md strings.Builder
	if err := rep.WriteMarkdown(&md); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"turn ms p50/p90/p99", "| a | 1500 / 1900 / 1990 |", "over 1/2 runs", "| 40% |"} {
		if !strings.Contains(md.String(), want) {
			t.Fatalf("markdown lacks %q:\n%s", want, md.String())
		}
	}
	md.Reset()
	if err := (Report{}).WriteMarkdown(&md); err != nil || strings.Contains(md.String(), "turn ms") {
		t.Fatalf("a report without usage shows the usage table:\n%s", md.String())
	}
}
