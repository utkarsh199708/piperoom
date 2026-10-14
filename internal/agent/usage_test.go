package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// usageEvents decodes every agent.usage event the sink got, in order.
func usageEvents(t *testing.T, sink *fakeSink) []events.AgentUsagePayload {
	t.Helper()
	var out []events.AgentUsagePayload
	for _, e := range sink.ofType(events.AgentUsage) {
		var p events.AgentUsagePayload
		if err := json.Unmarshal(e.payload, &p); err != nil {
			t.Fatalf("decode agent.usage: %v", err)
		}
		out = append(out, p)
	}
	return out
}

func roles(ps []events.AgentUsagePayload) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.Role
	}
	return out
}

func TestUsageEventPerCallWithRoleAcrossTriageSwitch(t *testing.T) {
	m := &modelRecorder{scriptedModel: scriptedModel{turns: []anthropic.BetaMessage{
		toolTurnN(t, 1), toolTurnN(t, 2), toolTurnN(t, 3),
	}}}
	sink := &fakeSink{}
	r := triageRunner(t, m, &fakeInbox{waitErr: context.Canceled}, sink, 2)
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := usageEvents(t, sink)
	if len(got) != len(m.models) {
		t.Fatalf("usage events = %d, model calls = %d: want one per call", len(got), len(m.models))
	}
	if want := []string{"triage", "triage", "main", "main"}; !reflect.DeepEqual(roles(got), want) {
		t.Fatalf("roles = %v, want %v", roles(got), want)
	}
	for i, p := range got {
		if p.Model != m.models[i] {
			t.Fatalf("event %d model = %q, want the request's %q", i, p.Model, m.models[i])
		}
	}
	// The scripted tool turns report 60 input and 10 output tokens.
	if got[0].InputTokens != 60 || got[0].OutputTokens != 10 || got[0].LatencyMS < 0 {
		t.Fatalf("first event = %+v, want the call's own usage", got[0])
	}
}

func TestUsageEventsOfAProposeFixRerun(t *testing.T) {
	propose := mustMessage(t, `{"stop_reason":"tool_use","content":[
		{"type":"tool_use","id":"tu9","name":"propose_fix","input":{"title":"t"}}
	],"usage":{"input_tokens":50,"output_tokens":5}}`)
	mainAnswer := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"main"}],"usage":{"input_tokens":50,"output_tokens":7}}`)
	m := &modelRecorder{scriptedModel: scriptedModel{turns: []anthropic.BetaMessage{propose, mainAnswer}}}
	sink := &fakeSink{}
	r := triageRunner(t, m, &fakeInbox{waitErr: context.Canceled}, sink, 6)
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := usageEvents(t, sink)
	if want := []string{"triage", "main"}; !reflect.DeepEqual(roles(got), want) {
		t.Fatalf("roles = %v, want %v: the dropped triage call was still sent and paid for", roles(got), want)
	}
	if got[0].Model != "tri" || got[1].Model != "main" || got[1].OutputTokens != 7 {
		t.Fatalf("events = %+v", got)
	}
}

func TestUsageEventOfAFailedCall(t *testing.T) {
	sink := &fakeSink{}
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector")}
	r := newTestRunner(t, &failingModel{err: errors.New("stream reset")}, &fakeInbox{waitErr: context.Canceled}, sink, rec, Options{Model: "main"})
	if err := r.run(context.Background()); err == nil {
		t.Fatal("run: want the model error")
	}
	got := usageEvents(t, sink)
	if len(got) != 1 || got[0].Role != "main" || got[0].InputTokens != 40 || got[0].OutputTokens != 2 {
		t.Fatalf("usage events = %+v, want one main event with the partial usage of the failed call", got)
	}
}

func TestNoUsageEventForARequestABudgetRefused(t *testing.T) {
	scripted := &scriptedModel{turns: []anthropic.BetaMessage{toolTurnN(t, 1), toolTurnN(t, 2), toolTurnN(t, 3)}}
	sink := &fakeSink{}
	r, _ := budgetRunner(t, scripted, newTokenBudget(150), sink, Options{MaxTurns: 10})
	if _, ok := asBudgetError(r.run(context.Background())); !ok {
		t.Fatal("want a budget stop")
	}
	if got := len(usageEvents(t, sink)); got != len(scripted.calls) {
		t.Fatalf("usage events = %d, model calls = %d: a refused request was never sent", got, len(scripted.calls))
	}
}

func TestUsageEventSplitsTheCompactionPass(t *testing.T) {
	turn := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}],"usage":{
		"input_tokens":10,"cache_creation_input_tokens":20,"cache_read_input_tokens":300,"output_tokens":5,
		"iterations":[
			{"type":"compaction","input_tokens":1000,"output_tokens":50,"cache_creation_input_tokens":3,"cache_read_input_tokens":4},
			{"type":"message","input_tokens":10,"output_tokens":5,"cache_creation_input_tokens":20,"cache_read_input_tokens":300}
		]}}`)
	sink := &fakeSink{}
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector")}
	r := newTestRunner(t, &scriptedModel{turns: []anthropic.BetaMessage{turn}}, &fakeInbox{waitErr: context.Canceled}, sink, rec, Options{Model: "main"})
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := usageEvents(t, sink)
	want := []events.AgentUsagePayload{
		{Model: "main", Role: "main", InputTokens: 10, CacheCreationInputTokens: 20, CacheReadInputTokens: 300, OutputTokens: 5},
		{Model: "main", Role: "compaction", InputTokens: 1000, CacheCreationInputTokens: 3, CacheReadInputTokens: 4, OutputTokens: 50},
	}
	if len(got) != 2 {
		t.Fatalf("usage events = %+v, want the request and its compaction pass", got)
	}
	got[0].LatencyMS = 0 // measured; checked below
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("usage events = %+v\nwant %+v", got, want)
	}
	// The two events add up to what the budget counts.
	var sum int64
	for _, p := range got {
		sum += p.InputTokens + p.CacheCreationInputTokens + p.CacheReadInputTokens + p.OutputTokens
	}
	if u := messageUsage(turn); sum != u.total() {
		t.Fatalf("events sum to %d, messageUsage = %d", sum, u.total())
	}
}

// slowModel answers after a pause, so the latency has something to measure.
type slowModel struct{ d time.Duration }

func (m slowModel) Turn(ctx context.Context, _ anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	time.Sleep(m.d)
	return anthropic.BetaMessage{StopReason: anthropic.BetaStopReasonEndTurn, Content: []anthropic.BetaContentBlockUnion{{Type: "text", Text: "done"}}}, nil
}

func TestUsageEventMeasuresLatency(t *testing.T) {
	sink := &fakeSink{}
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector")}
	r := newTestRunner(t, slowModel{d: 30 * time.Millisecond}, &fakeInbox{waitErr: context.Canceled}, sink, rec, Options{Model: "main"})
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := usageEvents(t, sink)
	if len(got) != 1 || got[0].LatencyMS < 30 {
		t.Fatalf("usage events = %+v, want latency_ms >= 30", got)
	}
}

func TestUsageEventCarriesNoText(t *testing.T) {
	const marker = "ZQX-MARKER-7731"
	toolUse := mustMessage(t, `{"stop_reason":"tool_use","content":[
		{"type":"text","text":"thinking about `+marker+`"},
		{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"`+marker+`","connector":"`+marker+`"}}
	],"usage":{"input_tokens":60,"output_tokens":10,
		"iterations":[{"type":"compaction","input_tokens":5,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}]}}`)
	end := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"the cause is `+marker+`"}],"usage":{"input_tokens":70,"output_tokens":5}}`)
	sink := &fakeSink{}
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "status " + marker}}
	r := newTestRunner(t, &scriptedModel{turns: []anthropic.BetaMessage{toolUse, end}}, &fakeInbox{
		drains:  [][]HumanInput{{{Kind: "steer", Text: "look at " + marker}}},
		waitErr: context.Canceled,
	}, sink, rec, Options{Model: "main"})
	r.alert.Title = "alert " + marker
	r.alert.PipelineMap = marker
	r.alert.Raw = json.RawMessage(`{"note":"` + marker + `"}`)
	r.prices = Prices{"main": {Input: 1, Output: 2, CacheWrite: 3, CacheRead: 4}}
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	// The marker reached the model and the room, so the test can see a leak.
	if !strings.Contains(messagesJSON(r.model.(*scriptedModel).calls[0]), marker) {
		t.Fatal("test setup: the marker is not in the prompt")
	}
	evs := sink.ofType(events.AgentUsage)
	if len(evs) != 3 {
		t.Fatalf("usage events = %d, want 3 (two turns, one compaction)", len(evs))
	}
	allowed := []string{"cache_creation_input_tokens", "cache_read_input_tokens", "cost_usd", "input_tokens", "latency_ms", "model", "output_tokens", "role"}
	for _, e := range evs {
		if strings.Contains(string(e.payload), marker) {
			t.Fatalf("agent.usage carries text: %s", e.payload)
		}
		var keys map[string]json.RawMessage
		if err := json.Unmarshal(e.payload, &keys); err != nil {
			t.Fatal(err)
		}
		var names []string
		for k := range keys {
			names = append(names, k)
		}
		sort.Strings(names)
		if !reflect.DeepEqual(names, allowed) {
			t.Fatalf("agent.usage fields = %v, want only %v", names, allowed)
		}
	}
}

func TestUsageCostOmittedWithoutAPriceTable(t *testing.T) {
	run := func(prices Prices) []sinkEvent {
		sink := &fakeSink{}
		rec := &recordingTool{name: "connect_status", schema: testSchema("connector")}
		turn := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}],"usage":{
			"input_tokens":1000,"cache_creation_input_tokens":2000,"cache_read_input_tokens":3000,"output_tokens":4000}}`)
		r := newTestRunner(t, &scriptedModel{turns: []anthropic.BetaMessage{turn}}, &fakeInbox{waitErr: context.Canceled}, sink, rec, Options{Model: "main"})
		r.prices = prices
		if err := r.run(context.Background()); err != nil {
			t.Fatalf("run: %v", err)
		}
		return sink.ofType(events.AgentUsage)
	}
	for name, prices := range map[string]Prices{"no table": nil, "model not in table": {"other": {Input: 1}}} {
		evs := run(prices)
		if len(evs) != 1 || strings.Contains(string(evs[0].payload), "cost_usd") {
			t.Fatalf("%s: usage = %s, want no cost_usd", name, evs[0].payload)
		}
	}
	// USD per million tokens: 1000*5 + 2000*6.25 + 3000*0.5 + 4000*25 = 119000 millionths.
	evs := run(Prices{"main": {Input: 5, Output: 25, CacheWrite: 6.25, CacheRead: 0.5}})
	var p events.AgentUsagePayload
	if err := json.Unmarshal(evs[0].payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.CostUSD == nil || *p.CostUSD != 0.119 {
		t.Fatalf("cost_usd = %v, want 0.119", p.CostUSD)
	}
}

func TestParseModelPrices(t *testing.T) {
	ok := `{"m":{"input":5,"output":25,"cache_write":6.25,"cache_read":0.5}}`
	p, err := ParseModelPrices([]byte(ok))
	if err != nil || !p.Has("m") || p["m"].CacheWrite != 6.25 {
		t.Fatalf("valid table: %+v, %v", p, err)
	}
	if p.Has("other") {
		t.Fatal("Has(other) = true")
	}
	bad := map[string]string{
		"missing field": `{"m":{"input":5,"output":25,"cache_write":6.25}}`,
		"unknown field": `{"m":{"input":5,"output":25,"cache_write":6.25,"cache_read":0.5,"cache_raed":1}}`,
		"negative":      `{"m":{"input":-1,"output":25,"cache_write":6.25,"cache_read":0.5}}`,
		"too large":     `{"m":{"input":5000000,"output":25,"cache_write":6.25,"cache_read":0.5}}`,
		"string price":  `{"m":{"input":"5","output":25,"cache_write":6.25,"cache_read":0.5}}`,
		"empty table":   `{}`,
		"empty model":   `{"":{"input":5,"output":25,"cache_write":6.25,"cache_read":0.5}}`,
		"not an object": `[1]`,
		"trailing data": ok + `{}`,
		"not JSON":      `input=5`,
	}
	for name, in := range bad {
		if _, err := ParseModelPrices([]byte(in)); err == nil {
			t.Errorf("%s: accepted %s", name, in)
		}
	}
}

func TestLoadModelPrices(t *testing.T) {
	if p, err := LoadModelPrices(""); p != nil || err != nil {
		t.Fatalf("empty path = %v, %v; want no table", p, err)
	}
	dir := t.TempDir()
	if _, err := LoadModelPrices(filepath.Join(dir, "missing.json")); err == nil || !strings.Contains(err.Error(), "PIPEROOM_MODEL_PRICES") {
		t.Fatalf("missing file: %v", err)
	}
	big := filepath.Join(dir, "big.json")
	if err := os.WriteFile(big, make([]byte, maxPricesFileBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadModelPrices(big); err == nil || !strings.Contains(err.Error(), "larger") {
		t.Fatalf("oversized file: %v", err)
	}
	good := filepath.Join(dir, "prices.json")
	if err := os.WriteFile(good, []byte(`{"m":{"input":1,"output":2,"cache_write":3,"cache_read":4}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if p, err := LoadModelPrices(good); err != nil || !p.Has("m") {
		t.Fatalf("good file: %v, %v", p, err)
	}
}

func TestHandoffUsageIsReportedThroughTheEndpoint(t *testing.T) {
	summary := `{\"what_broke\":\"x\",\"why\":\"y\",\"done\":[],\"ruled_out\":[],\"open\":[],\"risks\":[]}`
	turn := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"`+summary+`"}],"usage":{"input_tokens":40,"output_tokens":10}}`)
	ep := testEndpoint(&scriptedModel{turns: []anthropic.BetaMessage{turn}}, 100)
	type got struct {
		room string
		p    events.AgentUsagePayload
	}
	var reported []got
	if err := ep.SetSpend(SpendOptions{
		Prices: Prices{"claude-opus-5-5": {Input: 1, Output: 1, CacheWrite: 1, CacheRead: 1}},
		OnUsage: func(_ context.Context, roomID string, p events.AgentUsagePayload) {
			reported = append(reported, got{roomID, p})
		},
	}); err != nil {
		t.Fatal(err)
	}
	evs := []events.Event{{RoomID: "room-h", Seq: 1, Type: events.AgentMessage, Payload: json.RawMessage(`{"text":"hi"}`)}}
	if _, err := ep.WriteHandoff(context.Background(), evs, Options{}); err != nil {
		t.Fatalf("WriteHandoff: %v", err)
	}
	if len(reported) != 1 || reported[0].room != "room-h" || reported[0].p.Role != "handoff" ||
		reported[0].p.InputTokens != 40 || reported[0].p.CostUSD == nil || *reported[0].p.CostUSD != 0.00005 {
		t.Fatalf("reported = %+v, want one priced handoff event for room-h", reported)
	}
	if used := ep.budget("room-h").Used(); used != 50 {
		t.Fatalf("used = %d, want the hand-off counted", used)
	}
}

func TestHandoffRendererSkipsUsageEvents(t *testing.T) {
	usage, _ := json.Marshal(events.AgentUsagePayload{Model: "m", Role: "main", InputTokens: 5})
	var evs []events.Event
	for i := 1; i <= 3*handoffTailEvents; i++ {
		evs = append(evs, events.Event{RoomID: "r", Seq: int64(i), Type: events.AgentUsage, Actor: events.AgentActor, Payload: usage})
	}
	evs = append(evs, events.Event{RoomID: "r", Seq: 999, Type: events.AgentMessage, Actor: events.AgentActor, Payload: json.RawMessage(`{"text":"the cause"}`)})
	out := renderRoomEvents(evs)
	if strings.Contains(out, "agent.usage") || strings.Contains(out, "omitted") {
		t.Fatalf("hand-off input shows usage events:\n%s", out)
	}
	if !strings.Contains(out, "the cause") {
		t.Fatalf("hand-off input lost the message:\n%s", out)
	}
}

func TestSplitUsageAddsUpToMessageUsage(t *testing.T) {
	msg := mustMessage(t, `{"usage":{"input_tokens":1,"cache_creation_input_tokens":2,"cache_read_input_tokens":3,"output_tokens":4,
		"iterations":[{"type":"compaction","input_tokens":10,"output_tokens":20,"cache_creation_input_tokens":30,"cache_read_input_tokens":40},
		{"type":"compaction","input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":1,"cache_read_input_tokens":1}]}}`)
	own, comp, compacted := splitUsage(msg)
	if !compacted || own.total() != 10 || comp.total() != 104 {
		t.Fatalf("own %+v comp %+v compacted %v", own, comp, compacted)
	}
	if own.total()+comp.total() != messageUsage(msg).total() {
		t.Fatal("splitUsage and messageUsage disagree")
	}
}
