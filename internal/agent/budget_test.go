package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

func TestParseRoomTokenBudget(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{in: "", want: 2_000_000},
		{in: "   ", want: 2_000_000},
		{in: "0", want: 0},
		{in: " 0 ", want: 0},
		{in: "1", want: 1},
		{in: "500000", want: 500_000},
		{in: " 42 ", want: 42},
		{in: "-1", wantErr: true},
		{in: "-0", wantErr: true},
		{in: "00", wantErr: true},
		{in: "abc", wantErr: true},
		{in: "1.5", wantErr: true},
		{in: "1e6", wantErr: true},
		{in: "0x10", wantErr: true},
		{in: "2,000,000", wantErr: true},
		{in: "+5", wantErr: true},
		{in: "99999999999999999999", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseRoomTokenBudget(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseRoomTokenBudget(%q) = %d, want an error", tt.in, got)
				}
				if !strings.Contains(err.Error(), "PIPEROOM_ROOM_TOKEN_BUDGET") {
					t.Fatalf("error %q does not name the setting", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseRoomTokenBudget(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("ParseRoomTokenBudget(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestTokenBudgetAdmitAtTheBoundary(t *testing.T) {
	b := newTokenBudget(100)
	b.record(usageTokens{input: 90, output: 9}, true) // 99 used
	if err := b.admit(0); err != nil {
		t.Fatalf("99 of 100 used: the next call must be allowed, got %v", err)
	}
	b.record(usageTokens{output: 1}, true) // exactly 100
	err := b.admit(0)
	be, ok := asBudgetError(err)
	if !ok {
		t.Fatalf("100 of 100 used: want a BudgetError, got %v", err)
	}
	if be.Used != 100 || be.Limit != 100 {
		t.Fatalf("BudgetError = %+v, want 100 of 100", be)
	}
}

func TestTokenBudgetEstimateCountsTheNextInput(t *testing.T) {
	b := newTokenBudget(150)
	b.record(usageTokens{input: 60, output: 10}, true) // 70 used, last input 60
	if err := b.admit(b.nextEstimate()); err != nil {
		t.Fatalf("70 + 60 < 150: want allowed, got %v", err)
	}
	b.record(usageTokens{input: 60, output: 10}, true) // 140 used
	if err := b.admit(b.nextEstimate()); err == nil {
		t.Fatal("140 + 60 >= 150: want the call refused")
	}
	// A failed call reports no usage and must not reset the estimate.
	b.record(usageTokens{}, true)
	if got := b.nextEstimate(); got != 60 {
		t.Fatalf("estimate after an empty usage = %d, want 60", got)
	}
}

func TestTokenBudgetZeroMeansNoCap(t *testing.T) {
	b := newTokenBudget(0)
	b.record(usageTokens{input: 1 << 40, output: 1 << 40}, true)
	if err := b.admit(b.nextEstimate()); err != nil {
		t.Fatalf("limit 0 is no cap, got %v", err)
	}
}

func TestMessageUsageCountsCacheAndCompaction(t *testing.T) {
	msg := mustMessage(t, `{"stop_reason":"end_turn","content":[],"usage":{
		"input_tokens":10,"cache_creation_input_tokens":20,"cache_read_input_tokens":300,"output_tokens":5,
		"iterations":[
			{"type":"compaction","input_tokens":1000,"output_tokens":50,"cache_creation_input_tokens":0,"cache_read_input_tokens":0},
			{"type":"message","input_tokens":10,"output_tokens":5,"cache_creation_input_tokens":20,"cache_read_input_tokens":300}
		]}}`)
	got := messageUsage(sdkUsage(msg))
	if got.input != 330+1000 || got.output != 5+50 {
		t.Fatalf("messageUsage = %+v, want input 1330 and output 55 (compaction added once, message iteration not double counted)", got)
	}
}

// ---- loop tests ----

func budgetRunner(t *testing.T, scripted sdkModel, b *tokenBudget, sink *fakeSink, opts Options) (*runner, *recordingTool) {
	t.Helper()
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	r := newTestRunnerM(t, &budgetModel{inner: viaSDK(scripted), budget: b, loop: true}, &fakeInbox{waitErr: context.Canceled}, sink, rec, opts)
	return r, rec
}

const toolTurn = `{"stop_reason":"tool_use","content":[
	{"type":"tool_use","id":"tu%d","name":"connect_status","input":{"why":"check","connector":"c"}}
],"usage":{"input_tokens":60,"output_tokens":10}}`

func toolTurnN(t *testing.T, n int) anthropic.BetaMessage {
	t.Helper()
	return mustMessage(t, strings.Replace(toolTurn, "%d", string(rune('0'+n)), 1))
}

func TestRunStopsAtBudgetAfterTheToolsFinish(t *testing.T) {
	scripted := &scriptedModel{turns: []anthropic.BetaMessage{toolTurnN(t, 1), toolTurnN(t, 2), toolTurnN(t, 3)}}
	b := newTokenBudget(150)
	sink := &fakeSink{}
	r, rec := budgetRunner(t, scripted, b, sink, Options{MaxTurns: 10})

	err := r.run(context.Background())
	be, ok := asBudgetError(err)
	if !ok {
		t.Fatalf("a budget stop must return an error wrapping *BudgetError, got %v", err)
	}
	if be.Used != 140 || be.Limit != 150 {
		t.Fatalf("BudgetError = %+v, want 140 of 150", be)
	}
	// Turn 1: 70 used, estimate 60 -> allowed. Turn 2: 140 used, estimate 60 ->
	// refused before the third call.
	if len(scripted.calls) != 2 {
		t.Fatalf("model calls = %d, want 2", len(scripted.calls))
	}
	if got := len(rec.inputsAsJSON()); got != 2 {
		t.Fatalf("tool runs = %d, want both of the finished turns' tools to have run", got)
	}
	if got := b.Used(); got != 140 {
		t.Fatalf("used = %d, want 140", got)
	}

	const want = "token budget reached (140 of 150); a person must raise PIPEROOM_ROOM_TOKEN_BUDGET or continue manually"
	msgs := sink.ofType(events.AgentMessage)
	if len(msgs) != 1 {
		t.Fatalf("agent messages = %d, want 1", len(msgs))
	}
	var m events.AgentMessagePayload
	if err := json.Unmarshal(msgs[0].payload, &m); err != nil {
		t.Fatal(err)
	}
	if m.Text != want {
		t.Fatalf("message = %q, want %q", m.Text, want)
	}
	statuses := sink.ofType(events.AgentStatus)
	var last events.AgentStatusPayload
	if err := json.Unmarshal(statuses[len(statuses)-1].payload, &last); err != nil {
		t.Fatal(err)
	}
	if last.State != "idle" || last.Detail != want {
		t.Fatalf("last status = %+v, want idle with the budget text", last)
	}
	for _, s := range statuses {
		var p events.AgentStatusPayload
		_ = json.Unmarshal(s.payload, &p)
		if p.State == "error" {
			t.Fatalf("a budget stop must not report an error status: %+v", p)
		}
	}
}

func TestRunWithZeroBudgetIsNeverStopped(t *testing.T) {
	scripted := &scriptedModel{turns: []anthropic.BetaMessage{toolTurnN(t, 1), toolTurnN(t, 2), toolTurnN(t, 3)}}
	b := newTokenBudget(0)
	sink := &fakeSink{}
	r, _ := budgetRunner(t, scripted, b, sink, Options{MaxTurns: 10})
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	// Three tool turns, then the scripted default end_turn.
	if len(scripted.calls) != 4 {
		t.Fatalf("model calls = %d, want 4 (no cap)", len(scripted.calls))
	}
	if got := b.Used(); got != 210 {
		t.Fatalf("used = %d, want 210: usage is counted even with no cap", got)
	}
	if n := len(sink.ofType(events.AgentMessage)); n != 1 { // only the scripted "done"
		t.Fatalf("agent messages = %d, want 1", n)
	}
}

func TestUsageAccumulatesAcrossTurnsAndCacheTokens(t *testing.T) {
	cached := mustMessage(t, `{"stop_reason":"tool_use","content":[
		{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"check","connector":"c"}}
	],"usage":{"input_tokens":5,"cache_read_input_tokens":1000,"cache_creation_input_tokens":200,"output_tokens":30}}`)
	end := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}],"usage":{"input_tokens":2,"cache_read_input_tokens":1200,"output_tokens":8}}`)
	scripted := &scriptedModel{turns: []anthropic.BetaMessage{cached, end}}
	b := newTokenBudget(0)
	r, _ := budgetRunner(t, scripted, b, &fakeSink{}, Options{MaxTurns: 10})
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got, want := b.Used(), int64(5+1000+200+30+2+1200+8); got != want {
		t.Fatalf("used = %d, want %d", got, want)
	}
}

// ---- endpoint: the budget outlives a run ----

func testEndpoint(m sdkModel, limit int64) *Endpoint {
	return &Endpoint{provider: ProviderAnthropic, limit: limit, model: viaSDK(m), log: discardLogger(), budgets: map[string]*tokenBudget{}, triageTurns: defaultTriageTurns}
}

// endpointRun runs one room on ep and returns Run's error.
func endpointRun(t *testing.T, ep *Endpoint, roomID string, sink *fakeSink) error {
	t.Helper()
	reg := tools.NewRegistry()
	return ep.Run(context.Background(), Alert{RoomID: roomID, Title: "orders stale", PipelineID: "orders"}, Deps{
		Tools: reg, Inbox: &fakeInbox{waitErr: context.Canceled}, Sink: sink, Logger: discardLogger(),
	}, Options{})
}

func TestEndpointBudgetSurvivesAnAgentRestart(t *testing.T) {
	turn := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}],"usage":{"input_tokens":80,"output_tokens":30}}`)
	scripted := &scriptedModel{turns: []anthropic.BetaMessage{turn, turn, turn}}
	ep := testEndpoint(scripted, 100)

	first := &fakeSink{}
	if err := endpointRun(t, ep, "room-a", first); err != nil { // 110 used after the first run
		t.Fatalf("first run: %v", err)
	}
	if len(scripted.calls) != 1 {
		t.Fatalf("first run made %d calls, want 1", len(scripted.calls))
	}

	// The room service starts a new Run for the same room: the count carries over.
	second := &fakeSink{}
	if _, ok := asBudgetError(endpointRun(t, ep, "room-a", second)); !ok {
		t.Fatal("a restarted run on a spent budget must return a *BudgetError")
	}
	if len(scripted.calls) != 1 {
		t.Fatalf("restarted run called the model again (%d calls); the budget did not survive", len(scripted.calls))
	}
	msgs := second.ofType(events.AgentMessage)
	if len(msgs) != 1 || !strings.Contains(string(msgs[0].payload), "token budget reached (110 of 100)") {
		t.Fatalf("restarted run messages = %v, want the budget message", msgs)
	}

	// Another room has its own budget.
	if err := endpointRun(t, ep, "room-b", &fakeSink{}); err != nil {
		t.Fatalf("room-b: %v", err)
	}
	if len(scripted.calls) != 2 {
		t.Fatalf("a different room made %d total calls, want 2", len(scripted.calls))
	}
}

func TestEndpointHandoffIsCountedButNeverRefused(t *testing.T) {
	summary := `{\"what_broke\":\"x\",\"why\":\"y\",\"done\":[],\"ruled_out\":[],\"open\":[],\"risks\":[]}`
	turn := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"`+summary+`"}],"usage":{"input_tokens":40,"output_tokens":10}}`)
	scripted := &scriptedModel{turns: []anthropic.BetaMessage{turn, turn}}
	ep := testEndpoint(scripted, 100)
	b := ep.budget("room-a")
	b.record(usageTokens{input: 500}, true) // the loop is already over budget

	evs := []events.Event{{RoomID: "room-a", Seq: 1, Type: events.AgentMessage, Payload: json.RawMessage(`{"text":"hi"}`)}}
	got, err := ep.WriteHandoff(context.Background(), evs, Options{})
	if err != nil {
		t.Fatalf("WriteHandoff over budget: %v", err)
	}
	if got.WhatBroke != "x" {
		t.Fatalf("summary = %+v", got)
	}
	if used := b.Used(); used != 550 {
		t.Fatalf("used = %d, want 550: the hand-off must be counted", used)
	}
	if _, err := ep.WriteHandoff(context.Background(), nil, Options{}); err == nil {
		t.Fatal("an empty room must still be an error")
	}
}
