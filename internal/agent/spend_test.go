package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/agent/llm"
	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
)

// storeLedger is cmd/server's adapter, repeated here so the agent's tests run
// against the real store semantics (memstore, and Postgres in the
// integration test).
type storeLedger struct{ st store.SpendStore }

func (l storeLedger) AddSpend(ctx context.Context, c SpendChange) (SpendTotals, error) {
	sc := store.SpendChange{RoomID: c.RoomID, PipelineID: c.PipelineID, Day: c.Day, Tokens: c.Tokens, CostMicros: c.CostMicros}
	if c.Call != nil {
		sc.Call = &store.SpendCall{
			Model: c.Call.Model, InputTokens: c.Call.InputTokens, CacheCreationInputTokens: c.Call.CacheCreationInputTokens,
			CacheReadInputTokens: c.Call.CacheReadInputTokens, OutputTokens: c.Call.OutputTokens, CostMicros: c.Call.CostMicros, Priced: c.Call.Priced,
		}
	}
	t, err := l.st.AddSpend(ctx, sc)
	return SpendTotals(t), err
}

// spendRooms is a memstore holding rooms of pipeline "orders".
func spendRooms(t *testing.T, ids ...string) *memstore.Store {
	t.Helper()
	st := memstore.New()
	for _, id := range ids {
		if err := st.CreateRoom(context.Background(), store.Room{ID: id, Title: "t", PipelineID: "orders", Status: "open"}); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func spendEndpoint(t *testing.T, m sdkModel, limit int64, l SpendLedger, o SpendOptions) *Endpoint {
	t.Helper()
	ep := testEndpoint(m, limit)
	o.Ledger = l
	if o.EstimateFloor == 0 {
		// A floor of 1 keeps the arithmetic of these tests; the default
		// floor has its own test.
		o.EstimateFloor = 1
	}
	if err := ep.SetSpend(o); err != nil {
		t.Fatalf("SetSpend: %v", err)
	}
	return ep
}

// spendClock is a settable UTC clock.
type spendClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *spendClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *spendClock) set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}

const endTurn110 = `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}],"usage":{"input_tokens":80,"output_tokens":30}}`

func TestSpendBudgetPersistsAcrossARestart(t *testing.T) {
	st := spendRooms(t, "room-a")
	turn := mustMessage(t, endTurn110)
	first := &scriptedModel{turns: []anthropic.BetaMessage{turn}}
	if err := endpointRun(t, spendEndpoint(t, first, 100, storeLedger{st}, SpendOptions{}), "room-a", &fakeSink{}); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// A restart: a new endpoint (empty in-memory budgets) over the same store.
	second := &scriptedModel{turns: []anthropic.BetaMessage{turn}}
	sink := &fakeSink{}
	err := endpointRun(t, spendEndpoint(t, second, 100, storeLedger{st}, SpendOptions{}), "room-a", sink)
	be, ok := asBudgetError(err)
	if !ok {
		t.Fatalf("run after the restart = %v, want a *BudgetError", err)
	}
	if len(second.calls) != 0 {
		t.Fatalf("the restarted endpoint called the model %d times; the persisted 110 tokens were not checked", len(second.calls))
	}
	const want = "token budget reached (110 of 100); a person must raise PIPEROOM_ROOM_TOKEN_BUDGET or continue manually"
	if be.Error() != want {
		t.Fatalf("text = %q, want today's %q", be.Error(), want)
	}
	msgs := sink.ofType(events.AgentMessage)
	if len(msgs) != 1 || !strings.Contains(string(msgs[0].payload), want) {
		t.Fatalf("messages = %v", msgs)
	}

	rep, err := st.RoomSpend(context.Background(), "room-a")
	if err != nil {
		t.Fatal(err)
	}
	if rep.ChargedTokens != 110 || len(rep.ByModel) != 1 || rep.ByModel[0].Calls != 1 || rep.ByModel[0].Tokens() != 110 {
		t.Fatalf("room spend = %+v, want 110 charged in one finished call (the refused estimate taken back)", rep)
	}
}

// barrierModel holds every request until g have arrived (or a short wait has
// passed), so requests that were admitted together are in flight together.
type barrierModel struct {
	g       int
	calls   atomic.Int64
	once    sync.Once
	all     chan struct{}
	mu      sync.Mutex
	arrived int
	turn    anthropic.BetaMessage
}

func (m *barrierModel) Turn(ctx context.Context, _ anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	m.calls.Add(1)
	m.mu.Lock()
	m.arrived++
	if m.arrived == m.g {
		m.once.Do(func() { close(m.all) })
	}
	m.mu.Unlock()
	select {
	case <-m.all:
	case <-time.After(300 * time.Millisecond):
	case <-ctx.Done():
	}
	return m.turn, nil
}

// concurrentBurst sends g requests at once, each estimated at est and using
// actual (more than est), against a cap of capLimit, and checks the soft-cap
// bound of docs/design/cost.md 3.3: (a) the estimates of the admitted
// requests stay within the cap, and (b) the overshoot is at most
// admitted × (actual − est). The stored totals must equal what was spent.
func concurrentBurst(t *testing.T, st store.SpendStore, rooms []string, roomCap, dailyCap int64) {
	t.Helper()
	const est, actual = 100, 150
	g := len(rooms)
	capLimit := roomCap + dailyCap
	day := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	m := &barrierModel{g: g, all: make(chan struct{}), turn: mustMessage(t, `{"stop_reason":"end_turn","content":[],"usage":{"input_tokens":150}}`)}
	ep := spendEndpoint(t, m, roomCap, storeLedger{st}, SpendOptions{PipelineDailyTokens: dailyCap, Now: func() time.Time { return day }})
	for _, id := range uniq(rooms) {
		ep.budget(id).record(usageTokens{input: est}, true) // the next estimate
	}
	var wg sync.WaitGroup
	var refused atomic.Int64
	for _, id := range rooms {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			bm := &budgetModel{inner: viaSDK(m), budget: ep.budget(id), loop: true, guard: ep.guard(id, "orders")}
			if _, err := bm.Turn(context.Background(), llm.Request{Model: "m"}); err != nil {
				if _, ok := asBudgetError(err); !ok {
					t.Errorf("Turn: %v", err)
				}
				refused.Add(1)
			}
		}(id)
	}
	wg.Wait()

	admitted := m.calls.Load()
	if admitted+refused.Load() != int64(g) || refused.Load() == 0 {
		t.Fatalf("admitted %d + refused %d, want %d with some refused", admitted, refused.Load(), g)
	}
	if admitted*est > capLimit {
		t.Fatalf("(a) %d admitted requests reserved %d tokens against a cap of %d", admitted, admitted*est, capLimit)
	}
	spent := admitted * actual
	if over := spent - capLimit; over > admitted*(actual-est) {
		t.Fatalf("(b) overshoot %d > admitted %d × (usage − estimate) %d", over, admitted, actual-est)
	}
	// Settled and refunded, the stored totals are exactly what was spent.
	p, err := st.PipelineSpend(context.Background(), "orders", "2026-10-13")
	if err != nil {
		t.Fatal(err)
	}
	if p.ChargedTokens != spent || p.Totals().Tokens() != spent {
		t.Fatalf("pipeline spend = %+v, want %d charged and finished", p, spent)
	}
}

func TestSpendConcurrentAdmitsKeepEstimatesWithinTheCap(t *testing.T) {
	const g, capLimit = 20, 1000
	t.Run("one room", func(t *testing.T) {
		rooms := make([]string, g)
		for i := range rooms {
			rooms[i] = "room-0"
		}
		concurrentBurst(t, spendRooms(t, "room-0"), rooms, capLimit, 0)
	})
	t.Run("one pipeline", func(t *testing.T) {
		rooms := make([]string, g)
		for i := range rooms {
			rooms[i] = "room-" + string(rune('a'+i))
		}
		concurrentBurst(t, spendRooms(t, rooms...), rooms, 0, capLimit)
	})
}

func uniq(ss []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range ss {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func TestSpendRefusedEstimateIsTakenBackAndSettleReplacesIt(t *testing.T) {
	st := spendRooms(t, "room-a")
	scripted := &scriptedModel{turns: []anthropic.BetaMessage{toolTurnN(t, 1), toolTurnN(t, 2), toolTurnN(t, 3)}}
	sink := &fakeSink{}
	ep := spendEndpoint(t, scripted, 150, storeLedger{st}, SpendOptions{})
	reg := tools.NewRegistry()
	err := ep.Run(context.Background(), Alert{RoomID: "room-a", Title: "t", PipelineID: "orders"}, Deps{
		Tools: reg, Inbox: &fakeInbox{waitErr: context.Canceled}, Sink: sink, Logger: discardLogger(),
	}, Options{MaxTurns: 10})
	be, ok := asBudgetError(err)
	if !ok || be.Used != 140 || be.Limit != 150 {
		t.Fatalf("Run = %v, want a stop at 140 of 150", err)
	}
	if len(scripted.calls) != 2 {
		t.Fatalf("model calls = %d, want 2", len(scripted.calls))
	}
	rep, err := st.RoomSpend(context.Background(), "room-a")
	if err != nil {
		t.Fatal(err)
	}
	// Call 1: estimate 0, used 70. Call 2: estimate 60 charged, settled to 70.
	// Call 3: estimate 60 charged, refused, taken back.
	if rep.ChargedTokens != 140 || rep.Totals().Tokens() != 140 || rep.Totals().Calls != 2 {
		t.Fatalf("room spend = %+v, want 140 charged over 2 calls", rep)
	}
}

// brokenLedger fails every call.
type brokenLedger struct{ calls atomic.Int64 }

func (l *brokenLedger) AddSpend(context.Context, SpendChange) (SpendTotals, error) {
	l.calls.Add(1)
	return SpendTotals{}, errors.New("connection refused")
}

func TestSpendStoreErrorFailsClosedWithACapAndOpenWithout(t *testing.T) {
	turn := mustMessage(t, endTurn110)
	closed := func(t *testing.T, roomCap int64, o SpendOptions) {
		scripted := &scriptedModel{turns: []anthropic.BetaMessage{turn}}
		sink := &fakeSink{}
		err := endpointRun(t, spendEndpoint(t, scripted, roomCap, &brokenLedger{}, o), "room-a", sink)
		be, ok := asBudgetError(err)
		if !ok || be.Error() != storeUnavailableText {
			t.Fatalf("Run = %v, want a stop with the store text", err)
		}
		if len(scripted.calls) != 0 {
			t.Fatal("the model was called although the limit could not be checked")
		}
		statuses := sink.ofType(events.AgentStatus)
		var last events.AgentStatusPayload
		if err := json.Unmarshal(statuses[len(statuses)-1].payload, &last); err != nil {
			t.Fatal(err)
		}
		if last.State != "idle" || last.Detail != storeUnavailableText {
			t.Fatalf("last status = %+v, want idle with the store text", last)
		}
		if n := len(sink.ofType(events.AgentUsage)); n != 0 {
			t.Fatalf("usage events = %d for a request that was not sent", n)
		}
	}
	t.Run("room cap set", func(t *testing.T) { closed(t, 100, SpendOptions{}) })
	t.Run("only a daily cap set", func(t *testing.T) { closed(t, 0, SpendOptions{PipelineDailyTokens: 500}) })
	t.Run("only a cost cap set", func(t *testing.T) {
		closed(t, 0, SpendOptions{PipelineDailyCostUSD: 5, Prices: Prices{"claude-opus-5-5": {Input: 1}}})
	})

	t.Run("no cap set", func(t *testing.T) {
		scripted := &scriptedModel{turns: []anthropic.BetaMessage{turn}}
		var logs bytes.Buffer
		l := &brokenLedger{}
		ep := spendEndpoint(t, scripted, 0, l, SpendOptions{})
		ep.log = slog.New(slog.NewTextHandler(&logs, nil))
		if err := endpointRun(t, ep, "room-a", &fakeSink{}); err != nil {
			t.Fatalf("Run = %v, want the run to go on", err)
		}
		if len(scripted.calls) != 1 {
			t.Fatalf("model calls = %d, want 1", len(scripted.calls))
		}
		if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "spend store unavailable") {
			t.Fatalf("logs = %q, want a warning", logs.String())
		}
		if strings.Contains(logs.String(), "the run stops") {
			t.Fatalf("logs = %q, want no stop with no cap", logs.String())
		}
		if l.calls.Load() != 3 {
			t.Fatalf("store calls = %d, want the charge, the settle and its one retry", l.calls.Load())
		}
	})

	t.Run("the hand-off is never refused", func(t *testing.T) {
		summary := `{\"what_broke\":\"x\",\"why\":\"y\",\"done\":[],\"ruled_out\":[],\"open\":[],\"risks\":[]}`
		h := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"`+summary+`"}],"usage":{"input_tokens":4,"output_tokens":1}}`)
		ep := spendEndpoint(t, &scriptedModel{turns: []anthropic.BetaMessage{h}}, 100, &brokenLedger{}, SpendOptions{})
		evs := []events.Event{{RoomID: "room-a", Seq: 1, Type: events.AgentMessage, Payload: json.RawMessage(`{"text":"hi"}`)}}
		if _, err := ep.WriteHandoff(context.Background(), evs, Options{}); err != nil {
			t.Fatalf("WriteHandoff: %v", err)
		}
	})
}

// blockingLedger waits for its context, like a store that hangs.
type blockingLedger struct{}

func (blockingLedger) AddSpend(ctx context.Context, _ SpendChange) (SpendTotals, error) {
	<-ctx.Done()
	return SpendTotals{}, ctx.Err()
}

func TestSpendChargeCutShortByAClosedRoomIsNotAStoreOutage(t *testing.T) {
	sink := &fakeSink{}
	ep := spendEndpoint(t, &scriptedModel{}, 100, blockingLedger{}, SpendOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	err := ep.Run(ctx, Alert{RoomID: "room-a", Title: "t", PipelineID: "orders"}, Deps{
		Tools: tools.NewRegistry(), Inbox: &fakeInbox{waitErr: context.Canceled}, Sink: sink, Logger: discardLogger(),
	}, Options{})
	if err != nil {
		t.Fatalf("Run = %v, want a clean stop when the room closes", err)
	}
	if n := len(sink.ofType(events.AgentMessage)); n != 0 {
		t.Fatalf("messages = %d, want none: a closing room is not a store outage", n)
	}
	if n := len(sink.ofType(events.AgentUsage)); n != 0 {
		t.Fatalf("usage events = %d for a request that was not sent", n)
	}
}

// clockModel moves the clock while a request is in flight.
type clockModel struct {
	scriptedModel
	clock *spendClock
	to    time.Time
}

func (m *clockModel) Turn(ctx context.Context, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	if !m.to.IsZero() {
		m.clock.set(m.to)
	}
	return m.scriptedModel.Turn(ctx, p)
}

func TestSpendDailyBudgetRollsOverAtUTCMidnight(t *testing.T) {
	st := spendRooms(t, "room-a", "room-b", "room-c")
	beforeMidnight := time.Date(2026, 10, 13, 23, 59, 59, 0, time.UTC)
	afterMidnight := time.Date(2026, 10, 14, 0, 0, 1, 0, time.UTC)
	clock := &spendClock{t: beforeMidnight}
	turn := mustMessage(t, endTurn110)
	m := &clockModel{scriptedModel: scriptedModel{turns: []anthropic.BetaMessage{turn, turn}}, clock: clock, to: afterMidnight}
	ep := spendEndpoint(t, m, 0, storeLedger{st}, SpendOptions{PipelineDailyTokens: 100, Now: clock.now})

	// Admitted on the 13th, finished on the 14th: it counts on the 13th.
	if err := endpointRun(t, ep, "room-a", &fakeSink{}); err != nil {
		t.Fatalf("room-a: %v", err)
	}
	day := func(d string) int64 {
		rep, err := st.PipelineSpend(context.Background(), "orders", d)
		if err != nil {
			t.Fatal(err)
		}
		return rep.ChargedTokens
	}
	if day("2026-10-13") != 110 || day("2026-10-14") != 0 {
		t.Fatalf("13th = %d, 14th = %d: want the request on the day it was admitted", day("2026-10-13"), day("2026-10-14"))
	}

	// Still the 13th: the pipeline's day is spent, whatever the room.
	clock.set(beforeMidnight)
	m.to = time.Time{}
	sink := &fakeSink{}
	err := endpointRun(t, ep, "room-b", sink)
	be, ok := asBudgetError(err)
	const want = `pipeline daily token budget reached for "orders" on 2026-10-13 UTC (110 of 100); a person must raise PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET or continue manually`
	if !ok || be.Error() != want {
		t.Fatalf("room-b = %v, want %q", err, want)
	}
	if len(m.calls) != 1 {
		t.Fatalf("model calls = %d, want room-b refused", len(m.calls))
	}

	// Midnight UTC: a new day, a new budget.
	clock.set(time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC))
	if err := endpointRun(t, ep, "room-c", &fakeSink{}); err != nil {
		t.Fatalf("room-c after midnight: %v", err)
	}
	if len(m.calls) != 2 || day("2026-10-14") != 110 {
		t.Fatalf("calls = %d, 14th = %d: want room-c admitted on the new day", len(m.calls), day("2026-10-14"))
	}
}

func TestSpendDailyCostBudget(t *testing.T) {
	st := spendRooms(t, "room-a", "room-b")
	turn := mustMessage(t, endTurn110)
	scripted := &scriptedModel{turns: []anthropic.BetaMessage{turn, turn}}
	clock := &spendClock{t: time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)}
	// 110 tokens at 10 USD per million = 1100 millionths.
	ep := spendEndpoint(t, scripted, 0, storeLedger{st}, SpendOptions{
		PipelineDailyCostUSD: 0.001, Now: clock.now,
		Prices: Prices{"claude-opus-5-5": {Input: 10, Output: 10, CacheWrite: 10, CacheRead: 10}},
	})
	if err := endpointRun(t, ep, "room-a", &fakeSink{}); err != nil {
		t.Fatalf("room-a: %v", err)
	}
	err := endpointRun(t, ep, "room-b", &fakeSink{})
	be, ok := asBudgetError(err)
	const want = `pipeline daily cost budget reached for "orders" on 2026-10-13 UTC ($0.0011 of $0.001); a person must raise PIPEROOM_PIPELINE_DAILY_COST_USD or continue manually`
	if !ok || be.Error() != want || be.Used != 1100 || be.Limit != 1000 {
		t.Fatalf("room-b = %v (%+v), want %q", err, be, want)
	}
	if len(scripted.calls) != 1 {
		t.Fatalf("model calls = %d, want room-b refused", len(scripted.calls))
	}
	rep, err := st.PipelineSpend(context.Background(), "orders", "2026-10-13")
	if err != nil {
		t.Fatal(err)
	}
	if rep.ChargedCostMicros != 1100 || rep.Totals().CostMicros != 1100 || rep.Totals().UnpricedCalls != 0 {
		t.Fatalf("pipeline spend = %+v", rep)
	}
}

func TestSpendHandoffCountsOnTheRoomsPipeline(t *testing.T) {
	st := spendRooms(t, "room-h")
	summary := `{\"what_broke\":\"x\",\"why\":\"y\",\"done\":[],\"ruled_out\":[],\"open\":[],\"risks\":[]}`
	h := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"`+summary+`"}],"usage":{"input_tokens":40,"output_tokens":10}}`)
	day := time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	// The room is over its cap: the hand-off is still written and counted.
	ep := spendEndpoint(t, &scriptedModel{turns: []anthropic.BetaMessage{h}}, 1, storeLedger{st}, SpendOptions{Now: func() time.Time { return day }})
	created, _ := json.Marshal(events.RoomCreatedPayload{Title: "t", PipelineID: "orders"})
	evs := []events.Event{
		{RoomID: "room-h", Seq: 1, Type: events.RoomCreated, Payload: created},
		{RoomID: "room-h", Seq: 2, Type: events.AgentMessage, Payload: json.RawMessage(`{"text":"hi"}`)},
	}
	if _, err := ep.WriteHandoff(context.Background(), evs, Options{}); err != nil {
		t.Fatalf("WriteHandoff: %v", err)
	}
	rep, err := st.PipelineSpend(context.Background(), "orders", "2026-10-13")
	if err != nil {
		t.Fatal(err)
	}
	if rep.ChargedTokens != 50 || rep.Totals().Calls != 1 {
		t.Fatalf("pipeline spend = %+v, want the hand-off's 50 tokens on orders", rep)
	}
	if room, _ := st.RoomSpend(context.Background(), "room-h"); room.ChargedTokens != 50 {
		t.Fatalf("room spend = %+v, want 50", room)
	}
}

func TestSpendCapsRefuseAtExactlyTheLimit(t *testing.T) {
	// One run spends 110 tokens (1100 millionths at 10 USD per million); only
	// "total >= cap" refuses the restarted endpoint's first request.
	prices := Prices{"claude-opus-5-5": {Input: 10, Output: 10, CacheWrite: 10, CacheRead: 10}}
	day := func() time.Time { return time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC) }
	for name, c := range map[string]struct {
		roomCap int64
		o       SpendOptions
	}{
		// The restarted endpoint's first request is estimated at the floor
		// (1 token, 10 millionths), so the totals land exactly on these caps.
		"room":            {roomCap: 111, o: SpendOptions{Now: day}},
		"pipeline tokens": {o: SpendOptions{Now: day, PipelineDailyTokens: 111}},
		"pipeline cost":   {o: SpendOptions{Now: day, PipelineDailyCostUSD: 0.00111, Prices: prices}},
	} {
		t.Run(name, func(t *testing.T) {
			st := spendRooms(t, "room-a")
			turn := mustMessage(t, endTurn110)
			first := spendEndpoint(t, &scriptedModel{turns: []anthropic.BetaMessage{turn}}, c.roomCap, storeLedger{st}, c.o)
			if err := endpointRun(t, first, "room-a", &fakeSink{}); err != nil {
				t.Fatalf("first run: %v", err)
			}
			again := &scriptedModel{}
			err := endpointRun(t, spendEndpoint(t, again, c.roomCap, storeLedger{st}, c.o), "room-a", &fakeSink{})
			if _, ok := asBudgetError(err); !ok || len(again.calls) != 0 {
				t.Fatalf("at exactly the limit: Run = %v, calls = %d; want the request refused", err, len(again.calls))
			}
		})
	}
}

func TestSpendUnpricedModelUnderACostCapIsCountedAsUnpriced(t *testing.T) {
	st := spendRooms(t, "room-a")
	var logs bytes.Buffer
	ep := spendEndpoint(t, &scriptedModel{turns: []anthropic.BetaMessage{mustMessage(t, endTurn110)}}, 0, storeLedger{st}, SpendOptions{
		PipelineDailyCostUSD: 1, Prices: Prices{"other": {Input: 1}},
	})
	ep.log = slog.New(slog.NewTextHandler(&logs, nil))
	if err := endpointRun(t, ep, "room-a", &fakeSink{}); err != nil {
		t.Fatal(err)
	}
	rep, err := st.RoomSpend(context.Background(), "room-a")
	if err != nil {
		t.Fatal(err)
	}
	if u := rep.Totals(); u.Calls != 1 || u.UnpricedCalls != 1 || u.CostMicros != 0 {
		t.Fatalf("room spend = %+v, want one unpriced call", rep)
	}
	if !strings.Contains(logs.String(), "has no price") {
		t.Fatalf("logs = %q, want a warning about the unpriced model", logs.String())
	}
}

func TestSetSpendRejectsBadSettings(t *testing.T) {
	st := spendRooms(t)
	bad := map[string]SpendOptions{
		"negative tokens":      {Ledger: storeLedger{st}, PipelineDailyTokens: -1},
		"negative cost":        {Ledger: storeLedger{st}, PipelineDailyCostUSD: -1, Prices: Prices{"m": {}}},
		"daily cap, no ledger": {PipelineDailyTokens: 10},
		"cost cap, no prices":  {Ledger: storeLedger{st}, PipelineDailyCostUSD: 1},
	}
	for name, o := range bad {
		if err := testEndpoint(&scriptedModel{}, 0).SetSpend(o); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := testEndpoint(&scriptedModel{}, 0).SetSpend(SpendOptions{Prices: Prices{"m": {}}}); err != nil {
		t.Errorf("prices alone: %v", err)
	}
}

func TestUSDText(t *testing.T) {
	for micros, want := range map[int64]string{50_000_000: "50.00", 1100: "0.0011", 1000: "0.001", 12_500_000: "12.50", 0: "0.00", 1: "0.000001"} {
		if got := usdText(micros); got != want {
			t.Errorf("usdText(%d) = %q, want %q", micros, got, want)
		}
	}
}
