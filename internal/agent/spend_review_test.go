package agent

// Tests from the review of PR #117 (LEAD-REVIEW probes, kept and extended):
// guards that no earlier test caught.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

func inputTurn(t *testing.T, input int64) anthropic.BetaMessage {
	t.Helper()
	return mustMessage(t, `{"stop_reason":"end_turn","content":[],"usage":{"input_tokens":`+strconv.FormatInt(input, 10)+`}}`)
}

// clockLedger moves the clock after the first change, so a refund lands after
// midnight.
type clockLedger struct {
	st    store.SpendStore
	clock *spendClock
	to    time.Time
	n     atomic.Int64
}

func (l *clockLedger) AddSpend(ctx context.Context, c SpendChange) (SpendTotals, error) {
	t, err := storeLedger{l.st}.AddSpend(ctx, c)
	if l.n.Add(1) == 1 {
		l.clock.set(l.to)
	}
	return t, err
}

func TestSpendRefundGoesToTheDayItWasCharged(t *testing.T) {
	st := spendRooms(t, "room-a", "base")
	if _, err := st.AddSpend(context.Background(), store.SpendChange{RoomID: "base", PipelineID: "orders", Day: "2026-10-13", Tokens: 950}); err != nil {
		t.Fatal(err)
	}
	clock := &spendClock{t: time.Date(2026, 10, 13, 23, 59, 59, 0, time.UTC)}
	l := &clockLedger{st: st, clock: clock, to: time.Date(2026, 10, 14, 0, 0, 1, 0, time.UTC)}
	ep := spendEndpoint(t, &scriptedModel{}, 0, l, SpendOptions{PipelineDailyTokens: 1000, Now: clock.now})
	ep.budget("room-a").record(usageTokens{input: 100}, true)
	bm := &budgetModel{inner: &scriptedModel{}, budget: ep.budget("room-a"), loop: true, guard: ep.guard("room-a", "orders")}
	if _, err := bm.Turn(context.Background(), anthropic.BetaMessageNewParams{Model: "m"}); err == nil {
		t.Fatal("want a refusal")
	}
	d13, _ := st.PipelineSpend(context.Background(), "orders", "2026-10-13")
	d14, _ := st.PipelineSpend(context.Background(), "orders", "2026-10-14")
	if d13.ChargedTokens != 950 || d14.ChargedTokens != 0 {
		t.Fatalf("13th %d, 14th %d: want the refund on the 13th (950, 0)", d13.ChargedTokens, d14.ChargedTokens)
	}
}

// cancelAfterChargeLedger cancels the run context right after the charge, so
// the refund of the refused request runs after the room closed.
type cancelAfterChargeLedger struct {
	st     store.SpendStore
	cancel context.CancelFunc
	n      atomic.Int64
}

func (l *cancelAfterChargeLedger) AddSpend(ctx context.Context, c SpendChange) (SpendTotals, error) {
	if err := ctx.Err(); err != nil {
		return SpendTotals{}, err // a store call on a dead context fails
	}
	t, err := storeLedger{l.st}.AddSpend(ctx, c)
	if l.n.Add(1) == 1 {
		l.cancel()
	}
	return t, err
}

func TestSpendRefundSurvivesARoomClose(t *testing.T) {
	st := spendRooms(t, "room-a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := &cancelAfterChargeLedger{st: st, cancel: cancel}
	ep := spendEndpoint(t, &scriptedModel{}, 50, l, SpendOptions{})
	ep.budget("room-a").record(usageTokens{input: 100}, true)
	bm := &budgetModel{inner: &scriptedModel{}, budget: ep.budget("room-a"), loop: true, guard: ep.guard("room-a", "orders")}
	if _, err := bm.Turn(ctx, anthropic.BetaMessageNewParams{Model: "m"}); err == nil {
		t.Fatal("want a refusal")
	}
	if rep, _ := st.RoomSpend(context.Background(), "room-a"); rep.ChargedTokens != 0 {
		t.Fatalf("room charged %d after a refused request, want the estimate taken back", rep.ChargedTokens)
	}
}

// cancelModel cancels the run while the request is in flight, as a room close
// does, and returns the billed usage.
type cancelModel struct {
	cancel context.CancelFunc
	turn   anthropic.BetaMessage
}

func (m cancelModel) Turn(context.Context, anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	m.cancel()
	return m.turn, context.Canceled
}

// liveLedger fails on a dead context, like a real store.
type liveLedger struct{ st store.SpendStore }

func (l liveLedger) AddSpend(ctx context.Context, c SpendChange) (SpendTotals, error) {
	if err := ctx.Err(); err != nil {
		return SpendTotals{}, err
	}
	return storeLedger{l.st}.AddSpend(ctx, c)
}

func TestSpendSettleSurvivesARoomClose(t *testing.T) {
	st := spendRooms(t, "room-a")
	ep := spendEndpoint(t, &scriptedModel{}, 1_000_000, liveLedger{st}, SpendOptions{})
	ep.budget("room-a").record(usageTokens{input: 100}, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bm := &budgetModel{inner: cancelModel{cancel: cancel, turn: inputTurn(t, 700)}, budget: ep.budget("room-a"), loop: true, guard: ep.guard("room-a", "orders")}
	_, _ = bm.Turn(ctx, anthropic.BetaMessageNewParams{Model: "m"})
	if rep, _ := st.RoomSpend(context.Background(), "room-a"); rep.ChargedTokens != 700 || rep.Totals().Calls != 1 {
		t.Fatalf("room spend = %+v, want the billed 700 settled", rep)
	}
}

func TestSpendSettleNetsTheEstimatedCost(t *testing.T) {
	st := spendRooms(t, "room-a")
	prices := Prices{"m": {Input: 5, Output: 25, CacheWrite: 6.25, CacheRead: 1}}
	ep := spendEndpoint(t, &scriptedModel{}, 0, storeLedger{st}, SpendOptions{Prices: prices})
	ep.budget("room-a").record(usageTokens{input: 1000}, true)
	bm := &budgetModel{inner: &scriptedModel{turns: []anthropic.BetaMessage{inputTurn(t, 1000)}}, budget: ep.budget("room-a"), loop: true, guard: ep.guard("room-a", "orders")}
	if _, err := bm.Turn(context.Background(), anthropic.BetaMessageNewParams{Model: "m"}); err != nil {
		t.Fatal(err)
	}
	rep, _ := st.RoomSpend(context.Background(), "room-a")
	if rep.ChargedCostMicros != rep.Totals().CostMicros || rep.ChargedCostMicros != 5000 {
		t.Fatalf("charged cost %d, finished cost %d: want both 5000", rep.ChargedCostMicros, rep.Totals().CostMicros)
	}
}

func TestSpendCostEstimateUsesTheCacheReadPrice(t *testing.T) {
	p := Prices{"m": {Input: 5, Output: 25, CacheWrite: 6.25, CacheRead: 0.5}}
	if got := p.estimateMicros("m", 1000); got != 500 {
		t.Fatalf("estimate = %d, want 500 (1000 tokens at the cache read price)", got)
	}
	if got := p.estimateMicros("other", 1000); got != 0 {
		t.Fatalf("unpriced estimate = %d, want 0", got)
	}
}

// rejectTriage rejects every triage request with a 400 about the request
// shape, so the turn is redone on the main model.
type rejectTriage struct {
	scriptedModel
	models []string
}

func (m *rejectTriage) Turn(ctx context.Context, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	m.models = append(m.models, p.Model)
	if p.Model == "tri" {
		return anthropic.BetaMessage{}, apiErr(400, `{"error":{"message":"thinking.type.adaptive is not supported for this model"}}`)
	}
	return m.scriptedModel.Turn(ctx, p)
}

func TestUsageRoleOfATriageRedoAfterA4xx(t *testing.T) {
	m := &rejectTriage{scriptedModel: scriptedModel{turns: []anthropic.BetaMessage{toolTurnN(t, 1)}}}
	sink := &fakeSink{}
	r := triageRunner(t, m, &fakeInbox{waitErr: context.Canceled}, sink, 6)
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := usageEvents(t, sink)
	if len(got) != len(m.models) || len(got) < 2 {
		t.Fatalf("events %d, requests %v", len(got), m.models)
	}
	for i, p := range got {
		want := events.UsageRoleMain
		if m.models[i] == "tri" {
			want = events.UsageRoleTriage
		}
		if p.Role != want || p.Model != m.models[i] {
			t.Fatalf("event %d = %+v, request model %q: the redo must be reported as main", i, p, m.models[i])
		}
	}
}

// flakySettleLedger fails the first failN settles and passes everything else.
type flakySettleLedger struct {
	st      store.SpendStore
	failN   int64
	settles atomic.Int64
}

func (l *flakySettleLedger) AddSpend(ctx context.Context, c SpendChange) (SpendTotals, error) {
	if c.Call != nil && l.settles.Add(1) <= l.failN {
		return SpendTotals{}, errors.New("connection reset")
	}
	return storeLedger{l.st}.AddSpend(ctx, c)
}

func TestSpendSettleRetriesOnceThenLogsAnError(t *testing.T) {
	run := func(t *testing.T, failN int64) (*memSpend, *flakySettleLedger, string) {
		st := spendRooms(t, "room-a")
		l := &flakySettleLedger{st: st, failN: failN}
		var logs bytes.Buffer
		ep := spendEndpoint(t, &scriptedModel{}, 1_000_000, l, SpendOptions{})
		ep.log = slog.New(slog.NewTextHandler(&logs, nil))
		ep.budget("room-a").record(usageTokens{input: 500}, true)
		bm := &budgetModel{inner: &scriptedModel{turns: []anthropic.BetaMessage{
			mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"SECRET-MODEL-TEXT"}],"usage":{"input_tokens":1000}}`),
		}}, budget: ep.budget("room-a"), loop: true, guard: ep.guard("room-a", "orders")}
		if _, err := bm.Turn(context.Background(), anthropic.BetaMessageNewParams{Model: "m"}); err != nil {
			t.Fatal(err)
		}
		rep, _ := st.RoomSpend(context.Background(), "room-a")
		return &memSpend{charged: rep.ChargedTokens, calls: rep.Totals().Calls}, l, logs.String()
	}
	t.Run("one failure is retried", func(t *testing.T) {
		got, l, logs := run(t, 1)
		if got.charged != 1000 || got.calls != 1 || l.settles.Load() != 2 {
			t.Fatalf("charged %d calls %d settles %d, want the retry to record 1000 in one call", got.charged, got.calls, l.settles.Load())
		}
		if strings.Contains(logs, "level=ERROR") {
			t.Fatalf("logs = %q, want no error after a good retry", logs)
		}
	})
	t.Run("two failures log an error with counts only", func(t *testing.T) {
		got, l, logs := run(t, 2)
		if got.charged != 500 || got.calls != 0 || l.settles.Load() != 2 {
			t.Fatalf("charged %d calls %d settles %d, want the estimate left and exactly one retry", got.charged, got.calls, l.settles.Load())
		}
		for _, want := range []string{"level=ERROR", "room=room-a", "pipeline=orders", "estimate=500", "actual=1000"} {
			if !strings.Contains(logs, want) {
				t.Fatalf("logs = %q, want %q", logs, want)
			}
		}
		if strings.Contains(logs, "SECRET-MODEL-TEXT") {
			t.Fatal("the log carries model text")
		}
	})
}

type memSpend struct{ charged, calls int64 }

func TestSpendFirstRequestsReserveTheEstimateFloor(t *testing.T) {
	// Three rooms send their first request at once against a daily cap of
	// 30 000. With the default floor of 20 000 the first charge leaves no room
	// for a second; with no floor all three would be admitted.
	rooms := []string{"room-a", "room-b", "room-c"}
	st := spendRooms(t, rooms...)
	m := &barrierModel{g: len(rooms), all: make(chan struct{}), turn: inputTurn(t, 100)}
	ep := testEndpoint(m, 0)
	day := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	if err := ep.SetSpend(SpendOptions{Ledger: storeLedger{st}, PipelineDailyTokens: 30_000, Now: func() time.Time { return day }}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, id := range rooms {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			bm := &budgetModel{inner: m, budget: ep.budget(id), loop: true, guard: ep.guard(id, "orders")}
			_, _ = bm.Turn(context.Background(), anthropic.BetaMessageNewParams{Model: "m"})
		}(id)
	}
	wg.Wait()
	if m.calls.Load() != 1 {
		t.Fatalf("admitted %d first requests, want 1: the floor must reserve %d tokens each", m.calls.Load(), DefaultEstimateFloor)
	}
	p, _ := st.PipelineSpend(context.Background(), "orders", "2026-10-13")
	if p.ChargedTokens != 100 {
		t.Fatalf("charged %d, want the settled 100 (the floor replaced by the actual usage)", p.ChargedTokens)
	}
	if err := testEndpoint(m, 0).SetSpend(SpendOptions{EstimateFloor: -1}); err == nil {
		t.Fatal("a negative floor was accepted")
	}
}

// liveSink refuses an event once its context has ended, like a store append.
type liveSink struct{ fakeSink }

func (s *liveSink) Emit(ctx context.Context, typ events.Type, payload any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.fakeSink.Emit(ctx, typ, payload)
}

func TestUsageEventOfARequestInFlightWhenTheRoomCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &liveSink{}
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector")}
	r := newTestRunner(t, cancelModel{cancel: cancel, turn: inputTurn(t, 700)}, &fakeInbox{waitErr: context.Canceled}, sink, rec, Options{Model: "main"})
	_ = r.run(ctx)
	got := usageEvents(t, &sink.fakeSink)
	if len(got) != 1 || got[0].InputTokens != 700 {
		t.Fatalf("usage events = %+v, want the billed request reported after the close", got)
	}

	// The hand-off reports on the same outliving context.
	var reported atomic.Int64
	hctx, hcancel := context.WithCancel(context.Background())
	defer hcancel()
	rm := &reportingModel{inner: cancelModel{cancel: hcancel, turn: inputTurn(t, 5)}, role: events.UsageRoleHandoff,
		report: func(ctx context.Context, _ events.AgentUsagePayload) {
			if ctx.Err() == nil {
				reported.Add(1)
			}
		}}
	_, _ = rm.Turn(hctx, anthropic.BetaMessageNewParams{Model: "m"})
	if reported.Load() != 1 {
		t.Fatal("the hand-off's usage was reported on a dead context")
	}
}

func TestParseModelPricesRefusesDuplicateKeys(t *testing.T) {
	for name, in := range map[string]string{
		"model twice": `{"m":{"input":1,"output":1,"cache_write":1,"cache_read":1},"m":{"input":9,"output":9,"cache_write":9,"cache_read":9}}`,
		"field twice": `{"m":{"input":1,"input":2,"output":1,"cache_write":1,"cache_read":1}}`,
	} {
		if _, err := ParseModelPrices([]byte(in)); err == nil || !strings.Contains(err.Error(), "twice") {
			t.Errorf("%s: err = %v, want a duplicate-key error", name, err)
		}
	}
}
