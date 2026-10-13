package memstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"piperoom/internal/store"
)

func spendStore(t *testing.T, rooms ...string) *Store {
	t.Helper()
	s := New()
	for _, id := range rooms {
		if err := s.CreateRoom(context.Background(), store.Room{ID: id, Title: "t", PipelineID: "orders", Status: "open"}); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func TestAddSpendTotalsAndBreakdowns(t *testing.T) {
	ctx := context.Background()
	s := spendStore(t, "r1", "r2")
	call := func(model string, in, out, cost int64, priced bool) *store.SpendCall {
		return &store.SpendCall{Model: model, InputTokens: in, OutputTokens: out, CacheReadInputTokens: 1, CostMicros: cost, Priced: priced}
	}
	steps := []store.SpendChange{
		{RoomID: "r1", PipelineID: "orders", Day: "2026-10-13", Tokens: 50},                                                  // estimate
		{RoomID: "r1", PipelineID: "orders", Day: "2026-10-13", Tokens: 11, CostMicros: 7, Call: call("b", 50, 10, 7, true)}, // settle
		{RoomID: "r2", PipelineID: "orders", Day: "2026-10-13", Tokens: 21, Call: call("a", 10, 10, 99, false)},
		{RoomID: "r1", PipelineID: "orders", Day: "2026-10-14", Tokens: 5, Call: call("b", 2, 2, 3, true)},
	}
	var last store.SpendTotals
	for i, c := range steps {
		var err error
		if last, err = s.AddSpend(ctx, c); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	if last != (store.SpendTotals{RoomTokens: 66, RoomCostMicros: 7, PipelineTokens: 5, PipelineCostMicros: 0}) {
		t.Fatalf("totals after the last change = %+v", last)
	}

	r1, err := s.RoomSpend(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if r1.ChargedTokens != 66 || len(r1.ByModel) != 1 || r1.ByModel[0].Calls != 2 || r1.ByModel[0].Tokens() != 66 || r1.ByModel[0].CostMicros != 10 {
		t.Fatalf("r1 = %+v", r1)
	}
	day, err := s.PipelineSpend(ctx, "orders", "2026-10-13")
	if err != nil {
		t.Fatal(err)
	}
	// Sorted by model; the unpriced call adds no cost and counts as unpriced.
	if day.ChargedTokens != 82 || len(day.ByModel) != 2 || day.ByModel[0].Model != "a" || day.ByModel[0].UnpricedCalls != 1 || day.ByModel[0].CostMicros != 0 {
		t.Fatalf("pipeline day = %+v", day)
	}
	if tot := day.Totals(); tot.Calls != 2 || tot.UnpricedCalls != 1 || tot.Tokens() != 82 {
		t.Fatalf("pipeline day totals = %+v", tot)
	}
	empty, err := s.PipelineSpend(ctx, "orders", "2026-10-15")
	if err != nil || empty.ChargedTokens != 0 || empty.ByModel == nil || len(empty.ByModel) != 0 {
		t.Fatalf("empty day = %+v, %v; want zeros and an empty breakdown", empty, err)
	}
	// The returned rows are copies.
	r1.ByModel[0].Calls = 99
	if again, _ := s.RoomSpend(ctx, "r1"); again.ByModel[0].Calls != 2 {
		t.Fatal("RoomSpend returned the stored row")
	}
}

func TestAddSpendRefusesBadChanges(t *testing.T) {
	ctx := context.Background()
	s := spendStore(t, "r1")
	if _, err := s.AddSpend(ctx, store.SpendChange{RoomID: "nope", Day: "2026-10-13", Tokens: 1}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown room: %v, want ErrNotFound (the Postgres foreign key)", err)
	}
	for name, c := range map[string]store.SpendChange{
		"no room":         {Day: "2026-10-13"},
		"bad day":         {RoomID: "r1", Day: "13/10/2026"},
		"day with a time": {RoomID: "r1", Day: "2026-10-13T00:00:00Z"},
		"no model":        {RoomID: "r1", Day: "2026-10-13", Call: &store.SpendCall{}},
		"NUL in model":    {RoomID: "r1", Day: "2026-10-13", Call: &store.SpendCall{Model: "a\x00b"}},
	} {
		if _, err := s.AddSpend(ctx, c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := s.PipelineSpend(ctx, "orders", "yesterday"); err == nil {
		t.Error("PipelineSpend accepted a bad day")
	}
	if rep, _ := s.RoomSpend(ctx, "r1"); rep.ChargedTokens != 0 {
		t.Fatalf("a refused change was stored: %+v", rep)
	}
}

func TestAddSpendIsAtomicUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	s := spendStore(t, "r1")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.AddSpend(ctx, store.SpendChange{RoomID: "r1", PipelineID: "orders", Day: "2026-10-13", Tokens: 2,
				Call: &store.SpendCall{Model: "m", InputTokens: 2, Priced: true}}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	rep, _ := s.RoomSpend(ctx, "r1")
	if rep.ChargedTokens != 100 || rep.ByModel[0].Calls != 50 {
		t.Fatalf("room spend = %+v, want 100 tokens over 50 calls", rep)
	}
}

func TestPurgeDropsRoomSpendAndKeepsThePipelineDay(t *testing.T) {
	ctx := context.Background()
	closed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := spendStore(t, "old")
	if _, err := s.AddSpend(ctx, store.SpendChange{RoomID: "old", PipelineID: "orders", Day: "2026-01-01", Tokens: 9,
		Call: &store.SpendCall{Model: "m", InputTokens: 9}}); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseRoom(ctx, "old", closed); err != nil {
		t.Fatal(err)
	}
	res, err := s.PurgeClosedBefore(ctx, closed.Add(time.Hour), 10, nil)
	if err != nil || len(res.RoomIDs) != 1 {
		t.Fatalf("purge = %+v, %v", res, err)
	}
	if rep, _ := s.RoomSpend(ctx, "old"); rep.ChargedTokens != 0 || len(rep.ByModel) != 0 {
		t.Fatalf("purged room still has spend: %+v", rep)
	}
	if rep, _ := s.PipelineSpend(ctx, "orders", "2026-01-01"); rep.ChargedTokens != 9 || len(rep.ByModel) != 1 {
		t.Fatalf("pipeline day after the purge = %+v, want it kept", rep)
	}
}
