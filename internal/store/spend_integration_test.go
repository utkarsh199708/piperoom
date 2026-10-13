//go:build integration

package store_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/config"
	"piperoom/internal/store"
)

// reopenStore opens a second store on the same test schema, as a restarted
// server would.
func reopenStore(t *testing.T, schema string) *store.PG {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	poolCfg, err := pgxpool.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	poolCfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatal(err)
	}
	s := store.NewPool(pool, cfg.RoomDBURL, slog.Default())
	t.Cleanup(s.Close)
	return s
}

func spendCall(model string, in, out, cost int64, priced bool) *store.SpendCall {
	return &store.SpendCall{Model: model, InputTokens: in, OutputTokens: out, CacheCreationInputTokens: 1, CacheReadInputTokens: 2, CostMicros: cost, Priced: priced}
}

func TestSpendPersistsAcrossAReopen(t *testing.T) {
	s, _, schema := requireStoreSchema(t)
	ctx := context.Background()
	room := createTestRoom(t, s)
	if _, err := s.AddSpend(ctx, store.SpendChange{RoomID: room, PipelineID: "orders", Day: "2026-10-13", Tokens: 60}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddSpend(ctx, store.SpendChange{RoomID: room, PipelineID: "orders", Day: "2026-10-13", Tokens: 13, CostMicros: 5,
		Call: spendCall("b", 60, 10, 5, true)}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2 := reopenStore(t, schema)
	rep, err := s2.RoomSpend(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ChargedTokens != 73 || rep.ChargedCostMicros != 5 || len(rep.ByModel) != 1 || rep.ByModel[0].Tokens() != 73 || rep.ByModel[0].Calls != 1 {
		t.Fatalf("room spend after reopen = %+v", rep)
	}
	tot, err := s2.AddSpend(ctx, store.SpendChange{RoomID: room, PipelineID: "orders", Day: "2026-10-13", Tokens: 7})
	if err != nil {
		t.Fatal(err)
	}
	if tot != (store.SpendTotals{RoomTokens: 80, RoomCostMicros: 5, PipelineTokens: 80, PipelineCostMicros: 5}) {
		t.Fatalf("totals after reopen = %+v, want the old totals carried on", tot)
	}
}

func TestSpendConcurrentChangesLoseNothing(t *testing.T) {
	s, _ := requireStore(t)
	ctx := context.Background()
	rooms := []string{createTestRoom(t, s), createTestRoom(t, s), createTestRoom(t, s), createTestRoom(t, s)}
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Estimates and settles mixed, as concurrent runs send them.
			c := store.SpendChange{RoomID: rooms[i%4], PipelineID: "orders", Day: "2026-10-13", Tokens: 3, CostMicros: 2}
			if (i/4)%2 == 0 { // five of each room's ten
				c.Call = spendCall("m", 1, 0, 2, true)
			}
			if _, err := s.AddSpend(ctx, c); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	p, err := s.PipelineSpend(ctx, "orders", "2026-10-13")
	if err != nil {
		t.Fatal(err)
	}
	if p.ChargedTokens != 120 || p.ChargedCostMicros != 80 || p.Totals().Calls != 20 || p.Totals().CostMicros != 40 {
		t.Fatalf("pipeline spend = %+v, want 120 tokens, 80 cost, 20 calls: an update was lost", p)
	}
	for _, r := range rooms {
		rep, err := s.RoomSpend(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if rep.ChargedTokens != 30 || rep.Totals().Calls != 5 {
			t.Fatalf("room %s = %+v, want 30 tokens over 5 calls", r, rep)
		}
	}
}

func TestSpendBreakdownDaysAndErrors(t *testing.T) {
	s, _ := requireStore(t)
	ctx := context.Background()
	room := createTestRoom(t, s)
	changes := []store.SpendChange{
		{RoomID: room, PipelineID: "orders", Day: "2026-10-13", Tokens: 73, CostMicros: 9, Call: spendCall("alpha", 60, 10, 9, true)},
		{RoomID: room, PipelineID: "orders", Day: "2026-10-13", Tokens: 23, Call: spendCall("Zeta", 10, 10, 77, false)},
		{RoomID: room, PipelineID: "orders", Day: "2026-10-14", Tokens: 23, Call: spendCall("alpha", 10, 10, 1, true)},
	}
	for _, c := range changes {
		if _, err := s.AddSpend(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	day, err := s.PipelineSpend(ctx, "orders", "2026-10-13")
	if err != nil {
		t.Fatal(err)
	}
	// Byte order, like memstore: "Zeta" before "alpha" (a collation would put
	// alpha first). The unpriced call adds no cost.
	if day.ChargedTokens != 96 || len(day.ByModel) != 2 || day.ByModel[0].Model != "Zeta" ||
		day.ByModel[0].UnpricedCalls != 1 || day.ByModel[0].CostMicros != 0 || day.ByModel[1].CostMicros != 9 {
		t.Fatalf("2026-10-13 = %+v", day)
	}
	if next, _ := s.PipelineSpend(ctx, "orders", "2026-10-14"); next.ChargedTokens != 23 {
		t.Fatalf("2026-10-14 = %+v", next)
	}
	if empty, err := s.PipelineSpend(ctx, "nope", "2026-10-13"); err != nil || empty.ChargedTokens != 0 || empty.ByModel == nil {
		t.Fatalf("unknown pipeline = %+v, %v; want zeros", empty, err)
	}
	room2, _ := s.RoomSpend(ctx, room)
	if room2.ChargedTokens != 119 || room2.Totals().Calls != 3 {
		t.Fatalf("room = %+v", room2)
	}

	// An unknown room fails on the foreign key and rolls the pipeline row back.
	if _, err := s.AddSpend(ctx, store.SpendChange{RoomID: "r_missing", PipelineID: "ghost", Day: "2026-10-13", Tokens: 5}); err == nil {
		t.Fatal("unknown room accepted")
	}
	if ghost, _ := s.PipelineSpend(ctx, "ghost", "2026-10-13"); ghost.ChargedTokens != 0 {
		t.Fatalf("a failed change left a pipeline row: %+v", ghost)
	}
	if _, err := s.AddSpend(ctx, store.SpendChange{RoomID: room, Day: "2026-13-01"}); err == nil {
		t.Fatal("bad day accepted")
	}
	if _, err := s.PipelineSpend(ctx, "orders", "13-10-2026"); err == nil {
		t.Fatal("PipelineSpend accepted a bad day")
	}
}

func TestPurgeTakesRoomSpendAndKeepsThePipelineDay(t *testing.T) {
	s, _ := requireStore(t)
	ctx := context.Background()
	room := createTestRoom(t, s)
	if _, err := s.AddSpend(ctx, store.SpendChange{RoomID: room, PipelineID: "orders", Day: "2026-01-01", Tokens: 9, Call: spendCall("m", 6, 0, 0, false)}); err != nil {
		t.Fatal(err)
	}
	closed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := s.CloseRoom(ctx, room, closed); err != nil {
		t.Fatal(err)
	}
	res, err := s.PurgeClosedBefore(ctx, closed.Add(time.Hour), 10, nil)
	if err != nil || len(res.RoomIDs) != 1 {
		t.Fatalf("purge = %+v, %v", res, err)
	}
	if rep, _ := s.RoomSpend(ctx, room); rep.ChargedTokens != 0 || len(rep.ByModel) != 0 {
		t.Fatalf("purged room still has spend: %+v", rep)
	}
	if rep, _ := s.PipelineSpend(ctx, "orders", "2026-01-01"); rep.ChargedTokens != 9 || len(rep.ByModel) != 1 {
		t.Fatalf("pipeline day after the purge = %+v, want it kept", rep)
	}
}
