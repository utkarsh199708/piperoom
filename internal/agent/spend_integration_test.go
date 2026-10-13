//go:build integration

package agent

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"piperoom/internal/config"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
	"piperoom/migrations"
)

// pgSpendSchema applies the migrations into a fresh it_ schema of the room
// database and returns a function that opens a store on it (a "process"). It
// skips when the database is unreachable.
func pgSpendSchema(t *testing.T) func() *store.PG {
	t.Helper()
	url := config.Load().RoomDBURL
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Skipf("room Postgres unreachable at %s: %v", url, err)
	}
	defer conn.Close(ctx)
	schema := testenv.Name("schema")
	if _, err := conn.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Skipf("cannot create schema: %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dcancel()
		if c, err := pgx.Connect(dctx, url); err == nil {
			_, _ = c.Exec(dctx, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
			_ = c.Close(dctx)
		}
	})
	sqlCfg, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	sqlCfg.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*sqlCfg)
	if _, err := migrations.Up(ctx, db); err != nil {
		_ = db.Close()
		t.Fatalf("migrations: %v", err)
	}
	_ = db.Close()

	return func() *store.PG {
		t.Helper()
		poolCfg, err := pgxpool.ParseConfig(url)
		if err != nil {
			t.Fatal(err)
		}
		poolCfg.ConnConfig.RuntimeParams["search_path"] = schema
		pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
		if err != nil {
			t.Fatal(err)
		}
		s := store.NewPool(pool, url, slog.New(slog.DiscardHandler))
		t.Cleanup(s.Close)
		return s
	}
}

func pgRooms(t *testing.T, s *store.PG, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := s.CreateRoom(context.Background(), store.Room{ID: id, Title: "t", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSpendBudgetPersistsAcrossARestartPostgres(t *testing.T) {
	open := pgSpendSchema(t)
	first := open()
	room := testenv.Name("room")
	pgRooms(t, first, room)
	turn := mustMessage(t, endTurn110)
	m1 := &scriptedModel{turns: []anthropic.BetaMessage{turn}}
	if err := endpointRun(t, spendEndpoint(t, m1, 100, storeLedger{first}, SpendOptions{}), room, &fakeSink{}); err != nil {
		t.Fatalf("first run: %v", err)
	}
	first.Close() // the process stops

	second := open()
	m2 := &scriptedModel{turns: []anthropic.BetaMessage{turn}}
	err := endpointRun(t, spendEndpoint(t, m2, 100, storeLedger{second}, SpendOptions{}), room, &fakeSink{})
	be, ok := asBudgetError(err)
	if !ok || be.Used != 110 || be.Limit != 100 {
		t.Fatalf("run after the restart = %v, want 110 of 100 from the database", err)
	}
	if len(m2.calls) != 0 {
		t.Fatalf("the restarted endpoint called the model %d times", len(m2.calls))
	}
}

func TestSpendConcurrentAdmitsOvershootByAtMostOneCallPostgres(t *testing.T) {
	const (
		g        = 20
		perCall  = 100
		capLimit = 1000
	)
	s := pgSpendSchema(t)()
	rooms := make([]string, g)
	for i := range rooms {
		rooms[i] = testenv.Name("room")
	}
	pgRooms(t, s, rooms...)
	day := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	m := &barrierModel{g: g, all: make(chan struct{}), turn: mustMessage(t, `{"stop_reason":"end_turn","content":[],"usage":{"input_tokens":100}}`)}
	ep := spendEndpoint(t, m, 0, storeLedger{s}, SpendOptions{PipelineDailyTokens: capLimit, Now: func() time.Time { return day }})
	var wg sync.WaitGroup
	var refused atomic.Int64
	for _, id := range rooms {
		ep.budget(id).record(usageTokens{input: perCall}, true)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			bm := &budgetModel{inner: m, budget: ep.budget(id), loop: true, guard: ep.guard(id, "orders")}
			if _, err := bm.Turn(context.Background(), anthropic.BetaMessageNewParams{Model: "m"}); err != nil {
				if _, ok := asBudgetError(err); !ok {
					t.Errorf("Turn: %v", err)
				}
				refused.Add(1)
			}
		}(id)
	}
	wg.Wait()
	spent := m.calls.Load() * perCall
	if spent > capLimit+perCall || refused.Load() == 0 {
		t.Fatalf("%d concurrent requests spent %d against a cap of %d (refused %d)", g, spent, capLimit, refused.Load())
	}
	p, err := s.PipelineSpend(context.Background(), "orders", "2026-10-13")
	if err != nil {
		t.Fatal(err)
	}
	if p.ChargedTokens != spent || p.Totals().Tokens() != spent {
		t.Fatalf("pipeline spend = %+v, want exactly the %d spent", p, spent)
	}
}
