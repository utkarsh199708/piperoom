//go:build integration

// Package store_test integration tests run against the dev Postgres
// (dev/docker-compose.yml). They apply migrations/ themselves into a fresh
// it_<random> schema (pinned via search_path) and drop it in t.Cleanup, so no
// manual goose up is required. They skip (never fail) when the room database
// is unreachable.
package store_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
	"piperoom/migrations"
)

// requireStore applies the migrations into a fresh schema and returns a store
// pinned to it. The schema is dropped on cleanup.
func requireStore(t *testing.T) (*store.PG, string) {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		t.Skipf("store: room Postgres unreachable at %s: %v", cfg.RoomDBURL, err)
	}
	defer conn.Close(ctx)

	schema := testenv.Name("schema")
	if _, err := conn.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Skipf("store: cannot create schema: %v", err)
	}
	if _, err := conn.Exec(ctx, `SET search_path TO `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatalf("store: set search_path: %v", err)
	}
	for _, file := range []string{"00001_init.sql", "00002_handoff_and_steps.sql"} {
		if err := applyUp(ctx, conn, file); err != nil {
			t.Skipf("store: cannot apply %s: %v", file, err)
		}
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatalf("store: parse pool config: %v", err)
	}
	poolCfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("store: build pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("store: ping pool: %v", err)
	}
	s := store.NewPool(pool, cfg.RoomDBURL, slog.Default())
	t.Cleanup(func() {
		s.Close()
		dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dcancel()
		if c, err := pgx.Connect(dctx, cfg.RoomDBURL); err == nil {
			_, _ = c.Exec(dctx, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
			_ = c.Close(dctx)
		}
	})
	return s, cfg.RoomDBURL
}

// applyUp runs the "Up" section of a goose migration file.
func applyUp(ctx context.Context, conn *pgx.Conn, filename string) error {
	data, err := migrations.FS.ReadFile(filename)
	if err != nil {
		return err
	}
	sql := string(data)
	if i := strings.Index(sql, "-- +goose Down"); i >= 0 {
		sql = sql[:i]
	}
	if i := strings.Index(sql, "-- +goose Up"); i >= 0 {
		sql = sql[i+len("-- +goose Up"):]
	}
	for _, stmt := range strings.Split(sql, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("statement %q: %w", truncate(stmt, 60), err)
		}
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func createTestRoom(t *testing.T, s *store.PG) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	roomID := testenv.Name("store")
	if err := s.CreateRoom(ctx, store.Room{
		ID: roomID, Title: "integration", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("store: create room: %v", err)
	}
	return roomID
}

func commentActor() events.Actor {
	return events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
}

func appendComment(ctx context.Context, t *testing.T, s *store.PG, roomID string, i int) {
	t.Helper()
	_, err := s.Append(ctx, roomID, events.CommentAdded, commentActor(), events.CommentAddedPayload{
		CommentID: fmt.Sprintf("c_%d", i), Text: "hello",
	})
	if err != nil {
		t.Fatalf("store: append: %v", err)
	}
}

func TestAppendConcurrentNoGaps(t *testing.T) {
	s, _ := requireStore(t)
	roomID := createTestRoom(t, s)

	const n = 20
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.Append(ctx, roomID, events.CommentAdded, commentActor(), events.CommentAddedPayload{
				CommentID: fmt.Sprintf("c_%d", i), Text: "hello",
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	evs, err := s.Since(ctx, roomID, 0, 1000)
	if err != nil {
		t.Fatalf("store: since: %v", err)
	}
	if len(evs) != n {
		t.Fatalf("want %d events, got %d", n, len(evs))
	}
	for i, e := range evs {
		if e.Seq != int64(i+1) {
			t.Fatalf("seq gap at index %d: got %d, want %d", i, e.Seq, i+1)
		}
	}
}

func TestSubscriberSeesEveryEventExactlyOnce(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	roomID := createTestRoom(t, s)

	// Five events exist before the subscriber connects.
	for i := 1; i <= 5; i++ {
		appendComment(ctx, t, s, roomID, i)
	}

	// Connect mid-stream: replay must deliver 3,4,5, then live 6..10.
	ch, err := s.Subscribe(ctx, roomID, 2)
	if err != nil {
		t.Fatalf("store: subscribe: %v", err)
	}

	for i := 6; i <= 10; i++ {
		appendComment(ctx, t, s, roomID, i)
	}

	deadline := time.After(10 * time.Second)
	var got []int64
	for len(got) < 8 {
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatalf("store: subscriber channel closed early (got %v)", got)
			}
			got = append(got, e.Seq)
		case <-deadline:
			t.Fatalf("store: timed out waiting for events (got %v)", got)
		}
	}

	for i, seq := range got {
		if want := int64(i + 3); seq != want {
			t.Fatalf("store: subscriber event %d: got seq %d, want %d (no skip/repeat)", i, seq, want)
		}
	}
}

// TestListenerReconnects kills the hub's LISTEN backend and verifies the hub
// reconnects and the subscriber still sees every event exactly once.
func TestListenerReconnects(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	roomID := createTestRoom(t, s)

	ch, err := s.Subscribe(ctx, roomID, 0)
	if err != nil {
		t.Fatalf("store: subscribe: %v", err)
	}

	for i := 1; i <= 3; i++ {
		appendComment(ctx, t, s, roomID, i)
	}
	var got []int64
	for len(got) < 3 {
		select {
		case e := <-ch:
			got = append(got, e.Seq)
		case <-time.After(10 * time.Second):
			t.Fatalf("store: timed out waiting for first 3 (got %v)", got)
		}
	}

	terminateListen(t)
	waitForListen(t)

	for i := 4; i <= 6; i++ {
		appendComment(ctx, t, s, roomID, i)
	}
	for len(got) < 6 {
		select {
		case e := <-ch:
			got = append(got, e.Seq)
		case <-time.After(10 * time.Second):
			t.Fatalf("store: timed out waiting for 4..6 (got %v)", got)
		}
	}

	for i, seq := range got {
		if want := int64(i + 1); seq != want {
			t.Fatalf("store: event %d: got seq %d, want %d (no skip/repeat)", i, seq, want)
		}
	}
}

func terminateListen(t *testing.T) {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		t.Fatalf("store: connect to terminate listen: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'piperoom-listen'`); err != nil {
		t.Fatalf("store: terminate listen backend: %v", err)
	}
}

func waitForListen(t *testing.T) {
	t.Helper()
	cfg := config.Load()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
		if err == nil {
			var n int
			err = conn.QueryRow(ctx,
				`SELECT count(*) FROM pg_stat_activity WHERE application_name = 'piperoom-listen'`).Scan(&n)
			_ = conn.Close(ctx)
			if err == nil && n > 0 {
				cancel()
				return
			}
		}
		cancel()
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("store: LISTEN connection did not reconnect")
}
