//go:build integration

// Package store_test integration tests run against the dev Postgres
// (dev/docker-compose.yml). They apply migrations/ with the same goose runner
// the server uses (migrations.Up on migrations.FS) into a fresh it_<random>
// schema (pinned via search_path) and drop it in t.Cleanup, so no manual goose
// up is required and the tests can never drift from the server's runner
// (review item 1). They skip (never fail) when the room database is
// unreachable.
package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

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
	s, url, _ := requireStoreSchema(t)
	return s, url
}

// requireStoreSchema is requireStore that also returns the test schema's name,
// for tests that need their own connection to it (for example to add a trigger).
func requireStoreSchema(t *testing.T) (*store.PG, string, string) {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	t.Cleanup(func() { dropSchema(cfg.RoomDBURL, schema) })

	// Migrations run through the exact same library and FS as the server:
	// goose v3 over migrations.FS, pinned to the test schema by search_path.
	sqlCfg, err := pgx.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatalf("store: parse config: %v", err)
	}
	sqlCfg.RuntimeParams["search_path"] = schema
	sqlDB := stdlib.OpenDB(*sqlCfg)
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		t.Fatalf("store: ping migration handle: %v", err)
	}
	n, err := migrations.Up(ctx, sqlDB)
	if err != nil {
		_ = sqlDB.Close()
		t.Fatalf("store: apply migrations: %v", err)
	}
	t.Logf("store: applied %d migration(s) into %s", n, schema)
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("store: close migration handle: %v", err)
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
	t.Cleanup(s.Close)
	return s, cfg.RoomDBURL, schema
}

// dropSchema removes a test schema; failures are not fatal (best effort).
func dropSchema(url, schema string) {
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()
	c, err := pgx.Connect(dctx, url)
	if err != nil {
		return
	}
	_, _ = c.Exec(dctx, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
	_ = c.Close(dctx)
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

// room.created with alert_source and dedup_key (the room's alert index key,
// "<pipeline>|<key>") is written to the jsonb payload column and read back
// unchanged, with the raw alert body intact.
func TestAppendRoomCreatedWithAlertKey(t *testing.T) {
	s, _ := requireStore(t)
	roomID := createTestRoom(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	in := events.RoomCreatedPayload{
		Title: "Lag", Severity: "sev1", PipelineID: "orders",
		Alert:       json.RawMessage(`{"groupKey":"{}:{alertname=\"Lag\"}","status":"firing"}`),
		AlertSource: "alertmanager",
		DedupKey:    `orders|alertmanager:{}:{alertname="Lag"}`,
	}
	if _, err := s.Append(ctx, roomID, events.RoomCreated, events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}, in); err != nil {
		t.Fatalf("store: append room.created: %v", err)
	}
	evs, err := s.Since(ctx, roomID, 0, 10)
	if err != nil || len(evs) != 1 {
		t.Fatalf("store: since: %v (%d events)", err, len(evs))
	}
	out, err := events.Decode[events.RoomCreatedPayload](evs[0])
	if err != nil {
		t.Fatalf("store: decode: %v", err)
	}
	if out.AlertSource != in.AlertSource || out.DedupKey != in.DedupKey || out.PipelineID != "orders" {
		t.Fatalf("store: read back %+v, want source %q key %q", out, in.AlertSource, in.DedupKey)
	}
	var alert map[string]any
	if err := json.Unmarshal(out.Alert, &alert); err != nil || alert["status"] != "firing" {
		t.Fatalf("store: alert body not intact: %s (%v)", out.Alert, err)
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

	terminateListen(t, s)
	waitForListen(t, s)

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

// terminateListen kills this store's own LISTEN backend: every hub gets a
// unique application_name (pid + random suffix, see store.ListenAppName), so
// this can never terminate a concurrently running test's backend (review item
// 12).
func terminateListen(t *testing.T, s *store.PG) {
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
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1`, s.ListenAppName()); err != nil {
		t.Fatalf("store: terminate listen backend: %v", err)
	}
}

func waitForListen(t *testing.T, s *store.PG) {
	t.Helper()
	cfg := config.Load()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
		if err == nil {
			var n int
			err = conn.QueryRow(ctx,
				`SELECT count(*) FROM pg_stat_activity WHERE application_name = $1`, s.ListenAppName()).Scan(&n)
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

// createTestProposal inserts a proposal row for the aux-table tests.
func createTestProposal(t *testing.T, s *store.PG, roomID, proposalID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.CreateProposal(ctx, store.ProposalRow{
		ProposalID: proposalID, RoomID: roomID, Title: "fix", Status: "open",
		RequiredLayers: []string{"postgres"}, Steps: []byte(`[{"action":"pg_act","layer":"postgres"}]`),
	}); err != nil {
		t.Fatalf("store: create proposal: %v", err)
	}
}

// TestEventTimesComeBackUTC pins review item 5: scanEvent normalises event
// times to UTC, so a snapshot rebuilt after a restart never mixes locations.
func TestEventTimesComeBackUTC(t *testing.T) {
	s, _ := requireStore(t)
	roomID := createTestRoom(t, s)
	ctx := context.Background()
	if _, err := s.Append(ctx, roomID, events.CommentAdded, commentActor(), events.CommentAddedPayload{CommentID: "c_1", Text: "hi"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	evs, err := s.Since(ctx, roomID, 0, 1)
	if err != nil || len(evs) != 1 {
		t.Fatalf("since: %v (%d events)", err, len(evs))
	}
	if got := evs[0].At; got.Location() != time.UTC {
		t.Fatalf("event time location: got %v, want UTC", got.Location())
	}
}

// TestProposalStepStateMachine checks the claim/finish transitions at the DB
// level (review round 2, should-fix 1): a step can be started once, finished
// once, and out-of-order transitions return ErrStepOrder.
func TestProposalStepStateMachine(t *testing.T) {
	s, _ := requireStore(t)
	ctx := context.Background()
	roomID := createTestRoom(t, s)
	pid := testenv.Name("p_")
	createTestProposal(t, s, roomID, pid)

	if err := s.FinishProposalStep(ctx, pid, 0, "failed", "x"); !errors.Is(err, store.ErrStepOrder) {
		t.Fatalf("finish before start: want ErrStepOrder, got %v", err)
	}
	if err := s.StartProposalStep(ctx, pid, 0); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := s.StartProposalStep(ctx, pid, 0); !errors.Is(err, store.ErrStepOrder) {
		t.Fatalf("double start: want ErrStepOrder, got %v", err)
	}
	if err := s.FinishProposalStep(ctx, pid, 0, "done", "ok"); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if err := s.FinishProposalStep(ctx, pid, 0, "done", "ok"); !errors.Is(err, store.ErrStepOrder) {
		t.Fatalf("double finish: want ErrStepOrder, got %v", err)
	}
}

// TestProposalRowRoundTripAndCompensation covers CreateProposal/GetProposal,
// SetProposalStatus and the compensation deletes (review round 2,
// should-fix 1).
func TestProposalRowRoundTripAndCompensation(t *testing.T) {
	s, _ := requireStore(t)
	ctx := context.Background()
	roomID := createTestRoom(t, s)
	pid := testenv.Name("p_")
	createTestProposal(t, s, roomID, pid)

	p, err := s.GetProposal(ctx, pid)
	if err != nil {
		t.Fatalf("get proposal: %v", err)
	}
	if p.RoomID != roomID || p.Status != "open" || len(p.RequiredLayers) != 1 || p.RequiredLayers[0] != "postgres" {
		t.Fatalf("bad proposal row: %+v", p)
	}
	if _, err := s.GetProposal(ctx, testenv.Name("p_")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing proposal: want ErrNotFound, got %v", err)
	}

	if err := s.SetProposalStatus(ctx, pid, "failed"); err != nil {
		t.Fatalf("set status: %v", err)
	}
	if p, err = s.GetProposal(ctx, pid); err != nil || p.Status != "failed" {
		t.Fatalf("status not updated: %+v err=%v", p, err)
	}

	// Approvals: second decision on a layer is ErrAlreadyExists; delete
	// re-opens it for another decision (compensation path).
	if err := s.RecordApproval(ctx, store.ApprovalRow{ProposalID: pid, Layer: "postgres", Decision: "approved"}); err != nil {
		t.Fatalf("record approval: %v", err)
	}
	if err := s.RecordApproval(ctx, store.ApprovalRow{ProposalID: pid, Layer: "postgres", Decision: "approved"}); !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("duplicate approval: want ErrAlreadyExists, got %v", err)
	}
	if err := s.DeleteApproval(ctx, pid, "postgres"); err != nil {
		t.Fatalf("delete approval: %v", err)
	}
	if err := s.RecordApproval(ctx, store.ApprovalRow{ProposalID: pid, Layer: "postgres", Decision: "approved"}); err != nil {
		t.Fatalf("re-record approval: %v", err)
	}

	if err := s.DeleteProposal(ctx, pid); err != nil {
		t.Fatalf("delete proposal: %v", err)
	}
	if _, err := s.GetProposal(ctx, pid); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleted proposal: want ErrNotFound, got %v", err)
	}
}

// TestSuggestionAndHandoffRows covers the suggestion and handoff aux tables,
// including handoff voiding (review item 10) and the compensation deletes
// (review round 2, should-fix 1).
func TestSuggestionAndHandoffRows(t *testing.T) {
	s, _ := requireStore(t)
	ctx := context.Background()
	roomID := createTestRoom(t, s)

	sid := testenv.Name("g_")
	if err := s.CreateSuggestion(ctx, store.SuggestionRow{
		SuggestionID: sid, RoomID: roomID, FromID: "priya", FromName: "Priya", Text: "try the registry",
	}); err != nil {
		t.Fatalf("create suggestion: %v", err)
	}
	if sugg, err := s.GetSuggestion(ctx, sid); err != nil || sugg.Status != "pending" || sugg.RoomID != roomID {
		t.Fatalf("bad suggestion row: %+v err=%v", sugg, err)
	}
	if err := s.SetSuggestionStatus(ctx, sid, "sent"); err != nil {
		t.Fatalf("set suggestion status: %v", err)
	}
	if err := s.DeleteSuggestion(ctx, sid); err != nil {
		t.Fatalf("delete suggestion: %v", err)
	}
	if _, err := s.GetSuggestion(ctx, sid); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleted suggestion: want ErrNotFound, got %v", err)
	}

	h1 := testenv.Name("h_")
	h2 := testenv.Name("h_")
	for _, h := range []string{h1, h2} {
		if err := s.CreateHandoff(ctx, store.HandoffRow{
			HandoffID: h, RoomID: roomID, FromID: "arjun", FromName: "Arjun", ToID: "priya", ToName: "Priya",
			Summary: []byte(`{"what_broke":"x"}`),
		}); err != nil {
			t.Fatalf("create handoff: %v", err)
		}
	}
	// A new handoff voids the older pending ones, the new one stays pending.
	if err := s.VoidHandoffs(ctx, roomID, h2); err != nil {
		t.Fatalf("void handoffs: %v", err)
	}
	if got, err := s.GetHandoff(ctx, h1); err != nil || got.Status != "voided" {
		t.Fatalf("older handoff should be voided: %+v err=%v", got, err)
	}
	if got, err := s.GetHandoff(ctx, h2); err != nil || got.Status != "pending" {
		t.Fatalf("newer handoff should stay pending: %+v err=%v", got, err)
	}
	if err := s.SetHandoffStatus(ctx, h2, "accepted"); err != nil {
		t.Fatalf("set handoff status: %v", err)
	}
	if err := s.DeleteHandoff(ctx, h2); err != nil {
		t.Fatalf("delete handoff: %v", err)
	}
	if _, err := s.GetHandoff(ctx, h2); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleted handoff: want ErrNotFound, got %v", err)
	}
}
