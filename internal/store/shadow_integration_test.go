//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/migrations"
)

var shadowAgent = events.Actor{Kind: events.ActorAgent, ID: "agent", Name: "Agent"}

func createShadowRoom(ctx context.Context, t *testing.T, s *store.PG) string {
	t.Helper()
	roomID := createTestRoom(t, s)
	if _, err := s.Append(ctx, roomID, events.RoomCreated, events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"},
		events.RoomCreatedPayload{Title: "t", Severity: "sev2", PipelineID: "orders", AlertSource: "watcher", Shadow: true}); err != nil {
		t.Fatalf("room.created: %v", err)
	}
	if err := s.CreateShadowRun(ctx, store.ShadowRun{RoomID: roomID, Status: store.ShadowQueued}); err != nil {
		t.Fatalf("create shadow run: %v", err)
	}
	return roomID
}

// Migration 00009 goes down (both tables dropped) and up again cleanly.
func TestShadowMigrationUpDown(t *testing.T) {
	_, url, schema := requireStoreSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cc, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cc.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cc)
	defer db.Close()
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	exists := func(table string) bool {
		var ok bool
		if err := db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, schema+"."+table).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !exists("shadow_runs") || !exists("shadow_events") {
		t.Fatal("after up: shadow tables missing")
	}
	if _, err := p.DownTo(ctx, 8); err != nil {
		t.Fatalf("down to 8: %v", err)
	}
	if exists("shadow_runs") || exists("shadow_events") {
		t.Fatal("after down: shadow tables still there")
	}
	if !exists("rooms") || !exists("room_events") {
		t.Fatal("down went too far")
	}
	if _, err := p.UpTo(ctx, 9); err != nil {
		t.Fatalf("up to 9: %v", err)
	}
	if !exists("shadow_runs") || !exists("shadow_events") {
		t.Fatal("after re-up: shadow tables missing")
	}
}

// 20 parallel appends to one shadow log get seqs 1..20 with no gap and no
// duplicate, and the room log is not touched.
func TestShadowAppendGapFreeParallel(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	roomID := createShadowRoom(ctx, t, s)
	const n = 20
	var wg sync.WaitGroup
	seqs := make([]int64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e, err := s.AppendShadow(ctx, roomID, events.AgentStepStart, shadowAgent, events.AgentStepStartPayload{StepID: "s", Title: "t"})
			seqs[i], errs[i] = e.Seq, err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for i, q := range seqs {
		if q != int64(i+1) {
			t.Fatalf("seqs = %v, want 1..%d", seqs, n)
		}
	}
	got, err := s.ShadowSince(ctx, roomID, 0, 100)
	if err != nil || len(got) != n {
		t.Fatalf("ShadowSince = %d events, %v", len(got), err)
	}
	room, err := s.Since(ctx, roomID, 0, 100)
	if err != nil || len(room) != 1 {
		t.Fatalf("room log = %d events, %v; want only room.created", len(room), err)
	}
	run, err := s.GetShadowRun(ctx, roomID)
	if err != nil || run.LastSeq != n {
		t.Fatalf("run last_seq = %d, %v", run.LastSeq, err)
	}
}

// The jsonb guard refuses a NUL in a shadow payload before a seq is used.
func TestShadowAppendRefusesNUL(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	roomID := createShadowRoom(ctx, t, s)
	_, err := s.AppendShadow(ctx, roomID, events.AgentMessage, shadowAgent, json.RawMessage(`{"text":"a\u0000b"}`))
	if !errors.Is(err, store.ErrUnstorable) {
		t.Fatalf("NUL escape: err = %v, want ErrUnstorable", err)
	}
	_, err = s.AppendShadow(ctx, roomID, events.AgentMessage, shadowAgent, events.AgentMessagePayload{Text: "a\x00b"})
	if !errors.Is(err, store.ErrUnstorable) {
		t.Fatalf("raw NUL: err = %v, want ErrUnstorable", err)
	}
	e, err := s.AppendShadow(ctx, roomID, events.AgentMessage, shadowAgent, events.AgentMessagePayload{Text: "ok"})
	if err != nil || e.Seq != 1 {
		t.Fatalf("after refusals: seq %d, err %v; want 1 (no seq used)", e.Seq, err)
	}
}

// Create is idempotent, a room without a run row cannot append, hypothesis
// events keep the run row current, update leaves revealed_at alone, and
// MarkRevealed is write-once.
func TestShadowRunLifecycle(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	roomID := createShadowRoom(ctx, t, s)
	if err := s.CreateShadowRun(ctx, store.ShadowRun{RoomID: roomID, Status: store.ShadowRunning}); err != nil {
		t.Fatalf("second create: %v", err)
	}
	run, err := s.GetShadowRun(ctx, roomID)
	if err != nil || run.Status != store.ShadowQueued {
		t.Fatalf("after second create: %+v, %v; want the first row kept", run, err)
	}
	other := createTestRoom(t, s)
	if _, err := s.AppendShadow(ctx, other, events.AgentMessage, shadowAgent, events.AgentMessagePayload{Text: "x"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("append without run row: %v, want ErrNotFound", err)
	}
	if _, err := s.GetShadowRun(ctx, other); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get without row: %v", err)
	}
	if err := s.UpdateShadowRun(ctx, store.ShadowRun{RoomID: other, Status: "error"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("update without row: %v", err)
	}
	if _, err := s.MarkRevealed(ctx, other, time.Now()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("mark without row: %v", err)
	}

	for _, txt := range []string{"first", "final"} {
		if _, err := s.AppendShadow(ctx, roomID, events.AgentHypothesis, shadowAgent, events.AgentHypothesisPayload{Text: txt, Confidence: "high"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := s.AppendShadow(ctx, roomID, events.AgentToolCall, shadowAgent, events.AgentToolCallPayload{StepID: "s", Tool: "x"}); err != nil {
		t.Fatal(err)
	}
	run, err = s.GetShadowRun(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	if run.FirstHypothesisAt == nil || run.FinalHypothesisAt == nil || !run.FinalHypothesisAt.After(*run.FirstHypothesisAt) {
		t.Fatalf("hypothesis times: %v %v", run.FirstHypothesisAt, run.FinalHypothesisAt)
	}
	var h events.AgentHypothesisPayload
	if err := json.Unmarshal(run.FinalHypothesis, &h); err != nil || h.Text != "final" {
		t.Fatalf("final hypothesis = %s (%v)", run.FinalHypothesis, err)
	}
	if n, err := s.CountShadowEvents(ctx, roomID, events.AgentToolCall); err != nil || n != 1 {
		t.Fatalf("count tool calls = %d, %v", n, err)
	}

	first, err := s.MarkRevealed(ctx, roomID, time.Now())
	if err != nil || !first {
		t.Fatalf("first reveal: %v %v", first, err)
	}
	first, err = s.MarkRevealed(ctx, roomID, time.Now())
	if err != nil || first {
		t.Fatalf("second reveal: first=%v err=%v, want false", first, err)
	}
	fin := time.Now().UTC()
	run.Status, run.FinishedAt, run.Tokens, run.ToolCalls = store.ShadowHypothesised, &fin, 1234, 1
	run.RevealedAt = nil
	run.FirstHypothesisAt, run.FinalHypothesisAt, run.FinalHypothesis = nil, nil, json.RawMessage(`{"text":"stale"}`)
	if err := s.UpdateShadowRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetShadowRun(ctx, roomID)
	if err != nil || got.RevealedAt == nil || got.Status != store.ShadowHypothesised || got.Tokens != 1234 || got.LastSeq != 3 {
		t.Fatalf("after update: %+v %v", got, err)
	}
	// The hypothesis columns belong to AppendShadow: an update never changes them.
	var kept events.AgentHypothesisPayload
	if json.Unmarshal(got.FinalHypothesis, &kept); kept.Text != "final" || got.FirstHypothesisAt == nil || got.FinalHypothesisAt == nil {
		t.Fatalf("update touched the hypothesis columns: %+v", got)
	}
	if err := s.UpdateShadowRun(ctx, store.ShadowRun{RoomID: roomID, Status: "x", Model: "a\x00"}); !errors.Is(err, store.ErrUnstorable) {
		t.Fatalf("NUL model: %v", err)
	}
}

// ListShadowRuns joins closed rooms only, filters by pipeline and since, and
// UnrevealedShadowRuns lists finished, closed, unrevealed runs.
func TestShadowListAndUnrevealed(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	open := createShadowRoom(ctx, t, s)
	closedA := createShadowRoom(ctx, t, s)
	closedB := createShadowRoom(ctx, t, s)
	t0 := time.Now().UTC().Add(-time.Hour)
	if err := s.CloseRoom(ctx, closedA, t0); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseRoom(ctx, closedB, t0.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	fin := t0
	for _, id := range []string{closedA, open} {
		r, _ := s.GetShadowRun(ctx, id)
		r.FinishedAt, r.Status = &fin, store.ShadowHypothesised
		if err := s.UpdateShadowRun(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	all, err := s.ListShadowRuns(ctx, "", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, r := range all {
		if r.RoomID == closedA || r.RoomID == closedB || r.RoomID == open {
			ids = append(ids, r.RoomID)
			if r.AlertSource != "watcher" || r.PipelineID != "orders" || r.ClosedAt.IsZero() {
				t.Fatalf("row facts: %+v", r)
			}
		}
	}
	if len(ids) != 2 || ids[0] != closedB || ids[1] != closedA {
		t.Fatalf("list = %v, want [closedB closedA] (newest close first, open room left out)", ids)
	}
	recent, err := s.ListShadowRuns(ctx, "orders", t0.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recent {
		if r.RoomID == closedA {
			t.Fatal("since filter kept an older room")
		}
	}
	none, err := s.ListShadowRuns(ctx, "payments", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range none {
		if r.RoomID == closedA || r.RoomID == closedB {
			t.Fatal("pipeline filter kept an orders room")
		}
	}
	un, err := s.UnrevealedShadowRuns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	has := map[string]bool{}
	for _, id := range un {
		has[id] = true
	}
	if !has[closedA] || has[closedB] || has[open] {
		t.Fatalf("unrevealed = %v; want closedA only (closedB unfinished, open is open)", un)
	}
}

// Retention deletes a closed shadow room's shadow rows and counts them.
func TestShadowRetentionPurges(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	roomID := createShadowRoom(ctx, t, s)
	for i := 0; i < 3; i++ {
		if _, err := s.AppendShadow(ctx, roomID, events.AgentMessage, shadowAgent, events.AgentMessagePayload{Text: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	closed := time.Now().UTC().Add(-48 * time.Hour)
	if err := s.CloseRoom(ctx, roomID, closed); err != nil {
		t.Fatal(err)
	}
	res, err := s.PurgeClosedBefore(ctx, closed.Add(time.Minute), 1000, nil)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, id := range res.RoomIDs {
		found = found || id == roomID
	}
	if !found {
		t.Fatalf("room not purged: %+v", res)
	}
	if res.ShadowRuns < 1 || res.ShadowEvents < 3 {
		t.Fatalf("counts: runs %d events %d, want >= 1 and >= 3", res.ShadowRuns, res.ShadowEvents)
	}
	if _, err := s.GetShadowRun(ctx, roomID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("run row survived: %v", err)
	}
	if evs, err := s.ShadowSince(ctx, roomID, 0, 10); err != nil || len(evs) != 0 {
		t.Fatalf("shadow events survived: %d %v", len(evs), err)
	}
}
