//go:build integration

package room_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
	"piperoom/migrations"
)

// pgStore applies the migrations into a fresh it_ schema of the room database
// and returns a store pinned to it; it skips when the database is down.
func pgStore(t *testing.T) *store.PG {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		t.Skipf("room Postgres unreachable at %s: %v", cfg.RoomDBURL, err)
	}
	defer conn.Close(ctx)
	schema := testenv.Name("schema")
	if _, err := conn.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Skipf("cannot create schema: %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		if c, err := pgx.Connect(dctx, cfg.RoomDBURL); err == nil {
			_, _ = c.Exec(dctx, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
			_ = c.Close(dctx)
		}
	})
	sqlCfg, err := pgx.ParseConfig(cfg.RoomDBURL)
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

// TestPGRoomLogCap is TestLogCapWarningAndFull on Postgres: the 90% warning
// once, the stop at 100% once, the reserve for people, close always, the
// rooms row's counters match the log, and a restarted service keeps the room
// stopped. TestPGRestartReadsByteCount covers the byte counter after a
// restart.
func TestPGRoomLogCap(t *testing.T) {
	st := pgStore(t)
	l := config.Limits{RoomMaxEvents: 100, RoomMaxBytes: 1 << 30}
	st.SetLimits(store.Limits{MaxEvents: l.RoomMaxEvents, MaxBytes: l.RoomMaxBytes})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runs := newRunRecorder()
	svc := room.New(room.Deps{Store: st, Users: adminUsers(t), Owners: testOwners(), Run: runs.run, Limits: l})
	t.Cleanup(svc.Close)
	snap, err := svc.CreateRoom(ctx, mustUser(t, svc, "arjun"), "cap", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	sink := runs.sink(t)
	var refused error
	for i := 0; i < 200 && refused == nil; i++ {
		refused = sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: "step"})
	}
	if !errors.Is(refused, store.ErrLogFull) {
		t.Fatalf("agent at the cap: %v", refused)
	}
	evs, err := st.Since(ctx, snap.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	warns, fulls := 0, 0
	for _, e := range evs {
		switch e.Type {
		case events.RoomLogCapWarning:
			warns++
			if e.Seq != 91 {
				t.Fatalf("warning at seq %d, want 91", e.Seq)
			}
		case events.AgentStatus:
			if p, _ := events.Decode[events.AgentStatusPayload](e); p.State == room.AgentLogFull {
				fulls++
			}
		}
	}
	if warns != 1 || fulls != 1 {
		t.Fatalf("warnings %d, log_full %d; want 1 and 1", warns, fulls)
	}
	waitFor(t, 5*time.Second, func() bool { _, stops := runs.counts(); return stops == 1 })

	// The rooms row's counters match the log the store wrote.
	u, err := st.RoomUsage(ctx, snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	var sum int64
	for _, e := range evs {
		sum += int64(len(e.Payload))
	}
	// byte_size counts the JSON as written; jsonb reads back with more
	// spaces, never fewer bytes.
	if u.Events != int64(len(evs)) || u.Bytes <= 0 || u.Bytes > sum {
		t.Fatalf("usage %+v vs %d events / %d bytes read back", u, len(evs), sum)
	}

	svc2 := room.New(room.Deps{Store: st, Users: adminUsers(t), Owners: testOwners(), Run: fakeRun, Limits: l})
	t.Cleanup(svc2.Close)
	_ = svc2.Load(ctx)
	if g, _ := svc2.GetRoom(ctx, snap.ID); g.LogCap != room.LogCapFull {
		t.Fatalf("after restart log_cap = %q", g.LogCap)
	}
	for i := 0; i < 10; i++ {
		if _, err = svc2.AddComment(ctx, mustUser(t, svc2, "priya"), snap.ID, "", "comment"); err != nil {
			break
		}
	}
	if !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("comment past the reserve: %v", err)
	}
	if _, err := svc2.CloseRoom(ctx, mustUser(t, svc2, "arjun"), snap.ID, "done"); err != nil {
		t.Fatalf("close a full room: %v", err)
	}
	if r, _ := st.GetRoom(ctx, snap.ID); r.LastSeq != 103 {
		t.Fatalf("last_seq = %d, want 102 (cap + reserve) + room.closed", r.LastSeq)
	}
}

// TestPGRestartReadsByteCount is TestRestartReadsByteCount on Postgres: a
// restarted service takes the byte count from the rooms row, so a comment
// that takes the log past 90% gets the warning.
func TestPGRestartReadsByteCount(t *testing.T) {
	st := pgStore(t)
	l := config.Limits{RoomMaxEvents: 100000, RoomMaxBytes: 20000}
	st.SetLimits(store.Limits{MaxEvents: l.RoomMaxEvents, MaxBytes: l.RoomMaxBytes})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runs := newRunRecorder()
	svc := room.New(room.Deps{Store: st, Users: adminUsers(t), Owners: testOwners(), Run: runs.run, Limits: l})
	snap, err := svc.CreateRoom(ctx, mustUser(t, svc, "arjun"), "bytes", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	sink := runs.sink(t)
	text := strings.Repeat("x", 500)
	for {
		u, err := st.RoomUsage(ctx, snap.ID)
		if err != nil {
			t.Fatal(err)
		}
		if u.Bytes >= 16500 {
			break
		}
		if err := sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: text}); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}
	svc.Close()

	svc2 := room.New(room.Deps{Store: st, Users: adminUsers(t), Owners: testOwners(), Run: fakeRun, Limits: l})
	t.Cleanup(svc2.Close)
	if err := svc2.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := svc2.AddComment(ctx, mustUser(t, svc2, "priya"), snap.ID, "", strings.Repeat("y", 1600)); err != nil {
		t.Fatalf("comment: %v", err)
	}
	evs, err := st.Since(ctx, snap.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	warns := 0
	for _, e := range evs {
		if e.Type == events.RoomLogCapWarning {
			warns++
			p, _ := events.Decode[events.RoomLogCapWarningPayload](e)
			if p.Bytes < 18000 {
				t.Fatalf("warning payload = %+v, want the stored bytes counted", p)
			}
		}
	}
	if warns != 1 {
		t.Fatalf("cap warnings after restart = %d, want 1", warns)
	}
}

// TestPGDeleteRoomService: the admin delete on Postgres removes the room and
// writes the audit row; a repeat returns the same row.
func TestPGDeleteRoomService(t *testing.T) {
	st := pgStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	svc := room.New(room.Deps{Store: st, Users: adminUsers(t), Owners: testOwners(), Run: fakeRun, Limits: config.Limits{ArchiveDir: t.TempDir()}})
	t.Cleanup(svc.Close)
	snap, err := svc.CreateRoom(ctx, mustUser(t, svc, "arjun"), "del", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	d, err := svc.DeleteRoom(ctx, mustUser(t, svc, "ada"), snap.ID, "test", true)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Archived || d.EventCount != 2 || d.PipelineID != "orders" {
		t.Fatalf("deletion = %+v", d)
	}
	if _, err := st.GetRoom(ctx, snap.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("room left: %v", err)
	}
	again, err := svc.DeleteRoom(ctx, mustUser(t, svc, "ada"), snap.ID, "again", true)
	if err != nil || again.RoomDeletion != d.RoomDeletion || !again.Archived {
		t.Fatalf("repeat = %+v %v", again, err)
	}
}
