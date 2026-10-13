//go:build integration

package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"piperoom/internal/config"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
	"piperoom/internal/watch"
	"piperoom/migrations"
)

// leaderSchema makes a migrated throwaway schema (dropped at cleanup), or
// skips when the room database is unreachable.
func leaderSchema(t *testing.T) string {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		t.Skipf("room Postgres unreachable at %s: %v", cfg.RoomDBURL, err)
	}
	defer conn.Close(ctx)
	schema := testenv.Name("leaders")
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
	cc, err := pgx.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	cc.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cc)
	defer db.Close()
	// The server's own path: migrations under the migrations lock.
	if err := withMigrationLock(ctx, db, func() error { _, err := migrations.Up(ctx, db); return err }); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	return schema
}

func leaderStore(t *testing.T, schema string) *store.PG {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pc, err := pgxpool.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	pc.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewPool(pool, cfg.RoomDBURL, slog.Default())
	t.Cleanup(st.Close)
	return st
}

func killBackend(t *testing.T, app string) {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	var n int
	if err := c.QueryRow(ctx, `SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = $1`, app).Scan(&n); err != nil || n != 1 {
		t.Fatalf("terminate %s: n=%d err=%v", app, n, err)
	}
}

// purgeLog records which replica purged when.
type purgeLog struct {
	mu    sync.Mutex
	calls []purgeCall
}

type purgeCall struct {
	replica int
	at      time.Time
}

type replicaExpirer struct {
	log     *purgeLog
	replica int
}

func (e replicaExpirer) PurgeExpired(ctx context.Context, cutoff time.Time) (store.PurgeResult, error) {
	e.log.mu.Lock()
	e.log.calls = append(e.log.calls, purgeCall{replica: e.replica, at: time.Now()})
	e.log.mu.Unlock()
	return store.PurgeResult{}, nil
}

func (l *purgeLog) snapshot() []purgeCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]purgeCall(nil), l.calls...)
}

// Three server replicas, production elector timings: exactly one runs the
// retention loop and exactly one the watcher. Killing the leaders' lock
// connections moves both loops to other replicas within 30 s, and the old
// retention loop has stopped before the new one starts.
func TestLeaderLoopsOneReplicaAndTakeover(t *testing.T) {
	if testing.Short() {
		t.Skip("takes ~25 s with production timings")
	}
	schema := leaderSchema(t)
	const n = 3
	var (
		plog     = &purgeLog{}
		electors = make([]*store.PGElector, n)
		hooks    = make([]*roomClosedHook, n)
		stops    []func()
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		for _, s := range stops {
			s()
		}
	}()
	for i := 0; i < n; i++ {
		st := leaderStore(t, schema)
		electors[i] = st.Elector(fmt.Sprintf("r%d", i))
		hooks[i] = &roomClosedHook{}
		stops = append(stops, startElectedRetention(ctx, electors[i], replicaExpirer{log: plog, replica: i}, 30, 100*time.Millisecond, time.Second, time.Now, discardLog()))
		stop, err := startElectedWatcher(ctx, electors[i], watch.Deps{
			Owners: loadTestOwners(t), Config: config.Watch{Mode: config.WatchLog, Interval: time.Minute},
			Jitter: func(time.Duration) time.Duration { return 0 },
		}, hooks[i], discardLog())
		if err != nil {
			t.Fatal(err)
		}
		stops = append(stops, stop)
	}
	watcherOn := func() []int {
		var on []int
		for i, h := range hooks {
			h.mu.RLock()
			if h.watcher != nil {
				on = append(on, i)
			}
			h.mu.RUnlock()
		}
		return on
	}
	retentionOn := func(since time.Time) map[int]bool {
		on := map[int]bool{}
		for _, c := range plog.snapshot() {
			if c.at.After(since) {
				on[c.replica] = true
			}
		}
		return on
	}
	wait := func(what string, within time.Duration, cond func() bool) time.Duration {
		t.Helper()
		start := time.Now()
		for time.Since(start) < within {
			if cond() {
				return time.Since(start)
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("not within %s: %s", within, what)
		return 0
	}
	wait("a watcher leader", 30*time.Second, func() bool { return len(watcherOn()) == 1 })
	wait("a retention leader", 30*time.Second, func() bool { return len(retentionOn(time.Time{})) == 1 })
	// Two seconds of steady state: still one of each.
	time.Sleep(2 * time.Second)
	if on := watcherOn(); len(on) != 1 {
		t.Fatalf("watchers running on %v, want exactly one replica", on)
	}
	steady := retentionOn(time.Now().Add(-time.Second))
	if len(steady) != 1 {
		t.Fatalf("retention ran on %v in the last second, want exactly one replica", steady)
	}
	var retLeader int
	for r := range steady {
		retLeader = r
	}
	watchLeader := watcherOn()[0]

	killed := time.Now()
	killBackend(t, electors[retLeader].AppName(loopRetention))
	killBackend(t, electors[watchLeader].AppName(loopWatcher))
	took := wait("retention moves to another replica", 30*time.Second, func() bool {
		for r := range retentionOn(killed) {
			if r != retLeader {
				return true
			}
		}
		return false
	})
	t.Logf("retention moved from r%d in %s", retLeader, took.Round(time.Millisecond))
	took = wait("the watcher moves to another replica", 30*time.Second, func() bool {
		on := watcherOn()
		return len(on) == 1 && on[0] != watchLeader
	})
	t.Logf("watcher moved from r%d in %s", watchLeader, took.Round(time.Millisecond))

	// The old retention loop stopped before the new one ran: no call of the
	// old leader after the new leader's first call.
	var firstNew, lastOld time.Time
	for _, c := range plog.snapshot() {
		switch {
		case c.replica == retLeader && c.at.After(lastOld):
			lastOld = c.at
		case c.replica != retLeader && c.at.After(killed) && firstNew.IsZero():
			firstNew = c.at
		}
	}
	if !lastOld.Before(firstNew) {
		t.Fatalf("old leader purged at %s, new leader already at %s", lastOld.Format(time.StampMilli), firstNew.Format(time.StampMilli))
	}
	if len(watcherOn()) != 1 {
		t.Fatalf("watchers on %v after the move", watcherOn())
	}
}
