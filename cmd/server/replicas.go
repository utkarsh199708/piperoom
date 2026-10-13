package main

// Multi-replica wiring (docs/design/replicas.md): the replica id, the global
// agent cap, the migrations lock, and the loops that run on the leader only
// (watcher, retention purge, auth prune, Slack mirror).

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/watch"
)

// Leader loop names: one advisory lock each.
const (
	loopWatcher   = "watcher"
	loopRetention = "retention"
	loopAuthPrune = "auth-prune"
	loopSlack     = "slack"
)

// slackRescan is how often the Slack mirror's leader looks for open rooms it
// does not mirror yet: OnRoomOpened only fires on the replica that opened the
// room.
const slackRescan = 15 * time.Second

// replicaID is PIPEROOM_REPLICA_ID (default: the host name, in Kubernetes the
// pod name), cut to a safe charset and length, plus a random per-process
// suffix so a restarted container never takes its old process's leases or
// claims for its own.
func replicaID(configured string) string {
	base := strings.TrimSpace(configured)
	if base == "" {
		if h, err := os.Hostname(); err == nil {
			base = h
		}
	}
	var b strings.Builder
	for _, r := range base {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		}
		if b.Len() >= 40 {
			break
		}
	}
	base = b.String()
	if base == "" {
		base = "piperoom"
	}
	var suffix [3]byte
	_, _ = rand.Read(suffix[:]) // never fails (crypto/rand docs)
	return base + "-" + hex.EncodeToString(suffix[:])
}

// parseMaxAgentsGlobal reads PIPEROOM_MAX_AGENTS_GLOBAL: empty or "0" = off
// (0); otherwise plain decimal digits making a whole number of 1 or more. A
// bad value stops the start, as for PIPEROOM_MAX_AGENTS.
func parseMaxAgentsGlobal(raw string) (int, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return 0, nil
	}
	for _, r := range v {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("server: PIPEROOM_MAX_AGENTS_GLOBAL %q must be a whole number (0 or empty = off)", raw)
		}
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, fmt.Errorf("server: PIPEROOM_MAX_AGENTS_GLOBAL %q is too large", raw)
		}
		return 0, fmt.Errorf("server: PIPEROOM_MAX_AGENTS_GLOBAL %q is not a whole number: %w", raw, err)
	}
	return n, nil
}

// withMigrationLock runs fn while holding the "migrations" session advisory
// lock on a connection of db, so replicas that start together migrate one at
// a time: the first applies the migrations, the others then find none to
// apply.
func withMigrationLock(ctx context.Context, db *sql.DB, fn func() error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("server: migrations lock: connect: %w", err)
	}
	// Closing the connection also frees the lock on every path.
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext(current_schema()), hashtext('migrations'))`); err != nil {
		return fmt.Errorf("server: migrations lock: %w", err)
	}
	ferr := fn()
	uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(uctx, `SELECT pg_advisory_unlock(hashtext(current_schema()), hashtext('migrations'))`); err != nil && ferr == nil {
		return fmt.Errorf("server: migrations unlock: %w", err)
	}
	return ferr
}

// runElected runs fn on this replica whenever it is the leader of name, until
// ctx ends or stop is called; stop waits for the elector (and fn) to return.
func runElected(ctx context.Context, el store.Elector, name string, fn func(ctx context.Context)) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		el.Run(ctx, name, fn)
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
}

// startElectedRetention is startRetention on the leader of "retention" only.
// With retention off nothing is elected.
func startElectedRetention(ctx context.Context, el store.Elector, svc expirer, days int, every, timeout time.Duration, now func() time.Time, log *slog.Logger) (stop func()) {
	if days <= 0 {
		return startRetention(ctx, svc, days, every, timeout, now, log)
	}
	return runElected(ctx, el, loopRetention, func(lctx context.Context) {
		stopTerm := startRetention(lctx, svc, days, every, timeout, now, log)
		<-lctx.Done()
		stopTerm()
	})
}

// startElectedAuthPrune is startAuthPrune on the leader of "auth-prune" only.
func startElectedAuthPrune(ctx context.Context, el store.Elector, sessions auth.SessionPruner, loginStates auth.LoginStatePruner, every, timeout time.Duration, now func() time.Time, log *slog.Logger) (stop func()) {
	if sessions == nil && loginStates == nil {
		return func() {}
	}
	return runElected(ctx, el, loopAuthPrune, func(lctx context.Context) {
		stopTerm := startAuthPrune(lctx, sessions, loginStates, every, timeout, now, log)
		<-lctx.Done()
		stopTerm()
	})
}

// startElectedWatcher runs the watcher on the leader of "watcher" only. The
// deps are checked once now, so a bad configuration still stops the start;
// every leadership term then gets a fresh watcher (it never acts on episodes it
// saw before it lost the lock), which the room-closed hook points at while the
// term lasts. Mode off elects nothing.
func startElectedWatcher(ctx context.Context, el store.Elector, d watch.Deps, hook *roomClosedHook, log *slog.Logger) (stop func(), err error) {
	if d.Config.Mode == config.WatchOff {
		log.Info("server: watcher off")
		return func() {}, nil
	}
	d.Log = log
	w, err := watch.New(d)
	if err != nil {
		return nil, fmt.Errorf("server: build the watcher: %w", err)
	}
	log.Info("server: watcher configured; it runs on the leader replica",
		"mode", string(d.Config.Mode), "interval", d.Config.Interval.String(), "pipelines", w.Targets())
	return runElected(ctx, el, loopWatcher, func(lctx context.Context) {
		term, err := watch.New(d)
		if err != nil {
			log.Error("server: build the watcher for this term", "err", err)
			<-lctx.Done()
			return
		}
		hook.setWatcher(term)
		defer hook.clearWatcher(term)
		stopTerm := startWatch(lctx, term, watchStopTimeout, log)
		<-lctx.Done()
		stopTerm()
	}), nil
}

// elect mirrors rooms to Slack only while this replica leads "slack": each
// term starts a mirror for every open room, rescans every slackRescan for
// rooms opened elsewhere, and its mirrors stop when the term ends. Mirrors
// resume from slack_threads.last_seq, so the next leader carries on.
func (m *mirrors) elect(ctx context.Context, el store.Elector, svc *room.Service) (stop func()) {
	if m == nil {
		return func() {}
	}
	m.mu.Lock()
	m.lead = nil // not leading until a term starts
	m.mu.Unlock()
	return runElected(ctx, el, loopSlack, func(lctx context.Context) {
		m.mu.Lock()
		m.lead = lctx
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			if m.lead == lctx {
				m.lead = nil
			}
			m.mu.Unlock()
		}()
		t := time.NewTicker(slackRescan)
		defer t.Stop()
		for {
			if err := m.startOpen(lctx, svc); err != nil && lctx.Err() == nil {
				m.log.Warn("slack mirror: list open rooms failed", "err", err)
			}
			select {
			case <-lctx.Done():
				return
			case <-t.C:
			}
		}
	})
}
