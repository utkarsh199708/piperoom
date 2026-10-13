package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"piperoom/internal/auth"
)

type fakePruner struct {
	mu        sync.Mutex
	sessionAt []time.Time
	graces    []time.Duration
	stateAt   []time.Time
	sessErr   error
	stateErr  error
}

func (f *fakePruner) PruneSessions(_ context.Context, now time.Time, grace time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessionAt = append(f.sessionAt, now)
	f.graces = append(f.graces, grace)
	return 2, f.sessErr
}

func (f *fakePruner) PruneLoginStates(_ context.Context, now time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stateAt = append(f.stateAt, now)
	return 1, f.stateErr
}

func (f *fakePruner) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sessionAt), len(f.stateAt)
}

// The prune runs at start and on every tick, with the 7-day grace for sessions,
// and it is independent of PIPEROOM_RETENTION_DAYS: with retention off
// (days = 0) startRetention starts nothing, and this still runs.
func TestAuthPruneRunsEvenWhenRoomRetentionIsOff(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	log := slog.New(slog.DiscardHandler)

	rooms := &fakeExpirer{}
	stopRooms := startRetention(context.Background(), rooms, 0, time.Millisecond, time.Second, func() time.Time { return now }, log)
	defer stopRooms()

	p := &fakePruner{}
	stop := startAuthPrune(context.Background(), p, p, 5*time.Millisecond, time.Second, func() time.Time { return now }, log)
	waitUntil(t, func() bool { s, l := p.counts(); return s >= 3 && l >= 3 }) // start-up run + two ticks
	stop()

	if n := len(rooms.calls()); n != 0 {
		t.Fatalf("room retention is off but purged %d times", n)
	}
	for i, at := range p.sessionAt {
		if !at.Equal(now) || p.graces[i] != 7*24*time.Hour {
			t.Fatalf("sessions prune %d: at %v grace %v, want %v and 7 days", i, at, p.graces[i], now)
		}
	}
	for _, at := range p.stateAt {
		if !at.Equal(now) {
			t.Fatalf("login states prune at %v, want %v", at, now)
		}
	}
	// stop really stops.
	s, l := p.counts()
	time.Sleep(30 * time.Millisecond)
	if s2, l2 := p.counts(); s2 != s || l2 != l {
		t.Fatal("prune kept running after stop")
	}
}

// A failure in one table does not stop the other, and the loop goes on.
func TestAuthPruneOneTableFailingDoesNotStopTheOther(t *testing.T) {
	p := &fakePruner{sessErr: errors.New("db down")}
	stop := startAuthPrune(context.Background(), p, p, 5*time.Millisecond, time.Second, time.Now, slog.New(slog.DiscardHandler))
	waitUntil(t, func() bool { s, l := p.counts(); return s >= 2 && l >= 2 })
	stop()

	p2 := &fakePruner{stateErr: errors.New("db down")}
	stop = startAuthPrune(context.Background(), p2, p2, 5*time.Millisecond, time.Second, time.Now, slog.New(slog.DiscardHandler))
	waitUntil(t, func() bool { s, l := p2.counts(); return s >= 2 && l >= 2 })
	stop()
}

// With a nil pruner (a store that cannot prune) only the other table runs; with
// both nil nothing starts.
func TestAuthPruneSkipsNilPruners(t *testing.T) {
	p := &fakePruner{}
	stop := startAuthPrune(context.Background(), nil, p, 5*time.Millisecond, time.Second, time.Now, slog.New(slog.DiscardHandler))
	waitUntil(t, func() bool { _, l := p.counts(); return l >= 2 })
	stop()
	if s, _ := p.counts(); s != 0 {
		t.Fatalf("sessions pruned %d times with a nil sessions pruner", s)
	}

	stop = startAuthPrune(context.Background(), nil, nil, time.Millisecond, time.Second, time.Now, slog.New(slog.DiscardHandler))
	stop()

	sp, lp := authPruners(auth.NewMemStore(), auth.NewMemLoginStates())
	if sp == nil || lp == nil {
		t.Fatal("the in-memory stores must prune")
	}
}

// The real in-memory stores through the loop: dead rows go, live ones stay.
func TestAuthPruneDeletesDeadRowsInMemStores(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	sessions, states := auth.NewMemStore(), auth.NewMemLoginStates()
	mk := func(id string, expires, revoked time.Time) {
		_, h, _ := auth.NewToken()
		if err := sessions.Create(ctx, auth.Session{ID: id, TokenHash: h, UserID: id, Method: auth.MethodDev, ExpiresAt: expires, RevokedAt: revoked}); err != nil {
			t.Fatal(err)
		}
	}
	mk("live", now.Add(time.Hour), time.Time{})
	mk("dead", now.Add(-10*24*time.Hour), time.Time{})
	if err := states.Create(ctx, auth.LoginState{ID: "old", ExpiresAt: now.Add(-time.Minute)}, "", now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := states.Create(ctx, auth.LoginState{ID: "new", ExpiresAt: now.Add(time.Minute)}, "", now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	runAuthPrune(ctx, sessions, states, time.Second, func() time.Time { return now }, slog.New(slog.DiscardHandler))

	if _, err := sessions.ByID(ctx, "dead"); !errors.Is(err, auth.ErrNotFound) {
		t.Fatalf("dead session: %v, want ErrNotFound", err)
	}
	if _, err := sessions.ByID(ctx, "live"); err != nil {
		t.Fatalf("live session: %v", err)
	}
	if states.Len() != 1 {
		t.Fatalf("%d login states left, want 1", states.Len())
	}
}
