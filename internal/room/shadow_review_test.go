package room_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// Review round 1 (LEAD-REVIEW on PR #78).

// A crash between the shadow.revealed append and MarkRevealed leaves the event
// in the log with revealed_at NULL. The Load sweep must set revealed_at and
// must not write a second reveal (write-once).
func TestShadowLoadSweepHalfRevealed(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)}
	mem := memstore.NewWithClock(c.Now)
	ctx := context.Background()
	sys := events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}
	if err := mem.CreateRoom(ctx, store.Room{ID: "r_half", Title: "t", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: c.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := mem.Append(ctx, "r_half", events.RoomCreated, sys, events.RoomCreatedPayload{Title: "t", Severity: "sev2", PipelineID: "orders", Shadow: true}); err != nil {
		t.Fatal(err)
	}
	fin := c.Now()
	if err := mem.CreateShadowRun(ctx, store.ShadowRun{RoomID: "r_half", Status: store.ShadowHypothesised, FinishedAt: &fin}); err != nil {
		t.Fatal(err)
	}
	if _, err := mem.Append(ctx, "r_half", events.RoomClosed, events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}, events.RoomClosedPayload{Resolution: "fixed"}); err != nil {
		t.Fatal(err)
	}
	if err := mem.CloseRoom(ctx, "r_half", c.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := mem.Append(ctx, "r_half", events.ShadowRevealed, sys, events.ShadowRevealedPayload{Status: store.ShadowHypothesised}); err != nil {
		t.Fatal(err)
	}
	// ...and the process died here: revealed_at is still NULL.

	own, users := loadShadowFixtures(t)
	for i := 0; i < 2; i++ { // two restarts
		svc := room.New(room.Deps{Store: mem, Users: users, Owners: own, Now: c.Now, WriteHandoff: fakeWrite})
		if err := svc.Load(ctx); err != nil {
			t.Fatal(err)
		}
		svc.Close()
		if n := countReveals(mustSince(t, mem, "r_half")); n != 1 {
			t.Fatalf("restart %d: reveals = %d, want 1", i+1, n)
		}
		if r, _ := mem.GetShadowRun(ctx, "r_half"); r.RevealedAt == nil {
			t.Fatalf("restart %d: revealed_at still NULL", i+1)
		}
	}
}

// revealed_at is the store's write-once marker: when it is set, no reveal is
// written, even if this process's log view has none (for example a reveal
// written through another handle on the same database).
func TestShadowRevealedAtIsWriteOnceMarker(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)}
	mem := memstore.NewWithClock(c.Now)
	ctx := context.Background()
	sys := events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}
	if err := mem.CreateRoom(ctx, store.Room{ID: "r_marked", Title: "t", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: c.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := mem.Append(ctx, "r_marked", events.RoomCreated, sys, events.RoomCreatedPayload{Title: "t", Severity: "sev2", PipelineID: "orders", Shadow: true}); err != nil {
		t.Fatal(err)
	}
	fin := c.Now()
	if err := mem.CreateShadowRun(ctx, store.ShadowRun{RoomID: "r_marked", Status: store.ShadowHypothesised, FinishedAt: &fin}); err != nil {
		t.Fatal(err)
	}
	if first, err := mem.MarkRevealed(ctx, "r_marked", fin); err != nil || !first {
		t.Fatal(first, err)
	}
	own, users := loadShadowFixtures(t)
	svc := room.New(room.Deps{Store: mem, Users: users, Owners: own, Now: c.Now, WriteHandoff: fakeWrite})
	t.Cleanup(svc.Close)
	if err := svc.Load(ctx); err != nil {
		t.Fatal(err)
	}
	// CloseRoom reaches revealLocked with the run finished and revealed_at set.
	arjun, _ := users.Get("arjun")
	if _, err := svc.GrantControl(ctx, arjun, "r_marked", "arjun"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CloseRoom(ctx, arjun, "r_marked", "fixed"); err != nil {
		t.Fatal(err)
	}
	if n := countReveals(mustSince(t, mem, "r_marked")); n != 0 {
		t.Fatalf("reveals = %d, want 0: revealed_at was already set", n)
	}
}

// queuedRetry sets up review round 2's path: attempt 1 fails while the room is
// open, a live room takes the only slot, and the retry ends up queued in
// gate.acquire when the shadow room closes with grace > 0.
func queuedRetry(t *testing.T) (env *shadowEnv, shadowID, liveID string) {
	t.Helper()
	failed := make(chan struct{}, 8)
	env = newShadowEnv(t, shadowCfg{maxAgents: 1, grace: 10 * time.Second, fn: func(ctx context.Context, _ int, d agent.Deps) error {
		failed <- struct{}{}
		return errors.New("test: model down")
	}})
	snap := env.open(t, "queued-retry")
	<-failed
	arjun := env.user(t, "arjun")
	live, err := env.svc.CreateRoom(context.Background(), arjun, "live", "sev2", "live")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return env.run.live.Load() == 1 })
	time.Sleep(1300 * time.Millisecond) // the 1s backoff is over: the retry is queued
	return env, snap.ID, live.ID
}

// A retry queued for a slot when the room closes (grace > 0) never runs, even
// once the slot frees: no second attempt on a closed room (design 3.3, M4).
func TestShadowQueuedRetryNotRunAfterClose(t *testing.T) {
	env, shadowID, liveID := queuedRetry(t)
	env.close(t, shadowID, "fixed by hand")
	if _, err := env.svc.CloseRoom(context.Background(), env.user(t, "arjun"), liveID, "done"); err != nil {
		t.Fatal(err)
	}
	r := env.waitFinished(t, shadowID)
	time.Sleep(300 * time.Millisecond)
	if r.Status != store.ShadowInterrupted || env.run.Calls() != 1 {
		t.Fatalf("status %q after %d calls, want interrupted after 1", r.Status, env.run.Calls())
	}
	if n := countReveals(env.roomLog(t, shadowID)); n != 1 {
		t.Fatalf("reveals = %d", n)
	}
}

// The same queued retry ends at the close itself: with no attempt running the
// close cancels the run instead of arming the grace timer, so it does not sit
// in the slot queue until a slot or the grace frees it.
func TestShadowQueuedRetryEndsAtClose(t *testing.T) {
	env, shadowID, _ := queuedRetry(t)
	env.close(t, shadowID, "fixed by hand") // the live room keeps the slot
	var r store.ShadowRun
	waitFor(t, 2*time.Second, func() bool {
		got, err := env.st.GetShadowRun(context.Background(), shadowID)
		r = got
		return err == nil && got.Finished()
	})
	if r.Status != store.ShadowInterrupted || env.run.Calls() != 1 {
		t.Fatalf("status %q after %d calls", r.Status, env.run.Calls())
	}
	if n := countReveals(env.roomLog(t, shadowID)); n != 1 {
		t.Fatalf("reveals = %d", n)
	}
}

// Grace 10s, a failure while open, and the close during the retry backoff: the
// retry never runs (the room-closed hook dropped the budget, design 3.3 M4),
// and the backoff wakes on the close, so the run ends at once.
func TestShadowClosedDuringBackoffNotRetried(t *testing.T) {
	failed := make(chan struct{}, 8)
	env := newShadowEnv(t, shadowCfg{grace: 10 * time.Second, fn: func(ctx context.Context, _ int, d agent.Deps) error {
		failed <- struct{}{}
		return errors.New("test: model down")
	}})
	snap := env.open(t, "backoff")
	<-failed
	time.Sleep(100 * time.Millisecond) // inside the 1s backoff
	start := time.Now()
	env.close(t, snap.ID, "fixed by hand")
	r := env.waitFinished(t, snap.ID)
	took := time.Since(start)
	time.Sleep(2500 * time.Millisecond) // past every backoff
	if r.Status != store.ShadowInterrupted || env.run.Calls() != 1 {
		t.Fatalf("status %q after %d calls, want interrupted after 1", r.Status, env.run.Calls())
	}
	if took > 500*time.Millisecond {
		t.Fatalf("the backoff did not wake on close: run ended %s after the close", took)
	}
	if n := countReveals(env.roomLog(t, snap.ID)); n != 1 {
		t.Fatalf("reveals = %d", n)
	}
}
