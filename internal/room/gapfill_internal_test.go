package room

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// cancelAfterAppendCoord ends the caller's context right after an append is
// stored (the client went away after the commit), and its reads honour the
// context like Postgres does.
type cancelAfterAppendCoord struct {
	fakeCoord
	mu     sync.Mutex
	cancel context.CancelFunc
}

func (c *cancelAfterAppendCoord) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	e, err := c.fakeCoord.Store.Append(ctx, roomID, typ, actor, payload)
	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	c.mu.Unlock()
	return e, err
}

func (c *cancelAfterAppendCoord) Since(ctx context.Context, roomID string, after int64, limit int) ([]events.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.fakeCoord.Store.Since(ctx, roomID, after, limit)
}

// Our append lands after another replica's; the gap read must not fail just
// because the request that appended has ended, or the other replica's event
// (here a control.granted) is skipped on this replica for good.
func TestGapFillSurvivesEndedRequest(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: leaseT0}
	ms := memstore.NewWithClock(c.Now)
	setup := newLeaseRig(t, ms, c)
	arjun := leaseUser(t, setup.svc, "arjun")
	snap, err := setup.svc.CreateRoom(ctx, arjun, "r", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	setup.svc.Close()

	cc := &cancelAfterAppendCoord{fakeCoord: fakeCoord{Store: ms, leases: map[string]store.RoomLease{}}}
	svc := New(Deps{Store: cc, Users: DefaultUsers(), Now: c.Now, ReplicaID: "replica-a"})
	t.Cleanup(svc.Close)
	mustRoom(t, svc, snap.ID)
	priya := leaseUser(t, svc, "priya")

	// Another replica moves the driver role; this one has not folded it.
	remote, err := ms.Append(ctx, snap.ID, events.ControlGranted, svc.humanActor(arjun),
		events.ControlGrantedPayload{To: svc.humanActor(priya), LeaseUntil: leaseT0.Add(DriverLease)})
	if err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cc.mu.Lock()
	cc.cancel = cancel
	cc.mu.Unlock()
	if err := svc.emitAs(rctx, snap.ID, executorActor, events.AgentMessage, events.AgentMessagePayload{Text: "x"}); err != nil {
		t.Fatal(err)
	}
	svc.followRoom(snap.ID, remote.Seq) // the follower's notify for the remote append

	after := mustRoom(t, svc, snap.ID)
	if after.Driver == nil || after.Driver.ID != "priya" {
		t.Fatalf("remote control.granted (seq %d) lost: driver=%v last_seq=%d", remote.Seq, after.Driver, after.LastSeq)
	}
}

// failingSinceCoord fails every log read while failing is set (a store that
// accepts our append, then drops every read).
type failingSinceCoord struct {
	fakeCoord
	failing atomic.Bool
}

func (c *failingSinceCoord) Since(ctx context.Context, roomID string, after int64, limit int) ([]events.Event, error) {
	if c.failing.Load() {
		return nil, errors.New("connection reset")
	}
	return c.fakeCoord.Store.Since(ctx, roomID, after, limit)
}

// When the gap still cannot be read after the retry, our event is folded over
// it; the room must then be rebuilt from the log once the store answers, or
// the other replica's control.granted is skipped here for good (#284).
func TestGapFillFailureRebuildsFromLog(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: leaseT0}
	ms := memstore.NewWithClock(c.Now)
	setup := newLeaseRig(t, ms, c)
	arjun := leaseUser(t, setup.svc, "arjun")
	snap, err := setup.svc.CreateRoom(ctx, arjun, "r", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	setup.svc.Close()

	fc := &failingSinceCoord{fakeCoord: fakeCoord{Store: ms, leases: map[string]store.RoomLease{}}}
	svc := New(Deps{Store: fc, Users: DefaultUsers(), Now: c.Now, ReplicaID: "replica-a"})
	t.Cleanup(svc.Close)
	mustRoom(t, svc, snap.ID)
	priya := leaseUser(t, svc, "priya")

	remote, err := ms.Append(ctx, snap.ID, events.ControlGranted, svc.humanActor(arjun),
		events.ControlGrantedPayload{To: svc.humanActor(priya), LeaseUntil: leaseT0.Add(DriverLease)})
	if err != nil {
		t.Fatal(err)
	}
	fc.failing.Store(true)
	if err := svc.emitAs(ctx, snap.ID, executorActor, events.AgentMessage, events.AgentMessagePayload{Text: "x"}); err != nil {
		t.Fatal(err)
	}
	fc.failing.Store(false)
	svc.followRoom(snap.ID, remote.Seq) // the follower's notify for the remote append

	after := mustRoom(t, svc, snap.ID)
	if after.Driver == nil || after.Driver.ID != "priya" {
		t.Fatalf("remote control.granted (seq %d) lost after a failed gap read: driver=%v last_seq=%d", remote.Seq, after.Driver, after.LastSeq)
	}
	if after.LastSeq != remote.Seq+1 {
		t.Fatalf("last_seq = %d, want %d", after.LastSeq, remote.Seq+1)
	}
}

// staleRig is a room on replica-a whose store drops every read while failing
// is set; the room was opened on another service so this one holds no state
// beyond what it loads.
type staleRig struct {
	ms     *memstore.Store
	fc     *failingSinceCoord
	svc    *Service
	roomID string
	arjun  User
	priya  User
	closed atomic.Int32
}

func newStaleRig(t *testing.T) *staleRig {
	t.Helper()
	ctx := context.Background()
	c := &testClock{t: leaseT0}
	r := &staleRig{ms: memstore.NewWithClock(c.Now)}
	setup := newLeaseRig(t, r.ms, c)
	r.arjun = leaseUser(t, setup.svc, "arjun")
	snap, err := setup.svc.CreateRoom(ctx, r.arjun, "r", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	setup.svc.Close()
	r.roomID = snap.ID
	r.fc = &failingSinceCoord{fakeCoord: fakeCoord{Store: r.ms, leases: map[string]store.RoomLease{}}}
	r.svc = New(Deps{
		Store: r.fc, Users: DefaultUsers(), Now: c.Now, ReplicaID: "replica-a",
		OnRoomClosed: func(context.Context, string, string, string) { r.closed.Add(1) },
	})
	t.Cleanup(r.svc.Close)
	mustRoom(t, r.svc, r.roomID)
	r.priya = leaseUser(t, r.svc, "priya")
	return r
}

// remote appends an event as another replica would.
func (r *staleRig) remote(t *testing.T, typ events.Type, actor events.Actor, payload any) events.Event {
	t.Helper()
	e, err := r.ms.Append(context.Background(), r.roomID, typ, actor, payload)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// emitOverFailedGap appends our own event while every read fails, so the
// gap before it is skipped and the room is marked stale.
func (r *staleRig) emitOverFailedGap(t *testing.T) {
	t.Helper()
	r.fc.failing.Store(true)
	if err := r.svc.emitAs(context.Background(), r.roomID, executorActor, events.AgentMessage, events.AgentMessagePayload{Text: "x"}); err != nil {
		t.Fatal(err)
	}
	if st := r.svc.cached(r.roomID); st == nil || !st.stale.Load() {
		t.Fatal("room not marked stale after the failed gap read")
	}
}

func (r *staleRig) grant(t *testing.T) events.Event {
	t.Helper()
	return r.remote(t, events.ControlGranted, r.svc.humanActor(r.arjun),
		events.ControlGrantedPayload{To: r.svc.humanActor(r.priya), LeaseUntil: leaseT0.Add(DriverLease)})
}

// While the store still cannot be read, the rebuild fails: the room stays
// stale with its snapshot as it was, and the rebuild is queued again.
func TestStaleRebuildFailureStaysStaleAndRetries(t *testing.T) {
	r := newStaleRig(t)
	remote := r.grant(t)
	r.emitOverFailedGap(t)
	before := mustRoom(t, r.svc, r.roomID)

	r.svc.follow.take() // forget the queue fillGapLocked's rebuildLater may fill
	r.svc.followRoom(r.roomID, remote.Seq)
	st := r.svc.cached(r.roomID)
	if !st.stale.Load() {
		t.Fatal("room no longer stale after a failed rebuild")
	}
	if after := mustRoom(t, r.svc, r.roomID); after.LastSeq != before.LastSeq || after.Driver.ID != "arjun" {
		t.Fatalf("snapshot changed by a failed rebuild: last_seq %d -> %d, driver %v", before.LastSeq, after.LastSeq, after.Driver)
	}
	waitForCond(t, 3*time.Second, func() bool {
		r.svc.follow.mu.Lock()
		defer r.svc.follow.mu.Unlock()
		_, queued := r.svc.follow.dirty[r.roomID]
		return queued
	}, "rebuild not queued again after it failed")
}

// A room.closed in the skipped gap is applied by the rebuild, and the
// room-closed hook runs once, as closedElsewhereLocked does for a fold.
func TestStaleRebuildAppliesRemoteClose(t *testing.T) {
	r := newStaleRig(t)
	closed := r.remote(t, events.RoomClosed, r.svc.humanActor(r.arjun), events.RoomClosedPayload{Resolution: "done"})
	r.emitOverFailedGap(t)
	if got := mustRoom(t, r.svc, r.roomID); got.Status != "open" {
		t.Fatalf("status = %q before the rebuild, want open (the close was skipped)", got.Status)
	}
	r.fc.failing.Store(false)
	r.svc.followRoom(r.roomID, closed.Seq)
	if got := mustRoom(t, r.svc, r.roomID); got.Status != "closed" {
		t.Fatalf("status = %q after the rebuild, want closed", got.Status)
	}
	waitForCond(t, 3*time.Second, func() bool { return r.closed.Load() == 1 }, "room-closed hook not called")
	time.Sleep(50 * time.Millisecond)
	if n := r.closed.Load(); n != 1 {
		t.Fatalf("room-closed hook called %d times, want 1", n)
	}
}

// A command's sync rebuilds a stale room before the command runs, so it sees
// the remote control.granted the snapshot skipped.
func TestStaleRoomRebuiltBeforeCommand(t *testing.T) {
	r := newStaleRig(t)
	r.grant(t)
	r.emitOverFailedGap(t)
	r.fc.failing.Store(false)
	r.svc.syncRoom(context.Background(), r.roomID)
	if got := mustRoom(t, r.svc, r.roomID); got.Driver == nil || got.Driver.ID != "priya" {
		t.Fatalf("driver = %v after the sync, want priya", got.Driver)
	}
	if r.svc.cached(r.roomID).stale.Load() {
		t.Fatal("room still stale after the sync")
	}
}

// On the replica that runs the room's agent, the rebuild feeds the inbox the
// human input the snapshot never folded: a steer in the skipped gap and one
// appended after it, in log order and once each.
func TestStaleRebuildFeedsSkippedInput(t *testing.T) {
	r := newStaleRig(t)
	st := r.svc.cached(r.roomID)
	r.svc.lockState(st)
	st.setLeased(true)
	r.svc.unlockRoom(st)

	driver := r.svc.humanActor(r.arjun)
	inGap := r.remote(t, events.SteerSent, driver, events.SteerSentPayload{Text: "in the gap"})
	r.emitOverFailedGap(t)
	r.fc.failing.Store(false)
	after := r.remote(t, events.SteerSent, driver, events.SteerSentPayload{Text: "after the gap"})
	r.svc.followRoom(r.roomID, after.Seq)
	r.svc.followRoom(r.roomID, after.Seq) // a repeated notify feeds nothing twice

	st.inbox.mu.Lock()
	items := append([]agent.HumanInput(nil), st.inbox.items...)
	st.inbox.mu.Unlock()
	if len(items) != 2 || items[0].Seq != inGap.Seq || items[1].Seq != after.Seq {
		t.Fatalf("inbox = %+v, want the steers at seq %d and %d", items, inGap.Seq, after.Seq)
	}
}

func waitForCond(t *testing.T, d time.Duration, ok func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
