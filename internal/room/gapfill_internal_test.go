package room

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// failingSinceCoord is a store whose log reads can fail (failing), answer not
// found (notFound), leave one seq out of the page (drop), or run the next of
// afterReads right after a read from the start of the log (a rebuild) is
// taken, to change the store or block while the rebuild reads. sinces counts
// reads.
type failingSinceCoord struct {
	fakeCoord
	failing   atomic.Bool
	notFound  atomic.Bool
	drop      atomic.Int64
	sinces    atomic.Int32
	mu         sync.Mutex
	afterReads []func()
}

func (c *failingSinceCoord) Since(ctx context.Context, roomID string, after int64, limit int) ([]events.Event, error) {
	c.sinces.Add(1)
	if c.notFound.Load() {
		return nil, store.ErrNotFound
	}
	if c.failing.Load() {
		return nil, errors.New("connection reset")
	}
	evs, err := c.fakeCoord.Store.Since(ctx, roomID, after, limit)
	if d := c.drop.Load(); d != 0 {
		evs = slices.DeleteFunc(evs, func(e events.Event) bool { return e.Seq == d })
	}
	if after == 0 {
		c.mu.Lock()
		var f func()
		if len(c.afterReads) > 0 {
			f, c.afterReads = c.afterReads[0], c.afterReads[1:]
		}
		c.mu.Unlock()
		if f != nil {
			f()
		}
	}
	return evs, err
}

func (c *failingSinceCoord) onRebuildRead(f func()) {
	c.mu.Lock()
	c.afterReads = append(c.afterReads, f)
	c.mu.Unlock()
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

// emitEvent appends one event through the service as this replica would, and
// returns it.
func emitEvent(t *testing.T, svc *Service, roomID string, actor events.Actor, typ events.Type, payload any) events.Event {
	t.Helper()
	st := svc.lockCachedRoom(roomID)
	if st == nil {
		t.Fatalf("room %s not cached", roomID)
	}
	defer svc.unlockRoom(st)
	e, err := svc.emitLocked(context.Background(), st, roomID, typ, actor, payload)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// leased makes this replica the one that runs the room's agent, so folds feed
// its inbox.
func (r *staleRig) leased(t *testing.T) *roomState {
	t.Helper()
	st := r.svc.cached(r.roomID)
	r.svc.lockState(st)
	st.setLeased(true)
	r.svc.unlockRoom(st)
	return st
}

func inboxSeqs(st *roomState) []int64 {
	st.inbox.mu.Lock()
	defer st.inbox.mu.Unlock()
	seqs := make([]int64, 0, len(st.inbox.items))
	for _, in := range st.inbox.items {
		seqs = append(seqs, in.Seq)
	}
	return seqs
}

// While the store still cannot be read, the rebuild fails: the room stays
// stale with its snapshot as it was, and the rebuild is queued again; until
// that timer fires, notifies for the room read nothing.
func TestStaleRebuildFailureStaysStaleAndRetries(t *testing.T) {
	r := newStaleRig(t)
	remote := r.grant(t)
	r.emitOverFailedGap(t)
	before := mustRoom(t, r.svc, r.roomID)
	r.svc.follow.take() // markStaleLocked's immediate notify

	r.svc.followRoom(r.roomID, remote.Seq)
	st := r.svc.cached(r.roomID)
	if !st.stale.Load() {
		t.Fatal("room no longer stale after a failed rebuild")
	}
	if after := mustRoom(t, r.svc, r.roomID); after.LastSeq != before.LastSeq || after.Driver.ID != "arjun" {
		t.Fatalf("snapshot changed by a failed rebuild: last_seq %d -> %d, driver %v", before.LastSeq, after.LastSeq, after.Driver)
	}
	if !st.rebuildQueued.Load() {
		t.Fatal("failed rebuild not queued again")
	}
	r.fc.sinces.Store(0)
	r.svc.followRoom(r.roomID, remote.Seq)
	if n := r.fc.sinces.Load(); n != 0 {
		t.Fatalf("a notify while the retry is queued read the log %d times, want 0", n)
	}
	waitForCond(t, 3*time.Second, func() bool {
		r.svc.follow.mu.Lock()
		defer r.svc.follow.mu.Unlock()
		_, queued := r.svc.follow.dirty[r.roomID]
		return queued && !st.rebuildQueued.Load()
	}, "queued rebuild never handed back to the follower")
}

// A stale room whose lock is held (a command in flight) is not read: the
// rebuild is queued for later.
func TestStaleRebuildSkipsBusyRoom(t *testing.T) {
	r := newStaleRig(t)
	r.grant(t)
	r.emitOverFailedGap(t)
	r.fc.failing.Store(false)
	st := r.svc.cached(r.roomID)
	r.fc.sinces.Store(0)
	st.mu.Lock()
	r.svc.followRoom(r.roomID, 100)
	st.mu.Unlock()
	if n := r.fc.sinces.Load(); n != 0 {
		t.Fatalf("busy stale room read %d times, want 0", n)
	}
	if !st.stale.Load() || !st.rebuildQueued.Load() {
		t.Fatalf("busy stale room: stale=%v queued=%v, want both", st.stale.Load(), st.rebuildQueued.Load())
	}
}

// Our own append made while the rebuild reads is in the tail: the rebuild is
// swapped in with it, and its input (fed when it was folded) is not fed again.
func TestStaleRebuildKeepsOwnAppendsMadeDuringRead(t *testing.T) {
	r := newStaleRig(t)
	st := r.leased(t)
	r.grant(t)
	r.emitOverFailedGap(t)
	r.fc.failing.Store(false)
	var own events.Event
	r.fc.onRebuildRead(func() {
		own = emitEvent(t, r.svc, r.roomID, r.svc.humanActor(r.arjun), events.SteerSent, events.SteerSentPayload{Text: "mine"})
	})
	r.svc.followRoom(r.roomID, 100)
	got := mustRoom(t, r.svc, r.roomID)
	if own.Seq == 0 || st.stale.Load() || got.LastSeq != own.Seq || got.Driver == nil || got.Driver.ID != "priya" {
		t.Fatalf("after the rebuild: stale=%v last_seq=%d (want %d) driver=%v (want priya)", st.stale.Load(), got.LastSeq, own.Seq, got.Driver)
	}
	if seqs := inboxSeqs(st); !slices.Equal(seqs, []int64{own.Seq}) {
		t.Fatalf("inbox seqs = %v, want only our steer %d once", seqs, own.Seq)
	}
}

// When the tail cannot be read the rebuild is not installed: the room keeps
// its snapshot, stays stale and is queued again.
func TestStaleRebuildTailReadFailureLeavesRoom(t *testing.T) {
	r := newStaleRig(t)
	st := r.svc.cached(r.roomID)
	r.grant(t)
	r.emitOverFailedGap(t)
	r.fc.failing.Store(false)
	var own events.Event
	r.fc.onRebuildRead(func() {
		own = emitEvent(t, r.svc, r.roomID, executorActor, events.AgentMessage, events.AgentMessagePayload{Text: "y"})
		r.fc.failing.Store(true) // the tail read fails
	})
	r.svc.followRoom(r.roomID, 100)
	got := mustRoom(t, r.svc, r.roomID)
	if !st.stale.Load() || !st.rebuildQueued.Load() {
		t.Fatalf("after a failed tail read: stale=%v queued=%v, want both", st.stale.Load(), st.rebuildQueued.Load())
	}
	if own.Seq == 0 || got.LastSeq != own.Seq || got.Driver == nil || got.Driver.ID != "arjun" {
		t.Fatalf("snapshot changed by a failed swap: last_seq=%d (want %d) driver=%v (want arjun)", got.LastSeq, own.Seq, got.Driver)
	}
}

// A room deleted elsewhere while it was stale is evicted by its rebuild.
func TestStaleRebuildEvictsDeletedRoom(t *testing.T) {
	r := newStaleRig(t)
	r.grant(t)
	r.emitOverFailedGap(t)
	r.fc.failing.Store(false)
	r.fc.notFound.Store(true)
	r.svc.followRoom(r.roomID, 100)
	if r.svc.cached(r.roomID) != nil {
		t.Fatal("deleted stale room still cached after its rebuild")
	}
}

// A gap page that does not continue the snapshot (a hole) marks the room
// stale instead of folding our event over the hole.
func TestGapFillHoleMarksStale(t *testing.T) {
	r := newStaleRig(t)
	g1 := r.grant(t)
	r.remote(t, events.CommentAdded, r.svc.humanActor(r.arjun), events.CommentAddedPayload{Text: "c"})
	r.fc.drop.Store(g1.Seq)
	if err := r.svc.emitAs(context.Background(), r.roomID, executorActor, events.AgentMessage, events.AgentMessagePayload{Text: "x"}); err != nil {
		t.Fatal(err)
	}
	st := r.svc.cached(r.roomID)
	if !st.stale.Load() {
		t.Fatal("room not stale after a gap page with a hole")
	}
	r.fc.drop.Store(0)
	r.svc.followRoom(r.roomID, g1.Seq)
	if got := mustRoom(t, r.svc, r.roomID); got.Driver == nil || got.Driver.ID != "priya" {
		t.Fatalf("driver = %v after the rebuild, want priya", got.Driver)
	}
}

// On the replica that runs the room's agent, the rebuild feeds the inbox the
// human input the snapshot never folded: a steer in the skipped gap and one
// appended after it, in log order and once each; a steer folded before the
// gap is not fed again.
func TestStaleRebuildFeedsSkippedInput(t *testing.T) {
	r := newStaleRig(t)
	st := r.leased(t)
	driver := r.svc.humanActor(r.arjun)
	pre := r.remote(t, events.SteerSent, driver, events.SteerSentPayload{Text: "before the gap"})
	r.svc.followRoom(r.roomID, pre.Seq)
	inGap := r.remote(t, events.SteerSent, driver, events.SteerSentPayload{Text: "in the gap"})
	r.emitOverFailedGap(t)
	r.fc.failing.Store(false)
	after := r.remote(t, events.SteerSent, driver, events.SteerSentPayload{Text: "after the gap"})
	r.svc.followRoom(r.roomID, after.Seq)
	r.svc.followRoom(r.roomID, after.Seq) // a repeated notify feeds nothing twice

	want := []int64{pre.Seq, inGap.Seq, after.Seq}
	if seqs := inboxSeqs(st); !slices.Equal(seqs, want) {
		t.Fatalf("inbox seqs = %v, want %v", seqs, want)
	}
}

// A gap marked while the rebuild reads (a remote steer, then our append whose
// gap read fails) lies in the tail: its steer is fed too, once, in order.
func TestStaleRebuildFeedsGapMarkedDuringRead(t *testing.T) {
	r := newStaleRig(t)
	st := r.leased(t)
	driver := r.svc.humanActor(r.arjun)
	first := r.remote(t, events.SteerSent, driver, events.SteerSentPayload{Text: "first gap"})
	r.emitOverFailedGap(t)
	r.fc.failing.Store(false)
	var second, ours events.Event
	r.fc.onRebuildRead(func() {
		second = r.remote(t, events.SteerSent, driver, events.SteerSentPayload{Text: "second gap"})
		r.fc.failing.Store(true)
		ours = emitEvent(t, r.svc, r.roomID, executorActor, events.AgentMessage, events.AgentMessagePayload{Text: "y"})
		r.fc.failing.Store(false)
	})
	r.svc.followRoom(r.roomID, first.Seq)
	if got := mustRoom(t, r.svc, r.roomID); ours.Seq == 0 || st.stale.Load() || got.LastSeq != ours.Seq {
		t.Fatalf("after the rebuild: stale=%v last_seq=%d, want false and %d", st.stale.Load(), got.LastSeq, ours.Seq)
	}
	want := []int64{first.Seq, second.Seq}
	if seqs := inboxSeqs(st); !slices.Equal(seqs, want) {
		t.Fatalf("inbox seqs = %v, want %v", seqs, want)
	}
}

// blockRead makes the next rebuild read wait, once it has read the log, until
// release is closed; reading is closed when it starts waiting.
func (c *failingSinceCoord) blockRead() (reading, release chan struct{}) {
	reading, release = make(chan struct{}), make(chan struct{})
	c.onRebuildRead(func() {
		close(reading)
		<-release
	})
	return reading, release
}

// Two rebuilds overlap: a command's rebuild reads the log, a remote steer
// lands, the follower's rebuild reads it too, then the command's rebuild
// swaps first (without the steer). The follower's rebuild, finding the room
// no longer stale, must still fold and feed the steer, not drop its read.
func TestOverlappingRebuildsKeepRemoteSteer(t *testing.T) {
	r := newStaleRig(t)
	st := r.leased(t)
	r.grant(t)
	r.emitOverFailedGap(t)
	r.fc.failing.Store(false)
	ctx := context.Background()

	reading1, release1 := r.fc.blockRead()
	syncDone := make(chan struct{})
	go func() {
		defer close(syncDone)
		r.svc.syncRoom(ctx, r.roomID) // the command's rebuild (wait)
	}()
	<-reading1
	steer := r.remote(t, events.SteerSent, r.svc.humanActor(r.arjun), events.SteerSentPayload{Text: "late"})
	reading2, release2 := r.fc.blockRead()
	followDone := make(chan struct{})
	go func() {
		defer close(followDone)
		r.svc.followRoom(r.roomID, steer.Seq) // the steer's notify
	}()
	<-reading2
	close(release1)
	<-syncDone
	close(release2)
	<-followDone

	if got := mustRoom(t, r.svc, r.roomID); got.LastSeq != steer.Seq {
		t.Fatalf("last_seq = %d, want the steer's %d", got.LastSeq, steer.Seq)
	}
	if seqs := inboxSeqs(st); !slices.Equal(seqs, []int64{steer.Seq}) {
		t.Fatalf("inbox seqs = %v, want the steer %d once", seqs, steer.Seq)
	}
}

// The mirror: the follower's rebuild swaps first with an older read, and the
// command's rebuild, finding the room no longer stale, must still bring the
// room up to the row it read, so the command sees the steer.
func TestOverlappingRebuildsCommandReachesRow(t *testing.T) {
	r := newStaleRig(t)
	st := r.leased(t)
	r.grant(t)
	r.emitOverFailedGap(t)
	r.fc.failing.Store(false)
	ctx := context.Background()

	reading1, release1 := r.fc.blockRead()
	followDone := make(chan struct{})
	go func() {
		defer close(followDone)
		r.svc.followRoom(r.roomID, 100)
	}()
	<-reading1
	steer := r.remote(t, events.SteerSent, r.svc.humanActor(r.arjun), events.SteerSentPayload{Text: "late"})
	reading2, release2 := r.fc.blockRead()
	syncDone := make(chan struct{})
	go func() {
		defer close(syncDone)
		r.svc.syncRoom(ctx, r.roomID)
	}()
	<-reading2
	close(release1)
	<-followDone
	close(release2)
	<-syncDone

	if got := mustRoom(t, r.svc, r.roomID); got.LastSeq != steer.Seq {
		t.Fatalf("last_seq = %d after the command's sync, want the steer's %d", got.LastSeq, steer.Seq)
	}
	if seqs := inboxSeqs(st); !slices.Equal(seqs, []int64{steer.Seq}) {
		t.Fatalf("inbox seqs = %v, want the steer %d once", seqs, steer.Seq)
	}
}
