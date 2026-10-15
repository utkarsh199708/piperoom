package room

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

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
