package room_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

type deletedCall struct {
	roomID string
	ev     events.Event
}

// deletedRec records OnRoomDeleted calls. Each call also checks that the hook
// does not run under the service lock: a GetRoom started from inside it must
// finish while the hook still waits.
type deletedRec struct {
	t     *testing.T
	svc   *room.Service
	mu    sync.Mutex
	calls []deletedCall
}

func (r *deletedRec) hook(roomID string, ev events.Event) {
	done := make(chan struct{})
	go func() {
		_, _ = r.svc.GetRoom(context.Background(), "r_probe")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		r.t.Errorf("OnRoomDeleted for %s runs under the service lock", roomID)
	}
	r.mu.Lock()
	r.calls = append(r.calls, deletedCall{roomID, ev})
	r.mu.Unlock()
}

func (r *deletedRec) got() []deletedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]deletedCall(nil), r.calls...)
}

func newDeletedEnv(t *testing.T) (*room.Service, *memstore.Store, *clock, *deletedRec) {
	t.Helper()
	c := &clock{t: time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	rec := &deletedRec{t: t}
	svc := room.New(room.Deps{
		Store: st, Users: adminUsers(t), Owners: testOwners(), Run: fakeRun, WriteHandoff: fakeWrite,
		Now: c.Now, OnRoomDeleted: rec.hook,
	})
	rec.svc = svc
	t.Cleanup(svc.Close)
	return svc, st, c, rec
}

func deletedReason(t *testing.T, ev events.Event) string {
	t.Helper()
	var p events.RoomDeletedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("room.deleted payload %s: %v", ev.Payload, err)
	}
	return p.Reason
}

// An admin delete hands the room's WebSockets their last frame through
// OnRoomDeleted before DeleteRoom returns: the admin, the reason, seq last+1.
// A repeat delete does not call it again.
func TestDeleteRoomTellsOnRoomDeleted(t *testing.T) {
	svc, _, c, rec := newDeletedEnv(t)
	ctx := context.Background()
	snap := createRoom(t, svc, "arjun")
	if _, err := svc.AddComment(ctx, mustUser(t, svc, "priya"), snap.ID, "", "hello"); err != nil {
		t.Fatal(err)
	}
	c.Advance(time.Hour)
	ada := mustUser(t, svc, "ada")
	d, err := svc.DeleteRoom(ctx, ada, snap.ID, "pasted a password", false)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	calls := rec.got()
	if len(calls) != 1 || calls[0].roomID != snap.ID {
		t.Fatalf("hook calls = %+v, want one for %s", calls, snap.ID)
	}
	ev := calls[0].ev
	wantActor := events.Actor{Kind: events.ActorHuman, ID: "ada", Name: "Ada", Verified: true}
	if ev.Type != events.RoomDeleted || ev.RoomID != snap.ID || ev.Seq != d.EventCount+1 || ev.Actor != wantActor ||
		!ev.At.Equal(d.DeletedAt) || deletedReason(t, ev) != "pasted a password" {
		t.Fatalf("frame = %+v (%s), deletion %+v", ev, ev.Payload, d)
	}

	if _, err := svc.DeleteRoom(ctx, ada, snap.ID, "again", false); err != nil {
		t.Fatalf("repeat delete: %v", err)
	}
	if n := len(rec.got()); n != 1 {
		t.Fatalf("hook calls after a repeat = %d, want 1", n)
	}
}

// A retention purge hands each purged room's last frame to OnRoomDeleted, from
// the system actor with the retention reason; rooms it keeps get nothing.
func TestPurgeTellsOnRoomDeleted(t *testing.T) {
	svc, _, c, rec := newDeletedEnv(t)
	ctx := context.Background()
	day := 24 * time.Hour
	old := createRoom(t, svc, "arjun")
	kept := createRoom(t, svc, "arjun")
	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), old.ID, "fixed"); err != nil {
		t.Fatalf("close: %v", err)
	}
	// The close drafts its postmortem in the background; it is in the log
	// before the purge, so the frame's seq does not depend on which comes
	// first.
	var last room.RoomSnapshot
	waitFor(t, 5*time.Second, func() bool {
		snap, err := svc.GetRoom(ctx, old.ID)
		last = snap
		return err == nil && snap.Postmortem.Status == room.PostmortemDrafted
	})
	c.Advance(40 * day)
	if _, err := svc.PurgeExpired(ctx, c.Now().Add(-30*day)); err != nil {
		t.Fatalf("purge: %v", err)
	}
	calls := rec.got()
	if len(calls) != 1 || calls[0].roomID != old.ID {
		t.Fatalf("hook calls = %+v, want one for %s (not %s)", calls, old.ID, kept.ID)
	}
	ev := calls[0].ev
	if ev.Type != events.RoomDeleted || ev.Seq != last.LastSeq+1 || ev.Actor.Kind != events.ActorSystem ||
		ev.Actor.ID != "system" || !ev.At.Equal(c.Now()) || deletedReason(t, ev) != "retention: closed 30 days" {
		t.Fatalf("frame = %+v (%s)", ev, ev.Payload)
	}
}
