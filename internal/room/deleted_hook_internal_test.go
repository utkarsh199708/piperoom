package room

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// noAudit is a store whose deletion audit rows cannot be read.
type noAudit struct{ *memstore.Store }

func (noAudit) GetRoomDeletion(context.Context, string) (store.RoomDeletion, error) {
	return store.RoomDeletion{}, errors.New("audit table unreachable")
}

type hookCall struct {
	roomID string
	ev     events.Event
}

func newElsewhereService(t *testing.T, st store.Store) (*Service, chan hookCall) {
	t.Helper()
	users, err := ParseUsers([]byte("users:\n  - id: arjun\n    name: Arjun\n    layers: [kafka]\n  - id: ada\n    name: Ada\n    admin: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	calls := make(chan hookCall, 4)
	s := New(Deps{Store: st, Users: users, Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
		<-ctx.Done()
		return nil
	}, OnRoomDeleted: func(roomID string, ev events.Event) { calls <- hookCall{roomID, ev} }})
	t.Cleanup(s.Close)
	return s, calls
}

func nextHookCall(t *testing.T, calls chan hookCall) hookCall {
	t.Helper()
	select {
	case c := <-calls:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("OnRoomDeleted was not called")
		return hookCall{}
	}
}

// A room another replica deleted is evicted when the follower's read finds it
// gone, and OnRoomDeleted gets the frame built from the room's audit row.
func TestEvictedElsewhereTellsOnRoomDeleted(t *testing.T) {
	st := memstore.New()
	s, calls := newElsewhereService(t, st)
	ctx := context.Background()
	arjun, _ := s.LookupUser("arjun")
	snap, err := s.CreateRoom(ctx, arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	// The other replica's delete, straight in the shared store.
	d, _, err := st.DeleteRoom(ctx, store.RoomDeletion{RoomID: snap.ID, DeletedBy: "ada", DeletedAt: time.Now().UTC(), Reason: "test data"}, -1)
	if err != nil {
		t.Fatal(err)
	}
	s.followRoom(snap.ID, snap.LastSeq+1)

	c := nextHookCall(t, calls)
	var p events.RoomDeletedPayload
	_ = json.Unmarshal(c.ev.Payload, &p)
	if c.roomID != snap.ID || c.ev.Type != events.RoomDeleted || c.ev.Seq != d.EventCount+1 ||
		c.ev.Actor.ID != "ada" || c.ev.Actor.Name != "Ada" || p.Reason != "test data" {
		t.Fatalf("hook call = %+v (%s)", c, c.ev.Payload)
	}
	s.mu.Lock()
	_, cached := s.rooms[snap.ID]
	s.mu.Unlock()
	if cached {
		t.Fatal("the deleted room is still cached")
	}
}

// When the audit row cannot be read the hook still runs, with the zero Event:
// the sockets get the close without the frame.
func TestEvictedElsewhereWithoutAuditRow(t *testing.T) {
	st := memstore.New()
	s, calls := newElsewhereService(t, noAudit{st})
	ctx := context.Background()
	arjun, _ := s.LookupUser("arjun")
	snap, err := s.CreateRoom(ctx, arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.DeleteRoom(ctx, store.RoomDeletion{RoomID: snap.ID, DeletedBy: "ada", DeletedAt: time.Now().UTC(), Reason: "x"}, -1); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.evictLocked(snap.ID, s.rooms[snap.ID])
	s.mu.Unlock()
	if c := nextHookCall(t, calls); c.roomID != snap.ID || c.ev.Type != "" {
		t.Fatalf("hook call = %+v, want the zero Event", c)
	}
	if _, ok := s.DeletedEvent(ctx, snap.ID); ok {
		t.Fatal("DeletedEvent ok without an audit row")
	}
}
