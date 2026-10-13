package ws_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/ws"
)

// goneStore answers the replay as a store does for a room deleted after the
// gateway looked it up.
type goneStore struct{ *memstore.Store }

func (goneStore) Since(context.Context, string, int64, int) ([]events.Event, error) {
	return nil, fmt.Errorf("gone: %w", store.ErrNotFound)
}

// TestReplayOfDeletedRoomClosesAsDeleted: a room deleted while a connection is
// still replaying (not yet registered, so CloseRoom cannot reach it) closes
// with the room-deleted reason, not an internal error.
func TestReplayOfDeletedRoomClosesAsDeleted(t *testing.T) {
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	gw := ws.NewGateway(svc, goneStore{st}, nil)
	gw.Identify = headerIdentify(svc)
	mux := chi.NewRouter()
	mux.Get("/api/rooms/{id}/ws", gw.ServeHTTP)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() { srv.Close(); gw.Close() })

	conn := dialWS(t, srv, snap.ID, "priya", 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		if _, _, err = conn.Read(ctx); err != nil {
			break
		}
	}
	if websocket.CloseStatus(err) != websocket.StatusNormalClosure || !strings.Contains(err.Error(), ws.RoomDeletedReason) {
		t.Fatalf("close = %v, want 1000 %q", err, ws.RoomDeletedReason)
	}
}

// TestCloseRoomLeavesOtherRooms: CloseRoom ends only the deleted room's
// connections.
func TestCloseRoomLeavesOtherRooms(t *testing.T) {
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	a, _ := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "a", "sev2", "orders")
	b, _ := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "b", "sev2", "orders")
	gw := ws.NewGateway(svc, st, nil)
	gw.Identify = headerIdentify(svc)
	mux := chi.NewRouter()
	mux.Get("/api/rooms/{id}/ws", gw.ServeHTTP)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() { srv.Close(); gw.Close() })

	connA := dialWS(t, srv, a.ID, "priya", 0)
	connB := dialWS(t, srv, b.ID, "priya", 0)
	readPresence(t, connA) // registered
	readPresence(t, connB)

	gw.CloseRoom(a.ID, events.Event{RoomID: a.ID, Seq: 3, Type: events.RoomDeleted, Payload: []byte(`{"reason":"x"}`)})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var lastType string
	var err error
	for {
		var m serverMsg
		_, data, rerr := connA.Read(ctx)
		if rerr != nil {
			err = rerr
			break
		}
		if jsonErr := json.Unmarshal(data, &m); jsonErr == nil && m.Event != nil {
			lastType = string(m.Event.Type)
		}
	}
	if websocket.CloseStatus(err) != websocket.StatusNormalClosure || lastType != string(events.RoomDeleted) {
		t.Fatalf("room a: close %v, last event %q", err, lastType)
	}
	short, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	for {
		_, _, rerr := connB.Read(short)
		if rerr == nil {
			continue
		}
		if websocket.CloseStatus(rerr) != -1 {
			t.Fatalf("room b was closed: %v", rerr)
		}
		break
	}
}
