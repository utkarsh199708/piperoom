package ws_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
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

// endingStore hands out subscriptions the test ends by hand, as a store
// whose feed stopped for a reason other than a deletion.
type endingStore struct {
	*memstore.Store
	mu   sync.Mutex
	subs []chan events.Event
}

func (e *endingStore) Subscribe(ctx context.Context, roomID string, afterSeq int64) (<-chan events.Event, error) {
	if _, err := e.Store.GetRoom(ctx, roomID); err != nil {
		return nil, err
	}
	ch := make(chan events.Event)
	e.mu.Lock()
	e.subs = append(e.subs, ch)
	e.mu.Unlock()
	return ch, nil
}

func (e *endingStore) endAll() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, ch := range e.subs {
		close(ch)
	}
	e.subs = nil
}

// deleteUsers is a directory with ada, an admin.
func deleteUsers(t *testing.T) *room.Users {
	t.Helper()
	users, err := room.ParseUsers([]byte("users:\n  - id: priya\n    name: Priya\n    layers: [postgres]\n  - id: ada\n    name: Ada\n    admin: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	return users
}

// serveGateway mounts a gateway on st for svc, with no OnRoomDeleted hook and
// no HTTP delete handler: nothing calls CloseRoom.
func serveGateway(t *testing.T, svc *room.Service, st store.Store) (*httptest.Server, *ws.Gateway) {
	t.Helper()
	gw := ws.NewGateway(svc, st, nil)
	gw.Identify = headerIdentify(svc)
	mux := chi.NewRouter()
	mux.Get("/api/rooms/{id}/ws", gw.ServeHTTP)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() { srv.Close(); gw.Close() })
	return srv, gw
}

// readToClose reads until the server closes and returns the close error and
// every room.deleted frame seen.
func readToClose(t *testing.T, conn *websocket.Conn) (error, []events.Event) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var deleted []events.Event
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err, deleted
		}
		var m serverMsg
		if json.Unmarshal(data, &m) == nil && m.Event != nil && m.Event.Type == events.RoomDeleted {
			deleted = append(deleted, *m.Event)
		}
	}
}

// A room deleted with no CloseRoom call (another replica's delete or purge,
// before this replica's service notices) still ends its sockets: the store
// closes the subscription, and the gateway sends the room.deleted frame from
// the audit row and the room-deleted close.
func TestFeedEndOfDeletedRoomClosesAsDeleted(t *testing.T) {
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: deleteUsers(t), Run: fakeRun})
	t.Cleanup(svc.Close)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "priya"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	srv, _ := serveGateway(t, svc, st)
	conn := dialWS(t, srv, snap.ID, "priya", 0)
	readPresence(t, conn)

	d, err := svc.DeleteRoom(context.Background(), mustUser(t, svc, "ada"), snap.ID, "test data", false)
	if err != nil {
		t.Fatal(err)
	}
	cerr, deleted := readToClose(t, conn)
	if websocket.CloseStatus(cerr) != websocket.StatusNormalClosure || !strings.Contains(cerr.Error(), ws.RoomDeletedReason) {
		t.Fatalf("close = %v, want 1000 %q", cerr, ws.RoomDeletedReason)
	}
	if len(deleted) != 1 || deleted[0].Seq != d.EventCount+1 || deleted[0].Actor.ID != "ada" {
		t.Fatalf("room.deleted frames = %+v, want one from ada with seq %d", deleted, d.EventCount+1)
	}
}

// A subscription that ends while the room still exists is not reported as a
// deletion: the socket closes with an internal error so the client
// reconnects with after=.
func TestFeedEndOfLiveRoomIsInternalError(t *testing.T) {
	st := &endingStore{Store: memstore.New()}
	svc := room.New(room.Deps{Store: st, Users: deleteUsers(t), Run: fakeRun})
	t.Cleanup(svc.Close)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "priya"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	srv, gw := serveGateway(t, svc, st)
	conn := dialWS(t, srv, snap.ID, "priya", 0)
	readPresence(t, conn)
	// The subscription exists once the connection is registered and its
	// event loop runs; wait for it before ending it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		st.mu.Lock()
		n := len(st.subs)
		st.mu.Unlock()
		if n == 1 && gw.ConnCount() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no subscription")
		}
		time.Sleep(5 * time.Millisecond)
	}
	st.endAll()
	cerr, deleted := readToClose(t, conn)
	if websocket.CloseStatus(cerr) != websocket.StatusInternalError || !strings.Contains(cerr.Error(), "subscription ended") {
		t.Fatalf("close = %v, want 1011 subscription ended", cerr)
	}
	if len(deleted) != 0 {
		t.Fatalf("room.deleted frames for a live room: %+v", deleted)
	}
}

// CloseRoom is idempotent: the room service's hook and the HTTP delete
// handler both call it for an admin delete, and each socket gets one
// room.deleted frame. The zero Event closes without a frame.
func TestCloseRoomOnceAndWithoutFrame(t *testing.T) {
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	a, _ := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "a", "sev2", "orders")
	b, _ := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "b", "sev2", "orders")
	srv, gw := serveGateway(t, svc, st)
	connA := dialWS(t, srv, a.ID, "priya", 0)
	connB := dialWS(t, srv, b.ID, "priya", 0)
	readPresence(t, connA)
	readPresence(t, connB)

	frame := events.Event{RoomID: a.ID, Seq: 3, Type: events.RoomDeleted, Payload: []byte(`{"reason":"x"}`)}
	gw.CloseRoom(a.ID, frame)
	gw.CloseRoom(a.ID, frame)
	gw.CloseRoom(a.ID, events.Event{})
	cerr, deleted := readToClose(t, connA)
	if websocket.CloseStatus(cerr) != websocket.StatusNormalClosure || len(deleted) != 1 {
		t.Fatalf("room a: close %v, %d room.deleted frames, want 1000 and one", cerr, len(deleted))
	}

	gw.CloseRoom(b.ID, events.Event{})
	cerr, deleted = readToClose(t, connB)
	if websocket.CloseStatus(cerr) != websocket.StatusNormalClosure || !strings.Contains(cerr.Error(), ws.RoomDeletedReason) || len(deleted) != 0 {
		t.Fatalf("room b: close %v, frames %+v; want the close only", cerr, deleted)
	}
}
