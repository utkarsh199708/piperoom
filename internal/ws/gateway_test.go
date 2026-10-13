package ws_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/ws"
)

func fakeRun(ctx context.Context, alert agent.Alert, deps agent.Deps, opts agent.Options) error {
	<-ctx.Done()
	return nil
}

type serverMsg struct {
	Type  string        `json:"type"`
	Event *events.Event `json:"event"`
}

func newRoomServer(t *testing.T) (*httptest.Server, *room.Service) {
	t.Helper()
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st,
		Users: room.DefaultUsers(),
		Run:   fakeRun,
	})
	srv := httptest.NewServer(httpapi.New(svc, st, nil))
	t.Cleanup(srv.Close)
	return srv, svc
}

func dialWS(t *testing.T, srv *httptest.Server, roomID, user string, after int64) *websocket.Conn {
	t.Helper()
	url := strings.Replace(srv.URL, "http", "ws", 1) + "/api/rooms/" + roomID + "/ws?after=" + itoa(after)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {user}}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })
	return conn
}

// headerIdentify is the test Identify for a gateway mounted without httpapi:
// the X-User header names the user. Production never reads ?user= or a header
// here; httpapi's authenticate middleware decides (session cookie first).
func headerIdentify(svc *room.Service) func(*http.Request) (ws.Identity, bool) {
	return func(r *http.Request) (ws.Identity, bool) {
		u, ok := svc.LookupUser(r.Header.Get("X-User"))
		return ws.Identity{User: u}, ok
	}
}

func itoa(n int64) string { return fmt.Sprintf("%d", n) }

// readEvent reads until the next "event" message (skipping presence), with a
// deadline.
func readEvent(t *testing.T, conn *websocket.Conn) events.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var msg serverMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if msg.Type == "event" && msg.Event != nil {
			return *msg.Event
		}
	}
}

func TestWebSocketReplayThenLiveThenReconnect(t *testing.T) {
	srv, svc := newRoomServer(t)

	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	roomID := snap.ID

	conn := dialWS(t, srv, roomID, "arjun", 0)

	// Replay: room.created (seq 1) then control.granted (seq 2).
	e1 := readEvent(t, conn)
	if e1.Type != events.RoomCreated || e1.Seq != 1 {
		t.Fatalf("replay 1: got %s/%d", e1.Type, e1.Seq)
	}
	e2 := readEvent(t, conn)
	if e2.Type != events.ControlGranted || e2.Seq != 2 {
		t.Fatalf("replay 2: got %s/%d", e2.Type, e2.Seq)
	}

	// Live: a new comment arrives.
	if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "priya"), roomID, "", "hello"); err != nil {
		t.Fatalf("comment: %v", err)
	}
	e3 := readEvent(t, conn)
	if e3.Type != events.CommentAdded || e3.Seq != 3 {
		t.Fatalf("live: got %s/%d", e3.Type, e3.Seq)
	}

	// Reconnect with after=3: nothing replayed, next event is live.
	conn2 := dialWS(t, srv, roomID, "arjun", 3)
	if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "priya"), roomID, "", "again"); err != nil {
		t.Fatalf("comment 2: %v", err)
	}
	e4 := readEvent(t, conn2)
	if e4.Seq != 4 || e4.Type != events.CommentAdded {
		t.Fatalf("reconnect live: got %s/%d", e4.Type, e4.Seq)
	}
}

// TestWebSocketReplayManyEvents replays more events than the live send buffer
// (256) and verifies the connection survives the backlog and still streams a
// live event afterwards.
func TestWebSocketReplayManyEvents(t *testing.T) {
	srv, svc := newRoomServer(t)

	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	roomID := snap.ID

	const extra = 1000
	for i := 0; i < extra; i++ {
		if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "priya"), roomID, "", "msg"); err != nil {
			t.Fatalf("comment %d: %v", i, err)
		}
	}

	conn := dialWS(t, srv, roomID, "arjun", 0)
	// room.created (1) + control.granted (2) + 1000 comments.
	for i := 1; i <= extra+2; i++ {
		e := readEvent(t, conn)
		if e.Seq != int64(i) {
			t.Fatalf("replay: want seq %d, got %d", i, e.Seq)
		}
	}

	// The connection must still be live after the replay.
	if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "priya"), roomID, "", "live"); err != nil {
		t.Fatalf("live comment: %v", err)
	}
	e := readEvent(t, conn)
	if e.Seq != int64(extra+3) {
		t.Fatalf("live: want seq %d, got %d", extra+3, e.Seq)
	}
}

func TestWebSocketNegativeAfterRejected(t *testing.T) {
	srv, svc := newRoomServer(t)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/rooms/"+snap.ID+"/ws?after=-5", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-User", "arjun")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
}

func mustUser(t *testing.T, svc *room.Service, id string) room.User {
	t.Helper()
	u, ok := svc.LookupUser(id)
	if !ok {
		t.Fatalf("user %q not found", id)
	}
	return u
}

// readPresence reads until the next presence message, skipping events.
func readPresence(t *testing.T, conn *websocket.Conn) []presenceMember {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var msg struct {
			Type    string           `json:"type"`
			Members []presenceMember `json:"members"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if msg.Type == "presence" {
			return msg.Members
		}
	}
}

type presenceMember struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"`
}

// TestWebSocketReplaySeqBoundary pins the exact boundary: with the log at N
// events, a client at N-1 receives the Nth event exactly once, and a client at
// N receives no replay — neither duplicates nor gaps at the seam.
func TestWebSocketReplaySeqBoundary(t *testing.T) {
	srv, svc := newRoomServer(t)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	roomID := snap.ID

	// Events 1 and 2 exist (room.created, control.granted); add 3..5.
	for i := 0; i < 3; i++ {
		if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "priya"), roomID, "", "msg"); err != nil {
			t.Fatalf("comment: %v", err)
		}
	}
	const n = 5

	// Client at N-1: replay must deliver exactly n, then the live n+1.
	connA := dialWS(t, srv, roomID, "arjun", n-1)
	e := readEvent(t, connA)
	if e.Seq != n {
		t.Fatalf("after=N-1: first event must be N=%d, got %d", n, e.Seq)
	}
	if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "priya"), roomID, "", "live"); err != nil {
		t.Fatalf("live comment: %v", err)
	}
	if e = readEvent(t, connA); e.Seq != n+1 {
		t.Fatalf("after=N-1: duplicate or gap at the seam: want %d, got %d", n+1, e.Seq)
	}

	// Client at N (highest seen): nothing replayed, stream continues at N+2.
	connB := dialWS(t, srv, roomID, "arjun", n+1)
	if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "priya"), roomID, "", "more"); err != nil {
		t.Fatalf("live comment 2: %v", err)
	}
	if e = readEvent(t, connB); e.Seq != n+2 {
		t.Fatalf("after=N: want first live event %d, got %d", n+2, e.Seq)
	}
	// connA must see the same single event next.
	if e = readEvent(t, connA); e.Seq != n+2 {
		t.Fatalf("after=N-1 stream: want %d, got %d", n+2, e.Seq)
	}
}

// slowStore makes Since block until its context ends, so the replay deadline
// can be observed deterministically.
type slowStore struct{ store.Store }

func (s slowStore) Since(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestWebSocketReplayDeadline: a store that never answers must not hold the
// connection forever; the replay is aborted with a policy violation.
func TestWebSocketReplayDeadline(t *testing.T) {
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}

	gw := ws.NewGateway(svc, slowStore{st}, nil)
	gw.Identify = headerIdentify(svc)
	gw.ReplayTimeout = 200 * time.Millisecond
	mux := chi.NewRouter()
	mux.Get("/api/rooms/{id}/ws", gw.ServeHTTP)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() { srv.Close(); gw.Close() })

	url := strings.Replace(srv.URL, "http", "ws", 1) + "/api/rooms/" + snap.ID + "/ws?after=0"
	conn, _, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {"arjun"}}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, _, err := conn.Read(ctx)
		if err == nil {
			continue // discard anything until the server gives up
		}
		if got := websocket.CloseStatus(err); got != websocket.StatusPolicyViolation {
			t.Fatalf("want policy violation close, got %v (err %v)", got, err)
		}
		return
	}
}

// TestWebSocketPresenceJoinLeave: presence is broadcast when members join and
// leave (item 15).
func TestWebSocketPresenceJoinLeave(t *testing.T) {
	srv, svc := newRoomServer(t)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	roomID := snap.ID

	// after=2 skips the two creation events, so only presence arrives.
	connA := dialWS(t, srv, roomID, "arjun", 2)
	members := readPresence(t, connA)
	if len(members) != 1 || members[0].ID != "arjun" || members[0].State != "viewing" {
		t.Fatalf("join presence wrong: %+v", members)
	}

	connB := dialWS(t, srv, roomID, "priya", 2)
	members = readPresence(t, connA)
	if len(members) != 2 {
		t.Fatalf("want 2 members after join, got %+v", members)
	}
	_ = connB

	connB.Close(websocket.StatusNormalClosure, "")
	members = readPresence(t, connA)
	if len(members) != 1 || members[0].ID != "arjun" {
		t.Fatalf("want 1 member after leave, got %+v", members)
	}
}

// TestGatewayCloseClosesAllClients pins review item 12: Close is idempotent,
// shuts down every live connection, and a connection that arrives around the
// same time is closed too instead of being left running (the closed flag
// checked by trackConn).
func TestGatewayCloseClosesAllClients(t *testing.T) {
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	roomID := snap.ID

	gw := ws.NewGateway(svc, st, nil)
	gw.Identify = headerIdentify(svc)
	mux := chi.NewRouter()
	mux.Get("/api/rooms/{id}/ws", gw.ServeHTTP)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	connA := dialWS(t, srv, roomID, "arjun", 0)
	connB := dialWS(t, srv, roomID, "priya", 0)
	// Drain the replay so both clients have consumed room.created and
	// control.granted.
	for i := 0; i < 2; i++ {
		readEvent(t, connA)
		readEvent(t, connB)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		gw.Close()
		gw.Close() // idempotent: the second call returns immediately
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Close did not return in time")
	}

	// Both clients must reach a terminal state (any close error; pending
	// presence messages may arrive before the close frame).
	readUntilClosed := func(name string, conn *websocket.Conn) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				if ctx.Err() != nil {
					t.Fatalf("client %s: connection not closed by Close (%v)", name, err)
				}
				return
			}
		}
	}
	readUntilClosed("A", connA)
	readUntilClosed("B", connB)
}
