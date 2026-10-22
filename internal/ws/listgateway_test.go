package ws_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

type listFrame struct {
	Type   string              `json:"type"`
	Rooms  []room.RoomSnapshot `json:"rooms"`
	Room   *room.RoomSnapshot  `json:"room"`
	RoomID string              `json:"room_id"`
	raw    string
}

// listClient reads a list socket on its own goroutine: a websocket Read
// whose context ends closes the connection, so "no frame within d" must not
// be a Read deadline.
type listClient struct {
	conn   *websocket.Conn
	frames chan listFrame
}

func dialList(t *testing.T, srv *httptest.Server, user string) *listClient {
	t.Helper()
	url := strings.Replace(srv.URL, "http", "ws", 1) + "/api/rooms/ws"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {user}}})
	if err != nil {
		t.Fatalf("dial list: %v", err)
	}
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })
	lc := &listClient{conn: conn, frames: make(chan listFrame, 64)}
	go func() {
		defer close(lc.frames)
		for {
			_, data, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			var f listFrame
			if json.Unmarshal(data, &f) != nil {
				return
			}
			f.raw = string(data)
			lc.frames <- f
		}
	}()
	return lc
}

// readList returns the next frame within d; ok is false on timeout or close.
func readList(t *testing.T, lc *listClient, d time.Duration) (listFrame, bool) {
	t.Helper()
	select {
	case f, ok := <-lc.frames:
		return f, ok
	case <-time.After(d):
		return listFrame{}, false
	}
}

func getRoomsJSON(t *testing.T, srv *httptest.Server, user string) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/rooms", nil)
	req.Header.Set("X-User", user)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/rooms = %d %s", resp.StatusCode, b)
	}
	return strings.TrimSpace(string(b))
}

// roomIDs returns the ids of a GET /api/rooms body, in order.
func roomIDs(t *testing.T, body string) []string {
	t.Helper()
	var rooms []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &rooms); err != nil {
		t.Fatalf("decode rooms: %v", err)
	}
	ids := make([]string, len(rooms))
	for i := range rooms {
		ids[i] = rooms[i].ID
	}
	return ids
}

// frameRooms returns the "rooms" member of a list frame exactly as sent.
func frameRooms(t *testing.T, f listFrame) string {
	t.Helper()
	var env struct {
		Rooms json.RawMessage `json:"rooms"`
	}
	if err := json.Unmarshal([]byte(f.raw), &env); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	return string(env.Rooms)
}

// fixedClockServer is newRoomServer with one clock for the service and the
// store, so rooms opened back-to-back share an opened_at to the nanosecond:
// only the id tiebreaker can order them.
func fixedClockServer(t *testing.T, now time.Time) (*httptest.Server, *room.Service) {
	t.Helper()
	clock := func() time.Time { return now }
	st := memstore.NewWithClock(clock)
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun, Now: clock})
	srv := httptest.NewServer(httpapi.New(svc, st, nil))
	t.Cleanup(srv.Close)
	t.Cleanup(svc.Close)
	return srv, svc
}

func TestListGatewayFirstFrameEqualsList(t *testing.T) {
	srv, svc := newRoomServer(t)
	ctx := context.Background()
	for _, title := range []string{"one", "two"} {
		if _, err := svc.CreateRoom(ctx, mustUser(t, svc, "arjun"), title, "sev2", "orders"); err != nil {
			t.Fatal(err)
		}
	}
	want := getRoomsJSON(t, srv, "arjun")
	conn := dialList(t, srv, "arjun")
	f, ok := readList(t, conn, 5*time.Second)
	if !ok || f.Type != "rooms" {
		t.Fatalf("first frame = %+v", f)
	}
	var env struct {
		Rooms json.RawMessage `json:"rooms"`
	}
	_ = json.Unmarshal([]byte(f.raw), &env)
	if string(env.Rooms) != want {
		t.Fatalf("first frame rooms differ from GET /api/rooms:\n%s\n%s", env.Rooms, want)
	}

	// A room opened now arrives as a "room" frame without a poll.
	start := time.Now()
	snap, err := svc.CreateRoom(ctx, mustUser(t, svc, "priya"), "three", "sev1", "orders")
	if err != nil {
		t.Fatal(err)
	}
	f, ok = readList(t, conn, 2*time.Second)
	if !ok || f.Type != "room" || f.Room == nil || f.Room.ID != snap.ID {
		t.Fatalf("live frame = %+v", f)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("new room pushed after %s, want under 1s", took)
	}
}

// TestListGatewayEqualOpenedAtStable covers KAN-382 end to end: when rooms
// share an opened_at, repeated GET /api/rooms answers and the first frame of
// every list-socket connection (including a reconnect) use the same id
// tiebreaker, and closing a room moves only that room behind the open ones.
func TestListGatewayEqualOpenedAtStable(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	srv, svc := fixedClockServer(t, now)
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")
	var ids []string
	for _, title := range []string{"one", "two", "three"} {
		snap, err := svc.CreateRoom(ctx, arjun, title, "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, snap.ID)
	}
	// Every room shares opened_at now: the list order is the id order.
	sort.Strings(ids)
	want := ids

	first := getRoomsJSON(t, srv, "arjun")
	if got := roomIDs(t, first); !slices.Equal(got, want) {
		t.Fatalf("GET /api/rooms order = %v, want %v (equal opened_at broken by id)", got, want)
	}
	for range 10 {
		if got := getRoomsJSON(t, srv, "arjun"); got != first {
			t.Fatalf("GET /api/rooms changed between calls:\n%s\n%s", got, first)
		}
	}

	// The first frame is the HTTP list; a reconnect lists again and must
	// answer the same order, not the service's map order.
	for attempt := range 2 {
		conn := dialList(t, srv, "arjun")
		f, ok := readList(t, conn, 5*time.Second)
		if !ok || f.Type != "rooms" {
			t.Fatalf("connection %d first frame = %+v", attempt+1, f)
		}
		if got := frameRooms(t, f); got != first {
			t.Fatalf("connection %d first frame rooms differ from GET /api/rooms:\n%s\n%s", attempt+1, got, first)
		}
		conn.conn.Close(websocket.StatusNormalClosure, "")
	}

	// Closing the id-first room puts it after the open ones (status is the
	// first key); the open rooms keep their relative order on both paths.
	closed := want[0]
	if _, err := svc.CloseRoom(ctx, arjun, closed, "done"); err != nil {
		t.Fatal(err)
	}
	want = append(append([]string{}, want[1:]...), closed)
	after := getRoomsJSON(t, srv, "arjun")
	if got := roomIDs(t, after); !slices.Equal(got, want) {
		t.Fatalf("after close, GET /api/rooms order = %v, want %v", got, want)
	}
	conn := dialList(t, srv, "arjun")
	f, ok := readList(t, conn, 5*time.Second)
	if !ok || f.Type != "rooms" {
		t.Fatalf("after close, list socket frame = %+v", f)
	}
	// Only the order is compared here: closing starts the post-mortem draft,
	// whose status may change between the HTTP read and this frame.
	if got := roomIDs(t, frameRooms(t, f)); !slices.Equal(got, want) {
		t.Fatalf("after close, list socket order = %v, want %v", got, want)
	}
}

func TestListGatewayDigestSkipsStepChurn(t *testing.T) {
	srv, svc := newRoomServer(t)
	ctx := context.Background()
	snap, err := svc.CreateRoom(ctx, mustUser(t, svc, "arjun"), "orders down", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	conn := dialList(t, srv, "arjun")
	if f, ok := readList(t, conn, 5*time.Second); !ok || f.Type != "rooms" {
		t.Fatalf("first frame = %+v", f)
	}
	// Comments change last_seq and members only: nothing is sent.
	for i := range 3 {
		if _, err := svc.AddComment(ctx, mustUser(t, svc, "priya"), snap.ID, "", "look "+itoa(int64(i))); err != nil {
			t.Fatal(err)
		}
	}
	if f, ok := readList(t, conn, 800*time.Millisecond); ok {
		t.Fatalf("step churn sent a frame: %s", f.raw)
	}
	// A close changes the status: one frame.
	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), snap.ID, "fixed"); err != nil {
		t.Fatal(err)
	}
	f, ok := readList(t, conn, 2*time.Second)
	if !ok || f.Type != "room" || f.Room.Status != "closed" {
		t.Fatalf("close frame = %+v", f)
	}
}

func TestListGatewayRequiresAuth(t *testing.T) {
	srv, _ := newRoomServer(t)
	url := strings.Replace(srv.URL, "http", "ws", 1) + "/api/rooms/ws"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, url, nil)
	if err == nil {
		t.Fatal("dial without a user succeeded")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("resp = %v", resp)
	}
}
