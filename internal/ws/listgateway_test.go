package ws_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/room"
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
	lc := &listClient{frames: make(chan listFrame, 64)}
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
