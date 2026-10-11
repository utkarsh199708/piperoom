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

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
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
	url := strings.Replace(srv.URL, "http", "ws", 1) + "/api/rooms/" + roomID + "/ws?user=" + user + "&after=" + itoa(after)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })
	return conn
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

func mustUser(t *testing.T, svc *room.Service, id string) room.User {
	t.Helper()
	u, ok := svc.LookupUser(id)
	if !ok {
		t.Fatalf("user %q not found", id)
	}
	return u
}
