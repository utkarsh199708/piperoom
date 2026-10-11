package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/ws"
)

// TestRealWebSocketHandlerBehindTheRouter checks that the pieces fit: the
// request logger's response writer must let the WebSocket library hijack the
// connection, and chi must hand the room id to the WS handler.
func TestRealWebSocketHandlerBehindTheRouter(t *testing.T) {
	feed := make(chan events.Event, 4)
	feed <- events.Event{RoomID: "r_1", Seq: 1, Type: events.CommentAdded, Payload: json.RawMessage(`{"text":"hi"}`),
		At: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	gotRoom := make(chan string, 1)
	wsh := ws.New(ws.Config{
		Subscribe: func(_ context.Context, roomID string, after int64) (<-chan events.Event, error) {
			gotRoom <- roomID
			return feed, nil
		},
		Authenticate: func(id string) (room.User, bool) {
			u, ok := newFakeService().Authenticate(id)
			return u, ok
		},
		RoomExists: func(_ context.Context, id string) bool { return id == "r_1" },
	})
	h := newHarness(t, func(c *Config) { c.WS = wsh })
	srv := httptest.NewServer(h.h)
	defer srv.Close()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = wsh.Shutdown(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/rooms/r_1/ws?user=alice&after=0"
	conn, resp, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial: %v (status %d)", err, status)
	}
	defer conn.CloseNow()

	var sawEvent bool
	for !sawEvent {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var f struct {
			Type  string       `json:"type"`
			Event events.Event `json:"event"`
		}
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatal(err)
		}
		sawEvent = f.Type == "event" && f.Event.Seq == 1
	}
	if got := <-gotRoom; got != "r_1" {
		t.Fatalf("WS handler saw room %q", got)
	}

	// A plain GET with an unknown ?user= is a 401 JSON error from the WS handler.
	bad, err := http.Get(srv.URL + "/api/rooms/r_1/ws?user=mallory")
	if err != nil {
		t.Fatal(err)
	}
	bad.Body.Close()
	if bad.StatusCode != 401 {
		t.Fatalf("status for unknown ws user = %d, want 401", bad.StatusCode)
	}

	_ = conn.Close(websocket.StatusNormalClosure, "")
}
