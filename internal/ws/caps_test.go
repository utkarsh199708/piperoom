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
	"github.com/go-chi/chi/v5"

	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/ws"
)

// dialRefused dials and expects the upgrade to be refused with 429
// too_many_connections; it returns the error message.
func dialRefused(t *testing.T, srv *httptest.Server, roomID, user string) string {
	t.Helper()
	url := strings.Replace(srv.URL, "http", "ws", 1) + "/api/rooms/" + roomID + "/ws"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {user}}})
	if err == nil {
		conn.Close(websocket.StatusNormalClosure, "")
		t.Fatalf("%s: dial over the cap succeeded", user)
	}
	if resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("%s: want 429, got resp=%v err=%v", user, resp, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var body struct {
		Error struct{ Code, Message string } `json:"error"`
	}
	if err := json.Unmarshal(b, &body); err != nil || body.Error.Code != "too_many_connections" {
		t.Fatalf("%s: body %s", user, b)
	}
	return body.Error.Message
}

func TestWebSocketConnectionCaps(t *testing.T) {
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	a, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "a", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	b, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "b", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	gw := ws.NewGateway(svc, st, nil)
	gw.Identify = headerIdentify(svc)
	gw.MaxConnsPerRoom = 3
	gw.MaxConnsPerUser = 2
	mux := chi.NewRouter()
	mux.Get("/api/rooms/{id}/ws", gw.ServeHTTP)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() { srv.Close(); gw.Close() })

	// Per user: priya's third connection is refused, in any room.
	p1 := dialWS(t, srv, a.ID, "priya", 0)
	dialWS(t, srv, b.ID, "priya", 0)
	if msg := dialRefused(t, srv, a.ID, "priya"); !strings.Contains(msg, "2 live connections") {
		t.Fatalf("per-user message %q does not name the cap", msg)
	}

	// Per room: room a holds priya + arjun x2 = 3; meera is refused there
	// but not in room b.
	dialWS(t, srv, a.ID, "arjun", 0)
	dialWS(t, srv, a.ID, "arjun", 0)
	if msg := dialRefused(t, srv, a.ID, "meera"); !strings.Contains(msg, "this room already has 3") {
		t.Fatalf("per-room message %q does not name the cap", msg)
	}
	dialWS(t, srv, b.ID, "meera", 0)

	// Closing a connection gives both slots back.
	p1.Close(websocket.StatusNormalClosure, "")
	waitSlots(t, gw, a.ID, "priya", 2, 1)
	dialWS(t, srv, a.ID, "priya", 0)
	if r, u := gw.Slots(a.ID, "meera"); r != 3 || u != 1 {
		t.Fatalf("slots after reconnect: room %d, meera %d", r, u)
	}
}

// waitSlots waits (bounded) for the server to release a closed connection's
// slots: the close is noticed by the server's read loop, not by Close.
func waitSlots(t *testing.T, gw *ws.Gateway, roomID, userID string, wantRoom, wantUser int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r, u := gw.Slots(roomID, userID)
		if r == wantRoom && u == wantUser {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("slots room %d user %d, want %d/%d", r, u, wantRoom, wantUser)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
