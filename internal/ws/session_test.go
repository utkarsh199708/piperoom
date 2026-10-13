package ws_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"

	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/ws"
)

// When the presence tick finds a session gone (expired, revoked, user removed),
// the connection gets an error frame with code session_expired and then closes
// with the reason session_expired. A connection without a session (X-User) and
// a session that is still alive are left alone.
func TestPresenceTickClosesDeadSessions(t *testing.T) {
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}

	var deadSession atomic.Bool
	gw := ws.NewGateway(svc, st, nil)
	gw.Identify = func(r *http.Request) (ws.Identity, bool) {
		u, ok := svc.LookupUser(r.Header.Get("X-User"))
		return ws.Identity{User: u, SessionID: r.Header.Get("X-Session")}, ok
	}
	gw.SessionAlive = func(_ context.Context, id ws.Identity) bool {
		return id.SessionID != "dead" || !deadSession.Load()
	}
	mux := chi.NewRouter()
	mux.Get("/api/rooms/{id}/ws", gw.ServeHTTP)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() { srv.Close(); gw.Close() })

	dial := func(user, session string) *websocket.Conn {
		url := strings.Replace(srv.URL, "http", "ws", 1) + "/api/rooms/" + snap.ID + "/ws?after=0"
		conn, _, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{
			HTTPHeader: http.Header{"X-User": {user}, "X-Session": {session}},
		})
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { conn.CloseNow() })
		return conn
	}
	dead := dial("arjun", "dead")
	alive := dial("priya", "alive")
	bare := dial("meera", "")

	deadSession.Store(true)
	gw.RecheckSessions()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sawFrame := false
	for {
		_, data, err := dead.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) != websocket.StatusPolicyViolation || !strings.Contains(err.Error(), "session_expired") {
				t.Fatalf("close = %v, want policy violation with reason session_expired", err)
			}
			break
		}
		var m struct {
			Type  string `json:"type"`
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if json.Unmarshal(data, &m) == nil && m.Type == "error" && m.Error.Code == "session_expired" {
			sawFrame = true
		}
	}
	if !sawFrame {
		t.Fatal("no {type:error, error:{code:session_expired}} frame before the close")
	}

	for name, c := range map[string]*websocket.Conn{"live session": alive, "X-User connection": bare} {
		rctx, rcancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		for {
			_, _, err := c.Read(rctx)
			if err == nil {
				continue
			}
			if websocket.CloseStatus(err) != -1 {
				t.Fatalf("%s was closed: %v", name, err)
			}
			break
		}
		rcancel()
	}
}
