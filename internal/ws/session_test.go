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

	// Dial returns before the server registers the connection; a recheck that
	// ran first would not see the session and never close it.
	for deadline := time.Now().Add(5 * time.Second); gw.ConnCount() < 3; {
		if time.Now().After(deadline) {
			t.Fatalf("gateway registered %d of 3 connections", gw.ConnCount())
		}
		time.Sleep(5 * time.Millisecond)
	}

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

// A session that ended between the upgrade's authentication and the
// connection's registration (a logout in that gap finds nothing to close) is
// caught when the connection registers: one session_expired frame, then the
// close, without waiting for the presence tick.
func TestSessionEndedBeforeRegisterClosesAtOnce(t *testing.T) {
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	gw := ws.NewGateway(svc, st, nil)
	gw.Identify = func(r *http.Request) (ws.Identity, bool) {
		u, ok := svc.LookupUser(r.Header.Get("X-User"))
		return ws.Identity{User: u, SessionID: r.Header.Get("X-Session")}, ok
	}
	// The session was revoked right after Identify accepted it.
	gw.SessionAlive = func(_ context.Context, id ws.Identity) bool { return id.SessionID != "revoked" }
	mux := chi.NewRouter()
	mux.Get("/api/rooms/{id}/ws", gw.ServeHTTP)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() { srv.Close(); gw.Close() })

	url := strings.Replace(srv.URL, "http", "ws", 1) + "/api/rooms/" + snap.ID + "/ws?after=0"
	conn, _, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{
		HTTPHeader: http.Header{"X-User": {"arjun"}, "X-Session": {"revoked"}},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.CloseNow() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	frames := 0
	for {
		_, data, err := conn.Read(ctx)
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
			frames++
			// A logout landing now as well must not send a second frame.
			gw.CloseSession("revoked")
		}
	}
	if frames != 1 {
		t.Fatalf("session_expired frames = %d, want 1", frames)
	}
}
