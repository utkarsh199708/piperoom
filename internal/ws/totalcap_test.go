package ws_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"

	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/ws"
)

// newCapGateway serves the room and list sockets of one gateway with a total
// cap of max.
func newCapGateway(t *testing.T, max int) (*httptest.Server, *ws.Gateway, *room.Service) {
	t.Helper()
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	gw := ws.NewGateway(svc, st, nil)
	gw.Identify = headerIdentify(svc)
	gw.MaxConns = max
	mux := chi.NewRouter()
	mux.Get("/api/rooms/ws", gw.ServeRoomsHTTP)
	mux.Get("/api/rooms/{id}/ws", gw.ServeHTTP)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() { srv.Close(); gw.Close() })
	return srv, gw, svc
}

// The total cap counts room and list connections of every user and room: the
// next upgrade of anyone is refused with 429 too_many_connections before the
// handshake, and a closed connection gives its slot back.
func TestWebSocketTotalCap(t *testing.T) {
	srv, gw, svc := newCapGateway(t, 3)
	a, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "a", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	b, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "b", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	priya := dialWS(t, srv, a.ID, "priya", 0)
	dialWS(t, srv, b.ID, "arjun", 0)
	dialList(t, srv, "meera")
	if n := gw.TotalSlots(); n != 3 {
		t.Fatalf("total slots = %d, want 3", n)
	}

	// kabir is under his own and the room's cap, but the server is full.
	if msg := dialRefused(t, srv, b.ID, "kabir"); !strings.Contains(msg, "this server already has 3") {
		t.Fatalf("total-cap message %q does not name the cap", msg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listURL := strings.Replace(srv.URL, "http", "ws", 1) + "/api/rooms/ws"
	if conn, resp, err := websocket.Dial(ctx, listURL, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {"kabir"}}}); err == nil {
		conn.CloseNow()
		t.Fatal("a list socket over the total cap was accepted")
	} else if resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("list socket over the total cap: resp %v err %v, want 429", resp, err)
	}

	priya.Close(websocket.StatusNormalClosure, "")
	waitTotal(t, gw, 2)
	dialWS(t, srv, b.ID, "kabir", 0)
	if n := gw.TotalSlots(); n != 3 {
		t.Fatalf("total slots after reconnect = %d, want 3", n)
	}
}

// Concurrent upgrades never take more slots than the total cap: exactly cap
// of them are accepted, every other one is refused with 429.
func TestWebSocketTotalCapConcurrent(t *testing.T) {
	const max, dials = 5, 24
	srv, gw, svc := newCapGateway(t, max)
	r, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "a", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	url := strings.Replace(srv.URL, "http", "ws", 1) + "/api/rooms/" + r.ID + "/ws"
	users := []string{"arjun", "priya", "meera", "kabir"}
	var (
		mu       sync.Mutex
		accepted []*websocket.Conn
		refused  int
		other    []string
		wg       sync.WaitGroup
	)
	t.Cleanup(func() {
		for _, c := range accepted {
			c.CloseNow()
		}
	})
	start := make(chan struct{})
	for i := range dials {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {users[i%len(users)]}}})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				accepted = append(accepted, conn)
			case resp != nil && resp.StatusCode == http.StatusTooManyRequests:
				refused++
			default:
				other = append(other, err.Error())
			}
		}()
	}
	close(start)
	wg.Wait()
	if len(other) > 0 {
		t.Fatalf("unexpected dial errors: %v", other)
	}
	if len(accepted) != max || refused != dials-max {
		t.Fatalf("accepted %d, refused %d; want %d and %d", len(accepted), refused, max, dials-max)
	}
	if n := gw.TotalSlots(); n != max {
		t.Fatalf("total slots = %d, want %d", n, max)
	}
}

// waitTotal waits (bounded) until the gateway holds want slots in all.
func waitTotal(t *testing.T, gw *ws.Gateway, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for gw.TotalSlots() != want {
		if time.Now().After(deadline) {
			t.Fatalf("total slots %d, want %d", gw.TotalSlots(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
