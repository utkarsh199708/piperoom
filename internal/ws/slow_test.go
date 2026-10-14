package ws_test

import (
	"context"
	"errors"
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

// TestWebSocketAfterBeyondHeadRejected: a client that asks for events after a
// seq the room never reached would otherwise get no replay and silently miss
// every live event up to that seq. It is refused before the upgrade.
func TestWebSocketAfterBeyondHeadRejected(t *testing.T) {
	srv, svc := newRoomServer(t)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	for _, after := range []string{"3", "999"} {
		req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/rooms/"+snap.ID+"/ws?after="+after, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("X-User", "arjun")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("after=%s: want 400, got %d", after, resp.StatusCode)
		}
	}
	// after == head is fine (nothing to replay, live from here).
	conn := dialWS(t, srv, snap.ID, "arjun", 2)
	if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "priya"), snap.ID, "", "x"); err != nil {
		t.Fatalf("comment: %v", err)
	}
	if e := readEvent(t, conn); e.Seq != 3 {
		t.Fatalf("live: got seq %d", e.Seq)
	}
}

// TestWebSocketSlowConsumerGetsCloseReason: a client that stops reading long
// enough for its live buffer to fill is closed with 1008 and a reason it can
// act on, not a bare TCP reset. The pause is longer than the library's 5 s
// close-handshake wait, which is what used to drop the close frame. (A peer
// that reads nothing for longer than the write timeout is dead, not slow, and
// still gets only the TCP close.)
func TestWebSocketSlowConsumerGetsCloseReason(t *testing.T) {
	if testing.Short() {
		t.Skip("waits ~6 s")
	}
	// A long write timeout keeps this a slow peer (not a dead one) however
	// long the flood takes on a loaded machine.
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	gw := ws.NewGateway(svc, st, nil)
	gw.Identify = headerIdentify(svc)
	gw.WriteTimeout = 2 * time.Minute
	t.Cleanup(gw.Close)
	r := chi.NewRouter()
	r.Get("/api/rooms/{id}/ws", gw.ServeHTTP)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	conn := dialWS(t, srv, snap.ID, "arjun", 2)
	conn.SetReadLimit(1 << 20)
	big := strings.Repeat("x", 32<<10)
	priya := mustUser(t, svc, "priya")
	// The client keeps sending presence (it is slow to read, not gone), so
	// the server's 60 s read timeout does not end it first on a slow machine.
	beat, stopBeat := context.WithCancel(context.Background())
	beatDone := make(chan struct{})
	go func() {
		defer close(beatDone)
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-beat.Done():
				return
			case <-tick.C:
				_ = conn.Write(beat, websocket.MessageText, []byte(`{"type":"presence","state":"viewing"}`))
			}
		}
	}()
	defer func() { stopBeat(); <-beatDone }()
	// Enough data to fill the socket buffers and then the 256-slot queue.
	for i := 0; i < 600; i++ {
		if _, err := svc.AddComment(context.Background(), priya, snap.ID, "", big); err != nil {
			t.Fatalf("comment %d: %v", i, err)
		}
	}
	time.Sleep(6 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		_, _, err := conn.Read(ctx)
		if err == nil {
			continue
		}
		var ce websocket.CloseError
		if !errors.As(err, &ce) {
			t.Fatalf("want a close frame, got %v", err)
		}
		if ce.Code != websocket.StatusPolicyViolation || !strings.Contains(ce.Reason, "after=") {
			t.Fatalf("close = %d %q", ce.Code, ce.Reason)
		}
		return
	}
}
