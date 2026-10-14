package main

// End-to-end test: alert webhook → room created → tool result appended →
// WebSocket replay and live delivery over the real HTTP handler and gateway.

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

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// fakeRun blocks until the service closes, so the only events in the room are
// the ones the test itself creates.
func fakeRun(ctx context.Context, alert agent.Alert, deps agent.Deps, opts agent.Options) error {
	<-ctx.Done()
	return nil
}

type serverMsg struct {
	Type  string        `json:"type"`
	Event *events.Event `json:"event"`
}

func newE2EServer(t *testing.T) (*httptest.Server, *room.Service, *memstore.Store) {
	t.Helper()
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	api := httpapi.New(svc, st, nil)
	srv := httptest.NewServer(api)
	// Cleanups run LIFO: WS connections (closed by dialWS's cleanup, registered
	// later) close first, then the API and server stop, so api.Close can stop
	// every goroutine.
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})
	return srv, svc, st
}

// wsReader reads event frames from a WebSocket connection in the background
// and hands them to the test through a channel. A Read with a per-call
// deadline would close the connection on timeout (coder/websocket closes the
// conn when an operation's context ends), so all waiting happens on the
// channel with plain timers instead.
type wsReader struct {
	conn   *websocket.Conn
	events chan events.Event
}

func dialWS(t *testing.T, srv *httptest.Server, roomID, user string, after int64) *wsReader {
	t.Helper()
	url := strings.Replace(srv.URL, "http", "ws", 1) + "/api/rooms/" + roomID + "/ws?after=" + fmt.Sprintf("%d", after)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {user}}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })

	r := &wsReader{conn: conn, events: make(chan events.Event, 64)}
	go func() {
		defer close(r.events)
		for {
			// No deadline: the goroutine ends when the connection is closed
			// by the test cleanup, so it never leaks and never kills the conn.
			_, data, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			var msg serverMsg
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			if msg.Type == "event" && msg.Event != nil {
				r.events <- *msg.Event // presence frames are skipped
			}
		}
	}()
	return r
}

// nextEvent waits up to d for the next event frame (presence is skipped by the
// reader goroutine).
func (r *wsReader) nextEvent(t *testing.T, d time.Duration) events.Event {
	t.Helper()
	select {
	case e, ok := <-r.events:
		if !ok {
			t.Fatal("connection closed while waiting for an event")
		}
		return e
	case <-time.After(d):
		t.Fatal("timed out waiting for an event")
		return events.Event{}
	}
}

// noEvent fails if an event frame arrives within d; only presence may.
func (r *wsReader) noEvent(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case e, ok := <-r.events:
		if !ok {
			t.Fatal("connection closed while waiting out a quiet window")
		}
		t.Fatalf("unexpected event %s/%d while none was expected", e.Type, e.Seq)
	case <-time.After(d):
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

func TestE2EAlertToWebSocket(t *testing.T) {
	srv, svc, st := newE2EServer(t)

	// 1. The alert webhook opens a room (room.created = seq 1).
	body := `{"title":"orders lag","severity":"sev2","pipeline_id":"orders"}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/alerts/webhook", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User", "arjun")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("webhook: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("webhook: want 201, got %d", resp.StatusCode)
	}
	var snap struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snap.ID == "" {
		t.Fatal("webhook returned an empty room id")
	}

	// 2. The agent's first tool result lands in the log (seq 2).
	tr := events.AgentToolResultPayload{
		StepID: "s_1", ToolUseID: "tu_1", Tool: "connect_status",
		Summary: "pg-orders-connector task 0 FAILED",
	}
	if _, err := st.Append(context.Background(), snap.ID, events.AgentToolResult, events.AgentActor, tr); err != nil {
		t.Fatalf("append tool result: %v", err)
	}

	// 3. A viewer from scratch receives every event in exact seq order.
	viewer := dialWS(t, srv, snap.ID, "arjun", 0)
	e := viewer.nextEvent(t, 5*time.Second)
	if e.Seq != 1 || e.Type != events.RoomCreated {
		t.Fatalf("event 1: want room.created/1, got %s/%d", e.Type, e.Seq)
	}
	e = viewer.nextEvent(t, 5*time.Second)
	if e.Seq != 2 || e.Type != events.AgentToolResult {
		t.Fatalf("event 2: want agent.tool_result/2, got %s/%d", e.Type, e.Seq)
	}

	// 4. A client already at the last seq gets nothing until a new event is
	// appended.
	current := dialWS(t, srv, snap.ID, "priya", 2)
	current.noEvent(t, 400*time.Millisecond)

	// 5. One new event: both clients receive exactly that one, nothing else.
	if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "priya"), snap.ID, "", "any update?"); err != nil {
		t.Fatalf("comment: %v", err)
	}
	e = current.nextEvent(t, 5*time.Second)
	if e.Seq != 3 || e.Type != events.CommentAdded {
		t.Fatalf("event 3 on current: want comment.added/3, got %s/%d", e.Type, e.Seq)
	}
	e = viewer.nextEvent(t, 5*time.Second)
	if e.Seq != 3 || e.Type != events.CommentAdded {
		t.Fatalf("event 3 on viewer: want comment.added/3, got %s/%d", e.Type, e.Seq)
	}
	current.noEvent(t, 300*time.Millisecond)
}
