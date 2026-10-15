package ws_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/ws"
)

// headErrStore fails the gateway's head read with err.
type headErrStore struct {
	*memstore.Store
	err error
}

func (h headErrStore) GetRoom(context.Context, string) (store.Room, error) {
	return store.Room{}, h.err
}

// headBlockStore blocks the head read until its context ends.
type headBlockStore struct{ *memstore.Store }

func (h headBlockStore) GetRoom(ctx context.Context, _ string) (store.Room, error) {
	<-ctx.Done()
	return store.Room{}, ctx.Err()
}

// readFrames reads until the close and returns the text frames and the error.
func readFrames(t *testing.T, conn *websocket.Conn) ([]string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var frames []string
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return frames, err
		}
		frames = append(frames, string(data))
	}
}

func staleRoom(t *testing.T) (*memstore.Store, *room.Service, string, int64) {
	t.Helper()
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: deleteUsers(t), Run: fakeRun})
	t.Cleanup(svc.Close)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "priya"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	evs, err := st.Since(context.Background(), snap.ID, 0, 100)
	if err != nil || len(evs) == 0 {
		t.Fatalf("since: %v, %d events", err, len(evs))
	}
	return st, svc, snap.ID, evs[len(evs)-1].Seq
}

func TestAfterPastHeadIsStalePosition(t *testing.T) {
	st, svc, id, head := staleRoom(t)
	srv, _ := serveGateway(t, svc, st)
	conn := dialWS(t, srv, id, "priya", head+5)
	frames, err := readFrames(t, conn)
	if websocket.CloseStatus(err) != websocket.StatusPolicyViolation || !strings.Contains(err.Error(), "stale_position") {
		t.Fatalf("close = %v, want 1008 stale_position", err)
	}
	if len(frames) != 1 {
		t.Fatalf("frames = %v, want exactly the error frame", frames)
	}
	var m struct {
		Type  string `json:"type"`
		Error struct{ Code, Message string }
	}
	if json.Unmarshal([]byte(frames[0]), &m) != nil || m.Type != "error" || m.Error.Code != "stale_position" {
		t.Fatalf("frame = %s", frames[0])
	}
	want := fmt.Sprintf("after %d is past the room's last event %d", head+5, head)
	if m.Error.Message != want {
		t.Fatalf("message = %q, want %q", m.Error.Message, want)
	}
}

func TestAfterEqualHeadReplaysNothingAndStays(t *testing.T) {
	st, svc, id, head := staleRoom(t)
	srv, _ := serveGateway(t, svc, st)
	conn := dialWS(t, srv, id, "priya", head)
	// The first frame is presence: no replayed event and no error before it.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m struct{ Type string }
	if json.Unmarshal(data, &m) != nil || m.Type != "presence" {
		t.Fatalf("first frame = %s, want presence", data)
	}
	// And the socket stays open.
	short, cancelShort := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelShort()
	if _, data, err := conn.Read(short); err == nil {
		t.Logf("later frame: %s", data)
	} else if websocket.CloseStatus(err) != -1 {
		t.Fatalf("socket closed: %v", err)
	}
}

// The head comes from the store, not from the room service: an event another
// replica appended straight to the store is already part of the log.
func TestHeadIsReadFromStoreNotSnapshot(t *testing.T) {
	st, svc, id, head := staleRoom(t)
	if _, err := st.Append(context.Background(), id, events.CommentAdded, events.Actor{ID: "priya", Name: "Priya"},
		events.CommentAddedPayload{CommentID: "c", Text: "x"}); err != nil {
		t.Fatal(err)
	}
	srv, _ := serveGateway(t, svc, st)
	conn := dialWS(t, srv, id, "priya", head+1)
	readPresence(t, conn)
}

func TestHeadReadNotFoundClosesAsDeleted(t *testing.T) {
	st, svc, id, head := staleRoom(t)
	srv, _ := serveGateway(t, svc, headErrStore{st, fmt.Errorf("x: %w", store.ErrNotFound)})
	conn := dialWS(t, srv, id, "priya", head+5)
	frames, err := readFrames(t, conn)
	if websocket.CloseStatus(err) != websocket.StatusNormalClosure || !strings.Contains(err.Error(), ws.RoomDeletedReason) {
		t.Fatalf("close = %v, want 1000 %q", err, ws.RoomDeletedReason)
	}
	if len(frames) != 0 {
		t.Fatalf("frames = %v, want none", frames)
	}
}

func TestHeadReadTimeoutClosesAsReplayTimeout(t *testing.T) {
	st, svc, id, head := staleRoom(t)
	srv, gw := serveGateway(t, svc, headBlockStore{st})
	gw.ReplayTimeout = 200 * time.Millisecond
	conn := dialWS(t, srv, id, "priya", head+5)
	frames, err := readFrames(t, conn)
	if websocket.CloseStatus(err) != websocket.StatusPolicyViolation || !strings.Contains(err.Error(), "replay timeout") || strings.Contains(err.Error(), "stale_position") {
		t.Fatalf("close = %v (frames %v), want 1008 replay timeout", err, frames)
	}
	if len(frames) != 0 {
		t.Fatalf("frames = %v, want none", frames)
	}
}

func TestAfterBelowHeadReplays(t *testing.T) {
	st, svc, id, head := staleRoom(t)
	srv, _ := serveGateway(t, svc, st)
	conn := dialWS(t, srv, id, "priya", head-1)
	if e := readEvent(t, conn); e.Seq != head {
		t.Fatalf("first event seq = %d, want %d", e.Seq, head)
	}
}

func TestHeadReadErrorIsInternalError(t *testing.T) {
	st, svc, id, head := staleRoom(t)
	srv, _ := serveGateway(t, svc, headErrStore{st, errors.New("boom")})
	conn := dialWS(t, srv, id, "priya", head+5)
	frames, err := readFrames(t, conn)
	if websocket.CloseStatus(err) != websocket.StatusInternalError || !strings.Contains(err.Error(), "replay failed") {
		t.Fatalf("close = %v, want 1011 replay failed", err)
	}
	if len(frames) != 0 {
		t.Fatalf("frames = %v, want none", frames)
	}
}
