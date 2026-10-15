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

	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// headFailStore fails the room read the gateway uses for the head check.
// The service gets the plain store, so only the gateway's read fails.
type headFailStore struct{ *memstore.Store }

func (headFailStore) GetRoom(context.Context, string) (store.Room, error) {
	return store.Room{}, errors.New("boom")
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
	readPresence(t, conn) // an open socket: the first frame is presence, not an error
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
	srv, _ := serveGateway(t, svc, headFailStore{st})
	conn := dialWS(t, srv, id, "priya", head+5)
	frames, err := readFrames(t, conn)
	if websocket.CloseStatus(err) != websocket.StatusInternalError || strings.Contains(err.Error(), "stale_position") {
		t.Fatalf("close = %v (frames %v), want 1011 and not stale_position", err, frames)
	}
}
