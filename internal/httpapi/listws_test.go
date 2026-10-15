package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestListGatewayTenancyFilter: the list socket shows a person only the rooms
// of their tenants, in the first frame and live.
func TestListGatewayTenancyFilter(t *testing.T) {
	e := newTenancyEnv(t, true)
	ra := decodeID(t, e.mustDo(t, "alice", http.MethodPost, "/api/rooms", `{"title":"a","severity":"sev2","pipeline_id":"pa"}`, http.StatusCreated))
	e.mustDo(t, "bob", http.MethodPost, "/api/rooms", `{"title":"b","severity":"sev2","pipeline_id":"pb"}`, http.StatusCreated)

	url := strings.Replace(e.srv.URL, "http", "ws", 1) + "/api/rooms/ws"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {"alice"}}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	type frame struct {
		Type  string `json:"type"`
		Rooms []struct {
			ID         string `json:"id"`
			PipelineID string `json:"pipeline_id"`
		} `json:"rooms"`
		Room *struct {
			ID         string `json:"id"`
			PipelineID string `json:"pipeline_id"`
		} `json:"room"`
	}
	read := func() frame {
		t.Helper()
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var f frame
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatal(err)
		}
		return f
	}
	first := read()
	if first.Type != "rooms" || len(first.Rooms) != 1 || first.Rooms[0].ID != ra {
		t.Fatalf("first frame = %+v, want only room %s", first, ra)
	}
	// Bob opens another tenant-b room, then alice one in tenant a: the
	// next frame alice gets is her own room.
	e.mustDo(t, "bob", http.MethodPost, "/api/rooms", `{"title":"b2","severity":"sev2","pipeline_id":"pb"}`, http.StatusCreated)
	ra2 := decodeID(t, e.mustDo(t, "alice", http.MethodPost, "/api/rooms", `{"title":"a2","severity":"sev2","pipeline_id":"pa"}`, http.StatusCreated))
	for {
		f := read()
		if f.Room == nil {
			t.Fatalf("frame = %+v", f)
		}
		if f.Room.PipelineID != "pa" {
			t.Fatalf("alice got a room of pipeline %s", f.Room.PipelineID)
		}
		if f.Room.ID == ra2 {
			return
		}
	}
}
