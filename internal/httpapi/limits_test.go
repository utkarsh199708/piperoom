package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// newLimitsServer is newServer with an admin (ada) in the directory, the
// open-room limit on and a small log cap on the store.
func newLimitsServer(t *testing.T) (*httptest.Server, *room.Service, *memstore.Store) {
	t.Helper()
	users, err := room.ParseUsers([]byte(`
users:
  - id: arjun
    name: Arjun
    layers: [kafka-connect, kafka]
  - id: priya
    name: Priya
    layers: [postgres, schema-registry]
  - id: ada
    name: Ada
    admin: true
`))
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	st.SetLimits(store.Limits{MaxEvents: 100})
	svc := room.New(room.Deps{
		Store: st, Users: users, Run: fakeRun,
		Limits: config.Limits{MaxOpenDays: 14, RoomMaxEvents: 100},
	})
	h := httpapi.New(svc, st, nil)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	t.Cleanup(h.Close)
	return srv, svc, st
}

func openRoomHTTP(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create room: %d", resp.StatusCode)
	}
	return decode[map[string]any](t, resp)["id"].(string)
}

func limitErrCode(t *testing.T, resp *http.Response) string {
	t.Helper()
	body := decode[map[string]map[string]string](t, resp)
	return body["error"]["code"]
}

func deleteURL(srv *httptest.Server, roomID, reason string) string {
	return srv.URL + "/api/rooms/" + roomID + "?reason=" + url.QueryEscape(reason)
}

// TestDeleteRoomRoute: admin 200 with the audit body, member 403, unknown 404,
// missing reason 400, a repeat 200 with the same body; the room's WebSocket
// gets the room.deleted frame and a normal close with reason "room deleted";
// a new WebSocket is 404.
func TestDeleteRoomRoute(t *testing.T) {
	srv, _, _ := newLimitsServer(t)
	roomID := openRoomHTTP(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/rooms/" + roomID + "/ws?after=0"
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {"priya"}}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	// The presence frame comes after the replay, once the connection is
	// registered for the room: from here CloseRoom reaches it.
	var last map[string]any
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read before delete: %v", err)
		}
		var m map[string]any
		_ = json.Unmarshal(data, &m)
		if m["type"] == "event" {
			last = m["event"].(map[string]any)
		}
		if m["type"] == "presence" {
			break
		}
	}

	resp := doJSON(t, http.MethodDelete, deleteURL(srv, roomID, "cleanup"), "arjun", "")
	if resp.StatusCode != http.StatusForbidden || limitErrCode(t, resp) != "not_allowed" {
		t.Fatalf("member delete: %d", resp.StatusCode)
	}
	resp = doJSON(t, http.MethodDelete, deleteURL(srv, "r_missing", "cleanup"), "arjun", "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("member delete of an unknown id: %d, want 403 (no id leak)", resp.StatusCode)
	}
	resp = doJSON(t, http.MethodDelete, deleteURL(srv, "r_missing", "cleanup"), "ada", "")
	if resp.StatusCode != http.StatusNotFound || limitErrCode(t, resp) != "not_found" {
		t.Fatalf("unknown room: %d", resp.StatusCode)
	}
	resp = doJSON(t, http.MethodDelete, srv.URL+"/api/rooms/"+roomID, "ada", "")
	if resp.StatusCode != http.StatusBadRequest || limitErrCode(t, resp) != "validation" {
		t.Fatalf("no reason: %d", resp.StatusCode)
	}

	resp = doJSON(t, http.MethodDelete, deleteURL(srv, roomID, "pasted a secret"), "ada", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin delete: %d", resp.StatusCode)
	}
	body := decode[map[string]any](t, resp)
	if body["room_id"] != roomID || body["pipeline_id"] != "orders" || body["deleted_by"] != "ada" ||
		body["reason"] != "pasted a secret" || body["event_count"] != float64(2) || body["archived"] != false || body["deleted_at"] == nil {
		t.Fatalf("delete body = %v", body)
	}

	// The WebSocket: room.deleted with seq last+1, then the close.
	if last == nil || last["seq"] != float64(2) {
		t.Fatalf("replay ended at %v, want seq 2", last)
	}
	var closeErr error
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			closeErr = err
			break
		}
		var m map[string]any
		if json.Unmarshal(data, &m) == nil && m["type"] == "event" {
			last = m["event"].(map[string]any)
		}
	}
	if websocket.CloseStatus(closeErr) != websocket.StatusNormalClosure || !strings.Contains(closeErr.Error(), "room deleted") {
		t.Fatalf("close = %v, want 1000 room deleted", closeErr)
	}
	if last == nil || last["type"] != string(events.RoomDeleted) || last["seq"] != float64(3) {
		t.Fatalf("last frame = %v, want room.deleted seq 3", last)
	}
	if p, _ := last["payload"].(map[string]any); p["reason"] != "pasted a secret" {
		t.Fatalf("room.deleted payload = %v", last["payload"])
	}
	if a, _ := last["actor"].(map[string]any); a["id"] != "ada" {
		t.Fatalf("room.deleted actor = %v", last["actor"])
	}

	// Idempotent: the same row again.
	resp = doJSON(t, http.MethodDelete, deleteURL(srv, roomID, "again"), "ada", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("repeat delete: %d", resp.StatusCode)
	}
	again := decode[map[string]any](t, resp)
	if again["reason"] != "pasted a secret" || again["deleted_at"] != body["deleted_at"] {
		t.Fatalf("repeat body = %v, want the first row %v", again, body)
	}
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/rooms/"+roomID, "ada", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET deleted room: %d", resp.StatusCode)
	}
	_, wresp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {"priya"}}})
	if err == nil || wresp == nil || wresp.StatusCode != http.StatusNotFound {
		t.Fatalf("WebSocket to a deleted room: %v %v", wresp, err)
	}
}

func TestMeAdminFlag(t *testing.T) {
	srv, _, _ := newLimitsServer(t)
	if me := decode[map[string]any](t, doJSON(t, http.MethodGet, srv.URL+"/api/me", "ada", "")); me["admin"] != true {
		t.Fatalf("ada: %v", me)
	}
	if me := decode[map[string]any](t, doJSON(t, http.MethodGet, srv.URL+"/api/me", "arjun", "")); me["admin"] != nil {
		t.Fatalf("arjun: admin must be left out, got %v", me)
	}
}

func TestExtendRoute(t *testing.T) {
	srv, _, _ := newLimitsServer(t)
	roomID := openRoomHTTP(t, srv)
	snap := decode[map[string]any](t, doJSON(t, http.MethodGet, srv.URL+"/api/rooms/"+roomID, "arjun", ""))
	if snap["auto_close_at"] == nil || snap["extended"] != false || snap["log_cap"] != "ok" {
		t.Fatalf("snapshot limit fields = %v %v %v", snap["auto_close_at"], snap["extended"], snap["log_cap"])
	}
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/extend", "priya", "")
	if resp.StatusCode != http.StatusForbidden || limitErrCode(t, resp) != "not_driver" {
		t.Fatalf("non-driver extend: %d", resp.StatusCode)
	}
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/extend", "arjun", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("extend: %d", resp.StatusCode)
	}
	if ev := decode[map[string]any](t, resp); ev["type"] != string(events.RoomExtended) {
		t.Fatalf("extend event = %v", ev)
	}
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/extend", "arjun", "")
	if resp.StatusCode != http.StatusConflict || limitErrCode(t, resp) != "already_extended" {
		t.Fatalf("second extend: %d", resp.StatusCode)
	}
}

// TestLogFullIs409: past the cap and its reserve a person's write is 409
// log_full, and close still works.
func TestLogFullIs409(t *testing.T) {
	srv, _, st := newLimitsServer(t)
	roomID := openRoomHTTP(t, srv)
	ctx := context.Background()
	sys := events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}
	for {
		if _, err := st.Append(ctx, roomID, events.AgentMessage, sys, events.AgentMessagePayload{Text: "note"}); err != nil {
			break
		}
	}
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/comments", "priya", `{"text":"hello"}`)
	if resp.StatusCode != http.StatusConflict || limitErrCode(t, resp) != "log_full" {
		t.Fatalf("comment on a full room: %d", resp.StatusCode)
	}
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/close", "arjun", `{"resolution":"done"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("close a full room: %d", resp.StatusCode)
	}
}
