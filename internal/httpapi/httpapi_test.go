package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

func fakeRun(ctx context.Context, alert agent.Alert, deps agent.Deps, opts agent.Options) error {
	<-ctx.Done()
	return nil
}

func newServer(t *testing.T) (*httptest.Server, *room.Service) {
	t.Helper()
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st,
		Users: room.DefaultUsers(),
		Run:   fakeRun,
		WriteHandoff: func(ctx context.Context, evs []events.Event, opts agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{WhatBroke: "x"}, nil
		},
	})
	h := httpapi.New(svc, st, nil)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, svc
}

func doJSON(t *testing.T, method, url, user, body string) *http.Response {
	t.Helper()
	var rdr *bytes.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if user != "" {
		req.Header.Set("X-User", user)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func decode[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var v T
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return v
}

func TestMeAndAuth(t *testing.T) {
	srv, _ := newServer(t)

	resp := doJSON(t, http.MethodGet, srv.URL+"/api/me", "arjun", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("me status: %d", resp.StatusCode)
	}
	me := decode[map[string]any](t, resp)
	if me["id"] != "arjun" || me["layers"] == nil {
		t.Fatalf("bad me: %+v", me)
	}

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/me", "", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing user status: %d", resp.StatusCode)
	}
	body := decode[map[string]any](t, resp)
	errObj := body["error"].(map[string]any)
	if errObj["code"] != "session_expired" {
		t.Fatalf("bad error code: %+v", errObj)
	}

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/me", "nobody", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown user status: %d", resp.StatusCode)
	}
}

func TestCreateRoomAndList(t *testing.T) {
	srv, _ := newServer(t)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status: %d", resp.StatusCode)
	}
	snap := decode[map[string]any](t, resp)
	if snap["status"] != "open" {
		t.Fatalf("bad snapshot: %+v", snap)
	}
	driver := snap["driver"].(map[string]any)
	if driver["id"] != "arjun" {
		t.Fatalf("creator should be driver: %+v", driver)
	}

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/rooms", "arjun", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status: %d", resp.StatusCode)
	}
	list := decode[[]map[string]any](t, resp)
	if len(list) != 1 {
		t.Fatalf("want 1 room, got %d", len(list))
	}
}

func TestSteerAndCommentAndErrors(t *testing.T) {
	srv, svc := newServer(t)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	roomID := decode[map[string]any](t, resp)["id"].(string)

	// Non-driver steer -> steer.suggested.
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/steer", "priya", `{"text":"check it"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("steer status: %d", resp.StatusCode)
	}
	ev := decode[map[string]any](t, resp)
	if ev["type"] != "steer.suggested" {
		t.Fatalf("want steer.suggested, got %v", ev["type"])
	}

	// Comment works.
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/comments", "priya", `{"text":"hi @agent"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("comment status: %d", resp.StatusCode)
	}

	// priya is not the driver but owns the postgres layer, so she may close
	// (Q0, QA G16; the 403 for a plain member is in TestCloseRouteWho).
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/close", "priya", `{"resolution":"x"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("close status: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 404 for unknown room.
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/rooms/nope", "arjun", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("404 status: %d", resp.StatusCode)
	}

	_ = svc
}

// failingStore returns an error from GetRoom so the 500 path is exercised.
type failingStore struct {
	*memstore.Store
}

func (f failingStore) GetRoom(ctx context.Context, id string) (store.Room, error) {
	return store.Room{}, errors.New("db down")
}

func TestStoreDownReturns500(t *testing.T) {
	st := memstore.New()
	fs := failingStore{Store: st}
	svc := room.New(room.Deps{Store: fs, Users: room.DefaultUsers(), Run: fakeRun})
	srv := httptest.NewServer(httpapi.New(svc, fs, nil))
	t.Cleanup(srv.Close)

	resp := doJSON(t, http.MethodGet, srv.URL+"/api/rooms/r1", "arjun", "")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d", resp.StatusCode)
	}
	body := decode[map[string]any](t, resp)
	if body["error"].(map[string]any)["code"] != "internal_error" {
		t.Fatalf("bad error: %+v", body)
	}
}

func TestUnknownProposalAndHandoffReturn404(t *testing.T) {
	srv, _ := newServer(t)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/proposals/nope/approve", "arjun", `{"layer":"postgres","params_hash":"x"}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("proposal approve: want 404, got %d", resp.StatusCode)
	}

	resp = doJSON(t, http.MethodPost, srv.URL+"/api/handoffs/nope/accept", "arjun", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("handoff accept: want 404, got %d", resp.StatusCode)
	}
}

func TestEventsEndpoint(t *testing.T) {
	srv, _ := newServer(t)
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	roomID := decode[map[string]any](t, resp)["id"].(string)

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/rooms/"+roomID+"/events?after=0&limit=500", "arjun", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("events status: %d", resp.StatusCode)
	}
	body := decode[map[string]any](t, resp)
	evs := body["events"].([]any)
	if len(evs) != 2 { // room.created + control.granted
		t.Fatalf("want 2 events, got %d", len(evs))
	}
	if body["last_seq"].(float64) != 2 {
		t.Fatalf("bad last_seq: %v", body["last_seq"])
	}

	// After the last seq, events is an empty array, never null (docs/api.md;
	// review item 4). The type assertion fails on nil, so this checks the JSON
	// really contains [].
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/rooms/"+roomID+"/events?after=2", "arjun", "")
	body = decode[map[string]any](t, resp)
	empty, ok := body["events"].([]any)
	if !ok || len(empty) != 0 {
		t.Fatalf("events after last seq must be [], got %#v", body["events"])
	}
}

// TestEveryRouteRequiresXUser: every handler (and the ws handshake) answers
// 401 unauthenticated for a missing or unknown X-User (review item 4).
func TestEveryRouteRequiresXUser(t *testing.T) {
	srv, _ := newServer(t)
	routes := []struct {
		method, path, body string
	}{
		{http.MethodGet, "/api/me", ""},
		{http.MethodGet, "/api/rooms", ""},
		{http.MethodPost, "/api/rooms", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`},
		{http.MethodPost, "/api/alerts/webhook", `{}`},
		{http.MethodGet, "/api/rooms/r1", ""},
		{http.MethodGet, "/api/rooms/r1/events", ""},
		{http.MethodPost, "/api/rooms/r1/comments", `{"text":"x"}`},
		{http.MethodPost, "/api/rooms/r1/steer", `{"text":"x"}`},
		{http.MethodPost, "/api/rooms/r1/suggestions/s1/send", ""},
		{http.MethodPost, "/api/rooms/r1/suggestions/s1/decline", ""},
		{http.MethodPost, "/api/rooms/r1/label", `{"cause":"x"}`},
		{http.MethodPost, "/api/rooms/r1/checklist", `{"step_id":"s","tool_use_id":"t","hit":0,"item":0,"done":true}`},
		{http.MethodPost, "/api/rooms/r1/control/request", ""},
		{http.MethodPost, "/api/rooms/r1/control/grant", `{"to":"arjun"}`},
		{http.MethodPost, "/api/rooms/r1/close", `{"resolution":"x"}`},
		{http.MethodPost, "/api/rooms/r1/handoffs", `{"to":"priya"}`},
		{http.MethodPost, "/api/rooms/r1/trace", `{"table":"t","key":"k"}`},
		{http.MethodPost, "/api/proposals/p1/approve", `{"layer":"postgres"}`},
		{http.MethodPost, "/api/proposals/p1/reject", `{"layer":"postgres"}`},
		{http.MethodPost, "/api/handoffs/h1/accept", ""},
		{http.MethodGet, "/api/pipelines/orders/topology", ""},
	}
	for _, rt := range routes {
		for _, user := range []string{"", "nobody"} {
			resp := doJSON(t, rt.method, srv.URL+rt.path, user, rt.body)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("%s %s (user %q): want 401, got %d", rt.method, rt.path, user, resp.StatusCode)
			}
			body := decode[map[string]any](t, resp)
			wantCode := "unauthenticated"
			if user == "" {
				wantCode = "session_expired" // no session at all: the web app shows the dev picker
			}
			if body["error"].(map[string]any)["code"] != wantCode {
				t.Fatalf("%s %s: bad envelope %+v", rt.method, rt.path, body)
			}
		}
	}
	// The WebSocket handshake authenticates via ?user= and needs it too.
	resp := doJSON(t, http.MethodGet, srv.URL+"/api/rooms/r1/ws", "", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("ws without user: want 401, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestBodyCap: request bodies over 1 MB are refused with 400 validation.
func TestBodyCap(t *testing.T) {
	srv, _ := newServer(t)
	big := strings.Repeat("a", 2<<20)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun",
		`{"title":"`+big+`","severity":"sev2","pipeline_id":"orders"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized create: want 400, got %d", resp.StatusCode)
	}
	body := decode[map[string]any](t, resp)
	if body["error"].(map[string]any)["code"] != "validation" {
		t.Fatalf("oversized create: bad envelope %+v", body)
	}

	resp = doJSON(t, http.MethodPost, srv.URL+"/api/alerts/webhook", "arjun", `{"x":"`+big+`"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized webhook: want 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestUnknownRoomReturns404Not403: a missing room is 404 even when the caller
// is not the driver (the room check wins over the role check, docs/api.md).
func TestUnknownRoomReturns404Not403(t *testing.T) {
	srv, _ := newServer(t)
	// priya is a non-driver everywhere; the rooms do not exist.
	routes := []struct{ method, path, body string }{
		{http.MethodPost, "/api/rooms/nope/close", `{"resolution":"x"}`},
		{http.MethodPost, "/api/rooms/nope/steer", `{"text":"x"}`},
		{http.MethodPost, "/api/rooms/nope/comments", `{"text":"x"}`},
		{http.MethodPost, "/api/rooms/nope/handoffs", `{"to":"arjun"}`},
		{http.MethodPost, "/api/rooms/nope/control/grant", `{"to":"arjun"}`},
	}
	for _, rt := range routes {
		resp := doJSON(t, rt.method, srv.URL+rt.path, "priya", rt.body)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: want 404, got %d", rt.path, resp.StatusCode)
		}
		body := decode[map[string]any](t, resp)
		if body["error"].(map[string]any)["code"] != "not_found" {
			t.Fatalf("%s: bad envelope %+v", rt.path, body)
		}
	}
}

// TestWebhookPassesRawBody: the handler forwards the raw JSON body verbatim
// (docs/api.md) and accepts non-object JSON.
func TestWebhookPassesRawBody(t *testing.T) {
	srv, svc := newServer(t)
	raw := `{"title":"Webhook room","severity":"sev1","pipeline_id":"orders","extra":[1,2]}`
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/alerts/webhook", "arjun", raw)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("webhook: want 201, got %d", resp.StatusCode)
	}
	snap := decode[map[string]any](t, resp)
	if snap["pipeline_id"] != "orders" || snap["severity"] != "sev1" {
		t.Fatalf("webhook fields not mapped: %+v", snap)
	}

	evs, _, err := svc.Events(context.Background(), snap["id"].(string), 0, 10)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	p, err := events.Decode[events.RoomCreatedPayload](evs[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(p.Alert) != raw {
		t.Fatalf("raw body not verbatim:\n got %s\nwant %s", p.Alert, raw)
	}

	// A non-object body maps nothing but still opens a room.
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/alerts/webhook", "arjun", `[1,2,3]`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("array webhook: want 201, got %d", resp.StatusCode)
	}
	snap = decode[map[string]any](t, resp)
	if snap["pipeline_id"] != "unknown" {
		t.Fatalf("array webhook defaults: %+v", snap)
	}
}

// TestWebhookRepeatReturnsOpenRoom: the same alert twice opens one room; the
// first answer is 201 Created and the repeat is 200 OK with that same room.
func TestWebhookRepeatReturnsOpenRoom(t *testing.T) {
	srv, _ := newServer(t)
	raw := `{"title":"Repeat me","pipeline_id":"orders","dedup_key":"repeat-me"}`
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/alerts/webhook", "arjun", raw)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("first alert: want 201, got %d", resp.StatusCode)
	}
	first := decode[map[string]any](t, resp)

	resp = doJSON(t, http.MethodPost, srv.URL+"/api/alerts/webhook", "arjun", raw)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("repeat alert: want 200, got %d", resp.StatusCode)
	}
	second := decode[map[string]any](t, resp)
	if second["id"] != first["id"] {
		t.Fatalf("repeat opened room %v, want %v", second["id"], first["id"])
	}
}

// TestRouterErrorEnvelopes: router-level 404/405 use the same JSON envelope.
func TestRouterErrorEnvelopes(t *testing.T) {
	srv, _ := newServer(t)

	resp := doJSON(t, http.MethodGet, srv.URL+"/api/nope", "arjun", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown route: want 404, got %d", resp.StatusCode)
	}
	body := decode[map[string]any](t, resp)
	if body["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("unknown route envelope: %+v", body)
	}

	resp = doJSON(t, http.MethodPost, srv.URL+"/api/me", "arjun", "")
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("wrong method: want 405, got %d", resp.StatusCode)
	}
	body = decode[map[string]any](t, resp)
	if body["error"].(map[string]any)["code"] != "method_not_allowed" {
		t.Fatalf("wrong method envelope: %+v", body)
	}
}
