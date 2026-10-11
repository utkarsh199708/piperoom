package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
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
	if errObj["code"] != "unauthenticated" {
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

	// priya cannot close (not driver).
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/close", "priya", `{"resolution":"x"}`)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("close status: %d", resp.StatusCode)
	}
	body := decode[map[string]any](t, resp)
	if body["error"].(map[string]any)["code"] != "not_driver" {
		t.Fatalf("bad error: %+v", body)
	}

	// 404 for unknown room.
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/rooms/nope", "arjun", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("404 status: %d", resp.StatusCode)
	}

	_ = svc
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
}
