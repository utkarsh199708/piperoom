package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/events"
)

func TestLabelRoute(t *testing.T) {
	srv, svc := newServer(t)
	ctx := context.Background()
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(ctx, arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/api/rooms/" + snap.ID + "/label"
	body := `{"actual_layer":"kafka-connect","cause":"slot dropped","fix_that_worked":"resume","agent_helped":true}`

	wantHTTPError(t, doJSON(t, http.MethodPost, url, "arjun", body), http.StatusConflict, "room_open")
	if _, err := svc.CloseRoom(ctx, arjun, snap.ID, "fixed"); err != nil {
		t.Fatal(err)
	}
	wantHTTPError(t, doJSON(t, http.MethodPost, srv.URL+"/api/rooms/r_nope/label", "arjun", body), http.StatusNotFound, "not_found")
	for _, bad := range []string{
		`{"cause":""}`, `{}`, `{"cause":"` + strings.Repeat("a", 501) + `"}`, `{"cause":"c","actual_layer":"moon"}`,
		`{"cause":"a\u0000b"}`, `{"cause":"c","fix_proposal_id":"p_nope"}`, `{"cause":`, `{"cause":5}`,
	} {
		wantHTTPError(t, doJSON(t, http.MethodPost, url, "arjun", bad), http.StatusBadRequest, "validation")
	}
	resp := doJSON(t, http.MethodPost, url, "arjun", body)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("label: %d %s", resp.StatusCode, b)
	}
	ev := decode[events.Event](t, resp)
	if ev.Type != events.RoomLabelled || ev.Actor.ID != "arjun" {
		t.Fatalf("event = %+v", ev)
	}
	var p events.RoomLabelledPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil || p.Cause != "slot dropped" || p.AgentHelped == nil || !*p.AgentHelped || p.Identity == nil {
		t.Fatalf("payload = %+v, %v", p, err)
	}
	// The snapshot says labelled.
	got := decode[map[string]any](t, doJSON(t, http.MethodGet, srv.URL+"/api/rooms/"+snap.ID, "arjun", ""))
	if got["labelled"] != true {
		t.Fatalf("snapshot labelled = %v", got["labelled"])
	}
}

func TestChecklistRoute(t *testing.T) {
	srv, svc := newServer(t)
	ctx := context.Background()
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(ctx, arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/api/rooms/" + snap.ID + "/checklist"
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "arjun", `{"step_id":"s","tool_use_id":"tu","hit":0,"item":0,"done":true}`), http.StatusBadRequest, "validation")
	for _, bad := range []string{`{}`, `{"step_id":"s","tool_use_id":"tu","item":0,"done":true}`, `{"step_id":"s","tool_use_id":"tu","hit":0,"item":0}`, `{"step_id":`} {
		wantHTTPError(t, doJSON(t, http.MethodPost, url, "arjun", bad), http.StatusBadRequest, "validation")
	}
	wantHTTPError(t, doJSON(t, http.MethodPost, srv.URL+"/api/rooms/r_nope/checklist", "arjun", `{"step_id":"s","tool_use_id":"tu","hit":0,"item":0,"done":true}`), http.StatusNotFound, "not_found")
}
