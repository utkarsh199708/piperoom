package httpapi_test

import (
	"context"
	"net/http"
	"testing"

	"piperoom/internal/room"
)

const webhookURL = "/api/alerts/webhook"

// webhookIgnored is the 202 body for a resolved notice with no open room.
var webhookIgnored = map[string]any{"ignored": true, "reason": "resolved alert with no open room"}

// A resolved generic alert with no open room answers 202 with the ignored body
// and stores nothing.
func TestWebhookResolvedWithoutRoomReturns202(t *testing.T) {
	srv, svc := newServer(t)
	body := `{"title":"disk full","pipeline_id":"orders","status":"resolved"}`

	resp := doJSON(t, http.MethodPost, srv.URL+webhookURL, "arjun", body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("resolved generic alert: want 202, got %d", resp.StatusCode)
	}
	got := decode[map[string]any](t, resp)
	if len(got) != len(webhookIgnored) || got["ignored"] != true || got["reason"] != webhookIgnored["reason"] {
		t.Fatalf("ignored body: got %v, want %v", got, webhookIgnored)
	}
	if rooms, _ := svc.ListRooms(context.Background()); len(rooms) != 0 {
		t.Fatalf("ignored alert stored %d rooms, want 0", len(rooms))
	}
}

// A resolved notice whose room was closed answers 202, and the closed room is
// the only room there is.
func TestWebhookResolvedForClosedRoomReturns202(t *testing.T) {
	srv, svc := newServer(t)
	ctx := context.Background()
	firing := `{"title":"consumer lag","pipeline_id":"orders","dedup_key":"lag"}`
	resolved := `{"title":"consumer lag","pipeline_id":"orders","dedup_key":"lag","status":"resolved"}`

	resp := doJSON(t, http.MethodPost, srv.URL+webhookURL, "arjun", firing)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("firing alert: want 201, got %d", resp.StatusCode)
	}
	first := decode[room.RoomSnapshot](t, resp)

	arjun, _ := svc.LookupUser("arjun")
	if _, err := svc.GrantControl(ctx, arjun, first.ID, "arjun"); err != nil {
		t.Fatalf("take control: %v", err)
	}
	if _, err := svc.CloseRoom(ctx, arjun, first.ID, "fixed"); err != nil {
		t.Fatalf("close: %v", err)
	}

	resp = doJSON(t, http.MethodPost, srv.URL+webhookURL, "arjun", resolved)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("resolved alert for closed room: want 202, got %d", resp.StatusCode)
	}
	if got := decode[map[string]any](t, resp); got["ignored"] != true {
		t.Fatalf("ignored body: got %v", got)
	}
	if rooms, _ := svc.ListRooms(ctx); len(rooms) != 1 {
		t.Fatalf("want only the closed room, got %d rooms", len(rooms))
	}
}

// A resolved notice for an open room answers 200 with that room, and the room
// stays open.
func TestWebhookResolvedWithOpenRoomJoinsWith200(t *testing.T) {
	srv, svc := newServer(t)
	firing := `{"title":"consumer lag","pipeline_id":"orders","dedup_key":"lag"}`
	resolved := `{"title":"consumer lag","pipeline_id":"orders","dedup_key":"lag","status":"resolved"}`

	resp := doJSON(t, http.MethodPost, srv.URL+webhookURL, "arjun", firing)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("firing alert: want 201, got %d", resp.StatusCode)
	}
	first := decode[room.RoomSnapshot](t, resp)

	resp = doJSON(t, http.MethodPost, srv.URL+webhookURL, "arjun", resolved)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resolved alert with open room: want 200, got %d", resp.StatusCode)
	}
	joined := decode[room.RoomSnapshot](t, resp)
	if joined.ID != first.ID || joined.Status != "open" {
		t.Fatalf("joined room %s status %s, want open room %s", joined.ID, joined.Status, first.ID)
	}
	if got, err := svc.GetRoom(context.Background(), first.ID); err != nil || got.Status != "open" {
		t.Fatalf("room after resolved join: status %q, err %v; want open", got.Status, err)
	}
}

// A firing alert with no open room still answers 201 with a new room.
func TestWebhookFiringOpensRoomWith201(t *testing.T) {
	srv, _ := newServer(t)
	resp := doJSON(t, http.MethodPost, srv.URL+webhookURL, "arjun", `{"title":"disk full","pipeline_id":"orders"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("firing alert: want 201, got %d", resp.StatusCode)
	}
	if snap := decode[room.RoomSnapshot](t, resp); snap.ID == "" || snap.Status != "open" {
		t.Fatalf("firing alert: room %+v, want an open room", snap)
	}
}
