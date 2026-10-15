package httpapi_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/events"
)

func wantHTTPError(t *testing.T, resp *http.Response, status int, code string) {
	t.Helper()
	if resp.StatusCode != status {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("status = %d (%s), want %d %s", resp.StatusCode, b, status, code)
	}
	body := decode[map[string]any](t, resp)
	errObj, _ := body["error"].(map[string]any)
	if errObj["code"] != code {
		t.Fatalf("error = %v, want %q (body %v)", body["error"], code, body)
	}
}

func TestDeclineSuggestionRoute(t *testing.T) {
	srv, svc := newServer(t)
	ctx := context.Background()
	arjun, _ := svc.LookupUser("arjun")
	priya, _ := svc.LookupUser("priya")
	snap, err := svc.CreateRoom(ctx, arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	other, err := svc.CreateRoom(ctx, arjun, "t2", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	suggest := func() string {
		ev, err := svc.Steer(ctx, priya, snap.ID, "try this")
		if err != nil {
			t.Fatal(err)
		}
		p, err := events.Decode[events.SteerSuggestedPayload](ev)
		if err != nil {
			t.Fatal(err)
		}
		return p.SuggestionID
	}
	url := func(roomID, sid string) string {
		return srv.URL + "/api/rooms/" + roomID + "/suggestions/" + sid + "/decline"
	}

	sid := suggest()
	// 403 for a non-driver, 404 for an unknown sid and for another room's.
	wantHTTPError(t, doJSON(t, http.MethodPost, url(snap.ID, sid), "priya", `{"reason":"x"}`), http.StatusForbidden, "not_driver")
	wantHTTPError(t, doJSON(t, http.MethodPost, url(snap.ID, "g_nope"), "arjun", ""), http.StatusNotFound, "not_found")
	wantHTTPError(t, doJSON(t, http.MethodPost, url(other.ID, sid), "arjun", ""), http.StatusNotFound, "not_found")
	wantHTTPError(t, doJSON(t, http.MethodPost, url("r_nope", sid), "arjun", ""), http.StatusNotFound, "not_found")
	// 400 for a long reason, NUL, and a bad body. (Raw invalid UTF-8 in a
	// JSON body never reaches the service: encoding/json turns it into
	// U+FFFD, as on every other route; the service-level test covers it.)
	for _, body := range []string{
		`{"reason":"` + strings.Repeat("a", 501) + `"}`,
		`{"reason":"a\u0000b"}`,
		`{"reason":`,
		`{"reason":5}`,
	} {
		wantHTTPError(t, doJSON(t, http.MethodPost, url(snap.ID, sid), "arjun", body), http.StatusBadRequest, "validation")
	}

	// 200 with the appended event.
	resp := doJSON(t, http.MethodPost, url(snap.ID, sid), "arjun", `{"reason":"`+strings.Repeat("é", 500)+`"}`)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("decline: %d %s", resp.StatusCode, b)
	}
	ev := decode[events.Event](t, resp)
	if ev.Type != events.SteerDeclined || ev.Actor.ID != "arjun" || ev.Actor.Kind != events.ActorHuman {
		t.Fatalf("bad event: %+v", ev)
	}
	p, err := events.Decode[events.SteerDeclinedPayload](ev)
	if err != nil || p.SuggestionID != sid {
		t.Fatalf("bad payload %+v (%v)", p, err)
	}
	got, err := svc.GetRoom(ctx, snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.PendingSuggestions) != 0 {
		t.Fatalf("suggestion still pending: %+v", got.PendingSuggestions)
	}

	// 409 already_decided for a second decline and for a send after it.
	wantHTTPError(t, doJSON(t, http.MethodPost, url(snap.ID, sid), "arjun", ""), http.StatusConflict, "already_decided")
	wantHTTPError(t, doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+snap.ID+"/suggestions/"+sid+"/send", "arjun", ""), http.StatusConflict, "already_decided")

	// An empty body declines with no reason.
	sid2 := suggest()
	resp = doJSON(t, http.MethodPost, url(snap.ID, sid2), "arjun", "")
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("decline with empty body: %d %s", resp.StatusCode, b)
	}
	resp.Body.Close()

	// 409 room_closed once the room is closed.
	sid3 := suggest()
	if _, err := svc.CloseRoom(ctx, arjun, snap.ID, "fixed"); err != nil {
		t.Fatal(err)
	}
	wantHTTPError(t, doJSON(t, http.MethodPost, url(snap.ID, sid3), "arjun", ""), http.StatusConflict, "room_closed")
}
