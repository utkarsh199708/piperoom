package httpapi_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestPageRoute(t *testing.T) {
	srv, svc := newServer(t)
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(context.Background(), arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/api/rooms/" + snap.ID + "/page"

	wantHTTPError(t, doJSON(t, http.MethodPost, srv.URL+"/api/rooms/r_nope/page", "priya", `{"layer":"consumer","reason":"x"}`), http.StatusNotFound, "not_found")
	for _, body := range []string{
		`{}`, `{"layer":"consumer"}`, `{"layer":"nope","reason":"x"}`, `{"layer":"consumer","reason":"   "}`,
		`{"layer":"consumer","reason":"` + strings.Repeat("a", 301) + `"}`, `{"layer":"consumer","reason":"a\u0000b"}`,
		`{"layer":"consumer","reason":"x","proposal_id":"p_nope"}`, `{"layer":`, `{"layer":5,"reason":"x"}`,
	} {
		wantHTTPError(t, doJSON(t, http.MethodPost, url, "priya", body), http.StatusBadRequest, "validation")
	}
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "", `{"layer":"consumer","reason":"x"}`), http.StatusUnauthorized, "session_expired")

	resp := doJSON(t, http.MethodPost, url, "priya", `{"layer":"consumer","reason":"lag keeps growing"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("page status = %d", resp.StatusCode)
	}
	ev := decode[map[string]any](t, resp)
	payload, _ := ev["payload"].(map[string]any)
	owners, _ := payload["owners"].([]any)
	if ev["type"] != "owner.paged" || payload["layer"] != "consumer" || len(owners) != 1 || owners[0] != "kabir" {
		t.Fatalf("event = %+v", ev)
	}
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "kabir", `{"layer":"consumer","reason":"again"}`), http.StatusConflict, "already_paged")

	// The snapshot and the room list carry the page.
	room := decode[map[string]any](t, doJSON(t, http.MethodGet, srv.URL+"/api/rooms/"+snap.ID, "priya", ""))
	if pages, _ := room["pages"].([]any); len(pages) != 1 {
		t.Fatalf("snapshot pages = %v", room["pages"])
	}
	list := decode[[]map[string]any](t, doJSON(t, http.MethodGet, srv.URL+"/api/rooms", "priya", ""))
	if pages, _ := list[0]["pages"].([]any); len(pages) != 1 {
		t.Fatalf("list pages = %v", list[0]["pages"])
	}

	if _, err := svc.CloseRoom(context.Background(), arjun, snap.ID, "done"); err != nil {
		t.Fatal(err)
	}
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "priya", `{"layer":"kafka","reason":"x"}`), http.StatusConflict, "room_closed")
}

func TestOIDCViewerCannotPage(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	c := e.mustLogin(t, idpViewer)
	resp := e.do(t, http.MethodPost, "/api/rooms/r_x/page", `{"layer":"consumer","reason":"x"}`, ck(c))
	wantStatus(t, resp, http.StatusForbidden)
	if code, _ := errCode(t, resp); code != "viewer" {
		t.Errorf("code %q, want viewer", code)
	}
}
