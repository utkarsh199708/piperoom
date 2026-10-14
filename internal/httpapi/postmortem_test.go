package httpapi_test

import (
	"context"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

func getPostmortem(t *testing.T, srvURL, query, roomID, user string) (*http.Response, string) {
	t.Helper()
	resp := doJSON(t, http.MethodGet, srvURL+"/api/rooms/"+roomID+"/postmortem.md"+query, user, "")
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(b)
}

func TestPostmortemRouteServesEscapedMarkdown(t *testing.T) {
	srv, _ := newServer(t)
	id := createRoomHTTP(t, srv.URL, "arjun")
	doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+id+"/comments", "kabir", `{"text":"# pwned\n[x](http://evil) <script>"}`).Body.Close()

	// kabir is not the driver and not an approver: the export refuses him, the
	// draft does not.
	resp, body := getPostmortem(t, srv.URL, "", id, "kabir")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/markdown; charset=utf-8" {
		t.Fatalf("content type %q", ct)
	}
	if resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("headers %v", resp.Header)
	}
	if resp.Header.Get("Content-Disposition") != "" {
		t.Fatal("no attachment without ?download=1")
	}
	for _, bad := range []string{"\n# pwned", "[x](", "<script>"} {
		if strings.Contains(body, bad) {
			t.Fatalf("unescaped %q in:\n%s", bad, body)
		}
	}
	if !strings.Contains(body, `Kabir commented: \# pwned`) || !strings.Contains(body, "Status: open (draft so far)") {
		t.Fatalf("draft:\n%s", body)
	}

	resp, _ = getPostmortem(t, srv.URL, "?download=1", id, "kabir")
	disp, params, err := mime.ParseMediaType(resp.Header.Get("Content-Disposition"))
	if err != nil || disp != "attachment" || params["filename"] != "piperoom-"+id+"-postmortem.md" {
		t.Fatalf("disposition %q (%v)", resp.Header.Get("Content-Disposition"), err)
	}

	if _, body := getPostmortem(t, srv.URL, "?with_hash=1", id, "kabir"); !strings.Contains(body, "Export sha256") {
		t.Fatal("with_hash=1 adds the hash")
	}
}

func TestPostmortemRouteErrors(t *testing.T) {
	srv, _ := newServer(t)
	if resp, body := getPostmortem(t, srv.URL, "", "r_missing", "arjun"); resp.StatusCode != http.StatusNotFound || !strings.Contains(body, `"not_found"`) {
		t.Fatalf("unknown room: %d %s", resp.StatusCode, body)
	}
	if resp, _ := getPostmortem(t, srv.URL, "", "r_missing", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no user: %d, want 401", resp.StatusCode)
	}

	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Run: fakeRun, ExportMaxEvents: 2,
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{WhatBroke: "x"}, nil
		},
	})
	small := httptest.NewServer(httpapi.New(svc, st, nil))
	defer small.Close()
	defer svc.Close()
	id := createRoomHTTP(t, small.URL, "arjun")
	for i := 0; i < 3; i++ {
		doJSON(t, http.MethodPost, small.URL+"/api/rooms/"+id+"/comments", "arjun", `{"text":"hi"}`).Body.Close()
	}
	if resp, body := getPostmortem(t, small.URL, "", id, "arjun"); resp.StatusCode != http.StatusRequestEntityTooLarge || !strings.Contains(body, "export_too_large") {
		t.Fatalf("over the cap: %d %s", resp.StatusCode, body)
	}
}
