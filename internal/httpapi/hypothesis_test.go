package httpapi_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestHypothesisVerdictRoute(t *testing.T) {
	srv, svc := newServer(t)
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(context.Background(), arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/api/rooms/" + snap.ID + "/hypothesis"

	wantHTTPError(t, doJSON(t, http.MethodPost, srv.URL+"/api/rooms/r_nope/hypothesis", "priya", `{"verdict":"confirmed"}`), http.StatusNotFound, "not_found")
	for _, body := range []string{`{"verdict":"maybe"}`, `{}`, `{"verdict":`, `{"verdict":5}`,
		`{"verdict":"disputed","note":"` + strings.Repeat("a", 501) + `"}`, `{"verdict":"disputed","note":"a\u0000b"}`} {
		wantHTTPError(t, doJSON(t, http.MethodPost, url, "priya", body), http.StatusBadRequest, "validation")
	}
	// A valid verdict before the agent has a hypothesis, from a non-driver.
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "priya", `{"verdict":"confirmed"}`), http.StatusConflict, "no_hypothesis")
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "", `{"verdict":"confirmed"}`), http.StatusUnauthorized, "session_expired")
}

func TestNudgeRoute(t *testing.T) {
	srv, svc := newServer(t)
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(context.Background(), arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/api/rooms/" + snap.ID + "/nudge"

	wantHTTPError(t, doJSON(t, http.MethodPost, srv.URL+"/api/rooms/r_nope/nudge", "priya", `{"proposal_id":"p_1","layer":"kafka"}`), http.StatusNotFound, "not_found")
	for _, body := range []string{`{}`, `{"proposal_id":"p_1","layer":"nope"}`, `{"proposal_id":"","layer":"kafka"}`, `{"proposal_id":`} {
		wantHTTPError(t, doJSON(t, http.MethodPost, url, "priya", body), http.StatusBadRequest, "validation")
	}
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "priya", `{"proposal_id":"p_nope","layer":"kafka"}`), http.StatusConflict, "already_decided")
}

// Viewers get 403 viewer from both routes (default-deny guard).
func TestOIDCViewerCannotDecideOrNudge(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	c := e.mustLogin(t, idpViewer)
	for path, body := range map[string]string{
		"/api/rooms/r_x/hypothesis": `{"verdict":"confirmed"}`,
		"/api/rooms/r_x/nudge":      `{"proposal_id":"p_1","layer":"kafka"}`,
	} {
		resp := e.do(t, http.MethodPost, path, body, ck(c))
		wantStatus(t, resp, http.StatusForbidden)
		if code, _ := errCode(t, resp); code != "viewer" {
			t.Errorf("%s: code %q, want viewer", path, code)
		}
	}
}
