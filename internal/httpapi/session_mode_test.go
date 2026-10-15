package httpapi_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/httpapi"
)

// A dev-login session row left in the shared sessions table from before a
// switch to oidc mode (or minted by a dev server on the same room DB) must not
// act in oidc mode: it has no IdP behind it and skips the users.yaml sub: pin.
func TestOIDCModeRefusesDevSession(t *testing.T) {
	e := newAuthEnv(t, httpapi.WithAuth(oidcSettings()))
	now := time.Now().UTC()
	devToken, devID := e.seedSessionMethod(t, auth.MethodDev, "arjun", now, now, now.Add(time.Hour))
	oidcToken, oidcID := e.seedSessionMethod(t, auth.MethodOIDC, "arjun", now, now, now.Add(time.Hour))

	resp := e.do(t, http.MethodGet, "/api/me", "", "Cookie: "+auth.OIDCSessionCookie+"="+devToken)
	wantStatus(t, resp, http.StatusUnauthorized)
	if b := readErr(t, resp); b.Error.Code != "session_expired" {
		t.Fatalf("dev session in oidc mode: code %q, want session_expired", b.Error.Code)
	}
	if e.api.SessionAliveForTest(context.Background(), devID) {
		t.Fatal("presence tick keeps a dev session alive in oidc mode")
	}

	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "Cookie: "+auth.OIDCSessionCookie+"="+oidcToken), http.StatusOK)
	if !e.api.SessionAliveForTest(context.Background(), oidcID) {
		t.Fatal("presence tick drops a live oidc session")
	}
}

// Lowering PIPEROOM_SESSION_MAX applies to sessions made under the old,
// longer one: the stored expires_at alone would keep them for the old length.
func TestLoweredSessionMaxEndsOlderSessions(t *testing.T) {
	e := newAuthEnv(t) // default Max 12 h
	now := time.Now().UTC()
	old, oldID := e.seedSession(t, "priya", now.Add(-13*time.Hour), now, now.Add(7*24*time.Hour))
	fresh, _ := e.seedSession(t, "priya", now.Add(-time.Hour), now, now.Add(7*24*time.Hour))

	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "Cookie: "+auth.DevSessionCookie+"="+old), http.StatusUnauthorized)
	if e.api.SessionAliveForTest(context.Background(), oldID) {
		t.Fatal("presence tick keeps a session past the current max")
	}
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "Cookie: "+auth.DevSessionCookie+"="+fresh), http.StatusOK)
}
