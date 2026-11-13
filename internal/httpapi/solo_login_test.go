package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// newSoloAuthEnv is newAuthEnv for a server started with a solo user.
func newSoloAuthEnv(t *testing.T, solo string) *authEnv {
	t.Helper()
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), SoloUser: solo,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
	})
	sessions := auth.NewMemStore()
	api := httpapi.New(svc, st, nil, httpapi.WithSessions(sessions))
	srv := httptest.NewServer(api)
	t.Cleanup(func() { srv.Close(); api.Close() })
	return &authEnv{srv: srv, svc: svc, sessions: sessions, api: api}
}

// KAN-612: in solo mode only the solo user can sign in; everyone else in
// users.yaml is refused like an unknown user.
func TestSoloDevLoginRefusesOthers(t *testing.T) {
	e := newSoloAuthEnv(t, "arjun")
	for _, u := range []string{"priya", "kabir", "meera"} {
		resp := e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"`+u+`"}`)
		wantStatus(t, resp, http.StatusUnauthorized)
		if len(resp.Cookies()) != 0 {
			t.Errorf("%s: dev-login set a cookie", u)
		}
	}
	unknown := e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"nobody"}`)
	wantStatus(t, unknown, http.StatusUnauthorized)
}

func TestSoloXUserRefusesOthers(t *testing.T) {
	e := newSoloAuthEnv(t, "arjun")
	for _, u := range []string{"kabir", "priya"} {
		resp := e.do(t, http.MethodGet, "/api/me", "", "X-User: "+u)
		wantStatus(t, resp, http.StatusUnauthorized)
		resp = e.do(t, http.MethodGet, "/api/rooms", "", "X-User: "+u)
		wantStatus(t, resp, http.StatusUnauthorized)
	}
}

func TestSoloUserSignsInUnchanged(t *testing.T) {
	e := newSoloAuthEnv(t, "arjun")
	c := e.devLogin(t, "arjun")
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", cookieHdr(c)), http.StatusOK)
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "X-User: arjun"), http.StatusOK)
}

func TestNoSoloEveryUserSignsIn(t *testing.T) {
	e := newSoloAuthEnv(t, "")
	for _, u := range []string{"arjun", "priya", "kabir", "meera"} {
		c := e.devLogin(t, u)
		wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", cookieHdr(c)), http.StatusOK)
		wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "X-User: "+u), http.StatusOK)
	}
}
