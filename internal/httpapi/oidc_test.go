package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/testenv"
)

// The OIDC login tests (docs/design/auth.md section 8, PR 3): a real
// httpapi.Server in oidc mode against testenv.FakeIdP. Requests go to the loopback
// httptest server, which the Host allow-list accepts; the cookies are sent by
// hand because the __Host- cookies are Secure and the server is plain http.

const oidcUsersYAML = `
users:
  - id: priya
    name: Priya
    email: priya@example.com
    layers: [postgres, schema-registry]
  - id: arjun
    name: Arjun
    sub: sub-arjun
    layers: [kafka-connect, kafka]
  - id: meera
    name: Meera
    email: meera@example.com
    layers: [warehouse]
  - id: kabir
    name: Kabir
    email: kabir@example.com
    sub: sub-kabir
    layers: [consumer]
  - id: ksenia
    name: Ksenia
    email: ksenia@example.com
`

// kafka-connect of the orders pipeline is replaced by a group and a backup, so
// arjun's users.yaml layer must not apply there. consumer has a user and a group.
const oidcOwnersYAML = `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
    owners:
      kafka-connect: { approvers: [group:dataeng-oncall], backup: [user:meera] }
layers:
  postgres: priya
  schema-registry: priya
  kafka: arjun
  kafka-connect: arjun
  warehouse: meera
  consumer: [user:kabir, group:consumers]
auditors: [group:security]
`

// testAction is a no-op action for one layer.
type testAction struct {
	layer string
	mu    sync.Mutex
	runs  int
}

func (a *testAction) Name() string     { return "fix_" + a.layer }
func (a *testAction) Layer() string    { return a.layer }
func (a *testAction) Describe() string { return "test action" }
func (a *testAction) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{}}
}
func (a *testAction) Validate(json.RawMessage) error                    { return nil }
func (a *testAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *testAction) Execute(context.Context, json.RawMessage) (string, error) {
	a.mu.Lock()
	a.runs++
	a.mu.Unlock()
	return "ok", nil
}

type oidcEnv struct {
	idp      *testenv.FakeIdP
	rp       *auth.OIDC
	srv      *httptest.Server
	api      *httpapi.Server
	svc      *room.Service
	own      *owners.Owners
	sessions *auth.MemStore
	states   *auth.MemLoginStates
	idents   *auth.MemIdentities
	logs     *syncBuf
	client   *http.Client
	acts     map[string]*testAction
}

type oidcOpts struct {
	trusted []netip.Prefix
	issuer  string // override: an unreachable IdP
	noInit  bool   // skip discovery
}

func newOIDCEnv(t *testing.T, o oidcOpts) *oidcEnv {
	t.Helper()
	idp := testenv.NewFakeIdP(t)
	issuer := idp.Issuer
	if o.issuer != "" {
		issuer = o.issuer
	}
	rp, err := auth.NewOIDC(auth.OIDCConfig{
		Issuer: issuer, ClientID: idp.ClientID, ClientSecret: idp.ClientSecret,
		RedirectURL: "https://piperoom.test/auth/callback",
		Scopes:      []string{"openid", "email", "profile"},
		GroupsClaim: "groups",
		HTTPClient:  &http.Client{Timeout: 2 * time.Second},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !o.noInit {
		if err := rp.Discover(context.Background()); err != nil {
			t.Fatalf("discovery: %v", err)
		}
	}

	users, err := room.ParseUsers([]byte(oidcUsersYAML))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(oidcOwnersYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	reg := actions.NewRegistry()
	acts := map[string]*testAction{}
	for _, l := range []string{"postgres", "kafka-connect", "consumer"} {
		a := &testAction{layer: l}
		acts[l] = a
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}

	st := memstore.New()
	sessions := auth.NewMemStore()
	logs := &syncBuf{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	svc := room.New(room.Deps{
		Store: st, Users: users, Owners: own, Actions: reg, Logger: log,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{WhatBroke: "x"}, nil
		},
		Sessions: sessions, SessionIdle: 8 * time.Hour, ViewerGroups: []string{"viewers"},
	})
	settings := auth.Default()
	settings.Mode = auth.ModeOIDC
	settings.ViewerGroups = []string{"viewers"}
	settings.TrustedProxies = o.trusted
	states, idents := auth.NewMemLoginStates(), auth.NewMemIdentities()
	api := httpapi.New(svc, st, log,
		httpapi.WithAuth(settings), httpapi.WithSessions(sessions), httpapi.WithLoginStates(states),
		httpapi.WithIdentities(idents), httpapi.WithOIDC(rp),
		httpapi.WithPublicOrigin(httpapi.PublicOrigin{Scheme: "https", Host: "piperoom.test"}))
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})
	return &oidcEnv{
		idp: idp, rp: rp, srv: srv, api: api, svc: svc, own: own, sessions: sessions, states: states, idents: idents, logs: logs, acts: acts,
		client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
}

// do sends a request without following redirects. hdr entries are "Name: value".
func (e *oidcEnv) do(t *testing.T, method, path, body string, hdr ...string) *http.Response {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, h := range hdr {
		k, v, _ := strings.Cut(h, ": ")
		req.Header.Add(k, v)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func cookieNamed(resp *http.Response, name string) *http.Cookie {
	for _, c := range resp.Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

func ck(c *http.Cookie) string { return "Cookie: " + c.Name + "=" + c.Value }

// loginOut is what a full login returned.
type loginOut struct {
	login    *http.Response // GET /auth/login
	callback *http.Response // GET /auth/callback
	lcookie  *http.Cookie   // the login cookie
	session  *http.Cookie   // the session cookie, nil when login failed
	authURL  *url.URL       // where /auth/login sent the browser
	code     string
	state    string
}

// startLogin runs GET /auth/login and returns the login cookie and the IdP URL.
func (e *oidcEnv) startLogin(t *testing.T, ret string, hdr ...string) (*http.Response, *http.Cookie, *url.URL) {
	t.Helper()
	path := "/auth/login"
	if ret != "" {
		path += "?return=" + url.QueryEscape(ret)
	}
	resp := e.do(t, http.MethodGet, path, "", hdr...)
	if resp.StatusCode != http.StatusFound {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s = %d, want 302; body %s", path, resp.StatusCode, b)
	}
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	return resp, cookieNamed(resp, auth.OIDCLoginCookie), loc
}

// idpAuthorize follows the IdP redirect without a browser and returns code and state.
func (e *oidcEnv) idpAuthorize(t *testing.T, authURL *url.URL) (code, state string) {
	t.Helper()
	resp, err := e.client.Get(authURL.String())
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("IdP authorize = %d", resp.StatusCode)
	}
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	return loc.Query().Get("code"), loc.Query().Get("state")
}

func (e *oidcEnv) callback(t *testing.T, code, state string, lc *http.Cookie) *http.Response {
	t.Helper()
	q := url.Values{"code": {code}, "state": {state}}
	var hdr []string
	if lc != nil {
		hdr = append(hdr, ck(lc))
	}
	return e.do(t, http.MethodGet, "/auth/callback?"+q.Encode(), "", hdr...)
}

// login runs the whole flow for u and returns every step.
func (e *oidcEnv) login(t *testing.T, u testenv.IDPUser, ret string) loginOut {
	t.Helper()
	e.idp.SetUser(u)
	lresp, lc, authURL := e.startLogin(t, ret)
	code, state := e.idpAuthorize(t, authURL)
	cb := e.callback(t, code, state, lc)
	return loginOut{login: lresp, callback: cb, lcookie: lc, session: cookieNamed(cb, auth.OIDCSessionCookie), authURL: authURL, code: code, state: state}
}

// mustLogin is login that must succeed; it returns the session cookie.
func (e *oidcEnv) mustLogin(t *testing.T, u testenv.IDPUser) *http.Cookie {
	t.Helper()
	out := e.login(t, u, "/")
	if out.callback.StatusCode != http.StatusFound || out.session == nil {
		b, _ := io.ReadAll(out.callback.Body)
		t.Fatalf("login of %s: status %d, body %s", u.Sub, out.callback.StatusCode, b)
	}
	return out.session
}

func (e *oidcEnv) me(t *testing.T, c *http.Cookie) map[string]any {
	t.Helper()
	resp := e.do(t, http.MethodGet, "/api/me", "", ck(c))
	wantStatus(t, resp, http.StatusOK)
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	return m
}

func errCode(t *testing.T, resp *http.Response) (code, msg string) {
	t.Helper()
	b := readErr(t, resp)
	return b.Error.Code, b.Error.Message
}

// Users the tests sign in as.
var (
	idpPriya   = testenv.IDPUser{Sub: "idp-priya", Email: "priya@example.com", EmailVerified: true, Name: "Priya From IdP", Groups: []string{}}
	idpOncall  = testenv.IDPUser{Sub: "idp-oncall", Email: "oncall@example.com", EmailVerified: true, Name: "On Call", Groups: []string{"dataeng-oncall"}}
	idpViewer  = testenv.IDPUser{Sub: "idp-viewer", Email: "viewer@example.com", EmailVerified: true, Name: "Vera Viewer", Groups: []string{"viewers"}}
	idpAuditor = testenv.IDPUser{Sub: "idp-auditor", Email: "audit@example.com", EmailVerified: true, Name: "Audrey Auditor", Groups: []string{"security"}}
)

// ---- the happy path and the session ----

func TestOIDCLoginFlow(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	out := e.login(t, idpPriya, "/rooms/r_x?tab=trace")

	// /auth/login: PKCE S256 and a nonce, state, the redirect URI, a login cookie.
	q := out.authURL.Query()
	if q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "" || q.Get("nonce") == "" || q.Get("state") == "" {
		t.Fatalf("authorize URL lacks PKCE S256, nonce or state: %s", out.authURL)
	}
	if got := e.idp.LastChallengeMethod(); got != "S256" {
		t.Fatalf("IdP saw code_challenge_method %q, want S256", got)
	}
	if q.Get("redirect_uri") != "https://piperoom.test/auth/callback" || q.Get("client_id") != e.idp.ClientID || q.Get("response_type") != "code" {
		t.Fatalf("authorize URL = %s", out.authURL)
	}
	if !strings.Contains(q.Get("scope"), "openid") {
		t.Fatalf("scope = %q", q.Get("scope"))
	}
	lc := out.lcookie
	if lc == nil || lc.Name != "__Host-piperoom_login" || !lc.Secure || !lc.HttpOnly || lc.SameSite != http.SameSiteLaxMode || lc.Path != "/" || lc.Domain != "" || lc.MaxAge != 600 {
		t.Fatalf("login cookie = %+v", lc)
	}
	if out.login.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("/auth/login is cacheable: %q", out.login.Header.Get("Cache-Control"))
	}
	if e.states.Len() != 0 {
		t.Errorf("the login row survives the callback: %d rows", e.states.Len())
	}

	// /auth/callback: redirect to the return path, session cookie, login cookie cleared.
	if out.callback.StatusCode != http.StatusFound || out.callback.Header.Get("Location") != "/rooms/r_x?tab=trace" {
		t.Fatalf("callback = %d, Location %q", out.callback.StatusCode, out.callback.Header.Get("Location"))
	}
	s := out.session
	if s == nil || s.Name != "__Host-piperoom_session" || !s.Secure || !s.HttpOnly || s.SameSite != http.SameSiteLaxMode || s.Path != "/" || s.Domain != "" || len(s.Value) != 43 {
		t.Fatalf("session cookie = %+v", s)
	}
	if c := cookieNamed(out.callback, auth.OIDCLoginCookie); c == nil || c.Value != "" || c.MaxAge >= 0 {
		t.Fatalf("login cookie not cleared: %+v", c)
	}

	// The stored session carries the claims. priya matched by verified email, so
	// the id is priya and the name is the users.yaml name.
	hash, _ := auth.HashToken(s.Value)
	sess, err := e.sessions.ByTokenHash(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if sess.UserID != "priya" || sess.Name != "Priya" || sess.Method != auth.MethodOIDC || sess.Iss != e.idp.Issuer || sess.Sub != "idp-priya" || sess.GroupsMissing {
		t.Fatalf("session = %+v", sess)
	}
	me := e.me(t, s)
	if me["id"] != "priya" || me["role"] != "member" || me["groups_missing"] != false {
		t.Fatalf("/api/me = %+v", me)
	}
	if got, _ := me["layers"].([]any); len(got) != 2 || got[0] != "postgres" || got[1] != "schema-registry" {
		t.Fatalf("layers = %v", me["layers"])
	}

	// Logging in again in the same browser retires the old session.
	e.idp.SetUser(idpPriya)
	_, lc2, au2 := e.startLogin(t, "/")
	code, state := e.idpAuthorize(t, au2)
	cb := e.do(t, http.MethodGet, "/auth/callback?"+url.Values{"code": {code}, "state": {state}}.Encode(), "", ck(lc2), ck(s))
	if cb.StatusCode != http.StatusFound {
		t.Fatalf("second login = %d", cb.StatusCode)
	}
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", ck(s)), http.StatusUnauthorized)

	// Logout revokes the session.
	s2 := cookieNamed(cb, auth.OIDCSessionCookie)
	wantStatus(t, e.do(t, http.MethodPost, "/auth/logout", "", ck(s2)), http.StatusNoContent)
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", ck(s2)), http.StatusUnauthorized)
}

func TestOIDCSessionStoresClaims(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	u := idpOncall
	u.AMR, u.ACR = []string{"pwd", "mfa"}, "urn:mfa"
	s := e.mustLogin(t, u)
	hash, _ := auth.HashToken(s.Value)
	sess, err := e.sessions.ByTokenHash(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(sess.Groups) != 1 || sess.Groups[0] != "dataeng-oncall" || len(sess.AMR) != 2 || sess.ACR != "urn:mfa" {
		t.Fatalf("session = %+v", sess)
	}
	// No users.yaml entry: id is u_ + 12 hex of sha256(iss|sub); name from the claim.
	want := httpapi.DerivedUserID(e.idp.Issuer, "idp-oncall")
	if sess.UserID != want || !strings.HasPrefix(want, "u_") || len(want) != 14 || sess.Name != "On Call" {
		t.Fatalf("session user = %q (%q), want %q / On Call", sess.UserID, sess.Name, want)
	}
}

// ---- callback failures ----

func TestOIDCCallbackFailures(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	e.idp.SetUser(idpPriya)

	t.Run("no login cookie is 400 login_expired", func(t *testing.T) {
		_, _, au := e.startLogin(t, "/")
		code, state := e.idpAuthorize(t, au)
		resp := e.callback(t, code, state, nil)
		wantStatus(t, resp, http.StatusBadRequest)
		if c, _ := errCode(t, resp); c != "login_expired" {
			t.Fatalf("code = %q", c)
		}
	})
	t.Run("a cookie naming no row is 400 login_expired", func(t *testing.T) {
		_, _, au := e.startLogin(t, "/")
		code, state := e.idpAuthorize(t, au)
		resp := e.callback(t, code, state, &http.Cookie{Name: auth.OIDCLoginCookie, Value: "nosuchrow"})
		wantStatus(t, resp, http.StatusBadRequest)
		if c, _ := errCode(t, resp); c != "login_expired" {
			t.Fatalf("code = %q", c)
		}
	})
	t.Run("an expired row is 400 login_expired", func(t *testing.T) {
		_, lc, au := e.startLogin(t, "/")
		code, state := e.idpAuthorize(t, au)
		// Age the row: replace it with an expired copy under the same id.
		past := time.Now().Add(-time.Minute)
		_ = e.states.Create(context.Background(), auth.LoginState{ID: lc.Value, State: state, Nonce: "n", Verifier: "v", ReturnPath: "/", ExpiresAt: past}, lc.Value, past.Add(-time.Hour))
		resp := e.callback(t, code, state, lc)
		wantStatus(t, resp, http.StatusBadRequest)
		if c, _ := errCode(t, resp); c != "login_expired" {
			t.Fatalf("code = %q", c)
		}
	})
	t.Run("a replayed callback is 400", func(t *testing.T) {
		out := e.login(t, idpPriya, "/")
		if out.callback.StatusCode != http.StatusFound {
			t.Fatalf("first callback = %d", out.callback.StatusCode)
		}
		again := e.callback(t, out.code, out.state, out.lcookie)
		wantStatus(t, again, http.StatusBadRequest)
		if c, _ := errCode(t, again); c != "login_expired" {
			t.Fatalf("replay code = %q, want login_expired (the row is single use)", c)
		}
	})
	t.Run("a wrong state is 401", func(t *testing.T) {
		_, lc, au := e.startLogin(t, "/")
		code, _ := e.idpAuthorize(t, au)
		resp := e.callback(t, code, "not-the-state", lc)
		wantStatus(t, resp, http.StatusUnauthorized)
		if cookieNamed(resp, auth.OIDCSessionCookie) != nil {
			t.Fatal("a session was issued")
		}
	})
	t.Run("a missing state is 401", func(t *testing.T) {
		_, lc, au := e.startLogin(t, "/")
		code, _ := e.idpAuthorize(t, au)
		resp := e.do(t, http.MethodGet, "/auth/callback?code="+url.QueryEscape(code), "", ck(lc))
		wantStatus(t, resp, http.StatusUnauthorized)
	})
	t.Run("login CSRF: the attacker's code and state in the victim's browser", func(t *testing.T) {
		// The victim started a login (cookie cv). The attacker finished the IdP
		// step in their own browser and sends the victim the callback URL.
		_, cv, _ := e.startLogin(t, "/")
		_, ca, au := e.startLogin(t, "/")
		codeA, stateA := e.idpAuthorize(t, au)
		resp := e.callback(t, codeA, stateA, cv)
		wantStatus(t, resp, http.StatusUnauthorized)
		if cookieNamed(resp, auth.OIDCSessionCookie) != nil {
			t.Fatal("the victim was logged in as the attacker")
		}
		_ = ca
	})
	for name, fault := range map[string]testenv.IDPFault{
		"a wrong nonce":    testenv.FaultWrongNonce,
		"a bad signature":  testenv.FaultBadSignature,
		"a wrong audience": testenv.FaultWrongAudience,
		"an expired token": testenv.FaultExpired,
	} {
		t.Run(name+" is 401", func(t *testing.T) {
			e.idp.SetFault(fault)
			defer e.idp.SetFault(testenv.FaultNone)
			out := e.login(t, idpPriya, "/")
			wantStatus(t, out.callback, http.StatusUnauthorized)
			if out.session != nil {
				t.Fatal("a session was issued")
			}
		})
	}
	t.Run("the IdP refusing the login is 401", func(t *testing.T) {
		_, lc, au := e.startLogin(t, "/")
		resp := e.do(t, http.MethodGet, "/auth/callback?error=access_denied&state="+url.QueryEscape(au.Query().Get("state")), "", ck(lc))
		wantStatus(t, resp, http.StatusUnauthorized)
		if !strings.Contains(e.logs.String(), "refused the login") {
			t.Fatal("the IdP refusal path was not taken")
		}
	})
	t.Run("a missing code is 400", func(t *testing.T) {
		_, lc, au := e.startLogin(t, "/")
		_, state := e.idpAuthorize(t, au)
		resp := e.do(t, http.MethodGet, "/auth/callback?state="+url.QueryEscape(state), "", ck(lc))
		wantStatus(t, resp, http.StatusBadRequest)
	})
}

func TestOIDCNewLoginReplacesTheOldRow(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	e.idp.SetUser(idpPriya)
	_, c1, _ := e.startLogin(t, "/")
	_, c2, au := e.startLogin(t, "/", ck(c1)) // same browser: sends its old cookie
	if e.states.Len() != 1 {
		t.Fatalf("rows = %d, want 1 live row per cookie", e.states.Len())
	}
	code, state := e.idpAuthorize(t, au)
	// The old cookie's row is gone.
	wantStatus(t, e.callback(t, code, state, c1), http.StatusBadRequest)
	_, c3, au3 := e.startLogin(t, "/")
	code3, state3 := e.idpAuthorize(t, au3)
	wantStatus(t, e.callback(t, code3, state3, c3), http.StatusFound)
	_ = c2
}

// ---- return= ----

func TestOIDCReturnOpenRedirect(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	cases := map[string]string{ // ?return= value -> final Location
		"/rooms/r_1":                    "/rooms/r_1",
		"/rooms/r_1?tab=x":              "/rooms/r_1?tab=x",
		"":                              "/",
		"//evil.com":                    "/",
		"https://evil.com/x":            "/",
		"http://evil.com":               "/",
		`/\evil.com`:                    "/",
		"/%2F%2Fevil.com":               "/",
		"/%5Cevil.com":                  "/",
		"javascript:alert(1)":           "/",
		"evil.com":                      "/",
		"/x%0d%0aSet-Cookie:a=b":        "/",
		"/" + strings.Repeat("a", 2100): "/",
	}
	for ret, want := range cases {
		out := e.login(t, idpPriya, ret)
		if out.callback.StatusCode != http.StatusFound {
			t.Fatalf("return=%q: callback %d", ret, out.callback.StatusCode)
		}
		if got := out.callback.Header.Get("Location"); got != want {
			t.Errorf("return=%q: redirected to %q, want %q", ret, got, want)
		}
	}
	// A raw (unescaped) control character in the query string is a bad request
	// for net/http itself, so it never reaches the handler.
}

// ---- the login gate and identity mapping ----

func TestOIDCLoginGate(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})

	t.Run("an unmapped user is 403 not_provisioned", func(t *testing.T) {
		out := e.login(t, testenv.IDPUser{Sub: "stranger", Email: "x@example.com", EmailVerified: true, Name: "Stranger"}, "/")
		wantStatus(t, out.callback, http.StatusForbidden)
		if out.session != nil || cookieNamed(out.callback, auth.OIDCSessionCookie) != nil {
			t.Fatal("a cookie was set")
		}
		if c, _ := errCode(t, out.callback); c != "not_provisioned" {
			t.Fatalf("code = %q", c)
		}
	})
	t.Run("a group that owners.yaml does not name is 403", func(t *testing.T) {
		out := e.login(t, testenv.IDPUser{Sub: "s2", Groups: []string{"random-group"}}, "/")
		wantStatus(t, out.callback, http.StatusForbidden)
	})
	t.Run("a group that owners.yaml names passes, as a member", func(t *testing.T) {
		s := e.mustLogin(t, idpOncall)
		if me := e.me(t, s); me["role"] != "member" {
			t.Fatalf("me = %v", me)
		}
	})
	t.Run("a viewer group passes, as a viewer", func(t *testing.T) {
		s := e.mustLogin(t, idpViewer)
		if me := e.me(t, s); me["role"] != "viewer" {
			t.Fatalf("me = %v", me)
		}
	})
	t.Run("auditors: alone passes, as a viewer", func(t *testing.T) {
		s := e.mustLogin(t, idpAuditor)
		if me := e.me(t, s); me["role"] != "viewer" {
			t.Fatalf("me = %v", me)
		}
	})
}

// The gate runs on every request: a session whose user is no longer allowed is
// 401 session_expired at once, not at the 12 h cap.
func TestOIDCGateRunsOnEveryRequest(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	// A user who is in no file (a users.yaml entry that was removed).
	now := time.Now().UTC()
	token, hash, _ := auth.NewToken()
	if err := e.sessions.Create(context.Background(), auth.Session{
		ID: room.NewID("ses_"), TokenHash: hash, UserID: "removed-user", Name: "Gone", Method: auth.MethodOIDC,
		Iss: e.idp.Issuer, Sub: "x", CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	resp := e.do(t, http.MethodGet, "/api/me", "", "Cookie: "+auth.OIDCSessionCookie+"="+token)
	wantStatus(t, resp, http.StatusUnauthorized)
	if b := readErr(t, resp); b.Error.Code != "session_expired" || b.Error.AuthMode != "oidc" {
		t.Fatalf("error = %+v", b.Error)
	}

	// A group-only member loses the group in owners.yaml: out on the next request.
	s := e.mustLogin(t, idpOncall)
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", ck(s)), http.StatusOK)
	e.own.Pipelines[0].Owners = nil
	delete(e.own.Layers, "consumer")
	e.own.Layers["warehouse"] = owners.Principals{{Kind: owners.KindUser, Name: "meera"}}
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", ck(s)), http.StatusUnauthorized)
}

func TestOIDCIdentityMapping(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	ctx := context.Background()
	user := func(c *http.Cookie) string { return e.me(t, c)["id"].(string) }

	// A users.yaml sub: wins, and needs no email.
	if got := user(e.mustLogin(t, testenv.IDPUser{Sub: "sub-arjun", Name: "Whatever"})); got != "arjun" {
		t.Fatalf("sub match: %q, want arjun", got)
	}

	// The first verified-email match binds iss+sub.
	if got := user(e.mustLogin(t, idpPriya)); got != "priya" {
		t.Fatalf("email match: %q, want priya", got)
	}
	if bound, err := e.idents.UserFor(ctx, e.idp.Issuer, "idp-priya"); err != nil || bound != "priya" {
		t.Fatalf("identities row = %q, %v", bound, err)
	}
	// After that the binding decides, whatever the email claim says.
	moved := idpPriya
	moved.Email, moved.EmailVerified = "new-address@example.com", false
	if got := user(e.mustLogin(t, moved)); got != "priya" {
		t.Fatalf("bound login: %q, want priya", got)
	}

	// A recycled email: the same address on a different sub is refused.
	recycled := testenv.IDPUser{Sub: "idp-priya-2", Email: "priya@example.com", EmailVerified: true, Name: "Someone Else"}
	out := e.login(t, recycled, "/")
	wantStatus(t, out.callback, http.StatusForbidden)
	if out.session != nil {
		t.Fatal("a recycled email got a session")
	}

	// Unverified email never matches: meera's address, not verified, is a stranger.
	out = e.login(t, testenv.IDPUser{Sub: "idp-meera-fake", Email: "meera@example.com", EmailVerified: false, Name: "Not Meera"}, "/")
	wantStatus(t, out.callback, http.StatusForbidden)
	if bound, err := e.idents.UserFor(ctx, e.idp.Issuer, "idp-meera-fake"); err == nil {
		t.Fatalf("an unverified email was bound to %q", bound)
	}

	// A users.yaml user pinned to a sub is not matched by email on another sub.
	out = e.login(t, testenv.IDPUser{Sub: "other-sub", Email: "kabir@example.com", EmailVerified: true}, "/")
	wantStatus(t, out.callback, http.StatusForbidden)
	if got := user(e.mustLogin(t, testenv.IDPUser{Sub: "sub-kabir"})); got != "kabir" {
		t.Fatalf("kabir by sub: %q", got)
	}

	// Unbind (devctl identities unbind priya): the new account can bind.
	if n, err := e.idents.Unbind(ctx, "priya"); err != nil || n != 1 {
		t.Fatalf("unbind = %d, %v", n, err)
	}
	if got := user(e.mustLogin(t, recycled)); got != "priya" {
		t.Fatalf("after unbind: %q, want priya", got)
	}
	// ... and the old sub now falls to the email rule, which finds priya bound
	// elsewhere, so it is refused.
	out = e.login(t, idpPriya, "/")
	wantStatus(t, out.callback, http.StatusForbidden)
}

// ---- roles ----

func (e *oidcEnv) newRoom(t *testing.T, owner string) room.RoomSnapshot {
	t.Helper()
	u, ok := e.svc.LookupUser(owner)
	if !ok {
		t.Fatalf("no user %s", owner)
	}
	snap, err := e.svc.CreateRoom(context.Background(), u, "title", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestOIDCViewerIsReadOnly(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	snap := e.newRoom(t, "priya")
	v := e.mustLogin(t, idpViewer)
	vh := ck(v)

	// GET and WebSocket work.
	wantStatus(t, e.do(t, http.MethodGet, "/api/rooms", "", vh), http.StatusOK)
	wantStatus(t, e.do(t, http.MethodGet, "/api/rooms/"+snap.ID, "", vh), http.StatusOK)
	wantStatus(t, e.do(t, http.MethodGet, "/api/rooms/"+snap.ID+"/events?after=0", "", vh), http.StatusOK)
	wantStatus(t, e.do(t, http.MethodGet, "/api/pipelines/orders/topology", "", vh), http.StatusOK)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, wresp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(e.srv.URL, "http")+"/api/rooms/"+snap.ID+"/ws?after=0",
		&websocket.DialOptions{HTTPHeader: http.Header{"Cookie": {v.Name + "=" + v.Value}}})
	if err != nil {
		t.Fatalf("viewer WebSocket: %v (resp %v)", err, wresp)
	}
	conn.Close(websocket.StatusNormalClosure, "")

	// Every non-GET under /api is 403 viewer, including the hand-off accept.
	posts := []struct{ path, body string }{
		{"/api/rooms", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`},
		{"/api/rooms/" + snap.ID + "/comments", `{"text":"hi"}`},
		{"/api/rooms/" + snap.ID + "/steer", `{"text":"hi"}`},
		{"/api/rooms/" + snap.ID + "/control/request", `{}`},
		{"/api/rooms/" + snap.ID + "/control/grant", `{"to":"priya"}`},
		{"/api/rooms/" + snap.ID + "/handoffs", `{"to":"priya"}`},
		{"/api/handoffs/h_nonexistent/accept", `{}`},
		{"/api/rooms/" + snap.ID + "/close", `{"resolution":"x"}`},
		{"/api/rooms/" + snap.ID + "/trace", `{"table":"public.orders","key":"1"}`},
		{"/api/proposals/p_x/approve", `{"layer":"postgres"}`},
		{"/api/proposals/p_x/reject", `{"layer":"postgres","reason":"no"}`},
		{"/api/rooms/" + snap.ID + "/suggestions/g_x/send", `{}`},
		{"/api/alerts/webhook", `{}`},
	}
	for _, p := range posts {
		resp := e.do(t, http.MethodPost, p.path, p.body, vh)
		if resp.StatusCode != http.StatusForbidden {
			b, _ := io.ReadAll(resp.Body)
			// The unsigned webhook is not mounted in oidc mode; a viewer never
			// reaches it, and it is a 404 for everybody.
			if p.path == "/api/alerts/webhook" && resp.StatusCode == http.StatusNotFound {
				continue
			}
			t.Errorf("POST %s = %d, want 403 viewer; body %s", p.path, resp.StatusCode, b)
			continue
		}
		if c, _ := errCode(t, resp); c != "viewer" {
			t.Errorf("POST %s: code %q, want viewer", p.path, c)
		}
	}
	// Other methods too: the rule is on the method, not on a list of routes.
	for _, m := range []string{http.MethodPut, http.MethodPatch, http.MethodDelete} {
		resp := e.do(t, m, "/api/rooms/"+snap.ID+"/comments", `{}`, vh)
		if resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s = %d", m, resp.StatusCode)
		}
	}

	// Export needs auditors:.
	resp := e.do(t, http.MethodGet, "/api/rooms/"+snap.ID+"/export", "", vh)
	wantStatus(t, resp, http.StatusForbidden)
	if c, _ := errCode(t, resp); c != "not_allowed" {
		t.Fatalf("viewer export code = %q, want not_allowed", c)
	}
	// A viewer can still sign out.
	wantStatus(t, e.do(t, http.MethodPost, "/auth/logout", "", vh), http.StatusNoContent)
}

func TestOIDCAuditorOnlyIsAViewerPlusExport(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	snap := e.newRoom(t, "priya")
	a := e.mustLogin(t, idpAuditor)
	ah := ck(a)
	wantStatus(t, e.do(t, http.MethodGet, "/api/rooms/"+snap.ID, "", ah), http.StatusOK)
	resp := e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/steer", `{"text":"x"}`, ah)
	wantStatus(t, resp, http.StatusForbidden)
	if c, _ := errCode(t, resp); c != "viewer" {
		t.Fatalf("steer code = %q, want viewer", c)
	}
	resp = e.do(t, http.MethodGet, "/api/rooms/"+snap.ID+"/export", "", ah)
	wantStatus(t, resp, http.StatusOK)
	if resp.Header.Get("X-Piperoom-Export-SHA256") == "" {
		t.Error("export has no hash header")
	}
	me := e.me(t, a)
	if me["role"] != "viewer" {
		t.Fatalf("me = %v", me)
	}
	if l, _ := me["layers"].([]any); len(l) != 0 {
		t.Fatalf("an auditor approves nothing, layers = %v", me["layers"])
	}
}

// ---- LookupPerson: control and hand-offs for people not in users.yaml ----

func TestOIDCLookupPerson(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	priya := e.mustLogin(t, idpPriya)
	oncall := e.mustLogin(t, idpOncall)
	e.mustLogin(t, idpViewer)
	e.mustLogin(t, idpAuditor)
	oncallID := httpapi.DerivedUserID(e.idp.Issuer, "idp-oncall")
	viewerID := httpapi.DerivedUserID(e.idp.Issuer, "idp-viewer")
	auditorID := httpapi.DerivedUserID(e.idp.Issuer, "idp-auditor")
	neverID := httpapi.DerivedUserID(e.idp.Issuer, "never-logged-in")

	grant := func(roomID, to string) *http.Response {
		return e.do(t, http.MethodPost, "/api/rooms/"+roomID+"/control/grant", fmt.Sprintf(`{"to":%q}`, to), ck(priya))
	}
	handoff := func(roomID, to string) *http.Response {
		return e.do(t, http.MethodPost, "/api/rooms/"+roomID+"/handoffs", fmt.Sprintf(`{"to":%q,"stay_as_watcher":true}`, to), ck(priya))
	}

	t.Run("a group-only member with a live session can be given control", func(t *testing.T) {
		snap := e.newRoom(t, "priya")
		wantStatus(t, grant(snap.ID, oncallID), http.StatusOK)
		got, err := e.svc.GetRoom(context.Background(), snap.ID)
		if err != nil || got.Driver == nil || got.Driver.ID != oncallID || got.Driver.Name != "On Call" {
			t.Fatalf("driver = %+v, %v", got.Driver, err)
		}
	})
	t.Run("a group-only member can receive a hand-off and accept it", func(t *testing.T) {
		snap := e.newRoom(t, "priya")
		resp := handoff(snap.ID, oncallID)
		wantStatus(t, resp, http.StatusOK)
		var ev events.Event
		_ = json.NewDecoder(resp.Body).Decode(&ev)
		var p events.HandoffCreatedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil || p.HandoffID == "" {
			t.Fatalf("payload = %s, %v", ev.Payload, err)
		}
		wantStatus(t, e.do(t, http.MethodPost, "/api/handoffs/"+p.HandoffID+"/accept", `{}`, ck(oncall)), http.StatusOK)
		got, _ := e.svc.GetRoom(context.Background(), snap.ID)
		if got.Driver == nil || got.Driver.ID != oncallID {
			t.Fatalf("driver after accept = %+v", got.Driver)
		}
	})
	t.Run("everyone else is 400 validation", func(t *testing.T) {
		snap := e.newRoom(t, "priya")
		first := ""
		for name, id := range map[string]string{
			"a viewer":                     viewerID,
			"an auditor-only principal":    auditorID,
			"a person who never logged in": neverID,
			"an unknown id":                "nobody-at-all",
			"an empty id":                  "",
		} {
			for kind, call := range map[string]func(string, string) *http.Response{"grant": grant, "handoff": handoff} {
				resp := call(snap.ID, id)
				if resp.StatusCode != http.StatusBadRequest {
					b, _ := io.ReadAll(resp.Body)
					t.Errorf("%s to %s = %d, want 400; body %s", kind, name, resp.StatusCode, b)
					continue
				}
				c, msg := errCode(t, resp)
				if c != "validation" {
					t.Errorf("%s to %s: code %q", kind, name, c)
				}
				// One message for every refusal, so a reply does not reveal which
				// ids exist or what role someone has.
				if first == "" {
					first = msg
				} else if msg != first {
					t.Errorf("%s to %s: message %q differs from %q", kind, name, msg, first)
				}
			}
		}
	})
	t.Run("a group member who signed out can no longer be picked", func(t *testing.T) {
		snap := e.newRoom(t, "priya")
		wantStatus(t, e.do(t, http.MethodPost, "/auth/logout", "", ck(oncall)), http.StatusNoContent)
		wantStatus(t, grant(snap.ID, oncallID), http.StatusBadRequest)
		// Signing in again makes them pickable again.
		e.mustLogin(t, idpOncall)
		wantStatus(t, grant(snap.ID, oncallID), http.StatusOK)
	})
	t.Run("a users.yaml user needs no session", func(t *testing.T) {
		snap := e.newRoom(t, "priya")
		wantStatus(t, grant(snap.ID, "meera"), http.StatusOK)
	})
}

// ---- approvals ----

func (e *oidcEnv) proposal(t *testing.T, roomID string, layers ...string) string {
	t.Helper()
	var steps []events.ProposalStep
	for _, l := range layers {
		steps = append(steps, events.ProposalStep{Action: "fix_" + l, Layer: l, Params: json.RawMessage(`{}`), Description: "do " + l})
	}
	pid, err := e.svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix", Steps: steps, Risk: "low risk", Rollback: "undo it",
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func (e *oidcEnv) approve(t *testing.T, c *http.Cookie, pid, layer string) *http.Response {
	t.Helper()
	return e.do(t, http.MethodPost, "/api/proposals/"+pid+"/approve", fmt.Sprintf(`{"layer":%q}`, layer), ck(c))
}

func TestOIDCApprovals(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	snap := e.newRoom(t, "priya")
	oncall := e.mustLogin(t, idpOncall)
	arjun := e.mustLogin(t, testenv.IDPUser{Sub: "sub-arjun"})
	meera := e.mustLogin(t, testenv.IDPUser{Sub: "idp-meera", Email: "meera@example.com", EmailVerified: true})

	t.Run("a group owner approves", func(t *testing.T) {
		pid := e.proposal(t, snap.ID, "kafka-connect")
		wantStatus(t, e.approve(t, oncall, pid, "kafka-connect"), http.StatusOK)
		waitFor(t, func() bool { return e.acts["kafka-connect"].count() == 1 }, "the approved step to run")
		if !strings.Contains(e.logs.String(), "grant=group:dataeng-oncall") {
			t.Errorf("the grant is not in the log:\n%s", e.logs.String())
		}
	})
	t.Run("the users.yaml layer does not apply where a pipeline owners: block replaces it", func(t *testing.T) {
		pid := e.proposal(t, snap.ID, "kafka-connect")
		resp := e.approve(t, arjun, pid, "kafka-connect")
		wantStatus(t, resp, http.StatusForbidden)
		if c, _ := errCode(t, resp); c != "not_layer_owner" {
			t.Fatalf("code = %q", c)
		}
	})
	t.Run("a backup approves at once and the room is told", func(t *testing.T) {
		pid := e.proposal(t, snap.ID, "kafka-connect")
		wantStatus(t, e.approve(t, meera, pid, "kafka-connect"), http.StatusOK)
		if !strings.Contains(e.logs.String(), "grant=backup:user:meera") {
			t.Errorf("no backup grant in the log:\n%s", e.logs.String())
		}
		evs, _, err := e.svc.Events(context.Background(), snap.ID, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, ev := range evs {
			if ev.Type == events.AgentMessage && strings.Contains(string(ev.Payload), "backup approver") {
				found = true
			}
		}
		if !found {
			t.Error("the room got no system message naming the backup")
		}
	})
	t.Run("users.yaml layers still work where nothing replaces them", func(t *testing.T) {
		priya := e.mustLogin(t, idpPriya)
		pid := e.proposal(t, snap.ID, "postgres")
		wantStatus(t, e.approve(t, priya, pid, "postgres"), http.StatusOK)
		// Someone who owns another layer cannot.
		pid = e.proposal(t, snap.ID, "postgres")
		wantStatus(t, e.approve(t, meera, pid, "postgres"), http.StatusForbidden)
	})
	t.Run("a group on a default layer", func(t *testing.T) {
		consumers := e.mustLogin(t, testenv.IDPUser{Sub: "idp-consumer", Groups: []string{"consumers"}})
		pid := e.proposal(t, snap.ID, "consumer")
		wantStatus(t, e.approve(t, consumers, pid, "consumer"), http.StatusOK)
		// Rejection uses the same rights.
		pid = e.proposal(t, snap.ID, "consumer")
		resp := e.do(t, http.MethodPost, "/api/proposals/"+pid+"/reject", `{"layer":"consumer","reason":"no"}`, ck(consumers))
		wantStatus(t, resp, http.StatusOK)
		resp = e.do(t, http.MethodPost, "/api/proposals/"+pid+"/reject", `{"layer":"consumer","reason":"no"}`, ck(oncall))
		wantStatus(t, resp, http.StatusConflict) // already decided, rights were not the problem
	})
}

func TestOIDCMeLayers(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	oncall := e.mustLogin(t, idpOncall)
	me := e.me(t, oncall)
	if l, _ := me["layers"].([]any); len(l) != 1 || l[0] != "kafka-connect" {
		t.Fatalf("on-call layers = %v, want [kafka-connect]", me["layers"])
	}
	both := e.mustLogin(t, testenv.IDPUser{Sub: "idp-both", Groups: []string{"dataeng-oncall", "consumers"}})
	me = e.me(t, both)
	if l, _ := me["layers"].([]any); len(l) != 2 || l[0] != "kafka-connect" || l[1] != "consumer" {
		t.Fatalf("two-group layers = %v, want [kafka-connect consumer] in layer order", me["layers"])
	}
	// meera: her users.yaml layer plus the backup layer.
	meera := e.mustLogin(t, testenv.IDPUser{Sub: "idp-meera", Email: "meera@example.com", EmailVerified: true})
	me = e.me(t, meera)
	if l, _ := me["layers"].([]any); len(l) != 2 || l[0] != "kafka-connect" || l[1] != "warehouse" {
		t.Fatalf("meera layers = %v, want [kafka-connect warehouse]", me["layers"])
	}
}

func TestOIDCMissingGroupsClaim(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	snap := e.newRoom(t, "priya")
	pid := e.proposal(t, snap.ID, "kafka-connect")

	// kabir is a users.yaml user (so the gate passes) but the token has no groups
	// claim, so the group that owns kafka-connect cannot apply.
	e.idp.SetFault(testenv.FaultMissingGroups)
	kabir := e.mustLogin(t, testenv.IDPUser{Sub: "sub-kabir", Groups: []string{"dataeng-oncall"}})
	e.idp.SetFault(testenv.FaultNone)

	me := e.me(t, kabir)
	if me["groups_missing"] != true {
		t.Fatalf("/api/me = %v, want groups_missing true", me)
	}
	if n := strings.Count(e.logs.String(), "no groups claim in id token"); n != 1 {
		t.Fatalf("Warn lines = %d, want exactly 1 per login:\n%s", n, e.logs.String())
	}
	resp := e.approve(t, kabir, pid, "kafka-connect")
	wantStatus(t, resp, http.StatusForbidden)
	code, msg := errCode(t, resp)
	if code != "not_layer_owner" || msg != "no groups claim in token (IdP overage?)" {
		t.Fatalf("error = %q %q", code, msg)
	}
	// A layer with no group in play keeps the ordinary message.
	pid2 := e.proposal(t, snap.ID, "postgres")
	resp = e.approve(t, kabir, pid2, "postgres")
	wantStatus(t, resp, http.StatusForbidden)
	if _, msg := errCode(t, resp); strings.Contains(msg, "groups claim") {
		t.Fatalf("a layer without a group blamed the groups claim: %q", msg)
	}
	// With the claim present the same person is just not an owner.
	k2 := e.mustLogin(t, testenv.IDPUser{Sub: "sub-kabir", Groups: []string{"unrelated"}})
	if e.me(t, k2)["groups_missing"] != false {
		t.Fatal("groups_missing stuck on")
	}
	resp = e.approve(t, k2, pid, "kafka-connect")
	wantStatus(t, resp, http.StatusForbidden)
	if _, msg := errCode(t, resp); strings.Contains(msg, "groups claim") {
		t.Fatalf("message %q blamed the claim although it was present", msg)
	}
}

// ---- IdP down, rate limit ----

func TestOIDCIdPUnavailable(t *testing.T) {
	// An address nobody listens on: discovery fails.
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	e := newOIDCEnv(t, oidcOpts{issuer: deadURL, noInit: true})

	for _, p := range []string{"/auth/login?return=/", "/auth/callback?code=x&state=y"} {
		resp := e.do(t, http.MethodGet, p, "")
		wantStatus(t, resp, http.StatusServiceUnavailable)
		if c, _ := errCode(t, resp); c != "idp_unavailable" {
			t.Fatalf("GET %s: code %q, want idp_unavailable", p, c)
		}
	}
	// Live sessions keep working: they never touch the IdP.
	now := time.Now().UTC()
	token, hash, _ := auth.NewToken()
	if err := e.sessions.Create(context.Background(), auth.Session{
		ID: room.NewID("ses_"), TokenHash: hash, UserID: "priya", Name: "Priya", Method: auth.MethodOIDC,
		CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "Cookie: "+auth.OIDCSessionCookie+"="+token), http.StatusOK)
}

func TestOIDCLoginRateLimit(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	for i := 1; i <= 300; i++ {
		resp := e.do(t, http.MethodGet, "/auth/login", "")
		if resp.StatusCode != http.StatusFound {
			t.Fatalf("request %d = %d, want 302 (the limit is 300 a minute)", i, resp.StatusCode)
		}
	}
	resp := e.do(t, http.MethodGet, "/auth/login", "")
	wantStatus(t, resp, http.StatusTooManyRequests)
	if resp.Header.Get("Retry-After") == "" {
		t.Error("no Retry-After")
	}
	// X-Forwarded-For does not buy a new budget from an untrusted peer.
	wantStatus(t, e.do(t, http.MethodGet, "/auth/login", "", "X-Forwarded-For: 203.0.113.9"), http.StatusTooManyRequests)
	// The login rows stayed bounded: one per cookie-less call is replaced only by
	// expiry, but never more than were made.
	if e.states.Len() > 300 {
		t.Fatalf("rows = %d", e.states.Len())
	}
}

func TestOIDCLoginRateLimitBehindATrustedProxy(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{trusted: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}})
	// Everyone shares the proxy's address; the client comes from X-Forwarded-For.
	for i := 1; i <= 300; i++ {
		resp := e.do(t, http.MethodGet, "/auth/login", "", "X-Forwarded-For: 203.0.113.9")
		if resp.StatusCode != http.StatusFound {
			t.Fatalf("request %d = %d", i, resp.StatusCode)
		}
	}
	wantStatus(t, e.do(t, http.MethodGet, "/auth/login", "", "X-Forwarded-For: 203.0.113.9"), http.StatusTooManyRequests)
	// Another client behind the same proxy is not locked out.
	wantStatus(t, e.do(t, http.MethodGet, "/auth/login", "", "X-Forwarded-For: 203.0.113.10"), http.StatusFound)
	// A spoofed leftmost entry does not help: the rightmost untrusted one counts.
	wantStatus(t, e.do(t, http.MethodGet, "/auth/login", "", "X-Forwarded-For: 198.51.100.1, 203.0.113.9"), http.StatusTooManyRequests)
	// With the proxy's own address at the end, it is skipped.
	wantStatus(t, e.do(t, http.MethodGet, "/auth/login", "", "X-Forwarded-For: 203.0.113.9, 127.0.0.1"), http.StatusTooManyRequests)
}

// ---- the secret ----

func TestOIDCClientSecretIsNeverLogged(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	e.mustLogin(t, idpPriya)
	e.login(t, idpPriya, "//evil")
	if strings.Contains(e.logs.String(), e.idp.ClientSecret) {
		t.Fatalf("the client secret is in the log:\n%s", e.logs.String())
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (a *testAction) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.runs
}
