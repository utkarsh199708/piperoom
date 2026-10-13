//go:build e2e

// Package oidc_test drives the real OIDC login of a piperoom container against a
// real Keycloak, with an HTTP client and a cookie jar instead of a browser:
// /auth/login -> Keycloak login form -> /auth/callback -> /api/me. The stack is
// test/e2e/oidc/docker-compose.yml; run it with test/e2e/oidc/run.sh (README.md).
//
// Error codes and paths are the ones in docs/api.md and
// internal/httpapi/auth_oidc.go: a failed browser login is a 302 to
// /?login_error=<code>, an API error is {"error":{"code":...}}.
package oidc_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	sessionCookie = "__Host-piperoom_session"
	loginCookie   = "__Host-piperoom_login"
	maxBody       = 4 << 20
)

// env returns the environment value or def. The defaults match the compose file.
func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

var (
	baseURL   = env("PIPEROOM_E2E_BASE", "https://localhost:8443")
	issuerURL = env("PIPEROOM_E2E_ISSUER", "https://localhost:8444/realms/piperoom")
	caFile    = env("PIPEROOM_E2E_CA", ".certs/ca.crt")
)

// ---- HTTP client ----

// client is one "browser": its own cookie jar, the test CA, no proxy, and no
// automatic redirects, so every hop of the flow is checked.
type client struct {
	t   *testing.T
	hc  *http.Client
	jar *cookiejar.Jar
}

var (
	rootsOnce sync.Once
	roots     *x509.CertPool
	rootsErr  error
)

func newClient(t *testing.T) *client {
	t.Helper()
	rootsOnce.Do(func() {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			rootsErr = err
			return
		}
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			rootsErr = os.ErrInvalid
		}
	})
	if rootsErr != nil {
		t.Fatalf("read the test CA %s (run ./run.sh, or go run ./certgen first): %v", caFile, rootsErr)
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookie jar: %v", err)
	}
	return &client{t: t, jar: jar, hc: &http.Client{
		Jar:     jar,
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			Proxy:           nil, // localhost only; never an outbound proxy
			TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// resp is a read response: status, headers and a capped body.
type resp struct {
	status int
	header http.Header
	body   []byte
}

func (r resp) location() string { return r.header.Get("Location") }

// setCookie returns the Set-Cookie of the response with that name, or nil.
func (r resp) setCookie(name string) *http.Cookie {
	for _, c := range (&http.Response{Header: r.header}).Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

func (r resp) errorCode() string {
	var e struct {
		Error struct {
			Code     string `json:"code"`
			AuthMode string `json:"auth_mode"`
		} `json:"error"`
	}
	_ = json.Unmarshal(r.body, &e)
	return e.Error.Code
}

func (c *client) do(req *http.Request) resp {
	c.t.Helper()
	res, err := c.hc.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", req.Method, req.URL, err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, maxBody))
	if err != nil {
		c.t.Fatalf("%s %s: read body: %v", req.Method, req.URL, err)
	}
	return resp{status: res.StatusCode, header: res.Header, body: b}
}

func (c *client) get(raw string) resp {
	c.t.Helper()
	req, err := http.NewRequest(http.MethodGet, raw, nil)
	if err != nil {
		c.t.Fatalf("new request %s: %v", raw, err)
	}
	return c.do(req)
}

// postJSON sends a JSON POST. No Origin or Sec-Fetch-Site header, like curl:
// CrossOriginProtection lets those through, so only auth decides.
func (c *client) postJSON(raw, body string) resp {
	c.t.Helper()
	req, err := http.NewRequest(http.MethodPost, raw, strings.NewReader(body))
	if err != nil {
		c.t.Fatalf("new request %s: %v", raw, err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req)
}

// withCookie sends a GET carrying only the given cookie (no jar), as an
// attacker who copied a cookie value would.
func withCookie(t *testing.T, raw, name, value string) resp {
	t.Helper()
	c := newClient(t)
	req, err := http.NewRequest(http.MethodGet, raw, nil)
	if err != nil {
		t.Fatalf("new request %s: %v", raw, err)
	}
	req.AddCookie(&http.Cookie{Name: name, Value: value})
	return c.do(req)
}

// ---- stack readiness ----

var (
	readyOnce sync.Once
	readyErr  string
)

// waitStack waits until Keycloak serves its discovery document with the
// expected issuer, the server is ready, and the server's own discovery has
// succeeded (GET /auth/login redirects to Keycloak instead of
// /?login_error=idp_unavailable).
func waitStack(t *testing.T) {
	t.Helper()
	readyOnce.Do(func() {
		c := newClient(t)
		deadline := time.Now().Add(4 * time.Minute)
		last := ""
		for time.Now().Before(deadline) {
			last = stackProblem(c)
			if last == "" {
				return
			}
			time.Sleep(2 * time.Second)
		}
		readyErr = "the e2e stack did not become ready in 4m: " + last
	})
	if readyErr != "" {
		t.Fatal(readyErr)
	}
}

// stackProblem returns "" when the stack is ready, else what is not ready. It
// must not t.Fatal on a connection error, so it uses the raw http.Client.
func stackProblem(c *client) string {
	get := func(raw string) (*http.Response, []byte, error) {
		res, err := c.hc.Get(raw)
		if err != nil {
			return nil, nil, err
		}
		defer res.Body.Close()
		b, err := io.ReadAll(io.LimitReader(res.Body, maxBody))
		return res, b, err
	}
	res, b, err := get(issuerURL + "/.well-known/openid-configuration")
	if err != nil {
		return "keycloak discovery: " + err.Error()
	}
	if res.StatusCode != http.StatusOK {
		return "keycloak discovery: " + res.Status
	}
	var disc struct {
		Issuer string `json:"issuer"`
	}
	if json.Unmarshal(b, &disc) != nil || disc.Issuer != issuerURL {
		// A wrong issuer never fixes itself; still reported as not ready so the
		// final message names it.
		return "keycloak issuer is " + disc.Issuer + ", want " + issuerURL
	}
	if res, _, err = get(baseURL + "/readyz"); err != nil {
		return "piperoom /readyz: " + err.Error()
	} else if res.StatusCode != http.StatusOK {
		return "piperoom /readyz: " + res.Status
	}
	res, _, err = get(baseURL + "/auth/login")
	if err != nil {
		return "piperoom /auth/login: " + err.Error()
	}
	if loc := res.Header.Get("Location"); !strings.HasPrefix(loc, issuerURL+"/") {
		return "piperoom /auth/login redirects to " + loc + " (oidc discovery not done yet?)"
	}
	return ""
}

// ---- the login flow ----

// started is a login that went through /auth/login and the Keycloak form and
// now holds the callback URL, not yet called.
type started struct {
	callback   *url.URL
	loginValue string // the __Host-piperoom_login value /auth/login set
}

// startLogin runs GET /auth/login?return=ret and returns the IdP URL and the
// login cookie value, after checking the redirect carries PKCE and a nonce.
func startLogin(t *testing.T, c *client, ret string) (idp *url.URL, loginValue string) {
	t.Helper()
	r := c.get(baseURL + "/auth/login?return=" + url.QueryEscape(ret))
	if r.status != http.StatusFound {
		t.Fatalf("GET /auth/login: status %d, want 302; body %s", r.status, r.body)
	}
	loc := r.location()
	if !strings.HasPrefix(loc, issuerURL+"/protocol/openid-connect/auth?") {
		t.Fatalf("GET /auth/login: Location %q, want the Keycloak authorization endpoint", loc)
	}
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("parse Location: %v", err)
	}
	q := u.Query()
	for k, want := range map[string]string{
		"client_id":             "piperoom",
		"redirect_uri":          baseURL + "/auth/callback",
		"response_type":         "code",
		"code_challenge_method": "S256",
	} {
		if q.Get(k) != want {
			t.Errorf("authorization request %s=%q, want %q", k, q.Get(k), want)
		}
	}
	for _, k := range []string{"state", "nonce", "code_challenge"} {
		if q.Get(k) == "" {
			t.Errorf("authorization request has no %s", k)
		}
	}
	lc := r.setCookie(loginCookie)
	if lc == nil || lc.Value == "" {
		t.Fatalf("GET /auth/login set no %s cookie", loginCookie)
	}
	if !lc.Secure || !lc.HttpOnly || lc.SameSite != http.SameSiteLaxMode || lc.Path != "/" || lc.Domain != "" {
		t.Errorf("login cookie attributes: %+v, want Secure HttpOnly SameSite=Lax Path=/ no Domain", lc)
	}
	return u, lc.Value
}

var formAction = regexp.MustCompile(`(?is)<form[^>]*\sid="kc-form-login"[^>]*\saction="([^"]+)"|<form[^>]*\saction="([^"]+)"[^>]*\sid="kc-form-login"`)

// keycloakLogin loads the Keycloak login page, posts the credentials to the
// form's action and returns the redirect back to /auth/callback.
func keycloakLogin(t *testing.T, c *client, idp *url.URL, user, password string) *url.URL {
	t.Helper()
	page := c.get(idp.String())
	if page.status != http.StatusOK {
		t.Fatalf("GET Keycloak authorization endpoint: status %d, want 200 (login page)", page.status)
	}
	m := formAction.FindSubmatch(page.body)
	if m == nil {
		t.Fatalf("no kc-form-login form in the Keycloak page (%d bytes)", len(page.body))
	}
	action := html.UnescapeString(string(m[1]) + string(m[2]))
	if !strings.HasPrefix(action, issuerURL+"/") {
		t.Fatalf("login form action %q is not on the issuer %s", action, issuerURL)
	}
	form := url.Values{"username": {user}, "password": {password}, "credentialId": {""}}
	req, err := http.NewRequest(http.MethodPost, action, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("new form request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r := c.do(req)
	if r.status != http.StatusFound {
		t.Fatalf("POST Keycloak login form as %s: status %d, want 302 (wrong password or a required action?)", user, r.status)
	}
	cb, err := url.Parse(r.location())
	if err != nil || !strings.HasPrefix(r.location(), baseURL+"/auth/callback?") {
		t.Fatalf("Keycloak redirected to %q, want %s/auth/callback?...", r.location(), baseURL)
	}
	if cb.Query().Get("code") == "" || cb.Query().Get("state") == "" {
		t.Fatalf("callback URL has no code or state: %s", cb.Redacted())
	}
	return cb
}

func login(t *testing.T, c *client, user, password, ret string) started {
	t.Helper()
	waitStack(t)
	idp, lv := startLogin(t, c, ret)
	return started{callback: keycloakLogin(t, c, idp, user, password), loginValue: lv}
}

// wantLoginError checks a 302 to /?login_error=<code> and no session cookie.
func wantLoginError(t *testing.T, r resp, code string) {
	t.Helper()
	want := "/?login_error=" + code
	if r.status != http.StatusFound || r.location() != want {
		t.Fatalf("got %d Location %q, want 302 Location %q", r.status, r.location(), want)
	}
	if sc := r.setCookie(sessionCookie); sc != nil && sc.Value != "" {
		t.Fatalf("a failed login set a session cookie")
	}
}

type me struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Layers        []string `json:"layers"`
	Role          string   `json:"role"`
	GroupsMissing bool     `json:"groups_missing"`
}

func getMe(t *testing.T, c *client) me {
	t.Helper()
	r := c.get(baseURL + "/api/me")
	if r.status != http.StatusOK {
		t.Fatalf("GET /api/me: %d %s", r.status, r.body)
	}
	var m me
	if err := json.Unmarshal(r.body, &m); err != nil {
		t.Fatalf("GET /api/me: decode: %v", err)
	}
	return m
}

// ---- the checks ----

// TestAliceMemberWithGroupLayers: alice has no users.yaml entry; she is a
// member through owners.yaml group:dataeng-oncall and may approve the two
// layers that group owns. Then the spent callback is replayed, and logout
// revokes the session on the server.
func TestAliceMemberWithGroupLayers(t *testing.T) {
	c := newClient(t)
	s := login(t, c, "alice", "alice-e2e-password", "/rooms/r_e2e?tab=log")

	r := c.get(s.callback.String())
	if r.status != http.StatusFound || r.location() != "/rooms/r_e2e?tab=log" {
		t.Fatalf("GET /auth/callback: %d Location %q, want 302 to the return path /rooms/r_e2e?tab=log", r.status, r.location())
	}
	sc := r.setCookie(sessionCookie)
	if sc == nil || sc.Value == "" {
		t.Fatalf("GET /auth/callback set no %s cookie", sessionCookie)
	}
	if !sc.Secure || !sc.HttpOnly || sc.SameSite != http.SameSiteLaxMode || sc.Path != "/" || sc.Domain != "" {
		t.Errorf("session cookie attributes: %+v, want Secure HttpOnly SameSite=Lax Path=/ no Domain", sc)
	}
	if lc := r.setCookie(loginCookie); lc == nil || lc.MaxAge >= 0 {
		t.Errorf("the callback did not clear the login cookie: %+v", lc)
	}

	m := getMe(t, c)
	if m.Role != "member" {
		t.Errorf("alice role %q, want member", m.Role)
	}
	if !strings.HasPrefix(m.ID, "u_") || len(m.ID) != len("u_")+12 {
		t.Errorf("alice id %q, want u_ + 12 hex (no users.yaml entry)", m.ID)
	}
	if want := []string{"kafka-connect", "kafka"}; !slices.Equal(m.Layers, want) {
		t.Errorf("alice layers %v, want %v (from group:dataeng-oncall)", m.Layers, want)
	}
	if m.GroupsMissing {
		t.Errorf("alice groups_missing = true; the Keycloak groups mapper should put groups in the ID token")
	}
	if m.Name != "Alice Oncall" {
		t.Errorf("alice name %q, want the name claim %q", m.Name, "Alice Oncall")
	}
	if r := c.get(baseURL + "/api/people"); r.status != http.StatusOK {
		t.Errorf("GET /api/people as a member: %d %s, want 200", r.status, r.body)
	}
	t.Logf("alice: id=%s role=%s layers=%v", m.ID, m.Role, m.Layers)

	t.Run("callback replay in the same browser", func(t *testing.T) {
		// The login cookie was cleared by the first callback.
		wantLoginError(t, c.get(s.callback.String()), "login_expired")
	})
	t.Run("callback replay with the stolen login cookie", func(t *testing.T) {
		// Code, state and the login cookie all replayed: the login_states row
		// (state, nonce, PKCE verifier) was deleted on first use.
		wantLoginError(t, withCookie(t, s.callback.String(), loginCookie, s.loginValue), "login_expired")
	})

	t.Run("logout revokes the session", func(t *testing.T) {
		token := sc.Value
		if r := c.postJSON(baseURL+"/auth/logout", "{}"); r.status != http.StatusNoContent {
			t.Fatalf("POST /auth/logout: %d %s, want 204", r.status, r.body)
		}
		if r := c.get(baseURL + "/api/me"); r.status != http.StatusUnauthorized || r.errorCode() != "session_expired" {
			t.Errorf("GET /api/me after logout: %d %s, want 401 session_expired", r.status, r.body)
		}
		// The old token, sent by hand: revoked in the sessions table, not only
		// dropped from this browser's jar.
		r := withCookie(t, baseURL+"/api/me", sessionCookie, token)
		if r.status != http.StatusUnauthorized || r.errorCode() != "session_expired" {
			t.Errorf("GET /api/me with the logged-out token: %d %s, want 401 session_expired", r.status, r.body)
		}
		if !strings.Contains(string(r.body), `"auth_mode":"oidc"`) {
			t.Errorf("401 body %s does not carry auth_mode oidc", r.body)
		}
	})
}

// TestBobNotProvisioned: bob authenticates at Keycloak but is in no users.yaml,
// owners.yaml or viewer group, so the login gate refuses him.
func TestBobNotProvisioned(t *testing.T) {
	c := newClient(t)
	s := login(t, c, "bob", "bob-e2e-password", "/")
	wantLoginError(t, c.get(s.callback.String()), "not_provisioned")
	if r := c.get(baseURL + "/api/me"); r.status != http.StatusUnauthorized || r.errorCode() != "session_expired" {
		t.Errorf("GET /api/me after a refused login: %d %s, want 401 session_expired", r.status, r.body)
	}
}

// TestCarolViewer: carol gets in through PIPEROOM_AUTH_VIEWER_GROUPS only, so
// she may read but every non-GET under /api answers 403 viewer.
func TestCarolViewer(t *testing.T) {
	c := newClient(t)
	s := login(t, c, "carol", "carol-e2e-password", "/")
	if r := c.get(s.callback.String()); r.status != http.StatusFound || r.location() != "/" {
		t.Fatalf("GET /auth/callback: %d Location %q, want 302 /", r.status, r.location())
	}
	m := getMe(t, c)
	if m.Role != "viewer" {
		t.Errorf("carol role %q, want viewer", m.Role)
	}
	if len(m.Layers) != 0 {
		t.Errorf("carol layers %v, want none", m.Layers)
	}
	if r := c.get(baseURL + "/api/rooms"); r.status != http.StatusOK {
		t.Errorf("GET /api/rooms as a viewer: %d %s, want 200", r.status, r.body)
	}
	r := c.postJSON(baseURL+"/api/rooms", `{"title":"viewer tries","severity":"sev3","pipeline_id":"orders"}`)
	if r.status != http.StatusForbidden || r.errorCode() != "viewer" {
		t.Errorf("POST /api/rooms as a viewer: %d %s, want 403 viewer", r.status, r.body)
	}
	if r := c.get(baseURL + "/api/people"); r.status != http.StatusForbidden || r.errorCode() != "viewer" {
		t.Errorf("GET /api/people as a viewer: %d %s, want 403 viewer", r.status, r.body)
	}
	t.Logf("carol: id=%s role=%s", m.ID, m.Role)
}

// TestBadStateAndCrossLogin covers the failure redirects of /auth/callback.
func TestBadStateAndCrossLogin(t *testing.T) {
	t.Run("no login cookie", func(t *testing.T) {
		waitStack(t)
		wantLoginError(t, newClient(t).get(baseURL+"/auth/callback?code=x&state=y"), "login_expired")
	})

	t.Run("tampered state", func(t *testing.T) {
		c := newClient(t)
		s := login(t, c, "alice", "alice-e2e-password", "/")
		bad := *s.callback
		q := bad.Query()
		q.Set("state", "tampered-"+q.Get("state"))
		bad.RawQuery = q.Encode()
		wantLoginError(t, c.get(bad.String()), "unauthenticated")
		// The failed attempt spent the row: the genuine callback, even with the
		// login cookie put back, no longer works.
		wantLoginError(t, withCookie(t, s.callback.String(), loginCookie, s.loginValue), "login_expired")
	})

	t.Run("code of an older login with the state of a newer one", func(t *testing.T) {
		// Login 1 is started, then login 2 replaces its row (one live row per
		// login cookie). The code Keycloak issued for login 1 is sent with
		// login 2's state: the state matches, but the code is bound to login 1's
		// PKCE challenge and nonce, so the exchange is refused.
		c := newClient(t)
		waitStack(t)
		idp1, lv1 := startLogin(t, c, "/")
		idp2, _ := startLogin(t, c, "/")
		cb1 := keycloakLogin(t, c, idp1, "alice", "alice-e2e-password")
		mixed := *cb1
		q := mixed.Query()
		q.Set("state", idp2.Query().Get("state"))
		mixed.RawQuery = q.Encode()
		wantLoginError(t, c.get(mixed.String()), "unauthenticated")
		// Login 1's row was replaced by login 2: its own callback is expired.
		wantLoginError(t, withCookie(t, cb1.String(), loginCookie, lv1), "login_expired")
	})
}
