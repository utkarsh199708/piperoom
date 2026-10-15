package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// authEnv is a server plus the pieces the auth tests poke at.
type authEnv struct {
	srv      *httptest.Server
	svc      *room.Service
	sessions *auth.MemStore
	api      *httpapi.Server
}

func newAuthEnv(t *testing.T, opts ...httpapi.Option) *authEnv {
	t.Helper()
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st,
		Users: room.DefaultUsers(),
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
	})
	sessions := auth.NewMemStore()
	opts = append([]httpapi.Option{httpapi.WithSessions(sessions)}, opts...)
	api := httpapi.New(svc, st, nil, opts...)
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})
	return &authEnv{srv: srv, svc: svc, sessions: sessions, api: api}
}

// do sends a request. hdr is a list of "Name: value" strings; "Host: x" sets
// the Host. The body, when not empty, goes out as application/json unless a
// Content-Type header is given.
func (e *authEnv) do(t *testing.T, method, path, body string, hdr ...string) *http.Response {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, h := range hdr {
		k, v, _ := strings.Cut(h, ": ")
		if strings.EqualFold(k, "Host") {
			req.Host = v
			continue
		}
		req.Header.Set(k, v)
	}
	resp, err := noRedirectClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// noRedirectClient does not follow redirects, so a test sees the 302 itself.
var noRedirectClient = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

type errBody struct {
	Error struct {
		Code     string `json:"code"`
		Message  string `json:"message"`
		AuthMode string `json:"auth_mode"`
	} `json:"error"`
}

func readErr(t *testing.T, resp *http.Response) errBody {
	t.Helper()
	var b errBody
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		t.Fatalf("decode error body (status %d): %v", resp.StatusCode, err)
	}
	return b
}

// devLogin signs user in and returns the session cookie.
func (e *authEnv) devLogin(t *testing.T, user string) *http.Cookie {
	t.Helper()
	resp := e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"`+user+`"}`)
	wantStatus(t, resp, http.StatusOK)
	for _, c := range resp.Cookies() {
		if c.Name == auth.DevSessionCookie {
			return c
		}
	}
	t.Fatalf("dev-login set no %s cookie", auth.DevSessionCookie)
	return nil
}

func cookieHdr(c *http.Cookie) string { return "Cookie: " + c.Name + "=" + c.Value }

// seedSession stores a session directly and returns its cookie value.
func (e *authEnv) seedSession(t *testing.T, user string, created, lastSeen, expires time.Time) (token, id string) {
	t.Helper()
	token, hash, err := auth.NewToken()
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	id = room.NewID("ses_")
	err = e.sessions.Create(context.Background(), auth.Session{
		ID: id, TokenHash: hash, UserID: user, Name: user, Method: auth.MethodDev,
		CreatedAt: created, LastSeenAt: lastSeen, ExpiresAt: expires,
	})
	if err != nil {
		t.Fatalf("seed session: %v", err)
	}
	return token, id
}

func oidcSettings() auth.Settings {
	s := auth.Default()
	s.Mode = auth.ModeOIDC
	return s
}

func TestDevLoginSetsCookieAndMe(t *testing.T) {
	e := newAuthEnv(t)
	resp := e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"priya"}`)
	wantStatus(t, resp, http.StatusOK)
	var c *http.Cookie
	for _, k := range resp.Cookies() {
		if k.Name == "piperoom_session" {
			c = k
		}
	}
	if c == nil {
		t.Fatal("no piperoom_session cookie")
	}
	if !c.HttpOnly || c.SameSite != http.SameSiteLaxMode || c.Path != "/" || c.Secure || c.Domain != "" {
		t.Fatalf("dev cookie attributes wrong: %+v", c)
	}
	// 32 random bytes, base64url: 43 characters.
	if len(c.Value) != 43 {
		t.Fatalf("cookie value length = %d, want 43", len(c.Value))
	}
	// /api/me is {id,name,layers,role,groups_missing,verified}.
	resp = e.do(t, http.MethodGet, "/api/me", "", cookieHdr(c))
	wantStatus(t, resp, http.StatusOK)
	var me map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&me)
	if me["id"] != "priya" || me["name"] == nil || me["layers"] == nil || me["role"] != "member" || me["groups_missing"] != false ||
		me["verified"] != true || len(me) != 6 {
		t.Fatalf("/api/me = %+v, want exactly id, name, layers, role, groups_missing, verified for priya", me)
	}
	// Only the hash is stored.
	hash, _ := auth.HashToken(c.Value)
	sess, err := e.sessions.ByTokenHash(context.Background(), hash)
	if err != nil || sess.UserID != "priya" || sess.Method != auth.MethodDev || !strings.HasPrefix(sess.ID, "ses_") {
		t.Fatalf("stored session = %+v, err %v", sess, err)
	}
	if got := sess.ExpiresAt.Sub(sess.CreatedAt); got != 12*time.Hour {
		t.Fatalf("absolute expiry = %v, want 12h", got)
	}
}

func TestDevLoginUnknownUser(t *testing.T) {
	e := newAuthEnv(t)
	resp := e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"nobody"}`)
	wantStatus(t, resp, http.StatusUnauthorized)
	resp = e.do(t, http.MethodPost, "/auth/dev-login", `{}`)
	wantStatus(t, resp, http.StatusBadRequest)
}

// X-User with no cookie keeps working for curl.
func TestXUserWithoutCookieWorks(t *testing.T) {
	e := newAuthEnv(t)
	resp := e.do(t, http.MethodGet, "/api/me", "", "X-User: arjun")
	wantStatus(t, resp, http.StatusOK)
	resp = e.do(t, http.MethodGet, "/api/me", "")
	wantStatus(t, resp, http.StatusUnauthorized)
	// No cookie and no X-User: the web app shows the dev picker on this code.
	if b := readErr(t, resp); b.Error.Code != "session_expired" || b.Error.AuthMode != "dev" {
		t.Fatalf("no credentials: %+v, want session_expired with auth_mode dev", b.Error)
	}
	// An X-User that names nobody (a curl typo) stays unauthenticated.
	resp = e.do(t, http.MethodGet, "/api/me", "", "X-User: nobody")
	wantStatus(t, resp, http.StatusUnauthorized)
	if b := readErr(t, resp); b.Error.Code != "unauthenticated" || b.Error.AuthMode != "" {
		t.Fatalf("unknown X-User: %+v, want unauthenticated without auth_mode", b.Error)
	}
}

func TestDevLoginWithXUserIsProxyIdentity(t *testing.T) {
	e := newAuthEnv(t)
	for _, v := range []string{"bob", "priya", ""} {
		resp := e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"priya"}`, "X-User: "+v)
		wantStatus(t, resp, http.StatusForbidden)
		b := readErr(t, resp)
		if b.Error.Code != "proxy_identity" || b.Error.Message != "a reverse proxy needs PIPEROOM_AUTH_MODE=oidc or tag pre-auth" {
			t.Fatalf("X-User %q: got %+v", v, b.Error)
		}
		if len(resp.Cookies()) != 0 {
			t.Fatalf("X-User %q: a cookie was set", v)
		}
	}
}

func TestCookieWithOtherXUserIsProxyIdentity(t *testing.T) {
	e := newAuthEnv(t)
	c := e.devLogin(t, "priya")

	resp := e.do(t, http.MethodGet, "/api/me", "", cookieHdr(c), "X-User: arjun")
	wantStatus(t, resp, http.StatusForbidden)
	if b := readErr(t, resp); b.Error.Code != "proxy_identity" {
		t.Fatalf("code = %q, want proxy_identity", b.Error.Code)
	}
	// The same user passes, and the cookie wins.
	resp = e.do(t, http.MethodGet, "/api/me", "", cookieHdr(c), "X-User: priya")
	wantStatus(t, resp, http.StatusOK)
	var me map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&me)
	if me["id"] != "priya" {
		t.Fatalf("me = %+v", me)
	}
	// The WebSocket upgrade counts as a request.
	snap, err := e.svc.CreateRoom(context.Background(), mustUserFrom(t, e.svc, "priya"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	_, wresp, err := websocket.Dial(context.Background(), e.wsURL(snap.ID), &websocket.DialOptions{
		HTTPHeader: http.Header{"Cookie": {c.Name + "=" + c.Value}, "X-User": {"arjun"}},
	})
	if err == nil || wresp == nil || wresp.StatusCode != http.StatusForbidden {
		t.Fatalf("ws cookie + other X-User: want 403, got resp %v err %v", wresp, err)
	}
}

func mustUserFrom(t *testing.T, svc *room.Service, id string) room.User {
	t.Helper()
	u, ok := svc.LookupUser(id)
	if !ok {
		t.Fatalf("no user %s", id)
	}
	return u
}

func (e *authEnv) wsURL(roomID string) string {
	return "ws" + strings.TrimPrefix(e.srv.URL, "http") + "/api/rooms/" + roomID + "/ws?after=0"
}

// X-Forwarded-For alone changes nothing: dev login still works, X-User still
// works, and no credentials still fail.
func TestXForwardedForAloneChangesNothing(t *testing.T) {
	e := newAuthEnv(t)
	xff := "X-Forwarded-For: 203.0.113.7"
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", xff, "X-User: arjun"), http.StatusOK)
	resp := e.do(t, http.MethodGet, "/api/me", "", xff)
	wantStatus(t, resp, http.StatusUnauthorized)
	if b := readErr(t, resp); b.Error.Code != "session_expired" {
		t.Fatalf("code = %q", b.Error.Code)
	}
	wantStatus(t, e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"meera"}`, xff), http.StatusOK)
}

// Dev mode, no cookie, no X-User: 401 session_expired with auth_mode dev, so a
// fresh browser gets the picker. An empty X-User counts as none.
func TestDevNoCredentialsIsSessionExpiredWithAuthMode(t *testing.T) {
	e := newAuthEnv(t)
	for _, hdr := range [][]string{nil, {"X-User: "}} {
		resp := e.do(t, http.MethodGet, "/api/me", "", hdr...)
		wantStatus(t, resp, http.StatusUnauthorized)
		b := readErr(t, resp)
		if b.Error.Code != "session_expired" || b.Error.AuthMode != "dev" {
			t.Fatalf("headers %v: %+v, want session_expired with auth_mode dev", hdr, b.Error)
		}
	}
}

// §5.2(b) with several X-User values: a proxy that APPENDS its own header
// leaves [client value, proxy value]. Reading only the first would let
// [priya, bob] pass as priya.
func TestMultipleXUserValuesAreProxyIdentity(t *testing.T) {
	e := newAuthEnv(t)
	c := e.devLogin(t, "priya")

	two := func(h http.Header, a, b string) { h.Add("X-User", a); h.Add("X-User", b) }
	do := func(withCookie bool, a, b string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, e.srv.URL+"/api/me", nil)
		two(req.Header, a, b)
		if withCookie {
			req.AddCookie(c)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	// Cookie priya + [priya, bob]: 403.
	resp := do(true, "priya", "bob")
	wantStatus(t, resp, http.StatusForbidden)
	if b := readErr(t, resp); b.Error.Code != "proxy_identity" {
		t.Fatalf("code = %q", b.Error.Code)
	}
	// Both orders, the same name twice, and without a cookie: 403.
	wantStatus(t, do(true, "bob", "priya"), http.StatusForbidden)
	wantStatus(t, do(true, "priya", "priya"), http.StatusForbidden)
	wantStatus(t, do(false, "bob", "priya"), http.StatusForbidden)
	wantStatus(t, do(false, "arjun", "arjun"), http.StatusForbidden)
	// One header line folded with a comma counts as several values.
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", cookieHdr(c), "X-User: priya, bob"), http.StatusForbidden)
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "X-User: bob,priya"), http.StatusForbidden)
	// A single value still works: priya with the cookie, and a lone curl user.
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", cookieHdr(c), "X-User: priya"), http.StatusOK)
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "X-User: bob"), http.StatusUnauthorized) // bob is not a user
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "X-User: arjun"), http.StatusOK)
	// One valid and one different single value with the cookie: 403.
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", cookieHdr(c), "X-User: arjun"), http.StatusForbidden)
}

// In oidc mode X-User is ignored, however many values it has.
func TestOIDCIgnoresMultipleXUserValues(t *testing.T) {
	e := newAuthEnv(t, httpapi.WithAuth(oidcSettings()))
	resp := e.do(t, http.MethodGet, "/api/me", "", "X-User: arjun, priya")
	wantStatus(t, resp, http.StatusUnauthorized)
	if b := readErr(t, resp); b.Error.Code != "session_expired" || b.Error.AuthMode != "oidc" {
		t.Fatalf("error = %+v", b.Error)
	}
}

func TestOIDCModeIgnoresXUser(t *testing.T) {
	e := newAuthEnv(t, httpapi.WithAuth(oidcSettings()))
	resp := e.do(t, http.MethodGet, "/api/me", "", "X-User: arjun")
	wantStatus(t, resp, http.StatusUnauthorized)
	b := readErr(t, resp)
	if b.Error.Code != "session_expired" || b.Error.AuthMode != "oidc" {
		t.Fatalf("error = %+v, want session_expired with auth_mode oidc", b.Error)
	}
	// Dev login is not mounted; the OIDC routes redirect to /?login_error=idp_unavailable while discovery has not run.
	wantStatus(t, e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"arjun"}`), http.StatusNotFound)
	wantLoginError(t, e.do(t, http.MethodGet, "/auth/login?return=/", ""), "idp_unavailable")
	wantLoginError(t, e.do(t, http.MethodGet, "/auth/callback", ""), "idp_unavailable")
}

// With the web UI served, an unmounted or unknown /auth path is a JSON 404, not
// the SPA shell: index.html for /auth/login would make the web client loop.
func TestAuthPathsAreNeverTheSPAShell(t *testing.T) {
	dev := newWebServer(t, httpapi.WithWebDir(webDir(t)))
	for _, p := range []string{"/auth/login", "/auth/callback", "/auth/nothing", "/auth/", "/auth", "/AUTH/login", "/auth/../auth/login"} {
		resp, body := fetch(t, http.MethodGet, dev.URL+p)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s in dev mode: status %d, want 404 (body %.60q)", p, resp.StatusCode, body)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" || strings.Contains(body, "<title>Piperoom</title>") || !strings.Contains(body, `"not_found"`) {
			t.Fatalf("GET %s: want the JSON not_found envelope, got %q %.80q", p, ct, body)
		}
	}
	// The SPA itself still loads.
	resp, body := fetch(t, http.MethodGet, dev.URL+"/")
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "<title>Piperoom</title>") {
		t.Fatalf("GET /: %d, want the shell", resp.StatusCode)
	}

	// In oidc mode the two routes exist (503 until discovery succeeds); the rest is still 404.
	oidc := newWebServer(t, httpapi.WithWebDir(webDir(t)), httpapi.WithAuth(oidcSettings()))
	resp, err := noRedirectClient.Get(oidc.URL + "/auth/login")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "/?login_error=idp_unavailable" {
		t.Fatalf("GET /auth/login in oidc mode: %d %q, want 302 to /?login_error=idp_unavailable", resp.StatusCode, resp.Header.Get("Location"))
	}
	resp, body = fetch(t, http.MethodGet, oidc.URL+"/auth/nothing")
	if resp.StatusCode != http.StatusNotFound || !strings.Contains(body, `"not_found"`) {
		t.Fatalf("GET /auth/nothing in oidc mode: %d %.80q", resp.StatusCode, body)
	}
}

func TestOIDCModeReadsOnlyItsOwnCookie(t *testing.T) {
	e := newAuthEnv(t, httpapi.WithAuth(oidcSettings()))
	now := time.Now().UTC()
	token, _ := e.seedSession(t, "arjun", now, now, now.Add(time.Hour))

	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "Cookie: "+auth.OIDCSessionCookie+"="+token), http.StatusOK)
	resp := e.do(t, http.MethodGet, "/api/me", "", "Cookie: "+auth.DevSessionCookie+"="+token)
	wantStatus(t, resp, http.StatusUnauthorized)
	if b := readErr(t, resp); b.Error.Code != "session_expired" {
		t.Fatalf("dev cookie name in oidc mode: code %q", b.Error.Code)
	}
}

func TestDevModeReadsOnlyItsOwnCookie(t *testing.T) {
	e := newAuthEnv(t)
	now := time.Now().UTC()
	token, _ := e.seedSession(t, "arjun", now, now, now.Add(time.Hour))
	resp := e.do(t, http.MethodGet, "/api/me", "", "Cookie: "+auth.OIDCSessionCookie+"="+token)
	wantStatus(t, resp, http.StatusUnauthorized) // no dev cookie, no X-User
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "Cookie: "+auth.DevSessionCookie+"="+token), http.StatusOK)
}

// The webhook route exists only in dev mode (404 in oidc).
func TestWebhookDevOnly(t *testing.T) {
	dev := newAuthEnv(t)
	resp := dev.do(t, http.MethodPost, "/api/alerts/webhook", `{}`, "X-User: arjun")
	if resp.StatusCode == http.StatusNotFound {
		t.Fatal("dev mode: webhook should be mounted")
	}
	oidc := newAuthEnv(t, httpapi.WithAuth(oidcSettings()))
	resp = oidc.do(t, http.MethodPost, "/api/alerts/webhook", `{}`, "X-User: arjun")
	wantStatus(t, resp, http.StatusNotFound)
}

func TestHostAllowList(t *testing.T) {
	pub := func(raw string) httpapi.Option {
		o, err := httpapi.ParsePublicURL(raw)
		if err != nil {
			t.Fatal(err)
		}
		return httpapi.WithPublicOrigin(o)
	}
	allowed := auth.Default()
	allowed.AllowedHosts = []string{"piperoom.internal:8090"}
	devRemote := auth.Default()
	devRemote.DevNonLoopback = true
	oidc := oidcSettings()

	cases := []struct {
		name string
		opts []httpapi.Option
		host string
		want int
	}{
		{"evil host", nil, "evil.test", http.StatusMisdirectedRequest},
		{"evil host with port", nil, "evil.test:8090", http.StatusMisdirectedRequest},
		{"localhost with the vite port", nil, "localhost:5173", http.StatusOK},
		{"127.0.0.1 with a compose port", nil, "127.0.0.1:9000", http.StatusOK},
		{"localhost without a port", nil, "localhost", http.StatusOK},
		{"ipv6 loopback", nil, "[::1]:8090", http.StatusOK},
		{"localhost upper case", nil, "LocalHost:1", http.StatusOK},
		{"localhost with a non-numeric port", nil, "localhost:evil.test", http.StatusMisdirectedRequest},
		{"localhost as a suffix of another name", nil, "evil.localhost", http.StatusMisdirectedRequest},
		{"other loopback address", nil, "127.0.0.2:8090", http.StatusMisdirectedRequest},
		{"allowed host as written", []httpapi.Option{httpapi.WithAuth(allowed)}, "piperoom.internal:8090", http.StatusOK},
		{"allowed host with another port", []httpapi.Option{httpapi.WithAuth(allowed)}, "piperoom.internal:9999", http.StatusMisdirectedRequest},
		{"allowed host without its port", []httpapi.Option{httpapi.WithAuth(allowed)}, "piperoom.internal", http.StatusMisdirectedRequest},
		{"public host in oidc mode", []httpapi.Option{httpapi.WithAuth(oidc), pub("https://piperoom.corp.example")}, "piperoom.corp.example", http.StatusOK},
		{"public host with another port in oidc mode", []httpapi.Option{httpapi.WithAuth(oidc), pub("https://piperoom.corp.example")}, "piperoom.corp.example:8443", http.StatusMisdirectedRequest},
		{"public host in plain dev mode", []httpapi.Option{pub("http://192.168.1.5:8090")}, "192.168.1.5:8090", http.StatusMisdirectedRequest},
		{"public host in dev mode with the switch", []httpapi.Option{httpapi.WithAuth(devRemote), pub("http://192.168.1.5:8090")}, "192.168.1.5:8090", http.StatusOK},
		{"public host with the wrong port in dev mode with the switch", []httpapi.Option{httpapi.WithAuth(devRemote), pub("http://192.168.1.5:8090")}, "192.168.1.5:80", http.StatusMisdirectedRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newAuthEnv(t, tc.opts...)
			// A plain API route follows the rule. (/healthz and /readyz are exempt
			// from it: TestHostGuardHealthProbeExemption.)
			resp := e.do(t, http.MethodGet, "/api/rooms", "", "Host: "+tc.host, "X-User: priya")
			// Past the Host check, dev mode answers 200 (X-User) and oidc mode
			// 401 session_expired (no cookie).
			want := tc.want
			if want == http.StatusOK && strings.Contains(tc.name, "oidc mode") {
				want = http.StatusUnauthorized
			}
			wantStatus(t, resp, want)
			if tc.want == http.StatusMisdirectedRequest {
				if b := readErr(t, resp); b.Error.Code != "bad_host" {
					t.Fatalf("code = %q, want bad_host", b.Error.Code)
				}
			}
		})
	}
}

// The signed alert routes skip the Host check (their signature is the auth, and
// in-cluster senders use internal DNS names): an unsigned request reaches the
// verifier and is 401, not 421. The unsigned dev webhook stays under the check.
func TestSignedAlertRoutesSkipHostCheck(t *testing.T) {
	e := newAuthEnv(t)
	for _, src := range []string{"generic", "alertmanager", "grafana", "pagerduty", "datadog"} {
		resp := e.do(t, http.MethodPost, "/api/alerts/"+src, `{}`, "Host: alertmanager.monitoring.svc")
		wantStatus(t, resp, http.StatusUnauthorized)
		if b := readErr(t, resp); b.Error.Code != "unauthenticated" {
			t.Fatalf("%s: code %q, want unauthenticated from the verifier", src, b.Error.Code)
		}
	}
	// Not exempt: the unsigned webhook, other paths under /api/alerts, other
	// methods, and every other route.
	wantStatus(t, e.do(t, http.MethodPost, "/api/alerts/webhook", `{}`, "Host: alertmanager.monitoring.svc", "X-User: arjun"), http.StatusMisdirectedRequest)
	wantStatus(t, e.do(t, http.MethodPost, "/api/alerts/generic/extra", `{}`, "Host: evil.test"), http.StatusMisdirectedRequest)
	wantStatus(t, e.do(t, http.MethodGet, "/api/alerts/generic", "", "Host: evil.test"), http.StatusMisdirectedRequest)
	wantStatus(t, e.do(t, http.MethodPost, "/api/rooms", `{}`, "Host: alertmanager.monitoring.svc", "X-User: arjun"), http.StatusMisdirectedRequest)
}

func TestCrossOriginPostRefused(t *testing.T) {
	e := newAuthEnv(t)
	body := `{"title":"t","severity":"sev2","pipeline_id":"orders"}`
	resp := e.do(t, http.MethodPost, "/api/rooms", body, "X-User: arjun", "Sec-Fetch-Site: cross-site")
	wantStatus(t, resp, http.StatusForbidden)
	resp = e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"arjun"}`, "Sec-Fetch-Site: cross-site")
	wantStatus(t, resp, http.StatusForbidden)
	// An Origin that is not the Host is refused too (no Sec-Fetch-Site).
	resp = e.do(t, http.MethodPost, "/api/rooms", body, "X-User: arjun", "Origin: https://evil.test")
	wantStatus(t, resp, http.StatusForbidden)
	// Same-origin and header-less (curl) requests pass.
	wantStatus(t, e.do(t, http.MethodPost, "/api/rooms", body, "X-User: arjun", "Sec-Fetch-Site: same-origin"), http.StatusCreated)
	wantStatus(t, e.do(t, http.MethodPost, "/api/rooms", body, "X-User: arjun"), http.StatusCreated)
	// GET is a safe method.
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "X-User: arjun", "Sec-Fetch-Site: cross-site"), http.StatusOK)
}

func TestPostNeedsJSONContentType(t *testing.T) {
	e := newAuthEnv(t)
	body := `{"title":"t","severity":"sev2","pipeline_id":"orders"}`
	for _, ct := range []string{"text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x", ""} {
		resp := e.do(t, http.MethodPost, "/api/rooms", body, "X-User: arjun", "Content-Type: "+ct)
		wantStatus(t, resp, http.StatusBadRequest)
		if b := readErr(t, resp); b.Error.Code != "bad_request" {
			t.Fatalf("Content-Type %q: code %q", ct, b.Error.Code)
		}
	}
	wantStatus(t, e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"arjun"}`, "Content-Type: text/plain"), http.StatusBadRequest)
	wantStatus(t, e.do(t, http.MethodPost, "/api/rooms", body, "X-User: arjun", "Content-Type: application/json; charset=utf-8"), http.StatusCreated)
	// The signed alert routes take the sender's own content type: this one
	// reaches the handler and is refused for its missing secret, not its type.
	resp := e.do(t, http.MethodPost, "/api/alerts/generic", `x=1`, "Content-Type: application/x-www-form-urlencoded")
	wantStatus(t, resp, http.StatusUnauthorized)
}

func TestSessionExpiry(t *testing.T) {
	e := newAuthEnv(t)
	now := time.Now().UTC()
	cases := []struct {
		name                       string
		created, lastSeen, expires time.Time
		want                       int
	}{
		{"live", now, now, now.Add(time.Hour), http.StatusOK},
		{"past the absolute expiry", now.Add(-13 * time.Hour), now, now.Add(-time.Minute), http.StatusUnauthorized},
		{"idle for more than 8h", now.Add(-9 * time.Hour), now.Add(-9 * time.Hour), now.Add(3 * time.Hour), http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token, _ := e.seedSession(t, "arjun", tc.created, tc.lastSeen, tc.expires)
			resp := e.do(t, http.MethodGet, "/api/me", "", "Cookie: piperoom_session="+token)
			wantStatus(t, resp, tc.want)
			if tc.want == http.StatusUnauthorized {
				b := readErr(t, resp)
				if b.Error.Code != "session_expired" || b.Error.AuthMode != "dev" {
					t.Fatalf("error = %+v, want session_expired with auth_mode dev", b.Error)
				}
			}
		})
	}
}

func TestSessionLengthsFromSettings(t *testing.T) {
	s := auth.Default()
	s.Idle = time.Minute
	e := newAuthEnv(t, httpapi.WithAuth(s))
	now := time.Now().UTC()
	token, _ := e.seedSession(t, "arjun", now, now.Add(-2*time.Minute), now.Add(time.Hour))
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "Cookie: piperoom_session="+token), http.StatusUnauthorized)
}

// A garbage or unknown cookie is 401 session_expired, also when X-User is sent.
func TestInvalidCookie(t *testing.T) {
	e := newAuthEnv(t)
	for _, v := range []string{"junk", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"} {
		resp := e.do(t, http.MethodGet, "/api/me", "", "Cookie: piperoom_session="+v, "X-User: arjun")
		wantStatus(t, resp, http.StatusUnauthorized)
		b := readErr(t, resp)
		if b.Error.Code != "session_expired" || b.Error.AuthMode != "dev" {
			t.Fatalf("cookie %q: %+v", v, b.Error)
		}
	}
}

// Only 401 session_expired carries auth_mode.
func TestAuthModeOnlyOnSessionExpired(t *testing.T) {
	e := newAuthEnv(t)
	resp := e.do(t, http.MethodGet, "/api/me", "", "X-User: nobody")
	wantStatus(t, resp, http.StatusUnauthorized)
	raw, _ := io.ReadAll(resp.Body)
	if bytes.Contains(raw, []byte("auth_mode")) {
		t.Fatalf("unauthenticated body carries auth_mode: %s", raw)
	}
}

// A user who left users.yaml is out on the next request, not at the 12 h cap.
func TestUserRemovedFromUsersFile(t *testing.T) {
	e := newAuthEnv(t)
	now := time.Now().UTC()
	// "ghost" is a user the directory no longer has.
	token, _ := e.seedSession(t, "ghost", now, now, now.Add(time.Hour))
	resp := e.do(t, http.MethodGet, "/api/me", "", "Cookie: piperoom_session="+token)
	wantStatus(t, resp, http.StatusUnauthorized)
	if b := readErr(t, resp); b.Error.Code != "session_expired" {
		t.Fatalf("code = %q", b.Error.Code)
	}
}

func TestLastSeenWrittenAtMostOncePerMinute(t *testing.T) {
	e := newAuthEnv(t)
	now := time.Now().UTC()
	recent := now.Add(-20 * time.Second)
	token, id := e.seedSession(t, "arjun", now, recent, now.Add(time.Hour))
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "Cookie: piperoom_session="+token), http.StatusOK)
	got, _ := e.sessions.ByID(context.Background(), id)
	if !got.LastSeenAt.Equal(recent) {
		t.Fatalf("last_seen_at moved within a minute: %v -> %v", recent, got.LastSeenAt)
	}

	old := now.Add(-5 * time.Minute)
	token, id = e.seedSession(t, "arjun", now, old, now.Add(time.Hour))
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "Cookie: piperoom_session="+token), http.StatusOK)
	got, _ = e.sessions.ByID(context.Background(), id)
	if !got.LastSeenAt.After(now.Add(-time.Minute)) {
		t.Fatalf("last_seen_at not refreshed after 5 minutes: %v", got.LastSeenAt)
	}
}

func TestDevLoginRevokesThePreviousSession(t *testing.T) {
	e := newAuthEnv(t)
	first := e.devLogin(t, "priya")
	resp := e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"meera"}`, cookieHdr(first))
	wantStatus(t, resp, http.StatusOK)
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", cookieHdr(first)), http.StatusUnauthorized)
}

func TestLogoutRevokesAndClearsCookie(t *testing.T) {
	e := newAuthEnv(t)
	c := e.devLogin(t, "priya")
	resp := e.do(t, http.MethodPost, "/auth/logout", "", cookieHdr(c))
	wantStatus(t, resp, http.StatusNoContent)
	var cleared *http.Cookie
	for _, k := range resp.Cookies() {
		if k.Name == c.Name {
			cleared = k
		}
	}
	if cleared == nil || cleared.MaxAge >= 0 || cleared.Value != "" || !cleared.HttpOnly {
		t.Fatalf("logout did not clear the cookie: %+v", cleared)
	}
	hash, _ := auth.HashToken(c.Value)
	sess, err := e.sessions.ByTokenHash(context.Background(), hash)
	if err != nil || sess.RevokedAt.IsZero() {
		t.Fatalf("session after logout = %+v, err %v; want revoked", sess, err)
	}
	// The old cookie is dead.
	resp = e.do(t, http.MethodGet, "/api/me", "", cookieHdr(c))
	wantStatus(t, resp, http.StatusUnauthorized)
	if b := readErr(t, resp); b.Error.Code != "session_expired" || b.Error.AuthMode != "dev" {
		t.Fatalf("after logout: %+v", b.Error)
	}
	// Logging out again, or with no cookie, is still 204.
	wantStatus(t, e.do(t, http.MethodPost, "/auth/logout", "", cookieHdr(c)), http.StatusNoContent)
	wantStatus(t, e.do(t, http.MethodPost, "/auth/logout", ""), http.StatusNoContent)
}

func TestWebSocketNeedsASession(t *testing.T) {
	e := newAuthEnv(t)
	snap, err := e.svc.CreateRoom(context.Background(), mustUserFrom(t, e.svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	dial := func(hdr http.Header, url string) (*http.Response, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: hdr})
		if err == nil {
			conn.Close(websocket.StatusNormalClosure, "")
		}
		return resp, err
	}

	// No cookie, no X-User: 401. The old ?user= no longer works.
	resp, err := dial(nil, e.wsURL(snap.ID))
	if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("ws with no cookie: want 401, got %v / %v", resp, err)
	}
	resp, err = dial(nil, e.wsURL(snap.ID)+"&user=arjun")
	if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("ws with ?user=: want 401, got %v / %v", resp, err)
	}
	// An expired cookie: 401.
	now := time.Now().UTC()
	old, _ := e.seedSession(t, "arjun", now.Add(-13*time.Hour), now, now.Add(-time.Hour))
	resp, err = dial(http.Header{"Cookie": {"piperoom_session=" + old}}, e.wsURL(snap.ID))
	if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("ws with expired cookie: want 401, got %v / %v", resp, err)
	}
	// A live cookie, and X-User in dev mode (wscat), upgrade.
	c := e.devLogin(t, "arjun")
	if _, err := dial(http.Header{"Cookie": {c.Name + "=" + c.Value}}, e.wsURL(snap.ID)); err != nil {
		t.Fatalf("ws with cookie: %v", err)
	}
	if _, err := dial(http.Header{"X-User": {"arjun"}}, e.wsURL(snap.ID)); err != nil {
		t.Fatalf("ws with X-User in dev mode: %v", err)
	}
}

func TestWebSocketRefusedInOIDCWithXUser(t *testing.T) {
	e := newAuthEnv(t, httpapi.WithAuth(oidcSettings()))
	snap, err := e.svc.CreateRoom(context.Background(), mustUserFrom(t, e.svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	_, resp, err := websocket.Dial(context.Background(), e.wsURL(snap.ID), &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {"arjun"}}})
	if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("oidc ws with X-User: want 401, got %v / %v", resp, err)
	}
}

// readUntilClosed reads frames until the connection closes. It returns every
// error frame it saw and the close error.
func readUntilClosed(t *testing.T, conn *websocket.Conn) (errFrames []map[string]any, closeErr error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return errFrames, err
		}
		var m map[string]any
		if json.Unmarshal(data, &m) == nil && m["type"] == "error" {
			errFrames = append(errFrames, m)
		}
	}
}

// Logout closes the session's WebSockets: an error frame with code
// session_expired, then a close whose reason is "session_expired". Another
// user's connection stays open.
func TestLogoutClosesWebSocket(t *testing.T) {
	e := newAuthEnv(t)
	snap, err := e.svc.CreateRoom(context.Background(), mustUserFrom(t, e.svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	priya := e.devLogin(t, "priya")
	arjun := e.devLogin(t, "arjun")
	dial := func(c *http.Cookie) *websocket.Conn {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, e.wsURL(snap.ID), &websocket.DialOptions{HTTPHeader: http.Header{"Cookie": {c.Name + "=" + c.Value}}})
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { conn.CloseNow() })
		// Dial returns before the gateway registers the connection, and a logout
		// that ran first would find nothing to close. The gateway sends a
		// presence frame only after it has registered the connection, so reading
		// up to that frame makes the logout below race-free.
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				t.Fatalf("waiting for the presence frame: %v", err)
			}
			var m struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(data, &m) == nil && m.Type == "presence" {
				break
			}
		}
		return conn
	}
	pConn := dial(priya)
	aConn := dial(arjun)

	wantStatus(t, e.do(t, http.MethodPost, "/auth/logout", "", cookieHdr(priya)), http.StatusNoContent)

	frames, closeErr := readUntilClosed(t, pConn)
	if got := websocket.CloseStatus(closeErr); got != websocket.StatusPolicyViolation {
		t.Fatalf("close status = %v (err %v), want policy violation", got, closeErr)
	}
	if !strings.Contains(closeErr.Error(), "session_expired") {
		t.Fatalf("close reason missing: %v", closeErr)
	}
	if len(frames) != 1 {
		t.Fatalf("error frames = %v, want exactly one", frames)
	}
	if e0, _ := frames[0]["error"].(map[string]any); e0["code"] != "session_expired" {
		t.Fatalf("error frame = %v, want code session_expired", frames[0])
	}

	// Arjun's connection is still up: it is sent the member list without
	// priya, which the gateway broadcasts only once her connection has left.
	// Receiving it proves the connection is open without a read deadline
	// standing in for "nothing happened".
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, data, err := aConn.Read(ctx)
		if err != nil {
			t.Fatalf("other user's connection closed or got no presence update: %v", err)
		}
		var m struct {
			Type    string `json:"type"`
			Members []struct {
				ID string `json:"id"`
			} `json:"members"`
		}
		if json.Unmarshal(data, &m) != nil || m.Type != "presence" {
			continue
		}
		gone := true
		for _, x := range m.Members {
			if x.ID == "priya" {
				gone = false
			}
		}
		if gone {
			break
		}
	}
}
