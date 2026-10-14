package httpapi_test

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// A Slack room link (<public URL>/rooms/<id>) opened while signed out goes
// through the IdP and lands on that room in the web app.
func TestDeepLinkThroughOIDCLogin(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	const link = "/rooms/r_01JABCDEF"

	// Signed out: to /auth/login with the room as the return path.
	resp := e.do(t, http.MethodGet, link, "", "Accept: text/html")
	wantStatus(t, resp, http.StatusFound)
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	if loc.Path != "/auth/login" || loc.Query().Get("return") != link || loc.Host != "" {
		t.Fatalf("signed-out deep link went to %q", loc)
	}

	// The login the browser runs next returns to the link...
	out := e.login(t, idpPriya, loc.Query().Get("return"))
	if out.session == nil {
		t.Fatalf("login failed: %d", out.callback.StatusCode)
	}
	if got := out.callback.Header.Get("Location"); got != link {
		t.Fatalf("callback went to %q, want %q", got, link)
	}
	// ...which, now signed in, opens the room in the hash-routed app.
	resp = e.do(t, http.MethodGet, link, "", ck(out.session))
	wantStatus(t, resp, http.StatusFound)
	if got := resp.Header.Get("Location"); got != "/#"+link {
		t.Fatalf("signed-in deep link went to %q", got)
	}
}

func TestDeepLinkDevModeAndBadIDs(t *testing.T) {
	e := newAuthEnv(t)
	// Dev mode: the in-app picker is the login and keeps the hash.
	resp := e.do(t, http.MethodGet, "/rooms/r_1", "")
	wantStatus(t, resp, http.StatusFound)
	if got := resp.Header.Get("Location"); got != "/#/rooms/r_1" {
		t.Fatalf("dev deep link went to %q", got)
	}
	// Anything that is not a plain id is not a room link: no redirect at all.
	for _, p := range []string{
		"/rooms/%2F%2Fevil.com",
		"/rooms/a%5Cb",
		"/rooms/" + strings.Repeat("a", 65),
		"/rooms/a.b",
		"/rooms/a@evil.com",
	} {
		resp := e.do(t, http.MethodGet, p, "")
		if resp.StatusCode == http.StatusFound {
			t.Errorf("GET %s redirected to %q", p, resp.Header.Get("Location"))
		}
	}
}

// The login route itself refuses encoded and backslash return targets: the
// login row stores "/" and the callback lands on the home page.
func TestOIDCReturnRejectsEncodedVariants(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	for _, ret := range []string{
		"/%252F%252Fevil.com",
		"/%255Cevil.com",
		`/rooms\..\..\\evil.com`,
		"/rooms/%5C%5Cevil.com",
	} {
		out := e.login(t, idpPriya, ret)
		if got := out.callback.Header.Get("Location"); got != "/" {
			t.Errorf("return=%q: callback went to %q, want /", ret, got)
		}
	}
}

func TestBadHostFriendlyPage(t *testing.T) {
	e := newAuthEnv(t)
	// net/http refuses '<', '>' and '"' in a Host before any handler runs;
	// '\'' and '&' get through and are what the page must escape (the unit
	// test in deeplink_internal_test.go covers the rest).
	const bad = `rebind.example&x='y`

	// A browser page load gets HTML with the Host escaped and no script.
	resp := e.do(t, http.MethodGet, "/rooms/r_1", "", "Host: "+bad, "Accept: text/html,application/xhtml+xml;q=0.9,*/*;q=0.8")
	wantStatus(t, resp, http.StatusMisdirectedRequest)
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("Content-Type %q", ct)
	}
	if csp := resp.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") || strings.Contains(csp, "script-src") {
		t.Fatalf("CSP %q", csp)
	}
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "&x='y") {
		t.Fatalf("Host echoed unescaped:\n%s", body)
	}
	if !strings.Contains(string(body), "rebind.example&amp;x=&#39;y") {
		t.Fatalf("escaped Host missing:\n%s", body)
	}
	if !strings.Contains(string(body), "PIPEROOM_ALLOWED_HOSTS") {
		t.Fatalf("page does not say what to set")
	}

	// API clients, and any /api or /auth path, keep the JSON error.
	for _, rq := range []struct{ path, accept string }{
		{"/api/me", "text/html"},
		{"/auth/login", "text/html"},
		{"/rooms/r_1", "application/json"},
		{"/rooms/r_1", ""},
	} {
		hdr := []string{"Host: " + bad}
		if rq.accept != "" {
			hdr = append(hdr, "Accept: "+rq.accept)
		}
		resp := e.do(t, http.MethodGet, rq.path, "", hdr...)
		wantStatus(t, resp, http.StatusMisdirectedRequest)
		if b := readErr(t, resp); b.Error.Code != "bad_host" || strings.Contains(b.Error.Message, "rebind") {
			t.Fatalf("%s (Accept %q): %+v", rq.path, rq.accept, b)
		}
	}
	// A POST is never a page load, whatever it accepts.
	resp = e.do(t, http.MethodPost, "/api/rooms", `{}`, "Host: "+bad, "Accept: text/html")
	wantStatus(t, resp, http.StatusMisdirectedRequest)
	if b := readErr(t, resp); b.Error.Code != "bad_host" {
		t.Fatalf("POST: %+v", b)
	}
}
