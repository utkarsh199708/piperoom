package httpapi_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

const (
	indexHTML = "<!doctype html><title>Piperoom</title><div id=root></div>"
	assetJS   = "console.log('hashed');"
)

// webDir writes a tiny built UI: index.html, a hashed asset and a favicon.
func webDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "assets"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"index.html":           indexHTML,
		"assets/index-abc.js":  assetJS,
		"vite.svg":             "<svg xmlns='http://www.w3.org/2000/svg'/>",
		"assets/other-123.css": "body{}",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// withPublic is WithPublicOrigin(ParsePublicURL(raw)) for a URL that must be valid.
func withPublic(t *testing.T, raw string) httpapi.Option {
	t.Helper()
	o, err := httpapi.ParsePublicURL(raw)
	if err != nil {
		t.Fatalf("ParsePublicURL(%q): %v", raw, err)
	}
	return httpapi.WithPublicOrigin(o)
}

func newWebHandler(t *testing.T, opts ...httpapi.Option) *httpapi.Server {
	t.Helper()
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	h := httpapi.New(svc, st, nil, opts...)
	t.Cleanup(h.Close)
	return h
}

func newWebServer(t *testing.T, opts ...httpapi.Option) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(newWebHandler(t, opts...))
	t.Cleanup(srv.Close)
	return srv
}

func fetch(t *testing.T, method, url string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(b)
}

func TestStaticServesIndexAtRoot(t *testing.T) {
	srv := newWebServer(t, httpapi.WithWebDir(webDir(t)))
	resp, body := fetch(t, http.MethodGet, srv.URL+"/")
	if resp.StatusCode != http.StatusOK || body != indexHTML {
		t.Fatalf("GET / = %d %q", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("index.html Cache-Control = %q, want no-cache", got)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("index.html Content-Type = %q", ct)
	}
}

func TestStaticAssetIsImmutableAndTyped(t *testing.T) {
	srv := newWebServer(t, httpapi.WithWebDir(webDir(t)))
	resp, body := fetch(t, http.MethodGet, srv.URL+"/assets/index-abc.js")
	if resp.StatusCode != http.StatusOK || body != assetJS {
		t.Fatalf("GET asset = %d %q", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Cache-Control"); !strings.Contains(got, "immutable") || !strings.Contains(got, "max-age=31536000") {
		t.Fatalf("asset Cache-Control = %q", got)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "javascript") {
		t.Fatalf("asset Content-Type = %q", ct)
	}
	// A root-level file is revalidated, not cached as immutable.
	resp, _ = fetch(t, http.MethodGet, srv.URL+"/vite.svg")
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Cache-Control") != "no-cache" {
		t.Fatalf("GET /vite.svg = %d, Cache-Control %q", resp.StatusCode, resp.Header.Get("Cache-Control"))
	}
}

func TestStaticSPAFallback(t *testing.T) {
	srv := newWebServer(t, httpapi.WithWebDir(webDir(t)))
	for _, p := range []string{"/rooms/abc123", "/rooms/abc123/", "/some/deep/route", "/assets"} {
		resp, body := fetch(t, http.MethodGet, srv.URL+p)
		if resp.StatusCode != http.StatusOK || body != indexHTML {
			t.Fatalf("GET %s = %d %q, want index.html", p, resp.StatusCode, body)
		}
		if resp.Header.Get("Cache-Control") != "no-cache" {
			t.Fatalf("GET %s Cache-Control = %q", p, resp.Header.Get("Cache-Control"))
		}
	}
	// HEAD works too, with no body.
	resp, body := fetch(t, http.MethodHead, srv.URL+"/rooms/abc123")
	if resp.StatusCode != http.StatusOK || body != "" {
		t.Fatalf("HEAD = %d %q", resp.StatusCode, body)
	}
}

func TestStaticMissingAssetIs404NotHTML(t *testing.T) {
	srv := newWebServer(t, httpapi.WithWebDir(webDir(t)))
	resp, body := fetch(t, http.MethodGet, srv.URL+"/assets/gone-999.js")
	if resp.StatusCode != http.StatusNotFound || strings.Contains(body, "Piperoom") {
		t.Fatalf("missing asset = %d %q, want 404 without the app shell", resp.StatusCode, body)
	}
}

func TestStaticDoesNotShadowAPI(t *testing.T) {
	srv := newWebServer(t, httpapi.WithWebDir(webDir(t)))

	// A real API route still answers (needs X-User: 401 JSON, not HTML).
	resp, body := fetch(t, http.MethodGet, srv.URL+"/api/me")
	if resp.StatusCode != http.StatusUnauthorized || !strings.Contains(body, `"session_expired"`) {
		t.Fatalf("GET /api/me = %d %q", resp.StatusCode, body)
	}
	// Probes are not shadowed.
	resp, body = fetch(t, http.MethodGet, srv.URL+"/healthz")
	if resp.StatusCode != http.StatusOK || body == indexHTML || !strings.Contains(body, `"ok"`) {
		t.Fatalf("GET /healthz = %d %q", resp.StatusCode, body)
	}
	// Unknown API paths keep the JSON 404 and never fall back to index.html.
	for _, p := range []string{"/api/nope", "/api/rooms/x/nope", "/api", "/api/"} {
		resp, body = fetch(t, http.MethodGet, srv.URL+p)
		if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET %s = %d %q, want a JSON error", p, resp.StatusCode, body)
		}
		if strings.Contains(body, "Piperoom") {
			t.Fatalf("GET %s returned the app shell: %q", p, body)
		}
	}
	// A POST to a path the SPA owns is refused, not served.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/rooms/abc", strings.NewReader("{}"))
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /rooms/abc = %d, want 405", r.StatusCode)
	}
}

func TestStaticPathTraversalStaysInsideDir(t *testing.T) {
	dir := webDir(t)
	secret := filepath.Join(filepath.Dir(dir), "secret.txt")
	if err := os.WriteFile(secret, []byte("TOPSECRET"), 0o644); err != nil {
		t.Fatal(err)
	}
	srv := newWebServer(t, httpapi.WithWebDir(dir))
	for _, p := range []string{"/../secret.txt", "/%2e%2e/secret.txt", "/assets/../../secret.txt", "/..%2fsecret.txt"} {
		_, body := fetch(t, http.MethodGet, srv.URL+p)
		if strings.Contains(body, "TOPSECRET") {
			t.Fatalf("GET %s leaked a file outside the web dir", p)
		}
	}
}

func TestStaticSecurityHeaders(t *testing.T) {
	srv := newWebServer(t, httpapi.WithWebDir(webDir(t)), withPublic(t, "https://piperoom.corp.example:8443/base"))
	host := strings.TrimPrefix(srv.URL, "http://")
	for _, p := range []string{"/", "/assets/index-abc.js", "/api/me", "/healthz"} {
		resp, _ := fetch(t, http.MethodGet, srv.URL+p)
		h := resp.Header
		if h.Get("X-Content-Type-Options") != "nosniff" || h.Get("Referrer-Policy") != "no-referrer" || h.Get("X-Frame-Options") != "DENY" {
			t.Fatalf("GET %s: missing hardening headers: %v", p, h)
		}
		csp := h.Get("Content-Security-Policy")
		for _, want := range []string{
			"default-src 'self'",
			"frame-ancestors 'none'",
			"object-src 'none'",
			"connect-src 'self' ws://" + host + " wss://" + host,
			"wss://piperoom.corp.example:8443",
		} {
			if !strings.Contains(csp, want) {
				t.Fatalf("GET %s: CSP lacks %q: %s", p, want, csp)
			}
		}
		if strings.Contains(csp, "unsafe-eval") || strings.Contains(csp, "script-src 'self' 'unsafe-inline'") {
			t.Fatalf("GET %s: CSP is too loose: %s", p, csp)
		}
	}
}

func TestStaticCSPIgnoresHostileHostHeader(t *testing.T) {
	// Call the handler directly: an HTTP client would refuse to send this Host.
	h := newWebHandler(t, httpapi.WithWebDir(webDir(t)))
	for _, host := range []string{"evil.example; script-src *", "evil.example script-src *", "a.example,b.example", "x'y"} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Host = host
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		csp := rec.Header().Get("Content-Security-Policy")
		if csp == "" {
			t.Fatalf("Host %q: no CSP at all", host)
		}
		if strings.Contains(csp, "evil.example") || strings.Contains(csp, "a.example") || strings.Contains(csp, "x'y") {
			t.Fatalf("Host %q reached the CSP: %s", host, csp)
		}
		if strings.Contains(csp, "script-src *") {
			t.Fatalf("Host %q injected a directive: %s", host, csp)
		}
		if !strings.Contains(csp, "connect-src 'self';") {
			t.Fatalf("Host %q: connect-src should fall back to 'self' only: %s", host, csp)
		}
	}
	// A normal Host still gets its explicit ws entries.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "piperoom.example:8090"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if csp := rec.Header().Get("Content-Security-Policy"); !strings.Contains(csp, "wss://piperoom.example:8090") {
		t.Fatalf("normal Host missing from CSP: %s", csp)
	}
}

func TestStaticSymlinkCannotEscapeWebDir(t *testing.T) {
	dir := webDir(t)
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("TOPSECRET"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A link to a file, and a link to a directory, both pointing outside.
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(dir, "assets", "leak.txt")); err != nil {
		t.Skipf("cannot create symlinks here: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "linkdir")); err != nil {
		t.Skipf("cannot create directory symlinks here: %v", err)
	}
	srv := newWebServer(t, httpapi.WithWebDir(dir))
	for _, p := range []string{"/assets/leak.txt", "/linkdir/secret.txt"} {
		resp, body := fetch(t, http.MethodGet, srv.URL+p)
		if strings.Contains(body, "TOPSECRET") {
			t.Fatalf("GET %s followed a symlink out of the web dir", p)
		}
		if resp.StatusCode == http.StatusOK && body != indexHTML {
			t.Fatalf("GET %s = 200 %q", p, body)
		}
	}
}

func TestStaticAllowHeaderAndCaseInsensitiveAPI(t *testing.T) {
	srv := newWebServer(t, httpapi.WithWebDir(webDir(t)))
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/rooms/abc", strings.NewReader("{}"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST /rooms/abc = %d, Allow %q", resp.StatusCode, resp.Header.Get("Allow"))
	}
	for _, p := range []string{"/API/rooms", "/Api/me", "/aPi"} {
		resp, body := fetch(t, http.MethodGet, srv.URL+p)
		if resp.StatusCode != http.StatusNotFound || !strings.Contains(body, `"not_found"`) || strings.Contains(body, "Piperoom") {
			t.Fatalf("GET %s = %d %q, want the JSON 404", p, resp.StatusCode, body)
		}
	}
}

// The web app is same-origin (Vite proxy in dev, the image in production), so
// the server sends no CORS headers in either mode (design §1.4).
func TestNoCORSHeaders(t *testing.T) {
	for name, srv := range map[string]*httptest.Server{
		"with web dir":    newWebServer(t, httpapi.WithWebDir(webDir(t))),
		"without web dir": newWebServer(t),
	} {
		req, _ := http.NewRequest(http.MethodOptions, srv.URL+"/api/rooms", nil)
		req.Header.Set("Origin", "http://localhost:5173")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		for h := range resp.Header {
			if strings.HasPrefix(strings.ToLower(h), "access-control-") {
				t.Fatalf("%s: unexpected CORS header %q", name, h)
			}
		}
	}
}

func TestWebDirUnsetKeepsTodaysBehaviour(t *testing.T) {
	srv := newWebServer(t) // no WithWebDir
	resp, body := fetch(t, http.MethodGet, srv.URL+"/")
	if resp.StatusCode != http.StatusNotFound || !strings.Contains(body, `"not_found"`) {
		t.Fatalf("GET / = %d %q, want the JSON 404", resp.StatusCode, body)
	}
	resp, _ = fetch(t, http.MethodGet, srv.URL+"/rooms/abc")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /rooms/abc = %d, want 404", resp.StatusCode)
	}
	if resp.Header.Get("Content-Security-Policy") != "" || resp.Header.Get("X-Content-Type-Options") != "" {
		t.Fatalf("security headers set without a web dir: %v", resp.Header)
	}
	// A public origin alone must not turn the UI on either.
	srv = newWebServer(t, withPublic(t, "https://piperoom.example"))
	resp, _ = fetch(t, http.MethodGet, srv.URL+"/")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET / with only a public URL = %d, want 404", resp.StatusCode)
	}
}
