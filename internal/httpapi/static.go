package httpapi

import (
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
)

// WithWebDir makes the server also serve the built web UI (web/dist) from
// dir at "/", with a single-page-app fallback to index.html for paths that are
// not files. It adds a security-header set to every response. An empty dir
// leaves the server exactly as it was: API and probes only.
func WithWebDir(dir string) Option {
	return func(s *Server) { s.webDir = dir }
}

// PublicOrigin is the validated scheme and host of PIPEROOM_PUBLIC_URL. The
// zero value means "not set".
type PublicOrigin struct {
	Scheme string
	Host   string // lower case; the default port for the scheme is removed
}

// publicHostRe is deliberately narrow: letters, digits, '.', '-' and an
// optional port. The host becomes a WebSocket origin pattern, and the
// websocket library matches patterns with path.Match, so '*', '?', '[' and
// '\' in it would widen the allowed origins (for example "*" would accept
// every site). IPv6 literals need '[' and are therefore not supported here.
var publicHostRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*(:[0-9]{1,5})?$`)

// ParsePublicURL validates PIPEROOM_PUBLIC_URL. An empty value is valid and
// returns the zero PublicOrigin. Anything else must be an http or https URL
// with a plain host name or IPv4 address, no user info; a path is ignored.
func ParsePublicURL(raw string) (PublicOrigin, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return PublicOrigin{}, nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return PublicOrigin{}, fmt.Errorf("PIPEROOM_PUBLIC_URL is not a valid URL: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return PublicOrigin{}, fmt.Errorf("PIPEROOM_PUBLIC_URL %q must start with http:// or https://", raw)
	}
	if u.User != nil {
		return PublicOrigin{}, fmt.Errorf("PIPEROOM_PUBLIC_URL %q must not contain a user name or password", raw)
	}
	if u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(raw, "?#") {
		return PublicOrigin{}, fmt.Errorf("PIPEROOM_PUBLIC_URL %q must not contain a query or fragment", raw)
	}
	host := strings.ToLower(u.Host)
	if !publicHostRe.MatchString(host) {
		return PublicOrigin{}, fmt.Errorf("PIPEROOM_PUBLIC_URL %q has an invalid host: use a plain host name or IPv4 address with an optional port (no wildcards)", raw)
	}
	if i := strings.LastIndexByte(host, ':'); i >= 0 {
		port, err := strconv.Atoi(host[i+1:])
		if err != nil || port < 1 || port > 65535 {
			return PublicOrigin{}, fmt.Errorf("PIPEROOM_PUBLIC_URL %q has an invalid port", raw)
		}
		// Browsers omit the default port from the Origin header, so the
		// pattern must too or it would never match.
		if (scheme == "https" && port == 443) || (scheme == "http" && port == 80) {
			host = host[:i]
		}
	}
	return PublicOrigin{Scheme: scheme, Host: host}, nil
}

// WithPublicOrigin sets the externally visible origin (see ParsePublicURL).
// Its host is allowed as a WebSocket origin and in the CSP connect-src, for
// deployments behind a proxy that rewrites the Host header.
func WithPublicOrigin(o PublicOrigin) Option {
	return func(s *Server) { s.public = o }
}

// Cache policies. Vite names everything under assets/ with a content hash, so
// those files never change for a given URL. index.html names those hashed
// files, so it must be revalidated on every load, or a deploy would leave
// browsers on a page that points at deleted assets.
const (
	cacheImmutable  = "public, max-age=31536000, immutable"
	cacheRevalidate = "no-cache"
)

// hostRe limits what a request's Host header may add to the CSP. The header
// is client-controlled; a value with ';' or a space could add a directive.
var hostRe = regexp.MustCompile(`^[A-Za-z0-9.\-:\[\]]+$`)

// staticHandler serves the SPA. It is the router's NotFound handler, so it
// only sees requests that no API or probe route matched.
type staticHandler struct {
	fsys fs.FS
}

// newStaticHandler serves files from root. os.Root refuses any path that
// leaves the directory, symbolic links included, so a link planted in the web
// dir cannot expose other files.
func newStaticHandler(root *os.Root) *staticHandler {
	return &staticHandler{fsys: root.FS()}
}

// isServerPath reports whether the lower-case, cleaned path p is prefix or lies
// under it. /api and /auth belong to the server, never to the web UI.
func isServerPath(p, prefix string) bool {
	return p == prefix || strings.HasPrefix(p, prefix+"/")
}

func (h *staticHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := path.Clean("/" + r.URL.Path)
	// An unknown /auth path is the same: the web app must not get index.html
	// for /auth/login where the route is not mounted (it would loop).
	// An unknown API path must stay a JSON 404, never the HTML shell: clients
	// parse the envelope, and a 200 HTML page would hide a wrong URL.
	// chi matches routes case-sensitively, so "/API/..." misses them too: keep
	// that a JSON 404 as well.
	if lp := strings.ToLower(p); isServerPath(lp, "/api") || isServerPath(lp, "/auth") {
		writeError(w, http.StatusNotFound, "not_found", "not found")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}

	name := strings.TrimPrefix(p, "/")
	if name == "" {
		name = "index.html"
	}
	if h.serveFile(w, r, name) {
		return
	}
	// A missing file with an extension is a missing asset: a 404 is honest,
	// and HTML served for a .js URL only gives a confusing parse error.
	if path.Ext(name) != "" {
		http.NotFound(w, r)
		return
	}
	// A client-side route such as /rooms/abc: hand the browser the app shell.
	if !h.serveFile(w, r, "index.html") {
		http.NotFound(w, r)
	}
}

// serveFile writes the regular file name from the web dir and reports whether
// it did. Directories and unreadable names report false.
func (h *staticHandler) serveFile(w http.ResponseWriter, r *http.Request, name string) bool {
	if !fs.ValidPath(name) {
		return false
	}
	f, err := h.fsys.Open(name)
	if err != nil {
		return false
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	rs, ok := f.(io.ReadSeeker)
	if !ok {
		return false
	}
	if strings.HasPrefix(name, "assets/") {
		w.Header().Set("Cache-Control", cacheImmutable)
	} else {
		w.Header().Set("Cache-Control", cacheRevalidate)
	}
	http.ServeContent(w, r, name, info.ModTime(), rs)
	return true
}

// securityHeaders adds the browser hardening headers. The CSP allows the
// app's own origin (HTTP and WebSocket) and nothing else, except the two
// Google Fonts hosts that the stylesheet link in index.html uses.
func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", s.csp(r.Host))
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Frame-Options", "DENY") // old browsers; CSP frame-ancestors covers the rest
		next.ServeHTTP(w, r)
	})
}

func (s *Server) csp(host string) string {
	connect := []string{"'self'"}
	// 'self' covers same-origin WebSockets in current browsers; the explicit
	// ws/wss entries cover older ones.
	if hostRe.MatchString(host) {
		connect = append(connect, "ws://"+host, "wss://"+host)
	}
	if s.public.Host != "" {
		h := s.public.Host
		connect = append(connect, s.public.Scheme+"://"+h, "ws://"+h, "wss://"+h)
	}
	return strings.Join([]string{
		"default-src 'self'",
		"script-src 'self'",
		// React sets style attributes; the stylesheet comes from Google Fonts.
		"style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
		"font-src 'self' https://fonts.gstatic.com",
		"img-src 'self' data:",
		"connect-src " + strings.Join(connect, " "),
		"object-src 'none'",
		"base-uri 'self'",
		"form-action 'self'",
		"frame-ancestors 'none'",
	}, "; ")
}
