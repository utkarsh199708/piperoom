package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/httpapi"
)

func TestParsePublicURL(t *testing.T) {
	ok := []struct {
		in           string
		scheme, host string
	}{
		{"", "", ""},
		{"  ", "", ""},
		{"https://piperoom.corp.example", "https", "piperoom.corp.example"},
		{"https://Piperoom.Corp.Example/some/path", "https", "piperoom.corp.example"},
		{"https://piperoom.corp.example:443", "https", "piperoom.corp.example"},
		{"http://piperoom.corp.example:80/", "http", "piperoom.corp.example"},
		{"http://piperoom.corp.example:443", "http", "piperoom.corp.example:443"},
		{"https://piperoom.corp.example:8443", "https", "piperoom.corp.example:8443"},
		{"http://10.8.0.5:8090", "http", "10.8.0.5:8090"},
		{"http://piperoom", "http", "piperoom"},
	}
	for _, tc := range ok {
		got, err := httpapi.ParsePublicURL(tc.in)
		if err != nil || got.Scheme != tc.scheme || got.Host != tc.host {
			t.Errorf("ParsePublicURL(%q) = %+v, %v; want %s %s", tc.in, got, err, tc.scheme, tc.host)
		}
	}

	bad := []string{
		"http://*",
		"https://*.corp.example",
		"http://*.x",
		"https://piperoom.*",
		"https://pipe?room.example",
		"https://[abc].example",
		"https://[::1]:8090",
		`https://pipe\room.example`,
		"https://user:pw@piperoom.example",
		"ftp://piperoom.example",
		"piperoom.example",
		"//piperoom.example",
		"https://",
		"https://piperoom.example:0",
		"https://piperoom.example:99999",
		"https://piperoom.example:abc",
		"https://-bad.example",
		"https://bad..example/ ok",
		"https://exa mple.com",
		"https://%2A.example",
	}
	for _, in := range bad {
		got, err := httpapi.ParsePublicURL(in)
		if err == nil {
			t.Errorf("ParsePublicURL(%q) = %+v, nil; want an error", in, got)
			continue
		}
		if !strings.Contains(err.Error(), "PIPEROOM_PUBLIC_URL") {
			t.Errorf("ParsePublicURL(%q) error %q does not name PIPEROOM_PUBLIC_URL", in, err)
		}
	}
}

// dialOrigin tries a WebSocket handshake with the given Origin header and
// reports whether the server accepted it.
func dialOrigin(t *testing.T, srv *httptest.Server, roomID, origin string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/rooms/" + roomID + "/ws?after=0"
	opts := &websocket.DialOptions{HTTPHeader: http.Header{}}
	opts.HTTPHeader.Set("X-User", "arjun")
	if origin != "" {
		opts.HTTPHeader.Set("Origin", origin)
	}
	conn, resp, err := websocket.Dial(ctx, url, opts)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusForbidden {
			return false
		}
		t.Fatalf("dial with Origin %q: %v", origin, err)
	}
	conn.Close(websocket.StatusNormalClosure, "")
	return true
}

func TestWebSocketOrigins(t *testing.T) {
	users := httpapi.WithWebDir(webDir(t))
	t.Run("web dir set: same origin, public host, nothing else", func(t *testing.T) {
		srv := newWebServer(t, users, withPublic(t, "https://piperoom.corp.example"))
		id := createRoom(t, srv)
		host := strings.TrimPrefix(srv.URL, "http://")
		for origin, want := range map[string]bool{
			"http://" + host:                true,  // the page served by this server
			"https://piperoom.corp.example": true,  // PIPEROOM_PUBLIC_URL
			"":                              true,  // no Origin header: not a browser
			"https://evil.example":          false, // foreign site
			"http://localhost:5173":         false, // dev UI is off in this mode
			"http://127.0.0.1:5173":         false,
			"https://piperoom.corp.example.evil.example": false,
			"https://sub.piperoom.corp.example":          false,
			"https://piperoom.corp.example:8443":         false, // other port is another origin
		} {
			if got := dialOrigin(t, srv, id, origin); got != want {
				t.Errorf("Origin %q accepted = %v, want %v", origin, got, want)
			}
		}
	})
	t.Run("default port in the URL still matches the browser's Origin", func(t *testing.T) {
		srv := newWebServer(t, users, withPublic(t, "https://piperoom.corp.example:443/"))
		id := createRoom(t, srv)
		if !dialOrigin(t, srv, id, "https://piperoom.corp.example") {
			t.Error("Origin without the default port was rejected")
		}
	})
	t.Run("web dir unset: dev origins as before", func(t *testing.T) {
		srv := newWebServer(t)
		id := createRoom(t, srv)
		if !dialOrigin(t, srv, id, "http://localhost:5173") {
			t.Error("dev origin rejected without a web dir")
		}
		if dialOrigin(t, srv, id, "https://evil.example") {
			t.Error("foreign origin accepted")
		}
	})
}

func createRoom(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create room = %d", resp.StatusCode)
	}
	v := decode[map[string]any](t, resp)
	if id, _ := v["id"].(string); id != "" {
		return id
	}
	if r, ok := v["room"].(map[string]any); ok {
		if id, _ := r["id"].(string); id != "" {
			return id
		}
	}
	t.Fatalf("no room id in %v", v)
	return ""
}
