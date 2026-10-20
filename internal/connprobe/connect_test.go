package connprobe

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

const (
	testUser     = "probe-user-7c1"
	testPassword = "probe-secret-9f2"
	secretValue  = "hunter2-db-password"
)

// fakeConnect answers like Kafka Connect for one connector, "orders-src".
func fakeConnect(t *testing.T, mux map[string]func(w http.ResponseWriter, r *http.Request)) http.Handler {
	t.Helper()
	base := map[string]func(w http.ResponseWriter, r *http.Request){
		"/connectors": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`["orders-src"]`))
		},
		"/connectors/orders-src/status": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"name":"orders-src","connector":{"state":"RUNNING","worker_id":"w:8083"},"tasks":[{"id":0,"state":"RUNNING"}]}`))
		},
		"/connectors/orders-src/config": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.password":"` + secretValue + `"}`))
		},
	}
	for k, v := range mux {
		base[k] = v
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("probe sent %s %s; probes only read", r.Method, r.URL.Path)
		}
		if h, ok := base[r.URL.EscapedPath()]; ok {
			h(w, r)
			return
		}
		http.NotFound(w, r)
	})
}

func connectTarget(url string, connectors ...string) Target {
	if len(connectors) == 0 {
		connectors = []string{"orders-src"}
	}
	return Target{
		Kind:      KindConnect,
		Config:    config.Config{ConnectURL: url},
		Pipelines: []owners.Pipeline{{ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}, Connectors: connectors}},
	}
}

func TestConnectOK(t *testing.T) {
	srv := httptest.NewServer(fakeConnect(t, nil))
	defer srv.Close()
	r := Run(context.Background(), connectTarget(srv.URL), loopGuard())
	wantChecks(t, r, "config", StatusOK, "reach", StatusOK, "login", StatusOK, "connectors", StatusOK, "config_read", StatusOK)
	if !r.OK {
		t.Error("want ok")
	}
	if d := check(t, r, "connectors").Detail; d != "orders-src RUNNING, 1/1 tasks RUNNING" {
		t.Errorf("connectors detail %q", d)
	}
	assertNoSecret(t, r, secretValue, "database.password", "PostgresConnector")
}

// tlsServer starts h with a certificate whose CA is written to the returned
// file, so basic auth (refused over http://) can be tested.
func tlsServer(t *testing.T, h http.Handler) (*httptest.Server, string) {
	t.Helper()
	cert, ca := selfSigned(t)
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, ca
}

func TestConnectBasicAuth(t *testing.T) {
	auth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			u, p, ok := r.BasicAuth()
			if !ok || u != testUser || p != testPassword {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error_code":401,"message":"bad credentials for ` + u + `"}`))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	srv, ca := tlsServer(t, auth(fakeConnect(t, nil)))

	tg := connectTarget(srv.URL)
	tg.Config.ConnectUsername, tg.Config.ConnectPassword, tg.Config.ConnectTLSCAFile = testUser, testPassword, ca
	r := Run(context.Background(), tg, loopGuard())
	wantChecks(t, r, "config", StatusOK, "reach", StatusOK, "login", StatusOK, "connectors", StatusOK, "config_read", StatusOK)
	assertNoSecret(t, r, testUser, testPassword, secretValue)

	tg.Config.ConnectPassword = "wrong-" + testPassword
	r = Run(context.Background(), tg, loopGuard())
	wantChecks(t, r, "config", StatusOK, "reach", StatusOK, "login", StatusFail, "connectors", StatusSkip, "config_read", StatusSkip)
	if f := check(t, r, "login").Fix; !strings.Contains(f, "CONNECT_USERNAME") || !strings.Contains(f, "CONNECT_PASSWORD") {
		t.Errorf("login fix %q does not name the settings", f)
	}
	assertNoSecret(t, r, testUser, testPassword, "bad credentials")

	// Without the CA the certificate is not trusted.
	tg.Config.ConnectTLSCAFile = ""
	r = Run(context.Background(), tg, loopGuard())
	if c := check(t, r, "reach"); c.Status != StatusFail || !strings.Contains(c.Fix, "CONNECT_TLS_CA_FILE") {
		t.Errorf("untrusted cert: %+v", c)
	}
}

func TestConnectStatuses(t *testing.T) {
	big := strings.Repeat("x", maxHTTPBody+10)
	tests := []struct {
		name string
		mux  map[string]func(http.ResponseWriter, *http.Request)
		want []string
		in   string // substring of the failed check's problem
	}{
		{"no login configured, 401", map[string]func(http.ResponseWriter, *http.Request){
			"/connectors": func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) },
		}, []string{"config", StatusOK, "reach", StatusOK, "login", StatusFail, "connectors", StatusSkip, "config_read", StatusSkip}, "needs a login"},
		{"403", map[string]func(http.ResponseWriter, *http.Request){
			"/connectors": func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) },
		}, []string{"config", StatusOK, "reach", StatusOK, "login", StatusFail, "connectors", StatusSkip, "config_read", StatusSkip}, "GET /connectors"},
		{"500", map[string]func(http.ResponseWriter, *http.Request){
			"/connectors": func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("java.lang.NullPointerException at " + secretValue))
			},
		}, []string{"config", StatusOK, "reach", StatusFail, "login", StatusSkip, "connectors", StatusSkip, "config_read", StatusSkip}, "HTTP 500"},
		{"not connect (404 on /connectors)", map[string]func(http.ResponseWriter, *http.Request){
			"/connectors": http.NotFound,
		}, []string{"config", StatusOK, "reach", StatusFail, "login", StatusSkip, "connectors", StatusSkip, "config_read", StatusSkip}, "404"},
		{"malformed JSON", map[string]func(http.ResponseWriter, *http.Request){
			"/connectors": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"oops":`)) },
		}, []string{"config", StatusOK, "reach", StatusFail, "login", StatusSkip, "connectors", StatusSkip, "config_read", StatusSkip}, "not what"},
		{"oversized body", map[string]func(http.ResponseWriter, *http.Request){
			"/connectors": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(big)) },
		}, []string{"config", StatusOK, "reach", StatusFail, "login", StatusSkip, "connectors", StatusSkip, "config_read", StatusSkip}, "4 MB"},
		{"connector missing", map[string]func(http.ResponseWriter, *http.Request){
			"/connectors/orders-src/status": http.NotFound,
		}, []string{"config", StatusOK, "reach", StatusOK, "login", StatusOK, "connectors", StatusFail, "config_read", StatusSkip}, "orders-src"},
		{"status malformed", map[string]func(http.ResponseWriter, *http.Request){
			"/connectors/orders-src/status": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`[]`)) },
		}, []string{"config", StatusOK, "reach", StatusOK, "login", StatusOK, "connectors", StatusFail, "config_read", StatusSkip}, "not Connect REST JSON"},
		{"config forbidden", map[string]func(http.ResponseWriter, *http.Request){
			"/connectors/orders-src/config": func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) },
		}, []string{"config", StatusOK, "reach", StatusOK, "login", StatusOK, "connectors", StatusOK, "config_read", StatusFail}, "orders-src"},
		{"config oversized", map[string]func(http.ResponseWriter, *http.Request){
			"/connectors/orders-src/config": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(big)) },
		}, []string{"config", StatusOK, "reach", StatusOK, "login", StatusOK, "connectors", StatusOK, "config_read", StatusFail}, "4 MB"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(fakeConnect(t, tc.mux))
			defer srv.Close()
			r := Run(context.Background(), connectTarget(srv.URL), loopGuard())
			wantChecks(t, r, tc.want...)
			if r.OK {
				t.Error("want not ok")
			}
			var found bool
			for _, c := range r.Checks {
				if c.Status == StatusFail {
					found = true
					if !strings.Contains(c.Problem, tc.in) {
						t.Errorf("problem %q does not contain %q", c.Problem, tc.in)
					}
					if c.Fix == "" {
						t.Error("failed check has no fix")
					}
				}
			}
			if !found {
				t.Error("no failed check")
			}
			assertNoSecret(t, r, secretValue, "NullPointer")
		})
	}
}

func TestConnectEscapesNames(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.EscapedPath())
		if r.URL.Path == "/connectors" {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	r := Run(context.Background(), connectTarget(srv.URL, "a/b?c", ".."), loopGuard())
	c := check(t, r, "connectors")
	if c.Status != StatusFail {
		t.Fatalf("want fail, got %+v", c)
	}
	for _, p := range seen {
		if p != "/connectors" && p != "/connectors/a%2Fb%3Fc/status" {
			t.Errorf("unexpected request path %q", p)
		}
	}
	// ".." is refused before any request (it would address another resource).
	if !strings.Contains(c.Problem, "connector .. cannot be requested") {
		t.Errorf("problem %q does not name the refused connector", c.Problem)
	}
}

func TestConnectTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	r := Run(ctx, connectTarget(srv.URL), loopGuard())
	if time.Since(start) > 5*time.Second {
		t.Errorf("probe took %v", time.Since(start))
	}
	c := check(t, r, "reach")
	if c.Status != StatusFail || !strings.Contains(c.Problem, "No answer") {
		t.Errorf("reach: %+v", c)
	}
}

func TestConnectGuardRefusal(t *testing.T) {
	srv := httptest.NewServer(fakeConnect(t, nil))
	defer srv.Close()
	for name, tc := range map[string]struct {
		url string
		g   *Guard
	}{
		"loopback not allowed": {srv.URL, &Guard{}},
		"metadata address":     {"http://169.254.169.254/", loopGuard()},
		"metadata name":        {"http://metadata.google.internal/", loopGuard()},
	} {
		t.Run(name, func(t *testing.T) {
			r := Run(context.Background(), connectTarget(tc.url), tc.g)
			wantChecks(t, r, "config", StatusOK, "reach", StatusFail, "login", StatusSkip, "connectors", StatusSkip, "config_read", StatusSkip)
			if p := check(t, r, "reach").Problem; !strings.Contains(p, "not allowed") {
				t.Errorf("problem %q", p)
			}
		})
	}
}

func TestConnectConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
		in   string
	}{
		{"empty url", config.Config{}, "CONNECT_URL is not set"},
		{"not a url", config.Config{ConnectURL: "localhost:8083"}, "not an http(s)"},
		{"userinfo", config.Config{ConnectURL: "https://u:" + testPassword + "@connect:8083"}, "carries"},
		{"half login", config.Config{ConnectURL: "https://connect:8083", ConnectUsername: testUser}, "Only one"},
		{"login over http", config.Config{ConnectURL: "http://connect:8083", ConnectUsername: testUser, ConnectPassword: testPassword}, "clear text"},
		{"tls on http", config.Config{ConnectURL: "http://connect:8083", ConnectTLSCAFile: "ca.pem"}, "TLS files are set"},
		{"bad ca file", config.Config{ConnectURL: "https://connect:8083", ConnectTLSCAFile: "does-not-exist.pem"}, "TLS settings"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := Run(context.Background(), Target{Kind: KindConnect, Config: tc.cfg}, loopGuard())
			wantChecks(t, r, "config", StatusFail, "reach", StatusSkip, "login", StatusSkip, "connectors", StatusSkip, "config_read", StatusSkip)
			if p := check(t, r, "config").Problem; !strings.Contains(p, tc.in) {
				t.Errorf("problem %q does not contain %q", p, tc.in)
			}
			assertNoSecret(t, r, testUser, testPassword)
		})
	}
}
