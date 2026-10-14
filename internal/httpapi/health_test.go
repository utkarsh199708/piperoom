package httpapi_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/version"
)

type fakePinger struct {
	err   error
	block bool // wait for ctx to end, like a database that does not answer
}

func (p *fakePinger) Ping(ctx context.Context) error {
	if p.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return p.err
}

// syncBuf is a goroutine-safe log sink: the request logger runs in the
// server's goroutine while the test reads.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func newProbeServer(t *testing.T, opts ...httpapi.Option) (*httptest.Server, *syncBuf) {
	t.Helper()
	logs := &syncBuf{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	h := httpapi.New(svc, st, log, opts...)
	srv := httptest.NewServer(h)
	t.Cleanup(func() {
		srv.Close()
		h.Close()
	})
	return srv, logs
}

func get(t *testing.T, method, url string) (int, string) {
	t.Helper()
	resp := doJSON(t, method, url, "", "")
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, strings.TrimSpace(string(b))
}

func TestHealthz(t *testing.T) {
	// Even a failing store must not affect liveness.
	srv, logs := newProbeServer(t, httpapi.WithReady(&fakePinger{err: errors.New("down")}))
	code, body := get(t, http.MethodGet, srv.URL+"/healthz")
	if code != http.StatusOK || body != `{"status":"ok","version":"dev"}` {
		t.Fatalf("GET /healthz = %d %s", code, body)
	}
	l := logs.String()
	if !strings.Contains(l, "level=DEBUG") || !strings.Contains(l, "path=/healthz") {
		t.Fatalf("probe request not logged at Debug: %s", l)
	}
	if strings.Contains(l, "level=INFO") {
		t.Fatalf("probe request logged at Info: %s", l)
	}
}

func TestReadyzReady(t *testing.T) {
	tests := []struct {
		name string
		opts []httpapi.Option
	}{
		{"no pinger", nil},
		{"nil pinger", []httpapi.Option{httpapi.WithReady(nil)}},
		{"pinger ok", []httpapi.Option{httpapi.WithReady(&fakePinger{})}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newProbeServer(t, tc.opts...)
			code, body := get(t, http.MethodGet, srv.URL+"/readyz")
			if code != http.StatusOK || body != `{"status":"ready","checks":{"store":"ok"}}` {
				t.Fatalf("GET /readyz = %d %s", code, body)
			}
		})
	}
}

func TestReadyzUnreachable(t *testing.T) {
	secret := "dial tcp db.internal:5432 postgres://piperoom:hunter2@db: connection refused"
	srv, _ := newProbeServer(t, httpapi.WithReady(&fakePinger{err: errors.New(secret)}))
	code, body := get(t, http.MethodGet, srv.URL+"/readyz")
	if code != http.StatusServiceUnavailable || body != `{"status":"not_ready","checks":{"store":"unreachable"}}` {
		t.Fatalf("GET /readyz = %d %s", code, body)
	}
	if strings.Contains(body, "hunter2") || strings.Contains(body, "db.internal") {
		t.Fatalf("body leaks the error: %s", body)
	}
}

func TestReadyzTimeout(t *testing.T) {
	srv, _ := newProbeServer(t,
		httpapi.WithReady(&fakePinger{block: true}),
		httpapi.WithProbeTimeout(20*time.Millisecond),
	)
	start := time.Now()
	code, body := get(t, http.MethodGet, srv.URL+"/readyz")
	if d := time.Since(start); d > time.Second {
		t.Fatalf("readyz took %v; it must use its own probe deadline", d)
	}
	if code != http.StatusServiceUnavailable || body != `{"status":"not_ready","checks":{"store":"timeout"}}` {
		t.Fatalf("GET /readyz = %d %s", code, body)
	}
}

func TestProbesNeedNoUserAndOnlyGET(t *testing.T) {
	srv, _ := newProbeServer(t, httpapi.WithReady(&fakePinger{}))
	for _, path := range []string{"/healthz", "/readyz"} {
		// No X-User header: the auth middleware is not on these routes.
		if code, body := get(t, http.MethodGet, srv.URL+path); code != http.StatusOK {
			t.Fatalf("GET %s = %d %s", path, code, body)
		}
		for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			code, body := get(t, m, srv.URL+path)
			if code != http.StatusMethodNotAllowed || !strings.Contains(body, `"method_not_allowed"`) {
				t.Fatalf("%s %s = %d %s", m, path, code, body)
			}
		}
	}
}

func TestReadyzWarnRateLimited(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}
	srv, logs := newProbeServer(t,
		httpapi.WithReady(&fakePinger{err: errors.New("refused")}),
		httpapi.WithClock(clock),
	)
	warns := func() int { return strings.Count(logs.String(), "readiness check failed") }

	get(t, http.MethodGet, srv.URL+"/readyz")
	advance(30 * time.Second)
	get(t, http.MethodGet, srv.URL+"/readyz")
	if n := warns(); n != 1 {
		t.Fatalf("two failures within a minute logged %d warnings, want 1\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("failure not logged at Warn: %s", logs.String())
	}

	advance(31 * time.Second)
	get(t, http.MethodGet, srv.URL+"/readyz")
	if n := warns(); n != 2 {
		t.Fatalf("failure after a minute: %d warnings, want 2\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "suppressed=1") {
		t.Fatalf("second warning should report 1 suppressed failure: %s", logs.String())
	}
}

// GET /healthz reports the linked build version (docs/api.md).
func TestHealthzVersion(t *testing.T) {
	srv, _ := newProbeServer(t)
	for _, tc := range []struct{ set, want string }{{"", "dev"}, {"v9.9.9", "v9.9.9"}} {
		old := version.Version
		version.Version = tc.set
		code, body := get(t, http.MethodGet, srv.URL+"/healthz")
		version.Version = old
		if code != http.StatusOK || body != `{"status":"ok","version":"`+tc.want+`"}` {
			t.Fatalf("Version=%q: GET /healthz = %d %s", tc.set, code, body)
		}
	}
}
