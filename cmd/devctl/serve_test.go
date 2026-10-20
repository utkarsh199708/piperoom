package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/stacklock"
)

// fakeRunner plays scenarioRunner without touching the stack or Docker.
type fakeRunner struct {
	mu         sync.Mutex
	breakCalls int
	fixCalls   int

	// entered, when set, receives once when a break starts (buffered so the
	// runner never blocks the caller); release, when set, is what breakIt waits
	// on before returning, so a test can hold a run in flight.
	entered chan struct{}
	release chan struct{}
}

func (f *fakeRunner) breakIt(_ context.Context, _ string) error {
	f.mu.Lock()
	f.breakCalls++
	f.mu.Unlock()
	if f.entered != nil {
		f.entered <- struct{}{}
	}
	if f.release != nil {
		<-f.release
	}
	return nil
}

func (f *fakeRunner) fixIt(_ context.Context, _ string) error {
	f.mu.Lock()
	f.fixCalls++
	f.mu.Unlock()
	return nil
}

// lockOK is a fake lock check that always reports the caller holds the stack
// lock, so the happy-path serve tests never touch the real lock file.
func lockOK(string) error { return nil }

// newServeServer starts the serve API on a loopback httptest server. The
// listener is created first so the security middleware's allowed-Host set can be
// built from the real port before the handler is installed.
func newServeServer(t *testing.T, r scenarioRunner, cfg serveConfig) *httptest.Server {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewUnstartedServer(nil)
	cfg.addr = srv.Listener.Addr().String()
	srv.Config.Handler = newServeAPI(r, log, cfg).routes()
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

func TestServeScenarios(t *testing.T) {
	srv := newServeServer(t, &fakeRunner{}, serveConfig{lockCheck: lockOK})
	resp, err := http.Get(srv.URL + "/scenarios")
	if err != nil {
		t.Fatalf("GET /scenarios: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /scenarios status = %d, want 200", resp.StatusCode)
	}
	var body scenariosResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode scenarios: %v", err)
	}
	got := map[string]bool{}
	for _, n := range body.Scenarios {
		got[n] = true
	}
	for _, want := range []string{"schema-change", "connector-down", "connector-auth", "dlq-backlog"} {
		if !got[want] {
			t.Errorf("scenarios list missing %q (got %v)", want, body.Scenarios)
		}
	}
}

func TestServeUnknownScenarioIs404(t *testing.T) {
	srv := newServeServer(t, &fakeRunner{}, serveConfig{lockCheck: lockOK})
	resp, err := http.Post(srv.URL+"/break/nope", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /break/nope: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("POST /break/nope status = %d, want 404", resp.StatusCode)
	}
	var body serveStatus
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "error" {
		t.Errorf("status = %q, want error", body.Status)
	}
}

func TestServeBreakAndFixSuccess(t *testing.T) {
	tests := []struct {
		name string
		op   string
	}{
		{"break", "break"},
		{"fix", "fix"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fr := &fakeRunner{}
			srv := newServeServer(t, fr, serveConfig{lockCheck: lockOK})
			resp, err := http.Post(srv.URL+"/"+tt.op+"/schema-change", "application/json", nil)
			if err != nil {
				t.Fatalf("POST /%s/schema-change: %v", tt.op, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("POST /%s/schema-change status = %d, want 200", tt.op, resp.StatusCode)
			}
			var body serveStatus
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body.Status != "ok" || body.Action != tt.op || body.Scenario != "schema-change" {
				t.Errorf("body = %+v, want status ok, action %s, scenario schema-change", body, tt.op)
			}
			fr.mu.Lock()
			defer fr.mu.Unlock()
			if tt.op == "break" && fr.breakCalls != 1 {
				t.Errorf("breakCalls = %d, want 1", fr.breakCalls)
			}
			if tt.op == "fix" && fr.fixCalls != 1 {
				t.Errorf("fixCalls = %d, want 1", fr.fixCalls)
			}
		})
	}
}

func TestServeConcurrentBreakIs409(t *testing.T) {
	fr := &fakeRunner{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	srv := newServeServer(t, fr, serveConfig{lockCheck: lockOK})

	type result struct {
		code int
		body string
	}
	first := make(chan result, 1)
	go func() {
		resp, err := http.Post(srv.URL+"/break/schema-change", "application/json", nil)
		if err != nil {
			first <- result{code: -1, body: err.Error()}
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		first <- result{code: resp.StatusCode, body: string(b)}
	}()

	// The first run has entered the runner (and holds the single-flight slot).
	<-fr.entered

	resp, err := http.Post(srv.URL+"/break/connector-down", "application/json", nil)
	if err != nil {
		t.Fatalf("second break: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second break status = %d, want 409 (body %s)", resp.StatusCode, b)
	}

	close(fr.release)
	firstRes := <-first
	if firstRes.code != http.StatusOK {
		t.Fatalf("first break status = %d, want 200 (body %s)", firstRes.code, firstRes.body)
	}

	fr.mu.Lock()
	defer fr.mu.Unlock()
	if fr.breakCalls != 1 {
		t.Errorf("breakCalls = %d, want 1 (the busy request must not run)", fr.breakCalls)
	}
}

func TestServeForeignHostIs403(t *testing.T) {
	srv := newServeServer(t, &fakeRunner{}, serveConfig{lockCheck: lockOK})
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/scenarios", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Host = "evil.example:7071"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET with foreign Host: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign Host status = %d, want 403", resp.StatusCode)
	}
}

func TestServeForeignOriginIs403(t *testing.T) {
	srv := newServeServer(t, &fakeRunner{}, serveConfig{allowOrigin: "http://localhost:5173", lockCheck: lockOK})
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/break/schema-change", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://evil.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST with foreign Origin: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign Origin status = %d, want 403", resp.StatusCode)
	}
}

func TestServePOSTWithoutMarkerIs403(t *testing.T) {
	srv := newServeServer(t, &fakeRunner{}, serveConfig{lockCheck: lockOK})
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/break/schema-change", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST without marker: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST without marker status = %d, want 403", resp.StatusCode)
	}
}

func TestServeRunRequiresLock(t *testing.T) {
	checkErr := errors.New(`devctl: stack is locked by "bob" (you are "alice")`)
	fr := &fakeRunner{}
	srv := newServeServer(t, fr, serveConfig{
		owner:     "alice",
		lockCheck: func(string) error { return checkErr },
	})
	resp, err := http.Post(srv.URL+"/break/schema-change", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /break/schema-change: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusLocked {
		t.Fatalf("status = %d, want 423", resp.StatusCode)
	}
	var body serveStatus
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "error" || !strings.Contains(body.Error, "bob") {
		t.Errorf("body = %+v, want error naming holder bob", body)
	}
	fr.mu.Lock()
	defer fr.mu.Unlock()
	if fr.breakCalls != 0 {
		t.Errorf("breakCalls = %d, want 0 (a lock failure must not run the break)", fr.breakCalls)
	}
}

func TestServeCmdRequiresLockAtStartup(t *testing.T) {
	t.Setenv(stacklock.EnvPath, filepath.Join(t.TempDir(), "stack.lock"))
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := &stack{log: log}
	err := serveCmd(context.Background(), st, []string{"--owner", "alice"})
	if err == nil || !strings.Contains(err.Error(), "not locked") {
		t.Fatalf("serveCmd without lock = %v, want 'not locked'", err)
	}
}

func TestParseLoopbackAddr(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		wantErr bool
	}{
		{"ipv4 loopback", "127.0.0.1:7071", false},
		{"ipv4 loopback low port", "127.0.0.2:0", false},
		{"ipv6 loopback", "[::1]:7071", false},
		{"all interfaces", "0.0.0.0:7071", true},
		{"unspecified ipv6", "[::]:7071", true},
		{"public ip", "192.168.1.10:7071", true},
		{"hostname", "localhost:7071", true},
		{"missing port", "127.0.0.1", true},
		{"missing host", ":7071", true},
		{"garbage", "not-an-addr", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLoopbackAddr(tt.addr)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseLoopbackAddr(%q) = %q, nil; want error", tt.addr, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLoopbackAddr(%q) error = %v, want nil", tt.addr, err)
			}
			if got == "" {
				t.Fatalf("parseLoopbackAddr(%q) = empty, want a listen address", tt.addr)
			}
		})
	}
}
