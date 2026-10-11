package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

type fakePinger struct {
	err   error
	block bool // wait for ctx to end, like a database that does not answer
	calls int
}

func (p *fakePinger) Ping(ctx context.Context) error {
	p.calls++
	if p.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return p.err
}

type notReadyCall struct {
	reason string
	err    error
}

func TestHealthz(t *testing.T) {
	h := newHarness(t, nil)
	rec := h.do(req{method: http.MethodGet, path: "/healthz"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d\n%s", rec.Code, rec.Body.String())
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"status":"ok"}` {
		t.Fatalf("body = %s", got)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
}

func TestReadyzReady(t *testing.T) {
	tests := []struct {
		name  string
		ready Pinger
	}{
		{"nil pinger", nil},
		{"pinger ok", &fakePinger{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, func(c *Config) { c.Ready = tc.ready })
			rec := h.do(req{method: http.MethodGet, path: "/readyz"})
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d\n%s", rec.Code, rec.Body.String())
			}
			if got := strings.TrimSpace(rec.Body.String()); got != `{"status":"ready","checks":{"store":"ok"}}` {
				t.Fatalf("body = %s", got)
			}
		})
	}
}

func TestReadyzUnreachable(t *testing.T) {
	secret := "dial tcp db.internal:5432 password=hunter2: connection refused"
	p := &fakePinger{err: errors.New(secret)}
	var calls []notReadyCall
	h := newHarness(t, func(c *Config) {
		c.Ready = p
		c.OnNotReady = func(reason string, err error) { calls = append(calls, notReadyCall{reason, err}) }
	})
	rec := h.do(req{method: http.MethodGet, path: "/readyz"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d\n%s", rec.Code, rec.Body.String())
	}
	body := strings.TrimSpace(rec.Body.String())
	if body != `{"status":"not_ready","checks":{"store":"unreachable"}}` {
		t.Fatalf("body = %s", body)
	}
	if strings.Contains(body, "hunter2") || strings.Contains(body, "db.internal") {
		t.Fatalf("body leaks the error: %s", body)
	}
	if strings.Contains(h.log.String(), "hunter2") {
		t.Fatalf("request log leaks the error: %s", h.log.String())
	}
	if len(calls) != 1 || calls[0].reason != "unreachable" || calls[0].err == nil {
		t.Fatalf("OnNotReady calls = %+v", calls)
	}
}

func TestReadyzTimeout(t *testing.T) {
	p := &fakePinger{block: true}
	var calls []notReadyCall
	h := newHarness(t, func(c *Config) {
		c.Ready = p
		c.ProbeTimeout = 20 * time.Millisecond
		c.OnNotReady = func(reason string, err error) { calls = append(calls, notReadyCall{reason, err}) }
	})
	start := time.Now()
	rec := h.do(req{method: http.MethodGet, path: "/readyz"})
	if d := time.Since(start); d > time.Second {
		t.Fatalf("readyz took %v", d)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d\n%s", rec.Code, rec.Body.String())
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"status":"not_ready","checks":{"store":"timeout"}}` {
		t.Fatalf("body = %s", got)
	}
	if len(calls) != 1 || calls[0].reason != "timeout" {
		t.Fatalf("OnNotReady calls = %+v", calls)
	}
}

// The probe must not inherit the 60 s request budget: with a long
// RequestTimeout, readyz still gives up after ProbeTimeout.
func TestReadyzOwnDeadline(t *testing.T) {
	h := newHarness(t, func(c *Config) {
		c.Ready = &fakePinger{block: true}
		c.RequestTimeout = time.Hour
		c.ProbeTimeout = 10 * time.Millisecond
	})
	done := make(chan int, 1)
	go func() { done <- h.do(req{method: http.MethodGet, path: "/readyz"}).Code }()
	select {
	case code := <-done:
		if code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readyz did not honour ProbeTimeout")
	}
}

func TestProbesNeedNoUserAndOnlyGET(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.Ready = &fakePinger{} })
	for _, path := range []string{"/healthz", "/readyz"} {
		// No X-User header: still 200 (the auth middleware is not on these routes).
		if rec := h.do(req{method: http.MethodGet, path: path}); rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d\n%s", path, rec.Code, rec.Body.String())
		}
		for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			rec := h.do(req{method: m, path: path})
			wantError(t, rec, http.StatusMethodNotAllowed, codeMethod)
		}
	}
}
