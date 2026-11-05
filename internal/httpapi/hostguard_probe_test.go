package httpapi_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/httpapi"
)

type failingPinger struct{}

func (failingPinger) Ping(context.Context) error {
	return errors.New("dial tcp db-secret.internal:5432: password authentication failed for user piperoom")
}

// Cloud load balancers probe a pod by IP, so the Host is an address nobody can
// list. GET /healthz and GET /readyz skip the Host allow-list; nothing else
// does, and the bodies tell an arbitrary Host nothing.
func TestHostGuardHealthProbeExemption(t *testing.T) {
	const podIP = "Host: 10.42.7.19:8090"
	e := newAuthEnv(t)

	for _, p := range []string{"/healthz", "/readyz", "/healthz?x=1", "/readyz?probe=lb"} {
		resp := e.do(t, http.MethodGet, p, "", podIP)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s from a pod-IP Host = %d, want 200", p, resp.StatusCode)
		}
	}

	// Everything near the exemption still goes through the Host check.
	for _, rt := range []struct{ method, path string }{
		{http.MethodPost, "/healthz"},
		{http.MethodPost, "/readyz"},
		{http.MethodHead, "/healthz"},
		{http.MethodGet, "/healthz/x"},
		{http.MethodGet, "/healthz/"},
		{http.MethodGet, "//healthz"},
		{http.MethodGet, "/HEALTHZ"},
		{http.MethodGet, "/Readyz"},
		{http.MethodGet, "/%68ealthz"},
		{http.MethodGet, "/readyz%2F"},
		{http.MethodGet, "/api/rooms"},
		{http.MethodGet, "/"},
	} {
		resp := e.do(t, rt.method, rt.path, "", podIP)
		if resp.StatusCode != http.StatusMisdirectedRequest {
			t.Errorf("%s %s from a pod-IP Host = %d, want 421", rt.method, rt.path, resp.StatusCode)
		}
	}
}

// A failing store must not put its error text, host name or user into a
// response served to an arbitrary Host.
func TestHostGuardReadyzLeaksNothingToAnyHost(t *testing.T) {
	e := newAuthEnv(t, httpapi.WithReady(failingPinger{}))
	resp := e.do(t, http.MethodGet, "/readyz", "", "Host: evil.test")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz = %d, want 503 (probe reached the handler)", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	for _, leak := range []string{"db-secret", "5432", "password", "piperoom", "dial"} {
		if strings.Contains(string(body), leak) {
			t.Errorf("readyz body leaks %q: %s", leak, body)
		}
	}
	resp = e.do(t, http.MethodGet, "/healthz", "", "Host: evil.test")
	body, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != `{"status":"ok","version":"dev","write_mode":"on"}` {
		t.Errorf("GET /healthz = %d %s", resp.StatusCode, body)
	}
}
