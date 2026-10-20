package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
)

// serveDefaultAddr is where the dev-only break/fix API listens. The API has no
// authentication, so it must never leave loopback: parseLoopbackAddr enforces
// that before net.Listen, the Dockerfile only builds cmd/server (never devctl),
// and the security middleware further rejects any request whose Host is not one
// of the loopback spellings (see dev/README.md).
const serveDefaultAddr = "127.0.0.1:7071"

// defaultAllowOrigin is the Origin the dev-only break/fix API answers CORS for.
// It is the Vite dev server the web "Break something" menu (A6) runs from;
// requests from any other Origin are rejected.
const defaultAllowOrigin = "http://localhost:5173"

// serveRunTimeout bounds a single break/fix run. It is generous enough for the
// slowest scenario (connector/restart waits) but keeps a hung scenario from
// holding the single-flight slot forever.
const serveRunTimeout = 10 * time.Minute

// scenarioRunner runs one break or fix for a scenario name. The serve API
// drives it; the real devctl stack implements it and unit tests use a fake.
type scenarioRunner interface {
	breakIt(ctx context.Context, name string) error
	fixIt(ctx context.Context, name string) error
}

// stackRunner adapts the devctl stack to scenarioRunner by looking the scenario
// up in the package scenarios map and running its break/fix function.
type stackRunner struct{ s *stack }

func (r stackRunner) breakIt(ctx context.Context, name string) error {
	sc, ok := scenarios[name]
	if !ok {
		return fmt.Errorf("devctl: unknown scenario %q", name)
	}
	return sc.breakIt(ctx, r.s)
}

func (r stackRunner) fixIt(ctx context.Context, name string) error {
	sc, ok := scenarios[name]
	if !ok {
		return fmt.Errorf("devctl: unknown scenario %q", name)
	}
	return sc.fix(ctx, r.s)
}

// serveStatus is the JSON body every route reports. Break/fix routes carry
// status ok|error|busy; the list route carries the scenario names instead.
type serveStatus struct {
	Status   string `json:"status"`
	Scenario string `json:"scenario,omitempty"`
	Action   string `json:"action,omitempty"`
	Error    string `json:"error,omitempty"`
}

// scenariosResponse is the body of GET /scenarios.
type scenariosResponse struct {
	Scenarios []string `json:"scenarios"`
}

// serveAPI is the HTTP API behind `devctl serve`. It is a dev-only helper with
// no authentication, so it must only ever listen on a loopback address. Because
// it is driven over HTTP, it also enforces the shared-stack lock (the same
// requireLock gate the other mutating commands use) and defends its
// state-changing routes against CSRF and DNS rebinding via the security
// middleware.
type serveAPI struct {
	runner      scenarioRunner
	log         *slog.Logger
	owner       string
	allowOrigin string
	hosts       map[string]bool
	lockCheck   func(owner string) error
	// mu serializes break/fix runs. Only one may run at a time; a second
	// request while one is in flight gets a 409 (TryLock fails) rather than
	// racing the stack.
	mu sync.Mutex
}

// serveConfig carries the non-runner wiring of a serveAPI, separated so tests
// can inject a fake lock check and the exact listen address.
type serveConfig struct {
	addr        string
	owner       string
	allowOrigin string
	lockCheck   func(owner string) error
}

func newServeAPI(r scenarioRunner, log *slog.Logger, cfg serveConfig) *serveAPI {
	if cfg.allowOrigin == "" {
		cfg.allowOrigin = defaultAllowOrigin
	}
	if cfg.lockCheck == nil {
		cfg.lockCheck = requireLock
	}
	return &serveAPI{
		runner:      r,
		log:         log,
		owner:       cfg.owner,
		allowOrigin: cfg.allowOrigin,
		hosts:       loopbackHosts(cfg.addr),
		lockCheck:   cfg.lockCheck,
	}
}

func (a *serveAPI) routes() http.Handler {
	r := chi.NewRouter()
	r.Use(a.security)
	r.Get("/scenarios", a.handleScenarios)
	r.Post("/break/{name}", a.handleRun("break", a.runner.breakIt))
	r.Post("/fix/{name}", a.handleRun("fix", a.runner.fixIt))
	return r
}

// security runs in front of every route and rejects requests that did not come
// from the loopback dev machine, so a page the developer happens to have open
// cannot drive or read the API:
//
//   - the Host header must be one of 127.0.0.1:<port>, [::1]:<port> or
//     localhost:<port> (a DNS-rebinding page sets Host to the attacker's
//     domain, so this blocks it even though the TCP connection lands here);
//   - an OPTIONS preflight is answered only for the allowed Origin, so a
//     cross-site request that needs a preflight fails here;
//   - a real request with a present Origin is answered only for the allowed
//     Origin, and only that Origin gets the CORS headers;
//   - a POST must carry a non-simple marker (Content-Type: application/json or
//     X-Piperoom-Dev: 1) so a cross-site "simple" request cannot skip the
//     preflight.
func (a *serveAPI) security(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.hosts[r.Host] {
			writeServeJSON(w, http.StatusForbidden, serveStatus{Status: "error", Error: "devctl serve: forbidden Host"})
			return
		}
		origin := r.Header.Get("Origin")
		if r.Method == http.MethodOptions {
			if origin != a.allowOrigin {
				writeServeJSON(w, http.StatusForbidden, serveStatus{Status: "error", Error: "devctl serve: forbidden preflight Origin"})
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", a.allowOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Piperoom-Dev")
			w.Header().Set("Vary", "Origin")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if origin != "" {
			if origin != a.allowOrigin {
				writeServeJSON(w, http.StatusForbidden, serveStatus{Status: "error", Error: "devctl serve: forbidden Origin"})
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", a.allowOrigin)
			w.Header().Set("Vary", "Origin")
		}
		if r.Method == http.MethodPost && !hasPostMarker(r) {
			writeServeJSON(w, http.StatusForbidden, serveStatus{Status: "error", Error: "devctl serve: POST requires Content-Type: application/json or X-Piperoom-Dev: 1"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// hasPostMarker reports whether a POST carries one of the non-simple markers
// that force a cross-site request through a CORS preflight.
func hasPostMarker(r *http.Request) bool {
	if strings.EqualFold(r.Header.Get("X-Piperoom-Dev"), "1") {
		return true
	}
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && mt == "application/json"
}

// loopbackHosts returns the set of Host header values the serve API accepts for
// a listen address: the three loopback spellings, each with the same port.
func loopbackHosts(addr string) map[string]bool {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return map[string]bool{}
	}
	return map[string]bool{
		"127.0.0.1:" + port: true,
		"[::1]:" + port:     true,
		"localhost:" + port: true,
	}
}

// handleScenarios lists every scenario the API can break or fix, from the
// package scenarios map (dev/scenarios.md documents what each one does).
func (a *serveAPI) handleScenarios(w http.ResponseWriter, _ *http.Request) {
	writeServeJSON(w, http.StatusOK, scenariosResponse{Scenarios: scenarioNames()})
}

// handleRun runs one break/fix for the scenario named in the URL. It is the
// shared body of the two mutation routes: lookup (404 for an unknown scenario),
// lock check (423 when the caller no longer holds the stack lock), single-flight
// (409 while another run is in progress), then run (500 on failure, 200 on
// success).
func (a *serveAPI) handleRun(op string, run func(context.Context, string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := chi.URLParam(r, "name")
		if _, ok := scenarios[name]; !ok {
			writeServeJSON(w, http.StatusNotFound, serveStatus{Status: "error", Scenario: name, Action: op, Error: "unknown scenario " + name})
			return
		}
		// The lock can be stolen after 15m, so re-check it on every run rather
		// than only once at startup.
		if err := a.lockCheck(a.owner); err != nil {
			writeServeJSON(w, http.StatusLocked, serveStatus{Status: "error", Scenario: name, Action: op, Error: err.Error()})
			return
		}
		if !a.mu.TryLock() {
			writeServeJSON(w, http.StatusConflict, serveStatus{Status: "busy"})
			return
		}
		defer a.mu.Unlock()
		// Run on a timeout derived from the request but not cancelled by it, so
		// a browser tab closing mid-run cannot leave a break half-applied.
		runCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), serveRunTimeout)
		defer cancel()
		if err := run(runCtx, name); err != nil {
			a.log.Error("devctl serve "+op, "scenario", name, "err", err)
			writeServeJSON(w, http.StatusInternalServerError, serveStatus{Status: "error", Scenario: name, Action: op, Error: err.Error()})
			return
		}
		writeServeJSON(w, http.StatusOK, serveStatus{Status: "ok", Scenario: name, Action: op})
	}
}

// writeServeJSON writes v as a JSON body with the given status code. A status
// value never fails to marshal, but if it somehow did the code is still reported.
func writeServeJSON(w http.ResponseWriter, code int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, `{"status":"error","error":"internal"}`, code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(b)
}

// parseLoopbackAddr validates addr as host:port whose host is a loopback IP
// literal. The serve API has no auth, so it must never bind to an address a
// remote host can reach; hostnames (even "localhost") and empty hosts are
// refused because they can resolve to a non-loopback address.
func parseLoopbackAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("devctl: serve: bad -addr %q: %w", addr, err)
	}
	if port == "" {
		return "", fmt.Errorf("devctl: serve: -addr %q has no port", addr)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return "", fmt.Errorf("devctl: serve: -addr %q is not loopback; the serve API has no auth and must stay on 127.0.0.1 or ::1", addr)
	}
	return net.JoinHostPort(host, port), nil
}

// serveCmd implements `devctl serve`. It binds a loopback-only HTTP API that the
// dev-only "Break something" web menu (A6) calls to break or fix a scenario.
// Like every other mutating command, it requires the shared-stack lock (--owner
// or PIPEROOM_STACK_OWNER) and refuses to start without it.
func serveCmd(ctx context.Context, st *stack, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addr := fs.String("addr", serveDefaultAddr, "listen address (loopback only)")
	allowOrigin := fs.String("allow-origin", defaultAllowOrigin, "allowed Origin for the dev web menu (CORS)")
	owner := fs.String("owner", os.Getenv(ownerEnv), "stack lock owner tag")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: devctl serve [-addr %s] [-allow-origin URL] [--owner <tag>]", serveDefaultAddr)
	}
	hostPort, err := parseLoopbackAddr(*addr)
	if err != nil {
		return err
	}
	if err := requireLock(*owner); err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              hostPort,
		Handler:           newServeAPI(stackRunner{st}, st.log, serveConfig{addr: hostPort, owner: *owner, allowOrigin: *allowOrigin}).routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return serveAndWait(ctx, srv, st.log)
}

// serveAndWait listens on srv.Addr and serves until ctx is done, then shuts down
// gracefully. A bind error (e.g. the port is taken) is returned to the caller;
// a clean shutdown returns nil.
func serveAndWait(ctx context.Context, srv *http.Server, log *slog.Logger) error {
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("devctl: serve: listen %s: %w", srv.Addr, err)
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("devctl serve listening (loopback only, no auth)", "addr", srv.Addr)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	select {
	case <-ctx.Done():
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(stopCtx); err != nil {
			return fmt.Errorf("devctl: serve: shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		return fmt.Errorf("devctl: serve: %w", err)
	}
}
