package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
)

// serveDefaultAddr is where the dev-only break/fix API listens. The API has no
// authentication, so it must never leave loopback: parseLoopbackAddr enforces
// that before net.Listen, and the Dockerfile only builds cmd/server, never
// devctl (see dev/README.md).
const serveDefaultAddr = "127.0.0.1:7071"

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
// no authentication, so it must only ever listen on a loopback address. It also
// deliberately skips the shared-stack lock: a single local developer drives it,
// and the routes are what `devctl break`/`fix` do minus the lock ceremony.
type serveAPI struct {
	runner scenarioRunner
	log    *slog.Logger
	// mu serializes break/fix runs. Only one may run at a time; a second
	// request while one is in flight gets a 409 (TryLock fails) rather than
	// racing the stack.
	mu sync.Mutex
}

func newServeAPI(r scenarioRunner, log *slog.Logger) *serveAPI {
	return &serveAPI{runner: r, log: log}
}

func (a *serveAPI) routes() http.Handler {
	r := chi.NewRouter()
	r.Get("/scenarios", a.handleScenarios)
	r.Post("/break/{name}", a.handleRun("break", a.runner.breakIt))
	r.Post("/fix/{name}", a.handleRun("fix", a.runner.fixIt))
	return r
}

// handleScenarios lists every scenario the API can break or fix, from the
// package scenarios map (dev/scenarios.md documents what each one does).
func (a *serveAPI) handleScenarios(w http.ResponseWriter, _ *http.Request) {
	writeServeJSON(w, http.StatusOK, scenariosResponse{Scenarios: scenarioNames()})
}

// handleRun runs one break/fix for the scenario named in the URL. It is the
// shared body of the two mutation routes: lookup (404 for an unknown scenario),
// single-flight (409 while another run is in progress), then run (500 on
// failure, 200 on success).
func (a *serveAPI) handleRun(op string, run func(context.Context, string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := chi.URLParam(r, "name")
		if _, ok := scenarios[name]; !ok {
			writeServeJSON(w, http.StatusNotFound, serveStatus{Status: "error", Scenario: name, Action: op, Error: "unknown scenario " + name})
			return
		}
		if !a.mu.TryLock() {
			writeServeJSON(w, http.StatusConflict, serveStatus{Status: "busy"})
			return
		}
		defer a.mu.Unlock()
		if err := run(r.Context(), name); err != nil {
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
func serveCmd(ctx context.Context, st *stack, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addr := fs.String("addr", serveDefaultAddr, "listen address (loopback only)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: devctl serve [-addr %s]", serveDefaultAddr)
	}
	hostPort, err := parseLoopbackAddr(*addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              hostPort,
		Handler:           newServeAPI(stackRunner{st}, st.log).routes(),
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
