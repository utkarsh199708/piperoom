// Package httpapi serves the REST API of docs/api.md. Handlers are thin: they
// authenticate, parse and validate the request, call a Service and write
// JSON. All room rules live in the Service.
//
// The WebSocket route is delegated to Config.WS (package ws) and does its own
// authentication, because browsers cannot set headers on a WebSocket.
package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

// Defaults for Config.
const (
	// DefaultRequestTimeout bounds every request except hand-offs.
	DefaultRequestTimeout = 60 * time.Second
	// DefaultHandoffTimeout bounds POST /api/rooms/{id}/handoffs. The server
	// asks the agent for a summary, which can take up to 3 minutes
	// (docs/api.md), so the route is exempt from DefaultRequestTimeout.
	DefaultHandoffTimeout = 4 * time.Minute
	// DefaultAllowedOrigin is the Vite dev server.
	DefaultAllowedOrigin = "http://localhost:5173"
)

// Config wires the handler. Service is required.
type Config struct {
	Service Service
	// Verifier checks alert webhooks. When nil, every webhook request is
	// refused with 401 (fail closed), like a source with no secret.
	Verifier Verifier
	// WS serves GET /api/rooms/{id}/ws. It authenticates ?user= itself. When
	// nil, that route answers 404.
	WS     http.Handler
	Logger *slog.Logger
	// AllowedOrigins are the browser origins (scheme://host:port) that may
	// call the API; matching is exact. Default: DefaultAllowedOrigin only.
	AllowedOrigins []string
	// RequestTimeout defaults to DefaultRequestTimeout, HandoffTimeout to
	// DefaultHandoffTimeout. The http.Server's WriteTimeout must be longer
	// than HandoffTimeout or it cuts the hand-off short.
	RequestTimeout time.Duration
	HandoffTimeout time.Duration
	// Ready backs GET /readyz. When nil, the server always reports ready
	// (the in-memory store has nothing to check).
	Ready Pinger
	// ProbeTimeout bounds the Ready check; default DefaultProbeTimeout.
	ProbeTimeout time.Duration
	// OnNotReady, when set, is called after each failed readiness check with
	// the fixed reason and the underlying error, so the caller can log it
	// (rate-limited). The error never reaches the HTTP response.
	OnNotReady func(reason string, err error)
}

type api struct {
	svc Service
	ver Verifier
	log *slog.Logger

	ready        Pinger
	probeTimeout time.Duration
	onNotReady   func(reason string, err error)
}

// New returns the router for docs/api.md.
func New(cfg Config) http.Handler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if len(cfg.AllowedOrigins) == 0 {
		cfg.AllowedOrigins = []string{DefaultAllowedOrigin}
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.HandoffTimeout <= 0 {
		cfg.HandoffTimeout = DefaultHandoffTimeout
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = DefaultProbeTimeout
	}
	a := &api{
		svc: cfg.Service, ver: cfg.Verifier, log: cfg.Logger,
		ready: cfg.Ready, probeTimeout: cfg.ProbeTimeout, onNotReady: cfg.OnNotReady,
	}
	timeout := timeoutMiddleware(cfg.RequestTimeout)

	r := chi.NewRouter()
	// Order matters: the logger sees the final status (including a recovered
	// panic), and CORS answers before any authentication.
	r.Use(requestLogger(cfg.Logger), recoverer(cfg.Logger), corsMiddleware(cfg.AllowedOrigins))
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, codeNotFound, "No such endpoint.")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, codeMethod, "That method is not allowed here.")
	})

	// Liveness and readiness probes: no X-User (a probe has no user) and not
	// under the 60 s request timeout; readyz bounds its own check.
	r.Get(pathHealthz, a.healthz)
	r.Get(pathReadyz, a.readyz)

	// WebSocket: no X-User (the handler checks ?user=) and no request
	// timeout (the connection lives for as long as the tab is open).
	r.Get("/api/rooms/{id}/ws", func(w http.ResponseWriter, req *http.Request) {
		if cfg.WS == nil {
			writeError(w, http.StatusNotFound, codeNotFound, "WebSocket is not enabled.")
			return
		}
		cfg.WS.ServeHTTP(w, req)
	})

	// Webhooks: authenticated by the sender's signature or token, not X-User.
	r.Group(func(r chi.Router) {
		r.Use(timeout)
		r.Post("/api/alerts/webhook", a.webhook)
		r.Post("/api/alerts/{source}", a.webhook)
	})

	// Everything else needs a known X-User.
	r.Group(func(r chi.Router) {
		r.Use(a.auth, bodyLimit)

		r.With(timeout).Group(func(r chi.Router) {
			r.Get("/api/me", a.me)
			r.Get("/api/rooms", a.listRooms)
			r.Post("/api/rooms", a.createRoom)
			r.Get("/api/rooms/{id}", a.getRoom)
			r.Get("/api/rooms/{id}/events", a.listEvents)
			r.Post("/api/rooms/{id}/comments", a.comment)
			r.Post("/api/rooms/{id}/steer", a.steer)
			r.Post("/api/rooms/{id}/suggestions/{sid}/send", a.sendSuggestion)
			r.Post("/api/rooms/{id}/control/request", a.requestControl)
			r.Post("/api/rooms/{id}/control/grant", a.grantControl)
			r.Post("/api/rooms/{id}/close", a.closeRoom)
			r.Post("/api/rooms/{id}/trace", a.trace)
			r.Post("/api/proposals/{pid}/approve", a.approve)
			r.Post("/api/proposals/{pid}/reject", a.reject)
			r.Post("/api/handoffs/{hid}/accept", a.acceptHandoff)
			r.Get("/api/pipelines/{pid}/topology", a.topology)
		})

		r.With(timeoutMiddleware(cfg.HandoffTimeout)).Post("/api/rooms/{id}/handoffs", a.createHandoff)
	})

	return r
}
