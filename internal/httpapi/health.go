package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"piperoom/internal/version"
)

// DefaultProbeTimeout bounds GET /readyz. Orchestrators usually give a probe a
// few seconds, so the store check must answer well inside that.
const DefaultProbeTimeout = 2 * time.Second

// readyWarnEvery limits readiness-failure logs: probes run every few seconds
// and a down database would otherwise flood the log.
const readyWarnEvery = time.Minute

// Probe paths. They sit outside /api: a probe carries no X-User.
const (
	pathHealthz = "/healthz"
	pathReadyz  = "/readyz"
)

// Fixed readiness reasons. The body never carries the raw error, which can
// name hosts, users or other connection details.
const (
	reasonOK          = "ok"
	reasonTimeout     = "timeout"
	reasonUnreachable = "unreachable"
)

// Pinger is the optional readiness check for the store. Ping must honour ctx.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Option configures optional Server behaviour.
type Option func(*Server)

// WithReady sets the store check behind GET /readyz. Without it (or with
// nil) /readyz always reports ready, which suits the in-memory demo store.
func WithReady(p Pinger) Option {
	return func(s *Server) { s.ready = p }
}

// WithProbeTimeout overrides DefaultProbeTimeout for GET /readyz.
func WithProbeTimeout(d time.Duration) Option {
	return func(s *Server) {
		if d > 0 {
			s.probeTimeout = d
		}
	}
}

// WithClock sets the clock used to rate-limit readiness-failure logs, so
// tests can move time without sleeping.
func WithClock(now func() time.Time) Option {
	return func(s *Server) {
		if now != nil {
			s.readyWarn.now = now
			s.refuseWarn.now = now
		}
	}
}

// isProbePath covers the scrape path too: like the probes it is hit every few
// seconds, often by pod IP, and its body holds only aggregate counts.
func isProbePath(p string) bool { return p == pathHealthz || p == pathReadyz || p == pathMetrics }

type healthBody struct {
	Status string `json:"status"`
	// Version is the linked build version (internal/version), "dev" for a
	// plain go build: it names a build, not a secret (docs/api.md).
	Version string `json:"version"`
	// WriteMode is "on" when fully approved proposals run, "off" when they
	// are recorded and never run (PIPEROOM_WRITE_MODE, P0 item 1), so a
	// probe or an operator can tell a read-only install from outside.
	WriteMode string `json:"write_mode"`
	// Demo is true only on a --demo server with the scripted agent's
	// presenter controls (KAN-398). The web UI shows its dev-only control
	// strip on it; left out everywhere else.
	Demo bool `json:"demo,omitempty"`
}

type readyBody struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

// handleHealthz answers as long as the process serves HTTP; it checks nothing
// else, so a slow database never gets the process restarted.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	mode := "off"
	if s.svc.WriteOn() {
		mode = "on"
	}
	writeJSON(w, http.StatusOK, healthBody{Status: "ok", Version: version.String(), WriteMode: mode, Demo: s.demo != nil})
}

// handleReadyz reports whether the store answers within the probe timeout.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.ready == nil {
		writeJSON(w, http.StatusOK, readyBody{Status: "ready", Checks: map[string]string{"store": reasonOK}})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.probeTimeout)
	defer cancel()
	err := s.ready.Ping(ctx)
	if err == nil {
		writeJSON(w, http.StatusOK, readyBody{Status: "ready", Checks: map[string]string{"store": reasonOK}})
		return
	}
	reason := reasonUnreachable
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		reason = reasonTimeout
	}
	s.readyWarn.notReady(s.log, reason, err)
	writeJSON(w, http.StatusServiceUnavailable, readyBody{Status: "not_ready", Checks: map[string]string{"store": reason}})
}

// readyWarner logs failed readiness checks at Warn, at most once per
// interval, and reports how many failures it held back in between.
type readyWarner struct {
	every time.Duration
	now   func() time.Time

	mu         sync.Mutex
	last       time.Time
	suppressed int
}

func (l *readyWarner) notReady(log *slog.Logger, reason string, err error) {
	l.mu.Lock()
	t := l.now()
	if !l.last.IsZero() && t.Sub(l.last) < l.every {
		l.suppressed++
		l.mu.Unlock()
		return
	}
	suppressed := l.suppressed
	l.last, l.suppressed = t, 0
	l.mu.Unlock()
	log.Warn("http: readiness check failed", "check", "store", "reason", reason, "err", err, "suppressed", suppressed)
}
