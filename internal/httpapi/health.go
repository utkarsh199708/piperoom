package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// DefaultProbeTimeout bounds GET /readyz. Orchestrators usually give a probe a
// few seconds, so the store check must answer well inside that.
const DefaultProbeTimeout = 2 * time.Second

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

type healthBody struct {
	Status string `json:"status"`
}

type readyBody struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

func isProbePath(p string) bool { return p == pathHealthz || p == pathReadyz }

// healthz answers as long as the process serves HTTP; it checks nothing else,
// so a slow database never gets the process restarted.
func (a *api) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, a.log, http.StatusOK, healthBody{Status: "ok"})
}

// readyz reports whether the store answers within the probe timeout.
func (a *api) readyz(w http.ResponseWriter, r *http.Request) {
	if a.ready == nil {
		writeJSON(w, a.log, http.StatusOK, readyBody{Status: "ready", Checks: map[string]string{"store": reasonOK}})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.probeTimeout)
	defer cancel()
	err := a.ready.Ping(ctx)
	if err == nil {
		writeJSON(w, a.log, http.StatusOK, readyBody{Status: "ready", Checks: map[string]string{"store": reasonOK}})
		return
	}
	reason := reasonUnreachable
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		reason = reasonTimeout
	}
	if a.onNotReady != nil {
		a.onNotReady(reason, err)
	}
	writeJSON(w, a.log, http.StatusServiceUnavailable, readyBody{Status: "not_ready", Checks: map[string]string{"store": reason}})
}
