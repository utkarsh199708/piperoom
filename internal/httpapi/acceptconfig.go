package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// ConfigAccepter moves a connector's config_drift baseline to its current
// config (X3b): cmd/server adapts the watcher to it. AcceptConfig returns the
// old baseline's hash ("" when there was none) and the new one's; it returns
// an error wrapping ErrWatchOff when this server keeps no baseline, and
// ErrConnectUnavailable when Kafka Connect cannot be read (the baseline is
// then unchanged).
type ConfigAccepter interface {
	AcceptConfig(ctx context.Context, pipelineID, connector string) (oldHash, newHash string, err error)
}

// Errors a ConfigAccepter returns (wrapped is fine).
var (
	// ErrWatchOff: the watcher is off, so there is no baseline to move
	// (409 watch_off).
	ErrWatchOff = errors.New("httpapi: the watcher is off")
	// ErrConnectUnavailable: Kafka Connect could not be read in time
	// (503 connect_unavailable).
	ErrConnectUnavailable = errors.New("httpapi: kafka connect cannot be read")
)

// WithConfigAccept serves POST /api/pipelines/{pid}/connectors/{name}/accept-config
// through a (X3b). Without it the route answers 409 watch_off after its
// visibility and role checks, as with PIPEROOM_WATCH_MODE=off.
func WithConfigAccept(a ConfigAccepter) Option {
	return func(s *Server) { s.configAccept = a }
}

// handleAcceptConfig serves POST
// /api/pipelines/{pid}/connectors/{name}/accept-config (docs/api.md, X3):
// visibility and the kafka-connect layer owner check first (room service),
// then the watcher. It writes no room event and changes no customer system;
// the log line carries the caller, the pipeline, the connector and both
// hashes, never a config value.
func (s *Server) handleAcceptConfig(w http.ResponseWriter, r *http.Request) {
	pid, name := chi.URLParam(r, "pid"), chi.URLParam(r, "name")
	u := userFrom(r.Context())
	if err := s.svc.AuthorizeAcceptConfig(u, pid, name); err != nil {
		s.writeErr(w, err)
		return
	}
	if s.configAccept == nil {
		writeError(w, http.StatusConflict, "watch_off", "the watcher is off (PIPEROOM_WATCH_MODE=off): there is no baseline to move")
		return
	}
	oldHash, newHash, err := s.configAccept.AcceptConfig(r.Context(), pid, name)
	switch {
	case errors.Is(err, ErrWatchOff):
		writeError(w, http.StatusConflict, "watch_off", "the watcher keeps no baseline for this connector here (it is off, does not watch the connector, or runs on another server replica)")
		return
	case errors.Is(err, ErrConnectUnavailable):
		s.log.Warn("http: accept connector config: kafka connect cannot be read", "user", u.ID, "pipeline", pid, "connector", name)
		writeError(w, http.StatusServiceUnavailable, "connect_unavailable", "Kafka Connect could not be read within 10 s; the approved config is unchanged")
		return
	case err != nil:
		s.writeErr(w, err)
		return
	}
	s.log.Info("http: connector config accepted as the approved baseline",
		"user", u.ID, "pipeline", pid, "connector", name, "old_hash", oldHash, "new_hash", newHash)
	writeJSON(w, http.StatusOK, map[string]string{"connector": name, "hash": newHash})
}
