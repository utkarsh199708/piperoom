package httpapi

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/alertin"
	"piperoom/internal/room"
)

// WithAlertIngest sets the verification config behind POST
// /api/alerts/{source}. A source whose secret is empty is disabled and its
// route answers 401; without this option every source is disabled.
func WithAlertIngest(cfg alertin.Config) Option {
	return func(s *Server) { s.alerts = cfg }
}

// WithChangesWebhook mounts POST /api/changes/webhook (X4, alertin.ChangeWebhook):
// signed like the generic alert route, so it sits outside the session group.
func WithChangesWebhook(h http.Handler) Option {
	return func(s *Server) { s.changes = h }
}

// handleSignedAlert is POST /api/alerts/{source}: the sender's signature or
// token (checked by alertin) is the auth, so there is no X-User. The body is
// read up to alertin.MaxBodyBytes, verified and parsed in one alertin call,
// then opens or joins a room through the same service entry point as the dev
// webhook, so dedup and resolved handling are identical.
func (s *Server) handleSignedAlert(w http.ResponseWriter, r *http.Request) {
	src := alertin.Source(chi.URLParam(r, "source"))
	if !slices.Contains(alertin.Sources, src) {
		writeError(w, http.StatusNotFound, "not_found", "unknown alert source")
		return
	}
	// A source with no secret is refused before the body is read.
	if !s.alerts.Enabled(src) {
		s.refuseWarn.refused(s.log, src, r, "source is not configured")
		writeError(w, http.StatusUnauthorized, "unauthenticated", "alert authentication failed")
		return
	}
	// One byte over the limit, so a body that is too large is seen by Ingest
	// (ErrTooLarge) rather than silently cut. A longer body fails the read.
	r.Body = http.MaxBytesReader(w, r.Body, alertin.MaxBodyBytes+1)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "too_large", "alert body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "validation", "unreadable request body")
		return
	}

	a, err := s.alerts.Ingest(src, r.Header, body)
	switch {
	case errors.Is(err, alertin.ErrIgnored):
		writeJSON(w, http.StatusAccepted, map[string]any{"ignored": true, "reason": err.Error()})
		return
	case errors.Is(err, alertin.ErrUnauthenticated), errors.Is(err, alertin.ErrStale):
		// The cause is logged for the operator; the client only learns that the
		// request was refused (alertin errors never carry secrets).
		s.refuseWarn.refused(s.log, src, r, err.Error())
		msg := "alert authentication failed"
		if errors.Is(err, alertin.ErrStale) {
			msg = "alert event too old"
		}
		writeError(w, http.StatusUnauthorized, "unauthenticated", msg)
		return
	case errors.Is(err, alertin.ErrTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "too_large", "alert body too large")
		return
	case err != nil:
		s.log.Warn("alerts: bad payload", "source", src, "reason", err.Error())
		writeError(w, http.StatusBadRequest, "validation", "invalid alert payload")
		return
	}

	snap, created, err := s.svc.OpenAlertRoom(r.Context(), a)
	s.writeSignedAlertRoom(w, snap, created, err)
}

// writeAlertRoom answers the result of room.Service.OpenAlertRoom /
// CreateRoomFromAlert for both webhook routes: a resolved notice with no open
// room is accepted and dropped (202, nothing stored); a new room is 201; a
// repeat that joined the open room is 200 with that room (docs/api.md).
func (s *Server) writeAlertRoom(w http.ResponseWriter, snap room.RoomSnapshot, created bool, err error) {
	if errors.Is(err, room.ErrResolvedAlertNoRoom) {
		writeJSON(w, http.StatusAccepted, map[string]any{
			"ignored": true,
			"reason":  room.ErrResolvedAlertNoRoom.Error(),
		})
		return
	}
	if err != nil {
		s.writeErr(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, snap)
}

// refuseWarnEvery limits refused-alert logs per source: anyone who can reach
// the port can make them, so they must not flood the log.
const refuseWarnEvery = time.Minute

// refuseWarner logs refused signed-alert requests at Warn, at most once per
// source per interval; the rest go to Debug.
type refuseWarner struct {
	every time.Duration
	now   func() time.Time

	mu   sync.Mutex
	last map[alertin.Source]time.Time
}

func (l *refuseWarner) refused(log *slog.Logger, src alertin.Source, r *http.Request, reason string) {
	level := slog.LevelDebug
	l.mu.Lock()
	if l.last == nil {
		l.last = map[alertin.Source]time.Time{}
	}
	if t := l.now(); t.Sub(l.last[src]) >= l.every {
		l.last[src] = t
		level = slog.LevelWarn
	}
	l.mu.Unlock()
	log.Log(r.Context(), level, "alerts: refused", "source", src, "remote", r.RemoteAddr, "reason", reason)
}
