package httpapi

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
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

// Alert intake limits. Verified alerts per source per window: each one may
// open a room and start an agent run, so a leaked secret or a misconfigured
// sender must not open rooms without bound. Generous for real senders, which
// group alerts (Alertmanager group_interval is minutes).
const (
	alertRateLimit  = 60
	alertRateWindow = time.Minute
)

// WithAlertRateLimit sets how many verified alerts one source may send per
// window before the route answers 429 rate_limited (default 60 per minute).
func WithAlertRateLimit(n int, window time.Duration) Option {
	return func(s *Server) { s.alertLimit = newRateLimiter(n, window) }
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
	// Everything the headers alone can settle (a disabled source, a wrong
	// token, a missing, malformed or stale signature header) is refused
	// before the body is read, so a body nobody signed costs no read.
	if err := s.alerts.PreVerify(src, r.Header); err != nil {
		s.refuseWarn.refused(s.log, src, r, err.Error())
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

	// Only verified alerts count against the source's budget, so forged
	// requests cannot lock the real sender out.
	now := s.alerts.Now
	if now == nil {
		now = time.Now
	}
	at := now()
	if !s.alertLimit.allow(string(src), at) {
		s.refuseWarn.refused(s.log, src, r, "rate limited")
		w.Header().Set("Retry-After", strconv.Itoa(int(s.alertLimit.window.Seconds())))
		writeError(w, http.StatusTooManyRequests, "rate_limited", "too many alerts from this source")
		return
	}
	// A verified request whose exact bytes (body and signed time) were already
	// processed is a copy: a replay of a captured request, or a bearer
	// sender's burst. It is acknowledged and dropped, without touching the
	// room or returning its content.
	key := s.alerts.DeliveryKey(src, r.Header, body)
	if s.alertSeen.Seen(key, s.alerts.ReplayWindow(src), at) {
		writeJSON(w, http.StatusAccepted, map[string]any{"ignored": true, "reason": "duplicate delivery"})
		return
	}

	snap, created, err := s.svc.OpenAlertRoom(r.Context(), a)
	if err != nil && !errors.Is(err, room.ErrResolvedAlertNoRoom) {
		// Not processed: the sender's retry of the same bytes must not be
		// taken for a copy.
		s.alertSeen.Forget(key)
	}
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
