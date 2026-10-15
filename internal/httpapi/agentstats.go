package httpapi

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/room"
)

// Bounds of ?days= on GET /api/pipelines/{pid}/agent-stats.
const (
	agentStatsDefaultDays = 90
	agentStatsMaxDays     = 365
)

// handleAgentStats serves GET /api/pipelines/{pid}/agent-stats?days=90 (F11).
// Visibility first: a pipeline owners.yaml does not define, or one outside the
// caller's tenants, is 404 before anything else. Then any logged-in user,
// viewers included: the answer is numbers only, like GET /api/shadow/metrics.
func (s *Server) handleAgentStats(w http.ResponseWriter, r *http.Request) {
	pid := chi.URLParam(r, "pid")
	if !s.svc.UsagePipelineVisible(userFrom(r.Context()), pid) {
		writeError(w, http.StatusNotFound, "not_found", "pipeline not found")
		return
	}
	days := agentStatsDefaultDays
	if v := r.URL.Query().Get("days"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > agentStatsMaxDays {
			s.writeErr(w, validation("days must be a whole number from 1 to 365"))
			return
		}
		days = n
	}
	st, err := s.svc.AgentStats(r.Context(), pid, days)
	if errors.Is(err, room.ErrAgentStatsUnsupported) {
		// A store that cannot list closed rooms has no track record to show.
		s.writeErr(w, errNotFound)
		return
	}
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}
