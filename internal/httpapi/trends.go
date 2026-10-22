package httpapi

import (
	"context"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/clients"
)

// Bounds of ?days= on GET /api/pipelines/{pid}/trends (samples are kept 7 days).
const (
	trendsDefaultDays = 1
	trendsMaxDays     = 7
	// trendsReadTimeout is the store read deadline of docs/api.md.
	trendsReadTimeout = 10 * time.Second
)

// WithSamples serves GET /api/pipelines/{pid}/trends from r (X3a; the room
// store's watch_samples). Without it the route answers [] for every valid
// request, as for a pipeline the watcher has not sampled yet.
func WithSamples(r clients.SampleReader) Option {
	return func(s *Server) { s.samples = r }
}

// handleTrends serves GET /api/pipelines/{pid}/trends?metric=&days= (X3a).
// Visibility first: an unknown pipeline and one outside the caller's tenants
// are both 404 (never 403, so another tenant's pipeline id is not
// confirmed); then any logged-in user, viewers included. Numbers only, from
// Piperoom's own table: no call reaches a customer system.
func (s *Server) handleTrends(w http.ResponseWriter, r *http.Request) {
	pid := chi.URLParam(r, "pid")
	if !s.svc.UsagePipelineVisible(userFrom(r.Context()), pid) {
		writeError(w, http.StatusNotFound, "not_found", "pipeline not found")
		return
	}
	q := r.URL.Query()
	metric := q.Get("metric")
	if !slices.Contains(clients.WatchMetrics(), metric) {
		s.writeErr(w, validation("metric must be one of "+strings.Join(clients.WatchMetrics(), ", ")))
		return
	}
	days := trendsDefaultDays
	if v := q.Get("days"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > trendsMaxDays {
			s.writeErr(w, validation("days must be a whole number from 1 to 7"))
			return
		}
		days = n
	}
	if s.samples == nil {
		writeJSON(w, http.StatusOK, []clients.SamplePoint{})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), trendsReadTimeout)
	defer cancel()
	since := s.now().UTC().Add(-time.Duration(days) * 24 * time.Hour)
	pts, err := s.samples.Samples(ctx, pid, metric, since)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	if pts == nil {
		pts = []clients.SamplePoint{}
	}
	writeJSON(w, http.StatusOK, pts)
}
