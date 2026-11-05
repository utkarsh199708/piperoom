package httpapi

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/room"
)

// Bounds of ?days= on GET /api/pipelines/{pid}/agent-stats.
const (
	agentStatsDefaultDays = 90
	agentStatsMaxDays     = 365
)

// infraHoldMax is how long one slot_wal_bytes sample counts at most: a longer
// gap in sampling counts nothing rather than guessing what the slot held.
const infraHoldMax = time.Hour

// agentStatsBody is the answer of agent-stats: the F11 fields unchanged, plus
// the X6 infra block (null when there is no history).
type agentStatsBody struct {
	room.AgentStats
	Infra *AgentStatsInfra `json:"infra"`
}

// AgentStatsInfra is the X6 `infra` block of agent-stats (docs/api.md):
// infrastructure cost hints from the pipeline's watch samples, in units only,
// never a currency. A metric with no sample is null, never zero.
type AgentStatsInfra struct {
	From              time.Time `json:"from"`
	To                time.Time `json:"to"`
	Samples           int       `json:"samples"`
	WALRetainedGBDays *float64  `json:"wal_retained_gb_days"`
	DLQRecords        *int64    `json:"dlq_records"`
	OldestUnreadAgeS  *int64    `json:"oldest_unread_age_s"`
}

// handleAgentStats serves GET /api/pipelines/{pid}/agent-stats?days=90 (F11,
// infra X6). Visibility first: a pipeline owners.yaml does not define, or one
// outside the caller's tenants, is 404 before anything else (and before any
// sample read). Then any logged-in user, viewers included: the answer is
// numbers only, like GET /api/shadow/metrics.
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
	infra, err := s.agentStatsInfra(r.Context(), pid)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, agentStatsBody{AgentStats: st, Infra: infra})
}

// agentStatsInfra reads the three cost metrics of one pipeline over the last
// config.WatchSampleRetention, whatever ?days= is: older samples are deleted.
// No sample reader (the watcher was never wired) is no history, not an error.
// The caller has checked visibility.
func (s *Server) agentStatsInfra(ctx context.Context, pid string) (*AgentStatsInfra, error) {
	if s.samples == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, trendsReadTimeout)
	defer cancel()
	since := s.now().UTC().Add(-config.WatchSampleRetention)
	metrics := [3]string{clients.WatchMetricSlotWALBytes, clients.WatchMetricDLQRecords, clients.WatchMetricTopicOldestUnreadAge}
	var series [3][]clients.SamplePoint
	for i, m := range metrics {
		pts, err := s.samples.Samples(ctx, pid, m, since)
		if err != nil {
			return nil, fmt.Errorf("httpapi: agent-stats infra: read %s samples: %w", m, err)
		}
		series[i] = pts
	}
	return infraFromSamples(series[0], series[1], series[2]), nil
}

// infraFromSamples builds the infra block from the samples of each metric,
// oldest first; nil when there is none of any. GB-days: each slot_wal_bytes
// sample holds until the next one, at most infraHoldMax, and the newest holds
// nothing; bytes × days over 10⁹ (GB, not GiB), to three decimals. DLQ and
// unread age are the newest sample.
func infraFromSamples(wal, dlq, age []clients.SamplePoint) *AgentStatsInfra {
	n := len(wal) + len(dlq) + len(age)
	if n == 0 {
		return nil
	}
	out := &AgentStatsInfra{Samples: n}
	for _, pts := range [][]clients.SamplePoint{wal, dlq, age} {
		for _, p := range pts {
			if out.From.IsZero() || p.At.Before(out.From) {
				out.From = p.At
			}
			if p.At.After(out.To) {
				out.To = p.At
			}
		}
	}
	out.From, out.To = out.From.UTC(), out.To.UTC()
	if len(wal) > 0 {
		var byteDays float64
		for i := 0; i+1 < len(wal); i++ {
			// An out-of-order pair holds nothing rather than subtracting.
			held := min(wal[i+1].At.Sub(wal[i].At), infraHoldMax)
			if held <= 0 || wal[i].Value <= 0 {
				continue
			}
			byteDays += wal[i].Value * held.Hours() / 24
		}
		gb := math.Round(byteDays/1e9*1000) / 1000
		out.WALRetainedGBDays = &gb
	}
	out.DLQRecords = newestWhole(dlq)
	out.OldestUnreadAgeS = newestWhole(age)
	return out
}

// newestWhole is the newest sample's value rounded to a whole number (never
// negative), or nil when there is no sample.
func newestWhole(pts []clients.SamplePoint) *int64 {
	if len(pts) == 0 {
		return nil
	}
	newest := pts[0]
	for _, p := range pts[1:] {
		if !p.At.Before(newest.At) {
			newest = p
		}
	}
	v := int64(math.Round(max(newest.Value, 0)))
	return &v
}
