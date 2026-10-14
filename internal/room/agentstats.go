package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Bounds of one agent-stats computation (F11): at most agentStatsMaxRooms rooms,
// agentStatsMaxEvents events per read, so a busy pipeline costs a fixed amount.
const (
	agentStatsMaxRooms  = 500
	agentStatsMaxEvents = 50
	agentStatsTTL       = 2 * time.Minute
	// AgentStatsEnoughGraded is the number of graded rooms under which clients
	// show counts and never a rate.
	AgentStatsEnoughGraded = 10
)

// AgentStatsBucket is one by_confidence row; the names are those of
// evals.ReliabilityBucket (without probability) so the eval report and the room
// agree.
type AgentStatsBucket struct {
	Confidence string  `json:"confidence"`
	Runs       int     `json:"runs"`
	Correct    int     `json:"correct"`
	Accuracy   float64 `json:"accuracy"`
}

// AgentStatsShadow is the shadow subset of AgentStats.
type AgentStatsShadow struct {
	Rooms        int `json:"rooms"`
	Graded       int `json:"graded"`
	LayerCorrect int `json:"layer_correct"`
}

// AgentStats is the body of GET /api/pipelines/{pid}/agent-stats (docs/api.md).
// Numbers only: no room titles, no hypothesis text.
type AgentStats struct {
	PipelineID   string             `json:"pipeline_id"`
	WindowDays   int                `json:"window_days"`
	GeneratedAt  time.Time          `json:"generated_at"`
	Rooms        int                `json:"rooms"`
	Hypothesised int                `json:"hypothesised"`
	Labelled     int                `json:"labelled"`
	Graded       int                `json:"graded"`
	LayerCorrect int                `json:"layer_correct"`
	RootCause    ShadowRootCause    `json:"root_cause"`
	ByConfidence []AgentStatsBucket `json:"by_confidence"`
	// Medians are whole seconds / USD, -1 when no room has the value.
	MedianTimeToFirstHypothesisS int64            `json:"median_time_to_first_hypothesis_s"`
	MedianHumanMTTRS             int64            `json:"median_human_mttr_s"`
	MedianCostUSD                float64          `json:"median_cost_usd"`
	CostAvailable                bool             `json:"cost_available"`
	EnoughData                   bool             `json:"enough_data"`
	Shadow                       AgentStatsShadow `json:"shadow"`
}

type agentStatsKey struct {
	pipeline string
	days     int
}

type agentStatsEntry struct {
	at    time.Time
	stats AgentStats
}

// agentStatsCache holds computed stats per (pipeline, days) for agentStatsTTL.
type agentStatsCache struct {
	mu sync.Mutex
	m  map[agentStatsKey]agentStatsEntry
}

// ErrAgentStatsUnsupported means the store cannot list closed rooms.
var ErrAgentStatsUnsupported = errors.New("room: store cannot list closed rooms")

// AgentStats returns the agent's track record on one pipeline over the closed
// rooms of the last days days. The caller has already checked visibility. It
// is cached for two minutes per (pipeline, days) under a mutex, which also
// stops concurrent callers from running the bounded computation twice.
func (s *Service) AgentStats(ctx context.Context, pipelineID string, days int) (AgentStats, error) {
	if days < 1 {
		return AgentStats{}, validation("days must be 1 to 365")
	}
	key := agentStatsKey{pipelineID, days}
	s.statsCache.mu.Lock()
	defer s.statsCache.mu.Unlock()
	now := s.now()
	if e, ok := s.statsCache.m[key]; ok && now.Sub(e.at) < agentStatsTTL {
		return e.stats, nil
	}
	st, err := s.computeAgentStats(ctx, pipelineID, days, now)
	if err != nil {
		return AgentStats{}, err
	}
	if s.statsCache.m == nil {
		s.statsCache.m = map[agentStatsKey]agentStatsEntry{}
	}
	s.statsCache.m[key] = agentStatsEntry{at: now, stats: st}
	return st, nil
}

// statsRoomEvents are the typed events read per room. agent.usage is read
// separately so a long cost log cannot push the hypothesis out of the limit.
var statsRoomEvents = []events.Type{
	events.RoomCreated, events.AgentHypothesis, events.RoomClosed,
	events.ShadowLabelled, events.ShadowGraded, events.RoomLabelled,
}

func (s *Service) computeAgentStats(ctx context.Context, pipelineID string, days int, now time.Time) (AgentStats, error) {
	rd, ok := s.store.(store.ClosedRoomReader)
	if !ok {
		return AgentStats{}, ErrAgentStatsUnsupported
	}
	since := now.Add(-time.Duration(days) * 24 * time.Hour)
	rooms, err := rd.ListClosedRooms(ctx, pipelineID, agentStatsMaxRooms)
	if err != nil {
		return AgentStats{}, fmt.Errorf("room: agent stats: list closed rooms: %w", err)
	}
	// Shadow rooms keep their agent events in the shadow log: the run row
	// carries the time to the first hypothesis and the final hypothesis.
	runs := map[string]store.ShadowRunRoom{}
	if s.shadow != nil {
		rs, err := s.shadow.ListShadowRuns(ctx, pipelineID, since)
		if err != nil {
			return AgentStats{}, fmt.Errorf("room: agent stats: shadow runs: %w", err)
		}
		for _, r := range rs {
			runs[r.RoomID] = r
		}
	}

	out := AgentStats{PipelineID: pipelineID, WindowDays: days, GeneratedAt: now.UTC(), ByConfidence: []AgentStatsBucket{}}
	buckets := map[string]*AgentStatsBucket{}
	var first, mttr []int64
	var costs []float64
	for _, r := range rooms {
		if r.ClosedAt == nil || r.ClosedAt.Before(since) {
			continue
		}
		evs, err := rd.EventsOfTypes(ctx, r.ID, statsRoomEvents, agentStatsMaxEvents)
		if err != nil {
			return AgentStats{}, fmt.Errorf("room: agent stats: events of %s: %w", r.ID, err)
		}
		f := foldStatsRoom(evs)
		out.Rooms++
		mttr = append(mttr, secondsSince(r.OpenedAt, r.ClosedAt))
		conf, hypothesised := f.confidence, f.hypothesisAt != nil
		firstAt := f.hypothesisAt
		if f.shadow {
			out.Shadow.Rooms++
			run, has := runs[r.ID]
			hypothesised, firstAt, conf = false, nil, ""
			if has {
				firstAt = run.FirstHypothesisAt
				hypothesised = firstAt != nil
				conf = shadowConfidence(run.FinalHypothesis)
			}
		}
		if hypothesised {
			out.Hypothesised++
		}
		if firstAt != nil {
			first = append(first, secondsSince(r.OpenedAt, firstAt))
		}
		if f.labelled {
			out.Labelled++
		}
		if g := f.grade; g != nil {
			out.Graded++
			if g.LayerCorrect {
				out.LayerCorrect++
			}
			switch g.RootCause {
			case "yes":
				out.RootCause.Yes++
			case "partial":
				out.RootCause.Partial++
			case "no":
				out.RootCause.No++
			}
			if f.shadow {
				out.Shadow.Graded++
				if g.LayerCorrect {
					out.Shadow.LayerCorrect++
				}
			}
			if hypothesised && conf != "" {
				b := buckets[conf]
				if b == nil {
					b = &AgentStatsBucket{Confidence: conf}
					buckets[conf] = b
				}
				b.Runs++
				if g.RootCause == "yes" {
					b.Correct++
				}
			}
		}
		if !f.shadow {
			cost, priced, err := s.statsRoomCost(ctx, rd, r.ID)
			if err != nil {
				return AgentStats{}, err
			}
			if priced {
				costs = append(costs, cost)
			}
		}
	}
	for _, c := range []string{"high", "medium", "low"} {
		if b := buckets[c]; b != nil {
			b.Accuracy = float64(b.Correct) / float64(b.Runs)
			out.ByConfidence = append(out.ByConfidence, *b)
		}
	}
	out.MedianTimeToFirstHypothesisS = median(first)
	out.MedianHumanMTTRS = median(mttr)
	out.MedianCostUSD, out.CostAvailable = medianFloat(costs)
	out.EnoughData = out.Graded >= AgentStatsEnoughGraded
	return out, nil
}

// statsRoomCost sums the room's priced agent.usage events (spend-store totals
// are the same numbers, charged per request; the events need no second store).
func (s *Service) statsRoomCost(ctx context.Context, rd store.ClosedRoomReader, roomID string) (float64, bool, error) {
	evs, err := rd.EventsOfTypes(ctx, roomID, []events.Type{events.AgentUsage}, agentStatsMaxEvents)
	if err != nil {
		return 0, false, fmt.Errorf("room: agent stats: usage of %s: %w", roomID, err)
	}
	var sum float64
	priced := false
	for _, e := range evs {
		if p, ok := decode[events.AgentUsagePayload](e); ok && p.CostUSD != nil {
			sum += *p.CostUSD
			priced = true
		}
	}
	return sum, priced, nil
}

// statsRoom is what one closed room's typed events say.
type statsRoom struct {
	shadow       bool
	hypothesisAt *time.Time
	confidence   string // of the last hypothesis
	labelled     bool
	grade        *events.ShadowGradedPayload
}

func foldStatsRoom(evs []events.Event) statsRoom {
	var f statsRoom
	for _, e := range evs {
		switch e.Type {
		case events.RoomCreated:
			if p, ok := decode[events.RoomCreatedPayload](e); ok {
				f.shadow = p.Shadow
			}
		case events.AgentHypothesis:
			if p, ok := decode[events.AgentHypothesisPayload](e); ok {
				if f.hypothesisAt == nil {
					at := e.At
					f.hypothesisAt = &at
				}
				f.confidence = p.Confidence
			}
		case events.ShadowLabelled, events.RoomLabelled:
			// Until F8 grades live rooms, only shadow.graded carries a grade.
			f.labelled = true
		case events.ShadowGraded:
			if p, ok := decode[events.ShadowGradedPayload](e); ok {
				f.grade = &p
			}
		}
	}
	return f
}

// shadowConfidence reads the confidence of a shadow run's final hypothesis.
func shadowConfidence(raw json.RawMessage) string {
	var h struct {
		Confidence string `json:"confidence"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &h) != nil {
		return ""
	}
	return h.Confidence
}

// medianFloat is the median of v and whether v was non-empty.
func medianFloat(v []float64) (float64, bool) {
	if len(v) == 0 {
		return -1, false
	}
	c := slices.Clone(v)
	slices.Sort(c)
	n := len(c)
	if n%2 == 1 {
		return c[n/2], true
	}
	return (c[n/2-1] + c[n/2]) / 2, true
}
