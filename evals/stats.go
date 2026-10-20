package evals

import (
	"encoding/json"
	"math"
	"sort"
	"sync"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// Percentile is the p-th percentile (0..100) of xs by linear interpolation
// between the closest ranks (Hyndman-Fan type 7, the NumPy and R default).
// It is -1 when xs is empty. xs is not changed.
func Percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return -1
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return sortedPercentile(s, p)
}

// sortedPercentile is Percentile on an already sorted sample.
func sortedPercentile(s []float64, p float64) float64 {
	if len(s) == 0 {
		return -1
	}
	p = math.Max(0, math.Min(100, p))
	h := (p / 100) * float64(len(s)-1)
	lo := int(math.Floor(h))
	if lo >= len(s)-1 {
		return s[len(s)-1]
	}
	return s[lo] + (h-float64(lo))*(s[lo+1]-s[lo])
}

// Dist is the p50, p90 and p99 of a sample; each is -1 when there is no data.
type Dist struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
}

func distOf(xs []float64) Dist {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return Dist{P50: sortedPercentile(s, 50), P90: sortedPercentile(s, 90), P99: sortedPercentile(s, 99)}
}

// MeanP90 is the mean and p90 of a sample; both are -1 when there is no data.
type MeanP90 struct {
	Mean float64 `json:"mean"`
	P90  float64 `json:"p90"`
}

func meanP90(xs []float64) MeanP90 {
	if len(xs) == 0 {
		return MeanP90{Mean: -1, P90: -1}
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	return MeanP90{Mean: sum / float64(len(xs)), P90: Percentile(xs, 90)}
}

// RunUsage is the latency, tokens, cost and model routing of one run, read
// from its agent.usage events (docs/design/cost.md 5).
type RunUsage struct {
	Scenario   string `json:"scenario"`
	Run        int    `json:"run"`
	DurationMS int64  `json:"duration_ms"`
	// Turns are investigation requests (roles triage and main).
	Turns         int     `json:"turns"`
	TriageTurns   int     `json:"triage_turns"`
	MainTurns     int     `json:"main_turns"`
	TurnLatencyMS Dist    `json:"turn_latency_ms"`
	TokensPerTurn MeanP90 `json:"tokens_per_turn"`
	// Tokens counts every request of the run, compaction and hand-off too.
	Tokens int64 `json:"tokens"`
	// CostUSD is set when a price table was given and priced every request.
	CostUSD *float64 `json:"cost_usd,omitempty"`

	latencies  []float64 // per turn, for pooling across runs
	turnTokens []float64
}

// RunUsageOf reads one run's usage from its transcript. A compaction pass
// counts toward the turn it followed. With prices, the cost is computed from
// the token counts, so a runner that emits no cost is priced too; a request
// on an unpriced model leaves the run's cost unknown.
func RunUsageOf(r RunResult, tr Transcript, prices agent.Prices) RunUsage {
	u := RunUsage{Scenario: r.Scenario, Run: r.Run, DurationMS: r.DurationMS}
	var cost float64
	priced, seen := prices != nil, false
	for _, e := range tr.Events {
		if e.Type != events.AgentUsage {
			continue
		}
		var p events.AgentUsagePayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			continue
		}
		seen = true
		tokens := p.InputTokens + p.CacheCreationInputTokens + p.CacheReadInputTokens + p.OutputTokens
		u.Tokens += tokens
		if priced {
			c, ok := prices.CostUSD(p.Model, p.InputTokens, p.CacheCreationInputTokens, p.CacheReadInputTokens, p.OutputTokens)
			priced = ok
			cost += c
		}
		switch p.Role {
		case events.UsageRoleTriage, events.UsageRoleMain:
			u.Turns++
			if p.Role == events.UsageRoleTriage {
				u.TriageTurns++
			} else {
				u.MainTurns++
			}
			u.latencies = append(u.latencies, float64(p.LatencyMS))
			u.turnTokens = append(u.turnTokens, float64(tokens))
		case events.UsageRoleCompaction:
			if n := len(u.turnTokens); n > 0 {
				u.turnTokens[n-1] += float64(tokens)
			}
		}
	}
	u.TurnLatencyMS = distOf(u.latencies)
	u.TokensPerTurn = meanP90(u.turnTokens)
	if priced && seen {
		c := math.Round(cost*1e6) / 1e6
		u.CostUSD = &c
	}
	return u
}

// UsageSummary is the latency, tokens, cost and model routing of a set of
// runs: one scenario, or every run for the overall row (pooled, not weighted
// by scenario).
type UsageSummary struct {
	Runs          int     `json:"runs"`
	DurationMS    Dist    `json:"duration_ms"`
	Turns         int     `json:"turns"`
	TurnLatencyMS Dist    `json:"turn_latency_ms"`
	TokensPerTurn MeanP90 `json:"tokens_per_turn"`
	// CostPerRunUSD is over the priced runs; nil when none was priced.
	CostPerRunUSD *MeanP90 `json:"cost_per_run_usd,omitempty"`
	PricedRuns    int      `json:"priced_runs"`
	TriageTurns   int      `json:"triage_turns"`
	MainTurns     int      `json:"main_turns"`
	// TriageShare is triage turns over all turns; -1 when there were none.
	TriageShare float64 `json:"triage_share"`
}

func summarizeUsage(runs []RunUsage) *UsageSummary {
	s := &UsageSummary{Runs: len(runs), TriageShare: -1}
	var durations, latencies, turnTokens, costs []float64
	for _, r := range runs {
		durations = append(durations, float64(r.DurationMS))
		latencies = append(latencies, r.latencies...)
		turnTokens = append(turnTokens, r.turnTokens...)
		s.Turns += r.Turns
		s.TriageTurns += r.TriageTurns
		s.MainTurns += r.MainTurns
		if r.CostUSD != nil {
			costs = append(costs, *r.CostUSD)
		}
	}
	s.DurationMS = distOf(durations)
	s.TurnLatencyMS = distOf(latencies)
	s.TokensPerTurn = meanP90(turnTokens)
	if len(costs) > 0 {
		c := meanP90(costs)
		s.CostPerRunUSD = &c
		s.PricedRuns = len(costs)
	}
	if s.Turns > 0 {
		s.TriageShare = float64(s.TriageTurns) / float64(s.Turns)
	}
	return s
}

// UsageCollector gathers each run's usage (wire Add into Config.OnRun) and
// fills a report's usage fields with Apply.
type UsageCollector struct {
	prices agent.Prices

	mu   sync.Mutex
	runs []RunUsage
}

// NewUsageCollector returns a collector that prices runs with prices (nil =
// no cost).
func NewUsageCollector(prices agent.Prices) *UsageCollector {
	return &UsageCollector{prices: prices}
}

// Add records one finished run.
func (c *UsageCollector) Add(r RunResult, tr Transcript) {
	if r.Infra() {
		return // never measured the agent
	}
	u := RunUsageOf(r, tr, c.prices)
	c.mu.Lock()
	c.runs = append(c.runs, u)
	c.mu.Unlock()
}

// Apply sets rep.RunUsage and the Usage of every scenario row and of the
// overall row, from the runs added so far.
func (c *UsageCollector) Apply(rep *Report) {
	c.mu.Lock()
	runs := append([]RunUsage(nil), c.runs...)
	c.mu.Unlock()
	sort.Slice(runs, func(i, j int) bool {
		if runs[i].Scenario != runs[j].Scenario {
			return runs[i].Scenario < runs[j].Scenario
		}
		return runs[i].Run < runs[j].Run
	})
	rep.RunUsage = runs
	by := map[string][]RunUsage{}
	for _, r := range runs {
		by[r.Scenario] = append(by[r.Scenario], r)
	}
	for i := range rep.Scenarios {
		if rs := by[rep.Scenarios[i].Name]; len(rs) > 0 {
			rep.Scenarios[i].Usage = summarizeUsage(rs)
		}
	}
	if len(runs) > 0 {
		rep.Overall.Usage = summarizeUsage(runs)
	}
}
