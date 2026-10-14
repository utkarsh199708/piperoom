package room

import (
	"time"

	"piperoom/internal/events"
)

// Insight is RoomSnapshot.insight (docs/api.md, F11): what the agent has cost
// and how fast it got to a hypothesis. Numbers only, folded from the room log.
// A shadow room carries zeros: its agent events live in the shadow log.
type Insight struct {
	FirstHypothesisAt *time.Time `json:"first_hypothesis_at"`
	// TimeToFirstHypothesisS is whole seconds from opened_at to the first
	// agent.hypothesis, -1 before one exists.
	TimeToFirstHypothesisS int64        `json:"time_to_first_hypothesis_s"`
	FinalConfidence        string       `json:"final_confidence"`
	Steps                  int          `json:"steps"`
	ToolCalls              int          `json:"tool_calls"`
	ToolErrors             int          `json:"tool_errors"`
	Usage                  InsightUsage `json:"usage"`
}

// InsightUsage sums the room's agent.usage events. CostUSD is nil while no call
// is priced (an unknown cost is never 0) and a lower bound when UnpricedCalls > 0.
type InsightUsage struct {
	Calls          int      `json:"calls"`
	UnpricedCalls  int      `json:"unpriced_calls"`
	Tokens         int64    `json:"tokens"`
	CostUSD        *float64 `json:"cost_usd"`
	LatencyMSTotal int64    `json:"latency_ms_total"`
}

// foldInsight applies one event to snap.Insight and returns the result. It does
// not mutate snap (CostUSD is re-allocated), so Apply stays pure.
func foldInsight(snap RoomSnapshot, e events.Event) Insight {
	in := snap.Insight
	if in.TimeToFirstHypothesisS == 0 && in.FirstHypothesisAt == nil {
		// A zero-value snapshot (not from NewSnapshot) means "none yet".
		in.TimeToFirstHypothesisS = -1
	}
	if snap.Shadow {
		return in
	}
	switch e.Type {
	case events.AgentStepStart:
		in.Steps++
	case events.AgentToolCall:
		in.ToolCalls++
	case events.AgentToolResult:
		if p, ok := decode[events.AgentToolResultPayload](e); ok && p.IsError {
			in.ToolErrors++
		}
	case events.AgentHypothesis:
		p, ok := decode[events.AgentHypothesisPayload](e)
		if !ok {
			break
		}
		if in.FirstHypothesisAt == nil {
			at := e.At
			in.FirstHypothesisAt = &at
			in.TimeToFirstHypothesisS = secondsSince(snap.OpenedAt, &at)
		}
		in.FinalConfidence = p.Confidence
	case events.AgentUsage:
		p, ok := decode[events.AgentUsagePayload](e)
		if !ok {
			break
		}
		u := in.Usage
		u.Calls++
		u.Tokens += p.InputTokens + p.CacheCreationInputTokens + p.CacheReadInputTokens + p.OutputTokens
		u.LatencyMSTotal += p.LatencyMS
		if p.CostUSD == nil {
			u.UnpricedCalls++
		} else {
			sum := *p.CostUSD
			if u.CostUSD != nil {
				sum += *u.CostUSD
			}
			u.CostUSD = &sum
		}
		in.Usage = u
	}
	return in
}
