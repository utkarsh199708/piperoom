package evals

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"piperoom/internal/events"
)

// ToolCallChecks counts the tool calls the agent refused before running them
// (V9r, docs/design/model-providers.md 5.1): the call named no registered
// tool, its arguments were not valid JSON, or they failed the tool's input
// schema. The agent marks each such agent.tool_result with Rejected (V1t).
// It is not rejected_attempts, which counts proposal steps an action's
// Validate refused: there the model called propose_fix well and concluded
// something the action does not allow; here the model did not call the tool
// well at all. A tool that ran and returned an error is neither.
//
// It sits in ReplayResult (and so in RunResult), set by withReplayChecks, so
// Judge in score.go (E2's) stays untouched. Nil InvalidToolCalls means the
// run was not checked: an infra run, or a report written before V9r.
type ToolCallChecks struct {
	// InvalidToolCalls is how many of the run's tool calls were refused
	// before they ran.
	InvalidToolCalls *int `json:"invalid_tool_calls,omitempty"`
	// InvalidToolCallReasons splits InvalidToolCalls by reason
	// (events.ToolRejected*). Empty when there were none.
	InvalidToolCallReasons map[string]int `json:"invalid_tool_call_reasons,omitempty"`
	// AllToolCalls is every agent.tool_call of the run, the rejected ones and
	// the reads after a fix included (tool_calls leaves those out): the
	// denominator of the invalid tool call rate.
	AllToolCalls int `json:"all_tool_calls,omitempty"`
}

// checkToolCalls counts the agent's tool calls and the ones refused before
// they ran. Each refused call has exactly one tool_result carrying its
// reason, so it is counted once; its tool_call event only counts toward the
// total.
func checkToolCalls(evs []RecordedEvent) ToolCallChecks {
	invalid := 0
	var out ToolCallChecks
	for _, e := range evs {
		if !fromAgent(e) {
			continue
		}
		switch e.Type {
		case events.AgentToolCall:
			out.AllToolCalls++
		case events.AgentToolResult:
			var p events.AgentToolResultPayload
			if json.Unmarshal(e.Payload, &p) != nil || p.Rejected == "" {
				continue
			}
			invalid++
			if out.InvalidToolCallReasons == nil {
				out.InvalidToolCallReasons = map[string]int{}
			}
			out.InvalidToolCallReasons[p.Rejected]++
		}
	}
	out.InvalidToolCalls = &invalid
	return out
}

// invalidTotals sums the checked counts of runs or summaries. ok is false
// when any of them is unchecked: a partial count would read as a low one.
type invalidTotals struct {
	invalid, all int
	ok           bool
}

func (t *invalidTotals) add(invalid *int, all int) {
	if invalid == nil {
		t.ok = false
		return
	}
	t.invalid += *invalid
	t.all += all
}

// setOn writes the totals on s, or leaves s unchecked (nil) when they are
// not known.
func (t invalidTotals) setOn(s *Summary) {
	if !t.ok {
		s.InvalidToolCalls, s.AllToolCalls, s.InvalidToolCallRate = nil, nil, nil
		return
	}
	n, all := t.invalid, t.all
	rate := 0.0
	if all > 0 {
		rate = float64(n) / float64(all)
	}
	s.InvalidToolCalls, s.AllToolCalls, s.InvalidToolCallRate = &n, &all, &rate
}

// summarizeInvalid sets the invalid tool call totals of scored runs on s.
func summarizeInvalid(s *Summary, rs []RunResult) {
	t := invalidTotals{ok: len(rs) > 0}
	for _, r := range rs {
		t.add(r.InvalidToolCalls, r.AllToolCalls)
	}
	t.setOn(s)
}

// combineInvalid sets the totals of per-scenario summaries on out. The rate
// is pooled over every tool call (invalid_tool_calls / all tool calls), the
// ratio the model gate reads (design 5.1), not a mean of scenario rates.
func combineInvalid(out *Summary, ss []Summary) {
	t := invalidTotals{ok: len(ss) > 0}
	for _, s := range ss {
		all := 0
		if s.AllToolCalls != nil {
			all = *s.AllToolCalls
		}
		t.add(s.InvalidToolCalls, all)
	}
	t.setOn(out)
}

// invalidCell is the summary table's cell: the count and the rate over every
// tool call, "-" when the runs were not checked.
func invalidCell(s Summary) string {
	if s.InvalidToolCalls == nil || s.InvalidToolCallRate == nil {
		return "-"
	}
	return fmt.Sprintf("%d (%.1f%%)", *s.InvalidToolCalls, 100**s.InvalidToolCallRate)
}

// writeInvalidLine prints the overall invalid tool calls by reason under the
// table, so a reader does not mistake them for rejected fixes.
func writeInvalidLine(b *strings.Builder, overall Summary, results []RunResult) {
	if overall.InvalidToolCalls == nil || overall.AllToolCalls == nil {
		return
	}
	reasons := map[string]int{}
	for _, r := range results {
		if r.Infra() {
			continue
		}
		for k, v := range r.InvalidToolCallReasons {
			reasons[k] += v
		}
	}
	keys := make([]string, 0, len(reasons))
	for k := range reasons {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s %d", k, reasons[k]))
	}
	by := ""
	if len(parts) > 0 {
		by = ": " + strings.Join(parts, ", ")
	}
	fmt.Fprintf(b, "\ninvalid tool calls: %d of %d tool calls (%.1f%%)%s · refused before they ran, not rejected fixes\n",
		*overall.InvalidToolCalls, *overall.AllToolCalls, 100**overall.InvalidToolCallRate, mdCell(by))
}

// InvalidCallsComparison compares invalid tool calls per run over the
// scenarios both reports scored, the way evals-live compares misses per run:
// per run, so a 3-run report compares with a 1-run one, and over the shared
// scenarios only, so an -only run compares with the same part of a baseline.
type InvalidCallsComparison struct {
	// Known is false when either report has a shared scenario without the
	// counter (a baseline written before V9r): nothing is compared and
	// nothing grew.
	Known bool `json:"known"`
	// Shared is how many scenarios both reports scored.
	Shared     int     `json:"shared_scenarios"`
	BasePerRun float64 `json:"base_per_run"`
	CandPerRun float64 `json:"candidate_per_run"`
	Grew       bool    `json:"grew"`
}

// compareInvalid builds the comparison from the scenario summaries.
func compareInvalid(base, cand Report) InvalidCallsComparison {
	candBy := map[string]Summary{}
	for _, s := range cand.Scenarios {
		if s.Runs > 0 {
			candBy[s.Name] = s.Summary
		}
	}
	out := InvalidCallsComparison{Known: true}
	var bInv, bRuns, cInv, cRuns int
	for _, s := range base.Scenarios {
		cs, ok := candBy[s.Name]
		if !ok || s.Runs == 0 {
			continue
		}
		out.Shared++
		if s.InvalidToolCalls == nil || cs.InvalidToolCalls == nil {
			out.Known = false
			continue
		}
		bInv += *s.InvalidToolCalls
		bRuns += s.Runs
		cInv += *cs.InvalidToolCalls
		cRuns += cs.Runs
	}
	if out.Shared == 0 || !out.Known {
		out.Known = false
		return out
	}
	out.BasePerRun = float64(bInv) / float64(bRuns)
	out.CandPerRun = float64(cInv) / float64(cRuns)
	const eps = 1e-9
	out.Grew = out.CandPerRun > out.BasePerRun+eps
	return out
}

// writeMarkdown prints the comparison line under the comparison table.
func (ic InvalidCallsComparison) writeMarkdown(b *strings.Builder) {
	switch {
	case ic.Shared == 0:
		b.WriteString("\ninvalid tool calls: no scenario scored in both reports; not compared.\n")
	case !ic.Known:
		fmt.Fprintf(b, "\ninvalid tool calls: unknown, not compared: a report has no invalid_tool_calls for a shared scenario (written before V9r). Not a regression.\n")
	case ic.Grew:
		fmt.Fprintf(b, "\n**invalid tool calls per run grew** over %d shared scenario(s): %.2f → %.2f.\n", ic.Shared, ic.BasePerRun, ic.CandPerRun)
	default:
		fmt.Fprintf(b, "\ninvalid tool calls per run over %d shared scenario(s): %.2f → %.2f.\n", ic.Shared, ic.BasePerRun, ic.CandPerRun)
	}
}
