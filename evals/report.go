package evals

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
)

// Report is the result of one evaluation.
type Report struct {
	Label      string            `json:"label"`
	Model      string            `json:"model"`
	Effort     string            `json:"effort"`
	MaxTurns   int               `json:"max_turns,omitempty"`
	Runs       int               `json:"runs_per_scenario"`
	StartedAt  time.Time         `json:"started_at"`
	DurationMS int64             `json:"duration_ms"`
	Overall    Summary           `json:"overall"`
	Scenarios  []ScenarioSummary `json:"scenarios"`
	Results    []RunResult       `json:"results"`
}

// Summary aggregates runs. Rates are 0..1. In Report.Overall every scenario
// weighs the same, however many runs finished.
type Summary struct {
	Runs          int     `json:"runs"`
	Passes        int     `json:"passes"`
	PassRate      float64 `json:"pass_rate"`
	MeanScore     float64 `json:"mean_score"`
	RootCauseRate float64 `json:"root_cause_rate"`
	LayerRate     float64 `json:"layer_rate"`
	MeanToolCalls float64 `json:"mean_tool_calls"`
	// MeanToolCallsToRootCause is over runs that found the cause; -1 if none did.
	MeanToolCallsToRootCause float64 `json:"mean_tool_calls_to_root_cause"`
	MeanDurationMS           float64 `json:"mean_duration_ms"`
	Misses                   int     `json:"misses"`
	Errors                   int     `json:"errors"`
}

// ScenarioSummary aggregates one scenario's runs.
type ScenarioSummary struct {
	Name string `json:"name"`
	Summary
}

func buildReport(cfg Config, scenarios []Scenario, results []RunResult, started time.Time) Report {
	rep := Report{
		Label:      cfg.Label,
		Model:      cfg.Options.Model,
		Effort:     cfg.Options.Effort,
		MaxTurns:   cfg.Options.MaxTurns,
		Runs:       cfg.Runs,
		StartedAt:  started.UTC(),
		DurationMS: time.Since(started).Milliseconds(),
		Results:    results,
	}
	byName := map[string][]RunResult{}
	for _, r := range results {
		byName[r.Scenario] = append(byName[r.Scenario], r)
	}
	var withRuns []Summary
	for _, sc := range scenarios {
		s := summarize(byName[sc.Name])
		rep.Scenarios = append(rep.Scenarios, ScenarioSummary{Name: sc.Name, Summary: s})
		if s.Runs > 0 {
			withRuns = append(withRuns, s)
		}
	}
	rep.Overall = combine(withRuns)
	return rep
}

func summarize(rs []RunResult) Summary {
	s := Summary{Runs: len(rs), MeanToolCallsToRootCause: -1}
	if len(rs) == 0 {
		return s
	}
	var score, calls, dur, toCause float64
	var rootCauses, layers, found int
	for _, r := range rs {
		if r.Pass {
			s.Passes++
		}
		if r.RootCause {
			rootCauses++
		}
		if r.LayerNamed {
			layers++
		}
		if r.ToolCallsToRootCause >= 0 {
			found++
			toCause += float64(r.ToolCallsToRootCause)
		}
		if r.Error != "" || r.TimedOut {
			s.Errors++
		}
		score += r.Score
		calls += float64(r.ToolCalls)
		dur += float64(r.DurationMS)
		s.Misses += len(r.Misses)
	}
	n := float64(len(rs))
	s.PassRate = float64(s.Passes) / n
	s.RootCauseRate = float64(rootCauses) / n
	s.LayerRate = float64(layers) / n
	s.MeanScore = score / n
	s.MeanToolCalls = calls / n
	s.MeanDurationMS = dur / n
	if found > 0 {
		s.MeanToolCallsToRootCause = toCause / float64(found)
	}
	return s
}

// combine averages per-scenario summaries so each scenario weighs the same.
func combine(ss []Summary) Summary {
	out := Summary{MeanToolCallsToRootCause: -1}
	if len(ss) == 0 {
		return out
	}
	var toCause float64
	var found int
	for _, s := range ss {
		out.Runs += s.Runs
		out.Passes += s.Passes
		out.Misses += s.Misses
		out.Errors += s.Errors
		out.PassRate += s.PassRate
		out.MeanScore += s.MeanScore
		out.RootCauseRate += s.RootCauseRate
		out.LayerRate += s.LayerRate
		out.MeanToolCalls += s.MeanToolCalls
		out.MeanDurationMS += s.MeanDurationMS
		if s.MeanToolCallsToRootCause >= 0 {
			toCause += s.MeanToolCallsToRootCause
			found++
		}
	}
	n := float64(len(ss))
	out.PassRate /= n
	out.MeanScore /= n
	out.RootCauseRate /= n
	out.LayerRate /= n
	out.MeanToolCalls /= n
	out.MeanDurationMS /= n
	if found > 0 {
		out.MeanToolCallsToRootCause = toCause / float64(found)
	}
	return out
}

// WriteJSON writes the report as indented JSON.
func (r Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		return fmt.Errorf("evals: write report: %w", err)
	}
	return nil
}

// LoadReport reads a report written by WriteJSON.
func LoadReport(path string) (Report, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Report{}, fmt.Errorf("evals: read report: %w", err)
	}
	var r Report
	if err := json.Unmarshal(b, &r); err != nil {
		return Report{}, fmt.Errorf("evals: parse report %s: %w", path, err)
	}
	return r, nil
}

// WriteMarkdown writes the report as a markdown table plus the failed runs.
func (r Report) WriteMarkdown(w io.Writer) error {
	var b strings.Builder
	fmt.Fprintf(&b, "## Agent eval: %s\n\n", mdCell(orDefault(r.Label)))
	fmt.Fprintf(&b, "model `%s` · effort `%s` · %d run(s) per scenario · took %s\n\n",
		orDefault(r.Model), orDefault(r.Effort), r.Runs,
		(time.Duration(r.DurationMS) * time.Millisecond).Round(time.Second))
	b.WriteString("| scenario | pass | score | root cause | layer | tool calls | calls to cause | misses | errors |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|\n")
	for _, s := range r.Scenarios {
		writeSummaryRow(&b, s.Name, s.Summary)
	}
	writeSummaryRow(&b, "**overall**", r.Overall)
	var failed []string
	for _, res := range r.Results {
		if !res.Pass {
			failed = append(failed, fmt.Sprintf("- %s #%d: %s", mdCell(res.Scenario), res.Run, mdCell(strings.Join(res.Failures, "; "))))
		}
	}
	if len(failed) > 0 {
		fmt.Fprintf(&b, "\n**Failed runs**\n\n%s\n", strings.Join(failed, "\n"))
	}
	if _, err := io.WriteString(w, b.String()); err != nil {
		return fmt.Errorf("evals: write report: %w", err)
	}
	return nil
}

func writeSummaryRow(b *strings.Builder, name string, s Summary) {
	fmt.Fprintf(b, "| %s | %d/%d | %.1f | %.0f%% | %.0f%% | %.1f | %s | %d | %d |\n",
		mdCell(name), s.Passes, s.Runs, s.MeanScore, 100*s.RootCauseRate, 100*s.LayerRate,
		s.MeanToolCalls, callsToCause(s.MeanToolCallsToRootCause), s.Misses, s.Errors)
}

// mdCell keeps text from breaking a markdown table row. Failure reasons can
// carry action errors, which can echo model-chosen parameters.
func mdCell(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.ReplaceAll(s, "|", "\\|")
}

func callsToCause(v float64) string {
	if v < 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f", v)
}

func orDefault(s string) string {
	if s == "" {
		return "(default)"
	}
	return s
}

// DefaultScoreTolerance is how many score points a scenario's mean may drop
// before Compare calls it a regression. Pass rate drops always count.
const DefaultScoreTolerance = 5.0

// Comparison sets a candidate report against a baseline.
type Comparison struct {
	Base        string       `json:"base"`
	Candidate   string       `json:"candidate"`
	Rows        []CompareRow `json:"rows"`
	Overall     CompareRow   `json:"overall"`
	Regressions int          `json:"regressions"`
	Tolerance   float64      `json:"score_tolerance"`
	Notes       []string     `json:"notes,omitempty"`
}

// CompareRow is one scenario in both reports.
type CompareRow struct {
	Scenario  string   `json:"scenario"`
	Base      *Summary `json:"base,omitempty"`
	Candidate *Summary `json:"candidate,omitempty"`
	Regressed bool     `json:"regressed"`
	Improved  bool     `json:"improved"`
	Reason    string   `json:"reason,omitempty"`
}

// Compare reports, per scenario, whether the candidate did worse than the
// baseline. A scenario regresses when its pass rate drops, its mean score
// drops by more than tolerance, or it is missing from the candidate.
func Compare(base, cand Report, tolerance float64) Comparison {
	if tolerance < 0 {
		tolerance = DefaultScoreTolerance
	}
	c := Comparison{Base: base.Label, Candidate: cand.Label, Tolerance: tolerance}
	candBy := map[string]Summary{}
	for _, s := range cand.Scenarios {
		candBy[s.Name] = s.Summary
	}
	inBase := map[string]bool{}
	for _, s := range base.Scenarios {
		inBase[s.Name] = true
		b := s.Summary
		row := CompareRow{Scenario: s.Name, Base: &b}
		if cs, ok := candBy[s.Name]; ok && cs.Runs > 0 {
			row.Candidate = &cs
		}
		judgeRow(&row, tolerance)
		c.Rows = append(c.Rows, row)
	}
	for _, s := range cand.Scenarios {
		if !inBase[s.Name] {
			cs := s.Summary
			c.Rows = append(c.Rows, CompareRow{Scenario: s.Name, Candidate: &cs, Reason: "new scenario"})
		}
	}
	for _, r := range c.Rows {
		if r.Regressed {
			c.Regressions++
		}
	}
	bo, co := base.Overall, cand.Overall
	c.Overall = CompareRow{Scenario: "OVERALL", Base: &bo, Candidate: &co}
	judgeRow(&c.Overall, tolerance)

	if base.Runs < 3 || cand.Runs < 3 {
		c.Notes = append(c.Notes, "fewer than 3 runs per scenario: the model is not deterministic, so small differences may be noise")
	}
	if base.Model != cand.Model {
		c.Notes = append(c.Notes, fmt.Sprintf("model differs: %s vs %s", orDefault(base.Model), orDefault(cand.Model)))
	}
	if base.Effort != cand.Effort {
		c.Notes = append(c.Notes, fmt.Sprintf("effort differs: %s vs %s", orDefault(base.Effort), orDefault(cand.Effort)))
	}
	return c
}

func judgeRow(row *CompareRow, tolerance float64) {
	b, cs := row.Base, row.Candidate
	switch {
	case b == nil || b.Runs == 0:
		return
	case cs == nil:
		row.Regressed = true
		row.Reason = "missing from candidate"
		return
	}
	const eps = 1e-9
	dPass := cs.PassRate - b.PassRate
	dScore := cs.MeanScore - b.MeanScore
	switch {
	case dPass < -eps:
		row.Regressed = true
		row.Reason = fmt.Sprintf("pass rate %.0f%% → %.0f%%", 100*b.PassRate, 100*cs.PassRate)
	case dScore < -tolerance:
		row.Regressed = true
		row.Reason = fmt.Sprintf("score %.1f → %.1f", b.MeanScore, cs.MeanScore)
	case dPass > eps:
		row.Improved = true
		row.Reason = fmt.Sprintf("pass rate %.0f%% → %.0f%%", 100*b.PassRate, 100*cs.PassRate)
	case dScore > tolerance:
		row.Improved = true
		row.Reason = fmt.Sprintf("score %.1f → %.1f", b.MeanScore, cs.MeanScore)
	}
}

// WriteMarkdown writes the comparison as a markdown table.
func (c Comparison) WriteMarkdown(w io.Writer) error {
	var b strings.Builder
	fmt.Fprintf(&b, "## Agent eval: %s → %s\n\n", mdCell(orDefault(c.Base)), mdCell(orDefault(c.Candidate)))
	b.WriteString("| scenario | pass | score | tool calls | calls to cause | verdict |\n|---|---|---|---|---|---|\n")
	rows := append(append([]CompareRow(nil), c.Rows...), c.Overall)
	for _, r := range rows {
		verdict := "same"
		switch {
		case r.Regressed:
			verdict = "**REGRESSED**: " + r.Reason
		case r.Improved:
			verdict = "improved: " + r.Reason
		case r.Reason != "":
			verdict = r.Reason
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n", mdCell(r.Scenario),
			pair(r.Base, r.Candidate, func(s *Summary) string { return fmt.Sprintf("%d/%d", s.Passes, s.Runs) }),
			delta(r.Base, r.Candidate, func(s *Summary) float64 { return s.MeanScore }),
			delta(r.Base, r.Candidate, func(s *Summary) float64 { return s.MeanToolCalls }),
			pair(r.Base, r.Candidate, func(s *Summary) string { return callsToCause(s.MeanToolCallsToRootCause) }),
			mdCell(verdict))
	}
	fmt.Fprintf(&b, "\n%d scenario(s) regressed (score tolerance %.1f).\n", c.Regressions, c.Tolerance)
	for _, n := range c.Notes {
		fmt.Fprintf(&b, "\nNote: %s\n", n)
	}
	if _, err := io.WriteString(w, b.String()); err != nil {
		return fmt.Errorf("evals: write comparison: %w", err)
	}
	return nil
}

func pair(b, c *Summary, f func(*Summary) string) string {
	return side(b, f) + " → " + side(c, f)
}

func side(s *Summary, f func(*Summary) string) string {
	if s == nil {
		return "-"
	}
	return f(s)
}

func delta(b, c *Summary, f func(*Summary) float64) string {
	if b == nil || c == nil {
		return pair(b, c, func(s *Summary) string { return fmt.Sprintf("%.1f", f(s)) })
	}
	d := f(c) - f(b)
	if math.Abs(d) < 0.05 {
		d = 0
	}
	return fmt.Sprintf("%.1f → %.1f (%+.1f)", f(b), f(c), d)
}
