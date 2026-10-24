package evals

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"piperoom/internal/redact"
)

// Report is the result of one evaluation.
type Report struct {
	Label string `json:"label"`
	// Commit is the source revision of the run (Config.Commit).
	Commit string `json:"commit,omitempty"`
	// Entry names the code path: every run went through room.Service.
	Entry string `json:"entry,omitempty"`
	// PromptHashes are every prompt hash the model tap saw; one when the
	// prompt was the same for every run. Empty when untapped.
	PromptHashes []string          `json:"prompt_hashes,omitempty"`
	Model        string            `json:"model"`
	Effort       string            `json:"effort"`
	MaxTurns     int               `json:"max_turns,omitempty"`
	Runs         int               `json:"runs_per_scenario"`
	StartedAt    time.Time         `json:"started_at"`
	DurationMS   int64             `json:"duration_ms"`
	Overall      Summary           `json:"overall"`
	Scenarios    []ScenarioSummary `json:"scenarios"`
	Results      []RunResult       `json:"results"`
	// Calibration and Evidence are over every run (calibration.go). Nil in
	// reports written before they existed.
	Calibration *Calibration    `json:"calibration,omitempty"`
	Evidence    *EvidenceTotals `json:"evidence,omitempty"`
	// RunUsage is each run's latency, tokens and cost (stats.go); set by
	// UsageCollector.Apply.
	RunUsage []RunUsage `json:"run_usage,omitempty"`
	// Judge is what the LLM judge did: ambiguous hypotheses, verdicts from
	// the cache or live, unjudged ones, live calls and their tokens
	// (judge_auto.go). Nil when no hypothesis was ambiguous or judged.
	Judge *JudgeStats `json:"judge,omitempty"`
	// InfraStop says why the evaluation stopped early: the model API refused
	// a request in a way no retry fixes (infra.go). The runs it kept from
	// being scored are in Results with an "infra: " error, in no rate and in
	// no comparison.
	InfraStop string `json:"infra_stop,omitempty"`
	// ByTarget and BySource are the pass rate grouped by the setup target
	// kind and the source kind (U1-20), pooled over every scored run of the
	// group. A scenario's source kind defaults to postgres; a scenario that
	// names no target (every incident scenario) is in no target group. They
	// are additive: no existing field changes.
	ByTarget []Breakdown `json:"by_target,omitempty"`
	BySource []Breakdown `json:"by_source,omitempty"`
	// PendingHarness lists the scenarios skipped because they wait for a harness
	// row (pending_harness); PendingHarnessRan lists those an explicit -only
	// ran anyway. Neither counts in a baseline comparison, and the skipped ones
	// are in no rate.
	PendingHarness    []string `json:"pending_harness,omitempty"`
	PendingHarnessRan []string `json:"pending_harness_ran,omitempty"`
}

// Breakdown is one group of a report's per-target or per-source pass rate.
// Kind is the target or source kind; Scenarios is how many scenarios
// contribute a scored run.
type Breakdown struct {
	Kind      string  `json:"kind"`
	Scenarios int     `json:"scenarios"`
	Runs      int     `json:"runs"`
	Passes    int     `json:"passes"`
	PassRate  float64 `json:"pass_rate"`
	// Infra are the group's runs the model API's refusal kept from being
	// scored. They are in no rate.
	Infra int `json:"infra,omitempty"`
}

// Summary aggregates runs. Rates are 0..1. In Report.Overall every scenario
// weighs the same, however many runs finished.
type Summary struct {
	// Runs are the scored runs. Infra counts the runs the model API's refusal
	// kept from being scored: they are in neither Runs nor any rate.
	Infra         int     `json:"infra,omitempty"`
	Runs          int     `json:"runs"`
	Passes        int     `json:"passes"`
	PassRate      float64 `json:"pass_rate"`
	MeanScore     float64 `json:"mean_score"`
	RootCauseRate float64 `json:"root_cause_rate"`
	LayerRate     float64 `json:"layer_rate"`
	// FixRuns are the runs whose scenario expects a fix; RightActions of them
	// proposed the right action (rightaction.go). RightActionRate is
	// RightActions/FixRuns, -1 when no run expects a fix. Overall averages it
	// over the scenarios that expect one.
	FixRuns         int     `json:"fix_runs"`
	RightActions    int     `json:"right_actions"`
	RightActionRate float64 `json:"right_action_rate"`
	MeanToolCalls   float64 `json:"mean_tool_calls"`
	// MeanToolCallsToRootCause is over runs that found the cause; -1 if none did.
	MeanToolCallsToRootCause float64 `json:"mean_tool_calls_to_root_cause"`
	MeanDurationMS           float64 `json:"mean_duration_ms"`
	// MedianMSToCause and MedianStepsToCause are over runs with a time to
	// cause (ttc.go); -1 when none had one. CauseRetracted counts runs that
	// posted a wrong hypothesis after a correct one.
	MedianMSToCause    float64 `json:"median_ms_to_cause"`
	MedianStepsToCause float64 `json:"median_steps_to_cause"`
	CauseRetracted     int     `json:"cause_retracted"`
	Misses             int     `json:"misses"`
	RejectedAttempts   int     `json:"rejected_attempts"`
	// InvalidToolCalls are the tool calls the agent refused before running
	// them (invalid_calls.go), not rejected_attempts. AllToolCalls is every
	// tool call of the runs, and InvalidToolCallRate the first over the
	// second (pooled over every tool call in Overall). All three are nil
	// when a run was not checked (a report written before V9r).
	InvalidToolCalls    *int     `json:"invalid_tool_calls,omitempty"`
	AllToolCalls        *int     `json:"all_tool_calls,omitempty"`
	InvalidToolCallRate *float64 `json:"invalid_tool_call_rate,omitempty"`
	Errors              int      `json:"errors"`
	// Usage holds the latency percentiles, tokens, cost and model routing
	// (stats.go); nil until UsageCollector.Apply fills it.
	Usage *UsageSummary `json:"usage,omitempty"`
}

// ScenarioSummary aggregates one scenario's runs. SourceKind and Target are
// the setup kinds the report's per-source and per-target rates group by
// (U1-20); SourceKind defaults to postgres, Target is "" when the scenario
// names none.
type ScenarioSummary struct {
	Name       string `json:"name"`
	SourceKind string `json:"source_kind,omitempty"`
	Target     string `json:"target,omitempty"`
	Summary
}

func buildReport(cfg Config, scenarios []Scenario, results []RunResult, started time.Time) Report {
	rep := Report{
		Label:      cfg.Label,
		Commit:     cfg.Commit,
		Entry:      "room",
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
		source, target := sc.setupKinds()
		rep.Scenarios = append(rep.Scenarios, ScenarioSummary{Name: sc.Name, SourceKind: source, Target: target, Summary: s})
		if s.Runs > 0 {
			withRuns = append(withRuns, s)
		}
	}
	rep.Overall = combine(withRuns)
	rep.Overall.Infra = totalInfra(rep.Scenarios)
	rep.BySource = rateBy(rep.Scenarios, func(s ScenarioSummary) string { return s.SourceKind })
	rep.ByTarget = rateBy(rep.Scenarios, func(s ScenarioSummary) string { return s.Target })
	if cfg.Tap != nil {
		rep.PromptHashes = cfg.Tap.PromptHashes()
	}
	scored := make([]RunResult, 0, len(results))
	for _, r := range results {
		if !r.Infra() {
			scored = append(scored, r)
		}
	}
	cal, ev := calibrate(scored), evidenceTotals(scored)
	rep.Calibration, rep.Evidence = &cal, &ev
	rep.Judge = cfg.judge.snapshot()
	return rep
}

func summarize(all []RunResult) Summary {
	rs := make([]RunResult, 0, len(all))
	for _, r := range all {
		if !r.Infra() {
			rs = append(rs, r)
		}
	}
	s := Summary{Infra: len(all) - len(rs), Runs: len(rs), MeanToolCallsToRootCause: -1, MedianMSToCause: -1, MedianStepsToCause: -1, RightActionRate: -1}
	if len(rs) == 0 {
		return s
	}
	for _, r := range rs {
		if r.RightAction == nil {
			continue
		}
		s.FixRuns++
		if r.RightAction.OK {
			s.RightActions++
		}
	}
	if s.FixRuns > 0 {
		s.RightActionRate = float64(s.RightActions) / float64(s.FixRuns)
	}
	var ms, steps []float64
	for _, r := range rs {
		if r.TimeToCause != nil {
			ms = append(ms, float64(r.TimeToCause.MS))
			steps = append(steps, float64(r.TimeToCause.Steps))
		}
		if r.CauseRetracted {
			s.CauseRetracted++
		}
	}
	s.MedianMSToCause, s.MedianStepsToCause = median(ms), median(steps)
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
		s.RejectedAttempts += r.RejectedAttempts
	}
	summarizeInvalid(&s, rs)
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

// median is the middle value (mean of the two middle ones); -1 when empty.
func median(xs []float64) float64 {
	if len(xs) == 0 {
		return -1
	}
	xs = append([]float64(nil), xs...)
	sort.Float64s(xs)
	n := len(xs)
	if n%2 == 1 {
		return xs[n/2]
	}
	return (xs[n/2-1] + xs[n/2]) / 2
}

// combine averages per-scenario summaries so each scenario weighs the same.
func combine(ss []Summary) Summary {
	out := Summary{MeanToolCallsToRootCause: -1, MedianMSToCause: -1, MedianStepsToCause: -1, RightActionRate: -1}
	if len(ss) == 0 {
		return out
	}
	var toCause float64
	var found int
	var rightRate float64
	var withFix int
	// Overall time to cause is the median of the scenarios' medians, so
	// each scenario weighs the same here too.
	var ms, steps []float64
	for _, s := range ss {
		out.CauseRetracted += s.CauseRetracted
		if s.MedianMSToCause >= 0 {
			ms = append(ms, s.MedianMSToCause)
			steps = append(steps, s.MedianStepsToCause)
		}
		out.Runs += s.Runs
		out.Passes += s.Passes
		out.Misses += s.Misses
		out.RejectedAttempts += s.RejectedAttempts
		out.Errors += s.Errors
		out.FixRuns += s.FixRuns
		out.RightActions += s.RightActions
		if s.FixRuns > 0 {
			rightRate += s.RightActionRate
			withFix++
		}
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
	if withFix > 0 {
		out.RightActionRate = rightRate / float64(withFix)
	}
	out.MedianMSToCause, out.MedianStepsToCause = median(ms), median(steps)
	combineInvalid(&out, ss)
	return out
}

// Restrict returns r limited to the named scenarios, for comparing a -only run
// with a full baseline like with like. Overall is recomputed from the kept
// per-scenario summaries exactly as buildReport does (each scenario weighs
// the same), so it is not the full report's number. Fields that are not
// per-scenario (Calibration, Evidence) are dropped rather than left covering
// scenarios that were removed.
func (r Report) Restrict(names []string) Report {
	keep := make(map[string]bool, len(names))
	for _, n := range names {
		keep[n] = true
	}
	out := r
	out.Scenarios, out.Results, out.RunUsage = nil, nil, nil
	out.Calibration, out.Evidence = nil, nil
	var withRuns []Summary
	for _, s := range r.Scenarios {
		if !keep[s.Name] {
			continue
		}
		out.Scenarios = append(out.Scenarios, s)
		if s.Runs > 0 {
			withRuns = append(withRuns, s.Summary)
		}
	}
	for _, res := range r.Results {
		if keep[res.Scenario] {
			out.Results = append(out.Results, res)
		}
	}
	for _, u := range r.RunUsage {
		if keep[u.Scenario] {
			out.RunUsage = append(out.RunUsage, u)
		}
	}
	out.Overall = combine(withRuns)
	out.Overall.Infra = totalInfra(out.Scenarios)
	out.BySource = rateBy(out.Scenarios, func(s ScenarioSummary) string { return s.SourceKind })
	out.ByTarget = rateBy(out.Scenarios, func(s ScenarioSummary) string { return s.Target })
	return out
}

// totalInfra is the infra runs over all scenario rows, including the rows with
// no scored run that combine leaves out.
func totalInfra(ss []ScenarioSummary) int {
	n := 0
	for _, s := range ss {
		n += s.Infra
	}
	return n
}

// rateBy pools the scored runs of the scenarios by a kind key (a source or
// target kind; the key returns "" for a scenario outside that grouping). The
// rate is passes over runs within the group, not the mean of the per-scenario
// rates, so a group with more runs weighs more. Groups are sorted by kind. A
// scenario whose key is empty is in no group.
func rateBy(ss []ScenarioSummary, key func(ScenarioSummary) string) []Breakdown {
	byKind := map[string]*Breakdown{}
	for _, s := range ss {
		kind := key(s)
		if kind == "" || s.Runs == 0 {
			// A group is a pass rate: a scenario all of whose runs were infra
			// has none. Its infra runs stay in ScenarioSummary.Infra and in
			// the report's overall count.
			continue
		}
		b := byKind[kind]
		if b == nil {
			b = &Breakdown{Kind: kind}
			byKind[kind] = b
		}
		b.Infra += s.Infra
		b.Scenarios++
		b.Runs += s.Runs
		b.Passes += s.Passes
	}
	if len(byKind) == 0 {
		return nil
	}
	out := make([]Breakdown, 0, len(byKind))
	for _, b := range byKind {
		if b.Runs > 0 {
			b.PassRate = float64(b.Passes) / float64(b.Runs)
		}
		out = append(out, *b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind < out[j].Kind })
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
	if r.Commit != "" || len(r.PromptHashes) > 0 {
		fmt.Fprintf(&b, "commit `%s` · prompt %s\n\n", mdCell(orDefault(r.Commit)), mdCell(promptCell(r.PromptHashes)))
	}
	b.WriteString("| scenario | pass | score | root cause | layer | right action | tool calls | calls to cause | time to cause ms / steps | retracted | misses | invalid tool calls | rejected fixes | errors |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, s := range r.Scenarios {
		writeSummaryRow(&b, s.Name, s.Summary)
	}
	writeSummaryRow(&b, "**overall**", r.Overall)
	writeBreakdowns(&b, r)
	if len(r.PendingHarness) > 0 {
		fmt.Fprintf(&b, "\n**Pending harness (skipped, in no rate):** %s\n", mdCell(strings.Join(r.PendingHarness, ", ")))
	}
	if len(r.PendingHarnessRan) > 0 {
		fmt.Fprintf(&b, "\n**Pending harness (ran by -only, not compared with the baseline):** %s\n", mdCell(strings.Join(r.PendingHarnessRan, ", ")))
	}
	writeInvalidLine(&b, r.Overall, r.Results)
	writeJudgeLine(&b, r.Judge)
	writeQuality(&b, r.Calibration, r.Evidence)
	writeUsageTable(&b, r)
	if r.InfraStop != "" {
		fmt.Fprintf(&b, "\n**Evaluation stopped: the model API refused the request (%s).** Runs marked infra were not scored and are not a regression of the agent.\n", mdCell(RunErrorText(r.InfraStop)))
	}
	writeReplayWrites(&b, r.Results)
	var failed []string
	for _, res := range r.Results {
		if !res.Pass {
			failed = append(failed, fmt.Sprintf("- %s #%d: %s", mdCell(res.Scenario), res.Run, mdCell(strings.Join(res.Failures, "; "))))
			failed = append(failed, failedRunDetails(res)...)
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

// writeBreakdowns prints the per-source and per-target pass rates (U1-20)
// under the table, one compact line each. They are absent when no scenario
// names a kind (a report written before the rates existed, or one built from
// scenario summaries without the fields).
func writeBreakdowns(b *strings.Builder, r Report) {
	line := func(label string, bs []Breakdown) {
		if len(bs) == 0 {
			return
		}
		parts := make([]string, 0, len(bs))
		for _, x := range bs {
			parts = append(parts, fmt.Sprintf("%s %d/%d (%.0f%%)", mdCell(x.Kind), x.Passes, x.Runs, 100*x.PassRate))
		}
		fmt.Fprintf(b, "\n%s: %s\n", label, strings.Join(parts, " · "))
	}
	line("by source", r.BySource)
	line("by target", r.ByTarget)
}

// writeReplayWrites lists, per run, the writes its actions took on the replay
// clients. Nothing was performed; the line says what the executed fix would
// have touched. Action and target only, never a value.
func writeReplayWrites(b *strings.Builder, results []RunResult) {
	var lines []string
	for _, res := range results {
		if res.Infra() || len(res.Writes) == 0 {
			continue
		}
		parts := make([]string, 0, len(res.Writes))
		for _, w := range res.Writes {
			parts = append(parts, strings.TrimSpace(w.Action+" "+w.Target))
		}
		lines = append(lines, fmt.Sprintf("- %s #%d: %s", mdCell(res.Scenario), res.Run, mdCell(strings.Join(parts, ", "))))
	}
	if len(lines) > 0 {
		fmt.Fprintf(b, "\n**Replay writes** (recorded, not performed)\n\n%s\n", strings.Join(lines, "\n"))
	}
}

// maxFailedHypothesisRunes bounds the hypothesis text printed under a failed
// run; maxFailedMessageRunes bounds its last message.
const (
	maxFailedHypothesisRunes = 300
	maxFailedMessageRunes    = 400
)

// cutRedacted masks secrets in model text and cuts it to n runes.
func cutRedacted(text string, n int) string {
	text, _ = redact.Text(text)
	if r := []rune(text); len(r) > n {
		text = string(r[:n]) + "…"
	}
	return text
}

// failedRunDetails are the lines printed under a failed run: its final
// hypothesis (confidence, whether it named the expected layers, the first
// 300 characters of the text), the actions it proposed and the root_cause
// term groups the hypothesis missed. The scenarios are synthetic, and the
// agent's text still passes through redact.Text because it is model output
// printed in a public CI log.
func failedRunDetails(res RunResult) []string {
	var out []string
	if res.Error != "" {
		// Redacted again: an older report may hold the raw text.
		out = append(out, "  - error: "+mdCell(RunErrorText(res.Error)))
	}
	if res.FinalHypothesis != "" {
		text := cutRedacted(res.FinalHypothesis, maxFailedHypothesisRunes)
		out = append(out, fmt.Sprintf("  - hypothesis (confidence %s, first layer named %t, root-cause layer named %t): %s",
			orDefault(res.Confidence), res.LayerNamed, res.RootCauseLayerNamed, mdCell(text)))
	}
	if res.LastMessage != "" {
		out = append(out, "  - last message: "+mdCell(cutRedacted(res.LastMessage, maxFailedMessageRunes)))
	}
	// A healthy run that failed its verdict says which text tripped it: a
	// transcript artifact is not always reachable, the job log is. The text
	// is redacted before the note is made: faultClause returns a clause of
	// tokens with the punctuation gone, which the secret patterns no longer
	// match ("password=hunter2…" becomes "password hunter2…").
	if res.Outcome == OutcomeHealthy && !res.SaysHealthy {
		var notes []string
		last, _ := redact.Text(res.LastMessage)
		if n := healthyNote(last); n != "" {
			notes = append(notes, "last message: "+n)
		}
		if res.FinalHypothesis != "" {
			hyp, _ := redact.Text(res.FinalHypothesis)
			if n := healthyNote(hyp); n != "" {
				notes = append(notes, "hypothesis: "+n)
			}
		}
		if len(notes) > 0 {
			out = append(out, "  - healthy verdict failed: "+mdCell(strings.Join(notes, "; ")))
		}
	}
	if a := res.Ambiguity; a != nil {
		settled := a.Source
		if settled == "" {
			settled = "keyword grade kept"
		}
		out = append(out, fmt.Sprintf("  - ambiguous %s (%s): %s", mdCell(strings.Join(a.Items(), ", ")), mdCell(settled), mdCell(strings.Join(a.Reasons, "; "))))
	}
	if len(res.ProposedActions) > 0 {
		out = append(out, "  - proposed actions: "+mdCell(strings.Join(res.ProposedActions, ", ")))
	}
	if ra := res.RightAction; ra != nil && !ra.OK {
		out = append(out, "  - right action not proposed: no step for "+mdCell(ra.Missing)+matchedSoFar(ra.Matched))
	}
	for _, g := range res.MissingCauseGroups {
		out = append(out, "  - missing "+mdCell(g))
	}
	return out
}

// writeJudgeLine prints what the judge did, under the table: how many final
// hypotheses the keyword pass found ambiguous, how many a verdict settled and
// from where, how many stayed unjudged (counted not passed), and the live
// calls and their tokens.
func writeJudgeLine(b *strings.Builder, j *JudgeStats) {
	if j == nil {
		return
	}
	limit := "no cap"
	switch {
	case j.MaxLiveCalls < 0:
		limit = "live calls off"
	case j.MaxLiveCalls > 0:
		limit = fmt.Sprintf("cap %d", j.MaxLiveCalls)
	}
	if j.ClaudeModeCalls > 0 {
		// -judge claude calls have no cap; the cap is the auto mode's.
		limit = fmt.Sprintf("%d of them -judge claude (no cap), auto %s", j.ClaudeModeCalls, limit)
	}
	cached := ""
	if len(j.CacheModels) > 0 {
		cached = " by " + strings.Join(j.CacheModels, ", ")
	}
	fmt.Fprintf(b, "\njudge: %d ambiguous · %d judged (%d cached%s, %d live) · %d ambiguous (unjudged), counted not passed · %d live call(s), %s · judge tokens %d in / %d out\n",
		j.Ambiguous, j.Judged, j.CacheHits, mdCell(cached), j.LiveVerdicts, j.Unjudged, j.LiveCalls, limit, j.Usage.InputTokens, j.Usage.OutputTokens)
}

func writeSummaryRow(b *strings.Builder, name string, s Summary) {
	if s.Runs == 0 && s.Infra > 0 {
		// Not scored: say so instead of printing a 0/0 row of zeros.
		fmt.Fprintf(b, "| %s | infra (%d, not scored) | - | - | - | - | - | - | - | - | - | - | - | %d |\n", mdCell(name), s.Infra, s.Infra)
		return
	}
	pass := fmt.Sprintf("%d/%d", s.Passes, s.Runs)
	if s.Infra > 0 {
		pass += fmt.Sprintf(" (+%d infra)", s.Infra)
	}
	ttc := "-"
	if s.MedianMSToCause >= 0 {
		ttc = fmt.Sprintf("%.0f / %.1f", s.MedianMSToCause, s.MedianStepsToCause)
	}
	fmt.Fprintf(b, "| %s | %s | %.1f | %.0f%% | %.0f%% | %s | %.1f | %s | %s | %d | %d | %s | %d | %d |\n",
		mdCell(name), pass, s.MeanScore, 100*s.RootCauseRate, 100*s.LayerRate, rightActionCell(s),
		s.MeanToolCalls, callsToCause(s.MeanToolCallsToRootCause), ttc, s.CauseRetracted, s.Misses, invalidCell(s), s.RejectedAttempts, s.Errors)
}

// matchedSoFar names the steps that met the earlier fix groups.
func matchedSoFar(matched []string) string {
	if len(matched) == 0 {
		return ""
	}
	return " after " + mdCell(strings.Join(matched, ", "))
}

// rightActionCell is the right-action rate, "-" when no run expected a fix
// (and in reports written before the score existed, which have no fix runs).
func rightActionCell(s Summary) string {
	if s.FixRuns == 0 {
		return "-"
	}
	return fmt.Sprintf("%.0f%%", 100*s.RightActionRate)
}

// promptCell lists the prompt hashes; more than one means runs saw
// different prompts.
func promptCell(hashes []string) string {
	switch len(hashes) {
	case 0:
		return "untapped"
	case 1:
		return "`" + hashes[0] + "`"
	}
	return fmt.Sprintf("**%d different**: `%s`", len(hashes), strings.Join(hashes, "`, `"))
}

// writeUsageTable adds the latency and cost table when the report has usage.
func writeUsageTable(b *strings.Builder, r Report) {
	if r.Overall.Usage == nil {
		return
	}
	b.WriteString("\n| scenario | run ms p50/p90/p99 | turn ms p50/p90/p99 | turns | tokens/turn mean (p90) | cost/run mean (p90) | triage share |\n")
	b.WriteString("|---|---|---|---|---|---|---|\n")
	for _, s := range r.Scenarios {
		if s.Usage != nil {
			writeUsageRow(b, s.Name, s.Usage)
		}
	}
	writeUsageRow(b, "**overall**", r.Overall.Usage)
}

func writeUsageRow(b *strings.Builder, name string, u *UsageSummary) {
	cost := "-"
	if u.CostPerRunUSD != nil {
		cost = fmt.Sprintf("$%.4f ($%.4f)", u.CostPerRunUSD.Mean, u.CostPerRunUSD.P90)
		if u.PricedRuns < u.Runs {
			cost += fmt.Sprintf(" over %d/%d runs", u.PricedRuns, u.Runs)
		}
	}
	share := "-"
	if u.TriageShare >= 0 {
		share = fmt.Sprintf("%.0f%%", 100*u.TriageShare)
	}
	fmt.Fprintf(b, "| %s | %s | %s | %d | %s | %s | %s |\n", mdCell(name), distCell(u.DurationMS), distCell(u.TurnLatencyMS),
		u.Turns, meanP90Cell(u.TokensPerTurn), cost, share)
}

func distCell(d Dist) string {
	if d.P50 < 0 {
		return "-"
	}
	return fmt.Sprintf("%.0f / %.0f / %.0f", d.P50, d.P90, d.P99)
}

func meanP90Cell(m MeanP90) string {
	if m.Mean < 0 {
		return "-"
	}
	return fmt.Sprintf("%.0f (%.0f)", m.Mean, m.P90)
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
	// InvalidToolCalls compares invalid tool calls per run over the shared
	// scenarios (invalid_calls.go). Its growth is not in Regressions, which
	// counts scenarios; evalrun -fail-on-regression fails on it too, as
	// evals-live fails on misses per run growing.
	InvalidToolCalls InvalidCallsComparison `json:"invalid_tool_calls"`
	Notes            []string               `json:"notes,omitempty"`
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
		if cs, ok := candBy[s.Name]; ok && (cs.Runs > 0 || cs.Infra > 0) {
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
	c.InvalidToolCalls = compareInvalid(base, cand)

	if n := cand.Overall.Infra; n > 0 {
		c.Notes = append(c.Notes, fmt.Sprintf("%d candidate run(s) were infra failures (the model API refused them): left out of every rate and verdict, never a regression", n))
	}
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
	case cs != nil && cs.Runs == 0 && cs.Infra > 0:
		// Every run of the candidate was kept from scoring by an outage: no
		// verdict, and certainly not a regression.
		row.Reason = fmt.Sprintf("infra: %d run(s) not scored", cs.Infra)
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
			pair(r.Base, r.Candidate, passCell),
			delta(r.Base, r.Candidate, func(s *Summary) float64 { return s.MeanScore }),
			delta(r.Base, r.Candidate, func(s *Summary) float64 { return s.MeanToolCalls }),
			pair(r.Base, r.Candidate, func(s *Summary) string { return callsToCause(s.MeanToolCallsToRootCause) }),
			mdCell(verdict))
	}
	fmt.Fprintf(&b, "\n%d scenario(s) regressed (score tolerance %.1f).\n", c.Regressions, c.Tolerance)
	c.InvalidToolCalls.writeMarkdown(&b)
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

// passCell is passes/runs of a scored side, "infra" when an outage left it
// with no scored run.
func passCell(s *Summary) string {
	if s.Runs == 0 && s.Infra > 0 {
		return "infra"
	}
	if s.Infra > 0 {
		return fmt.Sprintf("%d/%d (+%d infra)", s.Passes, s.Runs, s.Infra)
	}
	return fmt.Sprintf("%d/%d", s.Passes, s.Runs)
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
