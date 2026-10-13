package evals

import (
	"fmt"
	"strings"
)

// confidenceProb maps the agent's confidence word (update_hypothesis allows
// exactly these three) to the probability it is stating that its final
// answer is right.
var confidenceProb = map[string]float64{"low": 0.3, "medium": 0.6, "high": 0.9}

// confidenceLevels orders the reliability table.
var confidenceLevels = []string{"low", "medium", "high"}

// Calibration says whether the agent's confidence matches how often it is
// right, and whether it holds back where it should. Runs that ended in a run
// error (the runner itself failed) are left out: they say nothing about the
// agent's judgement. A run is right when RunResult.Correct holds.
type Calibration struct {
	// RatedRuns had a final hypothesis, so a confidence; UnratedRuns did not
	// and are not in Brier or the table.
	RatedRuns   int `json:"rated_runs"`
	UnratedRuns int `json:"unrated_runs"`
	// Brier is the mean of (p - right)^2 over rated runs, with p from
	// confidenceProb and right 1 or 0: 0 is perfect, 0.25 is a coin flip
	// said at "50%". -1 when no run is rated.
	Brier       float64             `json:"brier"`
	Reliability []ReliabilityBucket `json:"reliability"`
	// Overconfident counts runs at high confidence that were wrong.
	Overconfident int `json:"overconfident"`

	// AbstainRuns are runs on healthy, abstain and ask scenarios, where
	// holding back is right; Abstentions of them held back
	// (RunResult.Abstained). AbstentionRate is -1 when there are none.
	AbstainRuns    int     `json:"abstain_runs"`
	Abstentions    int     `json:"abstentions"`
	AbstentionRate float64 `json:"abstention_rate"`
	// DiagnoseRuns are runs on diagnose scenarios; FalseAbstentions of them
	// held back when a cause was there to find. -1 rate when there are none.
	DiagnoseRuns        int     `json:"diagnose_runs"`
	FalseAbstentions    int     `json:"false_abstentions"`
	FalseAbstentionRate float64 `json:"false_abstention_rate"`
}

// ReliabilityBucket is one confidence level: how many rated runs said it and
// how often they were right. Accuracy is -1 when Runs is 0.
type ReliabilityBucket struct {
	Confidence  string  `json:"confidence"`
	Probability float64 `json:"probability"`
	Runs        int     `json:"runs"`
	Correct     int     `json:"correct"`
	Accuracy    float64 `json:"accuracy"`
}

// EvidenceTotals add up, over every run, the tool calls that observed
// something and the facts and claims the scenarios expected.
type EvidenceTotals struct {
	ToolCallsAttempted int `json:"tool_calls_attempted"`
	ToolCallsOK        int `json:"tool_calls_ok"`
	ToolErrors         int `json:"tool_errors"`
	FactsExpected      int `json:"facts_expected"`
	FactsFound         int `json:"facts_found"`
	Claims             int `json:"claims"`
	ClaimsSupported    int `json:"claims_supported"`
}

// calibrate computes the calibration of results.
func calibrate(results []RunResult) Calibration {
	c := Calibration{Brier: -1, AbstentionRate: -1, FalseAbstentionRate: -1}
	buckets := map[string]*ReliabilityBucket{}
	for _, l := range confidenceLevels {
		buckets[l] = &ReliabilityBucket{Confidence: l, Probability: confidenceProb[l]}
	}
	var sq float64
	for _, r := range results {
		if r.Error != "" {
			continue
		}
		if r.Outcome == OutcomeDiagnose || r.Outcome == "" {
			c.DiagnoseRuns++
			if r.Abstained {
				c.FalseAbstentions++
			}
		} else {
			c.AbstainRuns++
			if r.Abstained {
				c.Abstentions++
			}
		}
		// Confidence is empty exactly when there was no hypothesis.
		b, rated := buckets[r.Confidence]
		if !rated {
			c.UnratedRuns++
			continue
		}
		c.RatedRuns++
		b.Runs++
		right := 0.0
		if r.Correct {
			right = 1
			b.Correct++
		} else if r.Confidence == "high" {
			c.Overconfident++
		}
		d := b.Probability - right
		sq += d * d
	}
	if c.RatedRuns > 0 {
		c.Brier = sq / float64(c.RatedRuns)
	}
	for _, l := range confidenceLevels {
		b := *buckets[l]
		b.Accuracy = rate(b.Correct, b.Runs)
		c.Reliability = append(c.Reliability, b)
	}
	c.AbstentionRate = rate(c.Abstentions, c.AbstainRuns)
	c.FalseAbstentionRate = rate(c.FalseAbstentions, c.DiagnoseRuns)
	return c
}

// rate is n/of, or -1 when of is 0.
func rate(n, of int) float64 {
	if of == 0 {
		return -1
	}
	return float64(n) / float64(of)
}

// evidenceTotals adds up the evidence counters of results.
func evidenceTotals(results []RunResult) EvidenceTotals {
	var e EvidenceTotals
	for _, r := range results {
		e.ToolCallsAttempted += r.ToolCalls
		e.ToolCallsOK += r.ToolCallsOK
		e.ToolErrors += r.ToolErrors
		e.FactsFound += len(r.FactsFound)
		e.FactsExpected += len(r.FactsFound) + len(r.FactsMissing)
		e.ClaimsSupported += len(r.SupportedClaims)
		e.Claims += len(r.SupportedClaims) + len(r.UnsupportedClaims)
	}
	return e
}

// writeQuality writes the calibration and evidence sections of a report's
// markdown. Reports written before these existed have neither.
func writeQuality(b *strings.Builder, c *Calibration, e *EvidenceTotals) {
	if e != nil {
		fmt.Fprintf(b, "\n**Evidence**: %d tool call(s) attempted, %d ok, %d errors · expected facts found %d/%d · claims backed by a cited fact %d/%d\n",
			e.ToolCallsAttempted, e.ToolCallsOK, e.ToolErrors, e.FactsFound, e.FactsExpected, e.ClaimsSupported, e.Claims)
	}
	if c == nil {
		return
	}
	brier := "-"
	if c.Brier >= 0 {
		brier = fmt.Sprintf("%.3f", c.Brier)
	}
	fmt.Fprintf(b, "\n**Calibration** (confidence low=%.1f, medium=%.1f, high=%.1f): Brier %s over %d rated run(s), %d without a hypothesis · overconfident (high and wrong) %d\n\n",
		confidenceProb["low"], confidenceProb["medium"], confidenceProb["high"], brier, c.RatedRuns, c.UnratedRuns, c.Overconfident)
	b.WriteString("| confidence | runs | right | accuracy |\n|---|---|---|---|\n")
	for _, r := range c.Reliability {
		fmt.Fprintf(b, "| %s (%.1f) | %d | %d | %s |\n", r.Confidence, r.Probability, r.Runs, r.Correct, pct(r.Accuracy))
	}
	fmt.Fprintf(b, "\n**Abstention**: held back on %d/%d healthy/abstain/ask run(s) (%s) · false abstention on %d/%d diagnose run(s) (%s)\n",
		c.Abstentions, c.AbstainRuns, pct(c.AbstentionRate), c.FalseAbstentions, c.DiagnoseRuns, pct(c.FalseAbstentionRate))
}

// pct formats a 0..1 rate, or "-" for -1.
func pct(v float64) string {
	if v < 0 {
		return "-"
	}
	return fmt.Sprintf("%.0f%%", 100*v)
}
