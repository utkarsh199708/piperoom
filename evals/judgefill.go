package evals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// The ambiguous-set manifest and the cache fill (#454). The manifest lists
// every committed corpus text the tri-state pass marks ambiguous, with the
// scenario it is graded against, the corpus label and main's keyword grade.
// TestAmbiguousCorpusSet keeps it equal to what the pass computes, and the
// fill command (evalrun -judge-fill) records one judge verdict per entry. The
// manifest is the only input the fill takes, so only committed synthetic
// texts ever reach the cache.

// DefaultAmbiguousSetPath is the committed manifest, relative to the
// repository root.
const DefaultAmbiguousSetPath = DefaultJudgeCacheDir + "/ambiguous.json"

// AmbiguousText is one manifest entry.
type AmbiguousText struct {
	// Scenario is the scenario name; a held-out scenario is "heldout/<name>".
	Scenario string `json:"scenario"`
	// Kind is what the corpus grades: "root_cause" (rootCauseMatches) or
	// "correct" (root cause and root-cause layer, RunResult.Correct).
	Kind string `json:"kind"`
	Text string `json:"text"`
	// Want is the corpus label: the correct grade.
	Want bool `json:"want"`
	// Keyword is main's keyword grade of the text (the gap rows differ from
	// Want).
	Keyword bool `json:"keyword"`
	// Items are the ambiguous items (Ambiguity.Items).
	Items []string `json:"items"`
	// Sources are the corpus rows the text comes from.
	Sources []string `json:"sources"`
}

// AmbiguousSet is the manifest.
type AmbiguousSet struct {
	Note    string          `json:"note"`
	Entries []AmbiguousText `json:"entries"`
}

// maxManifestBytes bounds the manifest read.
const maxManifestBytes = 8 << 20

// LoadAmbiguousSet reads a manifest.
func LoadAmbiguousSet(path string) (AmbiguousSet, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return AmbiguousSet{}, fmt.Errorf("evals: read ambiguous set: %w", err)
	}
	if len(b) > maxManifestBytes {
		return AmbiguousSet{}, fmt.Errorf("evals: ambiguous set %s is over %d bytes", path, maxManifestBytes)
	}
	var s AmbiguousSet
	if err := json.Unmarshal(b, &s); err != nil {
		return AmbiguousSet{}, fmt.Errorf("evals: parse ambiguous set %s: %w", path, err)
	}
	return s, nil
}

// heldoutPrefix marks a held-out scenario in the manifest.
const heldoutPrefix = "heldout/"

// FillResult is what a fill did or, dry, would do.
type FillResult struct {
	Entries int `json:"entries"`
	// Cached entries already had a valid verdict; Stale ones had a verdict
	// on old ground truth and are refilled.
	Cached int `json:"cached"`
	Stale  int `json:"stale"`
	// Filled were judged and written now; Remaining still need a verdict
	// (a dry run, or the call cap).
	Filled    int `json:"filled"`
	Remaining int `json:"remaining"`
	// EstInputTokens and EstOutputTokens estimate the calls still needed (a
	// dry run) or made; Usage is what the made calls reported.
	EstInputTokens  int64      `json:"est_input_tokens"`
	EstOutputTokens int64      `json:"est_output_tokens"`
	Usage           JudgeUsage `json:"usage"`
}

// estOutputTokensPerCall is the verdict (about 60 tokens) plus the thinking
// that low effort still allows; deliberately high, so an estimate does not
// undersell the spend.
const estOutputTokensPerCall = 500

// estimateInputTokens over-estimates the input tokens of one judge call at
// three characters per token (English prose is nearer four).
func estimateInputTokens(sc Scenario, hypothesis string) int64 {
	return int64(len(judgeSystem)+len(judgePrompt(sc, hypothesis))+2) / 3
}

// FillJudgeCache records a judge verdict for every manifest entry the cache
// lacks or holds stale, at most maxCalls of them (maxCalls <= 0: none). With
// judge nil it is a dry run: nothing is called or written, and the result
// estimates what a fill would spend. scenarios maps the manifest's scenario
// names (held-out ones as "heldout/<name>") to the committed scenarios; an
// entry for any other scenario is refused, so a learned or ad-hoc scenario
// can never put text in the public cache.
func FillJudgeCache(ctx context.Context, cache *JudgeCache, judge HypothesisJudge, scenarios map[string]Scenario, set AmbiguousSet, maxCalls int) (FillResult, error) {
	res := FillResult{Entries: len(set.Entries)}
	type todo struct {
		sc   Scenario
		text string
	}
	var need []todo
	for i, e := range set.Entries {
		sc, ok := scenarios[e.Scenario]
		if !ok {
			return res, fmt.Errorf("evals: ambiguous set entry %d: scenario %q is not a committed default or held-out scenario", i, e.Scenario)
		}
		_, hit, err := cache.Get(sc, e.Text)
		switch {
		case errors.Is(err, ErrJudgeCacheStale):
			res.Stale++
		case err != nil:
			return res, err
		case hit:
			res.Cached++
			continue
		}
		need = append(need, todo{sc: sc, text: e.Text})
	}
	for _, n := range need {
		res.EstInputTokens += estimateInputTokens(n.sc, n.text)
		res.EstOutputTokens += estOutputTokensPerCall
	}
	res.Remaining = len(need)
	if judge == nil {
		return res, nil
	}
	g := &judgeGate{live: judge, maxCalls: max(maxCalls, 0)}
	for _, n := range need {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if res.Filled >= maxCalls {
			break
		}
		g.stats.LiveCalls++
		v, err := g.call(ctx, n.sc, n.text)
		res.Usage = g.stats.Usage
		if err != nil {
			return res, err
		}
		if err := cache.Put(n.sc, n.text, v); err != nil {
			return res, err
		}
		res.Filled++
		res.Remaining--
	}
	return res, nil
}

// ManifestScenarios maps manifest scenario names to the default set and the
// held-out set (as "heldout/<name>").
func ManifestScenarios(defaultSet, heldout []Scenario) map[string]Scenario {
	out := make(map[string]Scenario, len(defaultSet)+len(heldout))
	for _, s := range defaultSet {
		out[s.Name] = s
	}
	for _, s := range heldout {
		out[heldoutPrefix+s.Name] = s
	}
	return out
}

// String summarises a fill result on one line.
func (r FillResult) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d entries: %d cached, %d stale, %d filled, %d remaining", r.Entries, r.Cached, r.Stale, r.Filled, r.Remaining)
	if r.Usage.InputTokens+r.Usage.OutputTokens > 0 {
		fmt.Fprintf(&b, "; judge tokens %d in / %d out", r.Usage.InputTokens, r.Usage.OutputTokens)
	}
	return b.String()
}
