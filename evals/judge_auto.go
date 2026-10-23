package evals

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// The auto judge (#454): the keyword pass grades a final hypothesis; when the
// tri-state pass (tristate.go) marks any of its items ambiguous, one judge
// verdict replaces the keyword verdicts of the root cause and both layers
// (ApplyVerdict), and the keyword ones stay in the result (Judged, Ambiguity).
// The verdict comes from the recorded cache (judgecache.go) first; on a miss,
// from the live judge while the run's call cap lasts. With no verdict the
// ambiguous items are "ambiguous (unjudged)" and count as not passed. A
// cache-only gate (unit tests, CI replays) fails the run on a miss instead.

// DefaultJudgeMaxCalls is the default cap on live judge calls in auto mode,
// per evaluation.
const DefaultJudgeMaxCalls = 50

// JudgeStats is what the judge did in one evaluation (Report.Judge).
type JudgeStats struct {
	// Ambiguous counts diagnose runs whose final hypothesis had an ambiguous
	// item, in any judge mode.
	Ambiguous int `json:"ambiguous"`
	// Judged counts the auto-mode runs a verdict settled, CacheHits and
	// LiveVerdicts of them by where the verdict came from.
	Judged       int `json:"judged"`
	CacheHits    int `json:"cache_hits"`
	LiveVerdicts int `json:"live_verdicts"`
	// Unjudged counts auto-mode runs left "ambiguous (unjudged)".
	Unjudged int `json:"unjudged"`
	// CacheModels are the judge models of the verdicts read from the cache:
	// "provider:model" (JudgeID), or the bare model of an entry recorded
	// before V9.
	CacheModels []string `json:"cache_models,omitempty"`
	// LiveCalls counts every live judge call, auto and llm mode, and
	// MaxLiveCalls is the auto-mode cap (-1: no live calls allowed);
	// ClaudeModeCalls of them were -judge llm (or its alias claude) calls,
	// which have no cap. The field keeps its name so reports compare.
	LiveCalls       int `json:"live_calls"`
	MaxLiveCalls    int `json:"max_live_calls"`
	ClaudeModeCalls int `json:"claude_mode_calls,omitempty"`
	// Usage is the judge's token spend over every live call (zero for a
	// judge that does not report tokens).
	Usage JudgeUsage `json:"usage"`
}

// unjudgedError says why an ambiguous hypothesis got no verdict.
type unjudgedError struct{ why string }

func (e unjudgedError) Error() string { return "evals: ambiguous hypothesis unjudged: " + e.why }

// judgeGate settles ambiguous hypotheses for one evaluation. It is shared by
// the evaluation's parallel runs.
type judgeGate struct {
	cache     *JudgeCache
	live      HypothesisJudge
	cacheOnly bool
	maxCalls  int // live calls allowed in auto mode; < 0 means none

	mu    sync.Mutex
	stats JudgeStats
}

// newJudgeGate builds the gate of cfg.
func newJudgeGate(cfg Config) *judgeGate {
	g := &judgeGate{live: cfg.JudgeClient, cacheOnly: cfg.JudgeCacheOnly, maxCalls: cfg.JudgeMaxCalls}
	if cfg.JudgeCache != "" {
		g.cache = NewJudgeCache(cfg.JudgeCache)
	}
	if g.maxCalls == 0 {
		g.maxCalls = DefaultJudgeMaxCalls
	}
	g.stats.MaxLiveCalls = g.maxCalls
	return g
}

// snapshot returns the stats so far, or nil when the judge did nothing.
func (g *judgeGate) snapshot() *JudgeStats {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.stats
	s.CacheModels = slices.Clone(s.CacheModels)
	if s.Ambiguous == 0 && s.LiveCalls == 0 && s.Judged == 0 && s.CacheHits == 0 {
		return nil
	}
	return &s
}

func (g *judgeGate) add(f func(*JudgeStats)) {
	g.mu.Lock()
	f(&g.stats)
	g.mu.Unlock()
}

// call asks the live judge once, adding its tokens to the stats.
func (g *judgeGate) call(ctx context.Context, sc Scenario, hypothesis string) (Verdict, error) {
	var (
		v   Verdict
		u   JudgeUsage
		err error
	)
	if m, ok := g.live.(MeteredJudge); ok {
		v, u, err = m.JudgeHypothesisMetered(ctx, sc, hypothesis)
	} else {
		v, err = g.live.JudgeHypothesis(ctx, sc, hypothesis)
	}
	g.add(func(s *JudgeStats) {
		s.Usage.InputTokens += u.InputTokens
		s.Usage.OutputTokens += u.OutputTokens
	})
	return v, err
}

// settle returns the verdict for an ambiguous hypothesis and its source
// ("cache" or "live"). The error is an unjudgedError when no verdict could be
// had within the rules, ErrJudgeCacheMiss for a cache-only gate, or a judge
// or cache failure.
func (g *judgeGate) settle(ctx context.Context, sc Scenario, hypothesis string) (Verdict, string, error) {
	if g.cache != nil {
		e, ok, err := g.cache.Lookup(sc, hypothesis)
		if err != nil {
			return Verdict{}, "", err
		}
		if ok {
			g.add(func(s *JudgeStats) {
				s.CacheHits++
				if m := e.Model; m != "" && !slices.Contains(s.CacheModels, m) {
					s.CacheModels = append(s.CacheModels, m)
					slices.Sort(s.CacheModels)
				}
			})
			return e.Verdict, ambiguityCache, nil
		}
	}
	if g.cacheOnly {
		dir := "(no cache configured)"
		if g.cache != nil {
			dir = g.cache.Dir()
		}
		return Verdict{}, "", fmt.Errorf("%w for scenario %q, key %s, in %s: add the text to the ambiguous-set manifest and fill the cache (evalrun -judge-fill, a paid run that needs the owner's approval)",
			ErrJudgeCacheMiss, sc.Name, JudgeCacheKey(JudgePromptVersion, sc.Name, hypothesis), dir)
	}
	if g.live == nil {
		return Verdict{}, "", unjudgedError{"no recorded verdict and no live judge (build evalrun with -tags livejudge)"}
	}
	g.mu.Lock()
	if g.maxCalls < 0 || g.stats.LiveCalls >= g.maxCalls {
		g.mu.Unlock()
		return Verdict{}, "", unjudgedError{fmt.Sprintf("the judge call cap (%d per run) is reached", max(g.maxCalls, 0))}
	}
	// Reserve the call before making it, so parallel runs cannot pass the cap.
	g.stats.LiveCalls++
	g.mu.Unlock()
	v, err := g.call(ctx, sc, hypothesis)
	if err != nil {
		return Verdict{}, "", err
	}
	g.add(func(s *JudgeStats) { s.LiveVerdicts++ })
	return v, ambiguityLive, nil
}

// gradeFinal applies the run's judge mode to a result Judge scored: terms
// keeps it; claude replaces the root cause and layers with a live verdict for
// every diagnose hypothesis; auto does so only when the tri-state pass finds
// an ambiguous item. The ambiguity is recorded in every mode.
func gradeFinal(ctx context.Context, cfg Config, g *judgeGate, sc Scenario, res RunResult) (RunResult, error) {
	// Nothing to grade without a hypothesis; the terms verdict already says
	// so. Healthy, abstain and ask scenarios have no root cause for a judge.
	if res.FinalHypothesis == "" || sc.Expect.outcome() != OutcomeDiagnose {
		return res, nil
	}
	mode := effectiveJudge(cfg.Judge, sc, cfg.DefaultJudge)
	if a := ambiguityOf(sc, res.FinalHypothesis); a.Any() {
		a.KeywordRootCause, a.KeywordLayer, a.KeywordRootCauseLayer = res.RootCause, res.LayerNamed, res.RootCauseLayerNamed
		res.Ambiguity = &a
		g.add(func(s *JudgeStats) { s.Ambiguous++ })
	}
	switch mode {
	case JudgeLLM:
		g.add(func(s *JudgeStats) { s.LiveCalls++; s.ClaudeModeCalls++ })
		v, err := g.call(ctx, sc, res.FinalHypothesis)
		if err != nil {
			return RunResult{}, err
		}
		return ApplyVerdict(sc, res, v), nil
	case JudgeAuto:
		if res.Ambiguity == nil {
			return res, nil
		}
		v, src, err := g.settle(ctx, sc, res.FinalHypothesis)
		var uj unjudgedError
		switch {
		case errors.As(err, &uj):
			g.add(func(s *JudgeStats) { s.Unjudged++ })
			return markUnjudged(sc, res, uj.why), nil
		case err != nil:
			return RunResult{}, err
		}
		g.add(func(s *JudgeStats) { s.Judged++ })
		res = ApplyVerdict(sc, res, v)
		res.Ambiguity.Source = src
		return res, nil
	}
	return res, nil
}

// markUnjudged makes every ambiguous item of res not passed and recomputes
// the outcome, score and failures.
func markUnjudged(sc Scenario, res RunResult, why string) RunResult {
	a := *res.Ambiguity
	a.Source, a.UnjudgedWhy = ambiguityUnjudged, why
	res.Ambiguity = &a
	if a.RootCause {
		res.RootCause = false
	}
	if a.Layer {
		res.LayerNamed = false
	}
	if a.RootCauseLayer {
		res.RootCauseLayerNamed = false
	}
	finish(sc, &res)
	return res
}

// unjudgedFailure is the failure line of an unjudged item.
func unjudgedFailure(item string, a *Ambiguity) string {
	return fmt.Sprintf("%s ambiguous (unjudged): %s", item, strings.TrimSpace(a.UnjudgedWhy))
}
