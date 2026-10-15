package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"piperoom/evals"
	"piperoom/internal/agent"
	"piperoom/internal/config"
)

// runJudgeFill records judge verdicts for the ambiguous-set manifest
// (evals/judgefill.go). It reads only committed scenarios (the default set
// and the held-out set, never -learned), so only synthetic texts reach the
// public cache. A dry run makes no call and prints the estimate; a real fill
// is a paid run that needs the owner's approval (#454).
func runJudgeFill(f flags, stdout io.Writer) (int, error) {
	if f.learned != "" {
		return 1, errors.New("-judge-fill never reads -learned: learned text must not reach the public verdict cache")
	}
	if f.judgeCache == "" {
		return 1, errors.New("-judge-fill needs -judge-cache")
	}
	set, err := evals.LoadAmbiguousSet(f.judgeFill)
	if err != nil {
		return 1, err
	}
	def, err := evals.LoadScenarios(f.scenarios)
	if err != nil {
		return 1, err
	}
	held, err := evals.LoadScenarios(filepath.Join(f.scenarios, setHeldout))
	if err != nil {
		return 1, err
	}
	scs := evals.ManifestScenarios(def, held)
	cache := evals.NewJudgeCache(f.judgeCache)

	cfg := config.Load()
	model := f.judgeModel
	if model == "" {
		model = cfg.Model
	}
	pricesFile := f.prices
	if pricesFile == "" {
		pricesFile = cfg.ModelPrices
	}
	prices, err := agent.LoadModelPrices(pricesFile)
	if err != nil {
		return 1, err
	}

	var judge evals.HypothesisJudge
	if !f.dryRun {
		if p := strings.ToLower(strings.TrimSpace(cfg.ModelProvider)); (p == "" || p == agent.ProviderAnthropic) && os.Getenv("ANTHROPIC_API_KEY") == "" {
			return 1, errors.New("ANTHROPIC_API_KEY is not set (-dry-run estimates the fill without it)")
		}
		if judge, err = newJudge(f.judgeModel); err != nil {
			return 1, err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	res, err := evals.FillJudgeCache(ctx, cache, judge, scs, set, f.maxCalls)
	fmt.Fprintf(stdout, "judge fill (%s, prompt %s, cache %s): %s\n", f.judgeFill, evals.JudgePromptVersion, f.judgeCache, res)
	if f.dryRun {
		fmt.Fprintf(stdout, "dry run: %d judge call(s) needed, about %d input and %d output tokens on %s%s\n",
			res.Remaining, res.EstInputTokens, res.EstOutputTokens, model, costNote(prices, model, res.EstInputTokens, res.EstOutputTokens))
		if res.Remaining > f.maxCalls {
			fmt.Fprintf(stdout, "a fill stops after -judge-max-calls %d; pass -judge-max-calls %d to fill everything in one run\n", f.maxCalls, res.Remaining)
		}
	} else if res.Remaining > 0 && err == nil {
		fmt.Fprintf(stdout, "stopped at -judge-max-calls %d: %d entries still need a verdict\n", f.maxCalls, res.Remaining)
	}
	if err != nil {
		return 1, err
	}
	return 0, nil
}

// costNote prices an estimate when the table knows the model.
func costNote(prices agent.Prices, model string, in, out int64) string {
	p, ok := prices[model]
	if !ok {
		return " (no price for the model: pass -prices)"
	}
	usd := (float64(in)*p.Input + float64(out)*p.Output) / 1e6
	return fmt.Sprintf(", about $%.2f at $%.2f / $%.2f per million input / output tokens", usd, p.Input, p.Output)
}
