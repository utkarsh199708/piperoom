package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"piperoom/evals"
	"piperoom/internal/agent"
	"piperoom/internal/config"
)

// tappedRunner builds the agent runner and the model tap its results read
// (nil for an untapped run, never a typed nil). anthropic goes through the
// HTTP tap, installed first because the SDK reads its base URL when the
// Endpoint's client is built; openai, gemini and bedrock-converse through the
// in-process tap that wraps each role's llm.Model (V11); bedrock and vertex
// stay untapped, as before. -no-tap turns both off.
func tappedRunner(ctx context.Context, cfg config.Config, noTap bool, log *slog.Logger, stderr io.Writer) (agent.Runner, evals.PromptTap, error) {
	if noTap {
		run, err := evals.NewEndpointRunner(ctx, cfg, log)
		return run, nil, err
	}
	if evals.NeutralTapProvider(cfg.ModelProvider) {
		nt := evals.NewNeutralTap()
		run, tapped, err := evals.NewTappedEndpointRunner(ctx, cfg, log, nt.Wrap)
		if err != nil {
			return nil, nil, err
		}
		if !tapped {
			fmt.Fprintln(stderr, "evalrun: note: the agent has no model-wrap hook yet, so this run is untapped (prompt_hash \"untapped\")")
			return run, nil, nil
		}
		return run, nt, nil
	}
	ht, err := evals.InstallModelTap(cfg.ModelProvider)
	if err != nil {
		return nil, nil, err
	}
	run, err := evals.NewEndpointRunner(ctx, cfg, log)
	if err != nil {
		if ht != nil {
			_ = ht.Close()
		}
		return nil, nil, err
	}
	if ht == nil {
		return run, nil, nil
	}
	return run, ht, nil
}
