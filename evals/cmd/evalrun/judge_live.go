//go:build livejudge || claude

package main

import (
	"context"

	"piperoom/evals"
	"piperoom/internal/config"
)

// newJudge builds the LLM judge on provider (-judge-provider), else the
// configured model provider (PIPEROOM_MODEL_PROVIDER): the Anthropic API (key
// from ANTHROPIC_API_KEY), Bedrock or Vertex. It reads cloud credentials but
// calls no model.
func newJudge(model, provider string) (evals.HypothesisJudge, error) {
	cfg := config.Load()
	if provider != "" {
		cfg.ModelProvider = provider
	}
	j, err := evals.NewProviderJudge(context.Background(), cfg, model)
	if err != nil {
		return nil, err
	}
	return j, nil
}
