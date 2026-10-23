//go:build livejudge || claude

package main

import (
	"context"

	"piperoom/evals"
	"piperoom/internal/config"
)

// newJudge builds the LLM judge on the configured model provider
// (PIPEROOM_MODEL_PROVIDER): the Anthropic API (key from ANTHROPIC_API_KEY),
// Bedrock or Vertex. It reads cloud credentials but calls no model.
func newJudge(model string) (evals.HypothesisJudge, error) {
	j, err := evals.NewProviderJudge(context.Background(), config.Load(), model)
	if err != nil {
		return nil, err
	}
	return j, nil
}
