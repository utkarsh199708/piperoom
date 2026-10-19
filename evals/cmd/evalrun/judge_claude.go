//go:build claude

package main

import (
	"context"

	"piperoom/evals"
	"piperoom/internal/config"
)

// newJudge builds the Claude judge on the configured model provider
// (PIPEROOM_MODEL_PROVIDER): the Anthropic API (key from ANTHROPIC_API_KEY),
// Bedrock or Vertex. It reads cloud credentials but calls no model.
func newJudge(model string) (evals.HypothesisJudge, error) {
	return evals.NewProviderJudge(context.Background(), config.Load(), model)
}
