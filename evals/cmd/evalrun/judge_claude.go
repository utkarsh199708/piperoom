//go:build claude

package main

import "piperoom/evals"

// newJudge builds the Claude judge (SDK client; key from ANTHROPIC_API_KEY).
func newJudge(model string) (evals.HypothesisJudge, error) {
	return evals.NewClaudeJudge(model), nil
}
