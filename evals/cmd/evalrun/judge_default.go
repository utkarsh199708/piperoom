//go:build !livejudge && !claude

package main

import "piperoom/evals"

// newJudge refuses: the live LLM judge is compiled in only with -tags
// livejudge (or its alias claude), so the default binary never needs a model
// API for judging.
func newJudge(string) (evals.HypothesisJudge, error) {
	return nil, errNoLiveJudge
}
