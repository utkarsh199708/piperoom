//go:build !claude

package main

import "piperoom/evals"

// newJudge refuses: the LLM judge is compiled in only with -tags claude, so
// the default binary never needs the API for judging.
func newJudge(string) (evals.HypothesisJudge, error) {
	return nil, errNoLiveJudge
}
