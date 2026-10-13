//go:build !claude

package main

import (
	"errors"

	"piperoom/evals"
)

// newJudge refuses: the LLM judge is compiled in only with -tags claude, so
// the default binary never needs the API for judging.
func newJudge(string) (evals.HypothesisJudge, error) {
	return nil, errors.New("the claude judge needs a build with -tags claude")
}
