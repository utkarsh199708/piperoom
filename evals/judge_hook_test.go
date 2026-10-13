//go:build !claude

package evals

// newTestClaudeJudge is nil without -tags claude: the agreement test skips.
var newTestClaudeJudge func() HypothesisJudge
