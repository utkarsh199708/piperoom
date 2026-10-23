//go:build !livejudge && !claude

package evals

// newTestClaudeJudge is nil without -tags claude: the agreement test skips.
var newTestClaudeJudge func() HypothesisJudge
