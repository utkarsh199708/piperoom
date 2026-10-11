//go:build claude

package evals

import (
	"context"
	"os"
	"testing"
	"time"

	"piperoom/internal/agent"
)

// TestLiveSchemaChange runs the real agent and model on the seed scenario. It
// needs ANTHROPIC_API_KEY and the claude build tag:
//
//	go test -tags claude ./evals -run TestLiveSchemaChange -v
//
// It asserts only what a working agent must always do (find the cause);
// pass rate across runs is what evalrun measures.
func TestLiveSchemaChange(t *testing.T) {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("ANTHROPIC_API_KEY not set")
	}
	rep, err := Evaluate(context.Background(), []Scenario{loadSeed()}, Config{
		Label:   "live",
		Options: agent.Options{Model: "claude-opus-5-5"},
		Timeout: 10 * time.Minute,
		Logger:  quietLogger(),
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	r := rep.Results[0]
	t.Logf("pass=%v score=%.0f tool_calls=%d misses=%d failures=%v hypothesis=%q",
		r.Pass, r.Score, r.ToolCalls, len(r.Misses), r.Failures, r.FinalHypothesis)
	if !r.RootCause {
		t.Errorf("agent did not name the root cause: %q", r.FinalHypothesis)
	}
	if r.Error != "" || r.TimedOut {
		t.Errorf("run failed: error %q timed out %v", r.Error, r.TimedOut)
	}
}
