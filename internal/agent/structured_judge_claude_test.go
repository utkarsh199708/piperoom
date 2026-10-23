//go:build claude

package agent

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/agent/llm"
)

// The hand-off judge on a model without structured output: a verdict through
// each fallback, and a second bad answer leaves the verdict unjudged (an error).
func TestJudgeThroughEachFallback(t *testing.T) {
	c := caseByName(t, "schema-change")
	for _, caps := range []llm.Capabilities{toolCaps, textCaps} {
		m := &plainModel{caps: caps, answer: func(llm.Request, int) llm.Response {
			return answerFor(caps, "judge_verdict", `{"contradictions":[{"claim":"a","evidence":"b"}]}`)
		}}
		v, err := JudgeHandoffWith(context.Background(), m, c.events, c.summary, Options{Model: "gpt-x"})
		if err != nil || len(v.Contradictions) != 1 || v.Contradictions[0].Claim != "a" {
			t.Fatalf("tools=%v: verdict %+v err %v", caps.ToolCalls, v, err)
		}
		checkFallbackRequest(t, m.requests()[0], caps, "judge_verdict")

		bad := &plainModel{caps: caps, answer: func(llm.Request, int) llm.Response {
			return answerFor(caps, "judge_verdict", `{"verdict":"fine"}`)
		}}
		if _, err := JudgeHandoffWith(context.Background(), bad, c.events, c.summary, Options{Model: "gpt-x"}); err == nil || !strings.Contains(err.Error(), "2 attempts") {
			t.Fatalf("tools=%v: err = %v, want unjudged after 2 attempts", caps.ToolCalls, err)
		}
		if n := len(bad.requests()); n != 2 {
			t.Fatalf("tools=%v: requests = %d, want 2", caps.ToolCalls, n)
		}
	}
}
