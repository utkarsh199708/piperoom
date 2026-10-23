//go:build claude

package agent

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/agent/llm"
)

// TestJudgeHandoffWithANeutralModel: the judge runs on any llm.Model,
// asking for its verdict schema as structured output with the capped effort.
func TestJudgeHandoffWithANeutralModel(t *testing.T) {
	c := caseByName(t, "schema-change")
	m := &neutralModel{answers: []llm.Response{textAnswer(`{"contradictions":[{"claim":"lag is 0","evidence":"lag=5"}]}`)}}
	v, err := JudgeHandoffWith(context.Background(), m, c.events, c.summary, Options{Model: "neutral-judge", Effort: "max"})
	if err != nil {
		t.Fatal(err)
	}
	if v.OK() || v.Contradictions[0].Evidence != "lag=5" {
		t.Fatalf("verdict = %+v", v)
	}
	req := m.reqs[0]
	if req.Model != "neutral-judge" || req.Effort != "medium" || req.System != judgePrompt() || req.MaxTokens != judgeMaxTokens {
		t.Fatalf("request = %+v", req)
	}
	if !reflect.DeepEqual(req.OutputSchema, judgeSchema()) {
		t.Fatalf("OutputSchema = %v, want the verdict schema", req.OutputSchema)
	}
	if req.Thinking || req.Cache != (llm.CacheHints{}) || len(req.Tools) != 0 {
		t.Fatalf("the judge asks for no thinking, cache or tools: %+v", req)
	}
	if !strings.Contains(req.Messages[0].Parts[0].Text, "<untrusted>") {
		t.Fatal("the summary must go in the untrusted wrapper")
	}

	// A verdict with trailing data is refused on every provider.
	m = &neutralModel{answers: []llm.Response{textAnswer(`{"contradictions":[]} {"x":1}`)}}
	if _, err := JudgeHandoffWith(context.Background(), m, c.events, c.summary, Options{}); err == nil {
		t.Fatal("trailing data must be refused")
	}
	if _, err := JudgeHandoffWith(context.Background(), nil, c.events, c.summary, Options{}); err == nil {
		t.Fatal("a nil model must be an error")
	}
}
