package evals

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/agent/llm"
)

// judgeFakeModel is an llm.Model that answers from a script and records the
// requests. structured is its Capabilities.StructuredOutput.
type judgeFakeModel struct {
	structured bool

	mu      sync.Mutex
	reqs    []llm.Request
	answers []judgeFakeAnswer
}

type judgeFakeAnswer struct {
	resp llm.Response
	err  error
}

func (m *judgeFakeModel) Turn(_ context.Context, req llm.Request) (llm.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reqs = append(m.reqs, req)
	if len(m.answers) == 0 {
		return llm.Response{}, errors.New("judgeFakeModel: no answer scripted")
	}
	a := m.answers[0]
	m.answers = m.answers[1:]
	return a.resp, a.err
}

func (m *judgeFakeModel) Capabilities(string) llm.Capabilities {
	return llm.Capabilities{StructuredOutput: m.structured}
}

func (m *judgeFakeModel) Provider() string { return "fake" }

func textReply(text string, stop llm.StopReason) judgeFakeAnswer {
	return judgeFakeAnswer{resp: llm.Response{
		Message: llm.Message{Role: llm.RoleAssistant, Parts: []llm.Part{{Kind: llm.PartText, Text: text}}},
		Stop:    stop,
		Usage:   llm.Usage{Input: 100, CacheRead: 10, CacheWrite: 1, Output: 20},
	}}
}

func toolReply(id, input string) judgeFakeAnswer {
	return judgeFakeAnswer{resp: llm.Response{
		Message: llm.Message{Role: llm.RoleAssistant, Parts: []llm.Part{{Kind: llm.PartToolCall,
			ToolCall: &llm.ToolCall{ID: id, Name: "emit_verdict", Input: json.RawMessage(input)}}}},
		Stop:  llm.StopToolUse,
		Usage: llm.Usage{Input: 50, Output: 5},
	}}
}

const goodVerdict = `{"layer_correct":true,"root_cause_correct":true,"root_cause_layer_correct":false,"reason":"ok"}`

// TestLLMJudgeRequest: the judge asks any model for the verdict schema as
// structured output at low effort, with the rubric as system prompt and the
// hypothesis wrapped as untrusted; billed cache tokens count as input.
func TestLLMJudgeRequest(t *testing.T) {
	m := &judgeFakeModel{structured: true, answers: []judgeFakeAnswer{textReply(goodVerdict, llm.StopEndTurn)}}
	j := NewLLMJudge(m, "judge-m")
	v, u, err := j.JudgeHypothesisMetered(context.Background(), loadSeed(), "amount became numeric")
	if err != nil {
		t.Fatal(err)
	}
	if !v.LayerCorrect || !v.RootCauseCorrect || v.RootCauseLayerCorrect || v.Reason != "ok" {
		t.Fatalf("verdict = %+v", v)
	}
	if u != (JudgeUsage{InputTokens: 111, OutputTokens: 20}) {
		t.Errorf("usage = %+v", u)
	}
	if len(m.reqs) != 1 {
		t.Fatalf("%d requests, want 1", len(m.reqs))
	}
	req := m.reqs[0]
	if req.Model != "judge-m" || req.System != judgeSystem || req.Effort != "low" || req.MaxTokens != judgeMaxTokens {
		t.Errorf("request = %+v", req)
	}
	if !reflect.DeepEqual(req.OutputSchema, verdictSchema) {
		t.Errorf("OutputSchema = %v", req.OutputSchema)
	}
	if req.Thinking || len(req.Tools) != 0 || req.Cache != (llm.CacheHints{}) || req.Compaction != nil {
		t.Errorf("the judge asks for no thinking, tools, cache or compaction: %+v", req)
	}
	if len(req.Messages) != 1 || req.Messages[0].Role != llm.RoleUser || req.Messages[0].Parts[0].Text != judgePrompt(loadSeed(), "amount became numeric") {
		t.Errorf("messages = %+v", req.Messages)
	}
	if !strings.Contains(req.Messages[0].Parts[0].Text, "<untrusted>amount became numeric</untrusted>") {
		t.Error("the hypothesis must go in the untrusted wrapper")
	}
	if got := j.JudgeID(); got != "fake:judge-m" {
		t.Errorf("JudgeID = %q", got)
	}
}

// TestLLMJudgeWithoutStructuredOutput: a model without structured output
// judges through the fallback: a forced tool call is read as the verdict, and
// a plain-text reply that is not the verdict gets one retry with the reply in
// the history.
func TestLLMJudgeWithoutStructuredOutput(t *testing.T) {
	t.Run("forced tool call", func(t *testing.T) {
		m := &judgeFakeModel{answers: []judgeFakeAnswer{toolReply("call_1", goodVerdict)}}
		v, err := NewLLMJudge(m, "m").JudgeHypothesis(context.Background(), loadSeed(), "x")
		if err != nil || !v.RootCauseCorrect {
			t.Fatalf("v = %+v err = %v", v, err)
		}
		if len(m.reqs) != 1 {
			t.Errorf("%d requests, want 1", len(m.reqs))
		}
	})

	t.Run("plain text, one retry", func(t *testing.T) {
		m := &judgeFakeModel{answers: []judgeFakeAnswer{
			textReply("Here is my verdict: the root cause is right.", llm.StopEndTurn),
			textReply("```json\n"+goodVerdict+"\n```", llm.StopEndTurn),
		}}
		v, u, err := NewLLMJudge(m, "m").JudgeHypothesisMetered(context.Background(), loadSeed(), "x")
		if err != nil || !v.RootCauseCorrect {
			t.Fatalf("v = %+v err = %v", v, err)
		}
		if u != (JudgeUsage{InputTokens: 222, OutputTokens: 40}) {
			t.Errorf("usage over both calls = %+v", u)
		}
		if len(m.reqs) != 2 {
			t.Fatalf("%d requests, want 2", len(m.reqs))
		}
		retry := m.reqs[1].Messages
		if len(retry) != 3 || retry[1].Role != llm.RoleAssistant || retry[1].Parts[0].Text != "Here is my verdict: the root cause is right." ||
			retry[2].Role != llm.RoleUser || retry[2].Parts[0].Text != judgeRetryNote {
			t.Errorf("retry history = %+v", retry)
		}
		if !reflect.DeepEqual(m.reqs[1].OutputSchema, verdictSchema) || m.reqs[1].System != judgeSystem {
			t.Error("the retry asks for the same verdict")
		}
	})

	t.Run("a bad tool call is answered before the retry note", func(t *testing.T) {
		m := &judgeFakeModel{answers: []judgeFakeAnswer{toolReply("call_9", `{"verdict":"yes"}`), toolReply("call_10", goodVerdict)}}
		if _, err := NewLLMJudge(m, "m").JudgeHypothesis(context.Background(), loadSeed(), "x"); err != nil {
			t.Fatal(err)
		}
		last := m.reqs[1].Messages[2]
		if len(last.Parts) != 2 || last.Parts[0].Kind != llm.PartToolResult || last.Parts[0].ToolResult.CallID != "call_9" ||
			!last.Parts[0].ToolResult.IsError || last.Parts[1].Text != judgeRetryNote {
			t.Errorf("retry turn = %+v", last)
		}
	})

	t.Run("two bad replies are an error, never a guessed verdict", func(t *testing.T) {
		m := &judgeFakeModel{answers: []judgeFakeAnswer{textReply("yes", llm.StopEndTurn), textReply("still yes", llm.StopEndTurn)}}
		if v, err := NewLLMJudge(m, "m").JudgeHypothesis(context.Background(), loadSeed(), "x"); err == nil {
			t.Fatalf("verdict %+v from two bad replies", v)
		}
		if len(m.reqs) != 2 {
			t.Errorf("%d requests, want 2 (one retry only)", len(m.reqs))
		}
	})
}

// TestLLMJudgeStructuredModelNoRetry: a model with structured output gets no
// retry (the Claude judge makes one call per verdict, as before V9).
func TestLLMJudgeStructuredModelNoRetry(t *testing.T) {
	m := &judgeFakeModel{structured: true, answers: []judgeFakeAnswer{textReply("yes", llm.StopEndTurn), textReply(goodVerdict, llm.StopEndTurn)}}
	if _, err := NewLLMJudge(m, "m").JudgeHypothesis(context.Background(), loadSeed(), "x"); err == nil {
		t.Fatal("a bad reply was accepted")
	}
	if len(m.reqs) != 1 {
		t.Errorf("%d requests, want 1", len(m.reqs))
	}
}

// TestLLMJudgeErrors: refusal, a cut-off verdict, an unknown stop, an empty
// reply and a provider error are clear errors; the provider error keeps its
// llm.APIError, so APIRefusal still reads it.
func TestLLMJudgeErrors(t *testing.T) {
	apiErr := &llm.APIError{Provider: "fake", Status: 401, Kind: llm.ErrAuth}
	for name, tc := range map[string]struct {
		a    judgeFakeAnswer
		want string
	}{
		"refusal":    {textReply(goodVerdict, llm.StopRefusal), "refused"},
		"max tokens": {textReply(`{"layer_correct":`, llm.StopMaxTokens), "cut off"},
		"pause":      {textReply(goodVerdict, llm.StopPause), "unexpected stop reason"},
		"empty":      {judgeFakeAnswer{resp: llm.Response{Stop: llm.StopEndTurn}}, "no text"},
		"api error":  {judgeFakeAnswer{err: apiErr}, "auth"},
	} {
		t.Run(name, func(t *testing.T) {
			for _, structured := range []bool{true, false} {
				m := &judgeFakeModel{structured: structured, answers: []judgeFakeAnswer{tc.a, textReply(goodVerdict, llm.StopEndTurn)}}
				_, err := NewLLMJudge(m, "m").JudgeHypothesis(context.Background(), loadSeed(), "x")
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("structured %v: err = %v, want %q", structured, err, tc.want)
				}
				if len(m.reqs) != 1 {
					t.Errorf("structured %v: %d requests; a failed call is not retried", structured, len(m.reqs))
				}
			}
		})
	}
	m := &judgeFakeModel{answers: []judgeFakeAnswer{{err: apiErr}}}
	_, err := NewLLMJudge(m, "m").JudgeHypothesis(context.Background(), loadSeed(), "x")
	if status, ok := APIRefusal(err); !ok || status != 401 {
		t.Errorf("APIRefusal(judge error) = %d, %v", status, ok)
	}
	if _, err := NewLLMJudge(nil, "m").JudgeHypothesis(context.Background(), loadSeed(), "x"); err == nil {
		t.Error("a judge without a model judged")
	}
}

// TestJudgeModeAlias: "llm" is the judge mode and "claude" its alias,
// wherever a mode is named.
func TestJudgeModeAlias(t *testing.T) {
	for _, m := range []string{JudgeLLM, JudgeClaude} {
		if !validJudge(m) || !IsLLMJudge(m) {
			t.Errorf("%q is not a valid LLM judge mode", m)
		}
		if got := effectiveJudge(m, Scenario{}, JudgeAuto); got != JudgeLLM {
			t.Errorf("flag %q: effective %q", m, got)
		}
		if got := effectiveJudge("", Scenario{Judge: m}, JudgeAuto); got != JudgeLLM {
			t.Errorf("scenario %q: effective %q", m, got)
		}
	}
	if IsLLMJudge(JudgeAuto) || IsLLMJudge("") {
		t.Error("auto and unset are not the LLM judge mode")
	}
	if got := effectiveJudge("", Scenario{}, JudgeAuto); got != JudgeAuto {
		t.Errorf("default: %q", got)
	}
}

// TestJudgeCacheRecordsProviderModel: an entry records the judge as
// "provider:model" under the same key as before, and an entry recorded
// before V9 with a bare model still loads.
func TestJudgeCacheRecordsProviderModel(t *testing.T) {
	if got := JudgeID("anthropic", "claude-opus-5-5"); got != "anthropic:claude-opus-5-5" {
		t.Errorf("JudgeID = %q", got)
	}
	if got := JudgeID("", "m"); got != "m" {
		t.Errorf("JudgeID without a provider = %q", got)
	}
	sc := loadSeed()
	dir := t.TempDir()
	c := NewJudgeCache(dir)
	v := Verdict{LayerCorrect: true, RootCauseCorrect: true, RootCauseLayerCorrect: true, Reason: "ok"}
	if err := c.Put(sc, "new text", JudgeID("openai", "gpt-x"), v); err != nil {
		t.Fatal(err)
	}
	key := JudgeCacheKey(JudgePromptVersion, sc.Name, "new text")
	if _, err := os.Stat(filepath.Join(dir, key+".json")); err != nil {
		t.Fatalf("the key changed: %v", err)
	}
	e, ok, err := c.Lookup(sc, "new text")
	if err != nil || !ok || e.Model != "openai:gpt-x" || e.Verdict != v {
		t.Fatalf("entry = %+v ok %v err %v", e, ok, err)
	}

	// A pre-V9 entry: the bare model, written by hand as the old fill did.
	old := JudgeCacheEntry{PromptVersion: JudgePromptVersion, Scenario: sc.Name, Hypothesis: "old text",
		TruthSHA256: judgeTruthSHA(sc), Model: "claude-opus-5-5", Verdict: v}
	b, err := json.MarshalIndent(old, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, JudgeCacheKey(JudgePromptVersion, sc.Name, "old text")+".json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	e, ok, err = c.Lookup(sc, "old text")
	if err != nil || !ok || e.Model != "claude-opus-5-5" || e.Verdict != v {
		t.Fatalf("pre-V9 entry = %+v ok %v err %v", e, ok, err)
	}
}

// TestFillRecordsJudgeID: a fill with the LLM judge records its
// "provider:model" in every entry it writes.
func TestFillRecordsJudgeID(t *testing.T) {
	set, err := LoadAmbiguousSet(filepath.Join("judgecache", "ambiguous.json"))
	if err != nil {
		t.Fatal(err)
	}
	def, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	held, err := LoadScenarios(filepath.Join("scenarios", "heldout"))
	if err != nil {
		t.Fatal(err)
	}
	set.Entries = set.Entries[:1]
	m := &judgeFakeModel{structured: true, answers: []judgeFakeAnswer{textReply(goodVerdict, llm.StopEndTurn)}}
	j := NewLLMJudge(m, "judge-m")
	c := NewJudgeCache(t.TempDir())
	res, err := FillJudgeCache(context.Background(), c, j, j.JudgeID(), ManifestScenarios(def, held), set, 5)
	if err != nil || res.Filled != 1 {
		t.Fatalf("res = %+v err = %v", res, err)
	}
	sc := ManifestScenarios(def, held)[set.Entries[0].Scenario]
	e, ok, err := c.Lookup(sc, set.Entries[0].Text)
	if err != nil || !ok || e.Model != "fake:judge-m" {
		t.Fatalf("entry = %+v ok %v err %v", e, ok, err)
	}
}
