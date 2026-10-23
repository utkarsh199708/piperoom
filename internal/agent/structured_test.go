package agent

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent/llm"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// plainModel is a provider-neutral fake without structured output. answer
// picks the reply to the n-th request (0-based).
type plainModel struct {
	caps   llm.Capabilities
	answer func(req llm.Request, n int) llm.Response

	mu   sync.Mutex
	reqs []llm.Request
}

func (m *plainModel) Turn(_ context.Context, req llm.Request) (llm.Response, error) {
	m.mu.Lock()
	n := len(m.reqs)
	m.reqs = append(m.reqs, req)
	m.mu.Unlock()
	return m.answer(req, n), nil
}

func (m *plainModel) Capabilities(string) llm.Capabilities { return m.caps }
func (m *plainModel) Provider() string                     { return llm.ProviderOpenAI }

func (m *plainModel) requests() []llm.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]llm.Request(nil), m.reqs...)
}

var (
	toolCaps = llm.Capabilities{ToolCalls: true}
	textCaps = llm.Capabilities{}
)

func emitAnswer(what, input string) llm.Response {
	return llm.Response{
		Message: llm.Message{Role: llm.RoleAssistant, Parts: []llm.Part{{Kind: llm.PartToolCall, ToolCall: &llm.ToolCall{ID: "call_1", Name: emitToolName(what), Input: json.RawMessage(input)}}}},
		Stop:    llm.StopToolUse,
		Usage:   llm.Usage{Input: 100, Output: 20},
	}
}

// proseAnswer wraps obj in prose and a code fence, as a chat model writes it.
func proseAnswer(obj string) llm.Response {
	return textAnswer("Here is the answer {not json}:\n```json\n" + obj + "\n```\nLet me know.")
}

// answerFor returns good in the fallback's shape for caps.
func answerFor(caps llm.Capabilities, what, good string) llm.Response {
	if caps.ToolCalls {
		return emitAnswer(what, good)
	}
	return proseAnswer(good)
}

// checkFallbackRequest asserts the request is fallback (a) or (b) for caps.
func checkFallbackRequest(t *testing.T, req llm.Request, caps llm.Capabilities, what string) {
	t.Helper()
	if req.OutputSchema != nil {
		t.Fatal("a model without structured output was sent OutputSchema")
	}
	if caps.ToolCalls {
		if len(req.Tools) != 1 || req.Tools[0].Name != emitToolName(what) || req.Tools[0].Schema == nil || req.ToolChoice != llm.ToolChoiceRequired {
			t.Fatalf("fallback (a): tools %+v choice %q", req.Tools, req.ToolChoice)
		}
		return
	}
	if len(req.Tools) != 0 || !strings.Contains(req.System, "JSON Schema") || !strings.Contains(req.System, `"required"`) {
		t.Fatalf("fallback (b): tools %d, system tail %q", len(req.Tools), req.System[max(0, len(req.System)-200):])
	}
}

func TestFirstJSONObject(t *testing.T) {
	for _, tt := range []struct {
		in, want string
		ok       bool
	}{
		{`{"a":1}`, `{"a":1}`, true},
		{"text {nope} then ```json\n{\"a\":{\"b\":[1]}}\n``` more {\"c\":2}", `{"a":{"b":[1]}}`, true},
		{`{"a":"}"}`, `{"a":"}"}`, true},
		{`no object [1,2]`, ``, false},
		{`{"a":`, ``, false},
		{``, ``, false},
	} {
		got, ok := firstJSONObject(tt.in)
		if ok != tt.ok || string(got) != tt.want {
			t.Errorf("firstJSONObject(%q) = %s, %v; want %s, %v", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}

// A model with structured output gets the request unchanged and one turn,
// even when the answer is bad: the Claude path is not touched by V2a.
func TestStructuredTurnUnchangedWithStructuredOutput(t *testing.T) {
	m := &neutralModel{answers: []llm.Response{textAnswer("not json")}}
	req := llm.Request{Model: "m", System: "s", OutputSchema: judgeLikeSchema(), Messages: []llm.Message{userMessage(textPart("x"))}}
	_, text, err := structuredTurn(context.Background(), m.Turn, m.Capabilities("m"), req, "x")
	if err != nil || text != "not json" {
		t.Fatalf("text %q err %v", text, err)
	}
	if len(m.reqs) != 1 || m.reqs[0].OutputSchema == nil || len(m.reqs[0].Tools) != 0 || m.reqs[0].System != "s" || m.reqs[0].ToolChoice != llm.ToolChoiceAuto {
		t.Fatalf("request changed: %+v", m.reqs)
	}
}

func judgeLikeSchema() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{"ok": map[string]any{"type": "boolean"}}, "required": []string{"ok"}, "additionalProperties": false}
}

// A first bad answer is retried once with the answer replayed: in fallback
// (a) each tool call gets a paired error tool_result.
func TestStructuredTurnRetryPairsToolCalls(t *testing.T) {
	for _, caps := range []llm.Capabilities{toolCaps, textCaps} {
		m := &plainModel{caps: caps, answer: func(_ llm.Request, n int) llm.Response {
			if n == 0 {
				return answerFor(caps, "x", `{"ok":"yes","extra":1}`)
			}
			return answerFor(caps, "x", `{"ok":true}`)
		}}
		req := llm.Request{Model: "m", System: "s", OutputSchema: judgeLikeSchema(), Messages: []llm.Message{userMessage(textPart("x"))}}
		_, text, err := structuredTurn(context.Background(), m.Turn, caps, req, "x")
		if err != nil || text != `{"ok":true}` {
			t.Fatalf("tools=%v: text %q err %v", caps.ToolCalls, text, err)
		}
		reqs := m.requests()
		if len(reqs) != 2 {
			t.Fatalf("requests = %d, want 2", len(reqs))
		}
		checkFallbackRequest(t, reqs[1], caps, "x")
		msgs := reqs[1].Messages
		if len(msgs) != 3 || msgs[1].Role != llm.RoleAssistant || msgs[2].Role != llm.RoleUser {
			t.Fatalf("retry history = %+v", msgs)
		}
		last := msgs[2].Parts
		if caps.ToolCalls {
			if last[0].Kind != llm.PartToolResult || last[0].ToolResult.CallID != "call_1" || !last[0].ToolResult.IsError {
				t.Fatalf("the emit call is not paired: %+v", last[0])
			}
		}
		if note := last[len(last)-1].Text; !strings.Contains(note, "<untrusted>") || !strings.Contains(note, "does not match the schema") {
			t.Fatalf("retry note = %q", note)
		}
		if len(req.Messages) != 1 {
			t.Fatal("the caller's request was mutated")
		}
	}
}

func TestSummariesThroughEachFallback(t *testing.T) {
	c := caseByName(t, "schema-change")
	good := summaryJSON(t, c.summary)
	for _, caps := range []llm.Capabilities{toolCaps, textCaps} {
		for _, what := range []string{"handoff", "postmortem"} {
			m := &plainModel{caps: caps, answer: func(_ llm.Request, _ int) llm.Response { return answerFor(caps, what, good) }}
			opts := Options{Model: "gpt-x"}.withDefaults()
			var whatBroke string
			if what == "handoff" {
				s, err := writeHandoff(context.Background(), c.events, opts, m)
				if err != nil {
					t.Fatalf("tools=%v handoff: %v", caps.ToolCalls, err)
				}
				whatBroke = s.WhatBroke
			} else {
				d, err := writePostmortem(context.Background(), c.events, opts, m)
				if err != nil {
					t.Fatalf("tools=%v postmortem: %v", caps.ToolCalls, err)
				}
				whatBroke = d.Summary.WhatBroke
			}
			if whatBroke != c.summary.WhatBroke {
				t.Fatalf("tools=%v %s: what_broke %q", caps.ToolCalls, what, whatBroke)
			}
			reqs := m.requests()
			checkFallbackRequest(t, reqs[0], caps, what)
		}
	}
}

// Two bad answers are the existing failure path: writePostmortem returns an
// error (the room service emits postmortem.failed) after exactly two requests.
func TestPostmortemSecondBadAnswerFails(t *testing.T) {
	c := caseByName(t, "schema-change")
	for _, caps := range []llm.Capabilities{toolCaps, textCaps} {
		m := &plainModel{caps: caps, answer: func(_ llm.Request, _ int) llm.Response {
			return answerFor(caps, "postmortem", `{"what_broke":5}`)
		}}
		_, err := writePostmortem(context.Background(), c.events, Options{Model: "gpt-x"}.withDefaults(), m)
		if err == nil || !strings.Contains(err.Error(), "2 attempts") {
			t.Fatalf("tools=%v: err = %v, want a failure after 2 attempts", caps.ToolCalls, err)
		}
		if n := len(m.requests()); n != 2 {
			t.Fatalf("tools=%v: requests = %d, want 2", caps.ToolCalls, n)
		}
	}
}

// A refusal in the fallback is a refusal, not a retry.
func TestFallbackRefusalIsNotRetried(t *testing.T) {
	c := caseByName(t, "schema-change")
	m := &plainModel{caps: toolCaps, answer: func(llm.Request, int) llm.Response {
		return llm.Response{Message: llm.Message{Role: llm.RoleAssistant}, Stop: llm.StopRefusal}
	}}
	if _, err := writeHandoff(context.Background(), c.events, Options{}.withDefaults(), m); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("err = %v, want a refusal", err)
	}
	if n := len(m.requests()); n != 1 {
		t.Fatalf("requests = %d, want 1", n)
	}
}

// firstLookOn runs the first look on an endpoint whose model is m and
// returns its agent.triage payload.
func firstLookOn(t *testing.T, m llm.Model) events.AgentTriagePayload {
	t.Helper()
	ep := testEndpoint(&scriptedModel{}, 0)
	ep.model = m
	ep.triagePass = triagePassConfig{on: true, model: "gpt-x-mini", timeout: 5 * time.Second}
	sink := &fakeSink{}
	reg := tools.NewRegistry()
	if err := reg.Register(&recordingTool{name: "connect_status", schema: testSchema(), result: tools.Result{Summary: "task 0 FAILED"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := ep.Triage(context.Background(), Alert{RoomID: "r_1", PipelineID: "orders", Title: "orders stale"},
		Deps{Tools: reg, Sink: sink, Logger: discardLogger()}, Options{}); err != nil {
		t.Fatalf("Triage: %v", err)
	}
	evs := sink.ofType(events.AgentTriage)
	if len(evs) != 1 {
		t.Fatalf("agent.triage events = %d", len(evs))
	}
	var p events.AgentTriagePayload
	if err := json.Unmarshal(evs[0].payload, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

// isVerdictRequest tells the first look's verdict request from its pick.
func isVerdictRequest(req llm.Request) bool {
	return strings.Contains(req.System, "JSON Schema") || (len(req.Tools) == 1 && req.Tools[0].Name == emitToolName("first_look"))
}

func TestFirstLookThroughEachFallback(t *testing.T) {
	const good = `{"headline":"the connector task failed","suspected_layer":"","confidence":"low","next_check":"connect_status","based_on_steps":[],"evidence_steps":[]}`
	for _, caps := range []llm.Capabilities{toolCaps, textCaps} {
		m := &plainModel{caps: caps, answer: func(req llm.Request, _ int) llm.Response {
			if isVerdictRequest(req) {
				return answerFor(caps, "first_look", good)
			}
			return textAnswer("no reads")
		}}
		p := firstLookOn(t, m)
		if p.Status != "ok" || p.Headline != "the connector task failed" {
			t.Fatalf("tools=%v: payload %+v", caps.ToolCalls, p)
		}
		var verdicts int
		for _, req := range m.requests() {
			if isVerdictRequest(req) {
				verdicts++
				checkFallbackRequest(t, req, caps, "first_look")
			}
		}
		if verdicts != 1 {
			t.Fatalf("tools=%v: verdict requests = %d", caps.ToolCalls, verdicts)
		}
	}
}

func TestFirstLookSecondBadAnswerIsModelError(t *testing.T) {
	for _, caps := range []llm.Capabilities{toolCaps, textCaps} {
		m := &plainModel{caps: caps, answer: func(req llm.Request, _ int) llm.Response {
			if isVerdictRequest(req) {
				return answerFor(caps, "first_look", `{"headline":"x"}`)
			}
			return textAnswer("no reads")
		}}
		p := firstLookOn(t, m)
		if p.Reason != triageReasonModelError {
			t.Fatalf("tools=%v: reason %q, want model_error", caps.ToolCalls, p.Reason)
		}
		var verdicts int
		for _, req := range m.requests() {
			if isVerdictRequest(req) {
				verdicts++
			}
		}
		if verdicts != 2 {
			t.Fatalf("tools=%v: verdict requests = %d, want 2", caps.ToolCalls, verdicts)
		}
	}
}
