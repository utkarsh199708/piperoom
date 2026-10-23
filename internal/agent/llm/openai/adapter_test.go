package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/retry"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
)

const testKey = netsec.Secret("sk-proj-adapter-NOT-REAL-91be3d")

// stub is a scripted chat completions host that records every request.
type stub struct {
	*httptest.Server
	mu      sync.Mutex
	reqs    []recorded
	replies []func(w http.ResponseWriter)
}

type recorded struct {
	path   string
	header http.Header
	body   []byte
}

func newStub(t *testing.T, replies ...func(w http.ResponseWriter)) *stub {
	t.Helper()
	s := &stub{replies: replies}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.reqs = append(s.reqs, recorded{path: r.URL.Path, header: r.Header.Clone(), body: b})
		var reply func(http.ResponseWriter)
		if len(s.replies) > 0 {
			reply = s.replies[0]
			if len(s.replies) > 1 {
				s.replies = s.replies[1:]
			}
		}
		s.mu.Unlock()
		if reply == nil {
			http.Error(w, "no reply", http.StatusTeapot)
			return
		}
		reply(w)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *stub) requests() []recorded {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recorded(nil), s.reqs...)
}

func jsonReply(status int, v any, header ...string) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		for i := 0; i+1 < len(header); i += 2 {
			w.Header().Set(header[i], header[i+1])
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
}

func completion(message map[string]any, finish string, usage any) map[string]any {
	if message["role"] == nil {
		message["role"] = "assistant"
	}
	v := map[string]any{
		"id": "chatcmpl-1", "object": "chat.completion", "model": "served-model",
		"choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finish}},
	}
	if usage != nil {
		v["usage"] = usage
	}
	return v
}

func okText(text string) func(http.ResponseWriter) {
	return jsonReply(200, completion(map[string]any{"content": text}, "stop",
		map[string]any{"prompt_tokens": 900, "completion_tokens": 7}))
}

func errReply(status int, code, message string, header ...string) func(http.ResponseWriter) {
	return jsonReply(status, map[string]any{"error": map[string]any{"message": message, "type": "invalid_request_error", "code": code}}, header...)
}

// sleeps records the retry waits and returns at once.
type sleeps struct {
	mu    sync.Mutex
	waits []time.Duration
}

func (s *sleeps) sleep(_ context.Context, d time.Duration) error {
	s.mu.Lock()
	s.waits = append(s.waits, d)
	s.mu.Unlock()
	return nil
}

// build makes a Model for flavor at url (path /v1 appended) with o's other
// settings; retries are off unless o.Retry is set.
func build(t *testing.T, flavor, url string, o Options, h hooks) *Model {
	t.Helper()
	b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", url+"/v1", o.AllowPrivateHTTP)
	if err != nil {
		t.Fatalf("ParseBaseURL: %v", err)
	}
	o.Flavor, o.BaseURL = flavor, &b
	if o.Retry == nil {
		o.Retry = &retry.Policy{}
	}
	if o.Key == "" && (flavor == FlavorOpenAI || flavor == FlavorAzure) {
		o.Key = testKey
	}
	m, err := newModel(o, h)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

func userText(s string) llm.Message {
	return llm.Message{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartText, Text: s}}}
}

func simpleRequest(model string) llm.Request {
	return llm.Request{Model: model, System: "You investigate pipelines.", MaxTokens: 512, Messages: []llm.Message{userText("hi")}}
}

func apiErr(t *testing.T, err error) *llm.APIError {
	t.Helper()
	var ae *llm.APIError
	if !errors.As(err, &ae) {
		t.Fatalf("err = %v (%T), want an *llm.APIError", err, err)
	}
	return ae
}

var strictTool = llm.ToolDef{
	Name: "connect_status", Description: "Connector state.", Strict: true,
	Schema: map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"connector": map[string]any{"type": "string"}, "why": map[string]any{"type": "string"}},
		"required":             []string{"connector", "why"},
		"additionalProperties": false,
	},
}

// looseTool asks for strict but has an optional property: it must go
// non-strict, unchanged.
var looseTool = llm.ToolDef{
	Name: "kafka_lag", Description: "Consumer lag.", Strict: true,
	Schema: map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"group": map[string]any{"type": "string"}, "topic": map[string]any{"type": "string"}},
		"required":             []string{"group"},
		"additionalProperties": false,
	},
}

func decodeBody(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("request body: %v", err)
	}
	return v
}

func TestWireRequestOpenAIReasoningModel(t *testing.T) {
	s := newStub(t, okText("ok"))
	m := build(t, FlavorOpenAI, s.URL, Options{}, hooks{})
	req := simpleRequest("gpt-5.2")
	req.Tools = []llm.ToolDef{strictTool, looseTool}
	req.Effort = "xhigh"
	req.Thinking = true
	req.ToolChoice = llm.ToolChoiceRequired
	req.Cache = llm.CacheHints{System: true, Tools: true, History: true}
	req.Compaction = &llm.Compaction{Instructions: "compact"}
	if _, err := m.Turn(context.Background(), req); err != nil {
		t.Fatalf("Turn: %v", err)
	}
	r := s.requests()[0]
	if r.path != "/v1/chat/completions" {
		t.Fatalf("path = %q", r.path)
	}
	if got := r.header.Get("Authorization"); got != "Bearer "+testKey.Reveal() {
		t.Fatalf("Authorization = %q", got)
	}
	if r.header.Get("api-key") != "" {
		t.Fatal("openai sent an Azure api-key header")
	}
	v := decodeBody(t, r.body)
	msgs := v["messages"].([]any)
	if role := msgs[0].(map[string]any)["role"]; role != "developer" {
		t.Fatalf("system role = %v, want developer for a reasoning model", role)
	}
	if v["max_completion_tokens"] != float64(512) || v["max_tokens"] != nil {
		t.Fatalf("token field: max_completion_tokens=%v max_tokens=%v", v["max_completion_tokens"], v["max_tokens"])
	}
	if v["reasoning_effort"] != "high" {
		t.Fatalf("reasoning_effort = %v, want high for xhigh", v["reasoning_effort"])
	}
	if v["tool_choice"] != "required" {
		t.Fatalf("tool_choice = %v", v["tool_choice"])
	}
	tools := v["tools"].([]any)
	strict := map[string]bool{}
	for _, tl := range tools {
		f := tl.(map[string]any)["function"].(map[string]any)
		strict[f["name"].(string)] = f["strict"] == true
	}
	if !strict["connect_status"] || strict["kafka_lag"] {
		t.Fatalf("strict = %v; only the schema already in the strict subset goes strict", strict)
	}
	for _, k := range []string{"temperature", "cache_control", "thinking", "context_management", "stream"} {
		if _, ok := v[k]; ok {
			t.Errorf("the request carries %q", k)
		}
	}
}

func TestWireRequestOpenAIPlainModelAndStructuredOutput(t *testing.T) {
	s := newStub(t, okText(`{"verdict":"ok"}`))
	m := build(t, FlavorOpenAI, s.URL, Options{}, hooks{})
	req := simpleRequest("gpt-4.1")
	req.Effort = "high"
	req.OutputSchema = map[string]any{
		"type": "object", "properties": map[string]any{"verdict": map[string]any{"type": "string"}},
		"required": []any{"verdict"}, "additionalProperties": false,
	}
	if _, err := m.Turn(context.Background(), req); err != nil {
		t.Fatalf("Turn: %v", err)
	}
	v := decodeBody(t, s.requests()[0].body)
	if role := v["messages"].([]any)[0].(map[string]any)["role"]; role != "system" {
		t.Fatalf("system role = %v, want system for a non-reasoning model", role)
	}
	if _, ok := v["reasoning_effort"]; ok {
		t.Fatal("reasoning_effort sent to a model without reasoning")
	}
	rf, ok := v["response_format"].(map[string]any)
	if !ok || rf["type"] != "json_schema" {
		t.Fatalf("response_format = %v", v["response_format"])
	}
	js := rf["json_schema"].(map[string]any)
	if js["name"] != "output" || js["strict"] != true || js["schema"] == nil {
		t.Fatalf("json_schema = %v", js)
	}
}

func TestWireRequestAzure(t *testing.T) {
	s := newStub(t, okText("ok"))
	b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", s.URL+"/openai/v1", false)
	if err != nil {
		t.Fatal(err)
	}
	m, err := newModel(Options{Flavor: FlavorAzure, BaseURL: &b, Key: testKey, Retry: &retry.Policy{}}, hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Turn(context.Background(), simpleRequest("prod-gpt-4o")); err != nil {
		t.Fatalf("Turn: %v", err)
	}
	r := s.requests()[0]
	if r.path != "/openai/v1/chat/completions" {
		t.Fatalf("path = %q, want the base path kept", r.path)
	}
	if r.header.Get("api-key") != testKey.Reveal() || r.header.Get("Authorization") != "" {
		t.Fatalf("auth headers: api-key=%q Authorization=%q; Azure takes api-key only", r.header.Get("api-key"), r.header.Get("Authorization"))
	}
	if v := decodeBody(t, r.body); v["model"] != "prod-gpt-4o" {
		t.Fatalf("model = %v, want the deployment name", v["model"])
	}
}

func TestWireRequestLocalFlavors(t *testing.T) {
	for _, flavor := range []string{FlavorOllama, FlavorVLLM, FlavorGeneric} {
		t.Run(flavor, func(t *testing.T) {
			s := newStub(t, okText("ok"), okText("ok"))
			req := simpleRequest("qwen3:32b")
			req.Tools = []llm.ToolDef{strictTool}
			req.Effort = "high"
			req.OutputSchema = map[string]any{"type": "object"}
			m := build(t, flavor, s.URL, Options{}, hooks{})
			if _, err := m.Turn(context.Background(), req); err != nil {
				t.Fatalf("Turn: %v", err)
			}
			r := s.requests()[0]
			if r.header.Get("Authorization") != "" {
				t.Fatal("a keyless local server got an Authorization header")
			}
			v := decodeBody(t, r.body)
			if v["max_tokens"] != float64(512) || v["max_completion_tokens"] != nil {
				t.Fatalf("token field: %v / %v", v["max_tokens"], v["max_completion_tokens"])
			}
			for _, k := range []string{"reasoning_effort", "response_format"} {
				if _, ok := v[k]; ok {
					t.Errorf("%s sent without PIPEROOM_MODEL_CAPS", k)
				}
			}
			if f := v["tools"].([]any)[0].(map[string]any)["function"].(map[string]any); f["strict"] != nil {
				t.Errorf("strict sent to a %s host", flavor)
			}

			// PIPEROOM_MODEL_CAPS turns reasoning and structured output on,
			// never strict tools.
			m = build(t, flavor, s.URL, Options{Caps: []string{"reasoning", "structured_output", "parallel_tools"}}, hooks{})
			if _, err := m.Turn(context.Background(), req); err != nil {
				t.Fatalf("Turn: %v", err)
			}
			v = decodeBody(t, s.requests()[1].body)
			if v["reasoning_effort"] != "high" {
				t.Errorf("reasoning_effort = %v with caps reasoning", v["reasoning_effort"])
			}
			rf, _ := v["response_format"].(map[string]any)
			if rf == nil || rf["json_schema"].(map[string]any)["strict"] != nil {
				t.Errorf("response_format = %v; want json_schema, not strict", v["response_format"])
			}
			if f := v["tools"].([]any)[0].(map[string]any)["function"].(map[string]any); f["strict"] != nil {
				t.Error("a cap turned strict tools on")
			}
		})
	}
}

func TestHistoryGoesBackByteForByte(t *testing.T) {
	const args = `{"connector": "pg-orders" ,"why":"check"}` // not canonical JSON: it must come back as is
	s := newStub(t,
		jsonReply(200, completion(map[string]any{
			"content": "Reading <both> & more.",
			"tool_calls": []any{
				map[string]any{"id": "call_a", "type": "function", "function": map[string]any{"name": "connect_status", "arguments": args}},
				map[string]any{"id": "call_b", "type": "function", "function": map[string]any{"name": "connect_status", "arguments": `{"connector":"x"`}},
			},
		}, "tool_calls", map[string]any{"prompt_tokens": 300, "completion_tokens": 30})),
		okText("done"),
	)
	m := build(t, FlavorOpenAI, s.URL, Options{}, hooks{})
	req := simpleRequest("gpt-4.1")
	req.Tools = []llm.ToolDef{strictTool}
	resp, err := m.Turn(context.Background(), req)
	if err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if resp.Stop != llm.StopToolUse {
		t.Fatalf("Stop = %q", resp.Stop)
	}
	calls := []*llm.ToolCall{}
	for _, p := range resp.Message.Parts {
		if p.Kind == llm.PartToolCall {
			calls = append(calls, p.ToolCall)
		}
	}
	if len(calls) != 2 || string(calls[0].Input) != args {
		t.Fatalf("calls = %+v; the arguments must be kept as sent", calls)
	}
	if string(calls[1].Input) != `{"connector":"x"` {
		t.Fatalf("invalid JSON arguments = %q; kept for the agent to reject", calls[1].Input)
	}
	results := llm.Message{Role: llm.RoleUser, Parts: []llm.Part{
		{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{CallID: "call_a", Name: "connect_status", Content: "<tool_output untrusted=\"true\">a & b</tool_output>"}},
		{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{CallID: "call_b", Name: "connect_status", Content: "error: arguments are not valid JSON", IsError: true}},
		{Kind: llm.PartText, Text: "Driver: <look at> the & sink."},
		{Kind: llm.PartText, Text: "second\ttext"},
	}}
	req.Messages = append(req.Messages, resp.Message, results)
	if _, err := m.Turn(context.Background(), req); err != nil {
		t.Fatalf("second Turn: %v", err)
	}
	body := s.requests()[1].body
	if strings.Contains(string(body), "\\u003c") || strings.Contains(string(body), "\\u0026") {
		t.Errorf("the body HTML-escapes text: %s", body)
	}
	var w struct {
		Messages []struct {
			Role       string          `json:"role"`
			Content    json.RawMessage `json:"content"`
			ToolCallID string          `json:"tool_call_id"`
			ToolCalls  []struct {
				ID       string `json:"id"`
				Function struct {
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &w); err != nil {
		t.Fatal(err)
	}
	roles := []string{}
	for _, msg := range w.Messages {
		roles = append(roles, msg.Role)
	}
	if fmt.Sprint(roles) != "[system user assistant tool tool user]" {
		t.Fatalf("roles = %v", roles)
	}
	a := w.Messages[2]
	var atext string
	_ = json.Unmarshal(a.Content, &atext)
	if atext != "Reading <both> & more." || len(a.ToolCalls) != 2 || a.ToolCalls[0].Function.Arguments != args || a.ToolCalls[0].ID != "call_a" {
		t.Fatalf("assistant message = %+v", a)
	}
	if w.Messages[3].ToolCallID != "call_a" || w.Messages[4].ToolCallID != "call_b" {
		t.Fatal("tool messages lost their call ids")
	}
	var parts []struct{ Type, Text string }
	if err := json.Unmarshal(w.Messages[5].Content, &parts); err != nil || len(parts) != 2 ||
		parts[0].Text != "Driver: <look at> the & sink." || parts[1].Text != "second\ttext" {
		t.Fatalf("user text parts = %s (%v)", w.Messages[5].Content, err)
	}
}

func TestToolCallQuirksOfLocalServers(t *testing.T) {
	s := newStub(t, jsonReply(200, completion(map[string]any{
		"content": "",
		"tool_calls": []any{
			// Ollama-style: no id, arguments as an object.
			map[string]any{"type": "function", "function": map[string]any{"name": "connect_status", "arguments": map[string]any{"connector": "pg"}}},
			map[string]any{"id": "", "type": "function", "function": map[string]any{"name": "connect_status", "arguments": ""}},
		},
	}, "tool_calls", map[string]any{"prompt_tokens": 100, "completion_tokens": 5})))
	m := build(t, FlavorOllama, s.URL, Options{}, hooks{})
	resp, err := m.Turn(context.Background(), simpleRequest("qwen3"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []*llm.ToolCall
	for _, p := range resp.Message.Parts {
		if p.Kind == llm.PartToolCall {
			calls = append(calls, p.ToolCall)
		}
		if p.Kind == llm.PartText {
			t.Error("an empty content became a text part")
		}
	}
	if len(calls) != 2 || calls[0].ID == "" || calls[1].ID == "" || calls[0].ID == calls[1].ID {
		t.Fatalf("ids = %+v; each call needs its own id", calls)
	}
	if string(calls[0].Input) != `{"connector":"pg"}` || string(calls[1].Input) != `{}` {
		t.Fatalf("inputs = %s / %s", calls[0].Input, calls[1].Input)
	}
}

func TestStopReasons(t *testing.T) {
	for _, tc := range []struct {
		finish string
		msg    map[string]any
		want   llm.StopReason
		detail string
	}{
		{"stop", map[string]any{"content": "x"}, llm.StopEndTurn, ""},
		{"length", map[string]any{"content": "x"}, llm.StopMaxTokens, ""},
		{"content_filter", map[string]any{"content": nil}, llm.StopRefusal, "content_filter"},
		{"stop", map[string]any{"content": nil, "refusal": "I can't help with that."}, llm.StopRefusal, "I can't help with that."},
		{"tool_calls", map[string]any{"content": nil}, llm.StopOther, ""},
		{"eos", map[string]any{"content": "x"}, llm.StopOther, ""},
	} {
		s := newStub(t, jsonReply(200, completion(tc.msg, tc.finish, map[string]any{"prompt_tokens": 100, "completion_tokens": 1})))
		resp, err := build(t, FlavorOpenAI, s.URL, Options{}, hooks{}).Turn(context.Background(), simpleRequest("gpt-4.1"))
		if err != nil {
			t.Fatalf("%s: %v", tc.finish, err)
		}
		if resp.Stop != tc.want || resp.StopDetail != tc.detail || resp.ProviderStop != tc.finish {
			t.Errorf("%s: Stop=%q detail=%q provider=%q, want %q %q", tc.finish, resp.Stop, resp.StopDetail, resp.ProviderStop, tc.want, tc.detail)
		}
	}
}

func TestAzureContentFilterFinishNamesCategories(t *testing.T) {
	body := completion(map[string]any{"content": nil}, "content_filter", map[string]any{"prompt_tokens": 100, "completion_tokens": 3})
	body["choices"].([]any)[0].(map[string]any)["content_filter_results"] = map[string]any{
		"hate": map[string]any{"filtered": false, "severity": "safe"}, "violence": map[string]any{"filtered": true, "severity": "high"},
		"self_harm": map[string]any{"filtered": true, "severity": "medium"},
	}
	s := newStub(t, jsonReply(200, body))
	resp, err := build(t, FlavorAzure, s.URL, Options{}, hooks{}).Turn(context.Background(), simpleRequest("prod"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Stop != llm.StopRefusal || resp.StopDetail != "content_filter: self_harm, violence" {
		t.Fatalf("Stop=%q detail=%q", resp.Stop, resp.StopDetail)
	}
}

func TestAzurePromptFilter400IsARefusal(t *testing.T) {
	s := newStub(t, jsonReply(400, map[string]any{"error": map[string]any{
		"message": "The response was filtered due to the prompt triggering Azure OpenAI's content management policy.",
		"type":    nil, "param": "prompt", "code": "content_filter", "status": 400,
		"innererror": map[string]any{"code": "ResponsibleAIPolicyViolation", "content_filter_result": map[string]any{
			"jailbreak": map[string]any{"filtered": true, "detected": true}, "hate": map[string]any{"filtered": false},
		}},
	}}))
	resp, err := build(t, FlavorAzure, s.URL, Options{}, hooks{}).Turn(context.Background(), simpleRequest("prod"))
	if err != nil {
		t.Fatalf("Turn: %v; a filtered prompt is a refusal, not an error", err)
	}
	if resp.Stop != llm.StopRefusal || resp.StopDetail != "content_filter: jailbreak" || resp.ProviderStop != "content_filter" {
		t.Fatalf("resp = %+v", resp)
	}
	if !resp.Usage.Estimated || resp.Usage.Input == 0 || resp.Usage.Output != 0 {
		t.Fatalf("usage = %+v; want the request estimated", resp.Usage)
	}
	if len(s.requests()) != 1 {
		t.Fatal("a filtered prompt was retried")
	}
}

func retrying(sl *sleeps) *retry.Policy {
	p := retry.Default()
	p.Sleep = sl.sleep
	p.Rand = func() float64 { return 0.5 }
	return &p
}

func TestQuota429IsBillingAndNotRetried(t *testing.T) {
	sl := &sleeps{}
	s := newStub(t, errReply(429, "insufficient_quota", "You exceeded your current quota, please check your plan and billing details."))
	_, err := build(t, FlavorOpenAI, s.URL, Options{Retry: retrying(sl)}, hooks{}).Turn(context.Background(), simpleRequest("gpt-4.1"))
	ae := apiErr(t, err)
	if ae.Kind != llm.ErrBilling || ae.Status != 429 || !llm.Unretryable(err) {
		t.Fatalf("APIError = %+v, want billing 429 unretryable", ae)
	}
	if n := len(s.requests()); n != 1 || len(sl.waits) != 0 {
		t.Fatalf("requests = %d waits = %v; a quota 429 is never retried", n, sl.waits)
	}
	if !strings.Contains(err.Error(), "insufficient_quota") || strings.Contains(err.Error(), "exceeded your current quota") {
		t.Fatalf("error text = %q; the code, never the message", err.Error())
	}
}

func Test503RetriedTwiceThenOverloaded(t *testing.T) {
	sl := &sleeps{}
	s := newStub(t, errReply(503, "", "upstream unavailable"))
	_, err := build(t, FlavorOpenAI, s.URL, Options{Retry: retrying(sl)}, hooks{}).Turn(context.Background(), simpleRequest("gpt-4.1"))
	if ae := apiErr(t, err); ae.Kind != llm.ErrOverloaded || ae.Status != 503 {
		t.Fatalf("APIError = %+v", ae)
	}
	if n := len(s.requests()); n != 3 {
		t.Fatalf("requests = %d, want 3 (the first and two retries)", n)
	}
	if fmt.Sprint(sl.waits) != "[1s 4s]" {
		t.Fatalf("waits = %v, want [1s 4s]", sl.waits)
	}
}

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	sl := &sleeps{}
	s := newStub(t, errReply(502, "", "bad gateway"), errReply(529, "", "overloaded"), okText("fine"))
	resp, err := build(t, FlavorOpenAI, s.URL, Options{Retry: retrying(sl)}, hooks{}).Turn(context.Background(), simpleRequest("gpt-4.1"))
	if err != nil || resp.Message.Parts[0].Text != "fine" {
		t.Fatalf("resp = %+v err = %v", resp, err)
	}
}

func TestRetryAfterHonouredAndCapped(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   time.Duration
	}{
		{"2", 2 * time.Second},
		{"3600", retry.MaxRetryAfter},
	} {
		sl := &sleeps{}
		s := newStub(t, errReply(429, "rate_limit_exceeded", "slow down", "Retry-After", tc.header), okText("ok"))
		if _, err := build(t, FlavorOpenAI, s.URL, Options{Retry: retrying(sl)}, hooks{}).Turn(context.Background(), simpleRequest("gpt-4.1")); err != nil {
			t.Fatal(err)
		}
		if len(sl.waits) != 1 || sl.waits[0] != tc.want {
			t.Fatalf("Retry-After %s: waits = %v, want [%v]", tc.header, sl.waits, tc.want)
		}
	}
}

func TestRateLimitAfterRetriesIsRateLimited(t *testing.T) {
	sl := &sleeps{}
	s := newStub(t, errReply(429, "rate_limit_exceeded", "slow down"))
	_, err := build(t, FlavorOpenAI, s.URL, Options{Retry: retrying(sl)}, hooks{}).Turn(context.Background(), simpleRequest("gpt-4.1"))
	if ae := apiErr(t, err); ae.Kind != llm.ErrRateLimited || llm.Unretryable(err) {
		t.Fatalf("APIError = %+v", ae)
	}
	if len(s.requests()) != 3 {
		t.Fatalf("requests = %d", len(s.requests()))
	}
}

func TestBadRequestIsNotRetried(t *testing.T) {
	sl := &sleeps{}
	s := newStub(t, errReply(400, "unsupported_parameter", "Unsupported parameter: 'reasoning_effort'"))
	_, err := build(t, FlavorOpenAI, s.URL, Options{Retry: retrying(sl)}, hooks{}).Turn(context.Background(), simpleRequest("gpt-4.1"))
	if ae := apiErr(t, err); ae.Kind != llm.ErrBadRequest || len(s.requests()) != 1 {
		t.Fatalf("APIError = %+v requests = %d", ae, len(s.requests()))
	}
}

func TestContextLength400IsTooLong(t *testing.T) {
	for _, reply := range []func(http.ResponseWriter){
		errReply(400, "context_length_exceeded", "This model's maximum context length is 128000 tokens. However, your messages resulted in 130000 tokens."),
		// vLLM's shape: top-level, no code string.
		jsonReply(400, map[string]any{"object": "error", "type": "BadRequestError", "code": 400,
			"message": "This model's maximum context length is 32768 tokens. However, you requested 40000 tokens."}),
	} {
		s := newStub(t, reply)
		_, err := build(t, FlavorVLLM, s.URL, Options{}, hooks{}).Turn(context.Background(), simpleRequest("m"))
		if ae := apiErr(t, err); ae.Kind != llm.ErrTooLong || llm.Unretryable(err) {
			t.Fatalf("APIError = %+v, want too_long (retryable after compaction)", ae)
		}
	}
}

func TestMissingUsageIsEstimated(t *testing.T) {
	for name, usage := range map[string]any{
		"absent": nil,
		"zeros":  map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
	} {
		t.Run(name, func(t *testing.T) {
			s := newStub(t, jsonReply(200, completion(map[string]any{"content": "answer"}, "stop", usage)))
			resp, err := build(t, FlavorOllama, s.URL, Options{}, hooks{}).Turn(context.Background(), simpleRequest("qwen3"))
			if err != nil {
				t.Fatalf("Turn: %v; an estimate skips the truncation check", err)
			}
			reqBytes := len(s.requests()[0].body)
			if u := resp.Usage; !u.Estimated || u.Input != int64((reqBytes+2)/3) || u.Output == 0 {
				t.Fatalf("usage = %+v, want Estimated with input ceil(%d/3)", u, reqBytes)
			}
		})
	}
}

func TestUsageNormalised(t *testing.T) {
	s := newStub(t, jsonReply(200, completion(map[string]any{"content": "x"}, "stop", map[string]any{
		"prompt_tokens": 1000, "completion_tokens": 300,
		"prompt_tokens_details":     map[string]any{"cached_tokens": 600},
		"completion_tokens_details": map[string]any{"reasoning_tokens": 250},
	})))
	resp, err := build(t, FlavorOpenAI, s.URL, Options{}, hooks{}).Turn(context.Background(), simpleRequest("gpt-5"))
	if err != nil {
		t.Fatal(err)
	}
	want := llm.Usage{Input: 400, CacheRead: 600, Output: 300, Reasoning: 250}
	if u := resp.Usage; u.Input != want.Input || u.CacheRead != want.CacheRead || u.Output != want.Output || u.Reasoning != want.Reasoning || u.CacheWrite != 0 || u.Estimated {
		t.Fatalf("usage = %+v, want %+v", u, want)
	}
	if resp.Model != "served-model" {
		t.Fatalf("Model = %q, want the served model", resp.Model)
	}
}

func TestTruncatedPromptIsAnError(t *testing.T) {
	s := newStub(t, jsonReply(200, completion(map[string]any{"content": "partial"}, "stop",
		map[string]any{"prompt_tokens": 40, "completion_tokens": 5})))
	req := simpleRequest("qwen3")
	req.System = strings.Repeat("Untrusted-data rules come first. ", 400) // about 13 KB: at least 1600 tokens
	resp, err := build(t, FlavorOllama, s.URL, Options{}, hooks{}).Turn(context.Background(), req)
	ae := apiErr(t, err)
	if ae.Kind != llm.ErrTruncated || !llm.Unretryable(err) {
		t.Fatalf("APIError = %+v, want truncated", ae)
	}
	if !strings.Contains(err.Error(), "raise its context length") {
		t.Fatalf("error text = %q", err.Error())
	}
	if resp.Usage.Input != 40 || resp.Usage.Output != 5 {
		t.Fatalf("usage = %+v; the billed usage comes back with the error", resp.Usage)
	}
}

func TestResponseIsCapped(t *testing.T) {
	s := newStub(t, func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"`))
		_, _ = w.Write([]byte(strings.Repeat("a", maxResponseBytes)))
		_, _ = w.Write([]byte(`"}}]}`))
	})
	_, err := build(t, FlavorOpenAI, s.URL, Options{}, hooks{}).Turn(context.Background(), simpleRequest("gpt-4.1"))
	if ae := apiErr(t, err); ae.Kind != llm.ErrNetwork || ae.Status != 200 {
		t.Fatalf("APIError = %+v", ae)
	}
}

func TestNoChoiceIsAnError(t *testing.T) {
	s := newStub(t, jsonReply(200, map[string]any{"choices": []any{}}))
	_, err := build(t, FlavorOpenAI, s.URL, Options{}, hooks{}).Turn(context.Background(), simpleRequest("gpt-4.1"))
	if ae := apiErr(t, err); ae.Kind != llm.ErrOverloaded {
		t.Fatalf("APIError = %+v", ae)
	}
}

func TestEncodeFailureIsNotSent(t *testing.T) {
	s := newStub(t)
	req := simpleRequest("gpt-4.1")
	req.Tools = []llm.ToolDef{{Name: "bad", Schema: map[string]any{"x": func() {}}}}
	_, err := build(t, FlavorOpenAI, s.URL, Options{}, hooks{}).Turn(context.Background(), req)
	if !errors.Is(err, llm.ErrNotSent) || apiErr(t, err).Kind != llm.ErrBadRequest {
		t.Fatalf("err = %v", err)
	}
	if len(s.requests()) != 0 {
		t.Fatal("an unencodable request was sent")
	}
}

func TestRedirectIsNotFollowed(t *testing.T) {
	other := newStub(t, okText("stolen"))
	s := newStub(t, func(w http.ResponseWriter) {
		w.Header().Set("Location", other.URL+"/v1/chat/completions")
		w.WriteHeader(http.StatusTemporaryRedirect)
	})
	_, err := build(t, FlavorOpenAI, s.URL, Options{}, hooks{}).Turn(context.Background(), simpleRequest("gpt-4.1"))
	if ae := apiErr(t, err); ae.Status != http.StatusTemporaryRedirect || ae.Kind != llm.ErrNetwork {
		t.Fatalf("APIError = %+v", ae)
	}
	if n := len(other.requests()); n != 0 {
		t.Fatalf("the redirect target got %d requests", n)
	}
}

func TestKeyIsBoundToItsHost(t *testing.T) {
	other := newStub(t, okText("x"))
	s := newStub(t)
	m := build(t, FlavorOpenAI, s.URL, Options{}, hooks{})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, other.URL+"/v1/chat/completions", strings.NewReader("{}"))
	if _, err := m.client.Do(req); !errors.Is(err, errOtherHost) {
		t.Fatalf("err = %v, want errOtherHost", err)
	}
	if len(other.requests()) != 0 {
		t.Fatal("the model client sent a request to another host")
	}
}

func TestCapabilities(t *testing.T) {
	s := newStub(t)
	oa := build(t, FlavorOpenAI, s.URL, Options{}, hooks{})
	for _, tc := range []struct {
		model     string
		reasoning bool
		ctx       int64
	}{
		{"gpt-5.2", true, 400_000}, {"gpt-5-chat-latest", false, 400_000}, {"o4-mini", true, 200_000},
		{"gpt-4.1-mini", false, 1_047_576}, {"gpt-4o", false, 128_000}, {"o3", true, 200_000}, {"unknown-x", false, 0},
	} {
		c := oa.Capabilities(tc.model)
		if c.Reasoning != tc.reasoning || c.Effort != tc.reasoning || c.ContextTokens != tc.ctx {
			t.Errorf("%s: %+v", tc.model, c)
		}
		if !c.ToolCalls || !c.ParallelToolCalls || !c.StrictTools || !c.StructuredOutput || !c.PromptCache || c.Compaction ||
			c.MaxTools != 128 || c.ToolNameMax != 64 || c.MaxStrictTools != 0 {
			t.Errorf("%s: %+v", tc.model, c)
		}
	}
	local := build(t, FlavorOllama, s.URL, Options{ContextTokens: 131072}, hooks{})
	c := local.Capabilities("qwen3")
	if c.ParallelToolCalls || c.StrictTools || c.StructuredOutput || c.PromptCache || c.Reasoning || c.ContextTokens != 131072 || !c.ToolCalls {
		t.Errorf("ollama: %+v", c)
	}
	local = build(t, FlavorGeneric, s.URL, Options{Key: testKey, Caps: []string{"parallel_tools", "structured_output", "reasoning"}}, hooks{})
	c = local.Capabilities("m")
	if !c.ParallelToolCalls || c.StrictTools || !c.StructuredOutput || !c.Reasoning || !c.Effort || c.ContextTokens != 0 {
		t.Errorf("generic with caps: %+v", c)
	}
	// Caps never change a hosted flavor.
	oa = build(t, FlavorOpenAI, s.URL, Options{Caps: []string{"reasoning"}}, hooks{})
	if oa.Capabilities("gpt-4o").Reasoning {
		t.Error("PIPEROOM_MODEL_CAPS changed a hosted flavor")
	}
	if oa.Provider() != llm.ProviderOpenAI || oa.Flavor() != FlavorOpenAI {
		t.Error("Provider or Flavor")
	}
	if len(s.requests()) != 0 {
		t.Fatal("Capabilities made a request")
	}
}

func TestNewRefuses(t *testing.T) {
	private, _ := config.ParseBaseURL("x", "http://10.0.0.5:8000/v1", true)
	for name, o := range map[string]Options{
		"unknown flavor":            {Flavor: "acme", Key: testKey},
		"hosted flavor without key": {Flavor: FlavorDeepSeek},
		"openai without key":        {Flavor: FlavorOpenAI},
		"azure without base URL":    {Flavor: FlavorAzure, Key: testKey},
		"generic without base URL":  {Flavor: FlavorGeneric, Key: testKey},
		"private http without knob": {Flavor: FlavorVLLM, BaseURL: &private},
		"hand-made metadata URL":    {Flavor: FlavorVLLM, BaseURL: hand("http://169.254.169.254/v1")},
		"hand-made metadata name":   {Flavor: FlavorOpenAI, Key: testKey, BaseURL: hand("https://metadata.google.internal/v1")},
		"hand-made user info":       {Flavor: FlavorOpenAI, Key: testKey, BaseURL: hand("https://u:p@api.example.com/v1")},
	} {
		if _, err := New(o); err == nil {
			t.Errorf("%s: New accepted it", name)
		} else if strings.Contains(err.Error(), testKey.Reveal()) {
			t.Errorf("%s: the error names the key", name)
		}
	}
	if _, err := New(Options{Flavor: FlavorVLLM, BaseURL: &private, AllowPrivateHTTP: true}); err != nil {
		t.Errorf("private http with the knob: %v", err)
	}
	if m, err := New(Options{Flavor: FlavorOllama}); err != nil || m.origin != "http://localhost:11434" {
		t.Errorf("ollama default: %v %v", m, err)
	}
	if m, err := New(Options{Key: testKey}); err != nil || m.endpoint != "https://api.openai.com/v1/chat/completions" {
		t.Errorf("openai default: %v", err)
	}
}

func hand(raw string) *config.BaseURL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return &config.BaseURL{URL: u, Host: u.Hostname()}
}

func TestKindTable(t *testing.T) {
	for _, tc := range []struct {
		status         int
		code, typ, msg string
		want           llm.ErrorKind
	}{
		{401, "invalid_api_key", "", "Incorrect API key", llm.ErrAuth},
		{403, "", "", "Country not supported", llm.ErrAuth},
		{403, "model_not_found", "", "no access to model", llm.ErrModelNotFound},
		{404, "", "", `model "x" not found, try pulling it first`, llm.ErrModelNotFound},
		{404, "DeploymentNotFound", "", "The API deployment for this resource does not exist.", llm.ErrModelNotFound},
		{400, "model_not_found", "", "The model `gpt-9` does not exist", llm.ErrModelNotFound},
		{429, "insufficient_quota", "insufficient_quota", "quota", llm.ErrBilling},
		{429, "", "", "You exceeded your current quota", llm.ErrBilling},
		{429, "billing_hard_limit_reached", "", "", llm.ErrBilling},
		{402, "", "", "payment required", llm.ErrBilling},
		{429, "rate_limit_exceeded", "", "Rate limit reached", llm.ErrRateLimited},
		{413, "", "", "", llm.ErrTooLong},
		{400, "context_length_exceeded", "", "", llm.ErrTooLong},
		{400, "", "", "prompt is too long for the context window", llm.ErrTooLong},
		{400, "unsupported_parameter", "", "Unsupported parameter: 'max_tokens'", llm.ErrBadRequest},
		{422, "", "", "bad", llm.ErrBadRequest},
		{408, "", "", "", llm.ErrOverloaded},
		{409, "", "", "", llm.ErrOverloaded},
		{500, "", "", "", llm.ErrOverloaded},
		{529, "", "", "", llm.ErrOverloaded},
		{302, "", "", "", llm.ErrNetwork},
	} {
		if got := Kind(tc.status, tc.code, tc.typ, tc.msg); got != tc.want {
			t.Errorf("Kind(%d, %q, %q) = %s, want %s", tc.status, tc.code, tc.msg, got, tc.want)
		}
	}
}

func TestStrictSchema(t *testing.T) {
	obj := func(props map[string]any, req []any) map[string]any {
		return map[string]any{"type": "object", "properties": props, "required": req, "additionalProperties": false}
	}
	for _, tc := range []struct {
		name string
		s    map[string]any
		want bool
	}{
		{"all required", obj(map[string]any{"a": map[string]any{"type": "string"}}, []any{"a"}), true},
		{"empty object", obj(map[string]any{}, []any{}), true},
		{"optional property", obj(map[string]any{"a": map[string]any{"type": "string"}, "b": map[string]any{"type": "integer"}}, []any{"a"}), false},
		{"additional allowed", map[string]any{"type": "object", "properties": map[string]any{}, "required": []any{}}, false},
		{"nested loose", obj(map[string]any{"a": map[string]any{"type": "object", "properties": map[string]any{"x": map[string]any{"type": "string"}}}}, []any{"a"}), false},
		{"array of strict", obj(map[string]any{"a": map[string]any{"type": "array", "items": obj(map[string]any{"x": map[string]any{"type": "string"}}, []any{"x"})}}, []any{"a"}), true},
		{"unsupported keyword", obj(map[string]any{"a": map[string]any{"type": "string", "minLength": 1}}, []any{"a"}), false},
		{"oneOf", obj(map[string]any{"a": map[string]any{"oneOf": []any{}}}, []any{"a"}), false},
		{"nullable union", obj(map[string]any{"a": map[string]any{"type": []any{"string", "null"}}}, []any{"a"}), true},
		{"anyOf strict", obj(map[string]any{"a": map[string]any{"anyOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "null"}}}}, []any{"a"}), true},
	} {
		if got := strictSchema(tc.s); got != tc.want {
			t.Errorf("%s: strictSchema = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestReasoningEffort(t *testing.T) {
	for in, want := range map[string]string{"low": "low", "medium": "medium", "high": "high", "xhigh": "high", "max": "high", "": "", "turbo": ""} {
		if got := reasoningEffort(in); got != want {
			t.Errorf("reasoningEffort(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLoadKey(t *testing.T) {
	env := map[string]string{"OPENAI_API_KEY": " sk-std ", "AZURE_OPENAI_API_KEY": "az-key"}
	getenv := func(k string) string { return env[k] }
	noFile := func(string) ([]byte, error) { return nil, errors.New("no file") }
	k, err := LoadKey(FlavorOpenAI, true, getenv, noFile)
	if err != nil || k.Reveal() != "sk-std" {
		t.Fatalf("standard variable: %q %v", k.Reveal(), err)
	}
	env["PIPEROOM_MODEL_API_KEY"] = "sk-main"
	if k, _ := LoadKey(FlavorOpenAI, true, getenv, noFile); k.Reveal() != "sk-main" {
		t.Fatalf("PIPEROOM_MODEL_API_KEY first: %q", k.Reveal())
	}
	if k, _ := LoadKey(FlavorAzure, false, getenv, noFile); k.Reveal() != "az-key" {
		t.Fatalf("another role reads only its standard variable: %q", k.Reveal())
	}
	if k, _ := LoadKey(FlavorGeneric, false, getenv, noFile); k != "" {
		t.Fatal("another role read the main provider's PIPEROOM_MODEL_API_KEY")
	}
	delete(env, "PIPEROOM_MODEL_API_KEY")
	env["PIPEROOM_MODEL_API_KEY_FILE"] = "/run/secrets/key"
	k, err = LoadKey(FlavorVLLM, true, getenv, func(p string) ([]byte, error) {
		if p != "/run/secrets/key" {
			t.Fatalf("path %q", p)
		}
		return []byte("file-key\n"), nil
	})
	if err != nil || k.Reveal() != "file-key" {
		t.Fatalf("key file: %q %v", k.Reveal(), err)
	}
	if _, err := LoadKey(FlavorVLLM, true, getenv, noFile); err == nil || !strings.Contains(err.Error(), "PIPEROOM_MODEL_API_KEY_FILE") {
		t.Fatalf("unreadable file: %v", err)
	}
	if _, err := LoadKey(FlavorVLLM, true, getenv, func(string) ([]byte, error) { return []byte("  \n"), nil }); err == nil {
		t.Fatal("an empty key file was accepted")
	}
	if k, _ := LoadKey(FlavorOllama, false, getenv, noFile); k != "" {
		t.Fatal("ollama has no key")
	}
}

func TestLimiterWaitPastDeadlineIsRateLimitedNotSent(t *testing.T) {
	s := newStub(t, okText("x"))
	lim := retry.NewLimiter("e", retry.Limits{TPM: 60}, nil) // 1 token a second
	m := build(t, FlavorOpenAI, s.URL, Options{Limiter: lim}, hooks{})
	release, _ := lim.Acquire(context.Background(), 60)
	release(60)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := m.Turn(ctx, simpleRequest("gpt-4.1"))
	if ae := apiErr(t, err); ae.Kind != llm.ErrRateLimited || !errors.Is(err, llm.ErrNotSent) {
		t.Fatalf("APIError = %+v err = %v", ae, err)
	}
	if len(s.requests()) != 0 {
		t.Fatal("a request went out past the limiter")
	}
}

func TestLimiterObservesHeaders(t *testing.T) {
	s := newStub(t, jsonReply(200, completion(map[string]any{"content": "x"}, "stop", map[string]any{"prompt_tokens": 50, "completion_tokens": 1}),
		"x-ratelimit-remaining-tokens", "0", "x-ratelimit-reset-tokens", "30s"))
	lim := retry.NewLimiter("e", retry.Limits{}, nil)
	m := build(t, FlavorOpenAI, s.URL, Options{Limiter: lim}, hooks{})
	if _, err := m.Turn(context.Background(), simpleRequest("gpt-4.1")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := lim.Acquire(ctx, 10); !errors.Is(err, retry.ErrWaitPastDeadline) {
		t.Fatalf("err = %v; the limiter did not read the server's window", err)
	}
}
