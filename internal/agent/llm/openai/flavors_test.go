package openai

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/agent/llm"
	"piperoom/internal/config"
)

// hostedV3b are the flavors V3b adds, with a model of each.
var hostedV3b = []struct{ flavor, model string }{
	{FlavorDeepSeek, "deepseek-chat"},
	{FlavorQwen, "qwen-plus"},
	{FlavorMistral, "mistral-large-latest"},
	{FlavorGroq, "llama-3.3-70b-versatile"},
	{FlavorTogether, "meta-llama/Llama-3.3-70B-Instruct-Turbo"},
	{FlavorOpenRouter, "deepseek/deepseek-chat-v3.1"},
	{FlavorXAI, "grok-4"},
}

// TestFlavorTableMatchesDesign checks every flavor's default base URL (in
// config, V0) and key variable against the flavor table of design section
// 3, and that the adapter serves exactly the flavors the table names.
func TestFlavorTableMatchesDesign(t *testing.T) {
	doc, err := os.ReadFile("../../../../docs/design/model-providers.md")
	if err != nil {
		t.Fatalf("design doc: %v", err)
	}
	row := regexp.MustCompile("^\\| `([a-z]+)` \\| ([^|]*) \\| ([^|]*) \\|")
	tick := regexp.MustCompile("`([^`]*)`")
	first := func(cell string) string {
		cell = strings.TrimSpace(cell)
		if strings.HasPrefix(cell, "none") {
			return ""
		}
		if m := tick.FindStringSubmatch(cell); m != nil {
			return m[1]
		}
		return ""
	}
	var seen []string
	for _, line := range strings.Split(string(doc), "\n") {
		m := row.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		name, base, key := m[1], first(m[2]), first(m[3])
		seen = append(seen, name)
		if _, ok := flavors[name]; !ok {
			t.Errorf("design flavor %q is not served", name)
			continue
		}
		if got := config.FlavorDefaultBaseURL(name); got != base {
			t.Errorf("%s: default base URL %q, design says %q", name, got, base)
		}
		if got := KeyEnv(name); got != key {
			t.Errorf("%s: key variable %q, design says %q", name, got, key)
		}
	}
	slices.Sort(seen)
	if !slices.Equal(seen, Flavors()) {
		t.Fatalf("design flavors %v, adapter flavors %v", seen, Flavors())
	}
	if len(flavors) != len(Flavors()) {
		t.Fatalf("Flavors() lists %d, the table holds %d", len(Flavors()), len(flavors))
	}
}

func TestHostedFlavorsDefaultsAndKeys(t *testing.T) {
	for _, f := range hostedV3b {
		t.Run(f.flavor, func(t *testing.T) {
			_, err := New(Options{Flavor: f.flavor})
			if err == nil || !strings.Contains(err.Error(), KeyEnv(f.flavor)) {
				t.Fatalf("New without a key: %v, want a refusal naming %s", err, KeyEnv(f.flavor))
			}
			m, err := New(Options{Flavor: f.flavor, Key: testKey})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if want := config.FlavorDefaultBaseURL(f.flavor) + "/chat/completions"; m.endpoint != want {
				t.Fatalf("endpoint = %q, want %q", m.endpoint, want)
			}
			if m.Flavor() != f.flavor || m.Provider() != llm.ProviderOpenAI {
				t.Fatalf("Flavor %q Provider %q", m.Flavor(), m.Provider())
			}
		})
	}
}

// TestHostedFlavorWireRequests checks each V3b flavor's request: bearer
// auth, the system role, the token field, no strict tools, and the fields
// only one host takes.
func TestHostedFlavorWireRequests(t *testing.T) {
	for _, f := range hostedV3b {
		t.Run(f.flavor, func(t *testing.T) {
			s := newStub(t, okText("ok"))
			m := build(t, f.flavor, s.URL, Options{Key: testKey}, hooks{})
			req := simpleRequest(f.model)
			req.Tools = []llm.ToolDef{strictTool}
			req.Effort = "max"
			req.Messages = append(req.Messages,
				llm.Message{Role: llm.RoleAssistant, Parts: []llm.Part{{Kind: llm.PartToolCall, ToolCall: &llm.ToolCall{ID: "abcDEF123", Name: "connect_status", Input: json.RawMessage(`{"connector":"pg"}`)}}}},
				llm.Message{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{CallID: "abcDEF123", Name: "connect_status", Content: "RUNNING"}}}},
			)
			if _, err := m.Turn(context.Background(), req); err != nil {
				t.Fatal(err)
			}
			r := s.requests()[0]
			if got := r.header.Get("Authorization"); got != "Bearer "+testKey.Reveal() {
				t.Fatalf("Authorization = %q", got)
			}
			if r.header.Get("api-key") != "" {
				t.Fatal("api-key header on a bearer host")
			}
			if r.path != "/v1/chat/completions" {
				t.Fatalf("path = %q", r.path)
			}
			b := decodeBody(t, r.body)
			msgs := b["messages"].([]any)
			if role := msgs[0].(map[string]any)["role"]; role != "system" {
				t.Fatalf("system role = %v", role)
			}
			tool := msgs[len(msgs)-1].(map[string]any)
			if _, named := tool["name"]; named != (f.flavor == FlavorMistral) {
				t.Fatalf("tool message name present = %v; only mistral names it", named)
			}
			if f.flavor == FlavorGroq {
				if b["max_completion_tokens"] != float64(512) || b["max_tokens"] != nil {
					t.Fatalf("groq token field: %v / %v", b["max_completion_tokens"], b["max_tokens"])
				}
			} else if b["max_tokens"] != float64(512) || b["max_completion_tokens"] != nil {
				t.Fatalf("token field: %v / %v", b["max_tokens"], b["max_completion_tokens"])
			}
			fn := b["tools"].([]any)[0].(map[string]any)["function"].(map[string]any)
			if _, strict := fn["strict"]; strict {
				t.Fatal("strict tools on a host that does not claim them")
			}
			if _, ok := b["reasoning_effort"]; ok {
				t.Fatalf("reasoning_effort on %s %s", f.flavor, f.model)
			}
			_, think := b["enable_thinking"]
			_, par := b["parallel_tool_calls"]
			_, prov := b["provider"]
			_, rf := b["reasoning_format"]
			if think != (f.flavor == FlavorQwen) || par != (f.flavor == FlavorQwen) || prov != (f.flavor == FlavorOpenRouter) || rf {
				t.Fatalf("flavor fields: enable_thinking %v parallel_tool_calls %v provider %v reasoning_format %v", think, par, prov, rf)
			}
		})
	}
}

func TestV3aFlavorsSendNoV3bFields(t *testing.T) {
	for _, fl := range []string{FlavorOpenAI, FlavorAzure, FlavorOllama, FlavorVLLM, FlavorGeneric} {
		s := newStub(t, okText("ok"))
		req := simpleRequest("gpt-4.1")
		req.Tools = []llm.ToolDef{strictTool}
		req.Messages = append(req.Messages,
			llm.Message{Role: llm.RoleAssistant, Parts: []llm.Part{
				deepSeekReasoningPart("thinking", "gpt-4.1"),
				{Kind: llm.PartToolCall, ToolCall: &llm.ToolCall{ID: "c1", Name: "connect_status", Input: json.RawMessage(`{}`)}},
			}},
			llm.Message{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{CallID: "c1", Name: "connect_status", Content: "x"}}}},
		)
		if _, err := build(t, fl, s.URL, Options{Key: testKey}, hooks{}).Turn(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		body := string(s.requests()[0].body)
		for _, f := range []string{`"enable_thinking"`, `"parallel_tool_calls"`, `"provider"`, `"reasoning_format"`, `"reasoning_content"`} {
			if strings.Contains(body, f) {
				t.Errorf("%s sent %s", fl, f)
			}
		}
		if strings.Contains(body, `"role":"tool","content":"x","tool_call_id":"c1","name"`) {
			t.Errorf("%s named the tool message", fl)
		}
	}
}

// deepSeekReasonerReply is DeepSeek's documented answer in thinking mode
// with a tool call: reasoning_content beside the call, the cache in
// prompt_cache_hit_tokens and prompt_cache_miss_tokens.
func deepSeekReasonerReply() func(http.ResponseWriter) {
	return jsonReply(200, map[string]any{
		"id": "a1b2", "object": "chat.completion", "created": 1760000000, "model": "deepseek-reasoner",
		"choices": []any{map[string]any{"index": 0, "finish_reason": "tool_calls", "logprobs": nil, "message": map[string]any{
			"role": "assistant", "content": "",
			"reasoning_content": "The lag points at the connector; read its status before anything else.",
			"tool_calls": []any{map[string]any{"index": 0, "id": "call_00_dsk1", "type": "function",
				"function": map[string]any{"name": "connect_status", "arguments": `{"connector":"pg-orders"}`}}},
		}}},
		"usage": map[string]any{
			"prompt_tokens": 1200, "completion_tokens": 180, "total_tokens": 1380,
			"prompt_tokens_details":   map[string]any{"cached_tokens": 1024},
			"prompt_cache_hit_tokens": 1024, "prompt_cache_miss_tokens": 176,
			"completion_tokens_details": map[string]any{"reasoning_tokens": 150},
		},
		"system_fingerprint": "fp_ffc7281d48_prod0820_fp8_kvcache",
	})
}

func assistantReasoning(t *testing.T, body []byte) []string {
	t.Helper()
	var out []string
	for _, m := range decodeBody(t, body)["messages"].([]any) {
		mm := m.(map[string]any)
		if mm["role"] == "assistant" {
			rc, _ := mm["reasoning_content"].(string)
			out = append(out, rc)
		}
	}
	return out
}

// TestDeepSeekReasoningRule proves the rule deepSeekReasoning documents on
// DeepSeek's thinking-mode fixture: the reasoning is provider state (never
// text), goes back on the assistant turns of the current question's tool
// loop, is left out once a new question starts, and never reaches another
// model or flavor.
func TestDeepSeekReasoningRule(t *testing.T) {
	const thought = "The lag points at the connector; read its status before anything else."
	s := newStub(t, deepSeekReasonerReply(), okText("The connector task failed."))
	m := build(t, FlavorDeepSeek, s.URL, Options{Key: testKey}, hooks{})
	req := simpleRequest("deepseek-reasoner")
	req.Tools = []llm.ToolDef{strictTool}

	resp, err := m.Turn(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Stop != llm.StopToolUse {
		t.Fatalf("Stop = %q", resp.Stop)
	}
	var reasoning *llm.Part
	for i, p := range resp.Message.Parts {
		switch p.Kind {
		case llm.PartText:
			t.Fatalf("text part %q: the reasoning (or an empty content) became text", p.Text)
		case llm.PartReasoning:
			reasoning = &resp.Message.Parts[i]
		}
	}
	if reasoning == nil || reasoning.Native == nil || reasoning.Native.Provider != llm.ProviderOpenAI || reasoning.Native.Model != "deepseek-reasoner" {
		t.Fatalf("reasoning part = %+v", reasoning)
	}
	if rc, ok := deepSeekReasoning(*reasoning, "deepseek-reasoner"); !ok || rc != thought {
		t.Fatalf("reasoning = %q %v", rc, ok)
	}
	if u := resp.Usage; u.Input != 176 || u.CacheRead != 1024 || u.Output != 180 || u.Reasoning != 150 {
		t.Fatalf("usage = %+v", u)
	}

	// Inside the tool loop the reasoning goes back on its turn.
	req.Messages = append(req.Messages, resp.Message, llm.Message{Role: llm.RoleUser, Parts: []llm.Part{
		{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{CallID: "call_00_dsk1", Name: "connect_status", Content: "FAILED"}},
	}})
	resp2, err := m.Turn(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := assistantReasoning(t, s.requests()[1].body); !slices.Equal(got, []string{thought}) {
		t.Fatalf("reasoning in the tool loop = %q, want it sent back", got)
	}

	// A new question starts: the earlier question's reasoning stays out.
	req.Messages = append(req.Messages, resp2.Message, userText("And the payments connector?"))
	if _, err := m.Turn(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := assistantReasoning(t, s.requests()[2].body); !slices.Equal(got, []string{"", ""}) {
		t.Fatalf("reasoning after a new question = %q, want none", got)
	}

	// Another model never gets it, nor another flavor.
	loop := req.Messages[:3]
	other := simpleRequest("deepseek-chat")
	other.Messages = append(other.Messages, loop[1:]...)
	if _, err := m.Turn(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(s.requests()[3].body), "reasoning_content") {
		t.Fatal("reasoning sent to another model")
	}
	o := newStub(t, okText("ok"))
	same := simpleRequest("deepseek-reasoner")
	same.Messages = append(same.Messages, loop[1:]...)
	if _, err := build(t, FlavorGeneric, o.URL, Options{Key: testKey}, hooks{}).Turn(context.Background(), same); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(o.requests()[0].body), "reasoning_content") || strings.Contains(string(o.requests()[0].body), thought) {
		t.Fatal("DeepSeek reasoning sent to another flavor")
	}
}

func TestDeepSeekChatHasNoReasoning(t *testing.T) {
	s := newStub(t, okText("ok"))
	m := build(t, FlavorDeepSeek, s.URL, Options{Key: testKey}, hooks{})
	if c := m.Capabilities("deepseek-chat"); c.Reasoning || c.Effort || c.StructuredOutput || c.StrictTools || !c.PromptCache || c.ContextTokens != 128_000 {
		t.Fatalf("deepseek-chat: %+v", c)
	}
	if c := m.Capabilities("deepseek-reasoner"); !c.Reasoning || c.Effort {
		t.Fatalf("deepseek-reasoner: %+v", c)
	}
	req := simpleRequest("deepseek-reasoner")
	req.Effort = "high"
	req.OutputSchema = map[string]any{"type": "object"}
	if _, err := m.Turn(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	b := decodeBody(t, s.requests()[0].body)
	if b["reasoning_effort"] != nil || b["response_format"] != nil {
		t.Fatalf("deepseek body: %v", b)
	}
}

// TestOpenRouterAlwaysSendsDataCollectionDeny checks the provider object on
// every request: plain, with tools, with an output schema, and each retry.
func TestOpenRouterAlwaysSendsDataCollectionDeny(t *testing.T) {
	sl := &sleeps{}
	s := newStub(t, okText("a"), okText("b"), okText("c"), errReply(503, "", "busy"), okText("d"))
	m := build(t, FlavorOpenRouter, s.URL, Options{Key: testKey, Retry: retrying(sl)}, hooks{})
	plain := simpleRequest("anthropic/claude-sonnet-4.5")
	tools := simpleRequest("deepseek/deepseek-chat-v3.1")
	tools.Tools = []llm.ToolDef{strictTool}
	schema := simpleRequest("openai/gpt-4.1")
	schema.OutputSchema = map[string]any{"type": "object"}
	for _, r := range []llm.Request{plain, tools, schema, plain} {
		if _, err := m.Turn(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	reqs := s.requests()
	if len(reqs) != 5 {
		t.Fatalf("requests = %d, want 5 (one retry)", len(reqs))
	}
	for i, r := range reqs {
		p, ok := decodeBody(t, r.body)["provider"].(map[string]any)
		if !ok || p["data_collection"] != "deny" || len(p) != 1 {
			t.Fatalf("request %d provider = %v", i, p)
		}
	}
}

func TestQwenEnableThinking(t *testing.T) {
	for _, model := range []string{"qwen-plus", "qwen3-235b-a22b", "qwen3-max", "qwen-flash"} {
		s := newStub(t, okText("a"), okText("b"))
		m := build(t, FlavorQwen, s.URL, Options{Key: testKey}, hooks{})
		req := simpleRequest(model)
		req.Effort = "max"
		if _, err := m.Turn(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		req.Tools = []llm.ToolDef{strictTool}
		if _, err := m.Turn(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		r := s.requests()
		b0, b1 := decodeBody(t, r[0].body), decodeBody(t, r[1].body)
		if b0["enable_thinking"] != false || b1["enable_thinking"] != false {
			t.Errorf("%s: enable_thinking = %v / %v, want false on every non-streamed call", model, b0["enable_thinking"], b1["enable_thinking"])
		}
		if _, ok := b0["parallel_tool_calls"]; ok || b1["parallel_tool_calls"] != true {
			t.Errorf("%s: parallel_tool_calls = %v / %v, want only with tools", model, b0["parallel_tool_calls"], b1["parallel_tool_calls"])
		}
		if b0["reasoning_effort"] != nil {
			t.Errorf("%s: reasoning_effort sent", model)
		}
		if c := m.Capabilities(model); c.Reasoning || c.Effort || !c.ParallelToolCalls || !c.PromptCache {
			t.Errorf("%s: %+v", model, c)
		}
	}
	for _, model := range []string{"qwq-plus", "qwen3-235b-a22b-thinking-2507"} {
		s := newStub(t, okText("x"))
		_, err := build(t, FlavorQwen, s.URL, Options{Key: testKey}, hooks{}).Turn(context.Background(), simpleRequest(model))
		if !errors.Is(err, llm.ErrNotSent) || apiErr(t, err).Kind != llm.ErrBadRequest {
			t.Errorf("%s: err = %v, want a bad request that was not sent", model, err)
		}
		if len(s.requests()) != 0 {
			t.Errorf("%s: a thinking-only model was sent a non-streamed request", model)
		}
	}
}

func TestReasoningRulesPerFlavor(t *testing.T) {
	for _, tc := range []struct {
		flavor, model string
		reasoning     bool
		effort        string // "" = not sent
		format        string
		structured    bool
	}{
		{FlavorGroq, "openai/gpt-oss-120b", true, "high", "", true},
		{FlavorGroq, "openai/gpt-oss-20b", true, "high", "", true},
		{FlavorGroq, "qwen/qwen3-32b", true, "", "hidden", false},
		{FlavorGroq, "llama-3.3-70b-versatile", false, "", "", false},
		{FlavorXAI, "grok-3-mini", true, "high", "", true},
		{FlavorXAI, "grok-4", true, "", "", true},
		{FlavorXAI, "grok-4-fast-non-reasoning", false, "", "", true},
		{FlavorMistral, "magistral-medium-latest", true, "", "", true},
		{FlavorMistral, "mistral-small-latest", false, "", "", true},
		{FlavorTogether, "deepseek-ai/DeepSeek-V3", false, "", "", false},
		{FlavorOpenRouter, "openai/o3", false, "", "", false},
	} {
		s := newStub(t, okText("ok"))
		m := build(t, tc.flavor, s.URL, Options{Key: testKey}, hooks{})
		c := m.Capabilities(tc.model)
		if c.Reasoning != tc.reasoning || c.Effort != (tc.effort != "") || c.StructuredOutput != tc.structured || c.StrictTools {
			t.Errorf("%s %s: %+v", tc.flavor, tc.model, c)
		}
		req := simpleRequest(tc.model)
		req.Effort = "max"
		req.OutputSchema = map[string]any{"type": "object", "properties": map[string]any{}, "required": []string{}, "additionalProperties": false}
		if _, err := m.Turn(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		b := decodeBody(t, s.requests()[0].body)
		got, _ := b["reasoning_effort"].(string)
		format, _ := b["reasoning_format"].(string)
		rf, hasRF := b["response_format"].(map[string]any)
		if got != tc.effort || format != tc.format || hasRF != tc.structured {
			t.Errorf("%s %s: reasoning_effort %q reasoning_format %q response_format %v", tc.flavor, tc.model, got, format, rf)
		}
		if hasRF {
			if _, strict := rf["json_schema"].(map[string]any)["strict"]; strict {
				t.Errorf("%s %s: strict response_format on a host that does not claim it", tc.flavor, tc.model)
			}
		}
	}
	if xaiEffort("", "low") != "low" || xaiEffort("", "medium") != "high" || xaiEffort("", "x") != "" {
		t.Error("xaiEffort")
	}
}

// TestCacheUsageFields checks each host's cache report lands in CacheRead
// and out of Input.
func TestCacheUsageFields(t *testing.T) {
	for _, tc := range []struct {
		flavor string
		usage  map[string]any
		input  int64
		cached int64
	}{
		{FlavorDeepSeek, map[string]any{"prompt_tokens": 1000, "completion_tokens": 10, "prompt_cache_hit_tokens": 640, "prompt_cache_miss_tokens": 360}, 360, 640},
		{FlavorDeepSeek, map[string]any{"prompt_tokens": 1000, "completion_tokens": 10, "prompt_cache_hit_tokens": 640, "prompt_tokens_details": map[string]any{"cached_tokens": 640}}, 360, 640},
		{FlavorQwen, map[string]any{"prompt_tokens": 1000, "completion_tokens": 10, "prompt_tokens_details": map[string]any{"cached_tokens": 256}}, 744, 256},
		{FlavorGroq, map[string]any{"prompt_tokens": 1000, "completion_tokens": 10, "prompt_tokens_details": map[string]any{"cached_tokens": 512}, "queue_time": 0.1}, 488, 512},
		{FlavorXAI, map[string]any{"prompt_tokens": 1000, "completion_tokens": 10, "prompt_tokens_details": map[string]any{"cached_tokens": 900, "text_tokens": 1000}}, 100, 900},
		{FlavorOpenRouter, map[string]any{"prompt_tokens": 1000, "completion_tokens": 10, "prompt_tokens_details": map[string]any{"cached_tokens": 100}, "cost": 0.001}, 900, 100},
		{FlavorMistral, map[string]any{"prompt_tokens": 1000, "completion_tokens": 10}, 1000, 0},
		{FlavorTogether, map[string]any{"prompt_tokens": 1000, "completion_tokens": 10, "prompt_cache_hit_tokens": 5000}, 0, 1000},
	} {
		s := newStub(t, jsonReply(200, completion(map[string]any{"content": "x"}, "stop", tc.usage)))
		resp, err := build(t, tc.flavor, s.URL, Options{Key: testKey}, hooks{}).Turn(context.Background(), simpleRequest("m"))
		if err != nil {
			t.Fatal(err)
		}
		if u := resp.Usage; u.Input != tc.input || u.CacheRead != tc.cached || u.CacheWrite != 0 || u.Output != 10 || u.Estimated {
			t.Errorf("%s %v: usage = %+v, want input %d cached %d", tc.flavor, tc.usage, u, tc.input, tc.cached)
		}
	}
}

func TestChinaOperated(t *testing.T) {
	china, _ := config.ParseBaseURL("x", "https://dashscope.aliyuncs.com/compatible-mode/v1", false)
	proxy, _ := config.ParseBaseURL("x", "https://llm-proxy.example.com/deepseek/v1", false)
	for _, tc := range []struct {
		flavor string
		base   *config.BaseURL
		want   string
	}{
		{FlavorDeepSeek, nil, "api.deepseek.com"},
		{FlavorQwen, nil, ""},
		{FlavorQwen, &china, "dashscope.aliyuncs.com"},
		{FlavorDeepSeek, &proxy, ""},
		{FlavorOpenAI, nil, ""},
		{FlavorMistral, nil, ""},
		{FlavorGroq, nil, ""},
		{FlavorTogether, nil, ""},
		{FlavorOpenRouter, nil, ""},
		{FlavorXAI, nil, ""},
		{FlavorOllama, nil, ""},
	} {
		if got := ChinaOperatedEndpoint(tc.flavor, tc.base); got != tc.want {
			t.Errorf("ChinaOperatedEndpoint(%s, %v) = %q, want %q", tc.flavor, tc.base, got, tc.want)
		}
		m, err := New(Options{Flavor: tc.flavor, BaseURL: tc.base, Key: testKey})
		if err != nil {
			t.Fatal(err)
		}
		if got := m.ChinaOperated(); got != tc.want {
			t.Errorf("%s %v: ChinaOperated() = %q, want %q", tc.flavor, tc.base, got, tc.want)
		}
	}
}

func TestHostErrorShapes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		flavor string
		reply  func(http.ResponseWriter)
		kind   llm.ErrorKind
		status int
	}{
		{"xai string error", FlavorXAI, jsonReply(401, map[string]any{"code": "The caller does not have permission", "error": "Incorrect API key provided"}), llm.ErrAuth, 401},
		{"xai quota", FlavorXAI, jsonReply(429, map[string]any{"code": "Some resource has been exhausted", "error": "You have exceeded your current quota"}), llm.ErrBilling, 429},
		{"deepseek balance", FlavorDeepSeek, jsonReply(402, map[string]any{"error": map[string]any{"message": "Insufficient Balance", "type": "unknown_error", "code": "invalid_request_error"}}), llm.ErrBilling, 402},
		{"dashscope arrearage", FlavorQwen, jsonReply(400, map[string]any{"error": map[string]any{"message": "Access denied, please make sure your account is in good standing.", "type": "Arrearage", "code": "Arrearage"}}), llm.ErrBilling, 400},
		{"openrouter credits", FlavorOpenRouter, jsonReply(402, map[string]any{"error": map[string]any{"code": 402, "message": "Insufficient credits"}}), llm.ErrBilling, 402},
		{"openrouter 403 without moderation", FlavorOpenRouter, jsonReply(403, map[string]any{"error": map[string]any{"code": 403, "message": "Key disabled"}}), llm.ErrAuth, 403},
		{"mistral top-level", FlavorMistral, jsonReply(422, map[string]any{"object": "error", "message": "Invalid model", "type": "invalid_model", "param": nil, "code": "1500"}), llm.ErrBadRequest, 422},
	} {
		s := newStub(t, tc.reply)
		_, err := build(t, tc.flavor, s.URL, Options{Key: testKey, Retry: retrying(&sleeps{})}, hooks{}).Turn(context.Background(), simpleRequest("m"))
		ae := apiErr(t, err)
		if ae.Kind != tc.kind || ae.Status != tc.status {
			t.Errorf("%s: APIError = {%s %d}, want {%s %d}", tc.name, ae.Kind, ae.Status, tc.kind, tc.status)
		}
		if tc.kind == llm.ErrBilling && len(s.requests()) != 1 {
			t.Errorf("%s: billing retried", tc.name)
		}
	}
}

func TestHostPromptFiltersAreRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, flavor, detail string
		reply                func(http.ResponseWriter)
	}{
		{"dashscope data inspection", FlavorQwen, "data_inspection_failed", jsonReply(400, map[string]any{"error": map[string]any{
			"message": "Input data may contain inappropriate content.", "type": "data_inspection_failed", "code": "data_inspection_failed"}})},
		{"openrouter moderation", FlavorOpenRouter, "moderation", jsonReply(403, map[string]any{"error": map[string]any{
			"code": 403, "message": "openai/gpt-4.1 requires moderation on OpenAI. Your input was flagged for \"harassment\".",
			"metadata": map[string]any{"reasons": []string{"harassment"}, "flagged_input": "prompt-echo-7f3a", "provider_name": "OpenAI", "model_slug": "openai/gpt-4.1"}}})},
	} {
		s := newStub(t, tc.reply)
		resp, err := build(t, tc.flavor, s.URL, Options{Key: testKey}, hooks{}).Turn(context.Background(), simpleRequest("m"))
		if err != nil {
			t.Fatalf("%s: %v; a filtered prompt is a refusal", tc.name, err)
		}
		if resp.Stop != llm.StopRefusal || resp.StopDetail != tc.detail || !resp.Usage.Estimated {
			t.Errorf("%s: resp = %+v", tc.name, resp)
		}
		if strings.Contains(resp.StopDetail, "prompt-echo") {
			t.Errorf("%s: the flagged input reached StopDetail", tc.name)
		}
	}
	// Only OpenRouter's 403 means moderation.
	s := newStub(t, jsonReply(403, map[string]any{"error": map[string]any{"code": 403, "message": "x", "metadata": map[string]any{"reasons": []string{"x"}}}}))
	_, err := build(t, FlavorGroq, s.URL, Options{Key: testKey}, hooks{}).Turn(context.Background(), simpleRequest("m"))
	if apiErr(t, err).Kind != llm.ErrAuth {
		t.Fatalf("groq 403 = %v", err)
	}
}

func TestContentFilterExplainedInContent(t *testing.T) {
	s := newStub(t, jsonReply(200, completion(map[string]any{"content": "I can't help with that."}, "content_filter",
		map[string]any{"prompt_tokens": 100, "completion_tokens": 3})))
	resp, err := build(t, FlavorDeepSeek, s.URL, Options{Key: testKey}, hooks{}).Turn(context.Background(), simpleRequest("deepseek-chat"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Stop != llm.StopRefusal || resp.StopDetail != "content_filter: I can't help with that." {
		t.Fatalf("Stop=%q detail=%q", resp.Stop, resp.StopDetail)
	}
}

func TestMistralMadeCallIDs(t *testing.T) {
	id := regexp.MustCompile(`^[a-zA-Z0-9]{9}$`)
	s := newStub(t, jsonReply(200, completion(map[string]any{"content": nil, "tool_calls": []any{
		map[string]any{"type": "function", "function": map[string]any{"name": "connect_status", "arguments": `{}`}},
	}}, "tool_calls", map[string]any{"prompt_tokens": 100, "completion_tokens": 3})))
	resp, err := build(t, FlavorMistral, s.URL, Options{Key: testKey}, hooks{}).Turn(context.Background(), simpleRequest("mistral-large-latest"))
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Message.Parts[0].ToolCall.ID; !id.MatchString(got) {
		t.Fatalf("made id %q, Mistral takes 9 letters and digits", got)
	}
	for range 50 {
		if got := newCallID(FlavorMistral); !id.MatchString(got) {
			t.Fatalf("newCallID = %q", got)
		}
	}
	if got := newCallID(FlavorGroq); !strings.HasPrefix(got, "call_") {
		t.Fatalf("newCallID(groq) = %q", got)
	}
}

func TestHostedContextTables(t *testing.T) {
	for _, tc := range []struct {
		flavor, model string
		want          int64
	}{
		{FlavorDeepSeek, "deepseek-reasoner", 128_000},
		{FlavorQwen, "qwen3-max", 262_144},
		{FlavorQwen, "qwen-plus-latest", 131_072},
		{FlavorQwen, "qwen-unknown", 0},
		{FlavorMistral, "mistral-large-latest", 131_072},
		{FlavorMistral, "codestral-latest", 256_000},
		{FlavorGroq, "openai/gpt-oss-120b", 131_072},
		{FlavorXAI, "grok-4-fast-reasoning", 2_000_000},
		{FlavorXAI, "grok-4-0709", 256_000},
		{FlavorXAI, "grok-3-mini", 131_072},
		{FlavorTogether, "anything", 0},
		{FlavorOpenRouter, "anything", 0},
	} {
		m, err := New(Options{Flavor: tc.flavor, Key: testKey})
		if err != nil {
			t.Fatal(err)
		}
		if got := m.Capabilities(tc.model).ContextTokens; got != tc.want {
			t.Errorf("%s %s: context %d, want %d", tc.flavor, tc.model, got, tc.want)
		}
	}
	m, _ := New(Options{Flavor: FlavorXAI, Key: testKey, ContextTokens: 50_000})
	if m.Capabilities("grok-4").ContextTokens != 50_000 {
		t.Error("ContextTokens does not override the table")
	}
}
