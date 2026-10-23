package converse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/llmtest"
)

// recorded is one request the fake Bedrock received.
type recorded struct {
	path   string
	header http.Header
	body   []byte
}

// fakeBedrock answers each request with the next scripted reply (the last
// one repeats) and records the requests.
type fakeBedrock struct {
	*httptest.Server
	mu      sync.Mutex
	reqs    []recorded
	replies []http.HandlerFunc
}

func newFake(t *testing.T, tls bool, replies ...http.HandlerFunc) *fakeBedrock {
	t.Helper()
	f := &fakeBedrock{replies: replies}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.reqs = append(f.reqs, recorded{path: r.URL.EscapedPath(), header: r.Header.Clone(), body: b})
		reply := f.replies[0]
		if len(f.replies) > 1 {
			f.replies = f.replies[1:]
		}
		f.mu.Unlock()
		reply(w, r)
	})
	if tls {
		f.Server = httptest.NewTLSServer(h)
	} else {
		f.Server = httptest.NewServer(h)
	}
	t.Cleanup(f.Close)
	return f
}

func (f *fakeBedrock) requests() []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recorded(nil), f.reqs...)
}

func jsonReply(status int, body string, headers ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		for i := 0; i+1 < len(headers); i += 2 {
			w.Header().Set(headers[i], headers[i+1])
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

func answer(t *testing.T, a llmtest.Answer) http.HandlerFunc {
	_, body := renderAnswer(t, a)
	return jsonReply(http.StatusOK, string(body))
}

func codeError(status int, code, message string) http.HandlerFunc {
	_, body := renderCodeError(code, message)
	return jsonReply(status, string(body))
}

func newModel(t *testing.T, cfg Config) *Model {
	t.Helper()
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

func userText(s string) llm.Message {
	return llm.Message{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartText, Text: s}}}
}

var lagTool = llm.ToolDef{
	Name:        "kafka_consumer_lag",
	Description: "Consumer group lag.",
	Schema: map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"group": map[string]any{"type": "string"}, "limit": map[string]any{"type": "integer", "maximum": json.Number("12345678901234567890"), "minimum": json.Number("-9007199254740991")}},
		"required":             []string{"group"},
		"additionalProperties": false,
	},
}

// loopRequest is a request in the loop's shape: cache hints, tools, and a
// history with a tool call answered by an error result.
func loopRequest(model string) llm.Request {
	return llm.Request{
		Model:     model,
		System:    "You investigate CDC incidents.",
		MaxTokens: 16000,
		Effort:    "high",
		Thinking:  true,
		Tools:     []llm.ToolDef{lagTool},
		Cache:     llm.CacheHints{System: true, Tools: true, History: true},
		Messages: []llm.Message{
			userText("The consumer is behind."),
			{Role: llm.RoleAssistant, Parts: []llm.Part{
				{Kind: llm.PartReasoning, Native: &llm.Native{Provider: llm.ProviderAnthropic, Raw: json.RawMessage(`{"type":"thinking"}`)}},
				{Kind: llm.PartText, Text: "Reading the lag."},
				{Kind: llm.PartToolCall, ToolCall: &llm.ToolCall{ID: "tooluse_1", Name: "kafka_consumer_lag", Input: json.RawMessage(`{"group":"order-sync","limit":12345678901234567,"ratio":1.5}`)}},
			}},
			{Role: llm.RoleUser, Parts: []llm.Part{
				{Kind: llm.PartText, Text: "Driver: check the DLQ too."},
				{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{CallID: "tooluse_1", Name: "kafka_consumer_lag", Content: `<tool_output tool="kafka_consumer_lag" untrusted="true">error: group not found</tool_output>`, IsError: true}},
			}},
		},
	}
}

func keysOf(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("block %s: %v", raw, err)
	}
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return strings.Join(ks, ",")
}

// TestWireShapeNova: the loop's request on a model that caches: system and
// history cache points, no tool cache point (Nova does not take one), the
// tool schema as is, the tool call's input with exact numbers, results
// before the user's text, status error, no effort or thinking field.
func TestWireShapeNova(t *testing.T) {
	f := newFake(t, false, answer(t, llmtest.Answer{Text: "ok", InputTokens: 300, OutputTokens: 2}))
	m := newModel(t, testConfig(f.URL))
	if _, err := m.Turn(context.Background(), loopRequest(testModel)); err != nil {
		t.Fatalf("Turn: %v", err)
	}
	r := f.requests()[0]
	if r.path != "/model/amazon.nova-pro-v1%3A0/converse" {
		t.Fatalf("path = %q", r.path)
	}
	if !strings.HasPrefix(r.header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
		t.Fatalf("Authorization = %q, want SigV4", r.header.Get("Authorization"))
	}
	for _, banned := range []string{"effort", "thinking", "reasoning", "additionalModelRequestFields", "strict"} {
		if bytes.Contains(r.body, []byte(banned)) {
			t.Fatalf("the body carries %q: %s", banned, r.body)
		}
	}
	b := parseBody(t, r.body)
	if len(b.System) != 2 || keysOf(t, b.System[0]) != "text" || string(b.System[1]) != `{"cachePoint":{"type":"default"}}` {
		t.Fatalf("system = %s", b.System)
	}
	if b.InferenceConfig["maxTokens"] != float64(16000) {
		t.Fatalf("inferenceConfig = %v", b.InferenceConfig)
	}
	if b.ToolConfig == nil || len(b.ToolConfig.Tools) != 1 {
		t.Fatalf("toolConfig = %+v", b.ToolConfig)
	}
	var spec struct {
		ToolSpec struct {
			Name        string `json:"name"`
			InputSchema struct {
				JSON json.RawMessage `json:"json"`
			} `json:"inputSchema"`
		} `json:"toolSpec"`
	}
	if err := json.Unmarshal(b.ToolConfig.Tools[0], &spec); err != nil {
		t.Fatal(err)
	}
	wantSchema, _ := json.Marshal(lagTool.Schema)
	if spec.ToolSpec.Name != lagTool.Name || !llmtestSameJSON(spec.ToolSpec.InputSchema.JSON, wantSchema) {
		t.Fatalf("toolSpec = %s %s", spec.ToolSpec.Name, spec.ToolSpec.InputSchema.JSON)
	}
	if b.ToolConfig.ToolChoice != nil {
		t.Fatalf("toolChoice = %v, want none sent (auto is the default)", b.ToolConfig.ToolChoice)
	}
	for _, lit := range []string{`"maximum":12345678901234567890`, `"minimum":-9007199254740991`} {
		if !bytes.Contains(b.ToolConfig.Tools[0], []byte(lit)) {
			t.Fatalf("schema number %s changed: %s", lit, b.ToolConfig.Tools[0])
		}
	}

	if len(b.Messages) != 3 {
		t.Fatalf("messages = %d, want 3", len(b.Messages))
	}
	asst := b.Messages[1]
	if asst.Role != "assistant" || len(asst.Content) != 2 || keysOf(t, asst.Content[0]) != "text" || keysOf(t, asst.Content[1]) != "toolUse" {
		t.Fatalf("assistant = %s (reasoning must be dropped)", asst.Content)
	}
	if !bytes.Contains(asst.Content[1], []byte(`"limit":12345678901234567`)) || !bytes.Contains(asst.Content[1], []byte(`"ratio":1.5`)) {
		t.Fatalf("tool input numbers changed: %s", asst.Content[1])
	}
	user := b.Messages[2]
	if len(user.Content) != 3 || keysOf(t, user.Content[0]) != "toolResult" || keysOf(t, user.Content[1]) != "text" || string(user.Content[2]) != `{"cachePoint":{"type":"default"}}` {
		t.Fatalf("last user message = %s", user.Content)
	}
	var tr toolResultBlock
	_ = json.Unmarshal(user.Content[0], &tr)
	if tr.ToolResult.Status != "error" || tr.ToolResult.ToolUseID != "tooluse_1" {
		t.Fatalf("toolResult = %+v", tr.ToolResult)
	}
}

func llmtestSameJSON(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	xb, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return bytes.Equal(xb, yb)
}

// TestWireShapeWithoutCacheOrStatus: a model the table does not know gets
// no cache point and no toolResult status (the wrapped text says error).
func TestWireShapeWithoutCacheOrStatus(t *testing.T) {
	f := newFake(t, false, answer(t, llmtest.Answer{Text: "ok", InputTokens: 300, OutputTokens: 2}))
	m := newModel(t, testConfig(f.URL))
	if _, err := m.Turn(context.Background(), loopRequest("mistral.mistral-large-2407-v1:0")); err != nil {
		t.Fatalf("Turn: %v", err)
	}
	body := f.requests()[0].body
	if bytes.Contains(body, []byte("cachePoint")) || bytes.Contains(body, []byte(`"status"`)) {
		t.Fatalf("body = %s", body)
	}
	if c := m.Capabilities("mistral.mistral-large-2407-v1:0"); c.PromptCache || c.StructuredOutput || c.StrictTools || c.Effort || c.Compaction || !c.ToolCalls {
		t.Fatalf("capabilities = %+v", c)
	}
}

func TestCapabilitiesByModel(t *testing.T) {
	for _, tc := range []struct {
		id    string
		cache bool
		ctx   int64
	}{
		{"amazon.nova-pro-v1:0", true, 300_000},
		{"us.amazon.nova-pro-v1:0", true, 300_000},
		{"arn:aws:bedrock:us-east-1:123456789012:inference-profile/eu.amazon.nova-lite-v1:0", true, 300_000},
		{"amazon.nova-premier-v1:0", true, 1_000_000},
		{"amazon.nova-micro-v1:0", true, 128_000},
		{"meta.llama3-3-70b-instruct-v1:0", false, 0},
		{"openai.gpt-oss-120b-1:0", false, 0},
	} {
		c := (&Model{}).Capabilities(tc.id)
		if c.PromptCache != tc.cache || c.ContextTokens != tc.ctx || !c.ToolCalls || c.ToolNameMax != 64 || c.StructuredOutput {
			t.Errorf("%s: %+v", tc.id, c)
		}
	}
}

// TestRefusals: a guardrail and a content filter are refusals, with the
// stop reason and the text the service answered as the detail.
func TestRefusals(t *testing.T) {
	for _, stop := range []string{"guardrail_intervened", "content_filtered"} {
		t.Run(stop, func(t *testing.T) {
			body := `{"output":{"message":{"role":"assistant","content":[{"text":"Sorry, the model cannot answer this."}]}},"stopReason":"` + stop + `","usage":{"inputTokens":40,"outputTokens":8,"totalTokens":48},"metrics":{"latencyMs":3}}`
			f := newFake(t, false, jsonReply(http.StatusOK, body))
			resp, err := newModel(t, testConfig(f.URL)).Turn(context.Background(), llm.Request{Model: testModel, Messages: []llm.Message{userText("hi")}})
			if err != nil {
				t.Fatalf("Turn: %v", err)
			}
			if resp.Stop != llm.StopRefusal || resp.ProviderStop != stop {
				t.Fatalf("stop = %q (%q)", resp.Stop, resp.ProviderStop)
			}
			if resp.StopDetail != stop+": Sorry, the model cannot answer this." {
				t.Fatalf("detail = %q", resp.StopDetail)
			}
		})
	}
}

func TestStopReasons(t *testing.T) {
	for _, tc := range []struct {
		reason string
		tool   bool
		want   llm.StopReason
	}{
		{"end_turn", false, llm.StopEndTurn},
		{"stop_sequence", false, llm.StopEndTurn},
		{"end_turn", true, llm.StopToolUse},
		{"tool_use", true, llm.StopToolUse},
		{"max_tokens", true, llm.StopMaxTokens},
		{"model_context_window_exceeded", false, llm.StopContextExceeded},
		{"malformed_tool_use", false, llm.StopOther},
		{"malformed_model_output", false, llm.StopOther},
		{"something_new", false, llm.StopOther},
	} {
		if got := stopReason(typesStop(tc.reason), tc.tool); got != tc.want {
			t.Errorf("%s (tool %v) = %s, want %s", tc.reason, tc.tool, got, tc.want)
		}
	}
}

// TestStructuredOutputThroughForcedTool: a request that carries OutputSchema
// (the evals judge sends it straight to the adapter) becomes one forced tool
// whose schema is the output schema, toolChoice any; the answer's call is
// returned as the turn's tool call. Capabilities says no structured output,
// so the agent's own fallback is what runs in a room.
func TestStructuredOutputThroughForcedTool(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{"verdict": map[string]any{"type": "string"}}, "required": []string{"verdict"}}
	f := newFake(t, false, answer(t, llmtest.Answer{Calls: []llmtest.Call{{ID: "tooluse_9", Name: outputToolName, Input: `{"verdict":"correct"}`}}, InputTokens: 90, OutputTokens: 12}))
	m := newModel(t, testConfig(f.URL))
	if m.Capabilities(testModel).StructuredOutput {
		t.Fatal("Converse must report no structured output, so the agent's fallback runs")
	}
	resp, err := m.Turn(context.Background(), llm.Request{Model: testModel, System: "Grade it.", MaxTokens: 512, OutputSchema: schema, Messages: []llm.Message{userText("grade")}})
	if err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if resp.Stop != llm.StopToolUse || len(resp.Message.Parts) != 1 || resp.Message.Parts[0].ToolCall == nil || string(resp.Message.Parts[0].ToolCall.Input) != `{"verdict":"correct"}` {
		t.Fatalf("resp = %+v", resp)
	}
	b := parseBody(t, f.requests()[0].body)
	if b.ToolConfig == nil || len(b.ToolConfig.Tools) != 1 || !bytes.Contains(b.ToolConfig.Tools[0], []byte(`"name":"emit_output"`)) {
		t.Fatalf("toolConfig = %+v", b.ToolConfig)
	}
	if _, ok := b.ToolConfig.ToolChoice["any"]; !ok {
		t.Fatalf("toolChoice = %v, want any", b.ToolConfig.ToolChoice)
	}
}

// TestToolChoiceNone: with no tool block in the history the tools are left
// out (Converse has no "none"); with one, toolConfig stays, on auto.
func TestToolChoiceNone(t *testing.T) {
	f := newFake(t, false, answer(t, llmtest.Answer{Text: "summary", InputTokens: 300, OutputTokens: 2}))
	m := newModel(t, testConfig(f.URL))
	req := llm.Request{Model: testModel, Tools: []llm.ToolDef{lagTool}, ToolChoice: llm.ToolChoiceNone, Messages: []llm.Message{userText("summarise")}}
	if _, err := m.Turn(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	withHistory := loopRequest(testModel)
	withHistory.ToolChoice = llm.ToolChoiceNone
	if _, err := m.Turn(context.Background(), withHistory); err != nil {
		t.Fatal(err)
	}
	reqs := f.requests()
	if b := parseBody(t, reqs[0].body); b.ToolConfig != nil {
		t.Fatalf("toolConfig sent without tool history: %+v", b.ToolConfig)
	}
	if b := parseBody(t, reqs[1].body); b.ToolConfig == nil || b.ToolConfig.ToolChoice != nil {
		t.Fatalf("toolConfig with tool history = %+v", b.ToolConfig)
	}
}

// TestHistoryKeepsRolesAlternating: a message left empty (only another
// provider's reasoning) is dropped and same-role neighbours are merged.
func TestHistoryKeepsRolesAlternating(t *testing.T) {
	msgs, _, err := encodeMessages([]llm.Message{
		userText("a"),
		{Role: llm.RoleAssistant, Parts: []llm.Part{{Kind: llm.PartReasoning}}},
		userText("b"),
		{Role: llm.RoleAssistant, Parts: []llm.Part{{Kind: llm.PartText, Text: ""}, {Kind: llm.PartText, Text: "c"}}},
	}, family{})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 || len(msgs[0].Content) != 2 || len(msgs[1].Content) != 1 {
		t.Fatalf("messages = %+v", msgs)
	}
	if _, _, err := encodeMessages([]llm.Message{{Role: "system", Parts: []llm.Part{{Kind: llm.PartText, Text: "x"}}}}, family{}); err == nil {
		t.Fatal("an unknown role must be refused before sending")
	}
}

// TestInvalidToolInputGoesBackAsAnObject: a call whose arguments were not a
// JSON object (answered by the agent with an error result) goes back with an
// empty object, so it stays paired.
func TestInvalidToolInputGoesBackAsAnObject(t *testing.T) {
	for _, in := range []string{`{"group":`, `[1,2]`, ``} {
		v := toolInput(json.RawMessage(in))
		if m, ok := v.(map[string]any); !ok || len(m) != 0 {
			t.Errorf("toolInput(%q) = %#v", in, v)
		}
	}
}

func TestUsageNormalisation(t *testing.T) {
	for _, tc := range []struct {
		name, usage string
		want        llm.Usage
	}{
		{"cache outside input", `{"inputTokens":100,"outputTokens":10,"totalTokens":610,"cacheReadInputTokens":400,"cacheWriteInputTokens":100}`,
			llm.Usage{Input: 100, Output: 10, CacheRead: 400, CacheWrite: 100}},
		{"cache inside input", `{"inputTokens":600,"outputTokens":10,"totalTokens":610,"cacheReadInputTokens":400,"cacheWriteInputTokens":100}`,
			llm.Usage{Input: 100, Output: 10, CacheRead: 400, CacheWrite: 100}},
		{"no cache", `{"inputTokens":600,"outputTokens":10,"totalTokens":610}`, llm.Usage{Input: 600, Output: 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"output":{"message":{"role":"assistant","content":[{"text":"ok"}]}},"stopReason":"end_turn","usage":` + tc.usage + `,"metrics":{"latencyMs":1}}`
			f := newFake(t, false, jsonReply(http.StatusOK, body))
			resp, err := newModel(t, testConfig(f.URL)).Turn(context.Background(), llm.Request{Model: testModel, Messages: []llm.Message{userText("hi")}})
			if err != nil {
				t.Fatalf("Turn: %v", err)
			}
			if resp.Usage != tc.want {
				t.Fatalf("usage = %+v, want %+v", resp.Usage, tc.want)
			}
		})
	}
}

// TestMissingUsageIsEstimated: an answer without usage is estimated from the
// bytes (design 8.3) and skips the truncation check.
func TestMissingUsageIsEstimated(t *testing.T) {
	body := `{"output":{"message":{"role":"assistant","content":[{"text":"ok"}]}},"stopReason":"end_turn","metrics":{"latencyMs":1}}`
	f := newFake(t, false, jsonReply(http.StatusOK, body))
	resp, err := newModel(t, testConfig(f.URL)).Turn(context.Background(), llm.Request{Model: testModel, Messages: []llm.Message{userText(strings.Repeat("lag ", 500))}})
	if err != nil {
		t.Fatalf("Turn: %v", err)
	}
	reqBytes := int64(len(f.requests()[0].body))
	if !resp.Usage.Estimated || resp.Usage.Input != (reqBytes+2)/3 || resp.Usage.Output != int64(len(body)+2)/3 {
		t.Fatalf("usage = %+v (request %d bytes, response %d)", resp.Usage, reqBytes, len(body))
	}
}

// TestTruncation: a reported prompt under half of the request's lower-bound
// estimate is ErrTruncated, unretryable, with the billed answer returned.
func TestTruncation(t *testing.T) {
	f := newFake(t, false, answer(t, llmtest.Answer{Text: "ok", InputTokens: 50, OutputTokens: 2}))
	req := llm.Request{Model: testModel, System: "rules", Messages: []llm.Message{userText(strings.Repeat("x", 4000))}}
	resp, err := newModel(t, testConfig(f.URL)).Turn(context.Background(), req)
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrTruncated || !llm.Unretryable(err) {
		t.Fatalf("err = %v, want an unretryable truncated APIError", err)
	}
	if resp.Usage.Input != 50 {
		t.Fatalf("the billed answer is lost: %+v", resp.Usage)
	}
	if !strings.Contains(err.Error(), "truncated the prompt") {
		t.Fatalf("err text = %q", err.Error())
	}

	// A prompt that fits its bytes passes.
	f2 := newFake(t, false, answer(t, llmtest.Answer{Text: "ok", InputTokens: 600, OutputTokens: 2}))
	if _, err := newModel(t, testConfig(f2.URL)).Turn(context.Background(), req); err != nil {
		t.Fatalf("a whole prompt: %v", err)
	}
}

// TestClaudeModelRefused: a Claude id under bedrock-converse is refused at
// start, and Turn refuses it before sending.
func TestClaudeModelRefused(t *testing.T) {
	for _, id := range []string{
		"anthropic.claude-opus-5-5-v1:0",
		"us.anthropic.claude-sonnet-5-v1:0",
		"global.anthropic.claude-haiku-5-5",
		"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-opus-5-5-v1:0",
		"claude-opus-5-5",
		"arn:aws:bedrock:us-east-1:123:custom-model/anthropic.claude-3-haiku-20240307-v1:0:200k/abc123",
	} {
		cfg := testConfig("http://127.0.0.1:1")
		cfg.Models = []string{"amazon.nova-pro-v1:0", id}
		_, err := New(cfg)
		if !errors.Is(err, ErrClaudeModel) || !strings.Contains(err.Error(), "use PIPEROOM_MODEL_PROVIDER=bedrock") {
			t.Errorf("New with %q: err = %v", id, err)
		}
	}
	f := newFake(t, false, answer(t, llmtest.Answer{Text: "never"}))
	_, err := newModel(t, testConfig(f.URL)).Turn(context.Background(), llm.Request{Model: "us.anthropic.claude-opus-5-5-v1:0", Messages: []llm.Message{userText("hi")}})
	var ae *llm.APIError
	if !errors.Is(err, llm.ErrNotSent) || !errors.As(err, &ae) || ae.Kind != llm.ErrBadRequest || !strings.Contains(err.Error(), "use PIPEROOM_MODEL_PROVIDER=bedrock") {
		t.Fatalf("err = %v", err)
	}
	if n := len(f.requests()); n != 0 {
		t.Fatalf("a Claude request was sent (%d)", n)
	}
	if err := CheckModel("amazon.nova-pro-v1:0"); err != nil {
		t.Fatalf("Nova refused: %v", err)
	}
}

func TestNewChecksConfig(t *testing.T) {
	noEnv := func(string) string { return "" }
	if _, err := New(Config{AWS: &aws.Config{Credentials: staticCreds}, Getenv: noEnv}); err == nil || !strings.Contains(err.Error(), "AWS_REGION") {
		t.Fatalf("no region: %v", err)
	}
	if _, err := New(Config{AWS: &aws.Config{Region: "us-east-1"}, Getenv: noEnv}); err == nil || !strings.Contains(err.Error(), BearerTokenEnv) {
		t.Fatalf("no credentials: %v", err)
	}
	if _, err := New(Config{Region: "us-east-1", Getenv: noEnv}); err == nil {
		t.Fatal("no AWS config and no key must fail")
	}
	withKey := func(k string) string {
		if k == BearerTokenEnv {
			return "set"
		}
		return ""
	}
	if _, err := New(Config{Region: "us-east-1", Getenv: withKey}); err != nil {
		t.Fatalf("a Bedrock API key alone: %v", err)
	}
	// The shared config is copied, never changed.
	shared := &aws.Config{Region: "eu-west-1", Credentials: staticCreds}
	if _, err := New(Config{AWS: shared, Region: "us-east-1", Getenv: noEnv}); err != nil || shared.Region != "eu-west-1" {
		t.Fatalf("err %v, shared region %q", err, shared.Region)
	}
}

// newCapture is a debug logger whose output the test reads.
func newCapture() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	h := slog.NewTextHandler(&lockedWriter{buf: buf}, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf
}

type lockedWriter struct {
	mu  sync.Mutex
	buf *bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// TestBedrockAPIKey: with AWS_BEARER_TOKEN_BEDROCK set and no AWS config the
// SDK sends the key as a bearer token (over TLS only), and the key reaches
// no error text and no log line when the request fails (design 10.2).
func TestBedrockAPIKey(t *testing.T) {
	const key = "ABSKbedrock-api-key-not-real-7c1e"
	t.Setenv(BearerTokenEnv, key)
	f := newFake(t, true,
		answer(t, llmtest.Answer{Text: "ok", InputTokens: 40, OutputTokens: 2}),
		codeError(http.StatusForbidden, "AccessDeniedException", "The security token included in the request is invalid: "+key),
	)
	log, buf := newCapture()
	m := newModel(t, Config{
		AWS:          &aws.Config{Region: "us-east-1", HTTPClient: f.Client()},
		BaseEndpoint: f.URL,
		Log:          log,
		attempts:     1,
	})
	if _, err := m.Turn(context.Background(), llm.Request{Model: testModel, Messages: []llm.Message{userText("hi")}}); err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if got := f.requests()[0].header.Get("Authorization"); got != "Bearer "+key {
		t.Fatalf("Authorization = %q, want the bearer key", got)
	}
	if s := fmt.Sprintf("%v %+v %#v", m, *m, m); strings.Contains(s, key) {
		t.Fatalf("fmt prints the key: %s", s)
	}
	_, err := m.Turn(context.Background(), llm.Request{Model: testModel, Messages: []llm.Message{userText("hi")}})
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrAuth {
		t.Fatalf("err = %v, want auth", err)
	}
	if strings.Contains(err.Error(), key) || strings.Contains(ae.Error(), key) || strings.Contains(ae.Body, key) {
		t.Fatalf("the key reached the error: %q / %q", err.Error(), ae.Body)
	}
	if strings.Contains(buf.String(), key) {
		t.Fatalf("the key reached the log: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "kind=auth") {
		t.Fatalf("the failure was not logged at debug: %s", buf.String())
	}
}

// TestBedrockAPIKeyNeedsTLS: the SDK refuses to send the key over plain
// HTTP; the request never reaches the server.
func TestBedrockAPIKeyNeedsTLS(t *testing.T) {
	t.Setenv(BearerTokenEnv, "ABSKbedrock-api-key-not-real")
	f := newFake(t, false, answer(t, llmtest.Answer{Text: "ok"}))
	cfg := Config{AWS: &aws.Config{Region: "us-east-1"}, BaseEndpoint: f.URL, attempts: 1, loopbackHTTP: true}
	if _, err := newModel(t, cfg).Turn(context.Background(), llm.Request{Model: testModel, Messages: []llm.Message{userText("hi")}}); err == nil {
		t.Fatal("a bearer key went over plain HTTP")
	}
	if n := len(f.requests()); n != 0 {
		t.Fatalf("requests = %d, want 0", n)
	}
}

// TestResponseIsCapped: a body past 4 MiB fails the turn instead of being
// decoded. End to end only the failure is asserted: under load the SDK can
// report the connection it closed after the read error instead of the error
// itself; the reader's own behaviour is pinned by TestCappedBody.
func TestResponseIsCapped(t *testing.T) {
	big := `{"output":{"message":{"role":"assistant","content":[{"text":"` + strings.Repeat("a", maxResponseBytes) + `"}]}},"stopReason":"end_turn"}`
	f := newFake(t, false, jsonReply(http.StatusOK, big))
	cfg := testConfig(f.URL)
	cfg.attempts = 1
	_, err := newModel(t, cfg).Turn(context.Background(), llm.Request{Model: testModel, Messages: []llm.Message{userText("hi")}})
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrNetwork {
		t.Fatalf("err = %v, want a failed turn", err)
	}
}

func TestCappedBody(t *testing.T) {
	read := func(n int) error {
		b := &cappedBody{rc: io.NopCloser(strings.NewReader(strings.Repeat("a", n))), left: maxResponseBytes}
		_, err := io.Copy(io.Discard, b)
		return err
	}
	if err := read(maxResponseBytes); err != nil {
		t.Fatalf("a body of exactly the cap: %v", err)
	}
	if err := read(maxResponseBytes + 1); !errors.Is(err, errResponseTooLarge) {
		t.Fatalf("a body past the cap: %v", err)
	}
}

func TestKindTable(t *testing.T) {
	for _, tc := range []struct {
		status    int
		code, msg string
		want      llm.ErrorKind
	}{
		{400, "ServiceQuotaExceededException", "Your account quota is exceeded", llm.ErrBilling},
		{429, "ThrottlingException", "Too many tokens per day, please wait before trying again.", llm.ErrBilling},
		{429, "ThrottlingException", "Too many requests, please wait before trying again.", llm.ErrRateLimited},
		{429, "ModelNotReadyException", "Model is not ready", llm.ErrRateLimited},
		{408, "ModelTimeoutException", "timed out", llm.ErrOverloaded},
		{424, "ModelErrorException", "model error", llm.ErrOverloaded},
		{500, "InternalServerException", "", llm.ErrOverloaded},
		{503, "ServiceUnavailableException", "", llm.ErrOverloaded},
		{404, "ResourceNotFoundException", "model not found", llm.ErrModelNotFound},
		{403, "AccessDeniedException", "You don't have access to the model with the specified model ID.", llm.ErrModelNotFound},
		{403, "AccessDeniedException", "User is not authorized to perform bedrock:InvokeModel", llm.ErrAuth},
		{403, "UnrecognizedClientException", "The security token included in the request is invalid.", llm.ErrAuth},
		{400, "ExpiredTokenException", "expired", llm.ErrAuth},
		{401, "", "", llm.ErrAuth},
		{400, "ValidationException", "The provided model identifier is invalid.", llm.ErrModelNotFound},
		{400, "ValidationException", "Input is too long for requested model.", llm.ErrTooLong},
		{400, "ValidationException", "The model returned the following errors: too many input tokens", llm.ErrTooLong},
		{413, "", "", llm.ErrTooLong},
		{400, "ValidationException", "This model doesn't support tool use.", llm.ErrBadRequest},
		{400, "ValidationException", "messages: roles must alternate", llm.ErrBadRequest},
		{409, "ConflictException", "", llm.ErrBadRequest},
	} {
		if got := Kind(tc.status, tc.code, tc.msg); got != tc.want {
			t.Errorf("Kind(%d, %s, %q) = %s, want %s", tc.status, tc.code, tc.msg, got, tc.want)
		}
	}
}

// TestErrorTextNeverCarriesTheMessage: the error's text names the provider,
// kind, status and AWS code only; the message (which can echo the prompt)
// stays in the redacted Body.
func TestErrorTextNeverCarriesTheMessage(t *testing.T) {
	f := newFake(t, false, codeError(http.StatusBadRequest, "ValidationException", "messages.1: the alert text 'orders-cdc down' is invalid"))
	cfg := testConfig(f.URL)
	_, err := newModel(t, cfg).Turn(context.Background(), llm.Request{Model: testModel, Messages: []llm.Message{userText("hi")}})
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrBadRequest || ae.Status != 400 {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(err.Error(), "orders-cdc") || !strings.Contains(err.Error(), "(ValidationException)") {
		t.Fatalf("text = %q", err.Error())
	}
	if !strings.Contains(ae.Body, "orders-cdc") {
		t.Fatalf("Body = %q, want the message for the debug log", ae.Body)
	}
	if n := len(f.requests()); n != 1 {
		t.Fatalf("a 400 was retried: %d requests", n)
	}
}

func typesStop(s string) types.StopReason { return types.StopReason(s) }
