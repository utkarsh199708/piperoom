package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	sdk "github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/agent/llm"
)

// recordedTurn is an assistant turn as the API sends it: thinking with its
// signature, a server-side compaction block, a redacted thinking block, text
// and a tool call.
const recordedTurn = `{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-5",
"content":[
 {"type":"thinking","thinking":"The connector is the first thing to read.","signature":"sig-1"},
 {"type":"compaction","content":"Summary: task 0 FAILED on a 409.","encrypted_content":"enc-1"},
 {"type":"redacted_thinking","data":"opaque-1"},
 {"type":"text","text":"Reading the consumer."},
 {"type":"tool_use","id":"toolu_1","name":"kafka_consumer_lag","input":{"group":"order-sync","why":"lag"}}
],
"stop_reason":"tool_use","stop_sequence":null,
"usage":{"input_tokens":1200,"cache_creation_input_tokens":30,"cache_read_input_tokens":400,"output_tokens":80,
 "iterations":[
  {"type":"compaction","input_tokens":900,"output_tokens":50,"cache_creation_input_tokens":2,"cache_read_input_tokens":3},
  {"type":"message","input_tokens":1200,"output_tokens":80,"cache_creation_input_tokens":30,"cache_read_input_tokens":400}]}}`

func mustMessage(t *testing.T, s string) sdk.BetaMessage {
	t.Helper()
	var m sdk.BetaMessage
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func marshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestNativeRoundTrip: a decoded assistant turn goes back exactly as the
// agent sent it before the switch-over (BetaMessage.ToParam), thinking
// signatures and the compaction block included.
func TestNativeRoundTrip(t *testing.T) {
	msg := mustMessage(t, recordedTurn)
	resp := DecodeMessage(llm.ProviderBedrock, "claude-opus-5-5", msg)

	kinds := []llm.PartKind{llm.PartReasoning, llm.PartReasoning, llm.PartReasoning, llm.PartText, llm.PartToolCall}
	if len(resp.Message.Parts) != len(kinds) {
		t.Fatalf("parts = %d, want %d", len(resp.Message.Parts), len(kinds))
	}
	for i, p := range resp.Message.Parts {
		if p.Kind != kinds[i] {
			t.Fatalf("part %d kind = %s, want %s", i, p.Kind, kinds[i])
		}
		if p.Native == nil || p.Native.Provider != llm.ProviderBedrock || p.Native.Model != "claude-opus-5-5" || len(p.Native.Raw) == 0 {
			t.Fatalf("part %d native = %+v", i, p.Native)
		}
	}
	if c := resp.Message.Parts[4].ToolCall; c == nil || c.ID != "toolu_1" || c.Name != "kafka_consumer_lag" || string(c.Input) != `{"group":"order-sync","why":"lag"}` {
		t.Fatalf("tool call = %+v", c)
	}
	if resp.Message.Parts[3].Text != "Reading the consumer." {
		t.Fatalf("text = %q", resp.Message.Parts[3].Text)
	}

	p, err := EncodeRequest(llm.Request{Model: "claude-opus-5-5", Messages: []llm.Message{resp.Message}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := marshal(t, p.Messages[0]), marshal(t, msg.ToParam()); got != want {
		t.Fatalf("assistant turn on the wire:\n got %s\nwant %s", got, want)
	}
	for _, want := range []string{`"signature":"sig-1"`, `"encrypted_content":"enc-1"`, `"data":"opaque-1"`} {
		if !strings.Contains(marshal(t, p.Messages[0]), want) {
			t.Fatalf("the turn lost %s", want)
		}
	}
}

// TestNativeRoundTripOfStreamedTurn does the same for a turn accumulated
// from the stream, whose blocks are built from deltas.
func TestNativeRoundTripOfStreamedTurn(t *testing.T) {
	events := []string{
		`{"type":"message_start","message":{"id":"msg_2","type":"message","role":"assistant","model":"claude-haiku-5-5","content":[],"stop_reason":null,"usage":{"input_tokens":10,"output_tokens":1}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Read it."}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig-2"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"compaction","content":null}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"compaction_delta","content":"Summary.","encrypted_content":"enc-2"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_2","name":"connect_status","input":{}}}`,
		`{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"connector\":"}}`,
		`{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"\"pg\"}"}}`,
		`{"type":"content_block_stop","index":2}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":20}}`,
		`{"type":"message_stop"}`,
	}
	var msg sdk.BetaMessage
	for _, e := range events {
		var ev sdk.BetaRawMessageStreamEventUnion
		if err := json.Unmarshal([]byte(e), &ev); err != nil {
			t.Fatal(err)
		}
		if err := msg.Accumulate(ev); err != nil {
			t.Fatal(err)
		}
	}
	resp := DecodeMessage(llm.ProviderAnthropic, "claude-haiku-5-5", msg)
	p, err := EncodeRequest(llm.Request{Messages: []llm.Message{resp.Message}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := marshal(t, p.Messages[0]), marshal(t, msg.ToParam()); got != want {
		t.Fatalf("streamed turn on the wire:\n got %s\nwant %s", got, want)
	}
	if resp.Stop != llm.StopToolUse || resp.Message.Parts[2].ToolCall == nil || string(resp.Message.Parts[2].ToolCall.Input) != `{"connector":"pg"}` {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestEncodeDropsOtherProvidersReasoning(t *testing.T) {
	foreign := &llm.Native{Provider: llm.ProviderOpenAI, Model: "gpt", Raw: json.RawMessage(`{"reasoning_content":"x"}`)}
	p, err := EncodeRequest(llm.Request{Messages: []llm.Message{{Role: llm.RoleAssistant, Parts: []llm.Part{
		{Kind: llm.PartReasoning, Native: foreign},
		{Kind: llm.PartText, Text: "hello", Native: foreign},
		{Kind: llm.PartToolCall, ToolCall: &llm.ToolCall{ID: "c1", Name: "t", Input: json.RawMessage(`{"a":1}`)}, Native: foreign},
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	got := marshal(t, p.Messages)
	want := `[{"content":[{"text":"hello","type":"text"},{"id":"c1","input":{"a":1},"name":"t","type":"tool_use"}],"role":"assistant"}]`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestEncodeUserParts(t *testing.T) {
	p, err := EncodeRequest(llm.Request{Messages: []llm.Message{{Role: llm.RoleUser, Parts: []llm.Part{
		{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{CallID: "c1", Name: "connect_status", Content: "<tool_output>x</tool_output>", IsError: true}},
		{Kind: llm.PartText, Text: "steer"},
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	want := []sdk.BetaMessageParam{sdk.NewBetaUserMessage(
		sdk.NewBetaToolResultBlock("c1", "<tool_output>x</tool_output>", true),
		sdk.NewBetaTextBlock("steer"),
	)}
	if got, w := marshal(t, p.Messages), marshal(t, want); got != w {
		t.Fatalf("got  %s\nwant %s", got, w)
	}
	if _, err := EncodeRequest(llm.Request{Messages: []llm.Message{{Role: llm.RoleUser, Parts: []llm.Part{{Kind: "image"}}}}}); err == nil {
		t.Fatal("an unknown part kind must be an error")
	}
	if _, err := EncodeRequest(llm.Request{Tools: []llm.ToolDef{{Name: "t"}}, ToolChoice: "sometimes"}); err == nil {
		t.Fatal("an unknown tool choice must be an error")
	}
}

// TestEncodeOutputSchema: OutputSchema is output_config.format, a JSON
// schema, next to the effort; no schema, no format.
func TestEncodeOutputSchema(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{"why": map[string]any{"type": "string"}}, "required": []string{"why"}, "additionalProperties": false}
	p, err := EncodeRequest(llm.Request{Model: "m", MaxTokens: 10, Effort: "low", OutputSchema: schema})
	if err != nil {
		t.Fatal(err)
	}
	body := marshal(t, p)
	want := `"output_config":{"effort":"low","format":{"schema":{"additionalProperties":false,"properties":{"why":{"type":"string"}},"required":["why"],"type":"object"},"type":"json_schema"}}`
	if !strings.Contains(body, want) {
		t.Fatalf("body %s lacks %s", body, want)
	}
	p, err = EncodeRequest(llm.Request{Model: "m", MaxTokens: 10, Effort: "high"})
	if err != nil {
		t.Fatal(err)
	}
	if body := marshal(t, p); strings.Contains(body, `"format"`) || !strings.Contains(body, `"output_config":{"effort":"high"}`) {
		t.Fatalf("no schema must send no format: %s", body)
	}
	p, err = EncodeRequest(llm.Request{Model: "m", MaxTokens: 10})
	if err != nil {
		t.Fatal(err)
	}
	if body := marshal(t, p); strings.Contains(body, `"output_config"`) || strings.Contains(body, `"thinking"`) || strings.Contains(body, `"tool_choice"`) || strings.Contains(body, `"cache_control"`) || len(p.Betas) != 0 {
		t.Fatalf("an empty request must set nothing extra: %s", body)
	}
}

func TestEncodeLoopShape(t *testing.T) {
	schema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"connector": map[string]any{"type": "string"}},
		"required":             []string{"connector"},
		"additionalProperties": false,
	}
	p, err := EncodeRequest(llm.Request{
		Model: "m", MaxTokens: 16000, System: "sys", Effort: "high", Thinking: true,
		Tools:      []llm.ToolDef{{Name: "a", Description: "A", Schema: schema, Strict: true}, {Name: "b", Description: "B", Schema: schema}},
		Compaction: &llm.Compaction{Instructions: "keep step ids"},
		Cache:      llm.CacheHints{System: true, Tools: true, History: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := marshal(t, p)
	for _, want := range []string{
		`"cache_control":{"type":"ephemeral"},"context_management":{"edits":[{"instructions":"keep step ids","type":"compact_20260112"}]}`,
		`"system":[{"text":"sys","cache_control":{"type":"ephemeral"},"type":"text"}]`,
		`"thinking":{"type":"adaptive"}`,
		`"tool_choice":{"type":"auto"}`,
		`{"input_schema":{"properties":{"connector":{"type":"string"}},"required":["connector"],"type":"object","additionalProperties":false},"name":"a","eager_input_streaming":true,"description":"A","strict":true,"type":"custom"}`,
		`{"input_schema":{"properties":{"connector":{"type":"string"}},"required":["connector"],"type":"object","additionalProperties":false},"name":"b","eager_input_streaming":true,"description":"B","type":"custom","cache_control":{"type":"ephemeral"}}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body lacks %s:\n%s", want, body)
		}
	}
	if len(p.Betas) != 1 || p.Betas[0] != sdk.AnthropicBetaCompact2026_01_12 {
		t.Fatalf("betas = %v", p.Betas)
	}
	for choice, want := range map[llm.ToolChoice]string{llm.ToolChoiceNone: `"tool_choice":{"type":"none"}`, llm.ToolChoiceRequired: `"tool_choice":{"type":"any"}`} {
		p, err := EncodeRequest(llm.Request{Tools: []llm.ToolDef{{Name: "a", Schema: schema}}, ToolChoice: choice})
		if err != nil {
			t.Fatal(err)
		}
		if body := marshal(t, p); !strings.Contains(body, want) {
			t.Fatalf("%s: body lacks %s: %s", choice, want, body)
		}
	}
}

func TestDecodeStopsAndRefusal(t *testing.T) {
	for stop, want := range map[string]llm.StopReason{
		"end_turn": llm.StopEndTurn, "stop_sequence": llm.StopEndTurn, "tool_use": llm.StopToolUse,
		"max_tokens": llm.StopMaxTokens, "pause_turn": llm.StopPause, "refusal": llm.StopRefusal,
		"model_context_window_exceeded": llm.StopContextExceeded, "something_new": llm.StopOther, "": llm.StopOther,
	} {
		msg := mustMessage(t, `{"stop_reason":"`+stop+`","content":[]}`)
		if stop == "" {
			msg = mustMessage(t, `{"content":[]}`)
		}
		resp := DecodeMessage(llm.ProviderAnthropic, "m", msg)
		if resp.Stop != want || resp.ProviderStop != stop {
			t.Errorf("%q: stop = %s (%q), want %s", stop, resp.Stop, resp.ProviderStop, want)
		}
	}
	for details, want := range map[string]string{
		`{"category":"cyber","explanation":"not this"}`: "cyber: not this",
		`{"category":"cyber","explanation":null}`:       "cyber",
		`{"category":null,"explanation":"not this"}`:    "not this",
		`null`: "",
	} {
		resp := DecodeMessage(llm.ProviderAnthropic, "m", mustMessage(t, `{"stop_reason":"refusal","stop_details":`+details+`,"content":[]}`))
		if resp.StopDetail != want {
			t.Errorf("%s: detail = %q, want %q", details, resp.StopDetail, want)
		}
	}
}

func TestDecodeUsage(t *testing.T) {
	resp := DecodeMessage(llm.ProviderAnthropic, "m", mustMessage(t, recordedTurn))
	u := resp.Usage
	if u.Input != 1200 || u.CacheWrite != 30 || u.CacheRead != 400 || u.Output != 80 {
		t.Fatalf("usage = %+v", u)
	}
	if c := u.Compaction; c == nil || c.Input != 900 || c.Output != 50 || c.CacheWrite != 2 || c.CacheRead != 3 {
		t.Fatalf("compaction = %+v", u.Compaction)
	}
	if DecodeMessage(llm.ProviderAnthropic, "m", mustMessage(t, `{"content":[],"usage":{"input_tokens":1}}`)).Usage.Compaction != nil {
		t.Fatal("no compaction iteration, no Compaction usage")
	}
}

type turnerFunc func(context.Context, sdk.BetaMessageNewParams) (sdk.BetaMessage, error)

func (f turnerFunc) Turn(ctx context.Context, p sdk.BetaMessageNewParams) (sdk.BetaMessage, error) {
	return f(ctx, p)
}

// TestTurnKeepsThePartialAnswerOfAFailedStream: tokens a broken stream used
// were billed, so the usage comes back with the error.
func TestTurnKeepsThePartialAnswerOfAFailedStream(t *testing.T) {
	partial := mustMessage(t, `{"content":[],"usage":{"input_tokens":700,"output_tokens":3}}`)
	m := NewWithTurner(llm.ProviderAnthropic, turnerFunc(func(context.Context, sdk.BetaMessageNewParams) (sdk.BetaMessage, error) {
		return partial, errors.New("connection reset")
	}), nil)
	resp, err := m.Turn(context.Background(), llm.Request{Model: "m"})
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrNetwork {
		t.Fatalf("err = %v", err)
	}
	if resp.Usage.Input != 700 || resp.Usage.Output != 3 {
		t.Fatalf("usage = %+v", resp.Usage)
	}
}

func TestTurnRefusesAnUnencodableRequestBeforeSending(t *testing.T) {
	sent := false
	m := NewWithTurner(llm.ProviderAnthropic, turnerFunc(func(context.Context, sdk.BetaMessageNewParams) (sdk.BetaMessage, error) {
		sent = true
		return sdk.BetaMessage{}, nil
	}), nil)
	bad := &llm.Native{Provider: llm.ProviderAnthropic, Raw: json.RawMessage(`not json`)}
	_, err := m.Turn(context.Background(), llm.Request{Messages: []llm.Message{{Role: llm.RoleAssistant, Parts: []llm.Part{{Kind: llm.PartText, Native: bad}}}}})
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrBadRequest || sent {
		t.Fatalf("err = %v, sent = %v", err, sent)
	}
}

func TestCapabilities(t *testing.T) {
	c := NewWithTurner(llm.ProviderVertex, nil, nil).Capabilities("claude-haiku-5-5")
	if !c.StrictTools || c.MaxStrictTools != MaxStrictTools || c.MaxStrictBytes != MaxStrictSchemaBytes ||
		!c.StructuredOutput || !c.Compaction || !c.PromptCache || !c.Effort || !c.Reasoning || !c.ToolCalls || !c.ParallelToolCalls {
		t.Fatalf("capabilities = %+v", c)
	}
	if MaxStrictTools != 8 || MaxStrictSchemaBytes != 5400 {
		t.Fatal("the strict caps are the live-measured 8 tools and 5400 schema bytes")
	}
}
