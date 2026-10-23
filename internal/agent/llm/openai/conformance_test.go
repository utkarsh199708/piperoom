package openai_test

import (
	"encoding/json"
	"testing"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/llmtest"
	"piperoom/internal/agent/llm/openai"
	"piperoom/internal/agent/llm/retry"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
)

// testKey is a fake key; tests grep for it to prove it leaks nowhere.
const testKey = "sk-proj-conformance-NOT-REAL-4f9c2a7e"

// TestConformance runs the adapter conformance suite on every flavor, each
// against chat completions answers in its host's own shape as the host
// documents it (an Azure filter verdict, a vLLM top-level error, Ollama's
// error; from V3b DeepSeek's reasoning_content and cache-hit usage,
// DashScope's cached tokens, Groq's x_groq block, OpenRouter's routed
// provider and cost, xAI's string error). The shapes come from the hosts'
// API references (2026-10), not from live captures.
func TestConformance(t *testing.T) {
	for _, tc := range []struct {
		flavor string
		key    netsec.Secret
		model  string
	}{
		{openai.FlavorOpenAI, testKey, "gpt-4.1"},
		{openai.FlavorAzure, testKey, "prod-gpt-4o"},
		{openai.FlavorOllama, "", "qwen3:32b"},
		{openai.FlavorVLLM, "", "Qwen/Qwen3-32B"},
		{openai.FlavorGeneric, testKey, "llama-4-maverick"},
		{openai.FlavorDeepSeek, testKey, "deepseek-reasoner"},
		{openai.FlavorQwen, testKey, "qwen-plus"},
		{openai.FlavorMistral, testKey, "mistral-large-latest"},
		{openai.FlavorGroq, testKey, "openai/gpt-oss-120b"},
		{openai.FlavorTogether, testKey, "meta-llama/Llama-3.3-70B-Instruct-Turbo"},
		{openai.FlavorOpenRouter, testKey, "deepseek/deepseek-chat-v3.1"},
		{openai.FlavorXAI, testKey, "grok-4"},
	} {
		t.Run(tc.flavor, func(t *testing.T) {
			llmtest.Run(t, llmtest.Fixture{
				Provider: llm.ProviderOpenAI,
				Model:    tc.model,
				New: func(t *testing.T, baseURL string) llm.Model {
					return newFlavor(t, tc.flavor, baseURL, tc.key, &retry.Policy{})
				},
				Render:      renderFor(tc.flavor),
				RenderError: renderErrorFor(tc.flavor),
				ToolResults: toolResults,
			})
		})
	}
}

// newFlavor builds the adapter for flavor at baseURL (an httptest URL).
func newFlavor(t *testing.T, flavor, baseURL string, key netsec.Secret, p *retry.Policy) *openai.Model {
	t.Helper()
	b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", baseURL+"/v1", false)
	if err != nil {
		t.Fatalf("ParseBaseURL: %v", err)
	}
	m, err := openai.New(openai.Options{Flavor: flavor, BaseURL: &b, Key: key, Retry: p})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

// renderFor writes an answer as flavor's chat completions response.
func renderFor(flavor string) func(t *testing.T, a llmtest.Answer) (string, []byte) {
	return func(t *testing.T, a llmtest.Answer) (string, []byte) {
		t.Helper()
		return render(t, flavor, a)
	}
}

// render writes an answer as a chat completions response in flavor's
// shape. DeepSeek and DashScope have no refusal field: they stop with
// content_filter and explain in the content.
func render(t *testing.T, flavor string, a llmtest.Answer) (string, []byte) {
	t.Helper()
	msg := map[string]any{"role": "assistant", "content": nil}
	finish := "stop"
	switch {
	case a.Refusal && (flavor == openai.FlavorDeepSeek || flavor == openai.FlavorQwen):
		msg["content"] = a.Text
		finish = "content_filter"
	case a.Refusal:
		msg["refusal"] = a.Text
	case a.Text != "":
		msg["content"] = a.Text
	}
	if len(a.Calls) > 0 {
		var calls []any
		for _, c := range a.Calls {
			calls = append(calls, map[string]any{"id": c.ID, "type": "function",
				"function": map[string]any{"name": c.Name, "arguments": c.Input}})
		}
		msg["tool_calls"] = calls
		finish = "tool_calls"
	}
	usage := map[string]any{"prompt_tokens": a.InputTokens, "completion_tokens": a.OutputTokens,
		"total_tokens": a.InputTokens + a.OutputTokens}
	resp := map[string]any{
		"id": "chatcmpl-conf", "object": "chat.completion", "model": "conformance-model",
		"choices": []any{map[string]any{"index": 0, "message": msg, "finish_reason": finish}},
		"usage":   usage,
	}
	switch flavor {
	case openai.FlavorDeepSeek:
		if !a.Refusal {
			msg["reasoning_content"] = "The connector state decides this; read it first."
		}
		usage["prompt_cache_hit_tokens"] = a.InputTokens / 2
		usage["prompt_cache_miss_tokens"] = a.InputTokens - a.InputTokens/2
		usage["completion_tokens_details"] = map[string]any{"reasoning_tokens": a.OutputTokens / 2}
		resp["system_fingerprint"] = "fp_conformance_prod0820_fp8_kvcache"
	case openai.FlavorQwen:
		usage["prompt_tokens_details"] = map[string]any{"cached_tokens": a.InputTokens / 4}
		resp["system_fingerprint"] = nil
	case openai.FlavorGroq:
		msg["reasoning"] = "Checking the connectors."
		usage["prompt_tokens_details"] = map[string]any{"cached_tokens": 0}
		usage["queue_time"], usage["prompt_time"], usage["completion_time"] = 0.02, 0.01, 0.05
		resp["x_groq"] = map[string]any{"id": "req_conformance"}
	case openai.FlavorOpenRouter:
		usage["prompt_tokens_details"] = map[string]any{"cached_tokens": 0}
		usage["cost"] = 0.00012
		resp["provider"] = "DeepInfra"
	case openai.FlavorXAI:
		usage["prompt_tokens_details"] = map[string]any{"cached_tokens": a.InputTokens / 3, "text_tokens": a.InputTokens}
		usage["completion_tokens_details"] = map[string]any{"reasoning_tokens": a.OutputTokens / 2}
	}
	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	return "application/json", body
}

// renderErrorFor writes an error answer in the host's own shape.
func renderErrorFor(flavor string) func(status int, message string) (string, []byte) {
	return func(status int, message string) (string, []byte) {
		var v any
		switch flavor {
		case openai.FlavorVLLM:
			v = map[string]any{"object": "error", "message": message, "type": "BadRequestError", "param": nil, "code": status}
		case openai.FlavorMistral:
			v = map[string]any{"object": "error", "message": message, "type": "invalid_request_error", "param": nil, "code": nil}
		case openai.FlavorOpenRouter:
			v = map[string]any{"error": map[string]any{"code": status, "message": message}, "user_id": "user_conformance"}
		case openai.FlavorXAI:
			v = map[string]any{"code": "Client specified an invalid argument", "error": message}
		case openai.FlavorDeepSeek:
			typ := map[int]string{401: "authentication_error", 429: "rate_limit_error"}[status]
			v = map[string]any{"error": map[string]any{"message": message, "type": typ, "param": nil, "code": "invalid_request_error"}}
		case openai.FlavorQwen:
			code := map[int]string{401: "invalid_api_key", 404: "model_not_found", 429: "limit_requests", 500: "internal_error", 503: "ServiceUnavailable"}[status]
			v = map[string]any{"error": map[string]any{"message": message, "type": "invalid_request_error", "param": nil, "code": code},
				"request_id": "conformance-1"}
		case openai.FlavorOllama:
			v = map[string]any{"error": map[string]any{"message": message, "type": "api_error", "param": nil, "code": nil}}
		default:
			code := map[int]string{401: "invalid_api_key", 404: "model_not_found", 429: "rate_limit_exceeded"}[status]
			v = map[string]any{"error": map[string]any{"message": message, "type": "invalid_request_error", "param": nil, "code": code}}
		}
		b, _ := json.Marshal(v)
		return "application/json", b
	}
}

// toolResults reads the content of every tool message of a request.
func toolResults(t *testing.T, body []byte) []string {
	t.Helper()
	var req struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		// A user message with content parts does not fit Content string;
		// the suite's requests have none, so this is a real failure.
		t.Fatalf("request body: %v", err)
	}
	var out []string
	for _, m := range req.Messages {
		if m.Role == "tool" {
			out = append(out, m.Content)
		}
	}
	return out
}
