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

// TestConformance runs the adapter conformance suite on every flavor V3a
// serves, each against recorded chat completions answers in its host's own
// shape (an Azure filter verdict, a vLLM top-level error, Ollama's error).
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
	} {
		t.Run(tc.flavor, func(t *testing.T) {
			llmtest.Run(t, llmtest.Fixture{
				Provider: llm.ProviderOpenAI,
				Model:    tc.model,
				New: func(t *testing.T, baseURL string) llm.Model {
					return newFlavor(t, tc.flavor, baseURL, tc.key, &retry.Policy{})
				},
				Render:      render,
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

// render writes an answer as a chat completions response.
func render(t *testing.T, a llmtest.Answer) (string, []byte) {
	t.Helper()
	msg := map[string]any{"role": "assistant", "content": nil}
	finish := "stop"
	switch {
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
	body, err := json.Marshal(map[string]any{
		"id": "chatcmpl-conf", "object": "chat.completion", "model": "conformance-model",
		"choices": []any{map[string]any{"index": 0, "message": msg, "finish_reason": finish}},
		"usage": map[string]any{"prompt_tokens": a.InputTokens, "completion_tokens": a.OutputTokens,
			"total_tokens": a.InputTokens + a.OutputTokens},
	})
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
