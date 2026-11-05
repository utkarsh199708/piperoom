package gemini

import (
	"context"
	"encoding/json"
	"testing"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/llmtest"
	"piperoom/internal/agent/llm/retry"
	"piperoom/internal/config"
)

const testKey = "AIzaTestKeyNotReal-0123456789abcdefghij"

// newTestModel builds the adapter on baseURL (an httptest server, plain
// http on loopback) with retries off unless the test sets its own policy.
func newTestModel(t *testing.T, baseURL string, mutate func(*Options)) *Model {
	t.Helper()
	b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", baseURL, false)
	if err != nil {
		t.Fatalf("base URL: %v", err)
	}
	o := Options{APIKey: testKey, BaseURL: &b}
	if mutate != nil {
		mutate(&o)
	}
	m, err := New(context.Background(), o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	m.policy = retry.Policy{}
	return m
}

func TestConformance(t *testing.T) {
	llmtest.Run(t, llmtest.Fixture{
		Provider: llm.ProviderGemini,
		Model:    "gemini-2.5-pro",
		New: func(t *testing.T, baseURL string) llm.Model {
			return newTestModel(t, baseURL, nil)
		},
		Render:      renderAnswer,
		RenderError: renderError,
		ToolResults: toolResults,
	})
}

// renderAnswer writes a as generateContent does.
func renderAnswer(t *testing.T, a llmtest.Answer) (string, []byte) {
	t.Helper()
	var parts []any
	cand := map[string]any{"finishReason": "STOP"}
	if a.Refusal {
		cand["finishReason"] = "SAFETY"
		cand["finishMessage"] = a.Text
		cand["safetyRatings"] = []any{map[string]any{"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "probability": "HIGH", "blocked": true}}
	} else if a.Text != "" {
		parts = append(parts, map[string]any{"text": a.Text})
	}
	for _, c := range a.Calls {
		fc := map[string]any{"name": c.Name, "args": json.RawMessage(c.Input)}
		if c.ID != "" {
			fc["id"] = c.ID
		}
		parts = append(parts, map[string]any{"functionCall": fc})
	}
	if parts != nil {
		cand["content"] = map[string]any{"role": "model", "parts": parts}
	}
	body := map[string]any{
		"candidates":    []any{cand},
		"usageMetadata": map[string]any{"promptTokenCount": a.InputTokens, "candidatesTokenCount": a.OutputTokens, "totalTokenCount": a.InputTokens + a.OutputTokens},
		"modelVersion":  "gemini-2.5-pro",
	}
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return "application/json", b
}

func renderError(status int, message string) (string, []byte) {
	st := map[int]string{400: "INVALID_ARGUMENT", 401: "UNAUTHENTICATED", 403: "PERMISSION_DENIED", 404: "NOT_FOUND",
		429: "RESOURCE_EXHAUSTED", 500: "INTERNAL", 503: "UNAVAILABLE"}[status]
	b, _ := json.Marshal(map[string]any{"error": map[string]any{"code": status, "message": message, "status": st}})
	return "application/json", b
}

// toolResults reads every functionResponse's content, JSON-decoded.
func toolResults(t *testing.T, body []byte) []string {
	t.Helper()
	var req struct {
		Contents []struct {
			Parts []struct {
				FunctionResponse *struct {
					Response struct {
						Content string `json:"content"`
					} `json:"response"`
				} `json:"functionResponse"`
			} `json:"parts"`
		} `json:"contents"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("request body: %v", err)
	}
	var out []string
	for _, c := range req.Contents {
		for _, p := range c.Parts {
			if p.FunctionResponse != nil {
				out = append(out, p.FunctionResponse.Response.Content)
			}
		}
	}
	return out
}
