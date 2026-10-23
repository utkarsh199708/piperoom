package anthropic_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/vertex"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"piperoom/internal/agent/llm"
	anthropicllm "piperoom/internal/agent/llm/anthropic"
	"piperoom/internal/agent/llm/llmtest"
)

// TestConformance runs the adapter conformance suite on the Claude adapter,
// with the production client (streaming) pointed at the suite's server: on
// the Anthropic API, and on Vertex AI (its own URL and OAuth, the same
// stream). Bedrock answers in the AWS event-stream encoding, which the suite's
// server does not speak; internal/agent's endpoint tests cover its URL,
// signing and body.
func TestConformance(t *testing.T) {
	t.Run(llm.ProviderAnthropic, func(t *testing.T) {
		llmtest.Run(t, claudeFixture(llm.ProviderAnthropic, func(baseURL string) []option.RequestOption {
			return []option.RequestOption{option.WithAPIKey("sk-ant-conformance-not-real"), option.WithBaseURL(baseURL), option.WithMaxRetries(0)}
		}))
	})
	t.Run(llm.ProviderVertex, func(t *testing.T) {
		llmtest.Run(t, claudeFixture(llm.ProviderVertex, func(baseURL string) []option.RequestOption {
			creds := &google.Credentials{TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "conformance-token"})}
			return []option.RequestOption{
				vertex.WithCredentials(context.Background(), "europe-west1", "acme", creds),
				option.WithBaseURL(baseURL), option.WithMaxRetries(0),
			}
		}))
	})
}

func claudeFixture(provider string, opts func(baseURL string) []option.RequestOption) llmtest.Fixture {
	return llmtest.Fixture{
		Provider: provider,
		Model:    "claude-opus-5-5",
		New: func(t *testing.T, baseURL string) llm.Model {
			return anthropicllm.New(provider, opts(baseURL), nil)
		},
		Render:      renderSSE,
		RenderError: renderError,
		ToolResults: toolResults,
	}
}

// renderSSE streams an answer the way the Messages API does.
func renderSSE(t *testing.T, a llmtest.Answer) (string, []byte) {
	t.Helper()
	var b strings.Builder
	ev := func(typ string, v map[string]any) {
		v["type"] = typ
		j, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "event: %s\ndata: %s\n\n", typ, j)
	}
	ev("message_start", map[string]any{"message": map[string]any{
		"id": "msg_conf", "type": "message", "role": "assistant", "model": "claude-opus-5-5", "content": []any{},
		"stop_reason": nil, "stop_sequence": nil, "usage": map[string]any{"input_tokens": a.InputTokens, "output_tokens": 1},
	}})
	i := 0
	if a.Text != "" && !a.Refusal {
		ev("content_block_start", map[string]any{"index": i, "content_block": map[string]any{"type": "text", "text": ""}})
		ev("content_block_delta", map[string]any{"index": i, "delta": map[string]any{"type": "text_delta", "text": a.Text}})
		ev("content_block_stop", map[string]any{"index": i})
		i++
	}
	for _, c := range a.Calls {
		ev("content_block_start", map[string]any{"index": i, "content_block": map[string]any{"type": "tool_use", "id": c.ID, "name": c.Name, "input": map[string]any{}}})
		ev("content_block_delta", map[string]any{"index": i, "delta": map[string]any{"type": "input_json_delta", "partial_json": c.Input}})
		ev("content_block_stop", map[string]any{"index": i})
		i++
	}
	stop := map[string]any{"stop_reason": "end_turn", "stop_sequence": nil}
	switch {
	case a.Refusal:
		stop = map[string]any{"stop_reason": "refusal", "stop_sequence": nil,
			"stop_details": map[string]any{"type": "refusal", "category": "cyber", "explanation": a.Text}}
	case len(a.Calls) > 0:
		stop["stop_reason"] = "tool_use"
	}
	ev("message_delta", map[string]any{"delta": stop, "usage": map[string]any{"output_tokens": a.OutputTokens}})
	ev("message_stop", map[string]any{})
	return "text/event-stream", []byte(b.String())
}

func renderError(status int, message string) (string, []byte) {
	typ := map[int]string{401: "authentication_error", 404: "not_found_error", 429: "rate_limit_error", 500: "api_error", 503: "overloaded_error"}[status]
	if typ == "" {
		typ = "invalid_request_error"
	}
	body, _ := json.Marshal(map[string]any{"type": "error", "error": map[string]any{"type": typ, "message": message}})
	return "application/json", body
}

// toolResults reads the text of every tool_result block of a request.
func toolResults(t *testing.T, body []byte) []string {
	t.Helper()
	var req struct {
		Messages []struct {
			Content []struct {
				Type    string `json:"type"`
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("request body: %v", err)
	}
	var out []string
	for _, m := range req.Messages {
		for _, c := range m.Content {
			if c.Type != "tool_result" {
				continue
			}
			var s strings.Builder
			for _, part := range c.Content {
				s.WriteString(part.Text)
			}
			out = append(out, s.String())
		}
	}
	return out
}
