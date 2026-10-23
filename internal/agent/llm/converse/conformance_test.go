package converse

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/llmtest"
)

// testModel is a Nova model: it takes cachePoint blocks and toolResult
// status, so the fixtures exercise both.
const testModel = "amazon.nova-pro-v1:0"

// staticCreds signs test requests with SigV4 and fixed, fake keys.
var staticCreds = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
	return aws.Credentials{AccessKeyID: "AKIDCONFORMANCE", SecretAccessKey: "conformance-secret-not-real", Source: "test"}, nil
})

// testConfig points the production client at baseURL: the SDK's own
// endpoint override (Options.BaseEndpoint), SigV4 with fake keys, and no
// Bedrock API key from the machine.
func testConfig(baseURL string) Config {
	return Config{
		AWS:          &aws.Config{Region: "us-east-1", Credentials: staticCreds},
		BaseEndpoint: baseURL,
		Getenv:       func(string) string { return "" },
	}
}

// TestConformance runs the adapter conformance suite on the Converse adapter
// with the production SDK client pointed at the suite's server through the
// SDK's endpoint override, retries off (the suite counts requests).
func TestConformance(t *testing.T) {
	llmtest.Run(t, llmtest.Fixture{
		Provider: llm.ProviderBedrockConverse,
		Model:    testModel,
		New: func(t *testing.T, baseURL string) llm.Model {
			cfg := testConfig(baseURL)
			cfg.attempts = 1
			m, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			return m
		},
		Render:      renderAnswer,
		RenderError: renderError,
		ToolResults: toolResults,
	})
}

// renderAnswer writes an answer the way Converse does.
func renderAnswer(t *testing.T, a llmtest.Answer) (string, []byte) {
	t.Helper()
	var content []any
	if a.Text != "" {
		content = append(content, map[string]any{"text": a.Text})
	}
	for _, c := range a.Calls {
		content = append(content, map[string]any{"toolUse": map[string]any{
			"toolUseId": c.ID, "name": c.Name, "input": json.RawMessage(c.Input),
		}})
	}
	stop := "end_turn"
	switch {
	case a.Refusal:
		stop = "content_filtered"
	case len(a.Calls) > 0:
		stop = "tool_use"
	}
	body, err := json.Marshal(map[string]any{
		"output":     map[string]any{"message": map[string]any{"role": "assistant", "content": content}},
		"stopReason": stop,
		"usage": map[string]any{
			"inputTokens": a.InputTokens, "outputTokens": a.OutputTokens, "totalTokens": a.InputTokens + a.OutputTokens,
		},
		"metrics": map[string]any{"latencyMs": 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	return "application/json", body
}

// errorCodeFor is the AWS error code Bedrock Runtime answers with status.
func errorCodeFor(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return "UnrecognizedClientException"
	case http.StatusForbidden:
		return "AccessDeniedException"
	case http.StatusNotFound:
		return "ResourceNotFoundException"
	case http.StatusTooManyRequests:
		return "ThrottlingException"
	case http.StatusServiceUnavailable:
		return "ServiceUnavailableException"
	case http.StatusInternalServerError:
		return "InternalServerException"
	}
	return "ValidationException"
}

func renderError(status int, message string) (string, []byte) {
	return renderCodeError(errorCodeFor(status), message)
}

func renderCodeError(code, message string) (string, []byte) {
	body, _ := json.Marshal(map[string]any{"__type": code, "message": message})
	return "application/json", body
}

// converseBody is the part of a Converse request body the tests read.
type converseBody struct {
	Messages []struct {
		Role    string            `json:"role"`
		Content []json.RawMessage `json:"content"`
	} `json:"messages"`
	System     []json.RawMessage `json:"system"`
	ToolConfig *struct {
		Tools      []json.RawMessage `json:"tools"`
		ToolChoice map[string]any    `json:"toolChoice"`
	} `json:"toolConfig"`
	InferenceConfig map[string]any `json:"inferenceConfig"`
}

func parseBody(t *testing.T, body []byte) converseBody {
	t.Helper()
	var b converseBody
	if err := json.Unmarshal(body, &b); err != nil {
		t.Fatalf("request body: %v\n%s", err, body)
	}
	return b
}

type toolResultBlock struct {
	ToolResult *struct {
		ToolUseID string `json:"toolUseId"`
		Status    string `json:"status"`
		Content   []struct {
			Text string `json:"text"`
		} `json:"content"`
	} `json:"toolResult"`
}

// toolResults reads the text of every toolResult block of a request.
func toolResults(t *testing.T, body []byte) []string {
	t.Helper()
	var out []string
	for _, m := range parseBody(t, body).Messages {
		for _, raw := range m.Content {
			var b toolResultBlock
			if err := json.Unmarshal(raw, &b); err != nil {
				t.Fatal(err)
			}
			if b.ToolResult == nil {
				continue
			}
			s := ""
			for _, c := range b.ToolResult.Content {
				s += c.Text
			}
			out = append(out, s)
		}
	}
	return out
}
