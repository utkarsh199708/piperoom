package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/converse"
)

// TestStructuredOutputOnConverse: Bedrock Converse has no structured output,
// so the V2a fallback runs on the real adapter: one forced emit_<what> tool
// (toolChoice any on the wire), the object read from the call's input and
// checked against the schema, and a bad first answer retried with the call
// answered by an error toolResult. No structured-output logic is duplicated
// in the adapter.
func TestStructuredOutputOnConverse(t *testing.T) {
	answers := []string{
		`{"output":{"message":{"role":"assistant","content":[{"toolUse":{"toolUseId":"tooluse_a","name":"emit_judge_verdict","input":{"why":"missing verdict"}}}]}},"stopReason":"tool_use","usage":{"inputTokens":120,"outputTokens":9,"totalTokens":129},"metrics":{"latencyMs":1}}`,
		`{"output":{"message":{"role":"assistant","content":[{"toolUse":{"toolUseId":"tooluse_b","name":"emit_judge_verdict","input":{"verdict":"correct"}}}]}},"stopReason":"tool_use","usage":{"inputTokens":200,"outputTokens":9,"totalTokens":209},"metrics":{"latencyMs":1}}`,
	}
	var mu sync.Mutex
	var bodies [][]byte
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		n := len(bodies)
		bodies = append(bodies, b)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, answers[min(n, len(answers)-1)])
	}))
	defer srv.Close()

	m, err := converse.New(converse.Config{
		AWS: &aws.Config{Region: "us-east-1", HTTPClient: srv.Client(), Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDTEST", SecretAccessKey: "not-real"}, nil
		})},
		BaseEndpoint: srv.URL,
		Getenv:       func(string) string { return "" },
	})
	if err != nil {
		t.Fatal(err)
	}
	const model = "amazon.nova-pro-v1:0"
	schema := map[string]any{
		"type":       "object",
		"properties": map[string]any{"verdict": map[string]any{"type": "string"}, "why": map[string]any{"type": "string"}},
		"required":   []string{"verdict"},
	}
	req := llm.Request{
		Model: model, System: "Grade the hand-off.", MaxTokens: 1024, OutputSchema: schema,
		Messages: []llm.Message{userMessage(textPart("grade it"))},
	}
	_, text, err := structuredTurn(context.Background(), m.Turn, m.Capabilities(model), req, "judge_verdict")
	if err != nil {
		t.Fatalf("structuredTurn: %v", err)
	}
	if text != `{"verdict":"correct"}` {
		t.Fatalf("text = %q", text)
	}
	if len(bodies) != 2 {
		t.Fatalf("requests = %d, want 2 (one retry)", len(bodies))
	}
	var first struct {
		ToolConfig struct {
			Tools []struct {
				ToolSpec struct {
					Name string `json:"name"`
				} `json:"toolSpec"`
			} `json:"tools"`
			ToolChoice map[string]any `json:"toolChoice"`
		} `json:"toolConfig"`
	}
	if err := json.Unmarshal(bodies[0], &first); err != nil {
		t.Fatal(err)
	}
	if len(first.ToolConfig.Tools) != 1 || first.ToolConfig.Tools[0].ToolSpec.Name != "emit_judge_verdict" || first.ToolConfig.ToolChoice["any"] == nil {
		t.Fatalf("first request toolConfig = %+v", first.ToolConfig)
	}
	if bytes.Contains(bodies[0], []byte("emit_output")) {
		t.Fatal("the adapter's own forced tool ran: the agent's fallback must strip OutputSchema first")
	}
	for _, want := range []string{`"toolUseId":"tooluse_a"`, `"status":"error"`} {
		if !bytes.Contains(bodies[1], []byte(want)) {
			t.Fatalf("retry request lacks %s: %s", want, bodies[1])
		}
	}
}
