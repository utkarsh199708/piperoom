package evals

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"piperoom/internal/agent/llm"
	"piperoom/internal/config"
)

// The tapped runner reaches the role models through wiring.WithModelWrap: a
// non-Claude endpoint built from config hands every role's client to the wrap.
func TestTappedEndpointRunnerWrapsRoleModels(t *testing.T) {
	srv, _ := serve(t, false, "application/json", mustJSON(t, map[string]any{
		"id": "chatcmpl-tap", "object": "chat.completion", "model": "gpt-5",
		// The start check's probe needs a tool call back.
		"choices": []any{map[string]any{"index": 0, "finish_reason": "tool_calls", "message": map[string]any{"role": "assistant",
			"tool_calls": []any{map[string]any{"id": "call_1", "type": "function", "function": map[string]any{"name": "piperoom_start_probe", "arguments": `{"status":"ready"}`}}}}}},
		"usage": map[string]any{"prompt_tokens": 20000, "completion_tokens": 2, "total_tokens": 20002},
	}))
	t.Setenv("OPENAI_API_KEY", "test-not-a-key")
	cfg := config.Config{
		Model:           "gpt-5",
		ModelProvider:   "openai",
		ModelFlavor:     "openai",
		ModelBaseURL:    srv.URL + "/v1",
		ModelUnverified: "on",
	}
	var mu sync.Mutex
	var roles []string
	wrap := func(role string, m llm.Model) llm.Model {
		mu.Lock()
		roles = append(roles, role)
		mu.Unlock()
		return m
	}
	run, tapped, err := NewTappedEndpointRunner(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), wrap)
	if err != nil {
		t.Fatal(err)
	}
	if !tapped || run == nil {
		t.Fatalf("tapped = %v, run nil = %v; want a tapped runner", tapped, run == nil)
	}
	if len(roles) == 0 || roles[0] != "main" {
		t.Fatalf("wrapped roles = %v; want main first", roles)
	}
	// A nil wrap builds the plain endpoint and reports untapped.
	if _, tapped, err := NewTappedEndpointRunner(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil); err != nil || tapped {
		t.Fatalf("nil wrap: tapped = %v, err = %v", tapped, err)
	}
}
