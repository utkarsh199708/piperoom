package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ProbeToolName is the fake read tool the fake model calls.
const ProbeToolName = "lt_probe"

// NoopActionName is the fake write action ModePropose rooms propose.
const NoopActionName = "lt_noop"

// probeTool is a read tool that waits Latency and returns a summary of
// SummaryBytes, standing in for connect_status, consumer_lag and the rest.
type probeTool struct {
	latency      time.Duration
	summaryBytes int
	runs         *atomic.Int64
}

func (t probeTool) Name() string { return ProbeToolName }
func (t probeTool) Description() string {
	return "Load-test probe: waits a fixed time and returns a fixed-size summary."
}
func (t probeTool) Kind() tools.Kind { return tools.KindRead }
func (t probeTool) InputSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"n": map[string]any{"type": "integer"}},
		"required":             []string{"n"},
		"additionalProperties": false,
	}
}

func (t probeTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var in struct {
		N *int `json:"n"`
	}
	if err := json.Unmarshal(input, &in); err != nil || in.N == nil {
		return tools.Result{}, fmt.Errorf("loadtest: probe: want {\"n\": int}: %w", tools.ErrBadInput)
	}
	if !sleepCtx(ctx, t.latency) {
		return tools.Result{}, fmt.Errorf("loadtest: probe: %w", ctx.Err())
	}
	t.runs.Add(1)
	summary, truncated := tools.Truncate(fmt.Sprintf("probe %d ok. ", *in.N) + fillerText(t.summaryBytes))
	return tools.Result{Summary: summary, Data: map[string]int{"n": *in.N, "bytes": t.summaryBytes}, Truncated: truncated}, nil
}

// noopAction is a scoped write action that changes nothing and counts its
// executions by the "key" param, so a run can prove an approved step ran
// exactly once across every server process.
type noopAction struct {
	mu   *sync.Mutex
	runs map[string]int
}

func (noopAction) Name() string     { return NoopActionName }
func (noopAction) Layer() string    { return "postgres" }
func (noopAction) Describe() string { return "load-test no-op; changes nothing" }
func (noopAction) ParamsSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"key": map[string]any{"type": "string"}},
		"required":             []string{"key"},
		"additionalProperties": false,
	}
}

func (noopAction) Validate(_ context.Context, params json.RawMessage) error {
	var p struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.Key == "" {
		return fmt.Errorf("loadtest: lt_noop wants {\"key\": string}")
	}
	return nil
}

func (noopAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }

func (a noopAction) Execute(_ context.Context, params json.RawMessage) (string, error) {
	var p struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return "", fmt.Errorf("loadtest: lt_noop: %w", err)
	}
	a.mu.Lock()
	a.runs[p.Key]++
	a.mu.Unlock()
	return "no-op done", nil
}
