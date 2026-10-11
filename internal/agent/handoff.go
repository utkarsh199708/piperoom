package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
)

// handoffMaxTokens bounds the hand-off summary request.
const handoffMaxTokens = 4096

// WriteHandoff turns the room log into a hand-off summary with one structured
// model request. On any failure it returns an error, which the server shows.
func WriteHandoff(ctx context.Context, roomEvents []events.Event, opts Options) (events.HandoffSummary, error) {
	opts = opts.withDefaults()
	return writeHandoff(ctx, roomEvents, opts, newModel(opts, nil))
}

// writeHandoff is the testable seam: the model is injected.
func writeHandoff(ctx context.Context, roomEvents []events.Event, opts Options, m model) (events.HandoffSummary, error) {
	if len(roomEvents) == 0 {
		return events.HandoffSummary{}, fmt.Errorf("agent: cannot write handoff from an empty room")
	}

	params := anthropic.MessageNewParams{
		Model:     opts.Model,
		MaxTokens: handoffMaxTokens,
		System: []anthropic.TextBlockParam{
			{Text: handoffPrompt(), CacheControl: anthropic.NewCacheControlEphemeralParam()},
		},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(renderRoomEvents(roomEvents))),
		},
		OutputConfig: anthropic.OutputConfigParam{
			Effort: anthropic.OutputConfigEffort(opts.effort()),
			Format: anthropic.JSONOutputFormatParam{Schema: handoffSchema()},
		},
		Thinking: anthropic.ThinkingConfigParamUnion{
			OfAdaptive: &anthropic.ThinkingConfigAdaptiveParam{},
		},
	}

	msg, err := m.Turn(ctx, params)
	if err != nil {
		return events.HandoffSummary{}, fmt.Errorf("agent: handoff request: %w", err)
	}
	if msg.StopReason == anthropic.StopReasonRefusal {
		return events.HandoffSummary{}, fmt.Errorf("agent: handoff: model refused the request")
	}

	text := extractText(msg.Content)
	if strings.TrimSpace(text) == "" {
		return events.HandoffSummary{}, fmt.Errorf("agent: handoff: model returned no text")
	}

	var summary events.HandoffSummary
	if err := json.Unmarshal([]byte(text), &summary); err != nil {
		return events.HandoffSummary{}, fmt.Errorf("agent: handoff: model output is not valid JSON: %w", err)
	}
	if strings.TrimSpace(summary.WhatBroke) == "" {
		return events.HandoffSummary{}, fmt.Errorf("agent: handoff: model omitted what_broke")
	}
	if strings.TrimSpace(summary.Why) == "" {
		return events.HandoffSummary{}, fmt.Errorf("agent: handoff: model omitted why")
	}
	return summary, nil
}

// extractText concatenates the text blocks of a message.
func extractText(content []anthropic.ContentBlockUnion) string {
	var b strings.Builder
	for _, block := range content {
		if block.Type == "text" {
			b.WriteString(block.Text)
		}
	}
	return b.String()
}

// handoffSchema is the JSON schema for the structured hand-off output.
func handoffSchema() map[string]any {
	strArr := map[string]any{
		"type":        "array",
		"items":       map[string]any{"type": "string"},
		"description": "short strings",
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"what_broke": map[string]any{"type": "string", "description": "one sentence: what broke"},
			"why":        map[string]any{"type": "string", "description": "the root cause"},
			"done":       strArr,
			"ruled_out":  strArr,
			"open":       strArr,
			"risks":      strArr,
		},
		"required":             []string{"what_broke", "why", "done", "ruled_out", "open", "risks"},
		"additionalProperties": false,
	}
}
