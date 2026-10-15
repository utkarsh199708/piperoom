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
	return writeSummary(ctx, roomEvents, opts, m, handoffPrompt(), "handoff")
}

// writeSummary is the request shared by the hand-off and the post-mortem: one
// structured call over the room log with the given system prompt. what names
// the caller in error text.
func writeSummary(ctx context.Context, roomEvents []events.Event, opts Options, m model, prompt, what string) (events.HandoffSummary, error) {
	if len(roomEvents) == 0 {
		return events.HandoffSummary{}, fmt.Errorf("agent: cannot write %s from an empty room", what)
	}

	params := anthropic.BetaMessageNewParams{
		Model:     opts.Model,
		MaxTokens: handoffMaxTokens,
		System: []anthropic.BetaTextBlockParam{
			{Text: prompt, CacheControl: anthropic.NewBetaCacheControlEphemeralParam()},
		},
		Messages: []anthropic.BetaMessageParam{
			anthropic.NewBetaUserMessage(anthropic.NewBetaTextBlock(renderRoomEvents(roomEvents))),
		},
		OutputConfig: anthropic.BetaOutputConfigParam{
			Effort: anthropic.BetaOutputConfigEffort(opts.effort()),
			Format: anthropic.BetaJSONOutputFormatParam{Schema: handoffSchema()},
		},
		Thinking: anthropic.BetaThinkingConfigParamUnion{
			OfAdaptive: &anthropic.BetaThinkingConfigAdaptiveParam{},
		},
	}

	msg, err := m.Turn(ctx, params)
	if err != nil {
		return events.HandoffSummary{}, fmt.Errorf("agent: %s request: %w", what, err)
	}
	if msg.StopReason == anthropic.BetaStopReasonRefusal {
		return events.HandoffSummary{}, fmt.Errorf("agent: %s: model refused the request", what)
	}

	text := extractText(msg.Content)
	if strings.TrimSpace(text) == "" {
		return events.HandoffSummary{}, fmt.Errorf("agent: %s: model returned no text", what)
	}

	var summary events.HandoffSummary
	if err := json.Unmarshal([]byte(text), &summary); err != nil {
		return events.HandoffSummary{}, fmt.Errorf("agent: %s: model output is not valid JSON: %w", what, err)
	}
	if strings.TrimSpace(summary.WhatBroke) == "" {
		return events.HandoffSummary{}, fmt.Errorf("agent: %s: model omitted what_broke", what)
	}
	if strings.TrimSpace(summary.Why) == "" {
		return events.HandoffSummary{}, fmt.Errorf("agent: %s: model omitted why", what)
	}
	return summary, nil
}

// extractText concatenates the text blocks of a message.
func extractText(content []anthropic.BetaContentBlockUnion) string {
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
