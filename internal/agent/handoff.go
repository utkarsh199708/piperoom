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

// writeHandoff is the testable seam: the model is injected. After the summary
// parses it is checked against the room log (CheckHandoff). On violations the
// model gets one chance to correct itself; whatever violations remain are
// stored on the summary, never a reason to refuse it.
func writeHandoff(ctx context.Context, roomEvents []events.Event, opts Options, m model) (events.HandoffSummary, error) {
	params, err := summaryParams(roomEvents, opts, handoffPrompt(), "handoff")
	if err != nil {
		return events.HandoffSummary{}, err
	}
	summary, raw, err := requestSummary(ctx, params, m, "handoff")
	if err != nil {
		return events.HandoffSummary{}, err
	}
	vs := CheckHandoff(roomEvents, summary)
	if len(vs) > 0 {
		retry := params
		retry.Messages = append(append([]anthropic.BetaMessageParam(nil), params.Messages...),
			anthropic.BetaMessageParam{
				Role:    anthropic.BetaMessageParamRoleAssistant,
				Content: []anthropic.BetaContentBlockParamUnion{anthropic.NewBetaTextBlock(raw)},
			},
			anthropic.NewBetaUserMessage(anthropic.NewBetaTextBlock(retryNote(vs))),
		)
		// A failed or late second attempt keeps the first summary and its
		// violations: the request deadline is shared, so the retry gets what is left.
		if second, _, rerr := requestSummary(ctx, retry, m, "handoff"); rerr == nil {
			summary, vs = second, CheckHandoff(roomEvents, second)
		}
	}
	summary.Violations = toEventViolations(vs)
	return summary, nil
}

// retryNote is the user turn that asks for a corrected summary. A violation's
// Detail quotes the model's own words, which may echo tool text, so it goes in
// the untrusted wrapper.
func retryNote(vs []HandoffViolation) string {
	lines := make([]string, len(vs))
	for i, v := range vs {
		lines[i] = v.Rule + " " + v.Field + ": " + wrapUntrusted(v.Detail)
	}
	return "The summary failed these checks; fix them and return the corrected JSON:\n- " + strings.Join(lines, "\n- ")
}

// writeSummary is the request shared by the hand-off and the post-mortem: one
// structured call over the room log with the given system prompt. what names
// the caller in error text.
func writeSummary(ctx context.Context, roomEvents []events.Event, opts Options, m model, prompt, what string) (events.HandoffSummary, error) {
	params, err := summaryParams(roomEvents, opts, prompt, what)
	if err != nil {
		return events.HandoffSummary{}, err
	}
	summary, _, err := requestSummary(ctx, params, m, what)
	return summary, err
}

// summaryParams builds the structured request over the room log.
func summaryParams(roomEvents []events.Event, opts Options, prompt, what string) (anthropic.BetaMessageNewParams, error) {
	if len(roomEvents) == 0 {
		return anthropic.BetaMessageNewParams{}, fmt.Errorf("agent: cannot write %s from an empty room", what)
	}
	return anthropic.BetaMessageNewParams{
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
	}, nil
}

// requestSummary sends one request and parses the summary. It also returns the
// model's raw text, which a retry replays as the assistant turn.
func requestSummary(ctx context.Context, params anthropic.BetaMessageNewParams, m model, what string) (events.HandoffSummary, string, error) {
	msg, err := m.Turn(ctx, params)
	if err != nil {
		return events.HandoffSummary{}, "", fmt.Errorf("agent: %s request: %w", what, err)
	}
	if msg.StopReason == anthropic.BetaStopReasonRefusal {
		return events.HandoffSummary{}, "", fmt.Errorf("agent: %s: model refused the request", what)
	}

	text := extractText(msg.Content)
	if strings.TrimSpace(text) == "" {
		return events.HandoffSummary{}, "", fmt.Errorf("agent: %s: model returned no text", what)
	}

	var summary events.HandoffSummary
	if err := json.Unmarshal([]byte(text), &summary); err != nil {
		return events.HandoffSummary{}, "", fmt.Errorf("agent: %s: model output is not valid JSON: %w", what, err)
	}
	if strings.TrimSpace(summary.WhatBroke) == "" {
		return events.HandoffSummary{}, "", fmt.Errorf("agent: %s: model omitted what_broke", what)
	}
	if strings.TrimSpace(summary.Why) == "" {
		return events.HandoffSummary{}, "", fmt.Errorf("agent: %s: model omitted why", what)
	}
	return summary, text, nil
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
