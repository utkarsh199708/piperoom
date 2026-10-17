package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
)

// handoffMaxTokens bounds the hand-off summary request.
const handoffMaxTokens = 4096

// ErrSummaryInvalid marks a summary request that got an answer but no usable
// summary out of it: the answer was refused, empty, not JSON, or left a
// required field (what_broke, why) empty, and stayed so after the one repair
// request (a refusal gets none). The room maps it, like any other writer error,
// to handoff.failed "summary_failed" and can tell the driver to write the
// summary by hand (QA G4). A transport or model error of the first request is
// not wrapped in it: that call failed, it did not answer badly.
var ErrSummaryInvalid = errors.New("agent: summary invalid")

// WriteHandoff turns the room log into a hand-off summary with one structured
// model request, plus one repair request when the first answer misses a
// required field. On any failure it returns an error, which the server shows.
func WriteHandoff(ctx context.Context, roomEvents []events.Event, opts Options) (events.HandoffSummary, error) {
	opts = opts.withDefaults()
	return writeHandoff(ctx, roomEvents, opts, newModel(opts, nil))
}

// writeHandoff is the testable seam: the model is injected. After the summary
// parses (with its one repair request, summarize) it is checked against the
// room log (CheckHandoff). On violations the model gets one chance to correct
// itself; whatever violations remain are stored on the summary, never a reason
// to refuse it.
func writeHandoff(ctx context.Context, roomEvents []events.Event, opts Options, m model) (events.HandoffSummary, error) {
	if len(roomEvents) == 0 {
		return events.HandoffSummary{}, fmt.Errorf("agent: cannot write %s from an empty room", "handoff")
	}
	prompt, input := handoffPrompt(), renderRoomEvents(roomEvents)
	summary, raw, err := summarizeRaw(ctx, m, opts, prompt, input, "handoff")
	if err != nil {
		return events.HandoffSummary{}, err
	}
	vs := CheckHandoff(roomEvents, summary)
	if len(vs) > 0 {
		retry := summaryRequest(opts, prompt, []anthropic.BetaMessageParam{
			anthropic.NewBetaUserMessage(anthropic.NewBetaTextBlock(input)),
			{
				Role:    anthropic.BetaMessageParamRoleAssistant,
				Content: []anthropic.BetaContentBlockParamUnion{anthropic.NewBetaTextBlock(raw)},
			},
			anthropic.NewBetaUserMessage(anthropic.NewBetaTextBlock(retryNote(vs))),
		})
		// A failed, unusable or late second attempt keeps the first summary and
		// its violations: the request deadline is shared, so the retry gets
		// what is left.
		second, _, bad, rerr := askWith(ctx, m, retry, "handoff")
		if rerr != nil || bad != nil {
			// Counts only: the error text can quote the model's output.
			slog.Warn("agent: handoff self-correction failed; keeping the first summary", "violations", len(vs), "ctx_done", ctx.Err() != nil)
		} else {
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
	if len(roomEvents) == 0 {
		return events.HandoffSummary{}, fmt.Errorf("agent: cannot write %s from an empty room", what)
	}
	return summarize(ctx, m, opts, prompt, renderRoomEvents(roomEvents), what)
}

// summarize asks the model for a summary of input (rendered room events) and
// checks the answer. An answer that is unusable but fixable gets exactly one
// repair request that names what was wrong; whatever that returns is final.
func summarize(ctx context.Context, m model, opts Options, prompt, input, what string) (events.HandoffSummary, error) {
	s, _, err := summarizeRaw(ctx, m, opts, prompt, input, what)
	return s, err
}

// summarizeRaw is summarize that also returns the model's raw text of the
// summary it returns, which the hand-off's self-correction replays as the
// assistant turn.
func summarizeRaw(ctx context.Context, m model, opts Options, prompt, input, what string) (events.HandoffSummary, string, error) {
	s, raw, bad, err := askSummary(ctx, m, opts, prompt, input, what, nil)
	if err != nil {
		return events.HandoffSummary{}, "", err
	}
	if bad == nil {
		return s, raw, nil
	}
	s, raw, again, err := askSummary(ctx, m, opts, prompt, input, what, bad)
	if err != nil {
		// Both the cause (a deadline, a budget stop) and the sentinel stay
		// matchable with errors.Is.
		return events.HandoffSummary{}, "", fmt.Errorf("agent: %s: repair request after %q: %w: %w", what, bad.reason, ErrSummaryInvalid, err)
	}
	if again != nil {
		return events.HandoffSummary{}, "", fmt.Errorf("agent: %s: summary still unusable after one repair (%s): %w", what, again.reason, ErrSummaryInvalid)
	}
	return s, raw, nil
}

// summaryProblem says why an answer cannot be used. reason is fixed text plus
// field names, never model or room text, so it can go into error text.
type summaryProblem struct {
	reason string
	// kind is what the repair note tells the model went wrong when no field is
	// named: "empty" or "not_json".
	kind string
	// missing lists the required fields that were empty, when the answer was
	// valid JSON.
	missing []string
	// cutOff is true when the answer stopped at the token cap.
	cutOff bool
}

// Problem kinds of summaryProblem.
const (
	problemEmpty   = "empty"
	problemNotJSON = "not_json"
)

// askSummary makes one summary request. fix is nil for the first request and
// describes the previous answer's problem for the repair request.
func askSummary(ctx context.Context, m model, opts Options, prompt, input, what string, fix *summaryProblem) (events.HandoffSummary, string, *summaryProblem, error) {
	user := []anthropic.BetaContentBlockParamUnion{anthropic.NewBetaTextBlock(input)}
	if fix != nil {
		user = append(user, anthropic.NewBetaTextBlock(repairNote(*fix)))
	}
	return askWith(ctx, m, summaryRequest(opts, prompt, []anthropic.BetaMessageParam{anthropic.NewBetaUserMessage(user...)}), what)
}

// summaryRequest is the structured summary request: the system prompt, then
// msgs.
func summaryRequest(opts Options, prompt string, msgs []anthropic.BetaMessageParam) anthropic.BetaMessageNewParams {
	return anthropic.BetaMessageNewParams{
		Model:     opts.Model,
		MaxTokens: handoffMaxTokens,
		System: []anthropic.BetaTextBlockParam{
			{Text: prompt, CacheControl: anthropic.NewBetaCacheControlEphemeralParam()},
		},
		Messages: msgs,
		OutputConfig: anthropic.BetaOutputConfigParam{
			Effort: anthropic.BetaOutputConfigEffort(opts.effort()),
			Format: anthropic.BetaJSONOutputFormatParam{Schema: handoffSchema()},
		},
		Thinking: anthropic.BetaThinkingConfigParamUnion{
			OfAdaptive: &anthropic.BetaThinkingConfigAdaptiveParam{},
		},
	}
}

// askWith sends one summary request and parses the answer. It also returns
// the model's raw text. err is an error of the call itself, or a refusal,
// which a repair cannot change; problem is set when the call worked but its
// answer cannot be used.
func askWith(ctx context.Context, m model, params anthropic.BetaMessageNewParams, what string) (events.HandoffSummary, string, *summaryProblem, error) {
	msg, err := m.Turn(ctx, params)
	if err != nil {
		return events.HandoffSummary{}, "", nil, fmt.Errorf("agent: %s request: %w", what, err)
	}
	if msg.StopReason == anthropic.BetaStopReasonRefusal {
		return events.HandoffSummary{}, "", nil, fmt.Errorf("agent: %s: model refused the request: %w", what, ErrSummaryInvalid)
	}
	cutOff := msg.StopReason == anthropic.BetaStopReasonMaxTokens

	text := extractText(msg.Content)
	if strings.TrimSpace(text) == "" {
		return events.HandoffSummary{}, "", &summaryProblem{reason: "model returned no text", kind: problemEmpty, cutOff: cutOff}, nil
	}
	var summary events.HandoffSummary
	if err := json.Unmarshal([]byte(text), &summary); err != nil {
		// The decoder's message can quote the answer, so it stays out of reason.
		return events.HandoffSummary{}, "", &summaryProblem{reason: "model output is not valid JSON", kind: problemNotJSON, cutOff: cutOff}, nil
	}
	var missing []string
	if strings.TrimSpace(summary.WhatBroke) == "" {
		missing = append(missing, "what_broke")
	}
	if strings.TrimSpace(summary.Why) == "" {
		missing = append(missing, "why")
	}
	if len(missing) > 0 {
		return events.HandoffSummary{}, "", &summaryProblem{reason: "model omitted " + strings.Join(missing, " and "), missing: missing, cutOff: cutOff}, nil
	}
	return summary, text, nil, nil
}

// repairNote is the second user block of the repair request. It names the
// problem in fixed words only: the answer it corrects is model text derived
// from untrusted room text and is not echoed back.
func repairNote(p summaryProblem) string {
	var what string
	switch {
	case len(p.missing) == 1:
		what = "the required field " + p.missing[0] + " was missing or empty"
	case len(p.missing) > 1:
		what = "the required fields " + strings.Join(p.missing, " and ") + " were missing or empty"
	case p.kind == problemEmpty:
		what = "it was empty"
	default:
		what = "it was not a valid JSON object"
	}
	if p.cutOff {
		what += ", and it was cut off at the length limit, so keep every string short"
	}
	return "Correction needed. Your previous answer could not be used: " + what + ". " +
		"Answer again with the complete JSON object, every field included. what_broke and why must each be a non-empty sentence based on the room events above; " +
		"done, ruled_out, open and risks are arrays (use [] when there is nothing to list). " +
		"If the events do not show the root cause, say that in why instead of leaving it empty."
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
