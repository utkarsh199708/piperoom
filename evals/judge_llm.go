package evals

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/agent/llm"
	"piperoom/internal/config"
)

// judgeTimeout bounds one judge call.
const judgeTimeout = 2 * time.Minute

// judgeMaxTokens leaves room for the thinking that is on by default on newer
// models; the verdict itself is a few dozen tokens. Thinking at low effort
// usually takes a few hundred tokens, but one outlier passed 2048 in CI run
// 39317961277 and lost its verdict. The ceiling is billed only as used.
const judgeMaxTokens = 8192

// judgeEffort is the judge's effort: a verdict on one hypothesis needs little
// reasoning. An adapter without effort drops it.
const judgeEffort = "low"

// judgeRetryNote is the user turn that asks once more for the verdict when a
// model without structured output answered with something else. It is fixed
// text: the bad reply goes back as the model's own turn, never quoted here.
const judgeRetryNote = "Your reply was not the one JSON object asked for. Answer again with exactly one JSON object with the four fields layer_correct, root_cause_correct, root_cause_layer_correct and reason, and nothing else."

// verdictSchema is the structured-output schema of the verdict. The judge is
// not given a temperature: current models reject non-default sampling
// parameters, so determinism comes from the schema and the strict parser.
var verdictSchema = map[string]any{
	"type": "object",
	"properties": map[string]any{
		"layer_correct":            map[string]any{"type": "boolean"},
		"root_cause_correct":       map[string]any{"type": "boolean"},
		"root_cause_layer_correct": map[string]any{"type": "boolean"},
		"reason":                   map[string]any{"type": "string"},
	},
	"required":             []string{"layer_correct", "root_cause_correct", "root_cause_layer_correct", "reason"},
	"additionalProperties": false,
}

// JudgeID is how a judge cache entry and a report name the judge model:
// "provider:model" (for example "anthropic:claude-opus-5-5"). An empty
// provider leaves the bare model, as entries recorded before V9 hold it.
func JudgeID(provider, model string) string {
	if provider == "" {
		return model
	}
	return provider + ":" + model
}

// LLMJudge is the provider-neutral LLM judge (NewLLMJudge). It is a
// MeteredJudge and safe for concurrent use.
type LLMJudge struct {
	m     llm.Model
	model string
}

var _ MeteredJudge = (*LLMJudge)(nil)

// NewLLMJudge returns the judge that grades on m with model (the id as m's
// provider names it; empty means the repo's configured model,
// PIPEROOM_MODEL). Every call asks for the verdict schema as structured
// output (llm.Request.OutputSchema) at low effort; an adapter without
// structured output answers through its fallback (a forced tool call or plain
// text, both read here), and such a model gets one retry when its reply is not
// the verdict. The reply is always parsed strictly (parseVerdict), never
// guessed.
func NewLLMJudge(m llm.Model, model string) *LLMJudge {
	if model == "" {
		model = config.Load().Model // PIPEROOM_MODEL, the repo's configured model
	}
	return &LLMJudge{m: m, model: model}
}

// JudgeID is the judge's "provider:model", what a cache fill records.
func (j *LLMJudge) JudgeID() string {
	if j == nil || j.m == nil {
		return ""
	}
	return JudgeID(j.m.Provider(), j.model)
}

// JudgeHypothesis grades hypothesis against sc's ground truth.
func (j *LLMJudge) JudgeHypothesis(ctx context.Context, sc Scenario, hypothesis string) (Verdict, error) {
	v, _, err := j.JudgeHypothesisMetered(ctx, sc, hypothesis)
	return v, err
}

// JudgeHypothesisMetered is JudgeHypothesis with the tokens of its calls.
func (j *LLMJudge) JudgeHypothesisMetered(ctx context.Context, sc Scenario, hypothesis string) (Verdict, JudgeUsage, error) {
	return NewMeteredTextJudge(j.send).JudgeHypothesisMetered(ctx, sc, hypothesis)
}

// send asks the model for the verdict and returns its JSON text. On a model
// without structured output, a reply that does not parse gets one retry with
// the reply in the history; the second reply is returned as it is, so the
// strict parser reports its failure.
func (j *LLMJudge) send(ctx context.Context, system, user string) (string, JudgeUsage, error) {
	return j.sendSchema(ctx, system, user, verdictSchema, func(s string) error {
		_, err := parseVerdict(s)
		return err
	}, judgeRetryNote)
}

// sendSchema is send for any reply schema: parse checks a reply and
// retryNote asks again for one.
func (j *LLMJudge) sendSchema(ctx context.Context, system, user string, schema map[string]any, parse func(string) error, retryNote string) (string, JudgeUsage, error) {
	if j == nil || j.m == nil {
		return "", JudgeUsage{}, errors.New("no judge model")
	}
	req := llm.Request{
		Model:        j.model,
		System:       system,
		Messages:     []llm.Message{{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartText, Text: user}}}},
		MaxTokens:    judgeMaxTokens,
		Effort:       judgeEffort,
		OutputSchema: schema,
	}
	text, msg, u, err := j.turn(ctx, req)
	if err != nil || j.m.Capabilities(j.model).StructuredOutput {
		return text, u, err
	}
	if parse(text) == nil {
		return text, u, nil
	}
	req.Messages = append(req.Messages, retryTurnsWith(msg, retryNote)...)
	text, _, u2, err := j.turn(ctx, req)
	u.InputTokens += u2.InputTokens
	u.OutputTokens += u2.OutputTokens
	return text, u, err
}

// retryTurns are the history a retry appends: the model's reply as it came,
// then the retry note. A tool call the reply made (an adapter's forced-tool
// fallback) is answered first with an error result, since every provider
// wants each call answered before the next user text.
func retryTurns(reply llm.Message) []llm.Message {
	return retryTurnsWith(reply, judgeRetryNote)
}

// retryTurnsWith is retryTurns with its own retry note.
func retryTurnsWith(reply llm.Message, note string) []llm.Message {
	reply.Role = llm.RoleAssistant
	var parts []llm.Part
	for _, p := range reply.Parts {
		if p.Kind == llm.PartToolCall && p.ToolCall != nil {
			parts = append(parts, llm.Part{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{
				CallID: p.ToolCall.ID, Name: p.ToolCall.Name, Content: "not a valid verdict", IsError: true}})
		}
	}
	parts = append(parts, llm.Part{Kind: llm.PartText, Text: note})
	return []llm.Message{reply, {Role: llm.RoleUser, Parts: parts}}
}

// turn makes one call and returns the verdict text: the reply's text, or,
// from an adapter that used a forced tool call for the schema, that call's
// input.
func (j *LLMJudge) turn(ctx context.Context, req llm.Request) (string, llm.Message, JudgeUsage, error) {
	ctx, cancel := context.WithTimeout(ctx, judgeTimeout)
	defer cancel()
	resp, err := j.m.Turn(ctx, req)
	// Cache reads and writes are input too: they are billed.
	u := JudgeUsage{
		InputTokens:  resp.Usage.Input + resp.Usage.CacheWrite + resp.Usage.CacheRead,
		OutputTokens: resp.Usage.Output,
	}
	if err != nil {
		return "", resp.Message, u, fmt.Errorf("model: %w", err)
	}
	switch resp.Stop {
	case llm.StopRefusal:
		return "", resp.Message, u, errors.New("model: the judge model refused to grade this hypothesis")
	case llm.StopMaxTokens:
		return "", resp.Message, u, fmt.Errorf("model: the verdict was cut off at max_tokens (%d)", judgeMaxTokens)
	case llm.StopEndTurn, llm.StopToolUse:
	default:
		return "", resp.Message, u, fmt.Errorf("model: unexpected stop reason %q", resp.Stop)
	}
	var b strings.Builder
	var call *llm.ToolCall
	for _, p := range resp.Message.Parts {
		switch {
		case p.Kind == llm.PartText:
			b.WriteString(p.Text)
		case p.Kind == llm.PartToolCall && p.ToolCall != nil && call == nil:
			call = p.ToolCall
		}
	}
	if b.Len() == 0 && call != nil {
		b.Write(call.Input)
	}
	if b.Len() == 0 {
		return "", resp.Message, u, errors.New("model: reply has no text")
	}
	return b.String(), resp.Message, u, nil
}
