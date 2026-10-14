//go:build claude

package evals

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"piperoom/internal/config"
)

// judgeTimeout bounds one judge call.
const judgeTimeout = 2 * time.Minute

// judgeMaxTokens leaves room for the thinking that is on by default on newer
// models; the verdict itself is a few dozen tokens.
const judgeMaxTokens = 2048

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

// NewClaudeJudge returns a HypothesisJudge that calls the Anthropic Messages
// API with the SDK client, at low effort and with a JSON-schema output format
// (the key comes from ANTHROPIC_API_KEY and is never read or logged here). The
// reply is still parsed strictly by parseVerdict. extra options, such as a
// test server's base URL, go to the client. An empty model means the repo's
// configured model (PIPEROOM_MODEL). The client is a bare anthropic.NewClient
// because internal/agent exports no single-message call through its Endpoint
// (see the PR's contract change request); it ignores PIPEROOM_MODEL_PROVIDER.
func NewClaudeJudge(model string, extra ...option.RequestOption) HypothesisJudge {
	if model == "" {
		model = config.Load().Model // PIPEROOM_MODEL, the repo's configured model
	}
	client := anthropic.NewClient(extra...)
	return NewTextJudge(func(ctx context.Context, system, user string) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, judgeTimeout)
		defer cancel()
		msg, err := client.Messages.New(ctx, anthropic.MessageNewParams{
			Model:     model,
			MaxTokens: judgeMaxTokens,
			System:    []anthropic.TextBlockParam{{Text: system}},
			Messages:  []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock(user))},
			OutputConfig: anthropic.OutputConfigParam{
				Effort: anthropic.OutputConfigEffortLow,
				Format: anthropic.JSONOutputFormatParam{Schema: verdictSchema},
			},
		})
		if err != nil {
			return "", fmt.Errorf("messages: %w", err)
		}
		switch msg.StopReason {
		case anthropic.StopReasonRefusal:
			return "", errors.New("messages: the judge model refused to grade this hypothesis")
		case anthropic.StopReasonMaxTokens:
			return "", fmt.Errorf("messages: the verdict was cut off at max_tokens (%d)", judgeMaxTokens)
		case anthropic.StopReasonEndTurn:
		default:
			return "", fmt.Errorf("messages: unexpected stop reason %q", msg.StopReason)
		}
		var b strings.Builder
		for _, c := range msg.Content {
			if c.Type == "text" {
				b.WriteString(c.AsText().Text)
			}
		}
		if b.Len() == 0 {
			return "", errors.New("messages: reply has no text")
		}
		return b.String(), nil
	})
}
