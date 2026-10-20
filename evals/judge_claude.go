//go:build claude

package evals

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"piperoom/internal/config"
)

// judgeTimeout bounds one judge call.
const judgeTimeout = 2 * time.Minute

// judgeMaxTokens leaves room for the thinking that is on by default on newer
// models; the verdict itself is a few dozen tokens. Thinking at low effort
// usually takes a few hundred tokens, but one outlier passed 2048 in CI run
// 39317961277 and lost its verdict. The ceiling is billed only as used.
const judgeMaxTokens = 8192

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
// API directly with the SDK client, at low effort and with a JSON-schema output
// format (the key comes from ANTHROPIC_API_KEY and is never read or logged
// here). The reply is still parsed strictly by parseVerdict. extra options,
// such as a test server's base URL, go to the client. An empty model means the
// repo's configured model (PIPEROOM_MODEL). It ignores PIPEROOM_MODEL_PROVIDER;
// NewProviderJudge honours it.
//
// The judge is metered (MeteredJudge): each call reports its input and output
// tokens, which the auto judge adds up for the report.
func NewClaudeJudge(model string, extra ...option.RequestOption) MeteredJudge {
	return newClaudeJudge(model, append(workspaceOptions(os.Getenv), extra...)...)
}

// NewProviderJudge is NewClaudeJudge on cfg's model provider
// (PIPEROOM_MODEL_PROVIDER): the Anthropic API, Bedrock or Vertex, with the
// same request (low effort, JSON-schema output; internal/agent sends both on
// every provider). On Bedrock and Vertex the model must be that provider's id
// (an empty model means cfg.Model, PIPEROOM_MODEL). It reads cloud credentials
// but calls no model; a missing setting or credential is an error. extra
// options go after the provider's, so a test server's base URL wins.
func NewProviderJudge(ctx context.Context, cfg config.Config, model string, extra ...option.RequestOption) (MeteredJudge, error) {
	return newProviderJudge(ctx, cfg, model, os.Getenv, defaultJudgeLoaders(), extra...)
}

// newProviderJudge is NewProviderJudge with the environment and credential
// loaders injected.
func newProviderJudge(ctx context.Context, cfg config.Config, model string, getenv func(string) string, ld judgeLoaders, extra ...option.RequestOption) (MeteredJudge, error) {
	_, opts, err := judgeClientOptions(ctx, cfg, getenv, ld)
	if err != nil {
		return nil, err
	}
	if model == "" {
		model = cfg.Model
	}
	return newClaudeJudge(model, append(opts, extra...)...), nil
}

// newClaudeJudge builds the metered judge on a client with opts.
func newClaudeJudge(model string, opts ...option.RequestOption) MeteredJudge {
	if model == "" {
		model = config.Load().Model // PIPEROOM_MODEL, the repo's configured model
	}
	client := anthropic.NewClient(opts...)
	return NewMeteredTextJudge(func(ctx context.Context, system, user string) (string, JudgeUsage, error) {
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
			return "", JudgeUsage{}, fmt.Errorf("messages: %w", err)
		}
		// Cache reads and writes are input too: they are billed.
		u := JudgeUsage{
			InputTokens:  msg.Usage.InputTokens + msg.Usage.CacheCreationInputTokens + msg.Usage.CacheReadInputTokens,
			OutputTokens: msg.Usage.OutputTokens,
		}
		switch msg.StopReason {
		case anthropic.StopReasonRefusal:
			return "", u, errors.New("messages: the judge model refused to grade this hypothesis")
		case anthropic.StopReasonMaxTokens:
			return "", u, fmt.Errorf("messages: the verdict was cut off at max_tokens (%d)", judgeMaxTokens)
		case anthropic.StopReasonEndTurn:
		default:
			return "", u, fmt.Errorf("messages: unexpected stop reason %q", msg.StopReason)
		}
		var b strings.Builder
		for _, c := range msg.Content {
			if c.Type == "text" {
				b.WriteString(c.AsText().Text)
			}
		}
		if b.Len() == 0 {
			return "", u, errors.New("messages: reply has no text")
		}
		return b.String(), u, nil
	})
}
