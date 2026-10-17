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

// workspaceOptions adds the workspace header when ANTHROPIC_WORKSPACE_ID is
// set. A user API key (sk-ant-usr-...) is not scoped to a workspace, and the
// API rejects its requests with a 400 unless they carry
// "anthropic-workspace-id"; internal/agent sends the same header for the
// agent. The id is not a secret. A blank value adds nothing, so a
// workspace-scoped key behaves as before.
func workspaceOptions(getenv func(string) string) []option.RequestOption {
	if id := strings.TrimSpace(getenv("ANTHROPIC_WORKSPACE_ID")); id != "" {
		return []option.RequestOption{option.WithHeader("anthropic-workspace-id", id)}
	}
	return nil
}

// NewClaudeJudge returns a HypothesisJudge that calls the Anthropic Messages
// API with the SDK client, at low effort and with a JSON-schema output format
// (the key comes from ANTHROPIC_API_KEY and is never read or logged here). The
// reply is still parsed strictly by parseVerdict. extra options, such as a
// test server's base URL, go to the client. An empty model means the repo's
// configured model (PIPEROOM_MODEL). The client is a bare anthropic.NewClient
// because internal/agent exports no single-message call through its Endpoint
// (see the PR's contract change request); it ignores PIPEROOM_MODEL_PROVIDER.
//
// The judge is metered (MeteredJudge): each call reports its input and output
// tokens, which the auto judge adds up for the report.
func NewClaudeJudge(model string, extra ...option.RequestOption) MeteredJudge {
	if model == "" {
		model = config.Load().Model // PIPEROOM_MODEL, the repo's configured model
	}
	client := anthropic.NewClient(append(workspaceOptions(os.Getenv), extra...)...)
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
