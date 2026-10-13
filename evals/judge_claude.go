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
)

// DefaultJudgeModel grades hypotheses. It differs from the agent's default
// model so the agent does not grade itself.
const DefaultJudgeModel = "claude-sonnet-5-5"

// judgeTimeout bounds one judge call.
const judgeTimeout = 2 * time.Minute

// NewClaudeJudge returns a HypothesisJudge that calls the Anthropic Messages
// API at temperature 0 with the SDK client (the key comes from
// ANTHROPIC_API_KEY and is never read or logged here). extra options, such as
// a test server's base URL, go to the client.
func NewClaudeJudge(model string, extra ...option.RequestOption) HypothesisJudge {
	if model == "" {
		model = DefaultJudgeModel
	}
	client := anthropic.NewClient(extra...)
	return NewTextJudge(func(ctx context.Context, system, user string) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, judgeTimeout)
		defer cancel()
		msg, err := client.Messages.New(ctx, anthropic.MessageNewParams{
			Model:       model,
			MaxTokens:   512,
			Temperature: anthropic.Float(0),
			System:      []anthropic.TextBlockParam{{Text: system}},
			Messages:    []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock(user))},
		})
		if err != nil {
			return "", fmt.Errorf("messages: %w", err)
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
