package agent

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
)

// model is the seam between the investigation loop and the Claude API. It
// returns one accumulated assistant turn per call. The production
// implementation streams; tests replace it with a scripted fake.
type model interface {
	Turn(ctx context.Context, params anthropic.MessageNewParams) (anthropic.Message, error)
}

// modelTimeout bounds a single model turn. The SDK itself recommends allowing
// up to ten minutes for a streaming request.
const modelTimeout = 10 * time.Minute

// anthropicModel streams a turn through the Anthropic SDK and returns the
// accumulated message.
type anthropicModel struct {
	client anthropic.Client
	log    *slog.Logger
}

// newModel builds the production model client. opts.Effort/model selection
// happens when the request params are assembled, so the client itself only
// needs the SDK defaults (which read ANTHROPIC_API_KEY from the environment).
func newModel(opts Options, log *slog.Logger) model {
	if log == nil {
		log = slog.Default()
	}
	return &anthropicModel{
		client: anthropic.NewClient(),
		log:    log,
	}
}

func (m *anthropicModel) Turn(ctx context.Context, params anthropic.MessageNewParams) (anthropic.Message, error) {
	ctx, cancel := context.WithTimeout(ctx, modelTimeout)
	defer cancel()

	stream := m.client.Messages.NewStreaming(ctx, params)
	var msg anthropic.Message
	for stream.Next() {
		if err := msg.Accumulate(stream.Current()); err != nil {
			return msg, fmt.Errorf("agent: accumulate stream event: %w", err)
		}
	}
	if err := stream.Err(); err != nil {
		return msg, fmt.Errorf("agent: stream: %w", err)
	}
	m.log.Debug("anthropic turn complete",
		"stop_reason", msg.StopReason,
		"input_tokens", msg.Usage.InputTokens,
		"output_tokens", msg.Usage.OutputTokens,
		"cache_read_input_tokens", msg.Usage.CacheReadInputTokens,
	)
	return msg, nil
}
