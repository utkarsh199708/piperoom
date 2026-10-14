package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// model is the seam between the investigation loop and the Claude API. It
// returns one accumulated assistant turn per call. The production
// implementation streams; tests replace it with a scripted fake.
//
// It talks to the Beta Messages API (anthropic.Client.Beta.Messages), not the
// stable one: server-side compaction (beta compact-2026-01-12) is only
// available there. See run.go's request() for the beta header and context
// management config.
type model interface {
	Turn(ctx context.Context, params anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error)
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

// workspaceEnv names the variable holding the Anthropic workspace id. A user
// API key (sk-ant-usr-...) is not scoped to a workspace, so the API rejects
// its requests unless they carry the anthropic-workspace-id header. The id is
// not a secret. It moves into internal/config later.
const workspaceEnv = "ANTHROPIC_WORKSPACE_ID"

// workspaceHeader is the request header the API reads the workspace id from.
const workspaceHeader = "anthropic-workspace-id"

// clientOptions returns the SDK request options for the production client.
// getenv is os.Getenv in production. An empty (or blank) workspace id adds
// nothing, so a workspace-scoped key behaves exactly as before. The API key is
// left to the SDK (ANTHROPIC_API_KEY) and is never read or logged here.
func clientOptions(getenv func(string) string) []option.RequestOption {
	var opts []option.RequestOption
	if id := strings.TrimSpace(getenv(workspaceEnv)); id != "" {
		opts = append(opts, option.WithHeader(workspaceHeader, id))
	}
	return opts
}

// newModel builds the production model client. opts.Effort/model selection
// happens when the request params are assembled, so the client itself only
// needs the SDK defaults (which read ANTHROPIC_API_KEY from the environment)
// plus the workspace header when ANTHROPIC_WORKSPACE_ID is set. Both the agent
// loop and the hand-off writer build their client here, so both send it.
func newModel(opts Options, log *slog.Logger) model {
	return newModelWith(os.Getenv, log)
}

// newModelWith is newModel with the environment lookup and extra SDK options
// (a test server's base URL and key) injected.
func newModelWith(getenv func(string) string, log *slog.Logger, extra ...option.RequestOption) model {
	if log == nil {
		log = slog.Default()
	}
	reqOpts := append(clientOptions(getenv), extra...)
	return &anthropicModel{
		client: anthropic.NewClient(reqOpts...),
		log:    log,
	}
}

func (m *anthropicModel) Turn(ctx context.Context, params anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, modelTimeout)
	defer cancel()

	stream := m.client.Beta.Messages.NewStreaming(ctx, params)
	var msg anthropic.BetaMessage
	for stream.Next() {
		if err := msg.Accumulate(stream.Current()); err != nil {
			return msg, fmt.Errorf("agent: accumulate stream event: %w", err)
		}
	}
	if err := stream.Err(); err != nil {
		var apiErr *anthropic.Error
		if !errors.As(err, &apiErr) && msg.ID == "" {
			// No HTTP response and no stream event: the request never reached
			// the API (no credentials, refused connection, cancelled before
			// sending), so it used no tokens and no agent.usage is written.
			return msg, fmt.Errorf("agent: stream: %w: %w", errNotSent, err)
		}
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
