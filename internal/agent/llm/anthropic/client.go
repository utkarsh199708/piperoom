package anthropic

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// TurnTimeout bounds a single model turn. The SDK itself recommends allowing
// up to ten minutes for a streaming request.
const TurnTimeout = 10 * time.Minute

// WorkspaceEnv names the variable holding the Anthropic workspace id. A user
// API key (sk-ant-usr-...) is not scoped to a workspace, so the API rejects
// its requests unless they carry the anthropic-workspace-id header. The id is
// not a secret.
const WorkspaceEnv = "ANTHROPIC_WORKSPACE_ID"

// WorkspaceHeader is the request header the API reads the workspace id from.
const WorkspaceHeader = "anthropic-workspace-id"

// WorkspaceOptions returns the request options the Anthropic API provider
// adds: the workspace header when ANTHROPIC_WORKSPACE_ID is set. getenv is
// os.Getenv in production. An empty (or blank) id adds nothing, so a
// workspace-scoped key behaves exactly as before. The API key is left to the
// SDK (ANTHROPIC_API_KEY) and is never read or logged here. Bedrock and
// Vertex sign their own requests and do not get it.
func WorkspaceOptions(getenv func(string) string) []option.RequestOption {
	var opts []option.RequestOption
	if id := strings.TrimSpace(getenv(WorkspaceEnv)); id != "" {
		opts = append(opts, option.WithHeader(WorkspaceHeader, id))
	}
	return opts
}

// New returns the adapter for provider on an SDK client built from opts (the
// provider's options: WorkspaceOptions, bedrock.WithConfig or
// vertex.WithCredentials, plus any base URL or key a caller adds). The client
// keeps the SDK's own retries.
func New(provider string, opts []option.RequestOption, log *slog.Logger) *Model {
	if log == nil {
		log = slog.Default()
	}
	return NewWithTurner(provider, &streamer{client: sdk.NewClient(opts...), log: log}, log)
}

// streamer streams a turn through the Beta Messages API and returns the
// accumulated message. It talks to the Beta API, not the stable one:
// server-side compaction (beta compact-2026-01-12) is only available there.
type streamer struct {
	client sdk.Client
	log    *slog.Logger
}

func (s *streamer) Turn(ctx context.Context, params sdk.BetaMessageNewParams) (sdk.BetaMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, TurnTimeout)
	defer cancel()

	stream := s.client.Beta.Messages.NewStreaming(ctx, params)
	var msg sdk.BetaMessage
	for stream.Next() {
		if err := msg.Accumulate(stream.Current()); err != nil {
			return msg, fmt.Errorf("llm/anthropic: accumulate stream event: %w", err)
		}
	}
	if err := stream.Err(); err != nil {
		// Unwrapped: ClassifyError keeps this text as the error's, and the
		// evals' run error shows the first 200 runes of it.
		return msg, err
	}
	s.log.Debug("anthropic turn complete",
		"stop_reason", msg.StopReason,
		"input_tokens", msg.Usage.InputTokens,
		"output_tokens", msg.Usage.OutputTokens,
		"cache_read_input_tokens", msg.Usage.CacheReadInputTokens,
	)
	return msg, nil
}
