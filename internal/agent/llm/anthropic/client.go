package anthropic

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/bedrock"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/vertex"
	"github.com/aws/aws-sdk-go-v2/aws"
	"golang.org/x/oauth2/google"
)

// RequestOption is an SDK client option: a provider's (WorkspaceOptions,
// BedrockOption, VertexOption) or a test's base URL and key. It is the SDK's
// own type, so callers outside this package need not import the SDK.
type RequestOption = option.RequestOption

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
func WorkspaceOptions(getenv func(string) string) []RequestOption {
	var opts []RequestOption
	if id := strings.TrimSpace(getenv(WorkspaceEnv)); id != "" {
		opts = append(opts, option.WithHeader(WorkspaceHeader, id))
	}
	return opts
}

// The first-party variables EnvOptions reads.
const (
	APIKeyEnv    = "ANTHROPIC_API_KEY"
	AuthTokenEnv = "ANTHROPIC_AUTH_TOKEN"
	BaseURLEnv   = "ANTHROPIC_BASE_URL"
)

// responseHeaderTimeout matches the SDK's default client, which
// option.WithoutEnvironmentDefaults drops along with the autoload.
const responseHeaderTimeout = 10 * time.Minute

// EnvOptions returns the options that make a first-party client take its
// credential and base URL from getenv instead of the process environment:
// the SDK's environment autoload is skipped (option.WithoutEnvironmentDefaults),
// then ANTHROPIC_API_KEY, else ANTHROPIC_AUTH_TOKEN, and ANTHROPIC_BASE_URL
// when set are read through getenv. ok is false when getenv has neither
// credential. The key is never logged or returned in an error.
func EnvOptions(getenv func(string) string) (opts []RequestOption, ok bool) {
	hc := &http.Client{Transport: http.DefaultTransport}
	if t, isT := http.DefaultTransport.(*http.Transport); isT {
		t = t.Clone()
		t.ResponseHeaderTimeout = responseHeaderTimeout
		hc = &http.Client{Transport: t}
	}
	opts = []RequestOption{option.WithoutEnvironmentDefaults(), option.WithHTTPClient(hc)}
	if u := strings.TrimSpace(getenv(BaseURLEnv)); u != "" {
		opts = append(opts, option.WithBaseURL(u))
	}
	if k := strings.TrimSpace(getenv(APIKeyEnv)); k != "" {
		return append(opts, option.WithAPIKey(k)), true
	}
	if t := strings.TrimSpace(getenv(AuthTokenEnv)); t != "" {
		return append(opts, option.WithAuthToken(t)), true
	}
	return opts, false
}

// New returns the adapter for provider on an SDK client built from opts (the
// provider's options: WorkspaceOptions, bedrock.WithConfig or
// vertex.WithCredentials, plus any base URL or key a caller adds). The client
// keeps the SDK's own retries.
func New(provider string, opts []RequestOption, log *slog.Logger) *Model {
	if log == nil {
		log = slog.Default()
	}
	return NewWithTurner(provider, &streamer{client: sdk.NewClient(opts...), log: log}, log)
}

// BedrockOption sends the client's requests to Claude on Amazon Bedrock with
// cfg: its region, and SigV4 with its credentials unless
// AWS_BEARER_TOKEN_BEDROCK is set (the SDK reads it). The client then skips
// the first-party credential autoload, so ANTHROPIC_API_KEY never reaches
// Bedrock.
func BedrockOption(cfg aws.Config) RequestOption {
	return bedrock.WithConfig(cfg)
}

// VertexOption sends the client's requests to Claude on Vertex AI in region
// and project, authorized by creds. ctx must outlive the client: the token
// source keeps it for every later refresh. The SDK panics on a failure it
// cannot report; that comes back as an error.
func VertexOption(ctx context.Context, region, project string, creds *google.Credentials) (opt RequestOption, err error) {
	defer func() {
		if p := recover(); p != nil {
			opt, err = nil, fmt.Errorf("set up vertex client: %v", p)
		}
	}()
	return vertex.WithCredentials(ctx, region, project, creds), nil
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
