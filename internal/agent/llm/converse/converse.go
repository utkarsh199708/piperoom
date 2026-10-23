// Package converse is the llm.Model adapter for non-Claude models on Amazon
// Bedrock, through the Converse API of the AWS SDK's bedrockruntime client
// (design docs/design/model-providers.md, row V5). Claude on Bedrock stays on
// the Claude adapter (internal/agent/llm/anthropic, PIPEROOM_MODEL_PROVIDER=
// bedrock): this adapter refuses a Claude model id.
//
// It translates the neutral request to Converse (design 7.3): the system
// prompt as system blocks, tools as toolSpec with the schema as is, tool calls
// as toolUse and their results as toolResult (status error where the model
// takes it), cachePoint blocks where the model supports them, and maps the
// answer back: guardrail_intervened and content_filtered are refusals, usage
// is normalised, and a prompt the server reports far below the bytes sent is
// an ErrTruncated error (design 6.2). Requests are not streamed (design 3.1).
//
// Structured output: Converse has none in general, so Capabilities reports
// StructuredOutput false and the agent's own fallback (internal/agent
// structured.go: a forced emit_<what> tool, schema checked in code, one retry)
// does the work. A caller that sends OutputSchema anyway (the evals judge)
// gets the same shape from Turn: one forced tool whose input is the answer;
// the caller validates it.
//
// The adapter takes the process's shared *aws.Config (as the Claude adapter's
// Bedrock path does through EndpointConfig.AWSConfig) or a Bedrock API key in
// AWS_BEARER_TOKEN_BEDROCK, which the SDK reads and sends itself; the adapter
// holds it only as a netsec.Secret to mask it in error bodies and log lines,
// and never logs it or puts it in an error. It never imports internal/wiring.
package converse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"

	"piperoom/internal/agent/llm"
	"piperoom/internal/clients/netsec"
)

// TurnTimeout bounds one model turn, retries included (design 7.4: never past
// the turn's 10 min deadline). A caller's shorter deadline wins.
const TurnTimeout = 10 * time.Minute

// BearerTokenEnv is the Bedrock API key variable. The SDK reads it itself
// (bedrockruntime.New) and prefers it over SigV4; this package checks that it
// is set and masks it in error text.
const BearerTokenEnv = "AWS_BEARER_TOKEN_BEDROCK"

// Config builds the adapter.
type Config struct {
	// AWS is the process's shared AWS config (wiring.LoadAWS). It is copied,
	// never changed: the region set here stays local, the credentials
	// provider and its cache are shared. Nil is allowed only with a Bedrock
	// API key in AWS_BEARER_TOKEN_BEDROCK.
	AWS *aws.Config
	// Region overrides AWS.Region (AWS_REGION). One of the two is required.
	Region string
	// BaseEndpoint, when set, replaces the regional Bedrock endpoint (a VPC
	// interface endpoint). Tests point it at an httptest server.
	BaseEndpoint string
	// Models are the model ids this adapter will serve (every role on the
	// provider). New refuses a Claude id among them.
	Models []string
	// Getenv reads AWS_BEARER_TOKEN_BEDROCK (presence, and the value to mask
	// in error text); nil means os.Getenv. The SDK itself always reads the
	// process environment.
	Getenv func(string) string
	// Log gets the debug lines (redacted error bodies); nil means
	// slog.Default().
	Log *slog.Logger

	// attempts and delay replace the retry bounds in tests: attempts 0 is
	// the design's 3 (two retries), 1 turns retries off; delay nil is the
	// design's schedule (retry.go).
	attempts int
	delay    func(attempt int, err error) time.Duration
}

// Model is the Bedrock Converse adapter. It is safe for concurrent use.
type Model struct {
	client *bedrockruntime.Client
	// key is the Bedrock API key, held only to mask it in error bodies and
	// log lines; the SDK sends it.
	key netsec.Secret
	log *slog.Logger
}

var _ llm.Model = (*Model)(nil)

// ErrClaudeModel is returned for a Claude model id: Claude on Bedrock runs on
// the Claude adapter, which keeps its prompt cache, thinking and compaction.
var ErrClaudeModel = errors.New("use PIPEROOM_MODEL_PROVIDER=bedrock")

// CheckModel refuses a model id this adapter must not serve: empty, or a
// Claude model (a foundation model id, a cross-region inference profile id or
// an ARN naming one). An application inference profile ARN names no model,
// so it passes; the start probe (V6) is what catches it.
func CheckModel(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("llm/converse: empty model id")
	}
	if isClaude(id) {
		return fmt.Errorf("llm/converse: %q is a Claude model: %w", id, ErrClaudeModel)
	}
	return nil
}

// New returns the adapter. It checks the configuration and reads no
// credentials: a missing region, no credentials provider and no Bedrock API
// key, or a Claude model id fail here, at start.
func New(cfg Config) (*Model, error) {
	getenv := cfg.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	for _, id := range cfg.Models {
		if err := CheckModel(id); err != nil {
			return nil, err
		}
	}
	var awsCfg aws.Config
	if cfg.AWS != nil {
		awsCfg = cfg.AWS.Copy()
	}
	if r := strings.TrimSpace(cfg.Region); r != "" {
		awsCfg.Region = r
	}
	if awsCfg.Region == "" {
		return nil, errors.New("llm/converse: PIPEROOM_MODEL_PROVIDER=bedrock-converse needs AWS_REGION")
	}
	if getenv(BearerTokenEnv) == "" && awsCfg.Credentials == nil {
		return nil, errors.New("llm/converse: bedrock-converse needs AWS credentials (environment, shared config, or an instance/task role) or " + BearerTokenEnv)
	}
	pol := retryPolicy{attempts: cfg.attempts, delay: cfg.delay}
	base := strings.TrimSpace(cfg.BaseEndpoint)
	client := bedrockruntime.NewFromConfig(awsCfg, func(o *bedrockruntime.Options) {
		if base != "" {
			o.BaseEndpoint = aws.String(base)
		}
		o.Retryer = pol.retryer()
		// A RetryMaxAttempts from the shared config (AWS_MAX_ATTEMPTS) would
		// wrap the retryer and widen the design's bounds.
		o.RetryMaxAttempts = 0
		o.HTTPClient = cappedClient{next: o.HTTPClient}
	})
	return &Model{client: client, key: netsec.Secret(getenv(BearerTokenEnv)), log: log}, nil
}

// Provider is llm.ProviderBedrockConverse.
func (m *Model) Provider() string { return llm.ProviderBedrockConverse }

// Capabilities is the model's row of the family table (models.go). It never
// calls the network.
func (m *Model) Capabilities(model string) llm.Capabilities {
	return familyOf(model).caps()
}

// Turn sends req through Converse and returns the whole assistant turn. A
// failed request returns an error wrapping an *llm.APIError, with the answer
// when one arrived (its usage was billed); a request that could not be
// encoded was never sent and wraps llm.ErrNotSent too.
func (m *Model) Turn(ctx context.Context, req llm.Request) (llm.Response, error) {
	if err := CheckModel(req.Model); err != nil {
		return llm.Response{}, notSent(err)
	}
	fam := familyOf(req.Model)
	in, err := encodeRequest(req, fam)
	if err != nil {
		return llm.Response{}, notSent(err)
	}
	ctx, cancel := context.WithTimeout(ctx, TurnTimeout)
	defer cancel()

	var size wireSize
	out, err := m.client.Converse(ctx, in, func(o *bedrockruntime.Options) {
		o.HTTPClient = recordingClient{next: o.HTTPClient, size: &size}
	})
	if err != nil {
		return llm.Response{}, classifyError(err, m.key, m.log)
	}
	resp := decodeOutput(req.Model, out, &size)
	if err := truncated(resp.Usage, size.request()); err != nil {
		return resp, err
	}
	m.log.Debug("converse turn complete",
		"stop_reason", resp.ProviderStop,
		"input_tokens", resp.Usage.Input,
		"output_tokens", resp.Usage.Output,
		"cache_read_input_tokens", resp.Usage.CacheRead,
		"cache_write_input_tokens", resp.Usage.CacheWrite,
	)
	return resp, nil
}

// notSent is the error of a request that never left the process. The cause's
// text can name request content, so it stays in Err only.
func notSent(cause error) error {
	ae := &llm.APIError{Provider: llm.ProviderBedrockConverse, Kind: llm.ErrBadRequest, Err: fmt.Errorf("%w: %w", llm.ErrNotSent, cause)}
	text := ae.Error() + ": the request could not be encoded"
	if errors.Is(cause, ErrClaudeModel) {
		// Fixed text, no request content: the operator's fix.
		text = ae.Error() + ": a Claude model id: " + ErrClaudeModel.Error()
	}
	return &classified{api: ae, text: text}
}
