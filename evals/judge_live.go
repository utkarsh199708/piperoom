//go:build livejudge || claude

// The live judge's provider wiring is linked only with the build tag
// livejudge (claude is its alias until the workflow switches), so a default
// build can never make a paid judge call.

package evals

import (
	"context"
	"os"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"piperoom/internal/agent/llm"
	anthropicllm "piperoom/internal/agent/llm/anthropic"
	"piperoom/internal/config"
)

// NewClaudeJudge is NewLLMJudge on the Anthropic API (the key comes from
// ANTHROPIC_API_KEY and is never read or logged here). extra options, such as
// a test server's base URL, go to the client. An empty model means the repo's
// configured model (PIPEROOM_MODEL). It ignores PIPEROOM_MODEL_PROVIDER;
// NewProviderJudge honours it.
//
// Deprecated: use NewProviderJudge, or NewLLMJudge on any llm.Model.
func NewClaudeJudge(model string, extra ...option.RequestOption) MeteredJudge {
	return newClaudeJudge(llm.ProviderAnthropic, model, append(workspaceOptions(os.Getenv), extra...)...)
}

// NewProviderJudge is NewLLMJudge on the Claude adapter for cfg's model
// provider (PIPEROOM_MODEL_PROVIDER): the Anthropic API, Bedrock or Vertex,
// with the same request on each (low effort, JSON-schema output). On Bedrock
// and Vertex the model must be that provider's id (an empty model means
// cfg.Model, PIPEROOM_MODEL). It reads cloud credentials but calls no model;
// a missing setting or credential is an error. extra options go after the
// provider's, so a test server's base URL wins.
func NewProviderJudge(ctx context.Context, cfg config.Config, model string, extra ...option.RequestOption) (*LLMJudge, error) {
	return newProviderJudge(ctx, cfg, model, os.Getenv, defaultJudgeLoaders(), extra...)
}

// newProviderJudge is NewProviderJudge with the environment and credential
// loaders injected.
func newProviderJudge(ctx context.Context, cfg config.Config, model string, getenv func(string) string, ld judgeLoaders, extra ...option.RequestOption) (*LLMJudge, error) {
	provider, opts, err := judgeClientOptions(ctx, cfg, getenv, ld)
	if err != nil {
		return nil, err
	}
	if model == "" {
		model = cfg.Model
	}
	return newClaudeJudge(provider, model, append(opts, extra...)...), nil
}

// newClaudeJudge builds the judge on the Claude adapter over a client with
// opts.
func newClaudeJudge(provider, model string, opts ...option.RequestOption) *LLMJudge {
	m := anthropicllm.NewWithTurner(provider, messagesTurner{client: sdk.NewClient(opts...)}, nil)
	return NewLLMJudge(m, model)
}

// messagesTurner sends the judge's request unstreamed, as the judge always
// did: a verdict is a few dozen tokens under an 8192-token ceiling, far below
// the size the SDK wants streamed, and Vertex then keeps its rawPredict
// route. The adapter encodes the request and classifies the error.
type messagesTurner struct {
	client sdk.Client
}

func (t messagesTurner) Turn(ctx context.Context, params sdk.BetaMessageNewParams) (sdk.BetaMessage, error) {
	msg, err := t.client.Beta.Messages.New(ctx, params)
	if msg == nil {
		return sdk.BetaMessage{}, err
	}
	return *msg, err
}
