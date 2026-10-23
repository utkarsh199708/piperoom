// Package anthropic is the llm.Model adapter for Claude: the Anthropic API,
// Claude on Amazon Bedrock and Claude on Google Vertex AI. All three speak the
// Messages API through the same SDK client (Bedrock and Vertex are request
// options), so one adapter serves them; only the client options differ.
//
// It sends exactly what the agent sent before the provider-neutral interface
// (design docs/design/model-providers.md, rows V1a and V1b): the Beta Messages
// API, streamed, with the same cache breakpoints, effort, adaptive thinking,
// strict tools and server-side compaction. The golden request tests in
// internal/agent pin those bytes.
//
// Package anthropic is the only package under internal/agent that imports
// anthropic-sdk-go.
package anthropic

import (
	"context"
	"log/slog"

	sdk "github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/agent/llm"
)

// Strict tool limits of the Messages API. The API enforces two limits on
// tools sent with strict: true: a count limit and a size limit on the grammar
// compiled from their schemas. A live bisect (TestLiveStrictToolCap in
// internal/agent, 2026-10-12, the full registry plus the room tools) accepted
// 0–16 strict tools, rejected 20 with "The compiled grammar is too large" and
// 21 with "Too many strict tools". Eight sits well inside the accepted range;
// MaxStrictSchemaBytes guards the grammar size as schemas grow. They are the
// adapter's Capabilities; the agent picks which tools take the slots.
const (
	// MaxStrictTools caps the tools sent with strict: true.
	MaxStrictTools = 8
	// MaxStrictSchemaBytes caps the summed size of the marshalled
	// input_schema of every strict tool. Bytes are only a proxy for the
	// compiled grammar size the API limits, but they grow with it. The budget
	// sits just under the largest sum the API is known to accept (5484 bytes,
	// internal/agent liveAcceptedSchemaBytes).
	MaxStrictSchemaBytes = 5400
)

// contextTokens is the context window assumed for every Claude model. It is
// informational on this adapter: the server compacts (Capabilities.Compaction),
// so the agent never compacts a Claude conversation itself.
const contextTokens = 200_000

// toolNameMax is the longest tool name the Messages API accepts.
const toolNameMax = 64

// Turner sends one Messages API request and returns the whole assistant
// message. The production Turner streams (New); tests and the load-test fake
// can supply their own (NewWithTurner). A Turner that fails after part of the
// answer arrived returns the partial message with the error, so its usage is
// still counted.
type Turner interface {
	Turn(ctx context.Context, params sdk.BetaMessageNewParams) (sdk.BetaMessage, error)
}

// Model is the Claude adapter. It is safe for concurrent use.
type Model struct {
	provider string
	turner   Turner
	log      *slog.Logger
}

var _ llm.Model = (*Model)(nil)

// NewWithTurner returns the adapter for provider (one of llm.ProviderAnthropic,
// llm.ProviderBedrock, llm.ProviderVertex) on t. A nil log means
// slog.Default().
func NewWithTurner(provider string, t Turner, log *slog.Logger) *Model {
	if log == nil {
		log = slog.Default()
	}
	return &Model{provider: provider, turner: t, log: log}
}

// Provider reports the provider this adapter was built for.
func (m *Model) Provider() string { return m.provider }

// Capabilities is the same for every Claude model the loop can run: a model
// that cannot take the loop's request (no adaptive thinking or effort) is
// caught by the agent's triage fallback, not here.
func (m *Model) Capabilities(string) llm.Capabilities {
	return llm.Capabilities{
		ToolCalls:         true,
		ParallelToolCalls: true,
		StrictTools:       true,
		MaxStrictTools:    MaxStrictTools,
		MaxStrictBytes:    MaxStrictSchemaBytes,
		StructuredOutput:  true,
		PromptCache:       true,
		Reasoning:         true,
		Effort:            true,
		Compaction:        true,
		ContextTokens:     contextTokens,
		ToolNameMax:       toolNameMax,
	}
}

// Turn encodes req as a Beta Messages request, sends it and decodes the
// answer. A failed request returns an *llm.APIError, with the partial answer
// when one arrived (its usage was billed).
func (m *Model) Turn(ctx context.Context, req llm.Request) (llm.Response, error) {
	params, err := EncodeRequest(req)
	if err != nil {
		return llm.Response{}, &llm.APIError{Provider: m.provider, Kind: llm.ErrBadRequest, Err: err}
	}
	msg, err := m.turner.Turn(ctx, params)
	resp := DecodeMessage(m.provider, req.Model, msg)
	if err != nil {
		return resp, ClassifyError(m.provider, err, m.log)
	}
	return resp, nil
}
