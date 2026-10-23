package agent

import (
	"context"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/agent/llm"
	anthropicllm "piperoom/internal/agent/llm/anthropic"
)

// sdkModel is the model seam as it was before the provider-neutral interface
// (V1a): the Messages API request in, the accumulated message out. The tests'
// scripted fakes implement it. viaSDK puts one behind the Claude adapter, so
// every request a fake records is the one the adapter encodes and every
// message it scripts goes through the adapter's decoder, exactly as a
// streamed answer does in production.
type sdkModel interface {
	Turn(ctx context.Context, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error)
}

// viaSDK is the Claude adapter (provider anthropic) on a scripted fake.
func viaSDK(m sdkModel) model {
	return anthropicllm.NewWithTurner(ProviderAnthropic, m, discardLogger())
}

// sdkRequest is req as the Claude adapter encodes it.
func sdkRequest(t *testing.T, req llm.Request) anthropic.BetaMessageNewParams {
	t.Helper()
	p, err := anthropicllm.EncodeRequest(req)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	return p
}

// sdkUsage is the usage of a scripted message as the Claude adapter decodes
// it.
func sdkUsage(m anthropic.BetaMessage) llm.Usage {
	return anthropicllm.DecodeMessage(ProviderAnthropic, "", m).Usage
}

// wire is the tool list as the Claude adapter sends it in a loop request:
// every tool, with the cache breakpoint after the last one.
func (tl toolList) wire() []anthropic.BetaToolUnionParam {
	return anthropicllm.EncodeTools(tl.defs, true)
}
