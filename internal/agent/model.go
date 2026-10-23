package agent

import (
	"log/slog"
	"os"

	"piperoom/internal/agent/llm"
	anthropicllm "piperoom/internal/agent/llm/anthropic"
)

// model is the seam between the agent and the model provider: one request,
// one complete assistant turn (llm.Model). The production implementation is
// the Claude adapter (internal/agent/llm/anthropic), which streams; tests
// wrap a scripted fake in it so their requests and answers go through the
// adapter's encoder and decoder.
type model = llm.Model

// modelTimeout bounds a single model turn (the adapter applies it).
const modelTimeout = anthropicllm.TurnTimeout

// workspaceEnv names the variable holding the Anthropic workspace id; see
// anthropicllm.WorkspaceEnv. It moves into internal/config later.
const workspaceEnv = anthropicllm.WorkspaceEnv

// workspaceHeader is the request header the API reads the workspace id from.
const workspaceHeader = anthropicllm.WorkspaceHeader

// clientOptions returns the SDK request options of the Anthropic API
// provider: the workspace header when ANTHROPIC_WORKSPACE_ID is set.
func clientOptions(getenv func(string) string) []anthropicllm.RequestOption {
	return anthropicllm.WorkspaceOptions(getenv)
}

// newModel builds the production model client. opts.Effort/model selection
// happens when the request is assembled, so the client itself only needs the
// SDK defaults (which read ANTHROPIC_API_KEY from the environment) plus the
// workspace header when ANTHROPIC_WORKSPACE_ID is set. Both the agent loop and
// the hand-off writer build their client here, so both send it.
func newModel(opts Options, log *slog.Logger) model {
	return newModelWith(os.Getenv, log)
}

// newModelWith is newModel with the environment lookup and extra SDK options
// (a test server's base URL and key) injected.
func newModelWith(getenv func(string) string, log *slog.Logger, extra ...anthropicllm.RequestOption) model {
	reqOpts := append(clientOptions(getenv), extra...)
	return anthropicllm.New(ProviderAnthropic, reqOpts, log)
}
