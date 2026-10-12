package wiring

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"piperoom/internal/agent"
	"piperoom/internal/config"
)

// EndpointConfig maps the model settings of cfg onto the agent package's
// endpoint settings. It is a plain copy so tests can check the mapping.
func EndpointConfig(cfg config.Config) agent.EndpointConfig {
	return agent.EndpointConfig{
		Provider:        cfg.ModelProvider,
		AWSRegion:       cfg.AWSRegion,
		VertexProject:   cfg.VertexProject,
		VertexRegion:    cfg.VertexRegion,
		TriageModel:     cfg.TriageModel,
		RoomTokenBudget: cfg.RoomTokenBudget,
	}
}

// NewAgentEndpoint builds the model endpoint shared by cmd/server and
// cmd/investigate: provider client, triage model and per-room token budgets. A
// bad provider, a missing region or project, or a bad token budget is an error
// here, so the process refuses to start.
func NewAgentEndpoint(ctx context.Context, cfg config.Config, log *slog.Logger) (*agent.Endpoint, error) {
	ep, err := agent.NewEndpoint(ctx, EndpointConfig(cfg), log)
	if err != nil {
		return nil, fmt.Errorf("wiring: model endpoint: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	if ep.Provider() == agent.ProviderBedrock {
		warnIfNotBedrockModelID(log, "PIPEROOM_MODEL", cfg.Model)
		if cfg.TriageModel != "" {
			warnIfNotBedrockModelID(log, "PIPEROOM_TRIAGE_MODEL", cfg.TriageModel)
		}
	}
	limit := "no cap"
	if n := ep.TokenLimit(); n > 0 {
		limit = fmt.Sprint(n)
	}
	log.Info("model endpoint ready", "provider", ep.Provider(), "model", cfg.Model, "triage_model", cfg.TriageModel, "room_token_budget", limit)
	return ep, nil
}

// warnIfNotBedrockModelID logs when a model name cannot be a Bedrock id. The
// id is passed through unchanged either way; this only catches the default
// first-party name ("claude-opus-5-5") left in place, which Bedrock rejects on
// the first request. Bedrock model and inference-profile ids contain
// "anthropic."; application inference profiles are ARNs.
func warnIfNotBedrockModelID(log *slog.Logger, name, id string) {
	if strings.Contains(id, "anthropic.") || strings.HasPrefix(id, "arn:") {
		return
	}
	log.Warn("model id does not look like a Bedrock model or inference-profile id; set it to the id from your Bedrock console", "setting", name, "value", id)
}
