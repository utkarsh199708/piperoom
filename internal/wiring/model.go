package wiring

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/agent/llm"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
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

		NeutraliseControlTokens: cfg.ModelNeutraliseControlTokens,
	}
}

// EndpointOption changes the endpoint settings NewAgentEndpoint builds from
// config.Config, after they are filled and before the endpoint is built.
type EndpointOption func(*agent.EndpointConfig)

// WithModelWrap sets agent.EndpointConfig.WrapModel: wrap sees each role's
// client once (the evals in-process model tap). It must return a model that
// behaves exactly like the one it is given.
func WithModelWrap(wrap func(role string, m llm.Model) llm.Model) EndpointOption {
	return func(c *agent.EndpointConfig) { c.WrapModel = wrap }
}

// WithGetenv sets agent.EndpointConfig.Getenv: the model keys are read
// through getenv instead of os.Getenv (the standalone picker's key). The
// rest of cfg is already parsed; this changes key lookups only.
func WithGetenv(getenv func(string) string) EndpointOption {
	return func(c *agent.EndpointConfig) { c.Getenv = getenv }
}

// NewAgentEndpointWithEnv is NewAgentEndpoint with the model keys read
// through getenv (nil = os.Getenv): NewAgentEndpointWith(ctx, cfg, log,
// awsCfg, WithGetenv(getenv)).
func NewAgentEndpointWithEnv(ctx context.Context, cfg config.Config, log *slog.Logger, awsCfg *aws.Config, getenv func(string) string) (*agent.Endpoint, error) {
	return NewAgentEndpointWith(ctx, cfg, log, awsCfg, WithGetenv(getenv))
}

// NewAgentEndpoint builds the model endpoint shared by cmd/server and
// cmd/investigate: provider client, triage model and per-room token budgets. A
// bad provider, a missing region or project, or a bad token budget is an error
// here, so the process refuses to start. awsCfg is the shared config from
// LoadAWS, also passed to Open; nil keeps the endpoint's own AWS loader.
//
// Its signature stays fixed because cmd/server passes it as a function value
// (wireAgent); options go through NewAgentEndpointWith.
func NewAgentEndpoint(ctx context.Context, cfg config.Config, log *slog.Logger, awsCfg *aws.Config) (*agent.Endpoint, error) {
	return NewAgentEndpointWith(ctx, cfg, log, awsCfg)
}

// NewAgentEndpointWith is NewAgentEndpoint with options (WithGetenv,
// WithModelWrap), applied in order after the settings are filled from cfg
// and before the endpoint is built.
func NewAgentEndpointWith(ctx context.Context, cfg config.Config, log *slog.Logger, awsCfg *aws.Config, opts ...EndpointOption) (*agent.Endpoint, error) {
	if log == nil {
		log = slog.Default()
	}
	ec := EndpointConfig(cfg)
	ec.AWSConfig = awsCfg
	tp, err := config.ParseTriagePass(cfg)
	if err != nil {
		return nil, fmt.Errorf("wiring: model endpoint: %w", err)
	}
	ec.TriagePass, ec.TriagePassModel, ec.TriagePassTimeout = tp.On, tp.Model, tp.Timeout
	// The roles and knobs of design section 9. A Claude deployment that sets
	// none of them parses to today's values, and nothing below changes for it.
	knobs, err := config.ParseModelKnobs(cfg)
	if err != nil {
		return nil, fmt.Errorf("wiring: model endpoint: %w", err)
	}
	for _, w := range knobs.Warnings {
		log.Warn(w)
	}
	ec.Knobs = &knobs
	ec.MaxConcurrency = modelConcurrency(knobs, cfg.MaxAgents)
	for _, o := range opts {
		if o != nil {
			o(&ec)
		}
	}
	ep, err := agent.NewEndpoint(ctx, ec, log)
	if err != nil {
		return nil, fmt.Errorf("wiring: model endpoint: %w", err)
	}
	if ep.NeedsStartChecks() {
		sets, err := startToolSets()
		if err != nil {
			return nil, fmt.Errorf("wiring: model endpoint: %w", err)
		}
		if err := ep.CheckStart(ctx, sets); err != nil {
			return nil, fmt.Errorf("wiring: model endpoint: %w", err)
		}
	}
	if ep.Provider() == agent.ProviderBedrock {
		warnIfNotBedrockModelID(log, "PIPEROOM_MODEL", cfg.Model)
		if cfg.TriageModel != "" {
			warnIfNotBedrockModelID(log, "PIPEROOM_TRIAGE_MODEL", cfg.TriageModel)
		}
	}
	if cfg.TriageModel != "" {
		if why := agent.TriageModelWarning(cfg.TriageModel); why != "" {
			log.Warn("PIPEROOM_TRIAGE_MODEL is known not to work with the investigation loop", "value", cfg.TriageModel, "why", why)
		}
	}
	if tp.On {
		if ep.Provider() == agent.ProviderBedrock {
			warnIfNotBedrockModelID(log, "PIPEROOM_TRIAGE_PASS_MODEL", tp.Model)
		}
		if why := agent.TriageModelWarning(tp.Model); why != "" {
			log.Warn("PIPEROOM_TRIAGE_PASS_MODEL is known not to work", "value", tp.Model, "why", why)
		}
	}
	limit := "no cap"
	if n := ep.TokenLimit(); n > 0 {
		limit = fmt.Sprint(n)
	}
	log.Info("model endpoint ready", "provider", ep.Provider(), "model", cfg.Model, "triage_model", cfg.TriageModel,
		"first_look", tp.On, "first_look_model", tp.Model, "first_look_timeout", tp.Timeout.String(), "room_token_budget", limit)
	return ep, nil
}

// defaultMaxAgents mirrors room.DefaultMaxAgents (room imports wiring, so
// wiring cannot import it): the default of PIPEROOM_MAX_AGENTS.
const defaultMaxAgents = 3

// modelConcurrency is PIPEROOM_MODEL_MAX_CONCURRENCY, else
// PIPEROOM_MAX_AGENTS (design 9). cmd/server validates PIPEROOM_MAX_AGENTS
// itself; a value it refuses never reaches a running endpoint, so here an
// unreadable one falls back to the default.
func modelConcurrency(k config.ModelKnobs, maxAgents string) int64 {
	if k.MaxConcurrency > 0 {
		return k.MaxConcurrency
	}
	if n, err := strconv.ParseInt(strings.TrimSpace(maxAgents), 10, 64); err == nil && n > 0 {
		return n
	}
	return defaultMaxAgents
}

// startToolSets are the tools and actions a room of each pipeline kind is
// offered, registered without clients (no client is opened, nothing is
// dialled): what the start checks measure the context fit and the
// adapters' tool limits on. Write actions are those of write mode on, the
// larger list.
func startToolSets() ([]agent.StartToolSet, error) {
	var out []agent.StartToolSet
	for _, p := range []owners.Pipeline{
		{ID: "start-check-cdc", Kind: events.PipelineKindCDC, TopicPrefix: "pg", SourceTables: []string{"public.t"}, Connectors: []string{"c"}},
		{ID: "start-check-batch", Kind: events.PipelineKindBatch, DAGs: []string{"d"}, Models: []string{"m"}},
	} {
		reg, areg := tools.NewReadOnlyRegistry(), actions.NewRegistry()
		if err := RegisterTools(reg, areg, &Wired{Owners: &owners.Owners{Pipelines: []owners.Pipeline{p}}}); err != nil {
			return nil, fmt.Errorf("start checks: register the %s tools: %w", p.Kind, err)
		}
		out = append(out, agent.StartToolSet{Kind: p.Kind, Tools: reg, Actions: areg})
	}
	return out, nil
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
