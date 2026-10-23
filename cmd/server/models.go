package main

// The model routes' server side (V7): the live endpoint a standalone or
// quickstart server swaps when the solo user picks a model, the no model
// start, and the read-only panel of a team server.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/agent"
	"piperoom/internal/agent/llm"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/quickstart"
	"piperoom/internal/room"
)

// errNoModel is what a room's agent gets while no model is configured.
var errNoModel = errors.New("server: no model is configured yet: choose one in the web (the model picker, PUT /api/models)")

// liveModel is the model endpoint of a standalone or quickstart server. The
// picker replaces it while rooms run: a run, hand-off or first look takes the
// endpoint current when it starts and keeps it to the end, so a swap never
// changes the model under a running request.
type liveModel struct {
	cur atomic.Pointer[liveEndpoint]
	// build makes a candidate endpoint for the picker (set by wireAgent,
	// which holds the AWS config and the spend options).
	build quickstart.BuildFunc
}

type liveEndpoint struct {
	ep *agent.Endpoint
	// model is PIPEROOM_MODEL of the endpoint's config: the room's agent
	// options were built from the start config, so a run on a newer endpoint
	// gets its model id from here.
	model string
}

func (l *liveModel) install(ep *agent.Endpoint, model string) {
	l.cur.Store(&liveEndpoint{ep: ep, model: model})
}

// ready reports an endpoint is installed.
func (l *liveModel) ready() bool { return l.cur.Load() != nil }

// Run is agent.Runner.
func (l *liveModel) Run(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
	c := l.cur.Load()
	if c == nil {
		return errNoModel
	}
	o.Model = c.model
	return c.ep.Run(ctx, a, d, o)
}

// WriteHandoff is agent.HandoffWriter.
func (l *liveModel) WriteHandoff(ctx context.Context, evs []events.Event, o agent.Options) (events.HandoffSummary, error) {
	c := l.cur.Load()
	if c == nil {
		return events.HandoffSummary{}, errNoModel
	}
	o.Model = c.model
	return c.ep.WriteHandoff(ctx, evs, o)
}

// Triage is agent.Triager.
func (l *liveModel) Triage(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) (agent.TriageResult, error) {
	c := l.cur.Load()
	if c == nil {
		return agent.TriageResult{}, errNoModel
	}
	o.Model = c.model
	return c.ep.Triage(ctx, a, d, o)
}

// Forget drops a closed room's token count on the current endpoint.
func (l *liveModel) Forget(roomID string) {
	if c := l.cur.Load(); c != nil {
		c.ep.Forget(roomID)
	}
}

// wireLive builds the start endpoint of a standalone or quickstart server.
// A model that does not build is not a refused start there: the server
// starts in the no model state and the web opens the picker.
func wireLive(ctx context.Context, l *liveModel, in wireAgentInput, awsCfg *aws.Config,
	newAgentEndpoint func(ctx context.Context, cfg config.Config, log *slog.Logger, awsCfg *aws.Config) (*agent.Endpoint, error),
) (agentWiring, error) {
	l.build = func(ctx context.Context, cfg config.Config) (func(), error) {
		ep, err := newAgentEndpoint(ctx, cfg, in.log, awsCfg)
		if err != nil {
			return nil, err
		}
		if err := ep.SetSpend(in.spend.options(in.spendStore, in.usage)); err != nil {
			return nil, err
		}
		return func() { l.install(ep, cfg.Model) }, nil
	}
	if commit, err := l.build(ctx, in.cfg); err != nil {
		// The agent's start errors name the variable to set, never a key.
		in.log.Warn("server: no usable model: the server starts without one; choose a model in the web", "reason", err.Error())
	} else {
		commit()
	}
	tp, err := config.ParseTriagePass(in.cfg)
	if err != nil {
		return agentWiring{}, fmt.Errorf("server: %w", err)
	}
	in.hook.setForget(l)
	return agentWiring{run: l.Run, write: l.WriteHandoff, triage: l.Triage, triageTimeout: tp.Timeout}, nil
}

// osEnv is the process environment, which the model adapters read their key
// from (quickstart.Env).
type osEnv struct{}

func (osEnv) Lookup(name string) (string, bool) { return os.LookupEnv(name) }
func (osEnv) Set(name, value string) error      { return os.Setenv(name, value) }
func (osEnv) Unset(name string) error           { return os.Unsetenv(name) }

// setStoredKey hands the adapters the key model.env holds, before the
// endpoint is built.
func setStoredKey(e quickstart.ModelEnv, env quickstart.Env) error {
	if e.KeyVar == "" || e.Key == "" {
		return nil
	}
	if err := env.Set(e.KeyVar, e.Key.Reveal()); err != nil {
		return errors.New("server: model.env: set the stored key in the environment")
	}
	return nil
}

// teamModels is GET /api/models on a team server: the configuration, read
// only. PUT answers 404.
type teamModels struct {
	cfg       config.Config
	prices    agent.Prices
	supported []llm.SupportedModel
}

func (t teamModels) Panel() httpapi.ModelPanel {
	return httpapi.BuildModelPanel(httpapi.PanelInput{Mode: httpapi.ModelModeTeam, State: httpapi.ModelStateReady,
		Config: t.cfg, Prices: t.prices, Supported: t.supported, Now: time.Now(), Getenv: os.Getenv})
}
func (teamModels) Editable() bool   { return false }
func (teamModels) SoloUser() string { return "" }
func (teamModels) Apply(context.Context, httpapi.ModelChoice) (httpapi.ModelPanel, error) {
	return httpapi.ModelPanel{}, errors.New("server: the model is read only on a team server")
}

// modelsOption builds the model routes' option: the picker on a standalone
// or quickstart server, the read-only panel elsewhere; none in --demo.
func modelsOption(m serveMode, cfg config.Config, prices agent.Prices, live *liveModel, soloUser string, users *room.Users) (httpapi.Option, error) {
	if m.demo {
		return nil, nil
	}
	supported, err := llm.SupportedModels()
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	if live == nil {
		return httpapi.WithModels(teamModels{cfg: cfg, prices: prices, supported: supported}), nil
	}
	if soloUser == "" {
		if all := users.All(); len(all) == 1 {
			soloUser = all[0].ID
		}
	}
	return httpapi.WithModels(quickstart.NewModels(quickstart.ModelsOptions{
		Mode:       m.modelMode,
		ProjectDir: m.projectDir,
		Config:     cfg,
		Stored:     m.modelEnv,
		Ready:      live.ready(),
		SoloUser:   soloUser,
		Build:      live.build,
		Env:        osEnv{},
		Panel:      httpapi.PanelInput{Prices: prices, Supported: supported, Getenv: os.Getenv},
	})), nil
}
