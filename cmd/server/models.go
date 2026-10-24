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
	"sync"
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
	// cancel ends the context the endpoint was built on. Vertex keeps that
	// context for every later token refresh, so it is cancelled only once
	// the endpoint is replaced and its last in-flight call has returned.
	cancel context.CancelFunc

	mu      sync.Mutex
	refs    int  // calls running on this endpoint
	retired bool // replaced by a newer endpoint
	done    bool // cancel called: no new call may start here
}

// acquire counts a call starting on e; false once e's context is cancelled.
func (e *liveEndpoint) acquire() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.done {
		return false
	}
	e.refs++
	return true
}

// release ends a call; the last call on a replaced endpoint cancels it.
func (e *liveEndpoint) release() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.refs--
	e.endLocked()
}

// retire marks e replaced; with no call running it is cancelled now.
func (e *liveEndpoint) retire() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.retired = true
	e.endLocked()
}

func (e *liveEndpoint) endLocked() {
	if e.retired && e.refs == 0 && !e.done {
		e.done = true
		if e.cancel != nil {
			e.cancel()
		}
	}
}

// install makes ep current. The endpoint it replaces keeps its context until
// the runs, hand-offs and first looks that started on it end: cancelling it
// at once would break a long Vertex run's next token refresh.
// A call that never returns therefore keeps the endpoint it started on, and
// that endpoint's context, alive until the server ends: the cost of one
// stale endpoint is accepted over breaking a run mid-way.
func (l *liveModel) install(ep *agent.Endpoint, model string, cancel context.CancelFunc) {
	if prev := l.cur.Swap(&liveEndpoint{ep: ep, model: model, cancel: cancel}); prev != nil {
		prev.retire()
	}
}

// acquire returns the current endpoint with a call counted on it (release
// it when done), nil when none is installed.
func (l *liveModel) acquire() *liveEndpoint {
	for {
		c := l.cur.Load()
		if c == nil || c.acquire() {
			return c
		}
		// Replaced and cancelled between the load and the count: the
		// newer endpoint is installed already.
	}
}

// ready reports an endpoint is installed.
func (l *liveModel) ready() bool { return l.cur.Load() != nil }

// Run is agent.Runner.
func (l *liveModel) Run(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
	c := l.acquire()
	if c == nil {
		return errNoModel
	}
	defer c.release()
	o.Model = c.model
	return c.ep.Run(ctx, a, d, o)
}

// WriteHandoff is agent.HandoffWriter.
func (l *liveModel) WriteHandoff(ctx context.Context, evs []events.Event, o agent.Options) (events.HandoffSummary, error) {
	c := l.acquire()
	if c == nil {
		return events.HandoffSummary{}, errNoModel
	}
	defer c.release()
	o.Model = c.model
	return c.ep.WriteHandoff(ctx, evs, o)
}

// Triage is agent.Triager.
func (l *liveModel) Triage(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) (agent.TriageResult, error) {
	c := l.acquire()
	if c == nil {
		return agent.TriageResult{}, errNoModel
	}
	defer c.release()
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
	// The endpoint lives on the server's context, not the PUT's: Vertex
	// (Claude and Gemini) keeps the build context for later token refreshes,
	// and a request context ends with the response. The request only bounds
	// the build and its probe.
	srvCtx := ctx
	l.build = func(reqCtx context.Context, cfg config.Config) (func(), func(), error) {
		bctx, cancel := context.WithCancel(srvCtx)
		stop := context.AfterFunc(reqCtx, cancel)
		ep, err := newAgentEndpoint(bctx, cfg, in.log, awsCfg)
		if !stop() && err == nil {
			// The request ended during the build, which cancelled bctx.
			err = reqCtx.Err()
		}
		if err == nil {
			err = ep.SetSpend(in.spend.options(in.spendStore, in.usage))
		}
		if err != nil {
			cancel()
			return nil, nil, err
		}
		return func() { l.install(ep, cfg.Model, cancel) }, cancel, nil
	}
	if commit, _, err := l.build(ctx, in.cfg); err != nil {
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
// or quickstart server, the read-only panel elsewhere. --demo gets the
// read-only panel too, so the web shell's GET /api/models never 404s there.
func modelsOption(m serveMode, cfg config.Config, prices agent.Prices, live *liveModel, soloUser string, users *room.Users) (httpapi.Option, error) {
	supported, err := llm.SupportedModels()
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	if live == nil || m.demo {
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
