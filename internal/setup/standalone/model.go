package standalone

// The standalone server's model endpoint (V7, KAN-525): the endpoint built at
// start from .piperoom/model.env's stored key and the one each PUT
// /api/models installs. Every key is read through quickstart.Keys
// (agent.EndpointConfig.Getenv), never os.Setenv: internal/setup/ops starts
// docker children that inherit the process environment, so a key put there
// would leak to them (V18 MP1).

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/quickstart"
	"piperoom/internal/wiring"
)

// errNoModel is what a room's agent gets while no model is configured.
var errNoModel = errors.New("setup: no model is configured yet: choose one in the web (the model picker, PUT /api/models)")

// modelHost is the model endpoint of the standalone server. The picker
// replaces the current endpoint while rooms run: a run or hand-off takes the
// endpoint current when it starts and keeps it to the end, so a swap never
// changes the model under a running request.
type modelHost struct {
	cur atomic.Pointer[modelEndpoint]
	// build makes a candidate endpoint for the picker (quickstart.BuildFunc).
	build quickstart.BuildFunc
	// startKeys are the picker's keys at start (model.env's stored key over
	// the process environment): the start endpoint reads its key through
	// them, so the stored key never enters the process environment.
	startKeys quickstart.Keys
}

type modelEndpoint struct {
	ep *agent.Endpoint
	// model is PIPEROOM_MODEL of the endpoint's config: the room's agent
	// options are built from the start config, so a run on a newer endpoint
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
func (e *modelEndpoint) acquire() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.done {
		return false
	}
	e.refs++
	return true
}

// release ends a call; the last call on a replaced endpoint cancels it.
func (e *modelEndpoint) release() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.refs--
	e.endLocked()
}

// retire marks e replaced; with no call running it is cancelled now.
func (e *modelEndpoint) retire() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.retired = true
	e.endLocked()
}

func (e *modelEndpoint) endLocked() {
	if e.retired && e.refs == 0 && !e.done {
		e.done = true
		if e.cancel != nil {
			e.cancel()
		}
	}
}

// install makes ep current. The endpoint it replaces keeps its context until
// the runs and hand-offs that started on it end.
func (h *modelHost) install(ep *agent.Endpoint, model string, cancel context.CancelFunc) {
	if prev := h.cur.Swap(&modelEndpoint{ep: ep, model: model, cancel: cancel}); prev != nil {
		prev.retire()
	}
}

// acquire returns the current endpoint with a call counted on it (release it
// when done), nil when none is installed.
func (h *modelHost) acquire() *modelEndpoint {
	for {
		c := h.cur.Load()
		if c == nil || c.acquire() {
			return c
		}
		// Replaced and cancelled between the load and the count: the newer
		// endpoint is installed already.
	}
}

// ready reports an endpoint is installed.
func (h *modelHost) ready() bool { return h.cur.Load() != nil }

// Run is agent.Runner.
func (h *modelHost) Run(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
	c := h.acquire()
	if c == nil {
		return errNoModel
	}
	defer c.release()
	o.Model = c.model
	return c.ep.Run(ctx, a, d, o)
}

// WriteHandoff is agent.HandoffWriter.
func (h *modelHost) WriteHandoff(ctx context.Context, evs []events.Event, o agent.Options) (events.HandoffSummary, error) {
	c := h.acquire()
	if c == nil {
		return events.HandoffSummary{}, errNoModel
	}
	defer c.release()
	o.Model = c.model
	return c.ep.WriteHandoff(ctx, evs, o)
}

// Forget drops a closed room's token count on the current endpoint.
func (h *modelHost) Forget(roomID string) {
	if c := h.cur.Load(); c != nil {
		c.ep.Forget(roomID)
	}
}

// wire builds the start endpoint on the stored key and sets h.build for the
// picker's later PUTs. A model that does not build is not a refused start in
// standalone mode: the server starts in the no model state and the web opens
// the picker, exactly as the quickstart and the full server do.
func (h *modelHost) wire(ctx context.Context, log *slog.Logger, cfg config.Config, prices agent.Prices) {
	if log == nil {
		log = slog.Default()
	}
	h.build = func(reqCtx context.Context, cfg config.Config, getenv func(string) string) (func(), func(), error) {
		bctx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(reqCtx, cancel)
		// getenv is the picker's overlay: the request's keys on a PUT, the
		// stored key at start. awsCfg is nil so a bedrock endpoint loads its
		// own credential chain, as it did before the picker existed.
		ep, err := wiring.NewAgentEndpointWithEnv(bctx, cfg, log, nil, getenv)
		if !stop() && err == nil {
			// The request ended during the build, which cancelled bctx.
			err = reqCtx.Err()
		}
		if err == nil {
			// Prices alone: standalone keeps no pipeline daily caps, so no
			// ledger; usage events still carry the cost.
			err = ep.SetSpend(agent.SpendOptions{Prices: prices})
		}
		if err != nil {
			cancel()
			return nil, nil, err
		}
		return func() { h.install(ep, cfg.Model, cancel) }, cancel, nil
	}
	if commit, _, err := h.build(ctx, cfg, h.startKeys.Getenv); err != nil {
		// The endpoint's errors name the variable to set, never a key.
		log.Warn("setup: no usable model: the standalone server starts without one; choose a model in the web", "reason", err.Error())
	} else {
		commit()
	}
}
