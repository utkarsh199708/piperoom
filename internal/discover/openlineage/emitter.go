// Package openlineage exports the pipeline map and executed fixes to an
// OpenLineage server (Marquez, DataHub): one RunEvent per topology build, at
// most every 15 minutes per pipeline, and one per action.executed
// (docs/design/lineage.md section 6, X7b).
//
// Events carry names, column lineage, governance tags and owners only: never
// node health or facts, room content, proposal params, an action's output, a
// row value, a config value or a credential. The emitter never blocks a room:
// callers hand it a graph or a step and return; a bounded queue feeds one
// goroutine that stops with its context.
package openlineage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/connprobe"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/topology"
)

// DefaultQueueSize bounds the events waiting to be sent; the oldest is
// dropped when it is full.
const DefaultQueueSize = 64

// actionName is what an actions.Action name looks like; any other text is
// never put in a job name.
var actionName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// Options tune an Emitter. Zero values take the defaults.
type Options struct {
	Logger *slog.Logger
	// Client posts the events; nil is a client that dials through
	// connprobe.Guard (no link-local or cloud-metadata address), uses no
	// proxy and follows no redirect.
	Client *http.Client
	Now    func() time.Time
	// Backoff is the wait before retry n (0-based); nil is 1 s, 2 s, 4 s.
	Backoff func(n int) time.Duration
	// Skip, when it reports true, leaves a pipeline's topology out: a
	// pipeline with its own connections: block has namespaces the
	// process-wide settings do not describe.
	Skip      func(pipelineID string) bool
	QueueSize int
}

// Emitter sends OpenLineage events. A nil *Emitter is off: every method is
// a no-op, so callers need no check.
type Emitter struct {
	cfg  config.OpenLineage
	ns   Namespaces
	o    Options
	wake chan struct{}

	mu      sync.Mutex
	running bool
	last    map[string]time.Time          // pipeline -> last topology event
	known   map[string]map[string]dataset // pipeline -> datasets of its last graph
	queue   []item
	dropped int
}

type item struct {
	pipeline, job string
	ev            runEvent
}

// New returns an emitter for cfg, or nil when cfg.URL is empty (off).
func New(cfg config.OpenLineage, ns Namespaces, o Options) *Emitter {
	if cfg.URL == "" {
		return nil
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Client == nil {
		o.Client = newClient()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Backoff == nil {
		o.Backoff = func(n int) time.Duration { return time.Second << n }
	}
	if o.QueueSize <= 0 {
		o.QueueSize = DefaultQueueSize
	}
	return &Emitter{cfg: cfg, ns: ns, o: o, wake: make(chan struct{}, 1),
		last: map[string]time.Time{}, known: map[string]map[string]dataset{}}
}

// newClient dials only addresses connprobe.Guard allows: loopback and
// private ranges are fine (Marquez usually runs next to the server), cloud
// metadata and link-local are not. No proxy, since the guard would then
// check the proxy; no redirect, so neither the body nor the key goes
// anywhere but OPENLINEAGE_URL.
func newClient() *http.Client {
	guard := &connprobe.Guard{AllowLoopback: true, AllowPrivate: true}
	return &http.Client{
		Timeout: config.OpenLineageTimeout,
		Transport: &http.Transport{
			DialContext:           guard.DialContext,
			ForceAttemptHTTP2:     true,
			TLSHandshakeTimeout:   config.OpenLineageTimeout,
			ResponseHeaderTimeout: config.OpenLineageTimeout,
			MaxIdleConns:          2,
			IdleConnTimeout:       90 * time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// Elector is store.Elector: Run calls fn while this process leads name.
type Elector interface {
	Run(ctx context.Context, name string, fn func(ctx context.Context))
}

// Start reads the OPENLINEAGE_* settings and, when OPENLINEAGE_URL is set,
// runs the emitter on the elected leader only, so N replicas send one event
// per build or step. Off, it starts nothing and returns a nil emitter. stop
// ends the loop and waits for it.
func Start(ctx context.Context, cfg config.Config, own *owners.Owners, el Elector, log *slog.Logger) (*Emitter, func(), error) {
	return start(ctx, cfg, own, el, Options{Logger: log})
}

func start(ctx context.Context, cfg config.Config, own *owners.Owners, el Elector, o Options) (*Emitter, func(), error) {
	ol, err := config.ParseOpenLineage(cfg)
	if err != nil {
		return nil, func() {}, err
	}
	o.Skip = func(id string) bool {
		p, ok := own.Pipeline(id)
		return !ok || p.HasConnections()
	}
	e := New(ol, NamespacesFromConfig(cfg), o)
	if e == nil {
		return nil, func() {}, nil
	}
	// The host only: a token in the URL's query must not reach the log.
	host := ""
	if u, err := url.Parse(ol.URL); err == nil {
		host = u.Host
	}
	e.o.Logger.Info("openlineage: emitter on (leader only)", "host", host, "namespace", ol.Namespace,
		"api_key_set", ol.APIKey != "", "source", e.ns.Source != "", "kafka", e.ns.Kafka != "", "warehouse", e.ns.Warehouse != "")
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		el.Run(ctx, "openlineage", e.Run)
	}()
	return e, func() { cancel(); <-done }, nil
}

// Run sends queued events until ctx ends. Events handed over while no Run
// is active (another replica leads) are not queued.
func (e *Emitter) Run(ctx context.Context) {
	if e == nil {
		return
	}
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.running, e.queue, e.dropped = false, nil, 0
		e.mu.Unlock()
	}()
	for {
		e.mu.Lock()
		var it *item
		dropped := e.dropped
		if len(e.queue) > 0 {
			it = &e.queue[0]
			e.queue = e.queue[1:]
		}
		e.dropped = 0
		e.mu.Unlock()
		if dropped > 0 {
			e.o.Logger.Warn("openlineage: queue full, dropped the oldest events", "dropped", dropped)
		}
		if it != nil {
			e.send(ctx, *it)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-e.wake:
		}
	}
}

func (e *Emitter) enqueue(it item) {
	if len(e.queue) >= e.o.QueueSize {
		e.queue = e.queue[1:]
		e.dropped++
	}
	e.queue = append(e.queue, it)
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// Topology hands over a built graph. The first build of a pipeline after
// config.OpenLineageEmitInterval becomes one event; builds in between only
// refresh the datasets the action events may name.
func (e *Emitter) Topology(g topology.Graph) {
	if e == nil || g.PipelineID == "" || (e.o.Skip != nil && e.o.Skip(g.PipelineID)) {
		return
	}
	now := e.o.Now()
	known := knownDatasets(g, e.ns)
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return
	}
	e.known[g.PipelineID] = known
	t, seen := e.last[g.PipelineID]
	emit := !seen || now.Sub(t) >= config.OpenLineageEmitInterval
	if emit {
		e.last[g.PipelineID] = now
	}
	e.mu.Unlock()
	if !emit {
		return
	}
	// Mapped outside the lock: a wide graph must not hold up other rooms.
	ev := topologyEvent(g, e.ns, e.cfg.Namespace, now)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		e.enqueue(item{pipeline: g.PipelineID, job: ev.Job.Name, ev: ev})
	}
}

// ActionExecuted hands over one executed step; it matches the room's
// OnActionResult hook. res.Output is never read.
func (e *Emitter) ActionExecuted(_ context.Context, pipelineID string, step events.ProposalStep, res events.ActionExecutedPayload) {
	if e == nil || !actionName.MatchString(step.Action) {
		return
	}
	now := e.o.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	ev := actionEvent(pipelineID, step, res, e.known[pipelineID], e.cfg.Namespace, now)
	e.enqueue(item{pipeline: pipelineID, job: ev.Job.Name, ev: ev})
}

// ActionHook returns ActionExecuted as a room hook, or nil when e is off.
func (e *Emitter) ActionHook() func(context.Context, string, events.ProposalStep, events.ActionExecutedPayload) {
	if e == nil {
		return nil
	}
	return e.ActionExecuted
}

// Wrap returns b with every successful Build handed to Topology; off, b
// itself.
func (e *Emitter) Wrap(b topology.Builder) topology.Builder {
	if e == nil || b == nil {
		return b
	}
	return builder{Builder: b, e: e}
}

type builder struct {
	topology.Builder
	e *Emitter
}

func (b builder) Build(ctx context.Context, pipelineID string) (topology.Graph, error) {
	g, err := b.Builder.Build(ctx, pipelineID)
	if err == nil {
		b.e.Topology(g)
	}
	return g, err
}

// send posts one event, retrying a network error, 429 or 5xx
// config.OpenLineageMaxRetries times, then logs one warning and drops it.
func (e *Emitter) send(ctx context.Context, it item) {
	body, err := json.Marshal(it.ev)
	if err == nil && len(body) > config.OpenLineageMaxEventBytes {
		// Too big: the column-level facets go first.
		for _, ds := range [][]dataset{it.ev.Inputs, it.ev.Outputs} {
			for i := range ds {
				ds[i].Facets.Schema, ds[i].Facets.ColumnLineage = nil, nil
			}
		}
		body, err = json.Marshal(it.ev)
	}
	if err != nil || len(body) > config.OpenLineageMaxEventBytes {
		e.o.Logger.Warn("openlineage: event too large, dropped", "pipeline", it.pipeline, "job", it.job,
			"bytes", len(body), "max", config.OpenLineageMaxEventBytes)
		return
	}
	for attempt := 0; ; attempt++ {
		status, err := e.post(ctx, body)
		if err == nil && status >= 200 && status < 300 {
			return
		}
		if ctx.Err() != nil {
			return
		}
		retry := err != nil || status == http.StatusTooManyRequests || status >= 500
		if !retry || attempt >= config.OpenLineageMaxRetries {
			args := []any{"pipeline", it.pipeline, "job", it.job, "status", status, "attempts", attempt + 1}
			if err != nil {
				args = append(args, "error", err.Error())
			}
			e.o.Logger.Warn("openlineage: event not accepted, dropped", args...)
			return
		}
		t := time.NewTimer(e.o.Backoff(attempt))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// post sends body once and returns the status. A response is read to at
// most config.OpenLineageMaxResponseBytes and discarded. The error never
// holds the URL (a query string may carry a token).
func (e *Emitter) post(ctx context.Context, body []byte) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, config.OpenLineageTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return 0, errors.New("openlineage: cannot build the request")
	}
	req.Header.Set("Content-Type", "application/json")
	if e.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+e.cfg.APIKey)
	}
	resp, err := e.o.Client.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return 0, fmt.Errorf("openlineage: post: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, config.OpenLineageMaxResponseBytes))
	return resp.StatusCode, nil
}
