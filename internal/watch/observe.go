package watch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

var (
	errNoConnect = errors.New("watch: no Kafka Connect client configured")
	errNoSource  = errors.New("watch: no source database client configured")
	// errNotObserved: the pipeline's client set was not read in this tick.
	errNotObserved = errors.New("watch: the pipeline's client set was not read")
)

// target is one pipeline the watcher looks at in a tick.
type target struct {
	p   owners.Pipeline
	cfg owners.WatchConfig
	// connectors are the pipeline's connectors, filtered through the tool scope
	// rules: object names come only from owners.Pipeline.
	connectors []string
	// set names the pipeline's client set; src and conn are its clients
	// (Deps.ClientsFor), or Deps.Source and Deps.Connect for every pipeline.
	set  string
	src  SourceDB
	conn ConnectClient
}

// connObs is what one tick learned about one connector.
type connObs struct {
	status    clients.ConnectorStatus
	statusErr error
	cfg       map[string]string
	cfgErr    error
	cfgTried  bool
}

// observation is the raw material of one tick.
type observation struct {
	slots      []clients.ReplicationSlot
	slotsErr   error
	slotsTried bool
	conns      map[string]*connObs
}

// targets lists the pipelines to watch, in owners order. A pipeline that opted
// out, or whose signals are all off, is left out.
func (w *Watcher) targets(ctx context.Context) []target {
	var out []target
	for _, p := range w.own.Pipelines {
		cfg := p.Watch()
		if !cfg.Enabled || !(cfg.ConnectorDown.Enabled || cfg.SlotInactive.Enabled || cfg.SlotWAL.Enabled) {
			continue
		}
		t := target{p: p, cfg: cfg}
		// ScopeFor with the pipeline in the context is the same rule the read
		// tools use (F11); the names below can only be this pipeline's.
		sc, err := tools.ScopeFor(tools.WithPipeline(ctx, p), w.own)
		if err != nil {
			w.log.Warn("watch: cannot scope pipeline", "pipeline", p.ID, "err", err)
			continue
		}
		t.connectors = sc.FilterConnectors(p.Connectors)
		if w.clientsFor != nil {
			pc := w.clientsFor(p.ID)
			t.set, t.src, t.conn = pc.Set, pc.Source, pc.Connect
		} else {
			t.src, t.conn = w.src, w.conn
		}
		out = append(out, t)
	}
	return out
}

// needsSlots reports whether any target reads the replication slots.
func needsSlots(ts []target) bool {
	for _, t := range ts {
		if t.cfg.SlotInactive.Enabled || t.cfg.SlotWAL.Enabled {
			return true
		}
	}
	return false
}

// observe makes every call of the tick, one observation per client set (a
// connector name or a slot in one set says nothing about another set's
// cluster). The sets are read at the same time.
func (w *Watcher) observe(ctx context.Context, ts []target) map[string]*observation {
	groups := map[string][]target{}
	for _, t := range ts {
		groups[t.set] = append(groups[t.set], t)
	}
	out := make(map[string]*observation, len(groups))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for set, g := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Every target of a set has that set's clients.
			o := w.observeSet(ctx, g, g[0].src, g[0].conn)
			mu.Lock()
			out[set] = o
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

// observeSet makes the calls of one client set. The source is read once and the
// rows are fanned out to the pipelines later; each connector is read in its own
// goroutine. Every call has its own deadline and its own error, so one slow or
// broken layer never holds up or hides another (same shape as devctl's
// runHealth).
func (w *Watcher) observeSet(ctx context.Context, ts []target, src SourceDB, conn ConnectClient) *observation {
	obs := &observation{conns: map[string]*connObs{}}
	needCfg := map[string]bool{}
	for _, t := range ts {
		slotCfg := t.cfg.SlotInactive.Enabled || t.cfg.SlotWAL.Enabled
		for _, c := range t.connectors {
			if _, ok := obs.conns[c]; !ok {
				obs.conns[c] = &connObs{}
			}
			if slotCfg {
				needCfg[c] = true // slot name and heartbeat come from the config
			}
		}
	}

	var wg sync.WaitGroup
	if needsSlots(ts) && src == nil {
		obs.slotsErr = errNoSource
	} else if needsSlots(ts) {
		obs.slotsTried = true
		wg.Add(1)
		go func() {
			defer wg.Done()
			obs.slots, obs.slotsErr = call(ctx, w.cfg.CheckTimeout, "read replication slots", src.ReplicationSlots)
		}()
	}
	if conn == nil {
		for _, co := range obs.conns {
			co.statusErr = errNoConnect
		}
	} else {
		for name, co := range obs.conns {
			wg.Add(1)
			go func() {
				defer wg.Done()
				co.status, co.statusErr = call(ctx, w.cfg.CheckTimeout, "connector status", func(c context.Context) (clients.ConnectorStatus, error) {
					return conn.Status(c, name)
				})
				if co.statusErr != nil || !needCfg[name] {
					return
				}
				co.cfgTried = true
				co.cfg, co.cfgErr = call(ctx, w.cfg.CheckTimeout, "connector config", func(c context.Context) (map[string]string, error) {
					return conn.Config(c, name)
				})
			}()
		}
	}
	wg.Wait()
	return obs
}

// call runs fn under its own deadline. fn runs in its own goroutine, so a client
// that ignores its context cannot hold the tick past the deadline; the goroutine
// then ends whenever fn returns (the result channel is buffered).
func call[T any](parent context.Context, timeout time.Duration, what string, fn func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	type outcome struct {
		v   T
		err error
	}
	ch := make(chan outcome, 1)
	go func() {
		v, err := fn(ctx)
		ch <- outcome{v, err}
	}()
	select {
	case o := <-ch:
		return o.v, o.err
	case <-ctx.Done():
		var zero T
		return zero, fmt.Errorf("watch: %s: %w", what, ctx.Err())
	}
}
