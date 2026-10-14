package watch

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	// cdcTopics are the pipeline's CDC topics: never a DLQ.
	cdcTopics []string
	// set names the pipeline's client set; src, conn and kafka are its
	// clients (Deps.ClientsFor), or Deps.Source, Deps.Connect and Deps.Kafka
	// for every pipeline.
	set   string
	src   SourceDB
	conn  ConnectClient
	kafka clients.DLQReader
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
	// set is the client set this observation read (target.set).
	set        string
	slots      []clients.ReplicationSlot
	slotsErr   error
	slotsTried bool
	conns      map[string]*connObs

	// DLQ offsets: one call per tick for all DLQ topics of the set's pipelines.
	// plan says which topics each pipeline looks at; dlqNoReader means the
	// Kafka client cannot read offsets of DLQ topics.
	plan        *dlqPlan
	dlqOffsets  map[string][]clients.PartitionOffsets
	dlqErr      error
	dlqTried    bool
	dlqNoReader bool
}

// targets lists the pipelines to watch, in owners order. A pipeline that opted
// out, or whose signals are all off, is left out.
func (w *Watcher) targets(ctx context.Context) []target {
	var out []target
	for _, p := range w.own.Pipelines {
		cfg := p.Watch()
		if !cfg.Enabled || !(cfg.ConnectorDown.Enabled || cfg.SlotInactive.Enabled || cfg.SlotWAL.Enabled ||
			cfg.DLQGrowth.Enabled || cfg.ConfigRisk.Enabled) {
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
		t.cdcTopics = sc.Topics()
		if w.clientsFor != nil {
			pc := w.clientsFor(p.ID)
			t.set, t.src, t.conn, t.kafka = pc.Set, pc.Source, pc.Connect, pc.Kafka
		} else {
			t.src, t.conn, t.kafka = w.src, w.conn, w.kafka
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
			o := w.observeSet(ctx, set, g, g[0].src, g[0].conn, g[0].kafka)
			mu.Lock()
			out[set] = o
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

// observeSet makes the calls of one client set. The source is read once and the
// rows are fanned out to the pipelines later; each connector's status and
// config are read in their own goroutines, and the DLQ offsets of the topics
// expected from the last tick are read at the same time. Every call has its
// own deadline and its own error, so one slow or broken layer never holds up
// or hides another (same shape as devctl's runHealth), and a tick takes about
// one CheckTimeout, not one per layer.
func (w *Watcher) observeSet(ctx context.Context, set string, ts []target, src SourceDB, conn ConnectClient, kafka clients.DLQReader) *observation {
	obs := &observation{set: set, conns: map[string]*connObs{}}
	needCfg := map[string]bool{}
	for _, t := range ts {
		// The slot name, the heartbeat, the DLQ topic and the risks all come from the config.
		slotCfg := t.cfg.SlotInactive.Enabled || t.cfg.SlotWAL.Enabled ||
			t.cfg.DLQGrowth.Enabled || t.cfg.ConfigRisk.Enabled
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
			}()
			if !needCfg[name] {
				continue
			}
			co.cfgTried = true
			wg.Add(1)
			go func() {
				defer wg.Done()
				co.cfg, co.cfgErr = call(ctx, w.cfg.CheckTimeout, "connector config", func(c context.Context) (map[string]string, error) {
					return conn.Config(c, name)
				})
			}()
		}
	}
	// The DLQ topics come from the configs, which are being read right now; the
	// topics found last tick are almost always the same, so read those now.
	spec := w.predictDLQ(ts)
	var specRes map[string][]clients.PartitionOffsets
	var specErr error
	if kafka != nil && len(spec) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			specRes, specErr = call(ctx, w.cfg.CheckTimeout, "read DLQ offsets", func(c context.Context) (map[string][]clients.PartitionOffsets, error) {
				return kafka.DLQOffsets(c, spec...)
			})
		}()
	}
	wg.Wait()
	for _, co := range obs.conns {
		// A config read only counts next to a good status, as when it was read
		// after it: a status error already says what is wrong with the connector.
		if co.statusErr != nil {
			co.cfg, co.cfgErr, co.cfgTried = nil, nil, false
		}
	}
	w.observeDLQ(ctx, ts, obs, kafka, dlqPrefetch{topics: spec, res: specRes, err: specErr})
	return obs
}

// dlqPrefetch is a DLQOffsets call made before the configs were known.
type dlqPrefetch struct {
	topics []string // sorted
	res    map[string][]clients.PartitionOffsets
	err    error
}

// covers reports whether the prefetch read every topic of want.
func (p dlqPrefetch) covers(want []string) bool {
	if len(p.topics) == 0 {
		return false
	}
	for _, t := range want {
		if _, ok := slices.BinarySearch(p.topics, t); !ok {
			return false
		}
	}
	return true
}

// predictDLQ is the DLQ topic list the plan will most likely ask for: the
// declared topics plus the topics the connectors' configs named last time.
func (w *Watcher) predictDLQ(ts []target) []string {
	want := map[string]bool{}
	w.mu.Lock()
	for _, t := range ts {
		if !t.cfg.DLQGrowth.Enabled && !t.cfg.ConfigRisk.Enabled {
			continue
		}
		mine := map[string]bool{}
		for _, d := range t.cfg.DLQTopics {
			mine[d.Topic] = true
		}
		for _, c := range t.connectors {
			if s, ok := w.dlqSeen[t.p.ID+"/"+c]; ok && s.ec.Topic != "" {
				mine[s.ec.Topic] = true
			}
		}
		for _, c := range t.cdcTopics {
			delete(mine, c)
		}
		for tp := range mine {
			want[tp] = true
		}
	}
	w.mu.Unlock()
	out := make([]string, 0, len(want))
	for t := range want {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
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
