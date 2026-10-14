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
	// cdcTopics are the pipeline's CDC topics: never a DLQ.
	cdcTopics []string
	// set names the pipeline's client set; src, conn and kafka are its
	// clients (Deps.ClientsFor), or Deps.Source, Deps.Connect and Deps.Kafka
	// for every pipeline.
	set   string
	src   SourceDB
	conn  ConnectClient
	kafka clients.DLQReader
	// reg and ddl feed the change feed; subjects are the pipeline's Schema
	// Registry subjects, derived from its topics through the tool scope.
	reg      clients.SchemaRegistryReader
	ddl      DDLReader
	subjects []string
	lag      LagReader
	// groups are the pipeline's consumer groups, filtered through the scope.
	groups []string
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

	// chg is the change feed's read of the set (nil when the feed is off).
	chg *setChanges
	// Consumer groups: one GroupLag per group of the set, then one offsets
	// call for the log start of the CDC topics they read (lag_signals.go).
	lags          map[string]*lagObs
	lagNoReader   bool
	logStart      map[string][]clients.PartitionOffsets
	logStartErr   error
	logStartTried bool
}

// targets lists the pipelines to watch, in owners order. A pipeline that opted
// out, or whose signals are all off, is left out.
func (w *Watcher) targets(ctx context.Context) []target {
	var out []target
	for _, p := range w.own.Pipelines {
		cfg := p.Watch()
		if !cfg.Enabled || !(cfg.ConnectorDown.Enabled || cfg.SlotInactive.Enabled || cfg.SlotWAL.Enabled ||
			cfg.DLQGrowth.Enabled || cfg.ConfigRisk.Enabled || cfg.ConsumerLag.Enabled || cfg.GroupUnstable.Enabled ||
			cfg.OffsetBelowLogStart.Enabled) {
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
		for _, topic := range t.cdcTopics {
			t.subjects = append(t.subjects, topic+"-key", topic+"-value")
		}
		t.groups = sc.FilterGroups(p.ConsumerGroups)
		if w.clientsFor != nil {
			pc := w.clientsFor(p.ID)
			t.set, t.src, t.conn, t.kafka, t.lag = pc.Set, pc.Source, pc.Connect, pc.Kafka, pc.Lag
			t.reg, t.ddl = pc.Registry, pc.DDL
		} else {
			t.src, t.conn, t.kafka, t.lag = w.src, w.conn, w.kafka, w.lag
			t.reg, t.ddl = w.registry, w.ddl
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
			o := w.observeSet(ctx, set, g, g[0].src, g[0].conn, g[0].kafka, g[0].lag)
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
func (w *Watcher) observeSet(ctx context.Context, set string, ts []target, src SourceDB, conn ConnectClient, kafka clients.DLQReader, lr LagReader) *observation {
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
			if slotCfg || w.changes != nil {
				needCfg[c] = true // slot name and heartbeat come from the config
			}
		}
	}

	var wg sync.WaitGroup
	// Group reads need nothing from the configs, so they run alongside.
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.observeLag(ctx, ts, obs, lr, kafka)
	}()
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
	// After the configs: the DLQ topics are discovered from them.
	w.observeDLQ(ctx, ts, obs, kafka)
	if w.changes != nil {
		obs.chg = w.readChanges(ctx, ts)
	}
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
