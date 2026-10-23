package wiring

import (
	"fmt"
	"slices"

	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/clients/airflow"
	"piperoom/internal/clients/dbt"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/batchtools"
	"piperoom/internal/tools/changes"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/dbttools"
	"piperoom/internal/tools/debeziumtools"
	"piperoom/internal/tools/kafkatools"
	"piperoom/internal/tools/logtools"
	"piperoom/internal/tools/runbooks"
	"piperoom/internal/tools/similarincidents"
)

// Pipeline kinds in wiring (tasks/P4-pipeline-kinds-batch.md, P4b-1).
//
// A client set opens the clients of the kinds its pipelines have and nothing
// else: a set of batch pipelines only opens no Kafka, Connect, Schema Registry
// or source Postgres client, and a set of CDC pipelines only opens no Airflow
// or dbt reader. The warehouse is opened for both. Its tool packages follow
// the same kinds (toolPackages), and each room then sees only the tools of its
// own kind (agent.ToolsForKind, from Alert.PipelineKind).

// kindSet is the set of pipeline kinds a client set serves.
type kindSet map[string]bool

// kindsOf returns the kinds of o's pipelines. No owners, or owners with no
// pipeline (a Wired built by hand, Open on an empty file), is cdc: every set
// built before kinds existed was a CDC set and keeps its clients and tools.
func kindsOf(o *owners.Owners) kindSet {
	ks := kindSet{}
	if o != nil {
		for i := range o.Pipelines {
			ks[o.Pipelines[i].KindOrDefault()] = true
		}
	}
	if len(ks) == 0 {
		ks[events.PipelineKindCDC] = true
	}
	return ks
}

func (ks kindSet) cdc() bool   { return ks[events.PipelineKindCDC] }
func (ks kindSet) batch() bool { return ks[events.PipelineKindBatch] }

// hasAny reports whether ks holds one of kinds; nil kinds (a package for every
// kind) always matches.
func (ks kindSet) hasAny(kinds []string) bool {
	if kinds == nil {
		return true
	}
	for _, k := range kinds {
		if ks[k] {
			return true
		}
	}
	return false
}

// Kinds returns the pipeline kinds w's set serves, sorted.
func (w *Wired) Kinds() []string {
	var out []string
	for k := range kindsOf(w.Owners) {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// toolPackage is one tool package and the pipeline kinds that need it.
type toolPackage struct {
	name string
	// kinds are the pipeline kinds whose presence in the set registers the
	// package; nil registers it always.
	kinds    []string
	register func(reg *tools.Registry, w *Wired) error
}

var (
	cdcKind   = []string{events.PipelineKindCDC}
	batchKind = []string{events.PipelineKindBatch}
	bothKinds = []string{events.PipelineKindCDC, events.PipelineKindBatch}
)

// toolPackages are the read tool packages in registration order, which is
// the order of the model's tool list: a new package goes where its tools
// should appear, and the CDC order stays as it was before kinds (a batch
// package's tools are not in a CDC room's list at all). The batch packages
// (P4c-2 batchtools, P4d-2 dbttools) come after debezium tools and are
// registered only for a set with a batch pipeline, so a CDC-only set's
// registry, and its rooms' tool list, stay byte-identical. Their readers are
// nil when AIRFLOW_URL or the dbt settings are off; the tools are registered
// anyway (the list does not depend on setup) and answer
// clients.ErrNotSupported.
//
// dbtools serves both kinds: its warehouse tools (freshness is batch
// verification) and pipeline_map are batch tools too. registerToolsWith keeps
// only the tools of the set's kinds (agent.ToolPipelineKinds), so a batch-only
// set gets no pg_*, reconcile or trace_record tool over its absent source.
func toolPackages() []toolPackage {
	return []toolPackage{
		{name: "db tools", kinds: bothKinds, register: func(reg *tools.Registry, w *Wired) error {
			return dbtools.Register(reg, dbtools.Deps{Src: w.Source, Wh: w.Warehouse, WarehouseKind: w.WarehouseKind, Builder: w.Builder, Owners: w.Owners, Connect: readConnect(w), Kafka: w.Kafka})
		}},
		{name: "kafka tools", kinds: cdcKind, register: func(reg *tools.Registry, w *Wired) error {
			// kafka_throughput reads the watcher's group_lag samples from the
			// room store when it has them (X6a); without, it says so.
			samples, _ := w.Rooms.(clients.SampleReader)
			return kafkatools.Register(reg, kafkatools.Deps{Connect: readConnect(w), Registry: readRegistry(w), Kafka: w.Kafka, Owners: w.Owners, Samples: samples,
				ConnectInternalTopics: kafkatools.ParseInternalTopics(w.ConnectInternalTopics)})
		}},
		{name: "debezium tools", kinds: cdcKind, register: func(reg *tools.Registry, w *Wired) error {
			// The source's row estimates give debezium_snapshot_progress its ETA (X6b).
			rows, _ := w.Source.(debeziumtools.RowEstimator)
			d := debeziumtools.Deps{Connect: readConnect(w), Metrics: w.DebeziumMetrics, Owners: w.Owners, Rows: rows}
			if err := debeziumtools.Register(reg, d); err != nil {
				return err
			}
			// connect_worker_metrics (Q3b) reads the same exporter endpoint;
			// without DEBEZIUM_METRICS_URL it answers "not configured".
			return debeziumtools.RegisterWorkerMetrics(reg, d, w.ConnectMetricsPrefix)
		}},
		// connect_worker_logs (Q3c) is registered whether or not a log store
		// is set, so the tool list does not depend on setup; without one it
		// answers "not configured". A typed nil reader counts as none.
		{name: "worker logs", kinds: cdcKind, register: func(reg *tools.Registry, w *Wired) error {
			d := logtools.Deps{Owners: w.Owners, Now: w.Now}
			if !isNilIface(w.WorkerLogs) {
				d.Logs = w.WorkerLogs
			}
			return logtools.Register(reg, d)
		}},
		// Schedule is left nil: batchtools.Register falls back to the
		// orchestrator's clients.DAGScheduleReader (*airflow.Client), the one
		// copy of that fallback.
		{name: "batch tools", kinds: batchKind, register: func(reg *tools.Registry, w *Wired) error {
			return batchtools.Register(reg, batchtools.Deps{Orchestrator: w.Orchestrator, Owners: w.Owners, Now: w.Now})
		}},
		{name: "dbt tools", kinds: batchKind, register: func(reg *tools.Registry, w *Wired) error {
			return dbttools.Register(reg, dbttools.Deps{Transform: w.Transform, Owners: w.Owners})
		}},
		// The runbook library is embedded in the binary, so it needs no client.
		{name: "runbooks", register: func(reg *tools.Registry, _ *Wired) error {
			return runbooks.Register(reg)
		}},
		// similar_incidents needs the room store, which only the room service
		// owns, so it is registered when the caller provided one.
		{name: "similar_incidents", register: func(reg *tools.Registry, w *Wired) error {
			if w.Rooms == nil {
				return nil
			}
			return similarincidents.Register(reg, similarincidents.Deps{Source: w.Rooms, Owners: w.Owners})
		}},
		// what_changed is always registered: the prompt names it, and the live
		// DDL read works without the change log. The change log is the room
		// store when it implements clients.ChangeReader (F8d); without it the
		// tool says "change log unavailable". A set without a source skips the
		// live DDL read (changes.Deps.Src nil).
		{name: "what_changed", register: func(reg *tools.Registry, w *Wired) error {
			chg := changes.Deps{Src: w.Source, Transform: w.Transform, Owners: w.Owners}
			if cr, ok := w.Rooms.(clients.ChangeReader); ok {
				chg.Changes = cr
			}
			return changes.Register(reg, chg)
		}},
	}
}

// registerToolsWith registers the packages the set's kinds need, each into a
// scratch registry first, then copies into reg only the tools that serve one
// of the set's kinds (agent.ToolServesKind), in package order.
func registerToolsWith(reg *tools.Registry, w *Wired, pkgs []toolPackage) error {
	ks := kindsOf(w.Owners)
	for _, p := range pkgs {
		if !ks.hasAny(p.kinds) {
			continue
		}
		scratch := tools.NewReadOnlyRegistry()
		if err := p.register(scratch, w); err != nil {
			return fmt.Errorf("wiring: register %s: %w", p.name, err)
		}
		for _, t := range scratch.All() {
			if !servesAny(t.Name(), ks) {
				continue
			}
			if err := reg.Register(t); err != nil {
				return fmt.Errorf("wiring: register %s: %w", p.name, err)
			}
		}
	}
	return nil
}

// servesAny reports whether a room of one of ks's kinds offers the tool.
func servesAny(name string, ks kindSet) bool {
	for k := range ks {
		if agent.ToolServesKind(name, k) {
			return true
		}
	}
	return false
}

// batchOpeners build the batch readers; tests replace them so no Airflow or
// dbt is needed. Each returns a nil interface when its settings are off.
type batchOpeners struct {
	airflow func(config.Airflow) (clients.OrchestratorReader, error)
	dbt     func(config.DBT) (clients.TransformReader, error)
}

var defaultBatchOpeners = batchOpeners{
	airflow: func(a config.Airflow) (clients.OrchestratorReader, error) {
		if a.URL == "" {
			return nil, nil
		}
		var opts []airflow.Option
		switch {
		case a.Token != "":
			opts = append(opts, airflow.WithToken(netsec.Secret(a.Token)))
		case a.Username != "":
			opts = append(opts, airflow.WithBasicAuth(a.Username, netsec.Secret(a.Password)))
		}
		c, err := airflow.New(a.URL, opts...)
		if err != nil {
			return nil, err
		}
		return c, nil
	},
	dbt: func(d config.DBT) (clients.TransformReader, error) {
		r, err := dbt.FromConfig(d)
		if err != nil || r == nil {
			// A nil Reader is returned as a nil interface: "off".
			return nil, err
		}
		return r, nil
	},
}

// openBatch builds the batch readers of one set from cfg: the Airflow client
// when AIRFLOW_URL is set and the dbt reader when DBT_ARTIFACTS_DIR or dbt
// Cloud is, each nil otherwise (the tools then answer
// clients.ErrNotSupported). Building does no I/O. Errors name the setting,
// never a secret: config's and the clients' errors quote no password or token.
func openBatch(cfg config.Config, op batchOpeners) (clients.OrchestratorReader, clients.TransformReader, error) {
	af, err := config.ParseAirflow(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("wiring: %w", err)
	}
	dc, err := config.ParseDBT(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("wiring: %w", err)
	}
	orch, err := op.airflow(af)
	if err != nil {
		return nil, nil, fmt.Errorf("wiring: airflow client: %w", err)
	}
	tr, err := op.dbt(dc)
	if err != nil {
		return nil, nil, fmt.Errorf("wiring: dbt reader: %w", err)
	}
	return orch, tr, nil
}
