// Package debeziumtools registers the read-only tools that read Debezium's
// own metrics (snapshot progress, streaming lag, queue) from the Prometheus
// text endpoint of the JMX exporter next to Kafka Connect.
package debeziumtools

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/prom"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Scraper reads a metrics endpoint; *prom.Client is the production one.
type Scraper interface {
	Scrape(ctx context.Context, url string, maxBytes int64) ([]prom.Sample, error)
}

// Deps are the collaborators of the tool.
type Deps struct {
	// Connect gives the connector's topic.prefix, the only link between a
	// connector and its metrics. Nil is allowed; the tool then errors when it
	// needs it.
	Connect clients.ConnectReader
	// Scraper reads the endpoint. Nil with a configured URL uses a new prom.Client.
	Scraper Scraper
	// Metrics is DEBEZIUM_METRICS_URL / _PREFIX (config.ParseDebeziumMetrics).
	// An empty URL makes the tool answer "not configured".
	Metrics config.DebeziumMetrics
	// Owners scopes the tool to the room's pipeline. It must be non-nil.
	Owners *owners.Owners
	// Rows is optional: the source database's row estimate per table
	// (*postgres.DB has it). debezium_snapshot_progress needs it for an ETA;
	// without it the tool says the ETA is unknown.
	Rows RowEstimator
}

// RowEstimator is the source client's planner estimate of a table's rows
// ("schema.table"; pg_class.reltuples, negative when never analysed). Not on
// clients.SourceDB; the wiring finds it with a type assertion.
type RowEstimator interface {
	EstimatedRows(ctx context.Context, table string) (int64, error)
}

// Register adds debezium_metrics and debezium_snapshot_progress to reg. Both
// are registered even without a metrics URL, so the tool list the model sees
// does not depend on setup; they then say "not configured".
func Register(reg *tools.Registry, d Deps) error {
	if d.Owners == nil {
		return fmt.Errorf("debeziumtools: Owners is nil (wiring error)")
	}
	if d.Scraper == nil {
		d.Scraper = prom.NewClient()
	}
	if d.Metrics.Prefix == "" {
		d.Metrics.Prefix = config.DefaultDebeziumMetricsPrefix
	}
	if err := reg.Register(&metricsTool{d: d}); err != nil {
		return fmt.Errorf("debeziumtools: register debezium_metrics: %w", err)
	}
	if err := reg.Register(&snapshotTool{d: d}); err != nil {
		return fmt.Errorf("debeziumtools: register debezium_snapshot_progress: %w", err)
	}
	return nil
}

// toolTimeout bounds one tool call: the Connect config read, the scrape and,
// for snapshot progress, the row estimates (AGENTS.md rule 4).
const toolTimeout = 10 * time.Second

// target is a tool call's connector after the input and scope checks.
type target struct {
	connector string
	prefix    string // the connector's topic.prefix: its metrics' server/name label
	sc        tools.Scope
	// configured is false when DEBEZIUM_METRICS_URL is empty; prefix is
	// then not read.
	configured bool
}

// resolve checks the input and the room's scope before any I/O, then reads
// the connector's topic.prefix (the only link between a connector and its
// metrics) and checks it is the room pipeline's.
func (d Deps) resolve(ctx context.Context, in json.RawMessage, tool string) (target, error) {
	var input struct {
		Connector string `json:"connector"`
	}
	if len(in) == 0 {
		return target{}, fmt.Errorf("%w: missing input", tools.ErrBadInput)
	}
	if err := json.Unmarshal(in, &input); err != nil {
		return target{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	if input.Connector == "" {
		return target{}, fmt.Errorf("%w: connector is required", tools.ErrBadInput)
	}
	sc, err := tools.ScopeFor(ctx, d.Owners)
	if err != nil {
		return target{}, err
	}
	if err := sc.CheckConnector(input.Connector); err != nil {
		return target{}, err
	}
	tg := target{connector: input.Connector, sc: sc}
	if d.Metrics.URL == "" {
		return tg, nil
	}
	tg.configured = true
	if d.Connect == nil {
		return target{}, fmt.Errorf("debeziumtools: %s: Kafka Connect is not configured", tool)
	}
	cfg, err := d.Connect.Config(ctx, input.Connector)
	if err != nil {
		return target{}, fmt.Errorf("debeziumtools: %s: connector config: %w", tool, err)
	}
	prefix := cfg["topic.prefix"]
	if prefix == "" {
		prefix = cfg["database.server.name"] // Debezium < 2.0
	}
	if prefix == "" {
		return target{}, fmt.Errorf("%w: connector %q has no topic.prefix, so its metrics cannot be selected", tools.ErrBadInput, input.Connector)
	}
	if !sc.Unscoped() && prefix != sc.Pipeline().TopicPrefix {
		return target{}, fmt.Errorf("%w: connector %q has topic.prefix %q, not this room's pipeline prefix %q", tools.ErrBadInput, input.Connector, prefix, sc.Pipeline().TopicPrefix)
	}
	tg.prefix = prefix
	return tg, nil
}

// notConfigured is the answer of every tool without a metrics URL.
func notConfigured() tools.Result {
	return tools.Result{Summary: "not configured: set DEBEZIUM_METRICS_URL", Data: map[string]any{"configured": false}}
}

type metricsTool struct{ d Deps }

func (t *metricsTool) Name() string { return "debezium_metrics" }
func (t *metricsTool) Description() string {
	return "Reads a Debezium source connector's own metrics from the JMX exporter: snapshot state (running, completed, rows scanned), " +
		"streaming state (connected, milliseconds behind the source, time since the last event), event counts and queue fill. " +
		"Use when a source connector is RUNNING but no change events arrive, to tell a long initial snapshot from a stalled stream, " +
		"before any restart (a restart restarts a snapshot from zero)."
}
func (t *metricsTool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string", "description": "Debezium source connector name from the room's pipeline."},
		},
		"required":             []string{"connector"},
		"additionalProperties": false,
	}
}
func (t *metricsTool) Kind() tools.Kind { return tools.KindRead }
func (t *metricsTool) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	return t.run(ctx, in)
}
