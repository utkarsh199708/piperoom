// Package debeziumtools registers the read-only tool that reads Debezium's
// own metrics (snapshot progress, streaming lag, queue) from the Prometheus
// text endpoint of the JMX exporter next to Kafka Connect.
package debeziumtools

import (
	"context"
	"encoding/json"
	"fmt"

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
}

// Register adds debezium_metrics to reg. The tool is registered even without
// a metrics URL, so the tool list the model sees does not depend on setup; it
// then says "not configured".
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
	return nil
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
