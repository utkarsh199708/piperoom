package topology

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// twoSinksYAML is the KAN-481 shape: one topic (pg.public.orders) with two
// independent sinks, each with its own writer. order-sync (a consumer group)
// fills Postgres analytics.orders; the ClickHouse Kafka engine table
// analytics.orders_kafka fills analytics_ch.orders through a materialized view.
const twoSinksYAML = `
pipelines:
  - id: orders
    title: Orders CDC
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    sinks:
      - table: analytics.orders
        warehouse: postgres
        writer: {kind: consumer_group, name: order-sync}
      - table: analytics_ch.orders
        warehouse: clickhouse
        writer: {kind: kafka_engine, name: analytics.orders_kafka}
    sink_ts_column: updated_at
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`

// twoSinksOwners loads the config through owners.Load, so the test covers the
// real schema path (validation, normalized values and the merged lists).
func twoSinksOwners(t *testing.T) *owners.Owners {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(twoSinksYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

func twoSinksBuilder(t *testing.T, fresh map[string]time.Time, loads map[string][]clients.LoadError) *builder {
	t.Helper()
	k := &fakeKafka{
		lags:    map[string]clients.GroupLag{"order-sync": lagOn("pg.public.orders")},
		offsets: map[string][]clients.PartitionOffsets{"pg.public.orders": {{Partition: 0, LastTimestamp: srNow.Add(-time.Minute)}}},
	}
	wh := &fakeWarehouseLoad{fakeWarehouse: fakeWarehouse{fresh: fresh}, loads: loads}
	b := newBuilder(twoSinksOwners(t), clickhouseConnect(), k,
		&fakeSource{slots: []clients.ReplicationSlot{{Name: "pg_orders_slot", Active: true, RetainedWALBytes: 10}}}, wh, nil)
	b.now = func() time.Time { return srNow }
	return b
}

// TestBuildTwoSinksOneTopic is TC2: the graph has two writer chains and two
// sink nodes; a stalled ClickHouse engine fails only the ClickHouse chain while
// the Postgres chain stays ok, and no edge claims order-sync feeds the
// ClickHouse table.
func TestBuildTwoSinksOneTopic(t *testing.T) {
	b := twoSinksBuilder(t,
		map[string]time.Time{
			"analytics.orders":    srNow.Add(-time.Minute),
			"analytics_ch.orders": srNow.Add(-45 * time.Minute),
		},
		map[string][]clients.LoadError{
			"analytics.orders_kafka": {{
				Table: "analytics.orders_kafka", At: srNow.Add(-2 * time.Minute),
				Status: "LOAD_FAILED", FirstError: "ClickHouse Kafka engine consumer reported an exception",
			}},
		})
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	wantEdges := []string{
		"connect:pg-orders-connector>topic:pg.public.orders",
		"group:order-sync>wh:analytics.orders",
		"kafka_engine:analytics.orders_kafka>materialized_view:analytics_ch.orders_mv ~",
		"materialized_view:analytics_ch.orders_mv>wh:analytics_ch.orders ~",
		"pg:public.orders>connect:pg-orders-connector",
		"topic:pg.public.orders>group:order-sync",
		"topic:pg.public.orders>kafka_engine:analytics.orders_kafka ~",
	}
	if got := edgeStrings(g.Edges); !slices.Equal(got, wantEdges) {
		t.Fatalf("edges:\n got %v\nwant %v", got, wantEdges)
	}

	// Each sink owns its health; the ClickHouse writer fails on its own node.
	pgSink := g.mustNode(t, "wh:analytics.orders")
	if pgSink.Health != HealthOK || pgSink.Facts["writer"] != "order-sync" || pgSink.Facts["warehouse"] != "postgres" {
		t.Fatalf("postgres sink = %+v", pgSink)
	}
	chSink := g.mustNode(t, "wh:analytics_ch.orders")
	if chSink.Health != HealthFail || chSink.Facts["writer"] != "analytics.orders_kafka" || chSink.Facts["warehouse"] != "clickhouse" {
		t.Fatalf("clickhouse sink = %+v", chSink)
	}
	engine := g.mustNode(t, "kafka_engine:analytics.orders_kafka")
	if engine.Health != HealthFail || engine.Layer != "warehouse" {
		t.Fatalf("engine = %+v", engine)
	}
	if _, inferred := engine.Facts["inferred"]; inferred {
		t.Fatalf("declared engine name must not read as a naming-convention guess: %+v", engine.Facts)
	}
	if view := g.mustNode(t, "materialized_view:analytics_ch.orders_mv"); view.Kind != KindMaterializedView {
		t.Fatalf("view = %+v", view)
	}
	group := g.mustNode(t, "group:order-sync")
	if group.Health != HealthOK || group.Layer != "consumer" {
		t.Fatalf("order-sync = %+v", group)
	}
}

// TestTwoSinksRenderListsBothWriters is TC3: the map text names each sink with
// its own writer, so the failing ClickHouse sink cannot be read as order-sync.
func TestTwoSinksRenderListsBothWriters(t *testing.T) {
	b := twoSinksBuilder(t,
		map[string]time.Time{
			"analytics.orders":    srNow.Add(-time.Minute),
			"analytics_ch.orders": srNow.Add(-45 * time.Minute),
		}, nil)
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	out := b.Render(g)
	var pgLine, chLine string
	for _, line := range strings.Split(out, "\n") {
		switch {
		case strings.Contains(line, "wh:analytics.orders ["):
			pgLine = line
		case strings.Contains(line, "wh:analytics_ch.orders ["):
			chLine = line
		}
	}
	if pgLine == "" || chLine == "" {
		t.Fatalf("render missing a sink line:\n%s", out)
	}
	if !strings.Contains(pgLine, "writer=order-sync") || !strings.Contains(pgLine, "warehouse=postgres") {
		t.Fatalf("postgres sink line = %q", pgLine)
	}
	if !strings.Contains(chLine, "writer=analytics.orders_kafka") || !strings.Contains(chLine, "warehouse=clickhouse") {
		t.Fatalf("clickhouse sink line = %q", chLine)
	}
	if strings.Contains(chLine, "order-sync") {
		t.Fatalf("clickhouse sink line names order-sync: %q", chLine)
	}
	if !strings.Contains(out, "from: group:order-sync") {
		t.Fatalf("render does not show order-sync feeding its sink:\n%s", out)
	}
	if !strings.Contains(out, "from: kafka_engine:analytics.orders_kafka (unconfirmed)") {
		t.Fatalf("render does not show the ClickHouse chain:\n%s", out)
	}
}

// TestExplicitSinkIgnoresPipelineWarehouse: a pipeline-level clickhouse
// warehouse must not turn a declared consumer_group sink into a Kafka-engine
// hop. The declared writer wins.
func TestExplicitSinkIgnoresPipelineWarehouse(t *testing.T) {
	o := testClickhouseOwners() // connections.warehouse_kind: clickhouse
	p := &o.Pipelines[0]
	p.ConsumerGroups = nil
	p.SinkTables = nil
	p.Sinks = []owners.Sink{{
		Table: "analytics.orders", Warehouse: "postgres",
		Writer: owners.SinkWriter{Kind: owners.SinkWriterConsumerGroup, Name: "order-sync"},
	}}
	k := &fakeKafka{
		lags:    map[string]clients.GroupLag{"order-sync": lagOn("pg.public.orders")},
		offsets: map[string][]clients.PartitionOffsets{"pg.public.orders": {{Partition: 0, LastTimestamp: srNow.Add(-time.Minute)}}},
	}
	wh := &fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": srNow.Add(-time.Minute)}}
	b := newBuilder(o, clickhouseConnect(), k,
		&fakeSource{slots: []clients.ReplicationSlot{{Name: "pg_orders_slot", Active: true, RetainedWALBytes: 10}}}, wh, nil)
	b.now = func() time.Time { return srNow }
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for _, n := range g.Nodes {
		if n.Kind == KindKafkaEngine || n.Kind == KindMaterializedView {
			t.Fatalf("consumer_group sink got a ClickHouse hop node: %+v", n)
		}
	}
	if got := edgeStrings(g.Edges); !slices.Contains(got, "group:order-sync>wh:analytics.orders") {
		t.Fatalf("declared writer edge missing:\n%v", got)
	}
}

// TestExplicitAmbiguousTopicKeepsWriterChain: when the last-segment convention
// cannot pick the engine's topic (two topics share the table name), the
// declared writer chain stays in the graph without a topic edge, instead of
// disappearing or being guessed.
func TestExplicitAmbiguousTopicKeepsWriterChain(t *testing.T) {
	p := owners.Pipeline{Sinks: []owners.Sink{{
		Table:     "analytics.orders",
		Warehouse: "clickhouse",
		Writer:    owners.SinkWriter{Kind: owners.SinkWriterKafkaEngine, Name: "analytics.orders_kafka"},
	}}}
	specs := explicitClickhouseHopSpecs(p, []string{"pg.public.orders", "pg.sales.orders"})
	if len(specs) != 1 || specs[0].topic != "" {
		t.Fatalf("specs = %+v, want one spec with no topic", specs)
	}
	fp := &flowPlan{}
	b := newBuilder(testOwners(), nil, nil, nil, nil, nil)
	b.wireClickhouseHops(fp, specs, true)
	want := []string{
		"kafka_engine:analytics.orders_kafka>materialized_view:analytics.orders_mv ~",
		"materialized_view:analytics.orders_mv>wh:analytics.orders ~",
	}
	if got := edgeStrings(fp.edges); !slices.Equal(got, want) {
		t.Fatalf("edges:\n got %v\nwant %v", got, want)
	}
}

// TestLegacySingleSinkUnchanged: the old consumer_groups + sink_tables form
// still draws one group into its sink, with no engine hop on a non-ClickHouse
// pipeline.
func TestLegacySingleSinkUnchanged(t *testing.T) {
	g := buildWith(t, testOwners(), clickhouseConnect(), &fakeKafka{
		lags:    map[string]clients.GroupLag{"order-sync": lagOn("pg.public.orders")},
		offsets: map[string][]clients.PartitionOffsets{"pg.public.orders": {{Partition: 0, LastTimestamp: srNow.Add(-time.Minute)}}},
	})
	if got := edgeStrings(g.Edges); !slices.Contains(got, "group:order-sync>wh:analytics.orders ~") {
		t.Fatalf("legacy group -> sink edge missing:\n%v", got)
	}
	for _, n := range g.Nodes {
		if n.Kind == KindKafkaEngine || n.Kind == KindMaterializedView {
			t.Fatalf("legacy postgres pipeline gained a ClickHouse node: %+v", n)
		}
	}
	if sink := g.mustNode(t, "wh:analytics.orders"); sink.Facts["writer"] != "" || sink.Facts["warehouse"] != "" {
		t.Fatalf("legacy sink gained per-sink facts: %+v", sink.Facts)
	}
}
