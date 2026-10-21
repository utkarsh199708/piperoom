package owners

import (
	"reflect"
	"strings"
	"testing"
)

const twoSinksYAML = `
pipelines:
  - id: orders
    title: Orders CDC
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    sinks:
      - table: analytics.orders
        warehouse: Postgres
        writer: {kind: consumer_group, name: order-sync}
      - table: analytics_ch.orders
        warehouse: CLICKHOUSE
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

func TestLoadTwoSinks(t *testing.T) {
	o, err := Load(writeTemp(t, twoSinksYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, ok := o.Pipeline("orders")
	if !ok {
		t.Fatal("pipeline orders not found")
	}
	if !p.UsesSinkBlocks() || len(p.Sinks) != 2 {
		t.Fatalf("sinks = %+v, want two declarations", p.Sinks)
	}
	// Values are trimmed and normalized; the warehouse of a kafka_engine sink
	// is ClickHouse by definition.
	pg, ch := p.Sinks[0], p.Sinks[1]
	if pg.Table != "analytics.orders" || pg.Warehouse != "postgres" || pg.Writer.Kind != SinkWriterConsumerGroup || pg.Writer.Name != "order-sync" {
		t.Fatalf("postgres sink = %+v", pg)
	}
	if ch.Table != "analytics_ch.orders" || ch.Warehouse != "clickhouse" || ch.Writer.Kind != SinkWriterKafkaEngine || ch.Writer.Name != "analytics.orders_kafka" {
		t.Fatalf("clickhouse sink = %+v", ch)
	}
	// The legacy lists are the effective lists after Load, so scope, the
	// watcher and alert intake see the same objects.
	if want := []string{"analytics.orders", "analytics_ch.orders"}; !reflect.DeepEqual(p.SinkTables, want) {
		t.Fatalf("sink tables = %v, want %v", p.SinkTables, want)
	}
	if want := []string{"order-sync"}; !reflect.DeepEqual(p.ConsumerGroups, want) {
		t.Fatalf("consumer groups = %v, want %v", p.ConsumerGroups, want)
	}
	if _, ok := p.SinkForTable("analytics_ch.orders"); !ok {
		t.Fatal("SinkForTable(analytics_ch.orders) not found")
	}
	if _, ok := p.SinkForTable("analytics.missing"); ok {
		t.Fatal("SinkForTable(analytics.missing) found")
	}
}

func TestLoadSinkWriterDefaultsAndExtraGroups(t *testing.T) {
	yaml := `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [audit-reader]
    sinks:
      - table: analytics_ch.orders
        writer: {kind: kafka_engine, name: analytics.orders_kafka}
      - table: analytics.orders
        writer: {kind: consumer_group, name: order-sync}
    sink_ts_column: updated_at
layers: {}
`
	o, err := Load(writeTemp(t, yaml))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	if p.Sinks[0].Warehouse != "clickhouse" {
		t.Fatalf("kafka_engine warehouse = %q, want clickhouse", p.Sinks[0].Warehouse)
	}
	// An extra consumer group is kept; it reads the topic and writes no sink.
	if want := []string{"audit-reader", "order-sync"}; !reflect.DeepEqual(p.ConsumerGroups, want) {
		t.Fatalf("consumer groups = %v, want %v", p.ConsumerGroups, want)
	}
	if want := []string{"analytics_ch.orders", "analytics.orders"}; !reflect.DeepEqual(p.SinkTableNames(), want) {
		t.Fatalf("SinkTableNames = %v, want %v", p.SinkTableNames(), want)
	}
	if _, err := Load(writeTemp(t, strings.Replace(yaml, "    consumer_groups: [audit-reader]\n", "", 1))); err != nil {
		t.Fatalf("Load without extra groups: %v", err)
	}
}

func TestLegacySinkListsUnchanged(t *testing.T) {
	o, err := Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	if p.UsesSinkBlocks() || p.Sinks != nil {
		t.Fatalf("legacy pipeline gained sinks: %+v", p.Sinks)
	}
	if want := []string{"analytics.orders"}; !reflect.DeepEqual(p.SinkTableNames(), want) {
		t.Fatalf("SinkTableNames = %v, want %v", p.SinkTableNames(), want)
	}
	if want := []string{"order-sync"}; !reflect.DeepEqual(p.ConsumerGroupNames(), want) {
		t.Fatalf("ConsumerGroupNames = %v, want %v", p.ConsumerGroupNames(), want)
	}
}

func TestLoadSinkErrors(t *testing.T) {
	cases := []struct {
		name    string
		extra   string
		wantErr string
	}{
		{
			name:    "sinks and sink_tables",
			extra:   "    sinks:\n      - {table: analytics.orders, writer: {kind: consumer_group, name: order-sync}}\n    sink_tables: [analytics.orders]\n",
			wantErr: "sinks and sink_tables are both set",
		},
		{
			name:    "missing table",
			extra:   "    sinks:\n      - {writer: {kind: consumer_group, name: order-sync}}\n",
			wantErr: "table is required",
		},
		{
			name:    "table twice",
			extra:   "    sinks:\n      - {table: analytics.orders, writer: {kind: consumer_group, name: order-sync}}\n      - {table: analytics.orders, writer: {kind: consumer_group, name: other}}\n",
			wantErr: `sinks lists table "analytics.orders" twice`,
		},
		{
			name:    "unknown writer kind",
			extra:   "    sinks:\n      - {table: analytics.orders, writer: {kind: connector, name: jdbc-sink}}\n",
			wantErr: `writer.kind "connector"`,
		},
		{
			name:    "missing writer kind",
			extra:   "    sinks:\n      - {table: analytics.orders, writer: {name: order-sync}}\n",
			wantErr: `writer.kind ""`,
		},
		{
			name:    "missing writer name",
			extra:   "    sinks:\n      - {table: analytics.orders, writer: {kind: consumer_group}}\n",
			wantErr: "writer.name is required",
		},
		{
			name:    "kafka engine in postgres",
			extra:   "    sinks:\n      - {table: analytics.orders, warehouse: postgres, writer: {kind: kafka_engine, name: analytics.orders_kafka}}\n",
			wantErr: "needs warehouse clickhouse",
		},
		{
			name:    "sinks need a ts column",
			extra:   "    sinks:\n      - {table: analytics.orders, writer: {kind: consumer_group, name: order-sync}}\n",
			wantErr: "sink_ts_column",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			yaml := "pipelines:\n  - id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]\n    sink_ts_column: updated_at\n" + tc.extra + "layers: {}\n"
			if tc.name == "sinks need a ts column" {
				yaml = strings.Replace(yaml, "    sink_ts_column: updated_at\n", "", 1)
			}
			_, err := Load(writeTemp(t, yaml))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestBatchPipelineRefusesSinks(t *testing.T) {
	yaml := `
pipelines:
  - id: revenue
    kind: batch
    dags: [revenue_daily]
    sinks:
      - {table: analytics.fct_revenue, writer: {kind: consumer_group, name: nope}}
    sink_ts_column: loaded_at
layers: {}
`
	_, err := Load(writeTemp(t, yaml))
	if err == nil || !strings.Contains(err.Error(), "sinks is a cdc field") {
		t.Fatalf("error = %v, want a sinks-is-a-cdc-field refusal", err)
	}
}

// TestOverlapSeparateWriters: the two writers of one topic are distinct
// objects, so their names never produce an overlap; overlap is about shared
// connectors, source tables, topic prefixes and hosts only.
func TestOverlapSeparateWriters(t *testing.T) {
	o, err := Load(writeTemp(t, twoSinksYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	other := Pipeline{
		ID:           "other",
		TopicPrefix:  "other",
		SourceTables: []string{"public.other"},
		Connectors:   []string{"other-connector"},
		SinkTables:   append([]string(nil), p.SinkTables...),
	}
	o.Pipelines = append(o.Pipelines, other)
	if got := o.Overlap(other, nil); len(got) != 0 {
		t.Fatalf("Overlap = %+v, want none", got)
	}
}
