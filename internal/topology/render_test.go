package topology

import (
	"context"
	"flag"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/pipeline_render.golden")

// legendLine is the line the agent's Method step 1 relies on.
const legendSentence = "First failing component and root cause can differ; check upstream dependencies of the first failing component."

// goldenGraph builds a pipeline with every kind of node and edge: a source
// connector with an Avro value converter, a sink connector reading the topic
// by its topics list and writing a DLQ, a consumer group proven by its
// offsets, both registry subjects, and the owners.yaml-only links to the
// warehouse.
func goldenGraph(t *testing.T) (*builder, Graph) {
	t.Helper()
	o := loadOwners(t, `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector, orders-sink]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
`+dlqLayers)
	c := &fakeConnect{
		config: map[string]map[string]string{
			"pg-orders-connector": {
				"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
				"topic.prefix":    "pg", "table.include.list": "public.orders", "slot.name": "debezium_orders",
				"value.converter": "io.confluent.connect.avro.AvroConverter",
			},
			"orders-sink": {
				"connector.class":  "io.confluent.connect.jdbc.JdbcSinkConnector",
				"topics":           "pg.public.orders",
				"errors.tolerance": "all", "errors.deadletterqueue.topic.name": "orders-dlq",
				"errors.log.enable": "true", "errors.deadletterqueue.context.headers.enable": "true",
			},
		},
		status: map[string]clients.ConnectorStatus{
			"pg-orders-connector": {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "FAILED"}}},
			"orders-sink":         {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}},
		},
	}
	k := &fakeKafka{
		lags: map[string]clients.GroupLag{"order-sync": lagOn("pg.public.orders")},
		offsets: map[string][]clients.PartitionOffsets{
			"pg.public.orders": {{Partition: 0, Start: 0, End: 10, LastTimestamp: srNow.Add(-40 * time.Minute)}},
			"orders-dlq":       {{Partition: 0, Start: 0, End: 0}},
		},
	}
	sr := &fakeSR{
		subjects: []string{"pg.public.orders-key", "pg.public.orders-value"},
		versions: map[string][]int{"pg.public.orders-key": {1}, "pg.public.orders-value": {1, 2}},
		compat:   map[string]string{"pg.public.orders-key": "BACKWARD", "pg.public.orders-value": "BACKWARD"},
	}
	b := newBuilder(o, c, k,
		&fakeSource{slots: []clients.ReplicationSlot{{Name: "debezium_orders", Active: false, RetainedWALBytes: 3 << 20}}},
		&fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": srNow.Add(-45 * time.Minute)}}, nil)
	b.sr = sr
	b.now = func() time.Time { return srNow }
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return b, g
}

func TestRenderGolden(t *testing.T) {
	b, g := goldenGraph(t)
	got := b.Render(g)
	const path = "testdata/pipeline_render.golden"
	if *updateGolden {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("Render mismatch.\n--- got ---\n%s--- want ---\n%s", got, want)
	}
}

func TestRenderDependencyListing(t *testing.T) {
	// Nodes deliberately out of flow order: the listing must not depend on it.
	g := Graph{
		PipelineID: "p",
		Nodes: []Node{
			{ID: "wh:s", Layer: "warehouse", Owner: "w", Health: HealthFail},
			{ID: "sr:t-value", Layer: "schema-registry", Owner: "r", Health: HealthOK},
			{ID: "group:g", Layer: "consumer", Owner: "c", Health: HealthWarn},
			{ID: "topic:t", Layer: "kafka", Owner: "k", Health: HealthOK},
			{ID: "connect:c", Layer: "kafka-connect", Owner: "k", Health: HealthFail},
			{ID: "pg:t", Layer: "postgres", Owner: "p", Health: HealthOK},
		},
		Edges: []Edge{
			{From: "group:g", To: "wh:s", Mapping: MappingInferred},
			{From: "topic:t", To: "group:g"},
			{From: "connect:c", To: "topic:t"},
			{From: "pg:t", To: "connect:c"},
			{From: "connect:c", To: "sr:t-value", Kind: EdgeDependsOn},
			{From: "group:g", To: "sr:t-value", Kind: EdgeDependsOn, Mapping: MappingInferred},
			{From: "group:g", To: "sr:gone", Kind: EdgeDependsOn}, // end not in the graph
		},
	}
	got := NewBuilder(testOwners(), nil, nil, nil, nil).Render(g)
	want := "pipeline p\n" +
		renderLegend + "\n" +
		"1 pg:t [postgres, owner p] OK\n" +
		"2 connect:c [kafka-connect, owner k] FAIL\n" +
		"  from: pg:t\n" +
		"  depends on: sr:t-value\n" +
		"3 topic:t [kafka, owner k] OK\n" +
		"  from: connect:c\n" +
		"4 group:g [consumer, owner c] WARN\n" +
		"  from: topic:t\n" +
		"  depends on: sr:t-value (unconfirmed), sr:gone\n" +
		"5 wh:s [warehouse, owner w] FAIL\n" +
		"  from: group:g (unconfirmed)\n" +
		"6 sr:t-value [schema-registry, owner r] OK\n"
	if got != want {
		t.Fatalf("Render:\n--- got ---\n%s--- want ---\n%s", got, want)
	}
	if !strings.Contains(renderLegend, legendSentence) {
		t.Fatalf("legend lost the root-cause sentence: %q", renderLegend)
	}
}

func TestRenderMergesDuplicateLinks(t *testing.T) {
	g := Graph{
		PipelineID: "p",
		Nodes:      []Node{{ID: "a"}, {ID: "b"}},
		Edges: []Edge{
			{From: "a", To: "b", Mapping: MappingInferred},
			{From: "a", To: "b"}, // proven once is proven
		},
	}
	got := NewBuilder(testOwners(), nil, nil, nil, nil).Render(g)
	if !strings.Contains(got, "\n  from: a\n") || strings.Count(got, "  from: ") != 1 {
		t.Fatalf("duplicate link not merged into one proven link:\n%s", got)
	}
}

func TestRenderCycleTerminates(t *testing.T) {
	g := Graph{
		PipelineID: "p",
		Nodes:      []Node{{ID: "a"}, {ID: "b"}, {ID: "c"}},
		Edges:      []Edge{{From: "a", To: "b"}, {From: "b", To: "a"}, {From: "b", To: "c"}},
	}
	got := NewBuilder(testOwners(), nil, nil, nil, nil).Render(g)
	for _, id := range []string{"1 a ", "2 b ", "3 c "} {
		if !strings.Contains(got, id) {
			t.Fatalf("listing missing %q:\n%s", id, got)
		}
	}
}

func TestFlowOrderSideNodesLast(t *testing.T) {
	// The subject comes first in g.Nodes and nothing flows into it; it is
	// still listed after the data flow, and a depends_on edge out of a
	// connector does not reorder the flow.
	g := Graph{
		Nodes: []Node{{ID: "sr:s"}, {ID: "topic:t"}, {ID: "connect:c"}, {ID: "pg:t"}},
		Edges: []Edge{
			{From: "connect:c", To: "sr:s", Kind: EdgeDependsOn},
			{From: "topic:t", To: "connect:c", Kind: EdgeDependsOn}, // not a data edge: no reordering
			{From: "pg:t", To: "connect:c"},
			{From: "connect:c", To: "topic:t"},
		},
	}
	if got := flowOrder(g); !slices.Equal(got, []int{3, 2, 1, 0}) {
		t.Fatalf("flowOrder = %v, want [3 2 1 0] (pg, connect, topic, subject)", got)
	}
}

func TestRenderIsDeterministic(t *testing.T) {
	b, g := goldenGraph(t)
	first := b.Render(g)
	for i := 0; i < 5; i++ {
		_, again := goldenGraph(t)
		if got := b.Render(again); got != first {
			t.Fatalf("render changed between builds:\n%s\n%s", first, got)
		}
	}
}
