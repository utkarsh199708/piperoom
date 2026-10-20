package topology

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// TestClickhouseHopFixture proves the CH-3 fixture: one ClickHouse
// Kafka-ingestion hop for analytics.orders yields exactly the expected nodes
// and edges and flows topic -> kafka engine -> materialized view -> sink.
func TestClickhouseHopFixture(t *testing.T) {
	// The dev naming convention (dev/clickhouse): the sink table
	// analytics.orders is filled by analytics.orders_mv, which selects from
	// the Kafka engine table analytics.orders_kafka.
	const sink = "wh:analytics.orders"
	const topic = "topic:pg.public.orders"
	const engine = "analytics.orders_kafka"
	const view = "analytics.orders_mv"

	g := Graph{
		PipelineID: "orders",
		Nodes: []Node{
			{ID: topic, Kind: KindTopic, Name: "pg.public.orders", Layer: "kafka", Owner: "arjun", Health: HealthOK},
			{ID: sink, Kind: KindSinkTable, Name: "analytics.orders", Layer: "warehouse", Owner: "meera", Health: HealthOK},
		},
	}
	nodes, edges := clickhouseHop("meera", topic, sink, engine, view)
	g.Nodes = append(g.Nodes, nodes...)
	g.Edges = append(g.Edges, edges...)

	// Expected nodes and edges.
	got := edgeStrings(g.Edges)
	want := []string{
		"kafka_engine:analytics.orders_kafka>materialized_view:analytics.orders_mv",
		"materialized_view:analytics.orders_mv>wh:analytics.orders",
		"topic:pg.public.orders>kafka_engine:analytics.orders_kafka",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("edges:\n got %v\nwant %v", got, want)
	}

	engineNode := g.mustNode(t, "kafka_engine:analytics.orders_kafka")
	if engineNode.Kind != KindKafkaEngine || engineNode.Name != engine || engineNode.Layer != "warehouse" || engineNode.Owner != "meera" || engineNode.Health != HealthUnknown {
		t.Fatalf("kafka engine node = %+v", engineNode)
	}
	viewNode := g.mustNode(t, "materialized_view:analytics.orders_mv")
	if viewNode.Kind != KindMaterializedView || viewNode.Name != view || viewNode.Layer != "warehouse" || viewNode.Owner != "meera" || viewNode.Health != HealthUnknown {
		t.Fatalf("materialized view node = %+v", viewNode)
	}

	// Flow order: topic before engine before view before sink.
	out := NewBuilder(testOwners(), nil, nil, nil, nil).Render(g)
	idxTopic := strings.Index(out, "topic:pg.public.orders [")
	idxEngine := strings.Index(out, "kafka_engine:analytics.orders_kafka [")
	idxView := strings.Index(out, "materialized_view:analytics.orders_mv [")
	idxSink := strings.Index(out, "wh:analytics.orders [")
	if idxTopic < 0 || idxEngine < 0 || idxView < 0 || idxSink < 0 {
		t.Fatalf("render missing a node:\n%s", out)
	}
	if !(idxTopic < idxEngine && idxEngine < idxView && idxView < idxSink) {
		t.Fatalf("render order wrong (topic=%d engine=%d view=%d sink=%d):\n%s", idxTopic, idxEngine, idxView, idxSink, out)
	}
	for _, line := range []string{
		"  from: topic:pg.public.orders\n",
		"  from: kafka_engine:analytics.orders_kafka\n",
		"  from: materialized_view:analytics.orders_mv\n",
	} {
		if !strings.Contains(out, line) {
			t.Fatalf("render missing %q:\n%s", line, out)
		}
	}
}

// TestClickhouseHopEdgesAreMapped: every hop edge is proven data flow, never
// inferred and never a dependency.
func TestClickhouseHopEdgesAreMapped(t *testing.T) {
	_, edges := clickhouseHop("meera", "topic:t", "wh:s", "s_kafka", "s_mv")
	if len(edges) != 3 {
		t.Fatalf("edges = %d, want 3", len(edges))
	}
	for _, e := range edges {
		if e.IsDependency() || e.IsInferred() {
			t.Fatalf("edge %+v must be mapped data flow", e)
		}
	}
}

// TestClickhouseHopJSON: the new kinds serialize as their string values, so a
// client or the UI reads "kafka_engine" and "materialized_view" kinds.
func TestClickhouseHopJSON(t *testing.T) {
	g := Graph{
		PipelineID: "orders",
		Nodes: []Node{
			{ID: "topic:pg.public.orders", Kind: KindTopic, Name: "pg.public.orders", Layer: "kafka", Owner: "arjun", Health: HealthOK},
			{ID: "wh:analytics.orders", Kind: KindSinkTable, Name: "analytics.orders", Layer: "warehouse", Owner: "meera", Health: HealthOK},
		},
	}
	nodes, edges := clickhouseHop("meera", "topic:pg.public.orders", "wh:analytics.orders", "analytics.orders_kafka", "analytics.orders_mv")
	g.Nodes = append(g.Nodes, nodes...)
	g.Edges = append(g.Edges, edges...)

	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{`"kind":"kafka_engine"`, `"kind":"materialized_view"`} {
		if !strings.Contains(string(raw), kind) {
			t.Fatalf("graph JSON has no %s: %s", kind, raw)
		}
	}
	// A proven data edge keeps its original shape.
	proven, err := json.Marshal(edges[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(proven) != `{"from":"topic:pg.public.orders","to":"kafka_engine:analytics.orders_kafka"}` {
		t.Fatalf("proven hop edge JSON = %s", proven)
	}
}
