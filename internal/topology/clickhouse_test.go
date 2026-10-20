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
		"kafka_engine:analytics.orders_kafka>materialized_view:analytics.orders_mv ~",
		"materialized_view:analytics.orders_mv>wh:analytics.orders ~",
		"topic:pg.public.orders>kafka_engine:analytics.orders_kafka ~",
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
		"  from: topic:pg.public.orders (unconfirmed)\n",
		"  from: kafka_engine:analytics.orders_kafka (unconfirmed)\n",
		"  from: materialized_view:analytics.orders_mv (unconfirmed)\n",
	} {
		if !strings.Contains(out, line) {
			t.Fatalf("render missing %q:\n%s", line, out)
		}
	}
}

// TestClickhouseHopEdgesAreInferred: every hop edge is a naming-convention
// guess, never proven data flow and never a dependency, so the agent treats it
// as a hypothesis to verify.
func TestClickhouseHopEdgesAreInferred(t *testing.T) {
	_, edges := clickhouseHop("meera", "topic:t", "wh:s", "s_kafka", "s_mv")
	if len(edges) != 3 {
		t.Fatalf("edges = %d, want 3", len(edges))
	}
	for _, e := range edges {
		if e.IsDependency() || !e.IsInferred() {
			t.Fatalf("edge %+v must be inferred data flow", e)
		}
	}
}

// TestClickhouseEngineTopicAmbiguous: when two pipeline topics share the table
// name (e.g. pg.public.orders and pg.sales.orders), the last-segment
// convention cannot say which one the engine reads, so the hop is left out
// (ok=false) rather than drawn with an arbitrary topic.
func TestClickhouseEngineTopicAmbiguous(t *testing.T) {
	topics := []string{"pg.public.orders", "pg.sales.orders"}
	if topic, ok := clickhouseEngineTopic(topics, "analytics.orders"); ok || topic != "" {
		t.Fatalf("clickhouseEngineTopic(%v, analytics.orders) = %q, %v; want no match", topics, topic, ok)
	}
	if topic, ok := clickhouseEngineTopic([]string{"pg.public.orders"}, "analytics.orders"); !ok || topic != "pg.public.orders" {
		t.Fatalf("single match = %q, %v; want pg.public.orders, true", topic, ok)
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
	// An inferred hop edge carries its mapping, so a client reads it as a
	// hypothesis, not proven flow.
	inferred, err := json.Marshal(edges[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(inferred) != `{"from":"topic:pg.public.orders","to":"kafka_engine:analytics.orders_kafka","mapping":"inferred"}` {
		t.Fatalf("inferred hop edge JSON = %s", inferred)
	}
}
