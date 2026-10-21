package topology

import (
	"errors"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// ClickHouse Kafka-ingestion hop (CH-3).
//
// A ClickHouse warehouse ingests a CDC topic through three objects
// (dev/clickhouse): a Kafka engine table that consumes the topic, a
// materialized view that selects from the engine table, and the
// MergeTree-family sink table the view fills. The pipeline map draws that hop
// as
//
//	topic -> kafka_engine -> materialized_view -> sink_table
//
// where the sink table is the pipeline's regular KindSinkTable ("wh:")
// node and the two intermediate objects are the contract's KindKafkaEngine
// and KindMaterializedView nodes (graph.go).

// clickhouseHop returns the two intermediate nodes of one ClickHouse
// Kafka-ingestion hop and the three data edges that join them into the graph:
//
//	topic -> kafka_engine:engine -> materialized_view:view -> sink
//
// The sink node itself is the pipeline's regular KindSinkTable node and is not
// returned; sink is its id ("wh:analytics.orders"), topic the topic id
// ("topic:pg.public.orders"), and engine/view the ClickHouse object names
// ("analytics.orders_kafka", "analytics.orders_mv"). All three edges are
// inferred: the engine, the view and their topic all come from the dev naming
// convention (clickhouseNames, clickhouseEngineTopic), not from live
// ClickHouse config, so the agent must verify them.
//
// owner is the warehouse layer owner. Both nodes are HealthUnknown here: Build
// replaces the engine node's health from the ClickHouse load-error reader
// (kafkaEngineNode) and leaves the view unknown (no reader reports a view's
// freshness).
func clickhouseHop(owner, topic, sink, engine, view string) (nodes []Node, edges []Edge) {
	nodes = []Node{
		{
			ID:     engineNodeID(engine),
			Kind:   KindKafkaEngine,
			Name:   engine,
			Layer:  "warehouse",
			Owner:  owner,
			Health: HealthUnknown,
		},
		{
			ID:     viewNodeID(view),
			Kind:   KindMaterializedView,
			Name:   view,
			Layer:  "warehouse",
			Owner:  owner,
			Health: HealthUnknown,
		},
	}
	edges = []Edge{
		{From: topic, To: nodes[0].ID, Mapping: MappingInferred},
		{From: nodes[0].ID, To: nodes[1].ID, Mapping: MappingInferred},
		{From: nodes[1].ID, To: sink, Mapping: MappingInferred},
	}
	return nodes, edges
}

// clickhouseLoadLimit is how many recent load errors the builder reads to judge
// a Kafka engine table. It matches the clickhouse client's default.
const clickhouseLoadLimit = 20

// engineNodeID and viewNodeID name a ClickHouse hop node. They are the single
// source of the id prefixes for the two new node kinds.
func engineNodeID(engine string) string { return "kafka_engine:" + engine }
func viewNodeID(view string) string     { return "materialized_view:" + view }

// clickhouseNames returns the ClickHouse object names of one Kafka-ingestion
// hop by the dev naming convention (dev/clickhouse): the Kafka engine table is
// "<sink>_kafka" and the materialized view "<sink>_mv". There is no owners.yaml
// declaration for these names yet, so a team with another convention would
// need one.
func clickhouseNames(sink string) (engine, view string) {
	return sink + "_kafka", sink + "_mv"
}

// lastSegment is the part of a qualified name after the final dot.
func lastSegment(s string) string {
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		return s[i+1:]
	}
	return s
}

// clickhouseEngineTopic returns the pipeline topic a sink table's Kafka engine
// consumes, by the convention that the topic's table part matches the sink's:
// sink "analytics.orders" is filled from topic "pg.public.orders" (both end in
// "orders"). ok is false when no topic matches or when more than one topic
// matches (e.g. "pg.public.orders" and "pg.sales.orders" both end in
// "orders"): the last-segment convention then cannot say which one the engine
// reads, so the hop is left out rather than drawn with a guessed topic.
func clickhouseEngineTopic(topics []string, sink string) (string, bool) {
	seg := lastSegment(sink)
	match := ""
	for _, t := range topics {
		if lastSegment(t) == seg {
			if match != "" {
				return "", false // ambiguous: two topics share the table name
			}
			match = t
		}
	}
	return match, match != ""
}

// clickhouseHopSpec is one Kafka-ingestion hop to wire into a graph.
type clickhouseHopSpec struct {
	sink, engine, view, topic string
}

// clickhouseHopSpecs plans the hop for every sink table of a ClickHouse
// pipeline. A sink whose topic cannot be matched has topic "" and its hop is
// left out (no engine/view nodes, no replacement edges).
func clickhouseHopSpecs(p owners.Pipeline, topics []string) []clickhouseHopSpec {
	specs := make([]clickhouseHopSpec, 0, len(p.SinkTables))
	for _, s := range p.SinkTables {
		engine, view := clickhouseNames(s)
		topic, _ := clickhouseEngineTopic(topics, s)
		specs = append(specs, clickhouseHopSpec{sink: s, engine: engine, view: view, topic: topic})
	}
	return specs
}

// wireClickhouseHops replaces the edges that carried data into each sink
// ("topic/group -> wh:sink") with the Kafka-ingestion hop
// "topic -> kafka_engine:<engine> -> materialized_view:<view> -> wh:sink".
// The engine table consumes the topic directly, so the sink's old readers no
// longer feed it. The hop edges are inferred: engine, view and topic all come
// from the dev naming convention, not from live ClickHouse config.
func (b *builder) wireClickhouseHops(fp *flowPlan, specs []clickhouseHopSpec) {
	for _, h := range specs {
		if h.topic == "" {
			continue
		}
		sinkID := "wh:" + h.sink
		kept := make([]Edge, 0, len(fp.edges))
		for _, e := range fp.edges {
			if e.To == sinkID {
				continue
			}
			kept = append(kept, e)
		}
		_, edges := clickhouseHop(b.layerOwner("warehouse"), "topic:"+h.topic, sinkID, h.engine, h.view)
		fp.edges = append(kept, edges...)
	}
}

// kafkaEngineNode draws a Kafka engine table's node, with its health derived
// from the ClickHouse load-error reader (clients.LoadErrorReader): a Kafka
// consumer exception in the engine's recent history means the engine stopped
// consuming, which fails this node before the sink it no longer fills goes
// stale (first-failing semantics). The reader reports only exceptions, never
// that the engine exists, and the engine's name is a naming-convention guess,
// so a reading without exceptions leaves the node unknown (inferred), not OK:
// the table may not exist. No reader, an unreachable reader, or a warehouse
// that cannot report load history leaves the node unknown too.
func (b *builder) kafkaEngineNode(engine string, info *collected) Node {
	n := Node{
		ID:     engineNodeID(engine),
		Kind:   KindKafkaEngine,
		Name:   engine,
		Layer:  "warehouse",
		Owner:  b.layerOwner("warehouse"),
		Health: HealthUnknown,
		Facts:  map[string]string{"inferred": "naming convention"},
	}
	snap, ok := info.engineLoads[engine]
	if !ok {
		return n
	}
	switch {
	case errors.Is(snap.err, clients.ErrNotSupported):
		return n
	case snap.err != nil:
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	}
	for _, le := range snap.loads {
		// LoadErrors returns only entries since the window we asked for, so any
		// entry is a recent failed ingestion into the engine table.
		n.Health = HealthFail
		n.Facts = map[string]string{"consumer_error": HumanAge(info.now.Sub(le.At)) + " ago"}
		break
	}
	return n
}

// materializedViewNode draws a materialized view's node. No reader reports a
// view's freshness, so it stays unknown.
func (b *builder) materializedViewNode(view string) Node {
	return Node{
		ID:     viewNodeID(view),
		Kind:   KindMaterializedView,
		Name:   view,
		Layer:  "warehouse",
		Owner:  b.layerOwner("warehouse"),
		Health: HealthUnknown,
	}
}

// engineLoadSnapshot is one engine table's load-error reading.
type engineLoadSnapshot struct {
	loads []clients.LoadError
	err   error
}
