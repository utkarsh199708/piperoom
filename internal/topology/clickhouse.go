package topology

import (
	"context"
	"errors"
	"slices"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// ClickHouse Kafka-ingestion hop (CH-3, KAN-459).
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
//
// The engine consumes with a Kafka consumer group of its own
// (kafka_group_name in its CREATE statement), which owners.yaml does not list.
// When the warehouse client can report it, the hop also draws
//
//	topic -> group:<kafka_group_name> -> kafka_engine
//
// as a regular KindConsumerGroup node owned by the warehouse layer, with the
// group's own lag. That is what keeps a stalled warehouse sink from being
// blamed on the pipeline's declared consumer: the CDC group can read healthy
// while the engine's group is the one that is behind. The builder records the
// group on the owners config so tools.Scope.CheckReadGroup accepts it for
// kafka_consumer_lag (health.go); the write guard (CheckGroup) stays on
// declared groups.

// clickhouseHop returns the intermediate nodes of one ClickHouse
// Kafka-ingestion hop and the data edges that join them into the graph:
//
//	topic -> [group:<group> ->] kafka_engine:engine -> materialized_view:view -> sink
//
// The sink node itself is the pipeline's regular KindSinkTable node and is not
// returned; sink is its id ("wh:analytics.orders"), topic the topic id
// ("topic:pg.public.orders"), and engine/view the ClickHouse object names
// ("analytics.orders_kafka", "analytics.orders_mv"). group is the engine's
// Kafka consumer group ("" when the warehouse could not report it); declared
// says owners.yaml already lists it, so the hop adds no second node with the
// same id and only the group -> engine edge. All hop edges are inferred: the
// engine, the view and their topic all come from the dev naming convention
// (clickhouseNames, clickhouseEngineTopic), not from live ClickHouse config,
// so the agent must verify them. The group -> engine edge is the exception: it
// is proven by the engine's own CREATE statement.
//
// owner is the warehouse layer owner. The nodes are HealthUnknown here: Build
// replaces the engine node's health from the ClickHouse load-error reader
// (kafkaEngineNode) and the group node's from its Kafka lag
// (kafkaEngineGroupNode); the view stays unknown (no reader reports a view's
// freshness).
func clickhouseHop(owner, topic, sink, engine, view, group string, declared bool) (nodes []Node, edges []Edge) {
	if group != "" && !declared {
		nodes = append(nodes, Node{
			ID:     groupNodeID(group),
			Kind:   KindConsumerGroup,
			Name:   group,
			Layer:  "warehouse",
			Owner:  owner,
			Health: HealthUnknown,
			Facts:  map[string]string{"group": group, "engine": engine},
		})
	}
	nodes = append(nodes,
		Node{
			ID:     engineNodeID(engine),
			Kind:   KindKafkaEngine,
			Name:   engine,
			Layer:  "warehouse",
			Owner:  owner,
			Health: HealthUnknown,
		},
		Node{
			ID:     viewNodeID(view),
			Kind:   KindMaterializedView,
			Name:   view,
			Layer:  "warehouse",
			Owner:  owner,
			Health: HealthUnknown,
		},
	)
	if group != "" {
		if !declared {
			edges = append(edges, Edge{From: topic, To: groupNodeID(group), Mapping: MappingInferred})
		}
		// The engine's CREATE statement names the group, so this edge is mapped.
		edges = append(edges, Edge{From: groupNodeID(group), To: engineNodeID(engine)})
	} else {
		edges = append(edges, Edge{From: topic, To: engineNodeID(engine), Mapping: MappingInferred})
	}
	edges = append(edges,
		Edge{From: engineNodeID(engine), To: viewNodeID(view), Mapping: MappingInferred},
		Edge{From: viewNodeID(view), To: sink, Mapping: MappingInferred},
	)
	return nodes, edges
}

// clickhouseLoadLimit is how many recent load errors the builder reads to judge
// a Kafka engine table. It matches the clickhouse client's default.
const clickhouseLoadLimit = 20

// engineNodeID, viewNodeID and groupNodeID name a ClickHouse hop node. They are
// the single source of the id prefixes for the node kinds the hop adds.
func engineNodeID(engine string) string { return "kafka_engine:" + engine }
func viewNodeID(view string) string     { return "materialized_view:" + view }
func groupNodeID(group string) string   { return "group:" + group }

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
	// group is the Kafka consumer group the engine's CREATE statement names
	// (kafka_group_name), read from the warehouse; "" when the warehouse
	// cannot report it (no reader, not a Kafka engine table, read failed).
	group string
	// declared is true when owners.yaml already lists group in consumer_groups.
	// The declared group node then stands for it and the hop adds no second
	// node with the same id.
	declared bool
}

// clickhouseHopSpecs plans the hop for every sink table of a ClickHouse
// pipeline. A sink whose topic cannot be matched has topic "" and its hop is
// left out (no engine/view nodes, no replacement edges). The engine's group,
// when info has one, comes from the warehouse read in collect, so the hop can
// draw the sink's own consumer.
func clickhouseHopSpecs(p owners.Pipeline, topics []string, info *collected) []clickhouseHopSpec {
	specs := make([]clickhouseHopSpec, 0, len(p.SinkTables))
	for _, s := range p.SinkTables {
		engine, view := clickhouseNames(s)
		topic, _ := clickhouseEngineTopic(topics, s)
		group := info.engineGroups[engine].group
		specs = append(specs, clickhouseHopSpec{
			sink: s, engine: engine, view: view, topic: topic,
			group:    group,
			declared: group != "" && slices.Contains(p.ConsumerGroups, group),
		})
	}
	return specs
}

// wireClickhouseHops replaces the edges that carried data into each sink
// ("topic/group -> wh:sink") with the Kafka-ingestion hop
// "topic -> [group ->] kafka_engine:<engine> -> materialized_view:<view> -> wh:sink".
// The engine table consumes the topic directly, so the sink's old readers no
// longer feed it. The hop edges are inferred (engine, view and topic come from
// the dev naming convention, not live ClickHouse config), except group ->
// engine: the engine's own CREATE statement names that group.
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
		_, edges := clickhouseHop(b.layerOwner("warehouse"), "topic:"+h.topic, sinkID, h.engine, h.view, h.group, h.declared)
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
//
// The consumer_group fact names the engine's own Kafka consumer group, or says
// why the map has no group node for it: "not monitored" when the warehouse
// cannot report the group, "unreadable" when the read failed. That keeps the
// sink's consumer visible as a fact even when its lag is not.
func (b *builder) kafkaEngineNode(engine string, info *collected) Node {
	n := Node{
		ID:     engineNodeID(engine),
		Kind:   KindKafkaEngine,
		Name:   engine,
		Layer:  "warehouse",
		Owner:  b.layerOwner("warehouse"),
		Health: HealthUnknown,
		Facts: map[string]string{
			"inferred":       "naming convention",
			"consumer_group": engineGroupFact(engine, info),
		},
	}
	snap, ok := info.engineLoads[engine]
	if !ok {
		return n
	}
	switch {
	case errors.Is(snap.err, clients.ErrNotSupported):
		return n
	case snap.err != nil:
		n.Facts["error"] = "unreachable"
		return n
	}
	for _, le := range snap.loads {
		// LoadErrors returns only entries since the window we asked for, so any
		// entry is a recent failed ingestion into the engine table.
		n.Health = HealthFail
		n.Facts["consumer_error"] = HumanAge(info.now.Sub(le.At)) + " ago"
		break
	}
	return n
}

// engineGroupFact is the engine node's consumer_group fact: the group id, or
// "not monitored" when no group could be read (no reader on the warehouse
// client, the table is not a Kafka engine table, or it does not exist) and
// "unreadable" when reading it failed (ClickHouse unreachable or denied).
// A sink consumer that is not monitored is never silent: the fact names it as
// one, so the agent does not read the declared CDC group as the sink's.
func engineGroupFact(engine string, info *collected) string {
	if !info.engineGroupRead {
		return "not monitored"
	}
	snap := info.engineGroups[engine]
	if snap.group != "" {
		return snap.group
	}
	if snap.groupErr == nil ||
		errors.Is(snap.groupErr, clients.ErrNotSupported) ||
		errors.Is(snap.groupErr, clients.ErrNotFound) {
		return "not monitored"
	}
	return "unreadable"
}

// kafkaEngineGroupNode draws the consumer group the engine's CREATE statement
// names (kafka_group_name). It is a consumer_group node on the warehouse layer,
// owned by the warehouse layer owner: the group consumes for the sink, not for
// the pipeline's declared consumer. Its lag is Kafka's reading for that group,
// filtered to the pipeline's own topics like groupNode; a failed lag read
// leaves it unknown (error=unreachable), never OK, and still shows the group
// name so the agent can ask for it.
func (b *builder) kafkaEngineGroupNode(engine string, snap engineGroupSnapshot, ownTopics map[string]bool) Node {
	n := Node{
		ID:     groupNodeID(snap.group),
		Kind:   KindConsumerGroup,
		Name:   snap.group,
		Layer:  "warehouse",
		Owner:  b.layerOwner("warehouse"),
		Health: HealthUnknown,
		Facts:  map[string]string{"group": snap.group, "engine": engine},
	}
	if snap.lagErr != nil {
		n.Facts["error"] = "unreachable"
		return n
	}
	lag, ok := pipelineLag(snap.lag, ownTopics)
	if !ok {
		n.Facts["lag"] = lagUnknown
		n.Facts["state"] = lag.State
		return n
	}
	health, facts := groupHealth(lag, b.t)
	n.Health = health
	for k, v := range facts {
		n.Facts[k] = v
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

// engineGroupSnapshot is one engine table's consumer-group reading: the group
// name the warehouse reported (kafka_group_name) and its Kafka lag.
type engineGroupSnapshot struct {
	group    string
	groupErr error // the warehouse read failed; group is then ""
	lag      clients.GroupLag
	lagErr   error
}

// engineGroupReader reads the Kafka consumer group a ClickHouse Kafka engine
// table consumes with (clickhouse.Client.EngineConsumerGroup). It is a local
// interface, not a clients contract type, like the other optional readers; a
// warehouse client without it leaves the group out of the graph and the engine
// node's consumer_group fact says "not monitored".
type engineGroupReader interface {
	EngineConsumerGroup(ctx context.Context, table string) (string, error)
}
