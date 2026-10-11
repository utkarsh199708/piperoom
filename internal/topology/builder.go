package topology

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// Thresholds holds the boundaries used to decide node health.
type Thresholds struct {
	// SlotRetainedWarnBytes is the retained WAL size above which a slot warns.
	SlotRetainedWarnBytes int64
	// GroupLagWarn is the consumer lag above which a group warns.
	GroupLagWarn int64
	// SinkStaleWarn is the sink freshness age that raises a warning.
	SinkStaleWarn time.Duration
	// SinkStaleFail is the sink freshness age that raises a failure.
	SinkStaleFail time.Duration
	// HealthBudget caps the parallel health-gathering phase.
	HealthBudget time.Duration
}

// DefaultThresholds returns the thresholds the task specifies.
func DefaultThresholds() Thresholds {
	return Thresholds{
		SlotRetainedWarnBytes: 1 << 30, // 1 GiB
		GroupLagWarn:          1000,
		SinkStaleWarn:         10 * time.Minute,
		SinkStaleFail:         30 * time.Minute,
		HealthBudget:          5 * time.Second,
	}
}

// builder builds pipeline graphs and traces records.
type builder struct {
	o       *owners.Owners
	connect clients.ConnectClient
	kafka   clients.KafkaAdmin
	src     clients.SourceDB
	wh      clients.Warehouse
	t       Thresholds
}

// NewBuilder constructs a Builder with default thresholds.
func NewBuilder(o *owners.Owners, connect clients.ConnectClient, kafka clients.KafkaAdmin, src clients.SourceDB, wh clients.Warehouse) Builder {
	return &builder{o: o, connect: connect, kafka: kafka, src: src, wh: wh, t: DefaultThresholds()}
}

// Build returns the current graph for a pipeline with live health. Unreachable
// systems give HealthUnknown on their nodes, never an error for the build.
func (b *builder) Build(ctx context.Context, pipelineID string) (Graph, error) {
	p, ok := b.o.Pipeline(pipelineID)
	if !ok {
		return Graph{}, fmt.Errorf("topology: pipeline %q not found", pipelineID)
	}

	cctx, cancel := context.WithTimeout(ctx, b.t.HealthBudget)
	defer cancel()

	info := b.collect(cctx, p)

	g := Graph{PipelineID: pipelineID}

	for _, tbl := range p.SourceTables {
		g.Nodes = append(g.Nodes, b.sourceNode(p, tbl, info))
	}

	var topics []string
	connTopics := map[string][]string{}
	for _, cn := range p.Connectors {
		g.Nodes = append(g.Nodes, b.connectorNode(p, cn, info))
		var ts []string
		if snap, ok := info.connect[cn]; ok && snap.err == nil {
			ts = deriveTopics(snap.config["topic.prefix"], snap.config["table.include.list"], p.SourceTables)
		}
		connTopics[cn] = ts
		for _, t := range ts {
			if !slices.Contains(topics, t) {
				topics = append(topics, t)
			}
		}
	}

	b.collectTopics(cctx, topics, info)

	for _, t := range topics {
		g.Nodes = append(g.Nodes, b.topicNode(p, t, info))
	}
	for _, gr := range p.ConsumerGroups {
		g.Nodes = append(g.Nodes, b.groupNode(p, gr, info))
	}
	for _, s := range p.SinkTables {
		g.Nodes = append(g.Nodes, b.sinkNode(p, s, info))
	}

	// Edges: table -> connector -> topic -> group -> sink.
	for _, tbl := range p.SourceTables {
		for _, cn := range p.Connectors {
			g.Edges = append(g.Edges, Edge{From: "pg:" + tbl, To: "connect:" + cn})
		}
	}
	for _, cn := range p.Connectors {
		for _, t := range connTopics[cn] {
			g.Edges = append(g.Edges, Edge{From: "connect:" + cn, To: "topic:" + t})
		}
	}
	for _, t := range topics {
		for _, gr := range p.ConsumerGroups {
			g.Edges = append(g.Edges, Edge{From: "topic:" + t, To: "group:" + gr})
		}
	}
	for _, gr := range p.ConsumerGroups {
		for _, s := range p.SinkTables {
			g.Edges = append(g.Edges, Edge{From: "group:" + gr, To: "wh:" + s})
		}
	}

	return g, nil
}

// LayerOwner returns the user id that owns a layer. Layer ownership is global
// in owners.yaml today, so the pipeline id is accepted for interface symmetry.
func (b *builder) LayerOwner(pipelineID, layer string) (string, bool) {
	return b.o.LayerOwner(layer)
}

func (b *builder) layerOwner(layer string) string {
	u, _ := b.o.LayerOwner(layer)
	return u
}

func (b *builder) sourceNode(p owners.Pipeline, tbl string, info *collected) Node {
	n := Node{
		ID:     "pg:" + tbl,
		Kind:   KindSourceTable,
		Name:   tbl,
		Layer:  "postgres",
		Owner:  b.layerOwner("postgres"),
		Health: HealthUnknown,
	}
	if info.slotsErr != nil {
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	}
	slotName := b.slotName(p, tbl, info)
	if slotName == "" {
		n.Facts = map[string]string{"slot": "unknown"}
		return n
	}
	var slot *clients.ReplicationSlot
	for i := range info.slots {
		if info.slots[i].Name == slotName {
			slot = &info.slots[i]
			break
		}
	}
	if slot == nil {
		n.Facts = map[string]string{"slot": "missing"}
		return n
	}
	n.Health, n.Facts = sourceHealth(*slot, b.t)
	return n
}

// slotName finds the replication slot name for the connector that streams tbl.
func (b *builder) slotName(p owners.Pipeline, tbl string, info *collected) string {
	for _, cn := range p.Connectors {
		snap, ok := info.connect[cn]
		if !ok || snap.err != nil {
			continue
		}
		if streamsTable(snap.config["table.include.list"], tbl) {
			return slotNameFor(cn, snap.config)
		}
	}
	return ""
}

func (b *builder) connectorNode(p owners.Pipeline, cn string, info *collected) Node {
	n := Node{
		ID:     "connect:" + cn,
		Kind:   KindConnector,
		Name:   cn,
		Layer:  "kafka-connect",
		Owner:  b.layerOwner("kafka-connect"),
		Health: HealthUnknown,
	}
	snap := info.connect[cn]
	if snap.err != nil {
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	}
	n.Health, n.Facts = connectorHealth(snap.status)
	return n
}

func (b *builder) topicNode(p owners.Pipeline, topic string, info *collected) Node {
	n := Node{
		ID:     "topic:" + topic,
		Kind:   KindTopic,
		Name:   topic,
		Layer:  "kafka",
		Owner:  b.layerOwner("kafka"),
		Health: HealthUnknown,
	}
	n.Health, n.Facts = topicHealth(info.topicErr[topic], info.topicNewest[topic], info.now)
	return n
}

func (b *builder) groupNode(p owners.Pipeline, gr string, info *collected) Node {
	n := Node{
		ID:     "group:" + gr,
		Kind:   KindConsumerGroup,
		Name:   gr,
		Layer:  "consumer",
		Owner:  b.layerOwner("consumer"),
		Health: HealthUnknown,
	}
	snap := info.groups[gr]
	if snap.err != nil {
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	}
	n.Health, n.Facts = groupHealth(snap.lag, b.t)
	return n
}

func (b *builder) sinkNode(p owners.Pipeline, s string, info *collected) Node {
	n := Node{
		ID:     "wh:" + s,
		Kind:   KindSinkTable,
		Name:   s,
		Layer:  "warehouse",
		Owner:  b.layerOwner("warehouse"),
		Health: HealthUnknown,
	}
	snap := info.freshness[s]
	if snap.err != nil {
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	}
	n.Health, n.Facts = sinkHealth(snap.ts, info.now, b.t)
	return n
}

// streamsTable reports whether a Debezium table.include.list includes tbl.
// An empty include list streams every table in the pipeline.
func streamsTable(includeList, tbl string) bool {
	inc := splitCSV(includeList)
	return len(inc) == 0 || slices.Contains(inc, tbl)
}

func slotNameFor(connector string, cfg map[string]string) string {
	if n := cfg["slot.name"]; n != "" {
		return n
	}
	return connector
}

// deriveTopics returns the Debezium topic names for the pipeline tables this
// connector streams: <topic.prefix>.<schema>.<table>.
func deriveTopics(prefix, includeList string, tables []string) []string {
	if prefix == "" {
		return nil
	}
	inc := splitCSV(includeList)
	var topics []string
	for _, t := range tables {
		if len(inc) == 0 || slices.Contains(inc, t) {
			topics = append(topics, prefix+"."+t)
		}
	}
	return topics
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
