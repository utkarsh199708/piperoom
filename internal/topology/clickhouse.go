package topology

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
// node and the two intermediate objects are the new node kinds below.
//
// CONTRACT CHANGE REQUEST: graph.go does not define these two kinds yet. Until
// it does, the constants below are a local adapter in this package: Node.Kind
// is a string type, so the builder may add NodeKind values of its own without
// touching the contract file. See the PR body for the exact graph.go addition.

const (
	// KindKafkaEngine is a ClickHouse Kafka engine table: it consumes one Kafka
	// topic and holds the raw rows for a materialized view to read. Its layer
	// and owner are the warehouse's. ID prefix "kafka_engine", e.g.
	// "kafka_engine:analytics.orders_kafka".
	KindKafkaEngine NodeKind = "kafka_engine"
	// KindMaterializedView is a ClickHouse materialized view that selects from
	// a Kafka engine table and fills a warehouse sink table. Its layer and
	// owner are the warehouse's. ID prefix "materialized_view", e.g.
	// "materialized_view:analytics.orders_mv".
	KindMaterializedView NodeKind = "materialized_view"
)

// clickhouseHop returns the two intermediate nodes of one ClickHouse
// Kafka-ingestion hop and the three data edges that join them into the graph:
//
//	topic -> kafka_engine:engine -> materialized_view:view -> sink
//
// The sink node itself is the pipeline's regular KindSinkTable node and is not
// returned; sink is its id ("wh:analytics.orders"), topic the topic id
// ("topic:pg.public.orders"), and engine/view the ClickHouse object names
// ("analytics.orders_kafka", "analytics.orders_mv"). All three edges are
// proven (mapped), like connector config: the engine table's Kafka settings
// name the topic, and the view's SELECT names both the engine table and the
// sink table.
//
// owner is the warehouse layer owner. The nodes are HealthUnknown: there is no
// reader yet that reports the freshness or lag of a Kafka engine table or a
// materialized view, so Build draws them without a live reading.
func clickhouseHop(owner, topic, sink, engine, view string) (nodes []Node, edges []Edge) {
	nodes = []Node{
		{
			ID:     "kafka_engine:" + engine,
			Kind:   KindKafkaEngine,
			Name:   engine,
			Layer:  "warehouse",
			Owner:  owner,
			Health: HealthUnknown,
		},
		{
			ID:     "materialized_view:" + view,
			Kind:   KindMaterializedView,
			Name:   view,
			Layer:  "warehouse",
			Owner:  owner,
			Health: HealthUnknown,
		},
	}
	edges = []Edge{
		{From: topic, To: nodes[0].ID},
		{From: nodes[0].ID, To: nodes[1].ID},
		{From: nodes[1].ID, To: sink},
	}
	return nodes, edges
}
