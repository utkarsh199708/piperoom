//go:build evalgen

package main

import (
	"time"

	"piperoom/internal/clients"
)

// The two C7 scenarios on the orders pipeline that E3 made time-aware. gen
// owns only the recordings below; C7's own recordings (the broken state) stay
// as C7 generated them, and this state continues from theirs.

const devOwners = "dev/owners.yaml"

func ordersSpecs() []spec {
	return []spec{slotInactiveRecoveredSpec(), poisonPillLagSpec()}
}

func ordersConnectorConfig() map[string]string {
	return map[string]string{
		"connector.class":    "io.debezium.connector.postgresql.PostgresConnector",
		"database.dbname":    "orders",
		"database.hostname":  "postgres",
		"database.password":  "redacted-by-the-tool",
		"database.port":      "5432",
		"database.user":      "piperoom",
		"slot.name":          "debezium_orders",
		"table.include.list": "public.orders",
		"topic.prefix":       "pg",
	}
}

// ---- slot-inactive-wal-growth: the resumed connector drains the slot ----

func slotInactiveRecoveredSpec() spec {
	rec := []string{"recovered"}
	seq := map[string]int{"recovered": 3}
	return spec{
		file: "evals/scenarios/slot-inactive-wal-growth.yaml", owners: devOwners, pipeline: "orders",
		phases: []string{"broken", "recovered"}, partial: true, world: slotInactiveWorld,
		calls: []call{
			{tool: "pipeline_map", input: m("pipeline_id", "orders"), only: rec},
			{tool: "connect_list_connectors", only: rec},
			{tool: "connect_status", input: m("connector", "pg-orders-connector"), only: rec},
			{tool: "kafka_topic_offsets", input: m("topic", "pg.public.orders"), only: rec},
			{tool: "kafka_consumer_lag", input: m("group", "order-sync"), only: rec},
			{tool: "pg_replication_slots", only: rec, seq: seq},
			{tool: "warehouse_freshness", input: m("table", "analytics.orders", "ts_column", "updated_at"), only: rec, seq: seq},
			{tool: "trace_record", input: m("pipeline_id", "orders", "table", "public.orders", "key", "5634120"), only: rec},
		},
	}
}

// slotInactiveWorld is the orders stack after pg-orders-connector was
// resumed: the task streams the 19.6 h of WAL the slot kept, so retained WAL
// shrinks and the warehouse's newest row moves forward on every read.
func slotInactiveWorld(c clock, _ string, tick int) *world {
	w := newWorld()
	w.addConnector("pg-orders-connector", "source", "RUNNING", "connect:8083", ordersConnectorConfig(),
		task(0, "RUNNING", "connect:8083", ""))
	topic := "pg.public.orders"
	ends := [3]int64{25610902 + 412881, 25377154 + 409330, 25792418 + 415102}
	w.topics[topic] = partitions(c, 2*time.Second,
		[2]int64{18204113, ends[0]}, [2]int64{17998640, ends[1]}, [2]int64{18377015, ends[2]})
	w.groups["order-sync"] = group("order-sync", "Stable", 1, topic,
		[2]int64{ends[0] - 288410, ends[0]}, [2]int64{ends[1] - 279950, ends[1]}, [2]int64{ends[2] - 291377, ends[2]})
	retained := [3]int64{44*gib + 100*mib, 31*gib + 700*mib, 18*gib + 200*mib}[tick]
	w.slots = []clients.ReplicationSlot{slot("debezium_orders", true, retained, "extended")}
	behind := [3]time.Duration{19*time.Hour + 20*time.Minute, 18*time.Hour + 43*time.Minute, 17*time.Hour + 2*time.Minute}[tick]
	w.whFresh["analytics.orders"] = c.ago(behind)
	// The sample row from C7's recordings: changed at 09:37, not yet through
	// the backlog.
	w.srcRows["public.orders"] = map[string]map[string]any{
		"5634120": {"id": "5634120", "updated_at": ts(c.ago(2*time.Minute + 59*time.Second + 881598*time.Microsecond))},
	}
	w.whRows["analytics.orders"] = map[string]map[string]any{}
	return w
}

// ---- consumer-poison-pill: lag on partition 1 grows on every read ----

func poisonPillLagSpec() spec {
	return spec{
		file: "evals/scenarios/consumer-poison-pill.yaml", owners: devOwners, pipeline: "orders",
		partial: true, world: poisonPillWorld,
		calls: []call{{tool: "kafka_consumer_lag", input: m("group", "order-sync"), seq: map[string]int{"": 3}}},
	}
}

// poisonPillWorld: order-sync crash-loops on pg.public.orders/1@25377154, so
// partition 1's committed offset never moves while the topic grows; the group
// flaps between Empty and PreparingRebalance, and partitions 0 and 2 keep a
// little in-flight lag that each crash loses. Tick 0 is C7's recording.
func poisonPillWorld(_ clock, _ string, tick int) *world {
	w := newWorld()
	topic := "pg.public.orders"
	type part struct{ committed, end int64 }
	reads := [3]struct {
		state   string
		members int
		p       [3]part
	}{
		{"Empty", 0, [3]part{{25610919, 25611131}, {25377154, 25391062}, {25792415, 25792602}}},
		{"PreparingRebalance", 1, [3]part{{25611190, 25611384}, {25377154, 25391523}, {25792540, 25792751}}},
		{"Empty", 0, [3]part{{25611402, 25611630}, {25377154, 25391990}, {25792760, 25792936}}},
	}
	r := reads[tick]
	w.groups["order-sync"] = group("order-sync", r.state, r.members, topic,
		[2]int64{r.p[0].committed, r.p[0].end}, [2]int64{r.p[1].committed, r.p[1].end}, [2]int64{r.p[2].committed, r.p[2].end})
	return w
}
