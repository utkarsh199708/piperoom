//go:build evalgen

package main

import (
	"time"

	"piperoom/internal/clients"
)

// The held-out set: six incidents on pipelines the agent was never tuned on
// (evals/scenarios/heldout/fixtures/owners.yaml). Each world below is the
// incident's state; the narrative, the expectations and the sources are in
// the scenario file's header.

const heldoutOwners = "evals/scenarios/heldout/fixtures/owners.yaml"

func heldoutSpecs() []spec {
	return []spec{
		paymentsSpec(), inventorySpec(), usersSpec(), shipmentsSpec(), billingSpec(), ledgerSpec(),
	}
}

func m(kv ...any) map[string]any {
	out := map[string]any{}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i].(string)] = kv[i+1]
	}
	return out
}

// pipe names the parts of a pipeline the standard calls read.
type pipe struct {
	id         string
	tables     []string // source tables; topics are prefix + "." + table
	prefix     string
	connectors []string
	group      string
	sinks      []string
	tsCol      string
	subjects   []string
	// key is a sample primary key for the row, find and trace calls; "" for
	// none. It is read from tables[0] and sinks[0].
	key string
}

// standardCalls is what a broad investigation of p can ask: every tool on
// every name the pipeline has, then wildcards that answer any other name the
// way the real tools do (mostly the scope refusal).
func standardCalls(p pipe) []call {
	calls := []call{
		{tool: "pipeline_map", input: m("pipeline_id", p.id)},
		{tool: "connect_list_connectors"},
	}
	for _, cn := range p.connectors {
		calls = append(calls,
			call{tool: "connect_status", input: m("connector", cn)},
			call{tool: "connect_config", input: m("connector", cn)})
	}
	calls = append(calls, call{tool: "schema_registry_subjects"})
	for _, s := range p.subjects {
		calls = append(calls, call{tool: "schema_registry_versions", input: m("subject", s)})
	}
	for _, t := range p.tables {
		topic := p.prefix + "." + t
		calls = append(calls, call{tool: "kafka_topic_offsets", input: m("topic", topic)})
		if len(p.tables) == 1 {
			calls = append(calls, call{tool: "kafka_topic_config", input: m("pipeline_id", p.id)})
		} else {
			calls = append(calls, call{tool: "kafka_topic_config", input: m("pipeline_id", p.id, "table", t)})
		}
	}
	calls = append(calls,
		call{tool: "kafka_list_groups"},
		call{tool: "kafka_consumer_lag", input: m("group", p.group)},
		call{tool: "pg_replication_slots"},
		call{tool: "pg_recent_ddl", input: m("since", ago(24*time.Hour)), free: []string{"since"}},
	)
	for _, t := range p.tables {
		calls = append(calls, call{tool: "pg_recent_ddl", input: m("table", t, "since", ago(24*time.Hour)), free: []string{"since"}})
	}
	calls = append(calls,
		call{tool: "pg_long_transactions"},
		call{tool: "pg_publication_tables", input: m("pipeline_id", p.id)},
	)
	for _, s := range p.sinks {
		calls = append(calls, call{tool: "warehouse_freshness", input: m("table", s, "ts_column", p.tsCol)})
	}
	if p.key != "" {
		topic := p.prefix + "." + p.tables[0]
		calls = append(calls,
			call{tool: "pg_row", input: m("pipeline_id", p.id, "table", p.tables[0], "pk_column", "id", "key", p.key)},
			call{tool: "warehouse_row", input: m("pipeline_id", p.id, "table", p.sinks[0], "pk_column", "id", "key", p.key)},
			call{tool: "kafka_find_by_key", input: m("topic", topic, "key", p.key, "since", ago(10*time.Minute)), free: []string{"since"}},
			call{tool: "trace_record", input: m("pipeline_id", p.id, "table", p.tables[0], "key", p.key)},
		)
	}
	// Wildcards last: an exact recording above always wins over them.
	calls = append(calls,
		call{tool: "pipeline_map", input: m("pipeline_id", "<id>"), wildcard: true},
		call{tool: "connect_status", input: m("connector", "<name>"), wildcard: true},
		call{tool: "connect_config", input: m("connector", "<name>"), wildcard: true},
		call{tool: "schema_registry_versions", input: m("subject", "<subject>"), wildcard: true},
		call{tool: "schema_registry_diff", input: m("subject", "<subject>", "from_version", 1), wildcard: true},
		call{tool: "kafka_topic_offsets", input: m("topic", "<topic>"), wildcard: true},
		call{tool: "kafka_consumer_lag", input: m("group", "<group>"), wildcard: true},
		call{tool: "kafka_find_by_key", input: m("topic", p.prefix+"."+p.tables[0], "key", "<key>", "since", ago(10*time.Minute)), wildcard: true},
		call{tool: "pg_row", input: m("pipeline_id", p.id, "table", p.tables[0], "pk_column", "id", "key", "<key>"), wildcard: true},
		call{tool: "warehouse_row", input: m("pipeline_id", p.id, "table", p.sinks[0], "pk_column", "id", "key", "<key>"), wildcard: true},
		call{tool: "trace_record", input: m("pipeline_id", p.id, "table", p.tables[0], "key", "<key>"), wildcard: true},
	)
	return calls
}

// ---- (a) payments: JDBC sink rejects every record, errors.tolerance=all hides it ----

func paymentsSpec() spec {
	p := pipe{
		id: "payments", tables: []string{"public.payments"}, prefix: "pay",
		connectors: []string{"pg-payments-source", "jdbc-payments-sink"},
		group:      "connect-jdbc-payments-sink", sinks: []string{"analytics.payments"}, tsCol: "updated_at",
		subjects: []string{"pay.public.payments-key", "pay.public.payments-value"}, key: "88123401",
	}
	calls := insertBeforeWildcards(standardCalls(p),
		call{tool: "schema_registry_diff", input: m("subject", "pay.public.payments-value", "from_version", 3)},
		call{tool: "schema_registry_diff", input: m("subject", "pay.public.payments-value", "from_version", 3, "to_version", 4)},
		// The DLQ named in the alert is not a pipeline topic: the tools refuse it.
		call{tool: "kafka_topic_offsets", input: m("topic", "dlq.jdbc-payments-sink")},
	)
	return spec{
		file: "evals/scenarios/heldout/payments-sink-dlq.yaml", owners: heldoutOwners, pipeline: p.id,
		world: paymentsWorld, calls: calls,
	}
}

func paymentsWorld(c clock, _ string, _ int) *world {
	w := newWorld()
	ddlAt := c.ago(47*time.Minute + 12*time.Second)
	w.addConnector("pg-payments-source", "source", "RUNNING", "connect-1:8083",
		debezium("payments-db", "payments", "public.payments", "pay", "debezium_payments"),
		task(0, "RUNNING", "connect-1:8083", ""))
	w.addConnector("jdbc-payments-sink", "sink", "RUNNING", "connect-2:8083", map[string]string{
		"connector.class":                   "io.debezium.connector.jdbc.JdbcSinkConnector",
		"topics":                            "pay.public.payments",
		"connection.url":                    "jdbc:postgresql://warehouse-db:5432/analytics",
		"connection.username":               "loader",
		"connection.password":               "redacted-by-the-tool",
		"insert.mode":                       "upsert",
		"primary.key.mode":                  "record_key",
		"schema.evolution":                  "none",
		"table.name.format":                 "analytics.payments",
		"errors.tolerance":                  "all",
		"errors.deadletterqueue.topic.name": "dlq.jdbc-payments-sink",
		"errors.deadletterqueue.context.headers.enable": "true",
		"errors.log.enable":                             "false",
		"key.converter":                                 "io.confluent.connect.avro.AvroConverter",
		"value.converter":                               "io.confluent.connect.avro.AvroConverter",
	}, task(0, "RUNNING", "connect-2:8083", ""))

	topic := "pay.public.payments"
	base := []string{
		avroField("id", avroLong), avroField("order_id", avroLong), avroField("amount", avroDecimal(12, 2)),
		avroField("currency", avroString), avroField("status", avroString), avroField("updated_at", avroZoned),
	}
	w.subjects[topic+"-key"] = []schema{{id: 41, schema: keySchema(topic, avroField("id", avroLong))}}
	w.subjects[topic+"-value"] = []schema{
		{id: 42, schema: envelope(topic, base[0], base[1], base[2], base[3], base[5])},
		{id: 57, schema: envelope(topic, base...)},
		{id: 63, schema: envelope(topic, append(append([]string{}, base...), optField("captured_at", avroZoned))...)},
		{id: 88, schema: envelope(topic, append(append([]string{}, base...), optField("captured_at", avroZoned), optField("risk_score", avroDecimal(5, 2)))...)},
	}

	w.topics[topic] = partitions(c, 9*time.Second, [2]int64{0, 6120344}, [2]int64{0, 6098117}, [2]int64{0, 6133502})
	w.topics["dlq.jdbc-payments-sink"] = partitions(c, 9*time.Second, [2]int64{0, 18204})
	w.topicCfg[topic] = topicConfig(topic, 3, "604800000")
	w.groups["connect-jdbc-payments-sink"] = group("connect-jdbc-payments-sink", "Stable", 1, topic,
		[2]int64{6120339, 6120344}, [2]int64{6098117, 6098117}, [2]int64{6133496, 6133502})
	w.records[topic] = []clients.RecordRef{{Topic: topic, Partition: 2, Offset: 6133311, Timestamp: c.ago(2*time.Minute + 59*time.Second), Key: "88123401"}}

	w.slots = []clients.ReplicationSlot{slot("debezium_payments", true, 18*mib+400*1024, "reserved")}
	w.ddl = []clients.DDLChange{{
		At: ddlAt, CommandTag: "ALTER TABLE", ObjectIdentity: "public.payments",
		Query: "ALTER TABLE public.payments ADD COLUMN risk_score numeric(5,2)", Username: "payments_migrator",
	}}
	w.srcRows["public.payments"] = map[string]map[string]any{
		"88123401": {"id": "88123401", "updated_at": ts(c.ago(3 * time.Minute)), "captured_at": ts(c.ago(3*time.Minute + 2*time.Second))},
	}
	w.pubs = pubs{{"public", "payments", true, true}}.tables()
	w.whFresh["analytics.payments"] = c.ago(47*time.Minute + 15*time.Second)
	w.whRows["analytics.payments"] = map[string]map[string]any{}
	return w
}

// ---- (b) inventory: one of two connectors on a shared prefix lost its slot ----

func inventorySpec() spec {
	p := pipe{
		id: "inventory", tables: []string{"public.stock_levels", "public.sites"}, prefix: "inv",
		connectors: []string{"pg-inventory-stock", "pg-inventory-sites"},
		group:      "inventory-sync", sinks: []string{"analytics.stock_levels", "analytics.sites"}, tsCol: "updated_at",
		subjects: []string{"inv.public.sites-key", "inv.public.sites-value", "inv.public.stock_levels-key", "inv.public.stock_levels-value"},
	}
	// kafka_topic_config without a table: the real tool asks for one, because
	// this pipeline has two source tables.
	calls := insertBeforeWildcards(standardCalls(p), call{tool: "kafka_topic_config", input: m("pipeline_id", "inventory")})
	return spec{
		file: "evals/scenarios/heldout/inventory-one-connector-slot-lost.yaml", owners: heldoutOwners, pipeline: p.id,
		world: inventoryWorld, calls: calls,
	}
}

const slotLostTrace = `org.apache.kafka.connect.errors.ConnectException: An exception occurred in the change event producer. This connector will be stopped.
	at io.debezium.pipeline.ErrorHandler.setProducerThrowable(ErrorHandler.java:67)
	at io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.execute(PostgresStreamingChangeEventSource.java:180)
	at io.debezium.pipeline.ChangeEventSourceCoordinator.streamEvents(ChangeEventSourceCoordinator.java:325)
Caused by: org.postgresql.util.PSQLException: ERROR: can no longer get changes from replication slot "debezium_inv_stock"
  Detail: This slot has been invalidated because it exceeded the maximum reserved size.
	at org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2725)
	at org.postgresql.core.v3.QueryExecutorImpl.processCopyResults(QueryExecutorImpl.java:1306)
	at org.postgresql.core.v3.QueryExecutorImpl.readFromCopy(QueryExecutorImpl.java:1188)`

func inventoryWorld(c clock, _ string, _ int) *world {
	w := newWorld()
	failedAt := 3*time.Hour + 12*time.Minute
	stockCfg := debezium("inventory-db", "inventory", "public.stock_levels", "inv", "debezium_inv_stock")
	stockCfg["publication.name"] = "dbz_inv_stock"
	sitesCfg := debezium("inventory-db", "inventory", "public.sites", "inv", "debezium_inv_sites")
	sitesCfg["publication.name"] = "dbz_inv_sites"
	w.addConnector("pg-inventory-stock", "source", "RUNNING", "connect-1:8083", stockCfg,
		task(0, "FAILED", "connect-1:8083", slotLostTrace))
	w.addConnector("pg-inventory-sites", "source", "RUNNING", "connect-2:8083", sitesCfg,
		task(0, "RUNNING", "connect-2:8083", ""))

	for _, t := range []string{"inv.public.stock_levels", "inv.public.sites"} {
		w.subjects[t+"-key"] = []schema{{id: 11, schema: keySchema(t, avroField("id", avroString))}}
	}
	w.subjects["inv.public.stock_levels-value"] = []schema{{id: 12, schema: envelope("inv.public.stock_levels",
		avroField("id", avroString), avroField("site_id", avroString), avroField("on_hand", avroInt), avroField("reserved", avroInt), avroField("updated_at", avroZoned))}}
	w.subjects["inv.public.sites-value"] = []schema{{id: 13, schema: envelope("inv.public.sites",
		avroField("id", avroString), avroField("name", avroString), avroField("country", avroString), avroField("updated_at", avroZoned))}}

	w.topics["inv.public.stock_levels"] = partitions(c, failedAt, [2]int64{0, 88412003}, [2]int64{0, 88397120}, [2]int64{0, 88420551}, [2]int64{0, 88389904})
	w.topics["inv.public.sites"] = partitions(c, 20*time.Second, [2]int64{0, 41209}, [2]int64{0, 40877})
	w.topicCfg["inv.public.stock_levels"] = topicConfig("inv.public.stock_levels", 4, "259200000")
	w.topicCfg["inv.public.sites"] = topicConfig("inv.public.sites", 2, "259200000")
	g := group("inventory-sync", "Stable", 2, "inv.public.stock_levels",
		[2]int64{88412003, 88412003}, [2]int64{88397120, 88397120}, [2]int64{88420551, 88420551}, [2]int64{88389904, 88389904})
	w.groups["inventory-sync"] = addGroupTopic(g, "inv.public.sites", [2]int64{41208, 41209}, [2]int64{40877, 40877})

	sites := slot("debezium_inv_sites", true, 4*mib+200*1024, "reserved")
	stock := slot("debezium_inv_stock", false, 0, "lost")
	stock.RestartLSN, stock.ConfirmedFlushLSN = "", "1F/A2000000"
	w.slots = []clients.ReplicationSlot{stock, sites}
	w.srcRows["public.stock_levels"] = map[string]map[string]any{
		"SKU-77120-DE3": {"id": "SKU-77120-DE3", "updated_at": ts(c.ago(14 * time.Minute))},
	}
	w.pubs = pubs{{"public", "stock_levels", true, true}, {"public", "sites", true, true}}.tables()
	w.whFresh["analytics.stock_levels"] = c.ago(failedAt + 9*time.Second)
	w.whFresh["analytics.sites"] = c.ago(31 * time.Second)
	w.whRows["analytics.stock_levels"] = map[string]map[string]any{
		"SKU-77120-DE3": {"id": "SKU-77120-DE3", "updated_at": ts(c.ago(failedAt + 41*time.Minute))},
	}
	return w
}

// ---- (c) users: compatibility override NONE lets an incompatible schema through ----

func usersSpec() spec {
	p := pipe{
		id: "users", tables: []string{"public.users"}, prefix: "usr",
		connectors: []string{"pg-users-connector"}, group: "user-profile-sync",
		sinks: []string{"analytics.user_profiles"}, tsCol: "updated_at",
		subjects: []string{"usr.public.users-key", "usr.public.users-value"},
	}
	calls := standardCalls(p)
	for i := range calls {
		if (calls[i].tool == "kafka_consumer_lag" || calls[i].tool == "kafka_topic_offsets") && !calls[i].wildcard {
			calls[i].seq = map[string]int{"": 3}
		}
	}
	calls = insertBeforeWildcards(calls,
		call{tool: "schema_registry_diff", input: m("subject", "usr.public.users-value", "from_version", 3)},
		call{tool: "schema_registry_diff", input: m("subject", "usr.public.users-value", "from_version", 3, "to_version", 4)},
	)
	return spec{
		file: "evals/scenarios/heldout/users-compat-none-consumer.yaml", owners: heldoutOwners, pipeline: p.id,
		world: usersWorld, calls: calls,
	}
}

func usersWorld(c clock, _ string, tick int) *world {
	w := newWorld()
	ddlAt := c.ago(71*time.Minute + 3*time.Second)
	w.addConnector("pg-users-connector", "source", "RUNNING", "connect-1:8083",
		debezium("users-db", "users", "public.users", "usr", "debezium_users"),
		task(0, "RUNNING", "connect-1:8083", ""))

	topic := "usr.public.users"
	fields := func(tier string) []string {
		return []string{
			avroField("id", avroLong), avroField("email", avroString), avroField("display_name", avroString),
			avroField("loyalty_tier", tier), avroField("updated_at", avroZoned),
		}
	}
	w.subjects[topic+"-key"] = []schema{{id: 21, schema: keySchema(topic, avroField("id", avroLong))}}
	w.subjects[topic+"-value"] = []schema{
		{id: 22, schema: envelope(topic, avroField("id", avroLong), avroField("email", avroString), avroField("updated_at", avroZoned))},
		{id: 31, schema: envelope(topic, avroField("id", avroLong), avroField("email", avroString), avroField("display_name", avroString), avroField("updated_at", avroZoned))},
		{id: 39, schema: envelope(topic, fields(avroInt)...)},
		{id: 74, schema: envelope(topic, fields(avroString)...)},
	}
	w.subjectLevel[topic+"-value"] = "NONE"

	// The consumer is stuck at the first v4 record of each partition; the
	// topic keeps growing, so lag rises on every read.
	grow := int64(tick) * 105
	ends := [3]int64{1187311 + grow, 1179102 + grow, 1190470 + grow}
	w.topics[topic] = partitions(c, 6*time.Second, [2]int64{0, ends[0]}, [2]int64{0, ends[1]}, [2]int64{0, ends[2]})
	w.topicCfg[topic] = topicConfig(topic, 3, "604800000")
	w.groups["user-profile-sync"] = group("user-profile-sync", "Empty", 0, topic,
		[2]int64{1184022, ends[0]}, [2]int64{1175868, ends[1]}, [2]int64{1187181, ends[2]})

	w.slots = []clients.ReplicationSlot{slot("debezium_users", true, 22*mib+300*1024, "reserved")}
	w.ddl = []clients.DDLChange{{
		At: ddlAt, CommandTag: "ALTER TABLE", ObjectIdentity: "public.users",
		Query: "ALTER TABLE public.users ALTER COLUMN loyalty_tier TYPE text USING loyalty_tier::text", Username: "app_migrator",
	}}
	w.pubs = pubs{{"public", "users", true, true}}.tables()
	w.whFresh["analytics.user_profiles"] = c.ago(71*time.Minute + 1*time.Second)
	return w
}

// ---- (d) shipments: healthy, a burst of updates is draining ----

func shipmentsSpec() spec {
	p := pipe{
		id: "shipments", tables: []string{"public.shipments"}, prefix: "shp",
		connectors: []string{"pg-shipments-connector"}, group: "shipment-tracker",
		sinks: []string{"analytics.shipments"}, tsCol: "updated_at",
		subjects: []string{"shp.public.shipments-key", "shp.public.shipments-value"},
	}
	calls := standardCalls(p)
	for i := range calls {
		if (calls[i].tool == "kafka_consumer_lag" || calls[i].tool == "kafka_topic_offsets") && !calls[i].wildcard {
			calls[i].seq = map[string]int{"": 3}
		}
	}
	return spec{
		file: "evals/scenarios/heldout/shipments-lag-burst-healthy.yaml", owners: heldoutOwners, pipeline: p.id,
		world: shipmentsWorld, calls: calls,
	}
}

func shipmentsWorld(c clock, _ string, tick int) *world {
	w := newWorld()
	w.addConnector("pg-shipments-connector", "source", "RUNNING", "connect-1:8083",
		debezium("shipping-db", "shipping", "public.shipments", "shp", "debezium_shipments"),
		task(0, "RUNNING", "connect-1:8083", ""))
	topic := "shp.public.shipments"
	w.subjects[topic+"-key"] = []schema{{id: 51, schema: keySchema(topic, avroField("id", avroLong))}}
	w.subjects[topic+"-value"] = []schema{{id: 52, schema: envelope(topic,
		avroField("id", avroLong), avroField("carrier", avroString), avroField("status", avroString), avroField("updated_at", avroZoned))}}

	// A 09:30 carrier-status import wrote ~62k updates in four minutes; the
	// three consumers work through them. Each read shows less lag.
	ends := [3]int64{30418220 + int64(tick)*40, 30402917 + int64(tick)*38, 30431006 + int64(tick)*41}
	lags := [3][3]int64{{16210, 15877, 16123}, {10561, 10433, 10883}, {4302, 4170, 4432}}[tick]
	w.topics[topic] = partitions(c, 4*time.Second, [2]int64{0, ends[0]}, [2]int64{0, ends[1]}, [2]int64{0, ends[2]})
	w.topicCfg[topic] = topicConfig(topic, 3, "604800000")
	w.groups["shipment-tracker"] = group("shipment-tracker", "Stable", 3, topic,
		[2]int64{ends[0] - lags[0], ends[0]}, [2]int64{ends[1] - lags[1], ends[1]}, [2]int64{ends[2] - lags[2], ends[2]})

	w.slots = []clients.ReplicationSlot{slot("debezium_shipments", true, 9*mib+600*1024, "reserved")}
	w.pubs = pubs{{"public", "shipments", true, true}}.tables()
	w.whFresh["analytics.shipments"] = c.ago(6*time.Minute + 40*time.Second)
	return w
}

// ---- (e) billing: Connect REST times out, the cause cannot be known ----

func billingSpec() spec {
	p := pipe{
		id: "billing", tables: []string{"public.invoices"}, prefix: "bill",
		connectors: []string{"pg-billing-connector"}, group: "invoice-loader",
		sinks: []string{"analytics.invoices"}, tsCol: "updated_at",
		subjects: []string{"bill.public.invoices-key", "bill.public.invoices-value"},
	}
	return spec{
		file: "evals/scenarios/heldout/billing-connect-unreachable.yaml", owners: heldoutOwners, pipeline: p.id,
		world: billingWorld, calls: standardCalls(p),
	}
}

func billingWorld(c clock, _ string, _ int) *world {
	w := newWorld()
	w.connectDown = true
	topic := "bill.public.invoices"
	w.subjects[topic+"-key"] = []schema{{id: 61, schema: keySchema(topic, avroField("id", avroLong))}}
	w.subjects[topic+"-value"] = []schema{{id: 62, schema: envelope(topic,
		avroField("id", avroLong), avroField("customer_id", avroLong), avroField("total", avroDecimal(14, 2)), avroField("updated_at", avroZoned))}}
	stale := 26*time.Minute + 30*time.Second
	w.topics[topic] = partitions(c, stale, [2]int64{0, 2210493}, [2]int64{0, 2208871})
	w.topicCfg[topic] = topicConfig(topic, 2, "604800000")
	w.groups["invoice-loader"] = group("invoice-loader", "Stable", 1, topic, [2]int64{2210493, 2210493}, [2]int64{2208871, 2208871})
	w.slots = []clients.ReplicationSlot{slot("debezium_billing", true, 2*gib+400*mib, "extended")}
	w.pubs = pubs{{"public", "invoices", true, true}}.tables()
	w.whFresh["analytics.invoices"] = c.ago(stale + 2*time.Second)
	return w
}

// ---- (f) ledger: task failed on a producer timeout; restart, then verify ----

func ledgerSpec() spec {
	p := pipe{
		id: "ledger", tables: []string{"public.ledger_entries"}, prefix: "ldg",
		connectors: []string{"pg-ledger-connector"}, group: "ledger-sink",
		sinks: []string{"analytics.ledger_entries"}, tsCol: "posted_at",
		subjects: []string{"ldg.public.ledger_entries-key", "ldg.public.ledger_entries-value"},
	}
	calls := standardCalls(p)
	for i := range calls {
		if calls[i].wildcard {
			continue
		}
		switch calls[i].tool {
		case "pg_replication_slots", "kafka_consumer_lag", "kafka_topic_offsets", "warehouse_freshness":
			// Recovery is visible over successive reads.
			calls[i].seq = map[string]int{"recovered": 3}
		}
	}
	return spec{
		file: "evals/scenarios/heldout/ledger-producer-timeout-recovery.yaml", owners: heldoutOwners, pipeline: p.id,
		phases: []string{"broken", "recovered"}, world: ledgerWorld, calls: calls,
	}
}

const producerTimeoutTrace = `org.apache.kafka.connect.errors.ConnectException: Unrecoverable exception from producer send callback
	at org.apache.kafka.connect.runtime.WorkerSourceTask.maybeThrowProducerSendException(WorkerSourceTask.java:312)
	at org.apache.kafka.connect.runtime.WorkerSourceTask.prepareToSendRecord(WorkerSourceTask.java:126)
	at org.apache.kafka.connect.runtime.AbstractWorkerSourceTask.sendRecords(AbstractWorkerSourceTask.java:395)
Caused by: org.apache.kafka.common.errors.TimeoutException: Expiring 412 record(s) for ldg.public.ledger_entries-1:120001 ms has passed since batch creation`

func ledgerWorld(c clock, phase string, tick int) *world {
	w := newWorld()
	failedAt := 47*time.Minute + 20*time.Second
	cfg := debezium("ledger-db", "ledger", "public.ledger_entries", "ldg", "debezium_ledger")
	topic := "ldg.public.ledger_entries"
	w.subjects[topic+"-key"] = []schema{{id: 71, schema: keySchema(topic, avroField("id", avroString))}}
	w.subjects[topic+"-value"] = []schema{{id: 72, schema: envelope(topic,
		avroField("id", avroString), avroField("account_id", avroLong), avroField("amount", avroDecimal(18, 2)),
		avroField("currency", avroString), avroField("posted_at", avroZoned))}}
	w.topicCfg[topic] = topicConfig(topic, 3, "1209600000")
	w.pubs = pubs{{"public", "ledger_entries", true, true}}.tables()
	w.srcRows["public.ledger_entries"] = map[string]map[string]any{
		"LE-2026-10-11-0048817": {"id": "LE-2026-10-11-0048817", "posted_at": ts(c.ago(12 * time.Minute)), "updated_at": ts(c.ago(12 * time.Minute))},
	}
	w.whRows["analytics.ledger_entries"] = map[string]map[string]any{}

	ends := [3]int64{4410233, 4398120, 4421907}
	if phase == "broken" {
		w.addConnector("pg-ledger-connector", "source", "RUNNING", "connect-1:8083", cfg,
			task(0, "FAILED", "connect-1:8083", producerTimeoutTrace))
		w.topics[topic] = partitions(c, failedAt, [2]int64{0, ends[0]}, [2]int64{0, ends[1]}, [2]int64{0, ends[2]})
		w.groups["ledger-sink"] = group("ledger-sink", "Stable", 2, topic,
			[2]int64{ends[0], ends[0]}, [2]int64{ends[1], ends[1]}, [2]int64{ends[2], ends[2]})
		w.slots = []clients.ReplicationSlot{slot("debezium_ledger", false, 4*gib+600*mib, "extended")}
		w.whFresh["analytics.ledger_entries"] = c.ago(failedAt + 4*time.Second)
		return w
	}

	// Recovered: the task drains the WAL the slot kept; the sink catches up.
	w.addConnector("pg-ledger-connector", "source", "RUNNING", "connect-1:8083", cfg,
		task(0, "RUNNING", "connect-1:8083", ""))
	produced := [3]int64{9800, 10450, 10690}[tick]
	lag := [3][3]int64{{6212, 6080, 6110}, {3101, 2987, 3025}, {72, 64, 78}}[tick]
	cur := [3]int64{ends[0] + produced, ends[1] + produced, ends[2] + produced}
	w.topics[topic] = partitions(c, 2*time.Second, [2]int64{0, cur[0]}, [2]int64{0, cur[1]}, [2]int64{0, cur[2]})
	w.groups["ledger-sink"] = group("ledger-sink", "Stable", 2, topic,
		[2]int64{cur[0] - lag[0], cur[0]}, [2]int64{cur[1] - lag[1], cur[1]}, [2]int64{cur[2] - lag[2], cur[2]})
	retained := [3]int64{3*gib + 900*mib, 2*gib + 100*mib, 412 * mib}[tick]
	walStatus := [3]string{"extended", "extended", "reserved"}[tick]
	w.slots = []clients.ReplicationSlot{slot("debezium_ledger", true, retained, walStatus)}
	behind := [3]time.Duration{41*time.Minute + 8*time.Second, 22*time.Minute + 51*time.Second, 1*time.Minute + 14*time.Second}[tick]
	w.whFresh["analytics.ledger_entries"] = c.ago(behind)
	return w
}
