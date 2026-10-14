package similarincidents_test

import (
	"time"

	"piperoom/internal/events"
	"piperoom/internal/tools/similarincidents/fake"
)

// day returns 2026-<month>-<d> 10:00 UTC.
func day(m time.Month, d int) time.Time { return time.Date(2026, m, d, 10, 0, 0, 0, time.UTC) }

const connectSchemaTrace = `pg-orders-connector (source): RUNNING on 172.18.0.6:8083.
Task 0: FAILED:
org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler
Caused by: org.apache.kafka.connect.errors.DataException: Failed to serialize Avro data from topic pg.public.orders :
Caused by: org.apache.kafka.common.errors.SerializationException: Error registering Avro schema<schema>
Caused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Schema being registered is incompatible with an earlier schema for subject "pg.public.orders-value", details: [{errorType:'MISSING_UNION_BRANCH'}]; error code: 409
Rejected schema vs pg.public.orders-value v1:
Value.amount: int → bytes(decimal 12,2)`

// corpus is the history the hit-rate test searches: 22 closed rooms of the
// "orders" pipeline, plus one room of "payments" and one shadow room that must
// never be returned for an orders search. IDs say what the incident was.
func corpus() []fake.Room {
	return []fake.Room{
		{ID: "r-schema-409", Title: "orders stale: connector task failed", ClosedAt: day(8, 3),
			Resolution:  "Debezium registered an incompatible Avro value schema after ALTER COLUMN amount TYPE numeric. Set compatibility of pg.public.orders-value to NONE for one registration, restarted task 0, then put BACKWARD back after the migration plan was agreed.",
			ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: connectSchemaTrace}}},
		{ID: "r-schema-deleted", Title: "consumer deserialization failing: schema id 2 not found", ClosedAt: day(9, 14),
			Resolution:  "A schema version was hard-deleted from the Schema Registry by a cleanup script. Re-registered the same schema with its original id using IMPORT mode, restarted order-sync, and revoked delete rights from the script.",
			ToolResults: []fake.ToolResult{{Tool: "kafka_consumer_lag", IsError: true, Summary: "org.apache.kafka.common.errors.SerializationException: Error retrieving Avro schema for id 2\nCaused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Schema 2 not found; error code: 40403"}}},
		{ID: "r-slot-wal", Title: "pg_wal 48 GiB, disk 81%", ClosedAt: day(7, 21),
			Resolution: "The connector was left STOPPED, so replication slot debezium_orders was inactive and Postgres kept all WAL. Resumed the connector; WAL drained in 20 minutes."},
		{ID: "r-poison", Title: "order-sync lag growing on partition 1, pod restarting", ClosedAt: day(9, 2),
			Resolution:  "One record that was not Avro (no magic byte) crash-looped the consumer. After owner approval the offset was skipped; a dead-letter topic was added.",
			ToolResults: []fake.ToolResult{{Tool: "kafka_read", IsError: true, Summary: "org.apache.kafka.common.errors.SerializationException: Unknown magic byte!\n\tat io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer.getByteBuffer(AbstractKafkaAvroDeserializer.java:260)"}}},
		{ID: "r-retention", Title: "reconciliation: 1.2M rows differ", ClosedAt: day(6, 30),
			Resolution: "Topic retention of 6 hours deleted change events before the lagging consumer read them; the consumer silently resumed at the log start. Raised retention.ms to 7 days and re-snapshotted the warehouse table."},
		{ID: "r-publication-perm", Title: "task 0 FAILED: cannot update publication", ClosedAt: day(8, 19),
			Resolution:  "Table ownership changed during a migration. Debezium could not run ALTER PUBLICATION on start. A DBA granted ownership of orders to the debezium role; task 0 restarted.",
			ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: "pg-orders-connector (source): RUNNING.\nTask 0: FAILED:\norg.apache.kafka.connect.errors.ConnectException: Unable to update filtered publication dbz_publication\nCaused by: org.postgresql.util.PSQLException: ERROR: must be owner of table orders"}}},
		{ID: "r-column-dropped", Title: "analytics.orders stale for 34 minutes", ClosedAt: day(9, 25),
			Resolution: "A contract step dropped column amount from the source table. The value schema stayed compatible but order-sync could not build rows. Re-added the column and redeployed the consumer."},
		{ID: "r-sr-down", Title: "producers failing: Schema Registry returning 503", ClosedAt: day(8, 27),
			Resolution:  "The Schema Registry pod was OOMKilled under a burst of lookups. Raised its memory limit to 1Gi and restarted it.",
			ToolResults: []fake.ToolResult{{Tool: "schema_registry_versions", IsError: true, Summary: "io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Error while forwarding register schema request to the leader; error code: 50003"}}},
		{ID: "r-connect-oom", Title: "Kafka Connect worker restarting", ClosedAt: day(7, 8),
			Resolution:  "The worker ran out of heap while snapshotting a large table. Raised the Connect heap to 2G and lowered max.batch.size.",
			ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: "pg-orders-connector (source): RUNNING.\nTask 0: FAILED:\njava.lang.OutOfMemoryError: Java heap space\n\tat java.base/java.util.Arrays.copyOf(Arrays.java:3512)"}}},
		{ID: "r-rebalance", Title: "consumer group order-sync rebalancing in a loop", ClosedAt: day(9, 9),
			Resolution: "Warehouse writes were slow, so poll processing exceeded max.poll.interval.ms and the member was kicked out again and again. Raised the interval to 10 minutes and lowered the batch size."},
		{ID: "r-wh-lock", Title: "warehouse load for analytics.orders blocked", ClosedAt: day(7, 29),
			Resolution: "A long-running transaction held a lock on analytics.orders. Terminated that session and set lock_timeout on the load role."},
		{ID: "r-pg-failover", Title: "Debezium lost its replication connection after failover", ClosedAt: day(8, 11),
			Resolution:  "After the Postgres failover the replication slot did not exist on the new primary. Recreated the slot and ran a fresh initial snapshot.",
			ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: "pg-orders-connector (source): RUNNING.\nTask 0: FAILED:\norg.apache.kafka.connect.errors.ConnectException: Could not create PG connection\nCaused by: org.postgresql.util.PSQLException: ERROR: replication slot \"debezium_orders\" does not exist"}}},
		{ID: "r-cert-expiry", Title: "TLS handshake failures to Kafka brokers", ClosedAt: day(9, 20),
			Resolution:  "The broker listener certificate had expired. Rotated the certificates and rolled the brokers.",
			ToolResults: []fake.ToolResult{{Tool: "kafka_list_groups", IsError: true, Summary: "javax.net.ssl.SSLHandshakeException: PKIX path validation failed: java.security.cert.CertPathValidatorException: validity check failed"}}},
		{ID: "r-toast", Title: "orders update events missing the description field", ClosedAt: day(7, 15),
			Resolution: "Unchanged TOAST columns arrive as __debezium_unavailable_value. Set REPLICA IDENTITY FULL on the table so updates carry the full row."},
		{ID: "r-dup-key", Title: "warehouse upsert failing", ClosedAt: day(8, 8),
			Resolution:  "Two sink jobs were writing the same table and violated the unique constraint orders_pkey. Stopped the old job.",
			ToolResults: []fake.ToolResult{{Tool: "warehouse_freshness", IsError: true, Summary: "ERROR: duplicate key value violates unique constraint \"orders_pkey\""}}},
		{ID: "r-kafka-disk", Title: "Kafka broker 2 disk full", ClosedAt: day(6, 18),
			Resolution: "Log directories filled up because a debug topic had no retention. Expanded the volume and set retention on the debug topic."},
		{ID: "r-acl", Title: "order-sync cannot read its topic", ClosedAt: day(9, 5),
			Resolution:  "A topic ACL change removed read access for the consumer group. Restored the ACL for order-sync.",
			ToolResults: []fake.ToolResult{{Tool: "kafka_consumer_lag", IsError: true, Summary: "org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [pg.public.orders]"}}},
		{ID: "r-snapshot", Title: "initial snapshot of orders never finishes", ClosedAt: day(7, 3),
			Resolution: "An idle-in-transaction session on the source blocked the snapshot's table lock. Terminated the session; the snapshot completed."},
		{ID: "r-timezone", Title: "analytics timestamps shifted by 5.5 hours", ClosedAt: day(8, 22),
			Resolution: "The consumer treated Debezium's zoned timestamps as local time. Fixed the converter to read them as UTC and backfilled the shifted rows."},
		{ID: "r-decimal", Title: "order amounts differ by a few paise in the warehouse", ClosedAt: day(9, 28),
			Resolution: "decimal.handling.mode was double, so amounts were rounded. Switched the connector to precise and backfilled the warehouse."},
		{ID: "r-oldest", Title: "alert test page", ClosedAt: day(1, 5), Resolution: "False alarm from a test of the pager."},
		// Never returned for an orders search:
		{ID: "r-pay-schema", PipelineID: "payments", Title: "payments stale: connector task failed", ClosedAt: day(9, 30),
			Resolution: "Incompatible Avro schema on the payments topic; set compatibility to NONE and restarted the task.", ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: connectSchemaTrace}}},
		{ID: "r-shadow", Shadow: true, Title: "orders stale: connector task failed", ClosedAt: day(9, 29),
			Resolution: "Fixed by the team.", Layer: "kafka-connect",
			Handoff:     &events.HandoffSummary{WhatBroke: "SHADOWSECRET agent text", Why: "SHADOWSECRET"},
			ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: connectSchemaTrace + "\nSHADOWSECRET"}}},
	}
}

// labelled is one query with the room a good answer must have in its top 3.
type labelled struct {
	query string
	want  string
}

// queries paraphrase the incident instead of quoting it, the way an agent
// phrases a search from an alert it has just read.
var queries = []labelled{
	{"connector task failed after changing a column type, schema registry rejected the new schema", "r-schema-409"},
	{"consumers fail because schema id 2 cannot be found in the registry", "r-schema-deleted"},
	{"someone deleted a schema version from the registry", "r-schema-deleted"},
	{"WAL disk usage growing on postgres and the replication slot is inactive", "r-slot-wal"},
	{"consumer pod crash-looping on a single bad record, unknown magic byte", "r-poison"},
	{"rows missing in warehouse because topic retention was too short", "r-retention"},
	{"publication update fails: must be owner of table orders", "r-publication-perm"},
	{"warehouse table stale after migration removed a column", "r-column-dropped"},
	{"schema registry pod restarting, out of memory, clients get 503", "r-sr-down"},
	{"connect worker dies with java heap space error", "r-connect-oom"},
	{"order-sync keeps rebalancing, poll interval exceeded", "r-rebalance"},
	{"replication slot does not exist after database failover", "r-pg-failover"},
	{"ssl certificate expired, handshake to brokers fails", "r-cert-expiry"},
	{"update events lack a field, toast value unavailable", "r-toast"},
	{"duplicate key violates unique constraint during warehouse load", "r-dup-key"},
	{"not authorized to access topic pg.public.orders", "r-acl"},
	{"amounts rounded wrongly, decimal handling mode", "r-decimal"},
	{"timestamps in analytics are hours off", "r-timezone"},
	// A pasted trace: same failure as r-schema-409 on another worker and with
	// other ids, found by fingerprint and words.
	{"org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler\nCaused by: org.apache.kafka.connect.errors.DataException: Failed to serialize Avro data from topic pg.public.orders :\nCaused by: org.apache.kafka.common.errors.SerializationException: Error registering Avro schema<schema>\nCaused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Schema being registered is incompatible with an earlier schema for subject \"pg.public.orders-value\", details: [{errorType:'MISSING_UNION_BRANCH'}]; error code: 409", "r-schema-409"},
	{"java.lang.OutOfMemoryError: Java heap space\n\tat java.base/java.util.Arrays.copyOf(Arrays.java:3512)", "r-connect-oom"},
}
