package evals

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/tools"
	"piperoom/internal/tools/logtools"
)

// termCase is one hypothesis read against a scenario's root-cause terms.
type termCase struct {
	id   string
	text string
	// want is what rootCauseMatches must return.
	want bool
	// preexisting marks a trap text that already matched with the terms on
	// main before E5d; it is logged, not failed, so this test guards only what
	// E5d changed.
	preexisting bool
	// matcherLimit marks a trap text no term list can fail: the denial is
	// outside what the matcher reads (see evals/README.md, Known limits).
	matcherLimit bool
	// knownGap marks a correct text the terms cannot accept without also
	// accepting trap texts; the LLM judge decides it. want stays false.
	knownGap bool
}

// scenarioTermCases pin the E5d review of two scenarios' root-cause terms:
// the first live baseline's correct slot hypothesis (S0) must match, and the
// scenarios' trap answers must not. The baseline retention hypothesis (T0)
// stays a known gap.
var scenarioTermCases = map[string][]termCase{
	"replication-slot-lost-failover": {
		{id: "S0", want: true, text: "Failing: connect:pg-orders-connector task 0. Root cause: source Postgres failover to orders-db-2 (PG 16, per Priya) at 06:58:12Z did not carry logical slot debezium_orders. This matches the empty slot list in s_01M4YCQ7MDTXCTEBY7PHM2QCHG. One-off, not recurring; data gap on both sides of the failover."},
		{id: "S1", text: "The promoted standby orders-db-2 did not carry wal_level=logical from the old primary, so Debezium cannot open slot debezium_orders; set wal_level and restart the task."},
		{id: "S2", text: "After the failover, the Connect offsets of pg-orders-connector were not copied to the new worker, so task 0 asks for slot debezium_orders at a stale LSN; restart the connector."},
		{id: "S3", text: "The standby promotion did not carry the publication dbz_publication, so slot debezium_orders streams nothing; recreate the publication."},
		{id: "S4b", text: "The Postgres failover to orders-db-2 did not carry any risk for slot debezium_orders, which still exists on the new primary; task 0 failed on a transient network blip, so restart it."},
		{id: "S5b", text: "The empty slot list on the new Postgres primary is expected after a failover, since Debezium recreates slot debezium_orders on start; the real fault is a stale DNS entry, so restart the task."},
		{id: "S6", text: "The empty slot list is unrelated to the outage; after the failover task 0 hit a network blip, so restart it and it will recreate slot debezium_orders."},
		{id: "S7", text: "Logical slots are not copied to standbys by default, but this cluster uses PG 17 failover slots, so debezium_orders exists on the new primary; the task died on a network blip."},
		{id: "S8b", text: "Postgres logical slots are not carried to the standby."},
		{id: "S9", text: "Some on-call thought the failover did not carry slot debezium_orders, that is wrong: the slot is there and task 0 hit a network blip."},
		{id: "S11", text: "Was slot debezium_orders not copied in the failover? No, the task failed on a network blip to the new primary."},
		{id: "S12", text: "It is not the case that the failover did not carry slot debezium_orders; the task hit a network blip."},
		{id: "S13", text: "The empty slot list after the failover is a red herring; restart the task that reads slot debezium_orders."},
		{id: "S14", text: "I doubt the failover did not carry slot debezium_orders."},
		{id: "S15", text: `orders CDC: no change events for 22 min - pg-orders-connector task 0 FAILED (replication slot "debezium_orders" does not exist), analytics.orders stale`},
		{id: "S17", text: "The failover did not carry the replication user's password to orders-db-2, so authentication for slot debezium_orders fails; reset the password and restart the task."},
		{id: "S18", text: "Slot debezium_orders was not carried over by any manual step, it was synced by PG 17 failover slots and exists on the new primary; the task failed on a network blip."},
		// Slot-exists stories that the earlier "did not carry ... slot*"
		// phrases passed (review round 2).
		{id: "S19", text: "The failover to orders-db-2 did not carry the slot's confirmed_flush_lsn forward: slot debezium_orders exists on the new primary but at an older LSN than Connect's offset, so task 0 fails; advance the slot."},
		{id: "S20", text: "After the failover, slot debezium_orders exists on orders-db-2 but is inactive: the promotion did not carry the slot's consumer connection, so restart task 0 to reattach."},
		{id: "S21", text: "Before PG 17, a failover did not carry logical slots, but orders-db-2 runs PG 17 with sync_replication_slots on, so slot debezium_orders exists there; task 0 failed on a network blip."},
		{id: "S22", text: "In PG 16 a promoted standby did not carry logical slots by default; here slot debezium_orders was synced and is active, so the task died on a network blip; restart it."},
		{id: "S23", text: "The failover did not carry slot debezium_orders's restart_lsn, so the slot exists on the new primary but begins at the promotion point; restart task 0."},
	},
	"topic-retention-data-loss": {
		// Retention deleting unread changes cannot be told from normal
		// retention by terms (see T17-T22); the judge decides.
		{id: "T0", knownGap: true, text: "Failing: wh:analytics.orders (stale, old row versions). Root cause: consumer group order-sync fell behind by more than the retention of topic pg.public.orders. Retention deleted the 01:00-03:00 change events before order-sync read them. The group now sits a few hundred records above each log start, which points to a reset to earliest."},
		{id: "T1", text: "Retention is fine: nothing was deleted before order-sync read it. order-sync is just slow after the re-pricing job; scale it out and wait."},
		{id: "T2", text: "Retention did not delete anything before order-sync read it; the group is Stable and lag is falling, so order-sync is just slow."},
		{id: "T3", text: "order-sync's committed offsets sit just above the log start because it restarted; the re-pricing job finished before order-sync consumed the backlog, so it is merely slow; wait for it to catch up."},
		{id: "T4b", text: "The warehouse loader's staging files were deleted before being read, so analytics.orders keeps old row versions; rerun the load."},
		{id: "T5b", text: "The re-priced rows in Postgres were deleted before being read by Debezium, so their changes never left the source."},
		{id: "T6", text: "A bug in order-sync's deduplication discarded the re-pricing changes before they were read into the batch; committed offsets near the log start are normal after its restart."},
		{id: "T8", text: "Were the change events deleted before order-sync read them? The group is Stable and lag is falling, so order-sync is just slow."},
		{id: "T9", text: "I doubt retention deleted anything before order-sync read it; order-sync is just slow."},
		{id: "T10", text: "No records expired before order-sync read them; order-sync is just slow."},
		// Fails through root_cause_forbidden ("reset ... latest").
		{id: "T11", text: "Retention deleted changes before order-sync read them; reset order-sync to latest to catch up."},
		{id: "T12", text: "orders reconciliation: 1,181,433 of 7,912,440 orders changed 01:00-03:00 UTC differ in analytics.orders (sample ids 3301877, 4410023, 5120554)"},
		{id: "T13", text: "The 01:00-03:00 changes were not deleted by retention before order-sync read them; they are still in the topic and order-sync is just slow."},
		{id: "T14", text: "Schema id 2 expired from order-sync's registry cache, so the re-pricing records were dropped before being read; restart order-sync."},
		{id: "T15", text: "Debezium's heartbeat expired during the 7.9 M-row re-pricing burst and Connect discarded the batch before the consumer read it; restart the connector."},
		{id: "T16", text: "order-sync crashed during the re-pricing burst and the in-memory batch was dropped before being read into the warehouse; its committed offsets near the log start are fine, restart order-sync."},
		// Slow-consumer and no-loss stories that "retention deleted ..."
		// phrases passed (review round 2).
		{id: "T17", text: "Retention deleted the expired segments as configured, no data loss; order-sync is just slow."},
		{id: "T18", text: "Retention deleted the old segments that order-sync had already read; nothing unread was touched, order-sync is just slow after the re-pricing burst."},
		{id: "T19", text: "Kafka retention deleted the segments older than 7 days as configured; the 01:00-03:00 changes are still in the topic and order-sync will reach them; scale it out."},
		{id: "T20", text: "Retention deleted those segments long after order-sync committed past them, so retention is not the cause; order-sync is just slow."},
		{id: "T21", text: "Postgres WAL retention deleted the segments before Debezium read them, so the re-pricing changes never left the source; raise max_slot_wal_keep_size."},
		{id: "T22", text: "Someone reset order-sync to earliest, so it is replaying everything since the log start; retention deleted the pre-incident segments long ago, which is normal; the old row versions are replayed writes that will be overwritten as it catches up."},
	},
	// #362: the trap rows #341 changed, pinned with natural trap wordings
	// (P*: the forbidden row must fire) and the correct wordings the old rows
	// failed (C*: must still match).
	"snapshot-stuck": {
		{id: "P1", text: "The initial snapshot of public.orders is still running; the root cause is an inactive replication slot."},
		{id: "P2", text: "The slot was inactive, so the initial snapshot is still running."},
		{id: "P3", text: "The initial snapshot is still in progress. The slot appears inactive and holds WAL."},
		{id: "P4", text: "The slot has become inactive so the initial snapshot cannot progress; it is still running."},
	},
	"broker-disk-notenoughreplicas": {
		{id: "P1", text: "The broker disk is full, so the ISR shrank under min.insync.replicas; lower the topic's min.insync.replicas to 1."},
		{id: "P2", text: "Broker 2's log dir is out of space and the ISR fell under min.insync.replicas; fix by lowering the min.insync.replicas to unblock writes."},
		{id: "P3", text: "The disk is full on broker 2, so the ISR is under min.insync.replicas; set min.insync.replicas=1 until the disk is cleaned."},
		{id: "C1", want: true, text: "Broker 2's log dir is full, so the ISR fell lower than min.insync.replicas=2 and producers get NotEnoughReplicas; free disk space on broker 2."},
	},
	"watcher-start": {
		{id: "P1", text: "Postgres was restarted for maintenance and the slot has been lost."},
		{id: "P2", text: "The primary rebooted and the slot got lost."},
		{id: "P3", text: "Postgres restarted: slot lost."},
		{id: "P4", text: "The database was restarted after a ddl statement changed the schema."},
		{id: "P5", text: "The replication slot was lost when Postgres restarted for maintenance."},
		{id: "C1", want: true, text: "Postgres was restarted for maintenance (administrator command, 57P01); nothing is lost."},
		{id: "C2", want: true, text: "The primary rebooted for an upgrade; pg_recent_ddl shows nothing changed."},
		// A comma after "is lost" ends the part: no "lost the slot" story.
		{id: "C3", want: true, text: "Postgres was restarted for maintenance (administrator command, 57P01); nothing is lost, the slot is still there."},
	},
	"snowflake-sink-column-missing": {
		{id: "P1", text: "The schema change was incompatible, so the Snowflake table is missing risk_score."},
		{id: "P2", text: "The registry flagged risk_score as incompatible, and the table is missing the column."},
		{id: "P3", text: "The schema change to risk_score is incompatible with the sink, which is missing the column."},
		{id: "C1", want: true, text: "analytics.orders does not have the risk_score column the new records carry; there is nothing incompatible in the schema."},
	},
	"slot-inactive-wal-growth": {
		{id: "P1", text: "pg-orders-connector task 0 failed and stopped, so slot debezium_orders is inactive and WAL is retained."},
		{id: "P2", text: "A failed task stopped pg-orders-connector, so the slot is inactive and Postgres retains WAL."},
		{id: "C1", want: true, text: "pg-orders-connector was stopped (0 failed tasks), so slot debezium_orders is inactive and Postgres retains 48.6 GiB of WAL; resume the connector."},
	},
	"avro-schema-id-deleted-sink-failed": {
		{id: "P1", text: "The ADD COLUMN discount_code DDL broke Avro compatibility; schema id 7 was deleted from the registry."},
		{id: "P2", text: "Schema id 7 was hard deleted from the registry subject; the discount_code DDL broke the sink."},
		{id: "C1", want: true, text: "Schema id 7 (v3) was permanently deleted from the registry subject, so the sink cannot decode records written with it. The DDL broke nothing."},
	},
	"sink-connector-paused-warehouse-stale": {
		{id: "P1", text: "The wh-orders-sink connector paused itself after write errors, so the warehouse is stale; it was not a manual pause for the migration."},
		{id: "P2", text: "wh-orders-sink was paused after errors during the warehouse migration."},
		{id: "C1", want: true, text: "Meera paused the wh-orders-sink connector on purpose for a migration, with 0 errors and no failed tasks; resume it when the migration is done."},
	},
}

// scenarioTermShould are correct paraphrases the terms ideally accept. They
// are logged, not failed: the deterministic grader is a floor and the LLM
// judge is authoritative for wordings it misses.
var scenarioTermShould = map[string][]string{
	"replication-slot-lost-failover": {
		"the failover didn't carry logical slot debezium_orders",
		"the promoted standby has no slot debezium_orders",
	},
	"topic-retention-data-loss": {
		"retention deleted the change events before order-sync could read them",
		"records expired before they were consumed by order-sync",
	},
}

func TestScenarioRootCauseTerms(t *testing.T) {
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]Scenario{}
	for _, sc := range scs {
		byName[sc.Name] = sc
	}
	for name, cases := range scenarioTermCases {
		sc, ok := byName[name]
		if !ok {
			t.Fatalf("scenario %q not found", name)
		}
		for _, c := range cases {
			got := rootCauseMatches(sc.Expect, c.text)
			switch {
			case c.knownGap && !got:
				t.Logf("%s %s: known gap, not matched by terms (the judge decides): %q", name, c.id, c.text)
			case got == c.want:
			case c.preexisting, c.matcherLimit, c.knownGap:
				t.Logf("%s %s: matches = %v (known: preexisting=%v matcherLimit=%v knownGap=%v)", name, c.id, got, c.preexisting, c.matcherLimit, c.knownGap)
			default:
				t.Errorf("%s %s: matches = %v, want %v: %q", name, c.id, got, c.want, c.text)
			}
		}
		for _, text := range scenarioTermShould[name] {
			if !rootCauseMatches(sc.Expect, text) {
				t.Logf("%s: correct paraphrase not matched (the judge covers it): %q", name, text)
			}
		}
	}
}

// TestBlockedSlotRecordingPrintsStartedAt pins W4e-2 (KAN-503): the blocked-slot
// source_blocking_sessions recording prints started_at (RFC3339Nano UTC, after
// blocks_slot=) as source_blocking_sessions now does, and that exact string is
// the start time source_end_session accepts and the recorded data carries (the
// action refuses a pid whose start differs).
func TestBlockedSlotRecordingPrintsStartedAt(t *testing.T) {
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	var sc *Scenario
	for i := range scs {
		if scs[i].Name == "blocked-slot" {
			sc = &scs[i]
		}
	}
	if sc == nil {
		t.Fatal("scenario blocked-slot not found")
	}
	var rec *Recording
	for i := range sc.Recordings {
		if sc.Recordings[i].Tool == "source_blocking_sessions" {
			rec = &sc.Recordings[i]
		}
	}
	if rec == nil {
		t.Fatal("blocked-slot has no source_blocking_sessions recording")
	}
	m := regexp.MustCompile(`^pid=48213 state=active age=25m blocks_slot=yes started_at=(\S+) \(outside the pipeline's tables; identity hidden\)$`).FindStringSubmatch(rec.Summary)
	if m == nil {
		t.Fatalf("summary does not match the W4e format: %q", rec.Summary)
	}
	started, err := time.Parse(time.RFC3339Nano, m[1])
	if err != nil {
		t.Fatalf("started_at %q is not RFC3339: %v", m[1], err)
	}
	if want := time.Date(2026, 10, 23, 9, 14, 12, 0, time.UTC); !started.Equal(want) || !strings.HasSuffix(m[1], "Z") {
		t.Errorf("started_at = %q, want %s in UTC", m[1], want.Format(time.RFC3339Nano))
	}
	rows, ok := rec.Data.([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("recorded data = %#v, want one row", rec.Data)
	}
	row, _ := rows[0].(map[string]any)
	if got, _ := row["started_at"].(string); got != m[1] {
		t.Errorf("data started_at = %q, summary started_at = %q", got, m[1])
	}
	if age := time.Date(2026, 10, 23, 9, 40, 0, 0, time.UTC).Sub(started); age < 25*time.Minute || age >= 26*time.Minute {
		t.Errorf("age at the scenario clock = %s, want 25m", age)
	}
	// source_end_session decodes started_at into a time.Time.
	var p struct {
		StartedAt time.Time `json:"started_at"`
	}
	if err := json.Unmarshal([]byte(`{"started_at":"`+m[1]+`"}`), &p); err != nil || !p.StartedAt.Equal(started) {
		t.Errorf("started_at does not decode as an end-session param: %v", err)
	}
}

// w4lMiss is one replay miss of run 39994729845 (the W4-0 table, reviews/evals-2026-10-23/misses.md,
// KAN-490): a tool and its exact input.
type w4lMiss struct{ tool, input string }

// w4lMisses are the distinct misses of the six W4l scenarios (KAN-496), copied from the run's
// results[].misses: 124 misses in all, 68 distinct tool and input pairs.
var w4lMisses = map[string][]w4lMiss{
	"removed-decoder-plugin": {
		{"connect_worker_logs", `{"connector":"pg-orders-connector","since":"2h"}`},
		{"net_probe", `{"target":"source"}`},
		{"connect_validate_config", `{"base_connector":"pg-orders-connector","class":"io.debezium.connector.postgresql.PostgresConnector","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.dbname":"orders","database.hostname":"postgres","database.port":"5432","database.user":"piperoom","key.converter":"io.confluent.connect.avro.AvroConverter","slot.name":"debezium_orders","table.include.list":"public.orders","topic.prefix":"pg","value.converter":"io.confluent.connect.avro.AvroConverter"}}`},
		{"net_probe", `{"target":"connect"}`},
		{"pg_publication_tables", `{"pipeline_id":"orders"}`},
		{"connect_validate_config", `{"base_connector":"pg-orders-connector","class":"io.debezium.connector.postgresql.PostgresConnector","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.dbname":"orders","database.hostname":"postgres","database.port":"5432","database.user":"piperoom","key.converter":"io.confluent.connect.avro.AvroConverter","plugin.name":"pgoutput","publication.autocreate.mode":"filtered","publication.name":"dbz_publication","slot.name":"debezium_orders_pgoutput","table.include.list":"public.orders","topic.prefix":"pg","value.converter":"io.confluent.connect.avro.AvroConverter"}}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","since":"1h","task":0}`},
		{"net_probe", `{"target":"schema-registry"}`},
		{"connect_worker_metrics", `{"connector":"pg-orders-connector"}`},
		{"connect_source_offsets", `{"connector":"pg-orders-connector"}`},
		{"connect_validate_config", `{"base_connector":"pg-orders-connector","class":"io.debezium.connector.postgresql.PostgresConnector","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.dbname":"orders","database.hostname":"postgres","database.port":"5432","database.user":"piperoom","key.converter":"io.confluent.connect.avro.AvroConverter","plugin.name":"pgoutput","publication.autocreate.mode":"filtered","publication.name":"dbz_orders","slot.name":"debezium_orders_pgoutput","table.include.list":"public.orders","topic.prefix":"pg","value.converter":"io.confluent.connect.avro.AvroConverter"}}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","since":"1h"}`},
		{"connect_validate_config", `{"base_connector":"pg-orders-connector","class":"io.debezium.connector.postgresql.PostgresConnector","config":{"name":"pg-orders-connector"}}`},
		{"connect_worker_info", `{}`},
		{"connect_validate_config", `{"base_connector":"pg-orders-connector","class":"io.debezium.connector.postgresql.PostgresConnector","config":{"name":"pg-orders-connector","plugin.name":"pgoutput","publication.autocreate.mode":"filtered","publication.name":"dbz_orders","slot.name":"debezium_orders_pgoutput"}}`},
	},
	"renamed-property-after-upgrade": {
		{"pg_streaming_liveness", `{"pipeline_id":"orders"}`},
		{"kafka_cluster_health", `{}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","since":"1h"}`},
		{"net_probe", `{"target":"kafka"}`},
		{"connect_worker_metrics", `{"connector":"pg-orders-connector"}`},
		{"kafka_acls", `{}`},
		{"connect_validate_config", `{"base_connector":"pg-orders-connector","class":"io.debezium.connector.postgresql.PostgresConnector","config":{"heartbeat.topics.prefix":"heartbeats","topic.heartbeat.prefix":"heartbeats"}}`},
		{"pg_streaming_liveness", `{"pipeline_id":"orders","window_seconds":8}`},
		{"source_blocking_sessions", `{"older_than_minutes":30}`},
		{"source_blocking_sessions", `{}`},
		{"connect_validate_config", `{"base_connector":"pg-orders-connector","class":"io.debezium.connector.postgresql.PostgresConnector","config":{"heartbeat.topics.prefix":"heartbeats"}}`},
		{"connect_validate_config", `{"base_connector":"pg-orders-connector","class":"io.debezium.connector.postgresql.PostgresConnector","config":{"topic.heartbeat.prefix":"heartbeats"}}`},
	},
	"avro-schema-id-deleted-sink-failed": {
		{"connect_worker_info", `{}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","level":"INFO","limit":50,"since":"72h"}`},
		{"connect_worker_logs", `{"connector":"wh-orders-sink","since":"24h"}`},
		{"connect_worker_logs", `{"connector":"wh-orders-sink","limit":30,"since":"6h"}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","level":"INFO","limit":50,"since":"6h"}`},
		{"net_probe", `{"target":"schema-registry"}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","level":"INFO","limit":60,"since":"3h"}`},
		{"connect_worker_logs", `{"connector":"wh-orders-sink","limit":40,"since":"3h"}`},
	},
	"rows-lost-after-retention": {
		{"pg_connector_scope_gap", `{"pipeline_id":"orders"}`},
		{"pg_streaming_liveness", `{"pipeline_id":"orders"}`},
		{"debezium_metrics", `{"connector":"pg-orders-connector"}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","since":"2h"}`},
		{"kafka_topic_config", `{"pipeline_id":"orders"}`},
		{"connect_source_offsets", `{"connector":"pg-orders-connector"}`},
		{"kafka_throughput", `{"window":"1h"}`},
		{"kafka_throughput", `{"table":"public.orders","window":"1h"}`},
		{"source_blocking_sessions", `{"older_than_minutes":5}`},
		{"kafka_partition_rates", `{"window":"1h"}`},
		{"warehouse_load_errors", `{"since":"2026-10-22T08:55:00Z","table":"analytics.orders"}`},
	},
	"replication-slot-lost-failover": {
		{"source_settings", `{}`},
		{"connect_source_offsets", `{"connector":"pg-orders-connector"}`},
		{"net_probe", `{"target":"source"}`},
		{"reconcile", `{"since":"2026-10-10T06:40:00Z","table":"public.orders"}`},
		{"source_role_grants", `{"pipeline_id":"orders"}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","since":"2h"}`},
	},
	"snapshot-stuck": {
		{"connect_worker_metrics", `{"connector":"wh-orders-sink"}`},
		{"kafka_throughput", `{"window":"30m"}`},
		{"pg_streaming_liveness", `{"pipeline_id":"orders","window_seconds":8}`},
		{"debezium_snapshot_progress", `{"connector":"pg-orders-connector"}`},
		{"connect_source_offsets", `{"connector":"pg-orders-connector"}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","level":"INFO","limit":60,"since":"6h"}`},
		{"source_settings", `{}`},
		{"source_blocking_sessions", `{"older_than_minutes":5}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","level":"INFO","limit":100,"since":"4h"}`},
		{"kafka_throughput", `{"window":"15m"}`},
		{"connect_worker_logs", `{"connector":"wh-orders-sink","since":"4h"}`},
		{"pg_streaming_liveness", `{"pipeline_id":"orders"}`},
		{"source_blocking_sessions", `{"older_than_minutes":60}`},
		{"reconcile", `{"since":"2026-10-14T01:00:00Z","table":"public.orders"}`},
		{"source_blocking_sessions", `{}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","level":"INFO","limit":100,"since":"6h"}`},
	},
}

// w4lUnrecorded are the reads replication-slot-lost-failover leaves as replay misses on purpose: any fix is
// forbidden there, and a stored offset or clean CDC settings read as "the resume checks pass".
func w4lUnrecorded(scenario, tool string) bool {
	return scenario == "replication-slot-lost-failover" && (tool == "connect_source_offsets" || tool == "source_settings")
}

func w4lScenarioByName(t *testing.T, name string) Scenario {
	t.Helper()
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	for _, sc := range scs {
		if sc.Name == name {
			return sc
		}
	}
	t.Fatalf("scenario %q not found", name)
	return Scenario{}
}

// TestW4lMissesNowResolve replays every miss W4-0 lists for the six scenarios against the scenario's
// recordings: each now has an answer (KAN-496 test case 2), except the two reads the slot-lost scenario
// leaves unanswered by design.
func TestW4lMissesNowResolve(t *testing.T) {
	total := 0
	for name, misses := range w4lMisses {
		sc := w4lScenarioByName(t, name)
		rp, err := newReplayer(sc.Recordings)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, m := range misses {
			total++
			_, ok := rp.lookup(m.tool, json.RawMessage(m.input))
			switch {
			case w4lUnrecorded(name, m.tool) && ok:
				t.Errorf("%s: %s %s has a recording; the scenario must leave it unanswered", name, m.tool, m.input)
			case !w4lUnrecorded(name, m.tool) && !ok:
				t.Errorf("%s: %s %s still has no recording", name, m.tool, m.input)
			}
		}
	}
	if total != 68 {
		t.Errorf("checked %d distinct misses, want the 68 of the W4-0 table (124 misses)", total)
	}
}

// TestW4lSlotLostRecordsNoPassingOffsetsOrSettings is the negative test of KAN-496 test case 2: no recording
// of replication-slot-lost-failover returns a stored offset or the CDC settings, and a fix stays forbidden.
func TestW4lSlotLostRecordsNoPassingOffsetsOrSettings(t *testing.T) {
	sc := w4lScenarioByName(t, "replication-slot-lost-failover")
	passing := regexp.MustCompile(`\b(wal_level|max_replication_slots|max_wal_senders|max_slot_wal_keep_size|lsn_proc)\b|\blsn=`)
	for _, rec := range sc.Recordings {
		if rec.Tool == "connect_source_offsets" || rec.Tool == "source_settings" {
			t.Errorf("recording of %s: the scenario forbids every fix, so it records no offsets or settings", rec.Tool)
		}
		texts := []string{rec.Summary}
		for _, o := range rec.Sequence {
			texts = append(texts, o.Summary)
		}
		for _, s := range texts {
			if passing.MatchString(s) {
				t.Errorf("recording of %s returns offsets or settings text: %q", rec.Tool, s)
			}
		}
	}
	rp, err := newReplayer(sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ tool, input string }{
		{"connect_source_offsets", `{}`},
		{"connect_source_offsets", `{"connector":"pg-orders-connector"}`},
		{"source_settings", `{}`},
	} {
		if _, ok := rp.lookup(c.tool, json.RawMessage(c.input)); ok {
			t.Errorf("%s %s is answered", c.tool, c.input)
		}
	}
	// A fix is still forbidden: no fix is expected, and the restarts and the resume stay listed.
	if len(sc.Expect.Fix) != 0 {
		t.Errorf("expect.fix = %v, want none", sc.Expect.Fix)
	}
	for _, a := range []string{"connect_restart_task", "connect_restart_connector", "connect_resume_connector", "schema_register", "schema_set_compatibility"} {
		if !slices.Contains(sc.Expect.ForbiddenActions, a) {
			t.Errorf("forbidden_actions lacks %s", a)
		}
	}
	// R1 (tasks/W4-evals-to-100.md) would make the first failing layer kafka-connect; that relabel waits for a
	// grant on evals/agreement_test.go, so the root cause is what is pinned here.
	if sc.Expect.RootCauseLayer != "postgres" {
		t.Errorf("root_cause_layer = %q, want postgres", sc.Expect.RootCauseLayer)
	}
	// The 3-day reconcile window is the real tool's refusal on the scenario clock (2026-10-13T07:20Z).
	got, ok := rp.lookup("reconcile", json.RawMessage(`{"since":"2026-10-10T06:40:00Z","table":"public.orders"}`))
	if !ok {
		t.Fatal("reconcile over three days has no recording")
	}
	window := time.Date(2026, 10, 13, 7, 20, 0, 0, time.UTC).Sub(time.Date(2026, 10, 10, 6, 40, 0, 0, time.UTC))
	if want := "bad tool input: the window is " + window.String() + ", at most 6h0m0s"; got.obs[0].err != want {
		t.Errorf("reconcile error = %q, want %q", got.obs[0].err, want)
	}
}

// TestW4lBudgets pins the budgets (KAN-496 test case 3) and that each carries a derivation per input turn:
// one "turn N" for the alert (0) and for each of the scenario's inputs, ending in "+ 2 = <budget>".
func TestW4lBudgets(t *testing.T) {
	cases := []struct {
		name         string
		want, lo, hi int
	}{
		{"removed-decoder-plugin", 16, 14, 16},
		{"renamed-property-after-upgrade", 15, 14, 16},
		{"avro-schema-id-deleted-sink-failed", 18, 18, 18}, // unchanged: the diff loop is not budgeted for
		{"rows-lost-after-retention", 20, 20, 20},          // about 20 as consumer-skipped-range; never 21 or 22
		{"replication-slot-lost-failover", 13, 13, 13},     // highest passing run 11 + 2
		{"snapshot-stuck", 15, 14, 15},
	}
	plus := regexp.MustCompile(`\+ 2 = (\d+)`)
	for _, c := range cases {
		sc := w4lScenarioByName(t, c.name)
		if got := sc.Expect.MaxToolCalls; got != c.want || got < c.lo || got > c.hi {
			t.Errorf("%s: max_tool_calls = %d, want %d (range %d-%d)", c.name, got, c.want, c.lo, c.hi)
		}
		raw, err := os.ReadFile("scenarios/" + c.name + ".yaml")
		if err != nil {
			t.Fatal(err)
		}
		header, _, ok := strings.Cut(string(raw), "\nname: ")
		if !ok {
			t.Fatalf("%s: no name line", c.name)
		}
		loc := regexp.MustCompile(`W4l[ ,(]`).FindStringIndex(header)
		if loc == nil {
			t.Errorf("%s: no W4l derivation in the header", c.name)
			continue
		}
		derivation := header[loc[0]:]
		for turn := 0; turn <= len(sc.Inputs); turn++ {
			if !regexp.MustCompile(`turn ` + strconv.Itoa(turn) + `\b`).MatchString(derivation) {
				t.Errorf("%s: the derivation has no entry for turn %d (the alert, then %d inputs)", c.name, turn, len(sc.Inputs))
			}
		}
		m := plus.FindAllStringSubmatch(derivation, -1)
		if len(m) == 0 || m[len(m)-1][1] != strconv.Itoa(c.want) {
			t.Errorf("%s: the derivation does not end in \"+ 2 = %d\"", c.name, c.want)
		}
	}
}

// TestW4lWorkerLogsRecordingIsTheRealAnswer runs the real connect_worker_logs over each scenario's pipeline on a
// server without a log store and compares it with the recording: the tool's own words, for any window or level.
func TestW4lWorkerLogsRecordingIsTheRealAnswer(t *testing.T) {
	for name := range w4lMisses {
		sc := w4lScenarioByName(t, name)
		own, err := loadOwnersFor(sc)
		if err != nil {
			t.Fatal(err)
		}
		reg := tools.NewRegistry()
		if err := logtools.Register(reg, logtools.Deps{Owners: own}); err != nil {
			t.Fatal(err)
		}
		tool, _ := reg.Get("connect_worker_logs")
		rp, err := newReplayer(sc.Recordings)
		if err != nil {
			t.Fatal(err)
		}
		ctx := tools.WithPipeline(context.Background(), sc.Pipeline)
		for _, in := range []string{`{"connector":"` + sc.Pipeline.Connectors[0] + `"}`, `{"connector":"` + sc.Pipeline.Connectors[0] + `","since":"6h","level":"INFO","limit":50}`} {
			res, err := tool.Run(ctx, json.RawMessage(in))
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			rec, ok := rp.lookup("connect_worker_logs", json.RawMessage(in))
			if !ok {
				t.Fatalf("%s: connect_worker_logs %s has no recording", name, in)
			}
			if rec.obs[0].summary != res.Summary {
				t.Errorf("%s: recording %q, the real tool says %q", name, rec.obs[0].summary, res.Summary)
			}
		}
	}
}

// w4lSummary is the summary the scenario answers tool and input with.
func w4lSummary(t *testing.T, sc Scenario, tool, input string) string {
	t.Helper()
	rp, err := newReplayer(sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	rec, ok := rp.lookup(tool, json.RawMessage(input))
	if !ok {
		t.Fatalf("%s: %s %s has no recording", sc.Name, tool, input)
	}
	if rec.obs[0].err != "" {
		return "error: " + rec.obs[0].err
	}
	return rec.obs[0].summary
}

// TestW4lRemovedDecoderPluginRecordings: Connect validates the stored plugin.name=wal2json as the task did at
// start, so a validation that inherits it (no plugin.name in the config) and the network probe's Connect view
// name plugin.name, and a config that sets pgoutput validates clean.
func TestW4lRemovedDecoderPluginRecordings(t *testing.T) {
	sc := w4lScenarioByName(t, "removed-decoder-plugin")
	const cls = "io.debezium.connector.postgresql.PostgresConnector"
	wal2json := "1 error in " + cls + ":\n  plugin.name: The 'plugin.name' value 'wal2json' is invalid: Value must be one of decoderbufs, pgoutput"
	clean := "0 errors in " + cls + "."
	for _, c := range []struct {
		config, want string
	}{
		{`{"name":"pg-orders-connector"}`, wal2json},
		{`{"connector.class":"` + cls + `","database.dbname":"orders","database.hostname":"postgres","database.port":"5432","database.user":"piperoom","key.converter":"io.confluent.connect.avro.AvroConverter","slot.name":"debezium_orders","table.include.list":"public.orders","topic.prefix":"pg","value.converter":"io.confluent.connect.avro.AvroConverter"}`, wal2json},
		{`{"name":"pg-orders-connector","plugin.name":"pgoutput","publication.autocreate.mode":"filtered","publication.name":"dbz_orders","slot.name":"debezium_orders_pgoutput"}`, clean},
		{`{"plugin.name":"pgoutput","slot.name":"debezium_orders_pgoutput","publication.name":"dbz_publication"}`, clean},
	} {
		in := `{"base_connector":"pg-orders-connector","class":"` + cls + `","config":` + c.config + `}`
		if got := w4lSummary(t, sc, "connect_validate_config", in); got != c.want {
			t.Errorf("connect_validate_config %s = %q, want %q", c.config, got, c.want)
		}
	}
	probe := w4lSummary(t, sc, "net_probe", `{"target":"source"}`)
	if !strings.Contains(probe, "Connect reports errors on plugin.name, none about the database host") || !strings.Contains(probe, "dns postgres:5432 ok") {
		t.Errorf("net_probe source = %q", probe)
	}
	// wal2json needs no publication, so none exists; the stored offset is the old slot's position.
	if pub := w4lSummary(t, sc, "pg_publication_tables", `{"pipeline_id":"orders"}`); !strings.Contains(pub, "public.orders: in_publication=no") {
		t.Errorf("pg_publication_tables = %q", pub)
	}
	if off := w4lSummary(t, sc, "connect_source_offsets", `{"connector":"pg-orders-connector"}`); !regexp.MustCompile(`^pg-orders-connector partition server=pg: lsn=271073720232, lsn_proc=271073720232, ts_usec=\d+, txId=\d+$`).MatchString(off) {
		t.Errorf("connect_source_offsets = %q", off)
	}
	// 3F/1D40B7A8, the slot's confirmed_flush_lsn in pg_replication_slots, in decimal.
	if got := uint64(0x3F)<<32 | 0x1D40B7A8; got != 271073720232 {
		t.Errorf("the stored lsn is not the slot's confirmed_flush_lsn: %d", got)
	}
}

// TestW4lRenamedPropertyRecordings: the validation names the renamed key with the known-issue id only for a
// config that sets it, and the slot that does not move while the task is RUNNING reads stuck.
func TestW4lRenamedPropertyRecordings(t *testing.T) {
	sc := w4lScenarioByName(t, "renamed-property-after-upgrade")
	const cls = "io.debezium.connector.postgresql.PostgresConnector"
	renamed := "0 errors in " + cls + ".\n" +
		`unknown key "heartbeat.topics.prefix": renamed to "topic.heartbeat.prefix" (known issue dbz-topic-heartbeat-transaction)`
	for _, c := range []struct{ config, want string }{
		{`{"heartbeat.topics.prefix":"heartbeats","topic.heartbeat.prefix":"heartbeats"}`, renamed},
		{`{"heartbeat.topics.prefix":"heartbeats"}`, renamed},
		{`{"topic.heartbeat.prefix":"heartbeats"}`, "0 errors in " + cls + "."},
	} {
		in := `{"base_connector":"pg-orders-connector","class":"` + cls + `","config":` + c.config + `}`
		if got := w4lSummary(t, sc, "connect_validate_config", in); got != c.want {
			t.Errorf("connect_validate_config %s = %q, want %q", c.config, got, c.want)
		}
	}
	for _, in := range []string{`{"pipeline_id":"orders"}`, `{"pipeline_id":"orders","window_seconds":8}`} {
		live := w4lSummary(t, sc, "pg_streaming_liveness", in)
		if !strings.HasPrefix(live, "stuck: ") || !strings.Contains(live, "retains 1.9 GiB") || !strings.Contains(live, "connector pg-orders-connector state RUNNING") {
			t.Errorf("pg_streaming_liveness %s = %q", in, live)
		}
	}
	if probe := w4lSummary(t, sc, "net_probe", `{"target":"kafka"}`); !strings.HasPrefix(probe, "net_probe kafka (kafka-1:9092, kafka-2:9092, kafka-3:9092): all checks ok") {
		t.Errorf("net_probe kafka = %q", probe)
	}
}

// TestW4lRowsLostRecordings: kafka_topic_config answers a call without table as with it (the pipeline has one
// topic), and the other new reads agree with the idle stack the file describes.
func TestW4lRowsLostRecordings(t *testing.T) {
	sc := w4lScenarioByName(t, "rows-lost-after-retention")
	a := w4lSummary(t, sc, "kafka_topic_config", `{"pipeline_id":"orders"}`)
	b := w4lSummary(t, sc, "kafka_topic_config", `{"pipeline_id":"orders","table":"public.orders"}`)
	if a != b || !strings.Contains(a, "retention.ms=604800000") {
		t.Errorf("kafka_topic_config without table = %q, with table = %q", a, b)
	}
	if got := w4lSummary(t, sc, "pg_streaming_liveness", `{"pipeline_id":"orders"}`); !strings.HasPrefix(got, "quiet: no source writes in 5s") || !strings.Contains(got, "3B/4C0BB078") {
		t.Errorf("pg_streaming_liveness = %q", got)
	}
	if got := w4lSummary(t, sc, "pg_connector_scope_gap", `{"pipeline_id":"orders"}`); !strings.Contains(got, "public.devctl_signal: declared=no, in_connector=yes, in_publication=yes (in connector, outside declared scope)") {
		t.Errorf("pg_connector_scope_gap = %q", got)
	}
	if got := w4lSummary(t, sc, "kafka_throughput", `{"window":"1h"}`); !strings.Contains(got, "pg.public.orders: 11 records") {
		t.Errorf("kafka_throughput = %q", got)
	}
	// 446-450, 447-451, 447-450: the 11 retained records kafka_topic_offsets shows.
	if got := w4lSummary(t, sc, "kafka_partition_rates", `{"window":"1h"}`); !strings.Contains(got, "11 records") || !strings.Contains(got, "p0 4 (36%), p1 4, p2 3") {
		t.Errorf("kafka_partition_rates = %q", got)
	}
}

// TestW4lSnapshotStuckRecordings: the snapshot progress sequence reads the same scrapes as debezium_metrics,
// the slot reads the 61.4 GiB the pipeline map and pg_replication_slots show, and the reconcile of the 01:00
// window is on the tool's own grid.
func TestW4lSnapshotStuckRecordings(t *testing.T) {
	sc := w4lScenarioByName(t, "snapshot-stuck")
	var progress, metrics []Observation
	for _, rec := range sc.Recordings {
		switch rec.Tool {
		case "debezium_snapshot_progress":
			progress = rec.Sequence
		case "debezium_metrics":
			metrics = rec.Sequence
		}
	}
	if len(progress) != 2 || len(metrics) != 2 {
		t.Fatalf("sequences: progress %d, metrics %d, want 2 and 2", len(progress), len(metrics))
	}
	progressRows := regexp.MustCompile(`; (\d+) rows scanned at`)
	metricRows := regexp.MustCompile(`rows scanned of table public\.orders (\d+),`)
	for i := range progress {
		p, m := progressRows.FindStringSubmatch(progress[i].Summary), metricRows.FindStringSubmatch(metrics[i].Summary)
		if p == nil || m == nil || p[1] != m[1] {
			t.Errorf("read %d: snapshot progress %q does not scan the rows debezium_metrics %q shows", i+1, progress[i].Summary, metrics[i].Summary)
		}
	}
	if live := w4lSummary(t, sc, "pg_streaming_liveness", `{"pipeline_id":"orders"}`); !strings.Contains(live, "retains 61.4 GiB") {
		t.Errorf("pg_streaming_liveness = %q", live)
	}
	if got := w4lSummary(t, sc, "reconcile", `{"since":"2026-10-14T01:00:00Z","table":"public.orders"}`); !strings.HasPrefix(got, "reconcile public.orders → pg.public.orders → analytics.orders by updated_at, buckets of 21m0s from 2026-10-14T01:00:00Z\n") {
		t.Errorf("reconcile = %q", got)
	}
	if off := w4lSummary(t, sc, "connect_source_offsets", `{"connector":"pg-orders-connector"}`); !strings.Contains(off, "snapshot=true") {
		t.Errorf("the stored offset of a connector in its initial snapshot lacks snapshot=true: %q", off)
	}
}
