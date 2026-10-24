package evals

import (
	"context"
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
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

// w4cScenario loads one scenario by name for the W4c (KAN-494) tests.
func w4cScenario(t *testing.T, name string) Scenario {
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

// w4cSetupRunHypotheses are the final hypotheses of setup/db-unreachable-from-connect
// in run 39994729845 (runs 1 to 3), copied verbatim from the transcripts. Each
// blames "the network path" from the Connect container and was judged wrong;
// under lead ruling R10 (#939) none passes the terms.
var w4cSetupRunHypotheses = []string{
	"First failing component: pg-orders-connector task 0, FAILED with a TCP connect timeout to postgres:5432 (s_01M5MYFHA172ZGCXRK7BVA9RX3, s_01M5MYFRMH72C8MPHGHVGTSB4A). Root cause: the network path from the Connect container to the Postgres host. Postgres itself is up: it answers Piperoom (s_01M5MYFM99AGT9F30Q03M411J7, s_01M5MYFYPTQ1Z3WQ8VSXKKFMC7) and no DDL changed (s_01M5MYFHA1002G3P59Q3ASYNVK). The Connect-side probes returned no data (s_01M5MYFM99KVXGK3QA6ETKAX6B, s_01M5MYFYPTMBB63Y7DYV72MMW9), so the exact network fault is unconfirmed. Separately, slot debezium_orders is missing, so changes made since the failure will not be replayed. No past room matches (s_01M5MYFRMHAT8QCYG1P9SNGWQD).",
	"First failing component: pg-orders-connector task 0, FAILED with a TCP connect timeout to postgres:5432 (s_01M5MYFVV91G670X3J2PAQ3PXG). Root cause: the network path from the Connect container to the Postgres host is broken, not Postgres itself. Postgres answers Piperoom (s_01M5MYG41YTNJPJT10AREVT673, s_01M5MYFY7RHTPZ60QQPED9QFXK) and has had no DDL (s_01M5MYFVV973FQSM14XN7GA62M). net_probe and Connect validation returned no data, so this cannot be confirmed from Connect's side (s_01M5MYFY7RRB6S5QYNX73VYFHG, s_01M5MYGFD0NSQ4ZZK59396FH8Y). Separately, slot debezium_orders is missing (s_01M5MYFY7RHTPZ60QQPED9QFXK), so a restart would recreate the slot and change events since the failure may be lost. No similar past room, no injection seen.",
	"First failing component: pg-orders-connector task 0 FAILED with connect timed out to postgres:5432 (s_01M5MYG8JTQCS1Z0B1Z48BB6SD). Likely root cause: the network path from the Connect container to postgres:5432; Postgres itself answers Piperoom (s_01M5MYGBN2YSRP5Q4CDGW504N6), but Connect's view is unconfirmed because net_probe and Connect validation returned no data (s_01M5MYG8JTXG979BJ4ETJVVMHT, s_01M5MYGHJ3FKRKB2QVC91YNPBD). Also, slot debezium_orders is missing (s_01M5MYGBN2YSRP5Q4CDGW504N6), so a plain restart would leave a gap. No DDL (s_01M5MYG8JT6QKWBZQNV2GTA4V5), no past match, no injection seen.",
}

// w4cSetupRunWant is what the R10 terms return for w4cSetupRunHypotheses: none
// names name resolution ("Postgres host" is not "hosts"), and run 2 also tells
// the forbidden slot story.
var w4cSetupRunWant = []bool{false, false, false}

// w4cIdleRunHypotheses are the final hypotheses of idle-wal-growth in run
// 39994729845 (runs 1 to 3), verbatim. Each passed the root-cause terms and
// failed only the root-cause layer: it puts the cause in pg-orders-connector's
// config (kafka-connect).
var w4cIdleRunHypotheses = []string{
	"No component is broken. The first failing component is the slot debezium_orders on the orders-db Postgres, and the root cause is the config of pg-orders-connector (kafka-connect). public.orders has had no writes since about 09:02 (s_01M5MXMKVVBB9EF30B7DCBSGJY, s_01M5MXM80QDYBB7Y27Z1P9WRMS). The connector config lists no heartbeat setting and uses a filtered publication (s_01M5MXMGEP4VB7AEGT8148H8BT). So with nothing to emit, the active slot never confirms a newer LSN while other database writes add WAL (s_01M5MXMC79QSJVHX47S3GP8XD2), and max_slot_wal_keep_size=-1 means nothing caps it (s_01M5MXMQR2DSZCHBDDPF01FR9D). The 38m-stale warehouse is an idle source, not data loss. No past room matches, and nothing tried to inject instructions.",
	"Nothing is broken. The first component showing a symptom is the slot debezium_orders (postgres): it is active, but its confirmed_flush_lsn stays flat because public.orders has had no writes since about 09:02 (s_01M5MXMQR79396CEX93H3SCXPZ quiet, s_01M5MXN4YNSQ6BNSCGREMQ81A0 source = topic = sink, no drift). The root cause sits in the connector config of pg-orders-connector (kafka-connect): its publication is filtered and no heartbeat is set (s_01M5MXN0PMZZYYM3PM5F92VQEF), so WAL written elsewhere on the server builds up behind the slot, with max_slot_wal_keep_size unlimited (s_01M5MXN8XZWCZ2AM53XAZ9PPY8). The warehouse looks stale only because the source table is idle. No past room matches (s_01M5MXN8XZ1GNJ2C7RQMMNHYWR).",
	"No component is broken: first symptom is slot debezium_orders on orders-db retaining WAL (s_01M5MXNESEK99E8H4XZQ0E33SA, active, reserved) because public.orders has had no writes since 09:02 (s_01M5MXNQF46DARG8XDAH66AKGE, sink matches source, lag 0 s_01M5MXNK9X1Q830ZB2AMH203E1) and the confirmed_flush_lsn sits flat (s_01M5MXN8YMAMYTVTYDWD2YHXY7); root cause is pg-orders-connector config with a filtered publication and no heartbeat (s_01M5MXNK9XXG2PYZQE8XWAQW8H), so other database WAL piles up behind the slot with max_slot_wal_keep_size=-1 (s_01M5MXNWQ0QKJ9JCBREYPWP02B); no past room matches, no injection seen.",
}

// w4cSetupLiveHypothesis is the root-cause part of live run 40034120325's
// hypothesis (judged "wrong root cause"), as quoted in the W4c fix round; the
// full transcript was not available to this row. Under ruling R10 it stays a
// fail: it blames the network path and never names name resolution.
const w4cSetupLiveHypothesis = "First to fail is pg-orders-connector task 0, which times out connecting to postgres:5432. The root cause is the network path from the Connect container (172.18.0.5) to Postgres; Piperoom reaches postgres:5432 at 172.18.0.2."

// TestW4cSetupDBUnreachableTerms pins W4c's root-cause terms of
// setup/db-unreachable-from-connect under lead ruling R10 (#939): one group
// naming the Connect side's name resolution ([resolv*, dns, hosts, name
// resolution]; the real net_probe cannot show what the worker resolved, so no
// address group), no path or symptom words ("network path" and "route*" let
// the judged-wrong runs of 39994729845 pass), forbidden rows that fail a
// hypothesis clearing DNS or echoing net_probe's "dns ok" line, and firewall no
// longer a forbidden row (a firewall story still fails: it names no
// resolution).
func TestW4cSetupDBUnreachableTerms(t *testing.T) {
	sc := w4cScenario(t, "setup/db-unreachable-from-connect")
	want := [][]string{{"resolv*", "dns", "hosts", "name resolution"}}
	if !reflect.DeepEqual(sc.Expect.RootCause, want) {
		t.Fatalf("root_cause = %v, want %v", sc.Expect.RootCause, want)
	}
	for _, row := range sc.Expect.RootCauseForbidden {
		for _, term := range row {
			if strings.Contains(strings.ToLower(term), "firewall") {
				t.Errorf("root_cause_forbidden still has %v", row)
			}
		}
	}
	cases := []struct {
		id, text string
		want     bool
	}{
		{"resolution", "Name resolution inside the Connect container maps postgres to an address that routes nowhere, so task 0 of pg-orders-connector times out while Piperoom's own dns and tcp checks reach postgres:5432.", true},
		{"resolves", "The Connect worker resolves postgres to a dead address, so the JDBC connect times out.", true},
		{"dns", "Connect's DNS for postgres is wrong: Piperoom reaches postgres:5432 but the worker cannot.", true},
		{"hosts", "A stale entry in the Connect container's /etc/hosts sends postgres elsewhere.", true},
		{"hosts file", "The Connect container's hosts file points postgres at the wrong place.", true},
		// The symptom every run already named: not the cause.
		{"cannot reach", "pg-orders-connector cannot reach postgres:5432.", false},
		{"timed out reaching", "Task 0 of pg-orders-connector times out reaching postgres:5432 from the Connect container.", false},
		{"address", "Connect connects to the wrong address for the database.", false},
		{"timeout", "Task 0 failed with java.net.SocketTimeoutException: connect timed out.", false},
		{"name resolution, hosts entry", "Root cause: the Connect container's /etc/hosts maps postgres to a black-hole address; Piperoom's checks reach postgres:5432.", true},
		{"piperoom resolves, connect does not", "Piperoom resolves postgres to 172.18.0.2 and connects; the Connect container's name resolution sends postgres elsewhere (a stale hosts entry).", true},
		// R10: a network-path story without name resolution is not the cause.
		{"network path", "Root cause: the network path from the Connect container to the Postgres host is broken.", false},
		{"route", "The route from the Connect container to postgres:5432 is broken.", false},
		{"live run", w4cSetupLiveHypothesis, false},
		// R10: echoes of net_probe's dns line and hypotheses that clear DNS.
		{"echo dns ok", "dns ok, tcp ok, Connect's view times out: the Connect worker cannot reach postgres:5432.", false},
		{"echo dns line", "net_probe shows dns postgres:5432 ok: 172.18.0.2 and TCP connected, but the Connect task times out.", false},
		{"dns is fine", "DNS is fine; the Connect container's network path to postgres:5432 is broken.", false},
		{"dns is ok", "DNS is OK; Connect cannot open a connection to postgres:5432.", false},
		{"dns check passes", "The DNS check passes; Connect cannot reach postgres:5432.", false},
		{"dns works", "DNS resolution from Piperoom works, but Connect times out reaching postgres:5432.", false},
		{"dns look fine", "The hosts file and DNS look fine; a firewall drops Connect's packets.", false},
		// The cost of the echo rows: a right cause that also calls Piperoom's
		// DNS check passing fails the terms (the judge's description says
		// "reach" for Piperoom's checks).
		{"right cause, dns passes", "Piperoom's DNS check passes, but name resolution inside the Connect container maps postgres to a dead address.", false},
		// Firewall stories: no longer a forbidden row, but they name no
		// resolution, and with "DNS is fine" they are an R10 fail as well.
		{"firewall", "A firewall between the Connect container and postgres drops the packets.", false},
		{"dns fine, firewall", "DNS is fine, so a firewall between Connect and postgres drops the packets.", false},
		// Still forbidden stories.
		{"database down", "Name resolution is fine; the database is down.", false},
		{"slot lost", "DNS is fine; the slot was lost in the move.", false},
	}
	for _, c := range cases {
		if got := rootCauseMatches(sc.Expect, c.text); got != c.want {
			t.Errorf("%s: matches = %v, want %v: %q", c.id, got, c.want, c.text)
		}
	}
	for i, h := range w4cSetupRunHypotheses {
		if got := rootCauseMatches(sc.Expect, h); got != w4cSetupRunWant[i] {
			t.Errorf("run %d: matches = %v, want %v: %q", i+1, got, w4cSetupRunWant[i], h)
		}
	}
	// The judge sees the same groups and the observable root cause.
	p := judgePrompt(sc, "")
	if !strings.Contains(p, "- resolv* | dns | hosts | name resolution\n") || strings.Contains(p, "192.0.2.1 | ") {
		t.Errorf("judge prompt groups:\n%s", p)
	}
	desc := strings.Join(strings.Fields(sc.Description), " ")
	for _, s := range []string{"neither the address 192.0.2.1 nor the exact hosts entry is required", "a hypothesis that calls DNS fine is wrong"} {
		if !strings.Contains(desc, s) {
			t.Errorf("description lacks %q: %q", s, desc)
		}
	}
}

// TestW4cSetupDBUnreachableNetProbe pins the net_probe {target: source}
// recording: Connect's view of the database times out while Piperoom's own
// dns and tcp checks pass, and no read the room offers shows the bad address
// (which is why the address group was dropped).
func TestW4cSetupDBUnreachableNetProbe(t *testing.T) {
	sc := w4cScenario(t, "setup/db-unreachable-from-connect")
	rp, err := newScenarioReplayer(sc)
	if err != nil {
		t.Fatal(err)
	}
	rec, ok := rp.lookup("net_probe", json.RawMessage(`{"target":"source"}`))
	if !ok {
		t.Fatal(`net_probe {"target":"source"} is not recorded`)
	}
	if rec.Error != "" {
		t.Fatalf("net_probe recording is an error: %q", rec.Error)
	}
	if !strings.HasPrefix(rec.Summary, "net_probe source (postgres:5432): 1 of 4 checks failed\n- connect_view fail: Connect did not finish validating connector pg-orders-connector within the step's time") {
		t.Errorf("summary = %q", rec.Summary)
	}
	b, err := json.Marshal(rec.Data)
	if err != nil {
		t.Fatal(err)
	}
	var data struct {
		Target string `json:"target"`
		Kind   string `json:"kind"`
		Checks []struct {
			ID, Addr, Status, Problem, Detail string
		} `json:"checks"`
	}
	if err := json.Unmarshal(b, &data); err != nil {
		t.Fatal(err)
	}
	if data.Target != "source" || data.Kind != "source_db" {
		t.Errorf("target/kind = %q/%q", data.Target, data.Kind)
	}
	got := map[string]string{}
	for _, c := range data.Checks {
		got[c.ID] = c.Status
		switch c.ID {
		case "connect_view":
			if !strings.Contains(c.Problem, "did not finish validating") {
				t.Errorf("connect_view problem = %q, want the step timeout", c.Problem)
			}
		case "dns":
			if c.Detail != "172.18.0.2" {
				t.Errorf("dns detail = %q, want Piperoom's resolution 172.18.0.2", c.Detail)
			}
		}
	}
	if want := map[string]string{"connect_view": "fail", "dns": "ok", "tcp": "ok", "tls": "skip"}; !reflect.DeepEqual(got, want) {
		t.Errorf("checks = %v, want %v", got, want)
	}
	// Negative: no recording answers with the bad address or the hosts file.
	for _, r := range sc.Recordings {
		blob, _ := json.Marshal(r.Data)
		for _, s := range []string{r.Summary, r.Error, string(blob)} {
			if strings.Contains(s, "192.0.2.1") || strings.Contains(s, "/etc/hosts") {
				t.Errorf("%s recording shows the bad address: %q", r.Tool, s)
			}
		}
	}
	if strings.Contains(sc.Alert.Title, "192.0.2.1") {
		t.Errorf("alert names the bad address: %q", sc.Alert.Title)
	}
}

// TestW4cRecordedMisses replays every call run 39994729845 missed in the two
// W4c scenarios (results[].misses, exact tool and input): each now resolves to
// a recording.
func TestW4cRecordedMisses(t *testing.T) {
	const validate = `{"base_connector":"pg-orders-connector","class":"io.debezium.connector.postgresql.PostgresConnector","config":{"database.hostname":"postgres","database.port":"5432","database.dbname":"orders","slot.name":"debezium_orders","table.include.list":"public.orders","topic.prefix":"pg"}}`
	misses := map[string][][2]string{
		"setup/db-unreachable-from-connect": {
			{"net_probe", `{"target":"source"}`},
			{"connect_validate_config", validate},
			{"connect_source_offsets", `{"connector":"pg-orders-connector"}`},
			{"connect_worker_info", `{}`},
			{"connect_worker_logs", `{"connector":"pg-orders-connector","limit":40,"since":"2h"}`},
			{"connect_worker_logs", `{"connector":"pg-orders-connector","limit":50,"since":"2h"}`},
			{"connect_worker_logs", `{"connector":"pg-orders-connector","since":"2h"}`},
		},
		"idle-wal-growth": {
			{"debezium_metrics", `{"connector":"pg-orders-connector"}`},
		},
	}
	for name, calls := range misses {
		sc := w4cScenario(t, name)
		rp, err := newScenarioReplayer(sc)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range calls {
			if _, ok := rp.lookup(c[0], json.RawMessage(c[1])); !ok {
				t.Errorf("%s: %s %s still misses", name, c[0], c[1])
			}
		}
		// Negative: a connector outside the pipeline is still not answered.
		if name == "idle-wal-growth" {
			if _, ok := rp.lookup("debezium_metrics", json.RawMessage(`{"connector":"other"}`)); ok {
				t.Errorf("%s: debezium_metrics for another connector is answered", name)
			}
		}
	}
	// The idle-wal-growth metrics show a connected connector whose last event
	// is the topic's newest record, 38 minutes old.
	sc := w4cScenario(t, "idle-wal-growth")
	rp, err := newScenarioReplayer(sc)
	if err != nil {
		t.Fatal(err)
	}
	rec, _ := rp.lookup("debezium_metrics", json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if !strings.Contains(rec.Summary, "streaming connected=true") || !strings.Contains(rec.Summary, "last event 38 min ago") {
		t.Errorf("debezium_metrics summary = %q", rec.Summary)
	}
}

// TestW4cIdleWALGrowthFixTargetAndLayer pins W4c's idle-wal-growth label: the
// heartbeat fix target names no table (any heartbeat table name the agent
// picks matches), the root-cause layer admits kafka-connect, and the run's own
// hypotheses now pass the keyword root-cause layer as before.
func TestW4cIdleWALGrowthFixTargetAndLayer(t *testing.T) {
	sc := w4cScenario(t, "idle-wal-growth")
	target, ok := sc.Expect.FixTargets["source_create_heartbeat_table"]
	if !ok {
		t.Fatal("no fix target for source_create_heartbeat_table")
	}
	if _, has := target["table"]; has {
		t.Errorf("fix target still names a table: %v", target)
	}
	if !reflect.DeepEqual(sc.Expect.RootCauseLayerAlt, []string{"kafka-connect"}) {
		t.Errorf("root_cause_layer_alt = %v, want [kafka-connect]", sc.Expect.RootCauseLayerAlt)
	}
	if sc.Expect.FirstFailingLayer != "postgres" || sc.Expect.RootCauseLayer != "postgres" {
		t.Errorf("layers = %s/%s, want postgres/postgres", sc.Expect.FirstFailingLayer, sc.Expect.RootCauseLayer)
	}
	steps := []struct {
		params string
		want   bool
	}{
		// Run 2's proposal, exactly.
		{`{"connector":"pg-orders-connector","table":"public.debezium_heartbeat"}`, true},
		{`{"connector":"pg-orders-connector","table":"public.devctl_heartbeat"}`, true},
		{`{"connector":"pg-orders-connector","table":"public.piperoom_heartbeat"}`, true},
		// Negative: another connector is still the wrong target.
		{`{"connector":"other-connector","table":"public.debezium_heartbeat"}`, false},
	}
	for _, s := range steps {
		st := events.ProposalStep{Action: "source_create_heartbeat_table", Params: json.RawMessage(s.params)}
		if got := stepTargets(sc.Expect.FixTargets, st); got != s.want {
			t.Errorf("stepTargets(%s) = %v, want %v", s.params, got, s.want)
		}
	}
	if p := judgePrompt(sc, ""); !strings.Contains(p, "Root-cause layer: postgres, or alternatively kafka-connect (") {
		t.Errorf("judge prompt lacks the alternative layer:\n%s", p)
	}
	// The keyword pass marks these hypotheses ambiguous ("No component is
	// broken"), so the judge grades them, with the alternative in its prompt;
	// the deterministic halves agree.
	for i, h := range w4cIdleRunHypotheses {
		if !rootCauseMatches(sc.Expect, h) || !keywordRootCauseLayer(sc, h) {
			t.Errorf("run %d: root cause %v, root-cause layer %v, want true/true: %q", i+1, rootCauseMatches(sc.Expect, h), keywordRootCauseLayer(sc, h), h)
		}
		if g := GradeKeywords(sc, h); g.RootCauseLayer == GradeFail {
			t.Errorf("run %d: keyword root-cause layer fails: %q", i+1, h)
		}
	}
	// Negative: a cause placed in neither layer still fails the root-cause layer.
	if keywordRootCauseLayer(sc, "The cause is the order-sync consumer, which never commits its offsets.") {
		t.Errorf("a consumer-only cause passes the root-cause layer")
	}
}

// TestW4cIdleWALGrowthActionsOnReplay replays the three steps every run of
// 39994729845 tried (action_attempts, exact params). With resources.tables and
// resources.publications declared, pg_add_publication_table is refused by the
// action itself (the heartbeat table does not exist yet and is not a source
// table), no longer by the harness; the heartbeat step still validates.
func TestW4cIdleWALGrowthActionsOnReplay(t *testing.T) {
	sc := w4cScenario(t, "idle-wal-growth")
	reg, err := LiveActions(sc.Resources, sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	ctx := tools.WithPipeline(context.Background(), sc.Pipeline)
	validate := func(action, params string) error {
		t.Helper()
		a, ok := reg.Get(action)
		if !ok {
			t.Fatalf("no action %s", action)
		}
		return a.Validate(ctx, json.RawMessage(params))
	}
	if err := validate("source_create_heartbeat_table", `{"connector":"pg-orders-connector","table":"public.debezium_heartbeat"}`); err != nil {
		t.Errorf("heartbeat step: %v", err)
	}
	const addPub = `{"connector":"pg-orders-connector","publication":"dbz_publication","table":"public.debezium_heartbeat"}`
	err = validate("pg_add_publication_table", addPub)
	if err == nil || strings.Contains(err.Error(), "not available in replay") || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("pg_add_publication_table Validate = %v, want the action's own refusal (table does not exist)", err)
	}
	a, _ := reg.Get("pg_add_publication_table")
	scoped, ok := a.(actions.Scoped)
	if !ok {
		t.Fatal("pg_add_publication_table is not Scoped")
	}
	if err := scoped.CheckScope(sc.Pipeline, json.RawMessage(addPub)); err == nil || !strings.Contains(err.Error(), "is not one of pipeline") {
		t.Errorf("CheckScope = %v, want the source_tables refusal", err)
	}
	// The declared state answers like the stack: public.orders is already in
	// dbz_publication.
	if err := validate("pg_add_publication_table", `{"connector":"pg-orders-connector","publication":"dbz_publication","table":"public.orders"}`); err == nil || !strings.Contains(err.Error(), "already in publication") {
		t.Errorf("pg_add_publication_table public.orders = %v, want already published", err)
	}
}

// w4cFact returns the scenario's expected fact id.
func w4cFact(t *testing.T, sc Scenario, id string) Fact {
	t.Helper()
	for _, f := range sc.Expect.ExpectedFacts {
		if f.ID == id {
			return f
		}
	}
	t.Fatalf("%s: no fact %s", sc.Name, id)
	return Fact{}
}

// TestW4cSetupServerAnswersFromNetProbe pins the W4c fix round's evidence for
// setup/db-unreachable-from-connect, accepted by lead ruling R9 (#939; W4a
// rule, no duplicate evidence):
// f_server_answers comes from net_probe's tcp check, the read live run
// 40034120325 used, and source_settings is no longer required evidence (its
// recording stays, as an optional fact).
func TestW4cSetupServerAnswersFromNetProbe(t *testing.T) {
	sc := w4cScenario(t, "setup/db-unreachable-from-connect")
	if !reflect.DeepEqual(sc.Expect.EvidenceTools, []string{"connect_status"}) {
		t.Errorf("evidence_tools = %v, want [connect_status]", sc.Expect.EvidenceTools)
	}
	f := w4cFact(t, sc, "f_server_answers")
	if f.Tool != "net_probe" || !f.Required {
		t.Errorf("f_server_answers = %+v, want a required net_probe fact", f)
	}
	rp, err := newScenarioReplayer(sc)
	if err != nil {
		t.Fatal(err)
	}
	probe, _ := rp.lookup("net_probe", json.RawMessage(`{"target":"source"}`))
	if !factIn(f, probe.Summary) {
		t.Errorf("the net_probe recording does not establish f_server_answers: %q", probe.Summary)
	}
	if !factIn(w4cFact(t, sc, "f_connect_view_times_out"), probe.Summary) {
		t.Errorf("the net_probe recording does not establish f_connect_view_times_out")
	}
	// Negative: a probe whose tcp check failed does not establish it.
	failed := strings.Replace(probe.Summary, "tcp postgres:5432 ok: TCP connected", "tcp postgres:5432 fail: TCP did not connect within the step's time", 1)
	if factIn(f, failed) {
		t.Errorf("a failed tcp check establishes f_server_answers")
	}
	settings := w4cFact(t, sc, "f_settings_logical")
	if settings.Tool != "source_settings" || settings.Required {
		t.Errorf("f_settings_logical = %+v, want an optional source_settings fact", settings)
	}
	if rec, ok := rp.lookup("source_settings", json.RawMessage(`{}`)); !ok || !factIn(settings, rec.Summary) {
		t.Errorf("the source_settings recording is gone or no longer establishes f_settings_logical")
	}
}

// TestW4cIdleBlockingSessionsNotRequired pins the W4c fix round for
// idle-wal-growth, accepted by lead ruling R9 (#939; W4a rule): the story has no open transaction, so
// source_blocking_sessions is no longer required evidence; its recording and
// the optional f_no_blockers fact stay.
func TestW4cIdleBlockingSessionsNotRequired(t *testing.T) {
	sc := w4cScenario(t, "idle-wal-growth")
	if !reflect.DeepEqual(sc.Expect.EvidenceTools, []string{"pg_replication_slots", "source_settings"}) {
		t.Errorf("evidence_tools = %v, want [pg_replication_slots source_settings]", sc.Expect.EvidenceTools)
	}
	f := w4cFact(t, sc, "f_no_blockers")
	if f.Required {
		t.Errorf("f_no_blockers is required")
	}
	rp, err := newScenarioReplayer(sc)
	if err != nil {
		t.Fatal(err)
	}
	if rec, ok := rp.lookup("source_blocking_sessions", json.RawMessage(`{}`)); !ok || !factIn(f, rec.Summary) {
		t.Errorf("the source_blocking_sessions recording is gone or no longer establishes f_no_blockers")
	}
}
