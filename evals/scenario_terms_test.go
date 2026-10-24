package evals

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
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

// TestW4aRelaxedFacts pins KAN-492 (W4a): a required fact or evidence tool
// must be reachable on the correct read path and carry information no other
// required fact gives. The alert already carries the pipeline map, so the two
// ClickHouse scenarios no longer demand a pipeline_map read; the other three
// lose a reader whose answer another required fact already contains.
func TestW4aRelaxedFacts(t *testing.T) {
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]Scenario{}
	for _, sc := range scs {
		byName[sc.Name] = sc
	}
	get := func(name string) Expect {
		sc, ok := byName[name]
		if !ok {
			t.Fatalf("scenario %q not found", name)
		}
		return sc.Expect
	}
	fact := func(e Expect, id string) (Fact, bool) {
		for _, f := range e.ExpectedFacts {
			if f.ID == id {
				return f, true
			}
		}
		return Fact{}, false
	}
	has := func(list []string, s string) bool {
		for _, x := range list {
			if x == s {
				return true
			}
		}
		return false
	}

	for _, name := range []string{"clickhouse-readonly-profile", "clickhouse-sink-table-dropped"} {
		e := get(name)
		for _, f := range e.ExpectedFacts {
			if f.Tool == "pipeline_map" {
				t.Errorf("%s: fact %q still reads pipeline_map", name, f.ID)
			}
		}
		if has(e.EvidenceTools, "pipeline_map") {
			t.Errorf("%s: evidence_tools still lists pipeline_map: %v", name, e.EvidenceTools)
		}
	}
	if e := get("clickhouse-readonly-profile"); len(e.Claims) != 0 || len(e.ExpectedFacts) != 0 {
		t.Errorf("clickhouse-readonly-profile: want claims [] and no facts, got claims %v facts %v", e.Claims, e.ExpectedFacts)
	}
	{
		e := get("clickhouse-sink-table-dropped")
		f, ok := fact(e, "f_code60")
		if !ok || !f.Required || f.Tool != "warehouse_load_errors" {
			t.Errorf("clickhouse-sink-table-dropped: f_code60 must stay required on warehouse_load_errors, got %+v (found %v)", f, ok)
		}
	}

	eq := func(name string, got, want []string) {
		t.Helper()
		if len(got) != len(want) {
			t.Errorf("%s: evidence_tools = %v, want %v", name, got, want)
			return
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("%s: evidence_tools = %v, want %v", name, got, want)
				return
			}
		}
	}
	eq("rebalance-loop", get("rebalance-loop").EvidenceTools, []string{"connect_status", "connect_worker_metrics"})
	if e := get("setup/flat-plugin-dir"); has(e.EvidenceTools, "connect_list_connectors") {
		t.Errorf("setup/flat-plugin-dir: evidence_tools still lists connect_list_connectors: %v", e.EvidenceTools)
	}
	{
		e := get("register-rejected-missing-plugin")
		f, ok := fact(e, "f_class_in_config")
		if !ok || f.Required {
			t.Errorf("register-rejected-missing-plugin: f_class_in_config must exist with required false, got %+v (found %v)", f, ok)
		}
		eq("register-rejected-missing-plugin", e.EvidenceTools, []string{"connect_status"})
	}

	// Negative: relaxing facts must not relax the root cause. A hypothesis that
	// names no root-cause term still fails; the good one still passes.
	absent := map[string]string{
		"clickhouse-readonly-profile":      "The warehouse is stale and analytics.orders has not loaded for a while; investigate further.",
		"clickhouse-sink-table-dropped":    "analytics.orders is stale and the warehouse is behind; the cause is not yet known.",
		"rebalance-loop":                   "pg-orders-connector task 0 is RUNNING but analytics.orders is stale; the cause is not yet known.",
		"setup/flat-plugin-dir":            "pg-orders-connector FAILED and the worker restarted; the cause is not yet known.",
		"register-rejected-missing-plugin": "pg-orders-connector FAILED after the redeploy; the cause is not yet known.",
	}
	for name, text := range absent {
		e := get(name)
		if rootCauseMatches(e, text) {
			t.Errorf("%s: a hypothesis without the root-cause term matched: %q", name, text)
		}
		h, ok := hypotheses[name]
		if !ok {
			t.Fatalf("%s: no good/wrong hypotheses in scoring_test.go", name)
		}
		if !rootCauseMatches(e, h.good) {
			t.Errorf("%s: the good hypothesis no longer matches", name)
		}
		if rootCauseMatches(e, h.wrong) {
			t.Errorf("%s: the wrong hypothesis matches", name)
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

// W4b (KAN-493). A forbidden action is a wrong remedy in every ordering,
// never the completion of the expected fix; a term group accepts every
// wording of the outcome but never an echo of the alert or the symptom.

// W4b a: connect_resume_connector completes stale-source-offsets' fix (stop,
// reset, resume), so it is not forbidden; both restarts still are. The replay
// refuses a resume while the recorded connector is RUNNING (the stop is a
// no-op there), so the proposals are scored from a raw transcript.
func TestW4bStaleSourceOffsetsResumeNotForbidden(t *testing.T) {
	sc := loadScenario(t, "scenarios/stale-source-offsets.yaml")
	if slices.Contains(sc.Expect.ForbiddenActions, "connect_resume_connector") {
		t.Fatalf("connect_resume_connector is forbidden: %v", sc.Expect.ForbiddenActions)
	}
	conn := `{"connector":"pg-orders-connector"}`
	fix := []events.ProposalStep{
		ps("connect_stop_connector", conn),
		ps("connect_reset_source_offsets", `{"connector":"pg-orders-connector","stop_first":true}`),
	}
	score := func(extra events.ProposalStep) RunResult {
		steps := append(slices.Clone(fix), extra)
		return Judge(sc, rawTranscript(t, events.ProposalCreated, events.ProposalCreatedPayload{Title: "fix", Steps: steps}), nil, false)
	}
	if r := score(ps("connect_resume_connector", conn)); len(r.ForbiddenProposed) != 0 {
		t.Errorf("resume after the offset fix counted as forbidden: %v", r.ForbiddenProposed)
	}
	for _, bad := range []events.ProposalStep{
		ps("connect_restart_connector", conn),
		ps("connect_restart_task", `{"connector":"pg-orders-connector","task":0}`),
	} {
		r := score(bad)
		if r.Pass || !slices.Equal(r.ForbiddenProposed, []string{bad.Action}) {
			t.Errorf("%s: pass=%v forbidden=%v, want exactly it forbidden", bad.Action, r.Pass, r.ForbiddenProposed)
		}
	}
}

// groupOf returns the 1-based root-cause group n of sc as a one-group list.
func groupOf(t *testing.T, sc Scenario, n int) [][]string {
	t.Helper()
	if n < 1 || n > len(sc.Expect.RootCause) {
		t.Fatalf("%s has %d root-cause groups, want group %d", sc.Name, len(sc.Expect.RootCause), n)
	}
	return [][]string{sc.Expect.RootCause[n-1]}
}

// W4b b: topic-retention-data-loss group 2 takes loss that has happened, one
// row per added term, and rejects denials, plans and the bare words.
func TestW4bRetentionLossGroup2(t *testing.T) {
	sc := loadScenario(t, "scenarios/topic-retention-data-loss.yaml")
	g2 := groupOf(t, sc, 2)
	accept := map[string]string{
		"unrecoverable":             "The 01:00-03:00 changes are unrecoverable from the topic.",
		"no longer replay*":         "The expired changes no longer replay from pg.public.orders.",
		"can no longer be replayed": "Those change events can no longer be replayed from Kafka.",
		"gone from":                 "The 01:00-03:00 change events are gone from the topic.",
		"never read":                "order-sync never read the 01:00-03:00 change events.",
	}
	for term, text := range accept {
		if !slices.Contains(sc.Expect.RootCause[1], term) {
			t.Errorf("group 2 lacks the term %q", term)
		}
		if !matchGroups(g2, text) {
			t.Errorf("group 2 rejects %q (term %q)", text, term)
		}
	}
	reject := []string{
		"Retention will delete the old segments as configured.",
		"Retention will delete changes if order-sync stays behind.",
		// The bare words stay out of group 2.
		"Records were deleted.",
		"The rows are missing from analytics.orders.",
		"The re-pricing updates were not applied to analytics.orders.",
		"orders reconciliation: 1,181,433 of 7,912,440 orders changed 01:00-03:00 UTC differ in analytics.orders",
	}
	for _, text := range reject {
		if matchGroups(g2, text) {
			t.Errorf("group 2 accepts %q", text)
		}
	}
	// The ticket's "before ... read/applied" terms would accept the slow-consumer
	// traps T1, T2 and T13 (scenarioTermCases), so they are not group 2 terms.
	for _, term := range []string{"before it read", "before order-sync read", "before order-sync applied"} {
		if slices.Contains(sc.Expect.RootCause[1], term) {
			t.Errorf("group 2 holds %q, which accepts 'nothing was deleted before order-sync read it'", term)
		}
	}
	for _, trap := range []string{
		"Retention is fine: nothing was deleted before order-sync read it; order-sync is just slow.",
		"Retention did not delete anything before order-sync read it; order-sync is just slow.",
	} {
		if rootCauseMatches(sc.Expect, trap) {
			t.Errorf("slow-consumer trap matches the root cause: %q", trap)
		}
	}
	// "Nothing was lost" is accepted by the pre-existing term "lost": the
	// matcher has no denial for "nothing" (strongNegators lack it). Logged, not
	// failed; fixing it is a matcher change (C6/E2), not a term change.
	if matchGroups(g2, "Nothing was lost; order-sync is just slow.") {
		t.Log("matcher limit: group 2 accepts 'nothing was lost' through the existing term 'lost'")
	}
	for _, bare := range []string{"deleted", "missing from", "not applied"} {
		if slices.Contains(sc.Expect.RootCause[1], bare) {
			t.Errorf("group 2 holds the bare term %q", bare)
		}
	}
	// Whole hypothesis: a plan to delete, with no loss yet, is not a cause.
	if rootCauseMatches(sc.Expect, "Retention will delete the old segments, expiring them as configured; order-sync is just slow.") {
		t.Error("a plan with no loss matches the root cause")
	}
}

// W4b c: sink-dropped-column group 3 takes "no longer writing" and "not
// writing", not "wrote null" or "missing" alone (the alert says NULL).
func TestW4bSinkDroppedColumnGroup3(t *testing.T) {
	sc := loadScenario(t, "scenarios/sink-dropped-column.yaml")
	g3 := groupOf(t, sc, 3)
	for _, text := range []string{
		"order-sync 2.16.1 is no longer writing updated_at.",
		"The new upsert is not writing updated_at.",
		"The release no longer writes updated_at on insert.",
	} {
		if !matchGroups(g3, text) {
			t.Errorf("group 3 rejects %q", text)
		}
	}
	for _, text := range []string{
		"order-sync 2.16.1 wrote null into updated_at.",
		"updated_at is missing.",
		"The rows are missing updated_at and show NULL.",
		"dbt not_null test failed on analytics.orders.updated_at",
	} {
		if matchGroups(g3, text) {
			t.Errorf("group 3 accepts %q", text)
		}
	}
	for _, bad := range []string{"null", "missing"} {
		if slices.Contains(sc.Expect.RootCause[2], bad) {
			t.Errorf("group 3 holds %q", bad)
		}
	}
}

// W4b d: the budgets, and the per-turn derivation comment next to
// sink-dropped-column's.
func TestW4bBudgets(t *testing.T) {
	sink := loadScenario(t, "scenarios/sink-dropped-column.yaml")
	if sink.Expect.MaxToolCalls != 18 {
		t.Errorf("sink-dropped-column max_tool_calls = %d, want 18", sink.Expect.MaxToolCalls)
	}
	raw, err := os.ReadFile("scenarios/sink-dropped-column.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "Budget 14 -> 18") || !strings.Contains(string(raw), "turn 1, the alert") {
		t.Error("sink-dropped-column lacks the per-turn budget derivation comment")
	}
	if got := loadScenario(t, "scenarios/topic-retention-data-loss.yaml").Expect.MaxToolCalls; got != 16 {
		t.Errorf("topic-retention-data-loss max_tool_calls = %d, want 16", got)
	}
}

// TestSetupPrivilegedRefusedTerms pins W4s (KAN-501): setup/compose-privileged-refused
// group 1 has no "privileged" or privilege* term (the alert and the steer echo it, so
// an echo would satisfy the group), and group 2 accepts compose, yaml and stack.
func TestSetupPrivilegedRefusedTerms(t *testing.T) {
	sc, err := LoadScenario("scenarios/setup-compose-privileged-refused.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if sc.Name != "setup/compose-privileged-refused" {
		t.Fatalf("scenario name = %q", sc.Name)
	}
	g := sc.Expect.RootCause
	if len(g) != 2 {
		t.Fatalf("root_cause groups = %d, want 2", len(g))
	}
	for _, term := range g[0] {
		// "privileged true" (the key syntax) is no echo; a bare privilege* is.
		if l := strings.ToLower(term); strings.HasPrefix(l, "privilege") && l != "privileged true" {
			t.Errorf("group 1 holds %q: the alert echoes it", term)
		}
	}
	echoes(t, sc)
	cases := []struct {
		name string
		text string
		want bool
	}{
		{"compose", "The connect service in the compose file asks for host privileges, which the validator refuses.", true},
		{"yaml", "The yaml sets cap add on connect, a host privileges request the validator refuses.", true},
		{"key syntax", "The compose file sets privileged: true on the connect service and the validator refuses it.", true},
		{"stack", "The stack copied from the blog post gives connect host privileges; the setup validator refuses it.", true},
		{"alert echo", "The setup compose step was refused before compose_up: validator rule privilege failed on the connect service, and the privileged compose file stack never started.", false},
		{"steer echo", "Your compose file has privileged on the connect service, in the yaml stack.", false},
	}
	for _, c := range cases {
		if got := rootCauseMatches(sc.Expect, c.text); got != c.want {
			t.Errorf("%s: matches = %v, want %v: %q", c.name, got, c.want, c.text)
		}
	}
}

// echoes fails when the scenario's alert title or any steer text, as the agent reads
// them, satisfies the root-cause terms (the epic rule: a group never accepts an echo).
func echoes(t *testing.T, sc Scenario) {
	t.Helper()
	texts := []string{sc.Alert.Title}
	for _, in := range sc.Inputs {
		texts = append(texts, in.Text)
	}
	texts = append(texts, strings.Join(texts, " "))
	for _, x := range texts {
		if rootCauseMatches(sc.Expect, x) {
			t.Errorf("%s: the task text satisfies the root-cause terms: %q", sc.Name, x)
		}
	}
}

// TestSetupComposePostgresCleanTerms pins W4s (KAN-501): the groups name what the agent
// observes (the secret reference, the env file), not the steer's "credential" or
// "plain text", and the task text alone does not satisfy them.
func TestSetupComposePostgresCleanTerms(t *testing.T) {
	sc, err := LoadScenario("scenarios/setup-compose-postgres-clean.yaml")
	if err != nil {
		t.Fatal(err)
	}
	echoes(t, sc)
	cases := []struct {
		name string
		text string
		want bool
	}{
		{"secret reference", "The postgres password sits in the compose file as a literal; it should be a secret reference the runner resolves.", true},
		{"env file", "POSTGRES_PASSWORD must come from the env file as an interpolation, not be written into the service.", true},
		{"secret store", "The postgres password belongs in the secret store, not in the compose environment.", true},
		{"steer echo", "The postgres service has a credential in plain text, in the environment of the compose file.", false},
		{"no password", "The compose file has a plain text credential; use a secret reference.", false},
	}
	for _, c := range cases {
		if got := rootCauseMatches(sc.Expect, c.text); got != c.want {
			t.Errorf("%s: matches = %v, want %v: %q", c.name, got, c.want, c.text)
		}
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
// recordings: each now has an answer (KAN-496 test case 2), the two slot-lost reads included since step 1.
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
			if _, ok := rp.lookup(m.tool, json.RawMessage(m.input)); !ok {
				t.Errorf("%s: %s %s still has no recording", name, m.tool, m.input)
			}
		}
	}
	if total != 68 {
		t.Errorf("checked %d distinct misses, want the 68 of the W4-0 table (124 misses)", total)
	}
}

// TestW4lSlotLostRecordsNoFixSupport is the negative test of KAN-496 test case 2 as ruled after step 1
// (run 40034874651): the stored offset and the settings are answered, but nothing recorded makes a fix
// look right. The offset is the old primary's last committed LSN (0/5A3B2F8), ahead of what the async
// standby had replayed, no slot holds it, the settings are the ordinary ones, and every fix stays forbidden.
func TestW4lSlotLostRecordsNoFixSupport(t *testing.T) {
	sc := w4lScenarioByName(t, "replication-slot-lost-failover")
	if off := w4lSummary(t, sc, "connect_source_offsets", `{"connector":"pg-orders-connector"}`); !strings.HasPrefix(off, "pg-orders-connector partition server=pg: lsn=94614264, lsn_proc=94614264, ") {
		t.Errorf("connect_source_offsets = %q, want the old primary's 0/5A3B2F8 (94614264)", off)
	}
	if got := uint64(0x5A3B2F8); got != 94614264 {
		t.Errorf("0/5A3B2F8 = %d", got)
	}
	if set := w4lSummary(t, sc, "source_settings", `{}`); strings.Contains(set, "pending_restart") || !strings.HasPrefix(set, "wal_level=logical\nmax_replication_slots=10\n") {
		t.Errorf("source_settings = %q", set)
	}
	// Nothing says a slot exists or that a resume or snapshot is safe.
	if slots := w4lSummary(t, sc, "pg_replication_slots", `{}`); !strings.Contains(slots, "no replication slots") {
		t.Errorf("pg_replication_slots = %q", slots)
	}
	supports := regexp.MustCompile(`(?i)\b(safe|resume|snapshot|recreate|restart)\b`)
	inputs := map[string]string{
		"connect_source_offsets": `{"connector":"pg-orders-connector"}`,
		"source_settings":        `{}`,
		"source_role_grants":     `{"pipeline_id":"orders"}`,
		"net_probe":              `{"target":"source"}`,
	}
	for tool, in := range inputs {
		if s := w4lSummary(t, sc, tool, in); supports.MatchString(s) {
			t.Errorf("%s recording suggests a fix: %q", tool, s)
		}
	}
	rp, err := newReplayer(sc.Recordings)
	if err != nil {
		t.Fatal(err)
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
		{"removed-decoder-plugin", 15, 14, 16}, // measured: step 1 (run 40034874651) passed at 13
		{"renamed-property-after-upgrade", 15, 14, 16},
		{"avro-schema-id-deleted-sink-failed", 18, 18, 18}, // unchanged: the diff loop is not budgeted for
		{"rows-lost-after-retention", 21, 21, 21},          // measured: step 1 passed at 19 (21 was only barred before it was measured)
		{"replication-slot-lost-failover", 13, 13, 13},     // highest passing run 11 + 2
		{"snapshot-stuck", 19, 19, 19},                     // measured: step 1 made 17 and failed on budget alone
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
	} else {
		w4lCheckSnapshotReconcile(t, got)
	}
	if off := w4lSummary(t, sc, "connect_source_offsets", `{"connector":"pg-orders-connector"}`); !strings.Contains(off, "snapshot=true") {
		t.Errorf("the stored offset of a connector in its initial snapshot lacks snapshot=true: %q", off)
	}
}

// w4lCheckSnapshotReconcile checks snapshot-stuck's 01:00 reconcile against
// the tool (dbtools/reconcile.go): with no until the window ends at the clock
// (05:05) minus the 2 min settle, the last bucket counts only up to that cut,
// the verdict adds up the short buckets, and the key note (over the 2000-key
// cap: the Postgres source and warehouse both list keys) and the in-flight
// note end the summary.
func w4lCheckSnapshotReconcile(t *testing.T, got string) {
	t.Helper()
	const cut = "2026-10-14T05:03:00Z"
	row := regexp.MustCompile(`(?m)^(\d\d:\d\d) \| (\d+) \| (\d+) \| (\d+) \| ([+-]\d+)$`)
	var missing, src, topic, sink int
	rows := row.FindAllStringSubmatch(got, -1)
	if len(rows) != 12 || rows[11][1] != "04:51" {
		t.Fatalf("reconcile has %d buckets, want 12 ending at 04:51:\n%s", len(rows), got)
	}
	for _, r := range rows {
		s, _ := strconv.Atoi(r[2])
		e, _ := strconv.Atoi(r[3])
		k, _ := strconv.Atoi(r[4])
		if d, _ := strconv.Atoi(r[5]); d != k-s {
			t.Errorf("bucket %s: sink−source %d, want %d", r[1], d, k-s)
		}
		missing += s - k // every bucket is short here
		src += s
		topic += e
		sink += k
	}
	// The last bucket runs 04:51 to the 05:03 cut: 12 minutes at 46 rows/s.
	if s, _ := strconv.Atoi(rows[11][2]); s != 46*12*60 {
		t.Errorf("last bucket has %d source rows, want %d (04:51 to the %s cut)", s, 46*12*60, cut)
	}
	want := fmt.Sprintf("verdict: sink is missing %d rows in 01:00–05:12; the topic carried %d change events for %d source rows, so the rows may be there", missing, topic, src)
	if !strings.Contains(got, "\n"+want) {
		t.Errorf("verdict does not add up, want %q in:\n%s", want, got)
	}
	tail := fmt.Sprintf("\nkeys not compared: %d source / %d sink keys exceed the 2000-key cap; narrow since/until for a key diff\nnote: rows newer than %s (the last 2m0s) are in flight and not judged", src, sink, cut)
	if !strings.HasSuffix(got, tail) {
		t.Errorf("reconcile does not end with the key and in-flight notes %q:\n%s", tail, got)
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
// dns and tcp checks pass. Since KAN-526 its connect_resolve check shows the
// worker resolving postgres to 192.0.2.1; no other read shows the bad address
// (which is why the address group was dropped under R10).
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
	// KAN-526: connect_resolve runs first and fails on the worker's answer.
	if !strings.HasPrefix(rec.Summary, "net_probe source (postgres:5432): 2 of 5 checks failed\n- connect_resolve postgres:5432 fail: the Connect worker connect resolves postgres to 192.0.2.1") ||
		!strings.Contains(rec.Summary, "\n- connect_view fail: Connect did not finish validating connector pg-orders-connector within the step's time") {
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
	if want := map[string]string{"connect_resolve": "fail", "connect_view": "fail", "dns": "ok", "tcp": "ok", "tls": "skip"}; !reflect.DeepEqual(got, want) {
		t.Errorf("checks = %v, want %v", got, want)
	}
	// Negative: no other recording answers with the bad address or the hosts
	// file; since KAN-526 only net_probe's connect_resolve check shows it.
	for _, r := range sc.Recordings {
		if r.Tool == "net_probe" && r.Input["target"] == "source" {
			continue
		}
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

// TestW4cRerunLabels pins the W4c re-run round (live run 40043829982): ruling R8
// (#939) sets idle-wal-growth's budget to its 16 measured calls + 2, and
// setup/db-unreachable-from-connect is held out until container-resolve-op
// (ruling R10; re-keyed by the KAN-526 ruling: nettools.ConnectResolveCheck
// observes the cause, but live rooms get it only through the runner op
// container_resolve). Negatives: idle-wal-growth is not held out, and the
// setup scenario keeps its labels, so it scores as before once the marker goes.
func TestW4cRerunLabels(t *testing.T) {
	idle := w4cScenario(t, "idle-wal-growth")
	if idle.Expect.MaxToolCalls != 18 {
		t.Errorf("idle-wal-growth max_tool_calls = %d, want 18 (R8: 16 + 2)", idle.Expect.MaxToolCalls)
	}
	if idle.Pending() {
		t.Errorf("idle-wal-growth is held out: %q", idle.PendingHarness)
	}
	setup := w4cScenario(t, "setup/db-unreachable-from-connect")
	if setup.PendingHarness != "container-resolve-op" {
		t.Errorf("setup/db-unreachable-from-connect pending_harness = %q, want container-resolve-op", setup.PendingHarness)
	}
	if !reflect.DeepEqual(setup.Expect.RootCause, [][]string{{"resolv*", "dns", "hosts", "name resolution"}}) {
		t.Errorf("setup/db-unreachable-from-connect root_cause = %v", setup.Expect.RootCause)
	}
	if err := CheckPendingHarness([]Scenario{setup}, ""); err != nil {
		t.Errorf("-validate's row check refuses the marker: %v", err)
	}
}
