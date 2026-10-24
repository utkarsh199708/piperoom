package evals

import (
	"encoding/json"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
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
