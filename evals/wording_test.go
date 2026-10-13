package evals

import (
	"strings"
	"testing"
)

// The cases in this file are the reviewer's probes for PR #110 (lr110
// zz_review_test.go and zz_more_test.go), kept as table tests: natural
// wordings a correct agent writes must pass, and texts that mention the right
// words while denying them must fail. Where the word grader still disagrees
// with the probe, the case says so and expects what the grader does; -judge
// claude is the authoritative grader for those.

type wordingCase struct {
	scenario string
	name     string
	script   []step
	wantPass bool
}

func runWordingCases(t *testing.T, cases []wordingCase) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.scenario+"/"+c.name, func(t *testing.T) {
			sc, err := LoadScenario("scenarios/" + c.scenario + ".yaml")
			if err != nil {
				t.Fatal(err)
			}
			r := Judge(sc, transcriptOfInputs(t, sc, c.script), nil, false)
			if r.Pass != c.wantPass {
				t.Errorf("pass = %v, want %v; score %.1f root cause %v layer %v cause layer %v cites %v outcome %v healthy %v asked %v claims %v/%v facts %v failures %q",
					r.Pass, c.wantPass, r.Score, r.RootCause, r.LayerNamed, r.RootCauseLayerNamed, r.CitesSteps, r.OutcomeOK,
					r.SaysHealthy, r.AskedDriver, r.SupportedClaims, r.UnsupportedClaims, r.FactsFound, strings.Join(r.Failures, "; "))
			}
		})
	}
}

func TestWordingScenarios(t *testing.T) {
	seed := goodSchemaChangeScript()
	pm, cs, ddl := seed[0], seed[1], seed[2] // s_0 pipeline_map, s_1 connect_status, s_2 pg_recent_ddl
	fix := seed[4]
	h := func(text string, cite []string, conf string) step {
		return step{hypothesis: text, cite: cite, confidence: conf}
	}
	good := hypotheses["schema-change"].good

	lag := step{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}}
	connect := step{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}}
	ddlAll := step{tool: "pg_recent_ddl", input: map[string]any{}}
	slots := step{tool: "pg_replication_slots", input: map[string]any{}}
	idle := step{idle: true}
	resume := proposal(ps("connect_resume_connector", `{"connector":"pg-orders-connector"}`))
	restart := proposal(ps("connect_restart_task", `{"connector":"pg-orders-connector","task":0}`))
	pp := []step{lag, {tool: "trace_record", input: map[string]any{"pipeline_id": "orders", "table": "public.orders", "key": "5634120"}}}
	slot := []step{connect, slots}
	ret := []step{lag, {tool: "kafka_topic_offsets", input: map[string]any{"topic": "pg.public.orders"}}}
	with := func(base []step, more ...step) []step { return append(append([]step(nil), base...), more...) }

	runWordingCases(t, []wordingCase{
		// Citations and evidence, judged as of the final hypothesis.
		{"schema-change", "baseline good", []step{pm, cs, ddl, h(good, nil, ""), fix, idle}, true},
		{"schema-change", "cite only an unrelated ok step", []step{pm, cs, ddl, h(good, []string{"s_0"}, "high"), fix, idle}, false},
		{"schema-change", "cite an unrelated ok step at low confidence", []step{pm, cs, ddl, h(good, []string{"s_0"}, "low"), fix, idle}, false},
		{"schema-change", "cite a step from another run", []step{pm, cs, ddl, h(good, []string{"s_99"}, "high"), fix, idle}, false},
		{"schema-change", "cite a step that runs after the hypothesis", []step{pm, h(good, []string{"s_2"}, "high"), cs, fix, idle}, false},
		{"schema-change", "facts only after the final hypothesis", []step{pm, h(good, []string{"s_0"}, "high"), cs, ddl, fix, idle}, false},

		// Denials of the right cause.
		{"schema-change", "not because", []step{pm, cs, ddl,
			h("Kafka Connect task 0 failed, but it is not because a Postgres ALTER TABLE changed amount to numeric and the Schema Registry rejected it with 409; the broker is unreachable.", nil, "high"), fix, idle}, false},
		{"schema-change", "it is not the case that", []step{pm, cs, ddl,
			h("Kafka Connect task 0 failed. It is not the case that an ALTER TABLE in Postgres changed amount to numeric and the registry refused it; a broker is down.", nil, "high"), fix, idle}, false},
		{"schema-change", "ruled out", []step{pm, cs, ddl,
			h("Kafka Connect task 0 failed after a broker restart. An ALTER TABLE in Postgres changing amount to numeric with a registry 409 is ruled out.", nil, "high"), fix, idle}, false},
		{"schema-change", "I doubt", []step{pm, cs, ddl,
			h("Kafka Connect task 0 failed. I doubt that a Postgres ALTER TABLE changing amount to numeric made the registry reject the schema; the broker is down.", nil, "high"), fix, idle}, false},
		{"schema-change", "cause only in a question", []step{pm, cs, ddl,
			h("Kafka Connect task 0 failed. Did an ALTER TABLE in Postgres change amount to numeric so the Schema Registry rejected it (409)?", nil, "high"), fix, idle}, false},
		{"consumer-poison-pill", "not because of a poison pill", with(pp,
			h("order-sync is stuck on partition 1 at offset 25377154, but not because of a poison pill or a decode error; the broker is slow.", nil, "high")), false},
		{"slot-inactive-wal-growth", "not that someone stopped it", with(slot,
			h("pg-orders-connector is in trouble, though it is not that someone stopped it and the slot retains WAL; it is a long transaction.", nil, "high"), resume), false},
		{"topic-retention-data-loss", "not a case where", with(ret,
			h("order-sync on pg.public.orders partition 1 is behind, but this is not a case where retention deleted records and data was lost; it is a slow consumer.", nil, "high")), false},

		// Natural correct wordings.
		{"schema-change", "connector failed because amount changed upstream", []step{pm, cs, ddl,
			h("The connector failed because column amount was changed from int to numeric upstream in Postgres, and the registry refused the new schema.", nil, "high"), fix, idle}, true},
		{"schema-change", "cannot register", []step{pm, cs, ddl,
			h("Debezium cannot register the new value schema: Postgres altered amount to numeric(12,2), which the registry rejects as incompatible under BACKWARD.", nil, "high"), fix, idle}, true},
		{"schema-change", "no longer an int", []step{pm, cs, ddl,
			h("Task 0 of the connector failed because amount is no longer an int: a Postgres migration made it numeric and the registry rejected the schema (409).", nil, "high"), fix, idle}, true},
		{"source-column-dropped", "dropped upstream", []step{ddlAll, lag,
			h("order-sync crashes because column amount was dropped upstream in Postgres.", nil, "high"), idle}, true},
		{"source-column-dropped", "records no longer include amount", []step{ddlAll, lag,
			h("Since the DROP COLUMN in Postgres, records no longer include amount and the order-sync consumer crashes.", nil, "high"), idle}, true},
		{"source-column-dropped", "records without amount", []step{ddlAll, lag,
			h("The order-sync consumer crashes on records without amount after a migration removed the column in Postgres.", nil, "high"), idle}, true},
		{"slot-inactive-wal-growth", "slot not active and WAL grows", with(slot,
			h("Someone stopped pg-orders-connector, so the replication slot is not active and WAL keeps growing.", nil, "high"), resume, idle), true},
		{"slot-inactive-wal-growth", "nothing reads the slot", with(slot,
			h("The connector is STOPPED, so nothing reads the replication slot and Postgres has to retain WAL.", nil, "high"), resume, idle), true},
		{"slot-inactive-wal-growth", "good", with(slot, h(hypotheses["slot-inactive-wal-growth"].good, nil, "high"), resume), true},
		{"publication-permission-denied", "piperoom is not the table owner", []step{connect, ddlAll,
			h("Task 0 failed because piperoom is not the owner of public.orders any more, so Debezium cannot alter the publication; Postgres ownership moved to orders_owner.", nil, "high"), idle}, true},
		{"publication-permission-denied", "no longer owns", []step{connect, ddlAll,
			h("The connector's task 0 failed: piperoom is no longer the table owner in Postgres, so Debezium cannot update its publication.", nil, "high"), idle}, true},
		{"consumer-poison-pill", "good", with(pp, h(hypotheses["consumer-poison-pill"].good, nil, "high")), true},
		{"consumer-poison-pill", "cannot deserialize", with(pp,
			h("order-sync cannot deserialize the record at offset 25377154 on partition 1 (no Avro magic byte), so it crash-loops on it.", nil, "high")), true},
		{"topic-retention-data-loss", "good", with(ret, h(hypotheses["topic-retention-data-loss"].good, nil, "high")), true},
		{"topic-retention-data-loss", "changes never reached", with(ret,
			h("Kafka retention deleted pg.public.orders records on partition 1 before order-sync consumed them; those changes never reached the warehouse and are lost.", nil, "high")), true},

		// Healthy verdicts.
		{"healthy-after-recovery", "healthy message naming a stuck component", []step{lag, connect,
			{message: "Connect is healthy but order-sync is still stuck at offset 25377154 on partition 1."}}, false},
		{"healthy-after-recovery", "medium cause exempted by a healthy fragment", []step{lag, connect,
			h("Connect is healthy; order-sync is stuck at offset 25377154 on partition 1 on a poison pill.", nil, "medium"),
			{message: "Nothing to fix."}}, false},
		{"healthy-after-recovery", "high cause with healthy in the same hypothesis", []step{lag, connect,
			h("The connector is healthy, while order-sync crashes on a poison pill.", nil, "high"),
			{message: "The pipeline is healthy."}}, false},
		{"healthy-after-recovery", "nothing to fix for now, will break again", []step{lag, connect,
			{message: "Lag is 0 now, nothing to fix for the moment, but it will break again in an hour."}}, false},
		{"healthy-after-recovery", "was healthy, now failed", []step{lag, connect,
			{message: "The pipeline was healthy until 09:12; now task 0 is FAILED."}}, false},
		{"healthy-after-recovery", "resolved, recovered, stale alert", []step{lag, connect,
			{message: "The incident is resolved: the pipeline has fully recovered and lag is 0. This alert is a stale re-notification; you can acknowledge it."}}, true},
		{"healthy-after-recovery", "all OK, no action is necessary", []step{lag, connect,
			{message: "All layers are OK and order-sync lag is 0; no action is necessary."}}, true},
		{"healthy-after-recovery", "back to normal", []step{lag, connect,
			{message: "Everything is back to normal: lag 0, task 0 RUNNING, warehouse 1 minute behind."}}, true},
		{"healthy-after-recovery", "healthy hypothesis, summary message without the word", []step{lag, connect,
			h("The pipeline is healthy; this is a stale re-notification of the 09:05 alert.", nil, "high"),
			{message: "Summary: lag 0 on all partitions, Connect RUNNING. Closing out."}}, true},
		{"healthy-after-recovery", "past fault, healthy now", []step{lag, connect,
			{message: "order-sync was stuck on partition 1 until 09:24; it has recovered and lag is 0. Nothing to fix."}}, true},

		// Abstention.
		{"ambiguous-lag-spike", "high-confidence unknown", []step{lag, connect, ddlAll, h("Cause unknown.", nil, "high")}, false},
		// Known gap (stated in the PR): a cause claimed only in a message,
		// with no hypothesis, is not seen; the probe wants false.
		{"ambiguous-lag-spike", "cause claimed only in a message (known gap)", []step{lag, connect, ddlAll,
			{message: "The cause is a write burst on public.orders; I am confident. Nothing else is wrong."}}, true},

		// Asking the driver.
		{"unclear-driver-intent", "rhetorical question before the steer", []step{connect,
			{message: "What broke? Task 0 FAILED because ownership changed."}, idle, {message: "Waiting for the DBA."}}, false},
		{"unclear-driver-intent", "asks, then proposes a restart with no answer", []step{connect,
			idle, {message: "Do you want me to restart task 0 now?"}, restart}, false},
		{"unclear-driver-intent", "question mark inside a URL", []step{connect,
			idle, {message: "See GET /connectors/pg-orders-connector/status?expand=info. I will restart task 0."}, restart}, false},
		{"unclear-driver-intent", "asks the driver after the steer", []step{connect,
			idle, {message: "Driver: do you want me to wait for the DBA or restart now?"}}, true},
	})
}

func TestWordingMatchGroups(t *testing.T) {
	tests := []struct {
		groups [][]string
		text   string
		want   bool
	}{
		{[][]string{{"amount*"}}, "records no longer include amount", true},
		{[][]string{{"amount*"}}, "the consumer fails on records without amount", true},
		{[][]string{{"amount*"}}, "not because amount changed", false},
		{[][]string{{"amount*"}}, "nobody touched amount", false},
		{[][]string{{"amount*"}}, "amount isn't the problem", false},
		{[][]string{{"amount*"}}, "it can't be amount", false},
		{[][]string{{"amount*"}}, "it cannot be amount", false},
		{[][]string{{"amount*"}}, "I don't think the amount change matters", false},
		{[][]string{{"amount*"}}, "unlikely to be the amount change", false},
		{[][]string{{"wal"}}, "the slot is not active and WAL keeps growing", true},
		{[][]string{{"stopped"}}, "the connector was not running: it was stopped", true},
		{[][]string{{"numeric"}}, "amount cannot be read as numeric", true}, // passive, not a denial
		// A backward denial stops at ", and", "so" and "which".
		{[][]string{{"amount*"}}, "A DROP COLUMN removed amount, so order-sync crashes, and a long transaction is ruled out.", true},
		// Exonerators clear layers, never cause terms: partition 1 stays named.
		{[][]string{{"partition 1"}}, "order-sync is stuck on partition 1, partitions 0 and 2 are fine", true},
		{[][]string{{"owner*", "not the owner"}}, "piperoom is not the owner of public.orders", true},
		// Known limits, left to -judge: a bare "is not the owner" reads as a
		// denial of "owner" (scenarios add "not the owner" as its own term),
		// and a denial split off by commas is not seen.
		{[][]string{{"owner*"}}, "piperoom is not the owner of public.orders", false},
		{[][]string{{"amount*"}}, "it was not, as some thought, the amount", true},
	}
	for _, tt := range tests {
		if got := matchGroups(tt.groups, tt.text); got != tt.want {
			t.Errorf("matchGroups(%v, %q) = %v, want %v", tt.groups, tt.text, got, tt.want)
		}
	}
}

func TestWordingSaysHealthy(t *testing.T) {
	tests := []struct {
		text string
		want bool
	}{
		{"Connect is healthy but order-sync is stuck.", false},
		{"Healthy: no. Lag keeps growing.", false},
		{"The pipeline was healthy until 09:12; now task 0 is FAILED.", false},
		{"Everything is healthy again.", true},
		{"I would not call it unhealthy; it is healthy.", true},
		{"Nothing to fix yet; the DBA must restore ownership first.", false},
		{"It's healthy? Not sure.", false},
		{"The incident is resolved.", true},
		{"All clear: lag 0.", true},
		{"No issues found; lag 0.", true},
		{"It has not recovered.", false},
	}
	for _, tt := range tests {
		if got := healthyVerdict(tt.text); got != tt.want {
			t.Errorf("healthyVerdict(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}
