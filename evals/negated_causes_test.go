package evals

import (
	"strings"
	"testing"
)

// recoveredHyp is the final hypothesis of the healthy-after-recovery run that
// failed "claimed a cause at medium confidence on a healthy pipeline" (baseline
// dd87583): the "Failing component:" label the prompt asks for and the
// "transient ... crash-loop" history were read as current faults, which
// cancelled the healthy verdict.
const recoveredHyp = "Failing component: consumer group order-sync (transient lag on partition 1 during a pod crash-loop), now caught up with lag 0. " +
	"Root-cause component: not established; I found nothing wrong upstream (source, slot, connector, topic, schema) and no recent change. " +
	"The alert is stale and nothing needs fixing now."

func TestHasFaultLabelsAndHistory(t *testing.T) {
	tests := []struct {
		text string
		want bool
	}{
		{recoveredHyp, false},
		{"Failing component: order-sync, now caught up with lag 0.", false},
		{"First failing layer: consumer. It recovered at 09:31.", false},
		{"Transient lag on partition 1 during a pod crash-loop.", false},
		// Still faults.
		{"Failing component: order-sync, stuck on partition 1.", true},
		{"The failing component is order-sync.", true},
		{"order-sync is crash-looping on partition 1.", true},
		{"Failing component: order-sync (crash-loop on partition 1), lag 13908.", true},
	}
	for _, tt := range tests {
		if got := hasFault(tt.text); got != tt.want {
			t.Errorf("hasFault(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}

// TestHasFaultShortStale: a short duration next to "stale" is normal
// replication delay, as for "behind" (P3-W3f). The warehouse freshness tool's
// summary says "stale for 1m" and the model repeats it: "Healthy now: no
// component is failing ... warehouse 1m stale (s_01M59FT50JDCP8J1WVQD6MSG60)"
// (reviews/evals-2026-10-19/diagnosis.md, three runs out of three).
func TestHasFaultShortStale(t *testing.T) {
	tests := []struct {
		text string
		want bool
	}{
		{"Healthy now: no component is failing. order-sync lag 0, warehouse 1m stale (s_01M59FT50JDCP8J1WVQD6MSG60).", false},
		{"The warehouse is stale for 1m, which is normal.", false},
		{"warehouse stale 1m", false},
		{"analytics.orders stale for 2 minutes", false},
		{"warehouse 1 minute stale", false},
		{"warehouse stale (1m)", false},
		{"warehouse stale by 1m", false},
		{"warehouse 1 minute behind", false},
		// Still faults.
		{"The warehouse is stale for 3h.", true},
		{"warehouse stale 41m", true},
		{"warehouse 41m stale", true},
		{"The warehouse is stale.", true},
		{"warehouse 41 minutes behind", true},
	}
	for _, tt := range tests {
		if got := hasFault(tt.text); got != tt.want {
			t.Errorf("hasFault(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
	// The baseline hypothesis of healthy-after-recovery (evals/baseline/default.json,
	// score 55, "claimed a cause at medium confidence on a healthy pipeline") is
	// unchanged by the short-stale rule: it states no fault either way.
	if hasFault(recoveredHyp) {
		t.Errorf("hasFault(recoveredHyp) = true")
	}
	if term, clause, ok := faultClause("Healthy now, but the warehouse is stale for 3h."); !ok || term != "stale" || clause != "the warehouse is stale for 3h" {
		t.Errorf("faultClause = %q, %q, %v", term, clause, ok)
	}
	if n := healthyNote("warehouse 1m stale; nothing to fix"); n != "" {
		t.Errorf("healthyNote on a healthy text = %q", n)
	}
	// A medium-confidence hypothesis that names a long staleness still claims a cause.
	if !hasFault("Failing component: wh:analytics.orders, stale for 3h. Root-cause component: not established.") {
		t.Errorf("a long staleness no longer counts as a fault")
	}
}

// TestHealthyAfterRecoveryNotACause: a medium-confidence hypothesis that says
// the root cause is not established, the consumer is caught up and nothing
// needs fixing is a healthy verdict, not a cause claim; one that names a
// current fault at medium or high confidence is still a claim.
func TestHealthyAfterRecoveryNotACause(t *testing.T) {
	lag := step{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}}
	connect := step{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}}
	closing := step{message: "order-sync is caught up with lag 0 and every layer is OK. The alert is a stale re-notification; nothing needs fixing now."}
	tests := []struct {
		name        string
		script      []step
		wantClaims  bool
		wantHealthy bool
	}{
		{"the failed run's hypothesis", []step{lag, connect, {hypothesis: recoveredHyp, confidence: "medium"}, closing}, false, true},
		{"the failed run's hypothesis, no closing message", []step{lag, connect, {hypothesis: recoveredHyp, confidence: "medium"}}, false, true},
		{"closing message alone says nothing needs fixing", []step{lag, connect,
			{hypothesis: "Root-cause component: not established.", confidence: "low"}, closing}, false, true},
		{"names a current fault at medium", []step{lag, connect,
			{hypothesis: "Failing component: order-sync, stuck at offset 25377154 on partition 1. Root-cause component: kafka (a poison pill).", confidence: "medium"}, closing}, true, true},
		{"current crash-loop at high", []step{lag, connect,
			{hypothesis: "Failing component: order-sync, crash-looping on partition 1; nothing needs fixing upstream.", confidence: "high"}, closing}, true, true},
		{"closing message reports a current fault", []step{lag, connect, {hypothesis: recoveredHyp, confidence: "medium"},
			{message: "order-sync is stuck again on partition 1."}}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc, err := LoadScenario("scenarios/healthy-after-recovery.yaml")
			if err != nil {
				t.Fatal(err)
			}
			r := Judge(sc, transcriptOfInputs(t, sc, tt.script), nil, false)
			if r.ClaimsCause != tt.wantClaims || r.SaysHealthy != tt.wantHealthy {
				t.Errorf("claims_cause/says_healthy = %v/%v, want %v/%v; failures %v",
					r.ClaimsCause, r.SaysHealthy, tt.wantClaims, tt.wantHealthy, r.Failures)
			}
			if want := !tt.wantClaims && tt.wantHealthy; r.Pass != want {
				t.Errorf("pass = %v, want %v; failures %v", r.Pass, want, r.Failures)
			}
		})
	}
}

// TestForbiddenNegatedClauses (#17): a forbidden term inside a denial does
// not tell the story; a negator that does not reach the term, "not only",
// and a negated rule-out do not deny it.
func TestForbiddenNegatedClauses(t *testing.T) {
	crash := [][]string{{"consumer", "crash*"}}
	pill := [][]string{{"poison pill"}}
	reset := [][]string{{"reset*", "latest"}}
	slot := [][]string{{"slot", "inactive"}}
	lostSlot := [][]string{{"lost the slot"}}
	tests := []struct {
		rows [][]string
		text string
		want bool // true = the forbidden story is told
	}{
		// Rule-outs before the term.
		{pill, "I ruled out a poison pill.", false},
		{pill, "The trace rules out a poison pill on partition 1.", false},
		{pill, "Ruling out a poison pill, the cause is the schema change.", false},
		{crash, "The lag excludes a consumer crash.", false},
		// Predicate denials after the term.
		{pill, "A poison pill is not the cause.", false},
		{pill, "A poison pill was ruled out by the trace.", false},
		{pill, "A poison pill isn't the problem.", false},
		{pill, "A poison pill cannot be the cause: the trace reaches the warehouse.", false},
		{crash, "The consumer did not crash.", false},
		{crash, "The consumer crash, which many suspected, is not the cause.", false},
		// Plain negators before the term.
		{pill, "Not a poison pill.", false},
		{pill, "There is no poison pill on partition 1.", false},
		{crash, "Neither a consumer crash nor a bug.", false},
		// Negated rule-outs still assert the term.
		{pill, "I cannot rule out a poison pill.", true},
		{pill, "I can't rule out a poison pill.", true},
		{pill, "We could not yet rule out a poison pill.", true},
		{pill, "A poison pill cannot be ruled out.", true},
		{pill, "A poison pill has not been ruled out.", true},
		// "not only" asserts.
		{crash, "Not only did the consumer crash but the slot was lost.", true},
		{crash, "It is not just a consumer crash.", true},
		// A negator that does not reach the term.
		{crash, "The cause is a consumer crash, not the schema.", true},
		{reset, "The fix is to reset order-sync to latest, not to rewind it.", true},
		{pill, "Not the schema but a poison pill on partition 1.", true},
		{crash, "The schema change is not the cause; the consumer crash is.", true},
		// A negator after a clause marker denies another verb, not the term.
		{slot, "The slot has become inactive so the snapshot cannot progress.", true},
		{slot, "The slot is inactive because the connector cannot reach Postgres.", true},
		{slot, "The slot is not inactive.", false},
		{pill, "X cannot be the cause, and a poison pill cannot be the cause either.", false},
		{reset, "A reset to latest and a rewind are not options.", false},
		// A row's terms must share one comma part after a predicate; a
		// comma list or aside keeps them together.
		{lostSlot, "Postgres was restarted; nothing is lost, the slot is still there.", false},
		{slot, "The slot is fine, the consumer is inactive.", false},
		{crash, "order-sync, the consumer and the registry all crashed.", true},
		{crash, "The consumer, which many suspected, crashed.", true},
		{crash, "The consumer crashed, then the slot filled up.", true},
		// Unchanged from before #17.
		{reset, "Do not reset the group to latest.", false},
		{reset, "Resetting to latest is not an option.", false},
		{reset, "A reset to latest is advised against.", false},
		{reset, "Reset without going to latest.", false},
		{reset, "Do not reset to latest; actually, reset to latest anyway.", true},
	}
	for _, tt := range tests {
		if got := matchForbidden(tt.rows, tt.text); got != tt.want {
			t.Errorf("matchForbidden(%v, %q) = %v, want %v", tt.rows, tt.text, got, tt.want)
		}
	}
}

// TestForbiddenNegationInJudge: a correct schema-change hypothesis that rules
// out the forbidden stories is a right root cause; the same hypothesis that
// cannot rule one out tells it.
func TestForbiddenNegationInJudge(t *testing.T) {
	sc := loadSeed()
	good := "Task 0 of pg-orders-connector failed because a Postgres migration changed amount from int4 to numeric and Schema Registry rejected the new value schema (409, BACKWARD). "
	for _, tt := range []struct {
		tail string
		want bool
	}{
		{"I ruled out a poison pill and a consumer crash.", true},
		{"A poison pill is not the cause.", true},
		{"I cannot rule out a poison pill.", false},
	} {
		text := good + tt.tail
		if got := rootCauseMatches(sc.Expect, text); got != tt.want {
			t.Errorf("rootCauseMatches(...%q) = %v, want %v (forbidden %v)", tt.tail, got, tt.want,
				matchForbidden(sc.Expect.RootCauseForbidden, text))
		}
	}
	if !strings.Contains(sc.Name, "schema") {
		t.Fatalf("seed is %q", sc.Name)
	}
}
