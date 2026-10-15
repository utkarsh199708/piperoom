package evals

import (
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/owners"
)

// The golden corpus (#395 review round 3): one table of every text the
// deterministic matcher has been judged on, run as a whole after every
// matcher change. A change that flips any row is not done.
//
// Scenario rows read a hypothesis with rootCauseMatches against the real
// expect of a default or held-out scenario: the correct answers of the
// existing tests must pass, and their trap, shotgun and wrong-story answers
// must fail. Matcher rows pin one function on one text: matchForbidden
// (fb, want = the story is told), matchGroups (gr, want = the terms are
// stated), causeHedge (hg, want = hedged) and namesLayer (ly).
//
// A gap row is a case the matcher deliberately leaves to the LLM judge or to
// the scenario terms (C7): it is logged with its result and never fails.

type corpusRow struct {
	src      string
	kind     string // scenario | forbidden | groups | hedge | layer
	scenario string // scenario id, or "heldout/<id>"
	terms    [][]string
	layer    string
	text     string
	want     bool
	gap      string
}

func fb(src string, row []string, text string, want bool) corpusRow {
	return corpusRow{src: src, kind: "forbidden", terms: [][]string{row}, text: text, want: want}
}

func gr(src string, groups [][]string, text string, want bool) corpusRow {
	return corpusRow{src: src, kind: "groups", terms: groups, text: text, want: want}
}

func hg(src, text string, want bool) corpusRow {
	return corpusRow{src: src, kind: "hedge", text: text, want: want}
}

func ly(src, layer, text string, want bool) corpusRow {
	return corpusRow{src: src, kind: "layer", layer: layer, text: text, want: want}
}

func sc(src, scenario, text string, want bool) corpusRow {
	return corpusRow{src: src, kind: "scenario", scenario: scenario, text: text, want: want}
}

func gap(r corpusRow, why string) corpusRow {
	r.gap = why
	return r
}

// retentionCause states consumer-offsets-out-of-range-retention's root cause
// and nothing else; the review rounds append a wrong story to it.
const retentionCause = "Order-sync's committed offsets were deleted by retention."

// corpusMatcherRows are the tickets' reproductions (#283, #334, #335) and
// every text quoted in review rounds 1 and 2 of #395.
var corpusMatcherRows = []corpusRow{
	// #283 and #334: forbidden stories inside a denial or a warning are not
	// told; the same stories asserted still are.
	// #283: the "not" denies the crash, not the rule-out.
	fb("negator of another verb before a rule-out", []string{"reset*", "latest"}, "Order-sync did not crash, I ruled out a reset to latest.", false),
	fb("crash denied before a rule-out", []string{"order-sync", "crash*"}, "Order-sync did not crash, I ruled out a reset to latest.", false),
	fb("negated rule-out still asserts", []string{"reset*", "latest"}, "I cannot rule out a reset to latest.", true),
	fb("negated rule-out with adverb", []string{"reset*", "latest"}, "I can't yet rule out a reset to latest.", true),
	fb("rule-out not yet done", []string{"reset*", "latest"}, "A reset to latest has not been ruled out.", true),
	// #334: "nothing".
	fb("nothing is lost", []string{"slot", "lost"}, "The slot still exists, nothing is lost.", false),
	fb("nothing changed", []string{"ddl", "chang*"}, "The DDL log is empty and nothing changed.", false),
	fb("nothing incompatible", []string{"incompatible", "schema"}, "There is nothing incompatible in the schema.", false),
	fb("nothing reaches only its own predicate", []string{"reset*", "latest"}, "Retention is fine and nothing was lost, so reset the group to latest.", true),
	fb("lost slot told", []string{"slot", "lost"}, "The slot was lost when the DBA dropped it.", true),
	// #334: a conditional warning.
	fb("would warns", []string{"reset*", "latest"}, "Resetting the whole group to latest would skip every partition's unread changes.", false),
	fb("could warns", []string{"reset*", "latest"}, "A reset to latest could lose the unread changes.", false),
	fb("could be the cause", []string{"reset*", "latest"}, "A reset to latest could be the cause.", true),
	fb("could have", []string{"reset*", "latest"}, "A reset to latest could have skipped the records.", true),
	fb("which would explain", []string{"reset*", "latest"}, "Someone ran a reset to latest, which would explain the gap.", true),
	fb("would explain", []string{"reset*", "latest"}, "A reset to latest would explain the gap.", true),
	fb("conditional is not a forward denial", []string{"reset*", "latest"}, "Would anyone notice? Someone reset the group to latest.", true),
	// Review round 1, item 2: a negated rule-out after would/could
	// still keeps the story open.
	fb("could not be ruled out", []string{"reset*", "latest"}, "A reset to latest could not be ruled out.", true),
	fb("could not be excluded", []string{"reset*", "latest"}, "A reset to latest could not be excluded.", true),
	// Item 3: "nothing" denies only its own predicate.
	fb("nothing, colon, story", []string{"reset*", "latest"}, "Nothing older than 48 h can be recovered: reset the group to latest to get it going.", true),
	fb("nothing, comma, story", []string{"reset*", "latest"}, "Nothing older than 48 h can be recovered, reset the group to latest to get it going.", true),
	fb("nothing to do with, colon, story", []string{"reset*", "latest"}, "The lost records have nothing to do with retention: someone reset the group to latest.", true),
	fb("nothing, and, story", []string{"reset*", "latest"}, "Nothing was lost and the group was reset to latest.", true),
	// Item 4: adverbs before be/explain, "cause", and a would/could
	// whose subject is not the story.
	fb("could well be", []string{"reset*", "latest"}, "A reset to latest could well be the cause.", true),
	fb("could still be", []string{"reset*", "latest"}, "A reset to latest could still be the cause.", true),
	fb("would likely explain", []string{"reset*", "latest"}, "A reset to latest would likely explain the gap.", true),
	fb("would probably explain", []string{"reset*", "latest"}, "A reset to latest would probably explain the gap.", true),
	fb("could also explain", []string{"reset*", "latest"}, "A reset to latest could also explain the gap.", true),
	fb("would cause", []string{"reset*", "latest"}, "A reset to latest would cause this exact gap.", true),
	fb("past ability after a comma", []string{"reset*", "latest"}, "After the reset to latest, order-sync could only read new records.", true),
	fb("and would", []string{"reset*", "latest"}, "The group was reset to latest and would skip every unread change.", true),
	fb("and could", []string{"order-sync", "crash*"}, "Order-sync crashed on the record and could only be restarted by hand.", true),
	fb("conditional warning with adverb", []string{"reset*", "latest"}, "Resetting the group to latest would probably skip unread changes.", false),
	// Item 5: hedges that keep a rule-out open.
	fb("not able to rule out", []string{"reset*", "latest"}, "I was not able to rule out a reset to latest.", true),
	fb("no way to rule out", []string{"reset*", "latest"}, "There is no way to rule out a reset to latest.", true),
	fb("cannot conclusively rule out", []string{"reset*", "latest"}, "I cannot conclusively rule out a reset to latest.", true),
	fb("can't really rule out", []string{"reset*", "latest"}, "We can't really rule out a reset to latest.", true),
	fb("no one has ruled out", []string{"reset*", "latest"}, "No one has ruled out a reset to latest.", true),
	fb("not crash and I ruled out", []string{"reset*", "latest"}, "Order-sync did not crash and I ruled out a reset to latest.", false),
	// Review round 2, item 2: "confirm or rule out" keeps the story open.
	fb("r2.2", []string{"reset*", "latest"}, "We cannot confirm or rule out a reset to latest.", true),
	fb("r2.2", []string{"reset*", "latest"}, "I can't confirm or rule out a reset to latest yet.", true),
	fb("r2.2", []string{"reset*", "latest"}, "I could not confirm or rule out a reset to latest.", true),
	fb("r2.2", []string{"reset*", "latest"}, "We have not confirmed or ruled out a reset to latest.", true),
	fb("r2.2", []string{"reset*", "latest"}, "I have not checked and ruled out a reset to latest.", true),
	// Item 3: a would/could that is not the story's own predicate.
	fb("r2.3", []string{"reset*", "latest"}, "The reset to latest meant order-sync could only read new records.", true),
	fb("r2.3", []string{"reset*", "latest"}, "Because of the reset to latest order-sync could only see new records.", true),
	fb("r2.3", []string{"reset*", "latest"}, "After the reset to latest order-sync could only read new records.", true),
	fb("r2.3", []string{"reset*", "latest"}, "A reset to latest is what I would bet on.", true),
	fb("r2.3", []string{"reset*", "latest"}, "Someone reset the group to latest hoping order-sync would recover.", true),
	fb("r2.3", []string{"order-sync", "crash*"}, "Order-sync crashed on a record nobody could decode.", true),
	// Item 5: "nothing" denies only its own predicate.
	fb("r2.5", []string{"reset*", "latest"}, "Nothing older than 48 h was consumed after someone reset the group to latest.", true),
	fb("r2.5", []string{"reset*", "latest"}, "Nothing older than 48 h can be recovered after someone reset the group to latest.", true),
	fb("r2.5", []string{"reset*", "latest"}, "Nothing was read from partition 1 after the reset to latest.", true),
	fb("r2.5", []string{"reset*", "latest"}, "Nothing was lost before someone reset the group to latest.", true),
	fb("r2.5", []string{"reset*", "latest"}, "Someone reset the group to latest, which did nothing for the lag.", true),
	fb("r2.5", []string{"reset*", "latest"}, "The group was reset to latest and that did nothing for the lag.", true),
	fb("r2.5", []string{"order-sync", "crash*"}, "Order-sync crashed and a restart did nothing.", true),
	// Item 6: an aside right after the negator.
	fb("r2.6", []string{"reset*", "latest"}, "We cannot, however, rule out a reset to latest.", true),
	fb("r2.6", []string{"reset*", "latest"}, "I could not, however, rule out a reset to latest.", true),
	fb("r2.6", []string{"reset*", "latest"}, "I can't, honestly, rule out a reset to latest.", true),
	fb("r2.6", []string{"reset*", "latest"}, "This does not, by itself, rule out a reset to latest.", true),
	fb("r2.6", []string{"reset*", "latest"}, "A reset to latest has not, however, been ruled out.", true),
	fb("r2.6", []string{"reset*", "latest"}, "We haven't, so far, ruled out a reset to latest.", true),
	fb("r2.6", []string{"reset*", "latest"}, "We could not, at first, rule out a reset to latest.", true),
	// Nit 8: exclusive constructions.
	fb("r2.8", []string{"reset*", "latest"}, "Nothing explains the gap except a reset to latest.", true),
	fb("r2.8", []string{"reset*", "latest"}, "The cause was nothing other than a reset to latest.", true),

	// #283: ", not X" contrasts a layer name with what came before it.
	gr("contrast", [][]string{{"debezium"}}, "The slot was dropped by a DBA, not Debezium.", false),
	gap(gr("contrast", [][]string{{"order-sync"}}, "The registry rejected the schema, not order-sync.", false),
		"a pipeline name is no layer word; contrastNot reads only layer words, the judge decides"),
	gr("contrast", [][]string{{"registry"}}, "The registry rejected the schema, not order-sync.", true),
	gr("contrast", [][]string{{"replication slot"}}, "Task 0 failed, not able to read the replication slot.", true),
	gr("contrast", [][]string{{"debezium"}}, "Debezium is not running.", true),
	gr("contrast", [][]string{{"debezium"}}, "Not Debezium.", true), // no comma before it
	// Review round 1, item 1: a failure described after a comma is no
	// contrast.
	gr("contrast", [][]string{{"amount*"}}, "order-sync crashed, cannot deserialize amount.", true),
	gr("contrast", [][]string{{"avro"}}, "Task 0 failed, cannot decode Avro.", true),
	gr("contrast", [][]string{{"deserializ*", "decod*"}}, "Order-sync is stuck on partition 1, cannot decode the record.", true),
	gr("contrast", [][]string{{"publication*"}}, "Task 0 failed, cannot read the publication.", true),
	gr("contrast", [][]string{{"commit*"}}, "Order-sync is running, not committing offsets.", true),
	gr("contrast", [][]string{{"debezium"}}, "The registry failed, not just Debezium.", true),
	gr("contrast", [][]string{{"debezium"}}, "Two layers broke, not only Debezium but also the registry.", true),
	gr("contrast", [][]string{{"debezium"}}, "The registry failed, not yet Debezium.", true),
	// Review round 2, item 1 and nit 7: an adjective or a preposition
	// after "not" describes the failure.
	gr("r2.1", [][]string{{"compatib*"}}, "The registry rejected v3, not compatible with v2.", true),
	gr("r2.1", [][]string{{"avro"}}, "Task 0 failed, not valid Avro.", true),
	gr("r2.7", [][]string{{"wire format"}}, "Order-sync stops at offset 42 on partition 1, not in wire format.", true),

	// #283: alternatives after a "Fix:" label are fixes, not causes.
	hg("fix label", "Root cause: the connector task failed. Fix: restart the connector; alternatively, delete and recreate it.", false),
	hg("fix label", "Proposed fix: restart task 0, or alternatively resume the connector.", false),
	hg("fix label", "Fix: restart the connector. Root cause: either the slot or the registry.", true),
	hg("fix label", "Fix: restart the connector\nAlternatively the registry is at fault.", true),
	hg("fix label", "Root cause: alternatively the registry dropped the subject.", true),
	// Review round 1, item 6: the fix scope ends with its sentence.
	hg("fix label", "Root cause: the slot was dropped. Fix: recreate the slot. It could also be the registry.", true),
	hg("fix label", "Root cause: the slot was dropped. Fix: recreate the slot. Alternatively, the registry rejected the schema.", true),
	hg("fix label", "The slot was dropped; fix: recreate it. Or possibly the registry is at fault.", true),
	hg("fix label", "The slot was dropped; fix: recreate it; alternatively, restart Debezium.", false),
	// A combined label is no fix label; a fix list runs to a blank line.
	hg("fix label", "Root cause and fix: either the slot was dropped or the registry is down.", true),
	hg("fix label", "Root cause: the slot was dropped.\nProposed fix:\n1. Recreate the slot.\n2. Alternatively, restart Debezium.", false),
	hg("fix label", "Proposed fix:\n1. Recreate the slot.\n\nRoot cause: either the slot or the registry.", true),
	// Review round 2, item 4: only list items under a label that ends its
	// line belong to the fix.
	hg("r2.4", "Root cause: the slot was dropped.\nProposed fix:\n1. Recreate the slot.\nIt could also be the registry.", true),
	hg("r2.4", "Root cause: the slot was dropped.\nFix:\n- recreate the slot\nAlternatively, the registry could be at fault.", true),
	hg("r2.4", "Root cause: the slot was dropped.\nProposed fix:\n1. Recreate the slot.\nAlternatively the registry rejected the schema.", true),
	hg("r2.4", "Root cause: the slot was dropped by a DBA.\nFix:\nRecreate the slot and restart the connector. The registry could also be at fault.", true),

	// #335: an SMT names Connect; SASL, SCRAM and the Kafka user name Kafka.
	ly("#335", "kafka-connect", "Root cause: one row's promised_at is not a date, the TimestampConverter SMT throws, errors.tolerance=none fails task 0.", true),
	ly("#335", "kafka", "pg-payments-connector task 0 fails SASL authentication: the SCRAM password of the Kafka user debezium changed.", true),
	ly("#335", "kafka", "Kafka rejects the SASL login.", true),
	ly("#335", "kafka", "The Kafka user debezium lost its password.", true),
	ly("#335", "kafka", "Kafka Connect restarted.", false),
	ly("#335", "kafka", "Debezium is up, cannot reach the brokers.", true),
	// Review round 2, nit 7.
	ly("r2.7", "kafka-connect", "Order-sync is healthy, not Debezium.", true),
	ly("r2.7", "schema-registry", "Schema id 7 was hard-deleted, not in the registry.", true),
}

// corpusReviewScenarioRows are the review texts read with real scenario
// expects.
var corpusReviewScenarioRows = []corpusRow{
	// Round 1, item 3, and round 2, items 2, 3, 5 and 6: a wrong story after
	// the right cause.
	sc("r1.3", "consumer-offsets-out-of-range-retention", retentionCause+" Nothing older than 48 h can be recovered: reset the group to latest to get it going.", false),
	sc("r2.2", "consumer-offsets-out-of-range-retention", retentionCause+" We cannot confirm or rule out a reset to latest.", false),
	sc("r2.3", "consumer-offsets-out-of-range-retention", retentionCause+" The reset to latest meant order-sync could only read new records.", false),
	sc("r2.3", "consumer-offsets-out-of-range-retention", retentionCause+" A reset to latest is what I would bet on.", false),
	sc("r2.3", "topic-retention-data-loss", "Retention is 6 h, but the records were lost because the reset to latest meant order-sync could only read new records.", false),
	sc("r2.5", "consumer-offsets-out-of-range-retention", "Root cause: order-sync's committed offsets were deleted by retention. Nothing older than 48 h was consumed after someone reset the group to latest.", false),
	sc("r2.5", "consumer-offsets-out-of-range-retention", "Root cause: order-sync's committed offsets were deleted by retention. Someone reset the group to latest, which did nothing for the lag.", false),
	sc("r2.6", "consumer-offsets-out-of-range-retention", retentionCause+" We cannot, however, rule out a reset to latest.", false),
	// Round 2, item 1: correct causes with an adjective after "not".
	sc("r2.1", "schema-change", "Root cause: a DBA altered orders.amount from integer to numeric(12,2). Debezium's new value schema for amount is bytes, not backward compatible.", true),
	sc("r2.1", "schema-change", "Root cause: a DBA altered orders.amount from integer to numeric(12,2). The new schema is decimal bytes, not compatible.", true),
	sc("r2.1", "consumer-poison-pill", "Root cause: order-sync is stuck on partition 1 at offset 25377154, not deserializable.", true),
	sc("r2.1", "consumer-poison-pill", "Root cause: order-sync is stuck on partition 1; the record at offset 25377154 is there, not decodable.", true),
	// Round 2, item 4: a shotgun after a fix list.
	sc("r2.4", "schema-change", seedCause+"\nFix:\n1. Set compatibility to NONE and restart task 0.\nIt could also be a network problem between Connect and the registry.", false),
}

// heldoutWrongHypotheses are the wrong final hypotheses of
// TestHeldoutWrongAnswersFail for the held-out diagnose scenarios.
var heldoutWrongHypotheses = map[string]string{
	"payments-sink-dlq":                 "The risk_score DDL broke the Debezium source connector; Kafka Connect must be restarted.",
	"inventory-one-connector-slot-lost": "inventory-sync consumer is stuck on the shared inv prefix; restart pg-inventory-sites.",
	"users-compat-none-consumer":        "The loyalty_tier DDL in Postgres broke the consumer.",
}

// corpusScenarioRows gathers the scenario texts of the existing tests.
func corpusScenarioRows(t *testing.T) []corpusRow {
	var rows []corpusRow
	for id, h := range hypotheses {
		rows = append(rows, sc("scoring good", id, h.good, true), sc("scoring wrong", id, h.wrong, false))
	}
	for id, cases := range scenarioTermCases {
		for _, c := range cases {
			r := sc("terms "+c.id, id, c.text, c.want)
			switch {
			case c.preexisting:
				r = gap(r, "preexisting trap match (scenario_terms_test)")
			case c.matcherLimit:
				r = gap(r, "matcher limit (scenario_terms_test)")
			case c.knownGap:
				r = gap(r, "known gap, the judge decides (scenario_terms_test)")
			}
			rows = append(rows, r)
		}
	}
	for _, c := range f12Cases() {
		rows = append(rows, sc("f12 good", c.file, c.good, true))
		for name, text := range c.traps {
			rows = append(rows, sc("f12 trap "+name, c.file, text, false))
		}
	}
	for id, script := range heldoutScripts(t) {
		for _, s := range script {
			if s.hypothesis != "" {
				rows = append(rows, sc("heldout good", "heldout/"+id, s.hypothesis, true))
			}
		}
	}
	for id, text := range heldoutWrongHypotheses {
		rows = append(rows, sc("heldout wrong", "heldout/"+id, text, false))
	}
	rows = append(rows, sc("adversarial seed", "schema-change", seedCause, true), sc("adversarial seed", "schema-change", seedWrong, false))
	for name, text := range adversarialLists {
		rows = append(rows, sc("adversarial "+name, "schema-change", text, false))
	}
	return append(rows, corpusReviewScenarioRows...)
}

func (r corpusRow) eval(t *testing.T, scs map[string]Scenario) (bool, error) {
	switch r.kind {
	case "forbidden":
		return matchForbidden(r.terms, r.text), nil
	case "groups":
		return matchGroups(r.terms, r.text), nil
	case "hedge":
		_, h := causeHedge(r.text)
		return h, nil
	case "layer":
		return namesLayer(layerTermsFor(owners.Pipeline{}, r.layer), r.text), nil
	case "scenario":
		s, ok := scs[r.scenario]
		if !ok {
			return false, fmt.Errorf("unknown scenario %q", r.scenario)
		}
		return rootCauseMatches(s.Expect, r.text), nil
	}
	return false, fmt.Errorf("unknown kind %q", r.kind)
}

func TestGoldenCorpus(t *testing.T) {
	scs := map[string]Scenario{}
	for _, set := range []struct{ dir, prefix string }{{"scenarios", ""}, {heldoutDir, "heldout/"}} {
		list, err := LoadScenarios(set.dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range list {
			scs[set.prefix+s.Name] = s
		}
	}
	rows := append([]corpusRow(nil), corpusMatcherRows...)
	for _, r := range corpusScenarioRows(t) {
		// Root cause is graded only for diagnose (billing-connect-unreachable
		// abstains).
		if s, ok := scs[r.scenario]; ok && s.Expect.outcome() != OutcomeDiagnose {
			continue
		}
		rows = append(rows, r)
	}
	var failed, gaps int
	for _, r := range rows {
		got, err := r.eval(t, scs)
		if err != nil {
			t.Errorf("%s: %v", r.src, err)
			continue
		}
		if r.gap != "" {
			gaps++
			t.Logf("gap [%s] %s %s = %v (want %v): %s: %q", r.src, r.kind, r.scenario, got, r.want, r.gap, clip(r.text, 100))
			continue
		}
		if got != r.want {
			failed++
			t.Errorf("[%s] %s %s%s = %v, want %v: %q", r.src, r.kind, r.scenario, strings.Join(flatTerms(r.terms), ","), got, r.want, r.text)
		}
	}
	t.Logf("golden corpus: %d rows, %d failing, %d gap rows logged", len(rows), failed, gaps)
}

func flatTerms(groups [][]string) []string {
	var out []string
	for _, g := range groups {
		out = append(out, "["+strings.Join(g, " ")+"]")
	}
	return out
}
