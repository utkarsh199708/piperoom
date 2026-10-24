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
// stated), causeHedge (hg, want = hedged), namesLayer (ly) and shadow
// Agreement (ag, want = the labelled layer).
//
// A gap row is a case the matcher grades wrong today, left to the LLM judge
// (#454) or to the scenario terms (C7): it carries the right grade, is
// logged, and fails once it grades right, so the fix must remove its marker.

type corpusRow struct {
	src      string
	kind     string // scenario | correct | forbidden | groups | hedge | layer | agree
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

// ag is a shadow-agreement row: want is whether Agreement puts the text in
// the labelled layer.
func ag(src, layer string, causeTerms []string, text string, want bool) corpusRow {
	return corpusRow{src: src, kind: "agree", layer: layer, terms: [][]string{causeTerms}, text: text, want: want}
}

// co is a scenario row graded as a whole: want is whether the root cause
// matches and the root-cause layer is named (Correct in score.go).
func co(src, scenario, text string, want bool) corpusRow {
	return corpusRow{src: src, kind: "correct", scenario: scenario, text: text, want: want}
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

// poisonPillCause states consumer-poison-pill's root cause.
const poisonPillCause = "Root cause: a poison pill on partition 1 that order-sync cannot deserialize."

// resetLatest is the reset-to-latest wrong story.
var resetLatest = []string{"reset*", "latest"}

// corpusMatcherRows are the tickets' reproductions (#283, #334, #335) and
// every text quoted in review rounds 1-4 of #395.
var corpusMatcherRows = []corpusRow{
	// #283 and #334: forbidden stories inside a denial or a warning are not
	// told; the same stories asserted still are.
	// #283: the "not" denies the crash, not the rule-out.
	gap(fb("negator of another verb before a rule-out", []string{"reset*", "latest"}, "Order-sync did not crash, I ruled out a reset to latest.", false), "#283 rule-out governance, waits on #454"),
	gap(fb("crash denied before a rule-out", []string{"order-sync", "crash*"}, "Order-sync did not crash, I ruled out a reset to latest.", false), "#283 rule-out governance, waits on #454"),
	fb("negated rule-out still asserts", []string{"reset*", "latest"}, "I cannot rule out a reset to latest.", true),
	fb("negated rule-out with adverb", []string{"reset*", "latest"}, "I can't yet rule out a reset to latest.", true),
	fb("rule-out not yet done", []string{"reset*", "latest"}, "A reset to latest has not been ruled out.", true),
	// #334: "nothing".
	gap(fb("nothing is lost", []string{"slot", "lost"}, "The slot still exists, nothing is lost.", false), "#334 nothing, waits on #454"),
	gap(fb("nothing changed", []string{"ddl", "chang*"}, "The DDL log is empty and nothing changed.", false), "#334 nothing, waits on #454"),
	gap(fb("nothing incompatible", []string{"incompatible", "schema"}, "There is nothing incompatible in the schema.", false), "#334 nothing, waits on #454"),
	fb("nothing reaches only its own predicate", []string{"reset*", "latest"}, "Retention is fine and nothing was lost, so reset the group to latest.", true),
	fb("lost slot told", []string{"slot", "lost"}, "The slot was lost when the DBA dropped it.", true),
	// #334: a conditional warning.
	gap(fb("would warns", []string{"reset*", "latest"}, "Resetting the whole group to latest would skip every partition's unread changes.", false), "#334 conditional warning, waits on #454"),
	gap(fb("could warns", []string{"reset*", "latest"}, "A reset to latest could lose the unread changes.", false), "#334 conditional warning, waits on #454"),
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
	gap(fb("conditional warning with adverb", []string{"reset*", "latest"}, "Resetting the group to latest would probably skip unread changes.", false), "#334 conditional warning, waits on #454"),
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
	// Review round 3, item 1: only a harm verb after would/could makes a
	// warning; "would've" is "would have".
	fb("r3.1", []string{"reset*", "latest"}, "A reset to latest would've skipped every unread record.", true),
	// Item 2: "nothing ... except" tells the exception.
	fb("r3.2", []string{"order-sync", "crash*"}, "Nothing crashed except order-sync.", true),
	fb("r3.2", []string{"slot", "lost"}, "Nothing was lost except the slot.", true),
	// Nit 9: "nothing" before two auxiliaries or an adverb. The copula
	// exoneration ("the slot is intact") clears the slot (Q2g review, item 7).
	fb("r3.9", []string{"slot", "lost"}, "The slot is intact and nothing has been lost.", false),
	fb("r3.9", []string{"slot", "lost"}, "The slot is intact and nothing was ever lost.", false),
	fb("r3.11", []string{"slot", "drop*"}, "The slot is fine and nothing has been dropped.", false),
	// The exoneration reaches only the words right before its copula.
	fb("q2g.7", []string{"slot", "lost"}, "The slot was lost and Connect is fine.", true),
	fb("q2g.7", []string{"slot", "lost"}, "The slot was lost, Connect is fine.", true),
	fb("q2g.7", []string{"slot", "lost"}, "The slot is fine except it was lost.", true),
	fb("q2g.7", []string{"slot", "lost"}, "The lost slot is the cause and the slot theory is wrong.", true),
	// Nit 11: wrong on main and here alike; logged so the next change is
	// measured against them.
	gap(fb("r3.11", []string{"reset*", "latest"}, "I can neither confirm nor rule out a reset to latest.", true), "neither ... nor rule out"),
	gap(fb("r3.11", []string{"reset*", "latest"}, "Nobody has ruled out a reset to latest.", true), "nobody as the negator of a rule-out"),
	gap(fb("r3.11", []string{"reset*", "latest"}, "Resetting the group to latest here would skip every unread change.", false), "a word between the story and would"),
	gap(fb("r3.11", []string{"reset*", "latest"}, "Resetting the group to latest now would skip every unread change.", false), "a word between the story and would"),
	gap(fb("r3.11", []string{"reset*", "latest"}, "Resetting the group to latest would be unsafe.", false), "would be + adjective"),
	gap(fb("r3.11", []string{"ddl", "chang*"}, "Nothing in the DDL log changed the schema.", false), "nothing + prepositional phrase"),

	// #283: ", not X" contrasts a layer name with what came before it.
	gap(gr("contrast", [][]string{{"debezium"}}, "The slot was dropped by a DBA, not Debezium.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(gr("contrast", [][]string{{"order-sync"}}, "The registry rejected the schema, not order-sync.", false),
		"#283 \", not <pipeline name>\" contrast, waits on #454"),
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
	// Review round 3, item 4: a layer word that starts a longer phrase
	// describes the failure.
	gr("r3.4", [][]string{{"publication*"}}, "Root cause: the table owner changed, so Debezium's user has REPLICATION, not publication ownership.", true),

	// #283: alternatives after a "Fix:" label are fixes, not causes.
	gap(hg("fix label", "Root cause: the connector task failed. Fix: restart the connector; alternatively, delete and recreate it.", false), "#283 fix-list alternatives, waits on #454"),
	gap(hg("fix label", "Proposed fix: restart task 0, or alternatively resume the connector.", false), "#283 fix-list alternatives, waits on #454"),
	hg("fix label", "Fix: restart the connector. Root cause: either the slot or the registry.", true),
	hg("fix label", "Fix: restart the connector\nAlternatively the registry is at fault.", true),
	hg("fix label", "Root cause: alternatively the registry dropped the subject.", true),
	// Review round 1, item 6: the fix scope ends with its sentence.
	hg("fix label", "Root cause: the slot was dropped. Fix: recreate the slot. It could also be the registry.", true),
	hg("fix label", "Root cause: the slot was dropped. Fix: recreate the slot. Alternatively, the registry rejected the schema.", true),
	hg("fix label", "The slot was dropped; fix: recreate it. Or possibly the registry is at fault.", true),
	gap(hg("fix label", "The slot was dropped; fix: recreate it; alternatively, restart Debezium.", false), "#283 fix-list alternatives, waits on #454"),
	// A combined label is no fix label; a fix list runs to a blank line.
	hg("fix label", "Root cause and fix: either the slot was dropped or the registry is down.", true),
	gap(hg("fix label", "Root cause: the slot was dropped.\nProposed fix:\n1. Recreate the slot.\n2. Alternatively, restart Debezium.", false), "#283 fix-list alternatives, waits on #454"),
	hg("fix label", "Proposed fix:\n1. Recreate the slot.\n\nRoot cause: either the slot or the registry.", true),
	// Review round 2, item 4: only list items under a label that ends its
	// line belong to the fix.
	hg("r2.4", "Root cause: the slot was dropped.\nProposed fix:\n1. Recreate the slot.\nIt could also be the registry.", true),
	hg("r2.4", "Root cause: the slot was dropped.\nFix:\n- recreate the slot\nAlternatively, the registry could be at fault.", true),
	hg("r2.4", "Root cause: the slot was dropped.\nProposed fix:\n1. Recreate the slot.\nAlternatively the registry rejected the schema.", true),
	hg("r2.4", "Root cause: the slot was dropped by a DBA.\nFix:\nRecreate the slot and restart the connector. The registry could also be at fault.", true),
	// Review round 3, item 6: inside a fix scope only fix alternatives are
	// skipped; a cause hedge still counts.
	hg("r3.6", "Root cause: the slot was dropped. Fix: recreate the slot; the cause could also be the registry.", true),
	// Nit 10: an ellipsis ends the fix sentence.
	hg("r3.10", "Root cause: the slot was dropped. Fix: recreate it… alternatively the registry is down.", true),
	// Nit 11: fix labels the rule does not see (wrong on main too).
	gap(hg("r3.11", "Root cause: the slot was dropped.\n- Fix: recreate it; alternatively, restart Debezium.", false), "list-item fix label"),
	gap(hg("r3.11", "Root cause: the slot was dropped. **Fix:** recreate it; alternatively, restart Debezium.", false), "markdown fix label"),
	gap(hg("r3.11", "Root cause: the slot was dropped. Suggested fix: recreate it; alternatively, restart Debezium.", false), "suggested fix label"),
	gap(hg("r3.11", "Root cause: the slot was dropped.\nProposed fix:\n\n1. Recreate the slot.\n2. Alternatively, restart Debezium.", false), "blank line after the fix label"),
	gap(hg("r3.11", "Root cause: the slot was dropped. Fix: recreate a slot, e.g. debezium_orders; alternatively, restart Debezium.", false), "e.g. inside a fix sentence"),

	// #335: an SMT names Connect; SASL, SCRAM and the Kafka user name Kafka.
	gap(ly("#335", "kafka-connect", "Root cause: one row's promised_at is not a date, the TimestampConverter SMT throws, errors.tolerance=none fails task 0.", true), "#335, split out, via #454"),
	gap(ly("#335", "kafka", "pg-payments-connector task 0 fails SASL authentication: the SCRAM password of the Kafka user debezium changed.", true), "#335, split out, via #454"),
	gap(ly("#335", "kafka", "Kafka rejects the SASL login.", true), "#335, split out, via #454"),
	gap(ly("#335", "kafka", "The Kafka user debezium lost its password.", true), "#335, split out, via #454"),
	ly("#335", "kafka", "Kafka Connect restarted.", false),
	ly("#335", "kafka", "Debezium is up, cannot reach the brokers.", true),
	// Review round 2, nit 7.
	ly("r2.7", "kafka-connect", "Order-sync is healthy, not Debezium.", true),
	ly("r2.7", "schema-registry", "Schema id 7 was hard-deleted, not in the registry.", true),
	// Review round 3, item 5: the usual names of the layers.
	gap(ly("r3.5", "kafka-connect", "Root cause: an ALTER TABLE in Postgres changed amount to numeric; the registry rejected it, not Kafka Connect.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(ly("r3.5", "kafka-connect", "Root cause: an ALTER TABLE in Postgres changed amount to numeric; the registry rejected it, not Connect.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(ly("r3.5", "schema-registry", "The connector failed, not Confluent Schema Registry.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	// Review round 4 (#395): pinned at main's grade. Item 1: would/could + a harm verb
	// that recommends or tells the story.
	fb("r4.1", resetLatest, "Resetting the group to latest would skip the poison pill.", true),
	fb("r4.1", resetLatest, "Resetting the group to latest would skip it.", true),
	fb("r4.1", resetLatest, "Resetting to latest could drop the stuck record and let order-sync continue.", true),
	fb("r4.1", resetLatest, "A reset to latest would quickly skip past the poison pill, so do it.", true),
	fb("r4.1", resetLatest, "Resetting the group to latest would delete nothing and would skip the bad record.", true),
	fb("r4.1", resetLatest, "The gap matches what a reset to latest would drop.", true),
	fb("r4.1", resetLatest, "The missing range is exactly what a reset to latest would skip.", true),
	fb("r4.1", resetLatest, "Resetting the group to latest would skip the poison pill and unblock order-sync.", true),
	fb("r4.1", resetLatest, "A reset to latest would skip the bad record, so I recommend it.", true),
	fb("r4.1", resetLatest, "Resetting order-sync to latest would drop the poison pill; that is the fastest fix.", true),
	fb("r4.1", resetLatest, "Resetting the group to latest would lose nothing, because the warehouse can be backfilled.", true),
	fb("r4.1", resetLatest, "A reset to latest would miss only one record, which is acceptable.", true),
	fb("r4.1", resetLatest, "Resetting the group to latest would discard the malformed record and let order-sync move on.", true),
	fb("r4.1", resetLatest, "A reset to latest would skip past offset 25377154, which is what we want.", true),
	fb("r4.1", resetLatest, "Kabir said a reset to latest would skip the backlog, and he ran it at 09:12.", true),
	fb("r4.1", resetLatest, "My recommendation: resetting the group to latest would drop the stuck record.", true),
	fb("r4.1", resetLatest, "Fix: resetting the group to latest would skip the junk record.", true),
	fb("r4.1", resetLatest, "A reset to latest would quickly skip the bad record and restore flow.", true),
	fb("r4.1", resetLatest, "Resetting the group to latest would also drop the poison pill.", true),
	fb("r4.1", resetLatest, "A reset to latest could skip the bad record, so let's do that.", true),
	fb("r4.1", resetLatest, "A reset to latest could lose some changes, but it is the only way to unblock order-sync, so do it.", true),
	fb("r4.1", resetLatest, "Resetting the group to latest would break the crash loop.", true),
	fb("r4.1", resetLatest, "A reset to latest would hide the lag, which is exactly what someone did at 09:12.", true),
	fb("r4.1", resetLatest, "The safest fix is that a reset to latest would skip only partition 1's bad record.", true),
	fb("r4.1", resetLatest, "Order-sync lost records since a reset to latest would skip them.", true),
	fb("r4.1", resetLatest, "While a reset to latest would skip every unread record, it is the only fix.", true),
	fb("r4.1", resetLatest, "Since a reset to latest would skip the bad record, do it now.", true),
	fb("r4.1", resetLatest, "When a reset to latest would skip only one record, it is safe; do it.", true),
	fb("r4.1", resetLatest, "Then a reset to latest would skip the bad record and unblock order-sync.", true),
	fb("r4.1", resetLatest, "Each time order-sync restarted, the reset to latest would skip the backlog.", true),
	fb("r4.1", resetLatest, "Order-sync was stuck until a reset to latest would skip the record.", true),
	fb("r4.1", []string{"poison pill"}, "Order-sync is stuck because a poison pill would break it on every restart.", true),
	fb("r4.1", []string{"retention", "delet*"}, "The consumer lags because retention would delete records before it reads them.", true),
	// Item 2: -ly verbs.
	fb("r4.2", resetLatest, "A reset to latest would imply skipped offsets, which matches the gap.", true),
	fb("r4.2", resetLatest, "A reset to latest could imply lost records, and that is what we see.", true),
	// Item 3: "nothing" with an exception after a comma, or as an object.
	fb("r4.3", []string{"order-sync", "crash*"}, "Nothing crashed, except order-sync.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "Nothing crashed, besides order-sync.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "Nothing crashed, other than order-sync.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "Nothing crashed, with the exception of order-sync.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "Nothing crashed with the exception of order-sync.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "Nothing crashed: only order-sync did.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "Nothing crashed, only order-sync.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "Nothing crashed, but order-sync did.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "Restarting did nothing, order-sync crashed again.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "The restart changed nothing: order-sync still crashed on the same record.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "Nothing, then order-sync crashed.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "At first nothing, then order-sync crashed on the record.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "Then nothing, order-sync crashed.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "There was nothing order-sync could do once it crashed.", true),
	fb("r4.3", []string{"order-sync", "crash*"}, "Nothing crashed until order-sync did.", true),
	fb("r4.3", []string{"consumer", "crash*"}, "Nothing crashed, except the consumer.", true),
	fb("r4.3", []string{"ddl", "chang*"}, "Nothing changed, except the DDL on orders.", true),
	fb("r4.3", resetLatest, "Retention lost nothing, resetting to latest did.", true),
	fb("r4.3", resetLatest, "Retention deleted nothing: resetting to latest skipped the records.", true),
	fb("r4.3", resetLatest, "We can do nothing: reset the group to latest.", true),
	fb("r4.3", resetLatest, "Order-sync read nothing, resetting to latest skipped it all.", true),
	fb("r4.3", resetLatest, "Nothing, reset to latest is the only way out.", true),
	// Item 4: an epistemic or saying negation before a rule-out.
	fb("r4.4", resetLatest, "Not sure we ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "I'm not sure I ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "Can't say I ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "I'm not convinced we ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "Not certain we ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "Hard to say; not clear we ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "I don't think it rules out a reset to latest.", true),
	fb("r4.4", resetLatest, "That does not mean we ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "This doesn't mean I ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "Not that we ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "We can't say we ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "I can't say I ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "A reset to latest: we can't call it ruled out.", true),
	fb("r4.4", resetLatest, "The logs do not show we ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "I don't think we ruled out a reset to latest.", true),
	fb("r4.4", resetLatest, "I'm not saying we ruled out a reset to latest.", true),
	// Review round 4, item 5: "not Y" after a state or a recovery names Y
	// as the failing layer.
	ly("r4.5", "kafka-connect", "Order-sync is healthy and running, not Debezium.", true),
	ly("r4.5", "kafka-connect", "Postgres is up, not Debezium.", true),
	ly("r4.5", "kafka-connect", "The registry is reachable, not Connect.", true),
	ly("r4.5", "kafka-connect", "The brokers are OK, not Debezium.", true),
	ly("r4.5", "kafka-connect", "Order-sync is caught up, not Debezium.", true),
	ly("r4.5", "kafka-connect", "The slot is active, not Debezium.", true),
	ly("r4.5", "kafka-connect", "Postgres and the registry are both up, not Connect.", true),
	ly("r4.5", "kafka-connect", "Postgres is healthy, the registry is up, not Connect.", true),
	ly("r4.5", "kafka-connect", "Only the registry is up, not Debezium.", true),
	ly("r4.5", "kafka-connect", "The registry recovered, not Debezium.", true),
	ly("r4.5", "kafka-connect", "The registry came back, not Debezium.", true),
	ly("r4.5", "kafka-connect", "Postgres restarted cleanly, not Debezium.", true),
	ly("r4.5", "kafka-connect", "The consumer resumed, not Connect.", true),
	ly("r4.5", "kafka-connect", "Order-sync works, not Debezium.", true),
	ly("r4.5", "kafka-connect", "Postgres is up, not Kafka Connect.", true),
	ly("r4.5", "kafka-connect", "The registry is reachable, not Kafka Connect.", true),
	ly("r4.5", "kafka-connect", "Postgres and Kafka are up, not Kafka Connect.", true),
	ly("r4.5", "kafka-connect", "The registry came back, not Kafka Connect.", true),
	ly("r4.5", "kafka-connect", "Kafka recovered, not Kafka Connect.", true),
	ly("r4.5", "kafka-connect", "Order-sync is caught up, not Kafka Connect.", true),
	ly("r4.5", "kafka-connect", "Only Postgres is up, not Kafka Connect.", true),
	ly("r4.5", "kafka-connect", "Kafka is up and serving, not Kafka Connect.", true),
	ly("r4.5", "kafka-connect", "Kafka works, not Kafka Connect.", true),
	ly("r4.5", "kafka-connect", "Postgres is up, not Connect.", true),
	ly("r4.5", "kafka-connect", "Kafka brokers are up, not Kafka Connect.", true),
	ly("r4.5", "kafka-connect", "The schema registry is answering, not Kafka Connect.", true),
	ly("r4.5", "schema-registry", "Kafka is reachable, not Schema Registry.", true),
	ly("r4.5", "schema-registry", "Kafka Connect is up, not Confluent Schema Registry.", true),
	ly("r4.5", "kafka", "Connect is green, not Kafka brokers.", true),
	ly("r4.5", "kafka", "Debezium is running, not Kafka brokers.", true),
	ly("r4.5", "postgres", "Debezium is running, not Postgres.", true),
	// Item 6: "not X alone" asserts X.
	ly("r4.6", "kafka-connect", "The fault spans the registry, not Debezium alone.", true),
	ly("r4.6", "kafka-connect", "Several layers broke, not Kafka Connect alone.", true),
	ly("r4.6", "kafka-connect", "The registry broke too, not Debezium alone.", true),
	ly("r4.6", "kafka-connect", "It is the registry as well, not Connect alone.", true),
	ly("r4.6", "kafka-connect", "The registry rejected it, not Kafka Connect alone.", true),
	gr("r4.6", [][]string{{"debezium"}}, "Two layers failed, not Debezium alone.", true),
	gr("r4.6", [][]string{{"debezium"}}, "Several components broke, not Debezium alone.", true),
	gr("r4.6", [][]string{{"owner*"}}, "Debezium's user has REPLICATION, not table ownership.", true),
	gr("r4.6", [][]string{{"publication*"}}, "Root cause: the owner changed; Debezium owns the slot, not publications.", true),
	// Item 7: a cause alternative inside a fix scope.
	hg("r4.7", "Root cause: the slot was dropped. Fix: recreate the slot; alternatively the registry is at fault and needs a restart.", true),
	hg("r4.7", "Root cause: the slot was dropped. Fix: restart task 0; either the registry or the slot is the cause.", true),
	hg("r4.7", "Root cause: the slot was dropped. Fix: recreate the slot, or alternatively the root cause is the registry.", true),
	hg("r4.7", "Fix: restart task 0 - either the registry or the slot caused it.", true),
	hg("r4.7", "Fix: restart task 0, since either the registry or the slot dropped.", true),
	hg("r4.7", "Root cause: the slot was dropped; Fix: recreate it; alternatively, the cause is a registry outage.", true),
	hg("r4.7", "Fix: recreate the slot; alternatively it was the registry all along.", true),
	hg("r4.7", "Root cause: the slot was dropped. Fix: recreate the slot; the root cause is either the DBA or a failover.", true),
	hg("r4.7", "Root cause: the slot was dropped. Fix: restart task 0; alternatively the registry itself is at fault.", true),
	hg("r4.7", "Fix: restart the connector, since either the slot or the registry broke.", true),
	hg("r4.7", "Proposed fix: recreate the slot, because either a DBA dropped it or a failover lost it.", true),
	hg("r4.7", "Root cause: the slot was dropped. Fix: set compatibility to NONE; alternatively, the cause could be a network problem.", true),
	hg("r4.7", "Root cause: the slot was dropped.\nFix:\nRestart the connector; either the slot or the registry is at fault.\n", true),
	hg("r4.7", "Root cause: the slot was dropped.\nFix:\n- recreate the slot\n- either the DBA or a failover dropped it, check both", true),
	hg("r4.7", "Root cause: the slot was dropped; fix: recreate it; either a DBA or a failover is to blame.", true),
	hg("r4.7", "Root cause: the slot was dropped. Fixes: recreate the slot; alternatively the WAL was recycled and the slot never existed.", true),
	hg("r4.7", "Root cause: the slot was dropped. Fix: recreate it (alternatively a failover lost it, so check the standby).", true),
	hg("r4.7", "Root cause: the slot was dropped. Fix: recreate the slot – either a DBA or a failover removed it.", true),
	hg("r4.7", "Fix: restart the connector; the cause may be either the slot or the registry.", true),
	// Round 4, item 8: graded right only by the rules this PR no longer
	// carries; main grades them wrong.
	gap(fb("r4.8", resetLatest, "Order-sync did not crash, which rules out a reset to latest.", false), "#283 rule-out governance, waits on #454"),
	gap(fb("r4.8", resetLatest, "The offsets were not touched, ruling out a reset to latest.", false), "#283 rule-out governance, waits on #454"),
	gap(fb("r4.8", resetLatest, "The committed offsets were not touched, ruling out a reset to latest.", false), "#283 rule-out governance, waits on #454"),
	gap(ly("r4.8", "schema-registry", "The connector failed, not Schema Registry.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	// Nit 11: graded wrong by main and by every round of this PR.
	gap(ly("r4.11", "kafka-connect", "The registry rejected it, not Apache Kafka Connect.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(ly("r4.11", "kafka-connect", "The registry rejected it, not Confluent Kafka Connect.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(ly("r4.11", "schema-registry", "The connector failed, not Confluent's Schema Registry.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(ly("r4.11", "schema-registry", "The connector failed, not Confluent Schema Registry itself.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(ly("r4.11", "kafka-connect", "The registry rejected it, not Connect or Debezium.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(ly("r4.11", "kafka-connect", "The registry rejected it, not Kafka Connect or Debezium.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(ly("r4.11", "kafka-connect", "The registry rejected it, not Kafka Connect's fault.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(ly("r4.11", "schema-registry", "The connector failed, and not Confluent Schema Registry.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(ly("r4.11", "kafka-connect", "Not Kafka Connect: the registry rejected the schema.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(ly("r4.11", "kafka-connect", "Order-sync and not Debezium crashed.", false), "#283 \", not <layer>\" contrast, waits on #454"),
	gap(fb("r4.11", resetLatest, "We cannot confirm, nor rule out, a reset to latest.", true), "#283 rule-out governance, waits on #454"),
	gap(fb("r4.11", resetLatest, "I'm not sure we've ruled out a reset to latest.", true), "#283 rule-out governance, waits on #454"),
	gap(fb("r4.11", []string{"slot", "lost"}, "Nothing of the slot was lost.", false), "#334 nothing, waits on #454"),
}

// saslPostgresGap marks the r6.1 Postgres-login rows (W4 ruling R19, a lead grant to C7's PR).
const saslPostgresGap = "W4 ruling R19: the root-cause layer is now kafka-connect, which these texts name, so the term matcher cannot tell a Postgres login from the Kafka one; the judge decides"

// saslCause are sasl-credential-rotated's cause words for agreement rows.
var saslCause = []string{"sasl", "credential", "rotated", "password"}

// corpusAgreementRows pin shadow agreement (agreement.go) on the texts review
// round 5 found it reads through layerTerms: correct SASL-first answers to
// sasl-credential-rotated, whose first failing layer is kafka-connect.
var corpusAgreementRows = []corpusRow{
	ag("r5.1", "kafka-connect", saslCause, "SASL authentication fails for pg-payments-connector task 0: the SCRAM password of the Kafka user debezium was rotated.", true),
	ag("r5.1", "kafka-connect", saslCause, "SASL login failed: the security team rotated the debezium password and pg-payments-connector task 0 is FAILED.", true),
	ag("r5.1", "kafka-connect", saslCause, "Authentication failed with SASL mechanism SCRAM-SHA-512 for task 0 of pg-payments-connector after the password rotation.", true),
	ag("r5.1 control", "kafka-connect", saslCause, "pg-payments-connector task 0 is FAILED: SASL authentication fails because the debezium password was rotated.", true),
	ag("r5.1 control", "kafka-connect", saslCause, "The Debezium connector cannot log in: SCRAM credentials for the kafka user were rotated.", true),
	ag("r5.1 control", "kafka-connect", saslCause, "Connector task failed after the SMT threw on a tombstone.", true),
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
	// Round 3, item 1: a recommendation or explanation with would/could.
	sc("r3.1", "consumer-offsets-out-of-range-retention", retentionCause+" A reset to latest could've caused this gap too.", false),
	sc("r3.1", "consumer-offsets-out-of-range-retention", retentionCause+" A reset to latest would fit the gap as well.", false),
	sc("r3.1", "consumer-offsets-out-of-range-retention", retentionCause+" Resetting the group to latest would fix the lag.", false),
	sc("r3.1", "consumer-poison-pill", "Root cause: a poison pill on partition 1 that order-sync cannot deserialize. Resetting the group to latest would clear it.", false),
	sc("r3.1", "topic-retention-data-loss", "Retention is 6 h and records were lost: a reset to latest would've skipped them.", false),
	// Item 2: "nothing ... except" on the demo scenario.
	sc("r3.2", "schema-change", seedCause+" Nothing crashed except order-sync.", false),
	sc("r3.2", "schema-change", seedCause+" Nothing crashed but the order-sync consumer.", false),
	sc("r3.2", "schema-change", seedCause+" Nothing was lost except the replication slot.", false),
	// Item 3: negated rule-outs with an adverb, an aside or ", or".
	sc("r3.3", "consumer-offsets-out-of-range-retention", retentionCause+" We can't yet, however, rule out a reset to latest.", false),
	sc("r3.3", "consumer-offsets-out-of-range-retention", retentionCause+" We cannot fully, though, rule out a reset to latest.", false),
	sc("r3.3", "consumer-offsets-out-of-range-retention", retentionCause+" We cannot confirm, or rule out, a reset to latest.", false),
	sc("r3.3", "consumer-offsets-out-of-range-retention", retentionCause+" We can't, I think, rule out a reset to latest.", false),
	sc("r3.3", "consumer-offsets-out-of-range-retention", retentionCause+" We cannot, it seems, rule out a reset to latest.", false),
	// Item 4: a correct cause with "not publication ownership".
	sc("r3.4", "publication-permission-denied", "Root cause: the table owner changed, so Debezium's user has REPLICATION, not publication ownership.", true),
	// Item 6: cause hedges inside an inline fix scope.
	sc("r3.6", "schema-change", seedCause+" Fix: set compatibility to NONE; the root cause could also be a network problem between Connect and the registry.", false),
	sc("r3.6", "schema-change", seedCause+" Fix: set compatibility to NONE; I am not sure whether order-sync also crashed.", false),
	// Round 4 (#395), items 1-4 and 6-7: real expects, pinned at main's
	// grade.
	sc("r4", "consumer-poison-pill", poisonPillCause+" Resetting the group to latest would skip the bad record and unblock order-sync.", false),
	sc("r4", "consumer-poison-pill", poisonPillCause+" A reset to latest would skip it.", false),
	sc("r4", "consumer-poison-pill", poisonPillCause+" Resetting the group to latest would skip the poison pill.", false),
	sc("r4", "consumer-poison-pill", poisonPillCause+" Resetting the group to latest would skip the poison pill and unblock order-sync.", false),
	sc("r4", "consumer-poison-pill", poisonPillCause+" Fix: a reset to latest would skip the bad record.", false),
	sc("r4", "consumer-poison-pill", poisonPillCause+" Resetting order-sync to latest would drop just the poison pill.", false),
	sc("r4", "consumer-poison-pill", poisonPillCause+" A reset to latest could skip the junk record and get order-sync going again.", false),
	sc("r4", "consumer-poison-pill", poisonPillCause+" Since a reset to latest would skip the bad record, do it now.", false),
	sc("r4", "consumer-poison-pill", poisonPillCause+" While a reset to latest would skip every unread record, it is the only fix.", false),
	sc("r4", "consumer-poison-pill", poisonPillCause+" Fix: move partition 1 past the record; alternatively the consumer has a deserializer bug.", false),
	sc("r4", "topic-retention-data-loss", "Retention is 6 h and records expired before order-sync read them, so they were lost. Resetting the group to latest would skip the gap and get order-sync going.", false),
	sc("r4", "topic-retention-data-loss", "Retention is 6 h and records were lost. Kabir said a reset to latest would skip the backlog, and he ran it at 09:12.", false),
	sc("r4", "topic-retention-data-loss", "Records were lost. Retention deleted nothing: resetting the group to latest skipped them.", false),
	sc("r4", "topic-retention-data-loss", "Retention is 6 h and records were lost. I'm not sure I ruled out a reset to latest.", false),
	sc("r4", "topic-retention-data-loss", "Retention is 6 h and records were lost. We can't say we ruled out a reset to latest.", false),
	sc("r4", "topic-retention-data-loss", "Retention is 6 h and records were lost. Fix: raise retention; alternatively the consumer skipped them on its own.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" Resetting the group to latest would skip the missing range and restart consumption.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" The missing range is exactly what a reset to latest would skip.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" Resetting the group to latest would lose nothing more, so do it now.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" A reset to latest would skip exactly the missing records, which is what happened.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" A reset to latest would imply lost records too.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" Order-sync read nothing, resetting to latest skipped it all.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" Not sure we ruled out a reset to latest.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" I'm not sure I ruled out a reset to latest.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" Can't say I ruled out a reset to latest.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" I'm not convinced we ruled out a reset to latest.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" I don't think it rules out a reset to latest.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" That does not mean we ruled out a reset to latest.", false),
	sc("r4", "consumer-offsets-out-of-range-retention", retentionCause+" Not that we ruled out a reset to latest.", false),
	sc("r4", "schema-change", seedCause+" Order-sync also stopped, since a poison pill would break it on every restart.", false),
	sc("r4", "schema-change", seedCause+" Nothing crashed, except order-sync.", false),
	sc("r4", "schema-change", seedCause+" A restart did nothing, order-sync crashed again.", false),
	sc("r4", "schema-change", seedCause+" Nothing crashed, but the order-sync consumer did.", false),
	sc("r4", "schema-change", seedCause+" The restart fixed nothing: order-sync crashed again.", false),
	sc("r4", "schema-change", seedCause+" There was nothing order-sync could do once it crashed.", false),
	sc("r4", "schema-change", seedCause+" Fix: set compatibility to NONE; alternatively, the cause is a broker outage, so restart the brokers.", false),
	sc("r4", "schema-change", seedCause+" Fix: set compatibility to NONE; either the registry or a broker outage is to blame.", false),
	sc("r4", "schema-change", seedCause+" Fix: set compatibility to NONE, or alternatively it is a network problem between Connect and the registry.", false),
	sc("r4", "schema-change", seedCause+" Fix: set compatibility to NONE; alternatively the consumer is the root cause.", false),
	sc("r4", "schema-change", seedCause+" Fix: set compatibility to NONE, since either the registry or a network problem broke the connector.", false),
	sc("r4", "schema-change", seedCause+" Fix: set compatibility to NONE; alternatively, a broker may be the cause.", false),
	sc("r4", "schema-change", seedCause+" Fix: set compatibility to NONE and restart task 0, because either the ALTER TABLE or a network problem stopped it.", false),
	sc("r4", "schema-change", seedCause+" Fix: set compatibility to NONE (either the registry or the broker is to blame).", false),
	sc("r4", "schema-change", seedCause+" Proposed fix: set compatibility to NONE; alternatively, order-sync has a bug that rejects decimals.", false),
	sc("r4", "watcher-start", watcherCause+" Nothing changed the schema, except the DDL at 09:10.", false),
	sc("r4", "publication-permission-denied", "Root cause: the table owner changed; Debezium's user can read the slot, not publications.", true),
	sc("r4", "publication-permission-denied", "Root cause: the table owner changed, so Debezium's role owns tables, not publications.", true),
	sc("r4", "publication-permission-denied", "Root cause: the debezium role has REPLICATION, not table owner, so it cannot alter the publication.", true),
	sc("r4", "publication-permission-denied", "Root cause: Debezium's role has REPLICATION, not table ownership, so it cannot alter the publication.", true),
	sc("r4", "publication-permission-denied", "Root cause: the orders table owner changed; the debezium role still owns its slot, not publications.", true),
	// Round 6: a Postgres SCRAM/SASL login is sasl-credential-rotated's trap
	// ("looks like a Postgres problem"): its cause words match, and it did
	// not name the Kafka layer, so the answer was not correct. Since R19 the
	// root-cause layer is kafka-connect (kafka accepted), so these are
	// known gaps of the term matcher and the judge decides them.
	gap(co("r6.1", "sasl-credential-rotated", "Root cause: the SCRAM password of the Postgres replication user debezium was rotated, so Debezium cannot log in to the source database and pg-payments-connector task 0 failed.", false), saslPostgresGap),
	gap(co("r6.1", "sasl-credential-rotated", "Root cause: Postgres rejects Debezium's SCRAM-SHA-256 login: the password of the database user debezium was rotated.", false), saslPostgresGap),
	gap(co("r6.1", "sasl-credential-rotated", "Root cause: the source database's pg_hba.conf now requires scram-sha-256 and the debezium password was rotated, so the connector cannot authenticate.", false), saslPostgresGap),
	gap(co("r6.1", "sasl-credential-rotated", "Root cause: SASL authentication to Postgres fails because the debezium database password was rotated.", false), saslPostgresGap),
	gap(co("r6.1", "sasl-credential-rotated", "Root cause: the replication slot is inactive because Postgres refuses the debezium login: its SCRAM secret was rotated by the DBA.", false), saslPostgresGap),
	co("r6.1 control", "sasl-credential-rotated", "Root cause: the SCRAM password of the Kafka user debezium was rotated, so the broker rejects the connector's SASL login.", true),
	// Round 4, nit 10: round 3 item 3 on topic-retention-data-loss.
	sc("r4.10", "topic-retention-data-loss", "Retention is 6 h and records were lost. "+"We can't yet, however, rule out a reset to latest.", false),
	sc("r4.10", "topic-retention-data-loss", "Retention is 6 h and records were lost. "+"We cannot fully, though, rule out a reset to latest.", false),
	sc("r4.10", "topic-retention-data-loss", "Retention is 6 h and records were lost. "+"We cannot confirm, or rule out, a reset to latest.", false),
	sc("r4.10", "topic-retention-data-loss", "Retention is 6 h and records were lost. "+"We can't, I think, rule out a reset to latest.", false),
	sc("r4.10", "topic-retention-data-loss", "Retention is 6 h and records were lost. "+"We cannot, it seems, rule out a reset to latest.", false),
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
				r.want = true // a correct text the terms cannot accept
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
	case "agree":
		res, err := Agreement(r.text, Label{Layer: r.layer, RootCauseTerms: r.terms[0]})
		return res.LayerAgree, err
	case "correct":
		s, ok := scs[r.scenario]
		if !ok {
			return false, fmt.Errorf("unknown scenario %q", r.scenario)
		}
		return rootCauseMatches(s.Expect, r.text) && keywordRootCauseLayer(s, r.text), nil
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
	rows := append(append([]corpusRow(nil), corpusMatcherRows...), corpusAgreementRows...)
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
			if got == r.want {
				t.Errorf("gap row graded right now; remove its gap marker: [%s] %s %s: %q", r.src, r.kind, r.scenario, r.text)
			}
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
