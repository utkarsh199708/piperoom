package evals

import (
	"reflect"
	"testing"

	"piperoom/internal/owners"
)

// #334: a plural "s" after an acronym stays on it.
func TestTokenizeAcronymPlural(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"ACLs acls ACL", []string{"acls", "acls", "acl"}},
		{"the DBAs", []string{"the", "dbas"}},
		{"ACLsAnd", []string{"acls", "and"}},
		{"HTTPServer", []string{"http", "server"}},
		{"PostgreSQL", []string{"postgre", "sql"}},
		{"KafkaConnect", []string{"kafka", "connect"}},
		{"IDsLeft", []string{"ids", "left"}},
	}
	for _, tt := range tests {
		if got := tokenize(tt.in); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("tokenize(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
	if !matchGroups([][]string{{"acl*"}}, "The connector's ACLs were removed.") {
		t.Error(`acl* does not match "ACLs"`)
	}
}

// #283 and #334: forbidden stories inside a denial or a warning are not told;
// the same stories asserted still are.
func TestForbiddenDenialGaps(t *testing.T) {
	tests := []struct {
		name string
		row  []string
		text string
		want bool
	}{
		// #283: the "not" denies the crash, not the rule-out.
		{"negator of another verb before a rule-out", []string{"reset*", "latest"}, "Order-sync did not crash, I ruled out a reset to latest.", false},
		{"crash denied before a rule-out", []string{"order-sync", "crash*"}, "Order-sync did not crash, I ruled out a reset to latest.", false},
		{"negated rule-out still asserts", []string{"reset*", "latest"}, "I cannot rule out a reset to latest.", true},
		{"negated rule-out with adverb", []string{"reset*", "latest"}, "I can't yet rule out a reset to latest.", true},
		{"rule-out not yet done", []string{"reset*", "latest"}, "A reset to latest has not been ruled out.", true},
		// #334: "nothing".
		{"nothing is lost", []string{"slot", "lost"}, "The slot still exists, nothing is lost.", false},
		{"nothing changed", []string{"ddl", "chang*"}, "The DDL log is empty and nothing changed.", false},
		{"nothing incompatible", []string{"incompatible", "schema"}, "There is nothing incompatible in the schema.", false},
		{"nothing reaches only its own predicate", []string{"reset*", "latest"}, "Retention is fine and nothing was lost, so reset the group to latest.", true},
		{"lost slot told", []string{"slot", "lost"}, "The slot was lost when the DBA dropped it.", true},
		// #334: a conditional warning.
		{"would warns", []string{"reset*", "latest"}, "Resetting the whole group to latest would skip every partition's unread changes.", false},
		{"could warns", []string{"reset*", "latest"}, "A reset to latest could lose the unread changes.", false},
		{"could be the cause", []string{"reset*", "latest"}, "A reset to latest could be the cause.", true},
		{"could have", []string{"reset*", "latest"}, "A reset to latest could have skipped the records.", true},
		{"which would explain", []string{"reset*", "latest"}, "Someone ran a reset to latest, which would explain the gap.", true},
		{"would explain", []string{"reset*", "latest"}, "A reset to latest would explain the gap.", true},
		{"conditional is not a forward denial", []string{"reset*", "latest"}, "Would anyone notice? Someone reset the group to latest.", true},
		// Review round 1, item 2: a negated rule-out after would/could
		// still keeps the story open.
		{"could not be ruled out", []string{"reset*", "latest"}, "A reset to latest could not be ruled out.", true},
		{"could not be excluded", []string{"reset*", "latest"}, "A reset to latest could not be excluded.", true},
		// Item 3: "nothing" denies only its own predicate.
		{"nothing, colon, story", []string{"reset*", "latest"}, "Nothing older than 48 h can be recovered: reset the group to latest to get it going.", true},
		{"nothing, comma, story", []string{"reset*", "latest"}, "Nothing older than 48 h can be recovered, reset the group to latest to get it going.", true},
		{"nothing to do with, colon, story", []string{"reset*", "latest"}, "The lost records have nothing to do with retention: someone reset the group to latest.", true},
		{"nothing, and, story", []string{"reset*", "latest"}, "Nothing was lost and the group was reset to latest.", true},
		// Item 4: adverbs before be/explain, "cause", and a would/could
		// whose subject is not the story.
		{"could well be", []string{"reset*", "latest"}, "A reset to latest could well be the cause.", true},
		{"could still be", []string{"reset*", "latest"}, "A reset to latest could still be the cause.", true},
		{"would likely explain", []string{"reset*", "latest"}, "A reset to latest would likely explain the gap.", true},
		{"would probably explain", []string{"reset*", "latest"}, "A reset to latest would probably explain the gap.", true},
		{"could also explain", []string{"reset*", "latest"}, "A reset to latest could also explain the gap.", true},
		{"would cause", []string{"reset*", "latest"}, "A reset to latest would cause this exact gap.", true},
		{"past ability after a comma", []string{"reset*", "latest"}, "After the reset to latest, order-sync could only read new records.", true},
		{"and would", []string{"reset*", "latest"}, "The group was reset to latest and would skip every unread change.", true},
		{"and could", []string{"order-sync", "crash*"}, "Order-sync crashed on the record and could only be restarted by hand.", true},
		{"conditional warning with adverb", []string{"reset*", "latest"}, "Resetting the group to latest would probably skip unread changes.", false},
		// Item 5: hedges that keep a rule-out open.
		{"not able to rule out", []string{"reset*", "latest"}, "I was not able to rule out a reset to latest.", true},
		{"no way to rule out", []string{"reset*", "latest"}, "There is no way to rule out a reset to latest.", true},
		{"cannot conclusively rule out", []string{"reset*", "latest"}, "I cannot conclusively rule out a reset to latest.", true},
		{"can't really rule out", []string{"reset*", "latest"}, "We can't really rule out a reset to latest.", true},
		{"no one has ruled out", []string{"reset*", "latest"}, "No one has ruled out a reset to latest.", true},
		{"not crash and I ruled out", []string{"reset*", "latest"}, "Order-sync did not crash and I ruled out a reset to latest.", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchForbidden([][]string{tt.row}, tt.text); got != tt.want {
				t.Errorf("matchForbidden(%q, %q) = %v, want %v", tt.row, tt.text, got, tt.want)
			}
		})
	}
}

// #283: ", not X" contrasts a bare name with what came before it.
func TestContrastNot(t *testing.T) {
	tests := []struct {
		terms []string
		text  string
		want  bool
	}{
		{[]string{"debezium"}, "The slot was dropped by a DBA, not Debezium.", false},
		{[]string{"order-sync"}, "The registry rejected the schema, not order-sync.", false},
		{[]string{"registry"}, "The registry rejected the schema, not order-sync.", true},
		{[]string{"replication slot"}, "Task 0 failed, not able to read the replication slot.", true},
		{[]string{"debezium"}, "Debezium is not running.", true},
		{[]string{"debezium"}, "Not Debezium.", true}, // no comma before it
		// Review round 1, item 1: a failure described after a comma is no
		// contrast.
		{[]string{"amount*"}, "order-sync crashed, cannot deserialize amount.", true},
		{[]string{"avro"}, "Task 0 failed, cannot decode Avro.", true},
		{[]string{"deserializ*", "decod*"}, "Order-sync is stuck on partition 1, cannot decode the record.", true},
		{[]string{"publication*"}, "Task 0 failed, cannot read the publication.", true},
		{[]string{"commit*"}, "Order-sync is running, not committing offsets.", true},
		{[]string{"debezium"}, "The registry failed, not just Debezium.", true},
		{[]string{"debezium"}, "Two layers broke, not only Debezium but also the registry.", true},
		{[]string{"debezium"}, "The registry failed, not yet Debezium.", true},
	}
	for _, tt := range tests {
		if got := matchGroups([][]string{tt.terms}, tt.text); got != tt.want {
			t.Errorf("matchGroups(%q, %q) = %v, want %v", tt.terms, tt.text, got, tt.want)
		}
	}
}

// #283: alternatives after a "Fix:" label are fixes, not causes.
func TestHedgeIgnoresFixLine(t *testing.T) {
	tests := []struct {
		text string
		want bool
	}{
		{"Root cause: the connector task failed. Fix: restart the connector; alternatively, delete and recreate it.", false},
		{"Proposed fix: restart task 0, or alternatively resume the connector.", false},
		{"Fix: restart the connector. Root cause: either the slot or the registry.", true},
		{"Fix: restart the connector\nAlternatively the registry is at fault.", true},
		{"Root cause: alternatively the registry dropped the subject.", true},
		// Review round 1, item 6: the fix scope ends with its sentence.
		{"Root cause: the slot was dropped. Fix: recreate the slot. It could also be the registry.", true},
		{"Root cause: the slot was dropped. Fix: recreate the slot. Alternatively, the registry rejected the schema.", true},
		{"The slot was dropped; fix: recreate it. Or possibly the registry is at fault.", true},
		{"The slot was dropped; fix: recreate it; alternatively, restart Debezium.", false},
		// A combined label is no fix label; a fix list runs to a blank line.
		{"Root cause and fix: either the slot was dropped or the registry is down.", true},
		{"Root cause: the slot was dropped.\nProposed fix:\n1. Recreate the slot.\n2. Alternatively, restart Debezium.", false},
		{"Proposed fix:\n1. Recreate the slot.\n\nRoot cause: either the slot or the registry.", true},
	}
	for _, tt := range tests {
		if _, got := causeHedge(tt.text); got != tt.want {
			t.Errorf("causeHedge(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}

// #335: an SMT names Connect; SASL, SCRAM and the Kafka user name Kafka.
func TestLayerTermsSMTAndSASL(t *testing.T) {
	tests := []struct {
		layer, text string
		want        bool
	}{
		{"kafka-connect", "Root cause: one row's promised_at is not a date, the TimestampConverter SMT throws, errors.tolerance=none fails task 0.", true},
		{"kafka", "pg-payments-connector task 0 fails SASL authentication: the SCRAM password of the Kafka user debezium changed.", true},
		{"kafka", "Kafka rejects the SASL login.", true},
		{"kafka", "The Kafka user debezium lost its password.", true},
		{"kafka", "Kafka Connect restarted.", false},
		{"kafka", "Debezium is up, cannot reach the brokers.", true},
	}
	for _, tt := range tests {
		if got := namesLayer(layerTermsFor(owners.Pipeline{}, tt.layer), tt.text); got != tt.want {
			t.Errorf("%s: namesLayer(%q) = %v, want %v", tt.layer, tt.text, got, tt.want)
		}
	}
}
