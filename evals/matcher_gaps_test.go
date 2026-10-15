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
	}
	for _, tt := range tests {
		if got := namesLayer(layerTermsFor(owners.Pipeline{}, tt.layer), tt.text); got != tt.want {
			t.Errorf("%s: namesLayer(%q) = %v, want %v", tt.layer, tt.text, got, tt.want)
		}
	}
}
