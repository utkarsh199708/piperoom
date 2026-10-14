package runbooks

import (
	"slices"
	"testing"
)

func TestTerms(t *testing.T) {
	cases := []struct {
		name, in string
		want     []string
	}{
		{"java class", "org.apache.kafka.connect.errors.DataException",
			[]string{"org.apache.kafka.connect.errors.dataexception", "org", "apach", "kafka", "connect", "error", "dataexception", "data", "exception"}},
		{"pascal case", "RecordDeserializationException", []string{"recorddeserializationexception", "record", "deserializ", "exception"}},
		{"acronym", "SQLState", []string{"sqlstate", "sql", "stat"}},
		{"camel case", "restartTask", []string{"restarttask", "restart", "task"}},
		{"snake case", "restart_lsn", []string{"restart_lsn", "restart", "lsn"}},
		{"config key", "max.poll.interval.ms", []string{"max.poll.interval.ms", "max", "poll", "interval", "ms"}},
		{"schema registry code", "error code 40403", []string{"error", "code", "40403"}},
		{"sqlstate value", "SQLSTATE 22P02, 57P01", []string{"sqlstat", "22p02", "57p01"}},
		{"http code in text", "error code: 409", []string{"error", "code", "409"}},
		{"lsn", "restart_lsn 0/16B3748 confirmed 1A/2B000028", []string{"restart_lsn", "restart", "lsn", "0/16b3748", "confirm", "1a/2b000028"}},
		{"topic partition offset", "pg.public.orders/2@799", []string{"pg.public.orders", "pg", "public", "order", "799"}},
		{"stopwords and short", "Why is the a lag", []string{"lag"}},
		{"stemming", "Rebalancing rebalanced REBALANCES rebalance", []string{"rebalanc", "rebalanc", "rebalanc", "rebalanc"}},
		{"upper case enum", "MISSING_UNION_BRANCH", []string{"missing_union_branch", "miss", "union", "branch"}},
		{"punctuation", `"(pod) restarted;"`, []string{"pod", "restart"}},
		{"empty", "  ,;  ", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := terms(c.in); !slices.Equal(got, c.want) {
				t.Errorf("terms(%q)\n got %q\nwant %q", c.in, got, c.want)
			}
		})
	}
}

func TestStem(t *testing.T) {
	groups := [][]string{
		{"rebalance", "rebalancing", "rebalanced", "rebalances"},
		{"lag", "lagging", "lagged", "lags"},
		{"fail", "failed", "failing", "fails"},
		{"stop", "stopped", "stopping", "stops"},
		{"pause", "paused", "pausing"},
		{"retain", "retained", "retaining"},
		{"query", "queries"},
		{"drop", "dropped", "dropping"},
	}
	for _, g := range groups {
		want := stem(g[0])
		for _, w := range g[1:] {
			if got := stem(w); got != want {
				t.Errorf("stem(%q) = %q, want %q (stem of %q)", w, got, want, g[0])
			}
		}
	}
	keep := []string{"status", "process", "analysis", "bed", "sing", "40403", "22p02", "max.poll", "restart_lsn", "kafka", "wal"}
	for _, w := range keep {
		if got := stem(w); got != w {
			t.Errorf("stem(%q) = %q, want unchanged", w, got)
		}
	}
}

func TestSynonymsExpand(t *testing.T) {
	s := newSynonyms()
	cases := []struct {
		query string
		want  []string // must all be in the expansion
		not   []string // must not be
	}{
		{"consumer behind", []string{"lag"}, nil},
		{"replication slot inactive", nil, []string{"replication", "slot"}}, // already typed: not added again
		{"slot inactive", []string{"replication"}, nil},
		{"dead letter queue", []string{"dlq"}, nil},
		{"unknown magic byte", []string{"poison", "pill"}, nil},
		{"log start offset", []string{"retention"}, nil},
		{"nothing relevant here", nil, []string{"lag", "slot", "retention"}},
	}
	for _, c := range cases {
		got := s.expand(terms(c.query))
		for _, w := range c.want {
			if !slices.Contains(got, w) {
				t.Errorf("expand(%q) = %q, missing %q", c.query, got, w)
			}
		}
		for _, w := range c.not {
			if slices.Contains(got, w) {
				t.Errorf("expand(%q) = %q, must not contain %q", c.query, got, w)
			}
		}
		if again := s.expand(terms(c.query)); !slices.Equal(got, again) {
			t.Errorf("expand(%q) is not deterministic: %q vs %q", c.query, got, again)
		}
	}
}
