package textsearch

import (
	"slices"
	"testing"
)

func TestTerms(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"the of and", nil},
		{"error code: 40403", []string{"error", "code", "40403"}},
		{"0/16B3748", []string{"0/16b3748"}},
		{"org.apache.kafka.DataException", []string{"org.apache.kafka.dataexception", "org", "apach", "kafka", "dataexception", "data", "exception"}},
		{"restart_lsn", []string{"restart_lsn", "restart", "lsn"}},
		{"rebalancing rebalanced", []string{"rebalanc", "rebalanc"}},
		{"SQLState", []string{"sqlstate", "sql", "stat"}},
	} {
		if got := Terms(tc.in); !slices.Equal(got, tc.want) {
			t.Errorf("Terms(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestBM25RanksRareTermsAndWeights(t *testing.T) {
	ix := New([][]Field{
		{{Text: "kafka consumer lag", Weight: 1}},
		{{Text: "kafka connect avro schema registry", Weight: 1}},
		{{Text: "kafka postgres slot", Weight: 1}},
		{{Text: "kafka", Weight: 3}, {Text: "avro", Weight: 1}},
	})
	if ix.Len() != 4 {
		t.Fatalf("len = %d", ix.Len())
	}
	q := QueryTerms("avro kafka kafka", 0)
	if len(q) != 2 {
		t.Fatalf("query terms = %v, want 2 (repeats dropped)", q)
	}
	best, bestScore := -1, 0.0
	for i := range 4 {
		if s := ix.Score(i, q); s > bestScore {
			best, bestScore = i, s
		}
	}
	if best != 1 && best != 3 {
		t.Errorf("best doc = %d, want one of the avro docs", best)
	}
	if ix.Score(0, QueryTerms("zzz", 0)) != 0 {
		t.Error("a doc without any query term must score 0")
	}
	// Same input, same scores.
	if a, b := ix.Score(1, q), ix.Score(1, q); a != b {
		t.Errorf("scores differ: %v %v", a, b)
	}
}

func TestQueryTermsCap(t *testing.T) {
	if got := QueryTerms("alpha beta gamma delta", 2); len(got) != 2 || got[0].Term != "alpha" {
		t.Errorf("QueryTerms cap = %v", got)
	}
}

func TestEmptyIndex(t *testing.T) {
	ix := New(nil)
	if ix.Len() != 0 {
		t.Fatal("empty index must have no docs")
	}
	ix = New([][]Field{{}})
	if s := ix.Score(0, QueryTerms("a b c", 0)); s != 0 {
		t.Errorf("score on empty doc = %v", s)
	}
}
