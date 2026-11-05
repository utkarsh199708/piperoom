package runner

import (
	"testing"
)

// TestClassNeverLowered: the class floor only ever rises; a lower incoming
// class (or an empty one, meaning "let the server derive it") never lowers it.
func TestClassNeverLowered(t *testing.T) {
	cases := []struct {
		name    string
		current string
		in      string
		want    string
	}{
		{"same local", "local", "local", "local"},
		{"local raised to shared", "local", "shared", "shared"},
		{"local raised to production", "local", "production", "production"},
		{"production never lowered to local", "production", "local", "production"},
		{"production never lowered to shared", "production", "shared", "production"},
		{"shared never lowered to local", "shared", "local", "shared"},
		{"shared raised to production", "shared", "production", "production"},
		{"empty never lowers", "production", "", "production"},
		{"empty incoming raises", "", "production", "production"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := maxClass(c.current, c.in); got != c.want {
				t.Errorf("maxClass(%q, %q) = %q, want %q", c.current, c.in, got, c.want)
			}
		})
	}
}

// TestTierRankAndClassRank: the ordering helpers are total and monotonic.
func TestTierRankAndClassRank(t *testing.T) {
	if tierRank("T0") >= tierRank("T1") || tierRank("T1") >= tierRank("T2") || tierRank("T2") >= tierRank("T3") {
		t.Error("tier rank is not increasing T0 < T1 < T2 < T3")
	}
	if classRank("local") >= classRank("shared") || classRank("shared") >= classRank("production") {
		t.Error("class rank is not increasing local < shared < production")
	}
	if classRank("garbage") != 0 {
		t.Error("an unknown class must rank as the lowest, never as production")
	}
}
