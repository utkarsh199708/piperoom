package evals

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

// pairedReports builds two reports over one scenario: n runs, base passes the
// first basePass runs; the candidate flips toFail of those to fail and passes
// toPass of the others.
func pairedReports(n, basePass, toFail, toPass int) (Report, Report) {
	var base, cand Report
	for i := 1; i <= n; i++ {
		b := i <= basePass
		c := b
		switch {
		case b && i <= toFail:
			c = false
		case !b && i <= basePass+toPass:
			c = true
		}
		base.Results = append(base.Results, RunResult{Scenario: "s", Run: i, Pass: b})
		cand.Results = append(cand.Results, RunResult{Scenario: "s", Run: i, Pass: c})
	}
	return base, cand
}

func TestComparePairedOnFixedInputs(t *testing.T) {
	base, cand := pairedReports(30, 15, 3, 10)
	pc := ComparePaired(base, cand)
	if pc.Pairs != 30 || pc.Unmatched != 0 || pc.TooSmall {
		t.Fatalf("pairs = %+v", pc)
	}
	// McNemar: 3 runs went pass→fail, 10 fail→pass; 12 pass in both, 5 fail in both.
	if pc.PassToFail != 3 || pc.FailToPass != 10 || pc.BothPass != 12 || pc.BothFail != 5 {
		t.Fatalf("McNemar counts = %+v", pc)
	}
	// Exact McNemar on 3 vs 10 discordant pairs: 2 × (1+13+78+286)/2^13.
	if !near(pc.McNemarP, 756.0/8192) || fmt.Sprintf("%.3f", pc.McNemarP) != "0.092" {
		t.Fatalf("McNemar p = %v, want 0.092", pc.McNemarP)
	}
	if !near(pc.BasePassRate, 0.5) || !near(pc.CandPassRate, 22.0/30) || !near(pc.Diff, 7.0/30) {
		t.Fatalf("rates = %+v", pc)
	}
	if pc.Resamples != 10000 || pc.Seed != BootstrapSeed {
		t.Fatalf("bootstrap settings = %d, %d", pc.Resamples, pc.Seed)
	}
	// The interval brackets the observed difference and, with 13 discordant
	// pairs out of 30, is a few tenths wide.
	if !(pc.CILow < pc.Diff && pc.Diff < pc.CIHigh) || pc.CIHigh-pc.CILow < 0.15 || pc.CIHigh-pc.CILow > 0.5 {
		t.Fatalf("CI [%v, %v] around %v", pc.CILow, pc.CIHigh, pc.Diff)
	}
	// Pinned: the seed and the PCG-DXSM source fix the interval exactly.
	if !near(pc.CILow, wantCILow) || !near(pc.CIHigh, wantCIHigh) {
		t.Fatalf("CI = [%v, %v], want the pinned [%v, %v]", pc.CILow, pc.CIHigh, wantCILow, wantCIHigh)
	}

	// The order the reports list their runs in does not matter.
	for i, j := 0, len(cand.Results)-1; i < j; i, j = i+1, j-1 {
		cand.Results[i], cand.Results[j] = cand.Results[j], cand.Results[i]
	}
	if again := ComparePaired(base, cand); again != pc {
		t.Fatalf("reordered runs changed the comparison:\n%+v\n%+v", again, pc)
	}
}

// The 95% interval of TestComparePairedOnFixedInputs with BootstrapSeed and
// 10 000 resamples, in pass-rate units (resampled means of 30 pairs fall on
// multiples of 1/30). A normal approximation gives about [0.01, 0.45].
const (
	wantCILow  = 0.0
	wantCIHigh = 14.0 / 30
)

func TestMcNemarExactP(t *testing.T) {
	for _, c := range []struct {
		b, c int
		want float64
	}{
		{0, 0, 1}, {1, 1, 1}, {0, 1, 1}, {0, 5, 0.0625}, {5, 0, 0.0625}, {3, 10, 756.0 / 8192}, {10, 3, 756.0 / 8192},
		{0, 10, 2.0 / 1024},
	} {
		if got := McNemarExactP(c.b, c.c); !near(got, c.want) {
			t.Errorf("p(%d, %d) = %v, want %v", c.b, c.c, got, c.want)
		}
	}
	if p := McNemarExactP(400, 600); p <= 0 || p >= 1e-9 || math.IsNaN(p) {
		t.Errorf("p(400, 600) = %v, want a tiny finite value", p)
	}
}

func TestBootstrapDiffCI(t *testing.T) {
	same := []bool{true, false, true, true, false}
	if lo, hi := BootstrapDiffCI(same, same, 1000, 1); lo != 0 || hi != 0 {
		t.Fatalf("no discordant pair: [%v, %v], want [0, 0]", lo, hi)
	}
	allFail, allPass := make([]bool, 25), make([]bool, 25)
	for i := range allPass {
		allPass[i] = true
	}
	if lo, hi := BootstrapDiffCI(allFail, allPass, 1000, 1); lo != 1 || hi != 1 {
		t.Fatalf("every pair fail→pass: [%v, %v], want [1, 1]", lo, hi)
	}
	base, cand := pairedReports(40, 20, 6, 6)
	b, c := passes(base), passes(cand)
	lo1, hi1 := BootstrapDiffCI(b, c, 2000, 7)
	lo2, hi2 := BootstrapDiffCI(b, c, 2000, 7)
	if lo1 != lo2 || hi1 != hi2 {
		t.Fatal("the same seed gave two intervals")
	}
	if !(lo1 < 0 && hi1 > 0) {
		t.Fatalf("a balanced flip (6 each way) must straddle 0: [%v, %v]", lo1, hi1)
	}
	if lo3, hi3 := BootstrapDiffCI(b, c, 2000, 8); lo3 == lo1 && hi3 == hi1 {
		t.Log("another seed gave the same interval (possible, but unlikely)")
	}
	if lo, hi := BootstrapDiffCI(nil, nil, 100, 1); lo != 0 || hi != 0 {
		t.Fatal("empty input")
	}
	if lo, hi := BootstrapDiffCI(b, c[:3], 100, 1); lo != 0 || hi != 0 {
		t.Fatal("unequal lengths must give no interval")
	}
}

func passes(r Report) []bool {
	out := make([]bool, len(r.Results))
	for i, res := range r.Results {
		out[i] = res.Pass
	}
	return out
}

func TestComparePairedMatchesOnScenarioAndRun(t *testing.T) {
	base := Report{Results: []RunResult{
		{Scenario: "a", Run: 1, Pass: true}, {Scenario: "a", Run: 2, Pass: false},
		{Scenario: "b", Run: 1, Pass: true}, {Scenario: "only-base", Run: 1, Pass: true},
	}}
	cand := Report{Results: []RunResult{
		{Scenario: "a", Run: 2, Pass: true}, {Scenario: "a", Run: 1, Pass: true},
		{Scenario: "b", Run: 1, Pass: false}, {Scenario: "a", Run: 3, Pass: true},
	}}
	pc := ComparePaired(base, cand)
	if pc.Pairs != 3 || pc.Unmatched != 2 || pc.FailToPass != 1 || pc.PassToFail != 1 || pc.BothPass != 1 || !pc.TooSmall {
		t.Fatalf("paired = %+v", pc)
	}
	var md strings.Builder
	if err := pc.WriteMarkdown(&md); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"3 pair(s), 2 unmatched", "pass→fail 1, fail→pass 1", "exact two-sided p = 1.000", "Sample too small to call: 3 pair(s), need at least 20"} {
		if !strings.Contains(md.String(), want) {
			t.Fatalf("markdown lacks %q:\n%s", want, md.String())
		}
	}
}

func TestPairRunsUsesAFixedOrder(t *testing.T) {
	base := Report{Results: []RunResult{{Scenario: "b", Run: 1, Pass: true}, {Scenario: "a", Run: 2}, {Scenario: "a", Run: 1, Pass: true}}}
	cand := Report{Results: []RunResult{{Scenario: "a", Run: 2, Pass: true}, {Scenario: "b", Run: 1}, {Scenario: "a", Run: 1}}}
	b, c, unmatched := pairRuns(base, cand)
	// (a,1) (a,2) (b,1), whatever order either report lists them in.
	if !reflect.DeepEqual(b, []bool{true, false, true}) || !reflect.DeepEqual(c, []bool{false, true, false}) || unmatched != 0 {
		t.Fatalf("pairs b=%v c=%v unmatched=%d", b, c, unmatched)
	}
}

func TestComparePairedSampleSizeNote(t *testing.T) {
	for _, c := range []struct {
		n    int
		note bool
	}{{0, true}, {19, true}, {20, false}, {35, false}} {
		base, cand := pairedReports(c.n, c.n/2, 1, 1)
		pc := ComparePaired(base, cand)
		var md strings.Builder
		if err := pc.WriteMarkdown(&md); err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(md.String(), "Sample too small to call"); got != c.note || pc.TooSmall != c.note {
			t.Errorf("n=%d: note %v (TooSmall %v), want %v", c.n, got, pc.TooSmall, c.note)
		}
		if c.n == 0 && (!strings.Contains(md.String(), "No run is in both reports") || pc.CILow != 0 || pc.CIHigh != 0) {
			t.Errorf("no pairs: %+v\n%s", pc, md.String())
		}
	}
}

func restrictFixture() Report {
	mk := func(name string, pass float64) ScenarioSummary {
		return ScenarioSummary{Name: name, Summary: Summary{Runs: 2, Passes: int(pass * 2), PassRate: pass, MeanScore: 100 * pass}}
	}
	r := Report{Runs: 3, Scenarios: []ScenarioSummary{mk("a", 1), mk("b", 0), mk("c", 1)}}
	for _, n := range []string{"a", "b", "c"} {
		r.Results = append(r.Results, RunResult{Scenario: n, Run: 1, Pass: n != "b"})
	}
	var ss []Summary
	for _, s := range r.Scenarios {
		ss = append(ss, s.Summary)
	}
	r.Overall = combine(ss)
	return r
}

func TestRestrictRecomputesOverall(t *testing.T) {
	r := restrictFixture().Restrict([]string{"a", "b"})
	if len(r.Scenarios) != 2 || len(r.Results) != 2 {
		t.Fatalf("scenarios=%d results=%d", len(r.Scenarios), len(r.Results))
	}
	if math.Abs(r.Overall.PassRate-0.5) > 1e-9 || r.Overall.Runs != 4 {
		t.Fatalf("overall = %+v", r.Overall)
	}
}

func TestCompareSubsetVersusFull(t *testing.T) {
	base := restrictFixture()
	cand := Report{Runs: 3, Scenarios: []ScenarioSummary{
		{Name: "a", Summary: base.Scenarios[0].Summary},
		{Name: "c"}, // selected but produced no runs
	}}
	cand.Overall = combine([]Summary{base.Scenarios[0].Summary})
	for _, tc := range []struct {
		name        string
		subset      bool
		regressions int
		rows        int
	}{
		{"full run keeps missing-from-candidate", false, 2, 3}, // b and c missing/empty
		{"subset compares only selected", true, 1, 2},          // c selected but empty
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := base
			if tc.subset {
				b = b.Restrict([]string{"a", "c"})
			}
			c := Compare(b, cand, 5)
			if c.Regressions != tc.regressions || len(c.Rows) != tc.rows {
				t.Fatalf("regressions=%d rows=%d, want %d/%d: %+v", c.Regressions, len(c.Rows), tc.regressions, tc.rows, c.Rows)
			}
			if tc.subset && c.Overall.Regressed {
				t.Fatalf("overall compared unlike sets: %+v", c.Overall)
			}
			if wantRuns := map[bool]int{false: 6, true: 4}[tc.subset]; c.Overall.Base.Runs != wantRuns {
				t.Fatalf("baseline overall runs = %d, want %d", c.Overall.Base.Runs, wantRuns)
			}
		})
	}
}
