package evals

import (
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
)

// Paired comparison settings (docs/design/cost.md 5).
const (
	// BootstrapResamples is how many resamples the pass-rate CI uses.
	BootstrapResamples = 10000
	// BootstrapSeed fixes the resampling, so a comparison always prints the
	// same interval for the same reports.
	BootstrapSeed uint64 = 20261013
	// MinPairs is the fewest matched runs worth calling a difference on.
	MinPairs = 20
)

// PairedComparison compares two reports run by run: a run of the candidate is
// paired with the base run of the same scenario and run index, so scenario
// difficulty cancels out.
type PairedComparison struct {
	Pairs int `json:"pairs"`
	// Unmatched counts runs present in only one of the reports.
	Unmatched    int     `json:"unmatched"`
	BasePassRate float64 `json:"base_pass_rate"`
	CandPassRate float64 `json:"candidate_pass_rate"`
	// Diff is CandPassRate - BasePassRate; CILow and CIHigh bound it (95%
	// bootstrap percentile interval). All three are 0 when Pairs is 0.
	Diff      float64 `json:"pass_rate_diff"`
	CILow     float64 `json:"ci95_low"`
	CIHigh    float64 `json:"ci95_high"`
	Resamples int     `json:"resamples"`
	Seed      uint64  `json:"seed"`
	// McNemar counts: the discordant pairs decide the test, the concordant
	// ones are shown for context.
	PassToFail int `json:"pass_to_fail"`
	FailToPass int `json:"fail_to_pass"`
	BothPass   int `json:"both_pass"`
	BothFail   int `json:"both_fail"`
	// McNemarP is the exact two-sided McNemar p-value: the binomial test of
	// the discordant pairs against an even split. 1 with none.
	McNemarP float64 `json:"mcnemar_p"`
	// TooSmall is true below MinPairs pairs.
	TooSmall bool `json:"too_small"`
}

type runKey struct {
	scenario string
	run      int
}

// ComparePaired pairs the runs of base and cand on (scenario, run index).
func ComparePaired(base, cand Report) PairedComparison {
	pc := PairedComparison{Resamples: BootstrapResamples, Seed: BootstrapSeed}
	b, c, unmatched := pairRuns(base, cand)
	pc.Pairs, pc.Unmatched = len(b), unmatched
	pc.TooSmall = pc.Pairs < MinPairs
	if pc.Pairs == 0 {
		return pc
	}
	var bp, cp int
	for i := range b {
		switch {
		case b[i] && c[i]:
			pc.BothPass++
		case b[i]:
			pc.PassToFail++
		case c[i]:
			pc.FailToPass++
		default:
			pc.BothFail++
		}
		if b[i] {
			bp++
		}
		if c[i] {
			cp++
		}
	}
	pc.McNemarP = McNemarExactP(pc.PassToFail, pc.FailToPass)
	n := float64(pc.Pairs)
	pc.BasePassRate, pc.CandPassRate = float64(bp)/n, float64(cp)/n
	pc.Diff = pc.CandPassRate - pc.BasePassRate
	pc.CILow, pc.CIHigh = BootstrapDiffCI(b, c, pc.Resamples, pc.Seed)
	return pc
}

// pairRuns returns the pass results of the runs in both reports, in
// (scenario, run) order, and how many runs are in only one of them. The fixed
// order keeps the bootstrap reproducible whatever order the reports list
// their runs in.
func pairRuns(base, cand Report) (b, c []bool, unmatched int) {
	baseBy := map[runKey]bool{}
	for _, r := range base.Results {
		baseBy[runKey{r.Scenario, r.Run}] = r.Pass
	}
	var keys []runKey
	candBy := map[runKey]bool{}
	for _, r := range cand.Results {
		k := runKey{r.Scenario, r.Run}
		candBy[k] = r.Pass
		if _, ok := baseBy[k]; ok {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].scenario != keys[j].scenario {
			return keys[i].scenario < keys[j].scenario
		}
		return keys[i].run < keys[j].run
	})
	b, c = make([]bool, len(keys)), make([]bool, len(keys))
	for i, k := range keys {
		b[i], c[i] = baseBy[k], candBy[k]
	}
	return b, c, len(baseBy) + len(candBy) - 2*len(keys)
}

// BootstrapDiffCI is the 95% percentile bootstrap interval of the mean paired
// difference cand[i] - base[i] (pass = 1): it draws len(base) pairs with
// replacement, resamples times, from a PCG-DXSM source seeded with seed. The
// index is the source's Uint64 modulo n (the bias is below 1e-15 for any eval
// size), so the interval depends only on that fixed algorithm: the same
// inputs and seed always give the same interval. Empty input gives 0, 0.
func BootstrapDiffCI(base, cand []bool, resamples int, seed uint64) (lo, hi float64) {
	n := len(base)
	if n == 0 || n != len(cand) || resamples <= 0 {
		return 0, 0
	}
	d := make([]float64, n)
	for i := range d {
		d[i] = b2f(cand[i]) - b2f(base[i])
	}
	src := rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)
	means := make([]float64, resamples)
	for r := range means {
		var sum float64
		for range n {
			sum += d[src.Uint64()%uint64(n)]
		}
		means[r] = sum / float64(n)
	}
	sort.Float64s(means)
	return sortedPercentile(means, 2.5), sortedPercentile(means, 97.5)
}

// McNemarExactP is the exact two-sided McNemar p-value for b and c
// discordant pairs: 2 × P(X <= min(b, c)) for X ~ Binomial(b+c, 1/2), at
// most 1. The terms are summed in log space, so large counts stay finite.
func McNemarExactP(b, c int) float64 {
	n := b + c
	if n == 0 {
		return 1
	}
	k := min(b, c)
	lgN, _ := math.Lgamma(float64(n + 1))
	var sum float64
	for i := 0; i <= k; i++ {
		lgI, _ := math.Lgamma(float64(i + 1))
		lgR, _ := math.Lgamma(float64(n - i + 1))
		sum += math.Exp(lgN - lgI - lgR - float64(n)*math.Ln2)
	}
	return math.Min(1, 2*sum)
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// WriteMarkdown writes the paired comparison as a short section.
func (pc PairedComparison) WriteMarkdown(w io.Writer) error {
	var b strings.Builder
	b.WriteString("\n### Paired comparison (same scenario and run index)\n\n")
	if pc.Pairs == 0 {
		fmt.Fprintf(&b, "No run is in both reports (%d unmatched); nothing to pair.\n", pc.Unmatched)
	} else {
		fmt.Fprintf(&b, "%d pair(s), %d unmatched run(s). Pass rate %.1f%% → %.1f%% (%+.1f points), 95%% bootstrap CI [%+.1f, %+.1f] points (%d resamples, seed %d).\n\n",
			pc.Pairs, pc.Unmatched, 100*pc.BasePassRate, 100*pc.CandPassRate, 100*pc.Diff, 100*pc.CILow, 100*pc.CIHigh, pc.Resamples, pc.Seed)
		fmt.Fprintf(&b, "McNemar discordant pairs: pass→fail %d, fail→pass %d (both pass %d, both fail %d); exact two-sided p = %.3f.\n",
			pc.PassToFail, pc.FailToPass, pc.BothPass, pc.BothFail, pc.McNemarP)
	}
	if pc.TooSmall {
		fmt.Fprintf(&b, "\nSample too small to call: %d pair(s), need at least %d.\n", pc.Pairs, MinPairs)
	}
	if _, err := io.WriteString(w, b.String()); err != nil {
		return fmt.Errorf("evals: write paired comparison: %w", err)
	}
	return nil
}
