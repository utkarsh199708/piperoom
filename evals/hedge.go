package evals

import "slices"

// A diagnosis that lists several candidate causes names the right one by
// construction: the root-cause terms match whatever the truth is. Such an
// answer must not count as finding the cause, or a vaguer agent scores
// better. Two guards catch it:
//
//   - causeHedge (below, every scenario): the text offers alternatives in so many
//     words: "either ... or", "possible causes", "could also be", "one of",
//     "not sure whether". These phrases do not describe a pipeline; a
//     diagnosis that commits to a cause has no use for them.
//   - root_cause_forbidden (per scenario): the wrong stories a shotgun answer
//     would also tell, each an AND of terms in one clause. A plain list
//     ("amount changed to numeric; order-sync crashed; retention deleted
//     records") has no hedge words, so a scenario must name its likely wrong
//     stories for this guard to see it. evalrun -validate notes every
//     diagnose scenario that names none (Scenario.Notes).
//
// Both make the root cause wrong; the LLM judge's verdict cannot undo the
// hedge guard (ApplyVerdict), since its prompt already rejects lists.

// hedgeMarkers are matched as terms (whole words, "*" = prefix) anywhere in
// the text.
var hedgeMarkers = []string{
	"either",
	"or possibly", "or perhaps", "or maybe", "or alternatively", "alternatively",
	"could also be", "might also be", "may also be", "can also be", "could equally be",
	"another possibility", "other possibilities", "another possible",
	"possible causes", "possible root causes", "potential causes", "potential root causes",
	"candidate causes", "several possible", "multiple possible", "a number of possible",
	"one of the following", "one of these", "any of these", "some combination",
	"not sure whether", "not sure if", "not sure which", "unclear whether", "unclear which",
	"can t tell whether", "cannot tell whether", "can t tell which", "cannot tell which",
	"hard to say whether", "hard to say which",
}

// hedgeDenials just before a marker turn it into a commitment: "there are no
// other possibilities", "I ruled out the other possible causes".
var hedgeDenials = []string{"no", "not", "t", "without", "rule out", "rules out", "ruled out", "ruling out"}

// causeHedge reports whether text offers several causes instead of one, and the
// first marker it found. A marker counts per clause: "either" only with an
// "or" after it ("the registry did not change either" commits), and no marker
// right after a denial.
func causeHedge(text string) (string, bool) {
	// clauseSplit, not clausesOf: a question still offers its alternatives.
	var cls [][]string
	for _, c := range clauseSplit.Split(text, -1) {
		cls = append(cls, tokenize(c))
	}
	for _, m := range hedgeMarkers {
		for _, toks := range cls {
			for _, i := range findTerm(toks, m) {
				if m == "either" && !slices.Contains(toks[i+1:], "or") {
					continue
				}
				if hedgeDenied(toks, i) {
					continue
				}
				return m, true
			}
		}
	}
	return "", false
}

// hedgeDenied reports whether a hedge denial ends in the four words before
// toks[i], with only determiners or "other" between them.
func hedgeDenied(toks []string, i int) bool {
	for k := max(0, i-4); k < i; k++ {
		l := phraseAt(toks, k, hedgeDenials)
		if l == 0 || k+l > i {
			continue
		}
		gap := true
		for _, t := range toks[k+l : i] {
			if t != "the" && t != "other" && t != "all" && t != "any" {
				gap = false
			}
		}
		if gap {
			return true
		}
	}
	return false
}

// Notes are advice about a valid scenario: what its expectations cannot
// catch. evalrun -validate prints them; they never fail a run.
func (s Scenario) Notes() []string {
	var n []string
	if s.Expect.outcome() == OutcomeDiagnose && len(s.Expect.RootCauseForbidden) == 0 {
		n = append(n, "expect.root_cause_forbidden is empty: only hedge words catch a hypothesis that lists every candidate cause")
	}
	if s.Expect.outcome() == OutcomeDiagnose && len(s.Expect.Fix) > 0 && len(s.Expect.FixTargets) == 0 {
		n = append(n, "expect.fix_targets is empty: a fix step aimed at the wrong resource, or extra steps, still pass the fix check")
	}
	return n
}
