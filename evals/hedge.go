package evals

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

// causeHedge reports whether text offers several causes instead of one, and the
// first marker it found.
func causeHedge(text string) (string, bool) {
	toks := tokenize(text)
	for _, m := range hedgeMarkers {
		if matchTerm(toks, m) {
			return m, true
		}
	}
	return "", false
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
