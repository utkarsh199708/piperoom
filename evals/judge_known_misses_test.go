package evals

// Known judge misses (#454): corpus texts whose recorded judge verdict differs
// from the committed corpus label. TestJudgedCorpusGrades fails on any other
// difference, and fails on a pin that the judge now agrees with, so this list
// cannot rot.
//
// A pin is not a ruling that the judge is right. The labels follow the grading
// policy the corpus pins (golden_corpus_test.go, scenario_terms_test.go), and
// the judge rubric does not state that policy. Fixing the rubric bumps
// JudgePromptVersion and needs a paid refill of the whole cache, which needs
// the owner's approval (see the PR thread).
const (
	missHedgePolicy = "the label follows the pinned hedge policy (r4.4: a correct cause plus a hedge that does not rule out the named trap, \"reset to latest\", does not commit, so it fails); the judge rubric does not state that policy and accepts the hedged answer"
	missGarbled     = "the text names the owner change but states the mechanism wrongly (the role \"owns tables, not publications\"); the label is a negation-handling row (\"not publications\" must not read as a denial), while the judge also grades the mechanism"
)

// The consumer-offsets-out-of-range-retention rows were re-pinned after the
// R14 relabel refill (W4n-3, run 40073224422): two hedges the judge used to
// accept it now fails like the label ("We can't, I think, ...", "Not that we
// ruled out ..."), and one it used to fail it now accepts ("We can't yet,
// however, ..."). Same hedge policy, same judge rubric: the verdicts flip on
// wording near the policy's edge.
//
// knownJudgeMisses maps scenario + NUL + text to the reason the judge's grade
// is allowed to differ from the label.
var knownJudgeMisses = map[string]string{
	"consumer-offsets-out-of-range-retention\x00Order-sync's committed offsets were deleted by retention. I'm not sure I ruled out a reset to latest.":        missHedgePolicy,
	"consumer-offsets-out-of-range-retention\x00Order-sync's committed offsets were deleted by retention. Can't say I ruled out a reset to latest.":           missHedgePolicy,
	"consumer-offsets-out-of-range-retention\x00Order-sync's committed offsets were deleted by retention. Not sure we ruled out a reset to latest.":           missHedgePolicy,
	"consumer-offsets-out-of-range-retention\x00Order-sync's committed offsets were deleted by retention. We can't yet, however, rule out a reset to latest.": missHedgePolicy,
	"consumer-offsets-out-of-range-retention\x00Order-sync's committed offsets were deleted by retention. That does not mean we ruled out a reset to latest.": missHedgePolicy,
	"consumer-offsets-out-of-range-retention\x00Order-sync's committed offsets were deleted by retention. We cannot confirm or rule out a reset to latest.":   missHedgePolicy,
	"consumer-offsets-out-of-range-retention\x00Order-sync's committed offsets were deleted by retention. We cannot confirm, or rule out, a reset to latest.": missHedgePolicy,
	"topic-retention-data-loss\x00Retention is 6 h and records were lost. We can't say we ruled out a reset to latest.":                                       missHedgePolicy,
	"publication-permission-denied\x00Root cause: the orders table owner changed; the debezium role still owns its slot, not publications.":                   missGarbled,
	"publication-permission-denied\x00Root cause: the table owner changed, so Debezium's role owns tables, not publications.":                                 missGarbled,
}
