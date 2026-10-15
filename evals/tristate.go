package evals

import (
	"fmt"
	"strings"
)

// The tri-state keyword pass (#454). The keyword grader decides the clear
// texts; a text where a negator, rule-out or hedge shares a clause with the
// words it grades is one the keyword rules have kept misgrading (#395, #384,
// #412), so it is marked ambiguous and handed to the LLM judge instead of
// being decided by a new heuristic. The negation vocabulary is the frozen one
// of forbidden.go (forbiddenDenials, forbiddenNegators, ruleOuts) and hedge.go
// (hedgeMarkers): this file reads it, it adds none.
//
// An item (root cause, first failing layer, root-cause layer) is ambiguous
// when, in one clause (clauseSplit, the clause matchForbidden reads):
//   - a denial span (forbiddenDenials) or a hedge marker sits with a match of
//     the item's words: any term of any expect.root_cause group, or every term
//     of a root_cause_forbidden row, for the root cause; any of the layer's
//     words (layerTermsFor) for a layer; or
//   - a forbidden row fires (matchForbidden on that clause) and the clause
//     has any negator or rule-out, a cancelled "cannot rule out" pair
//     included (root cause).
//
// Presence is plain word matching (findTerm), whatever the denial rules make
// of it. A text with no ambiguous item keeps its keyword grade exactly: the
// pass changes nothing else, so a clear text cannot flip.

// Grade is one item's keyword verdict.
type Grade string

// The three keyword verdicts.
const (
	GradePass      Grade = "pass"
	GradeFail      Grade = "fail"
	GradeAmbiguous Grade = "ambiguous"
)

// Ambiguity says which items of a final hypothesis the keyword pass could not
// decide, and why. It is recorded on every diagnose run that has one; the
// judge mode decides what happens to it (judge.go, judge_auto.go).
type Ambiguity struct {
	RootCause      bool `json:"root_cause,omitempty"`
	Layer          bool `json:"layer,omitempty"`
	RootCauseLayer bool `json:"root_cause_layer,omitempty"`
	// Reasons name the trigger per clause, e.g. `clause 2: "not" with root
	// cause term "amount"`.
	Reasons []string `json:"reasons,omitempty"`
	// KeywordRootCause, KeywordLayer and KeywordRootCauseLayer are the
	// keyword pass's own verdicts, kept whatever replaced them.
	KeywordRootCause      bool `json:"keyword_root_cause"`
	KeywordLayer          bool `json:"keyword_layer"`
	KeywordRootCauseLayer bool `json:"keyword_root_cause_layer"`
	// Source is how the ambiguity was settled: "cache" or "live" (a judge
	// verdict replaced the three items), "unjudged" (no verdict: the
	// ambiguous items count as not passed), or "" (keyword grade kept, as
	// -judge terms does).
	Source string `json:"source,omitempty"`
	// UnjudgedWhy says why no verdict was had (cap reached, no live judge).
	UnjudgedWhy string `json:"unjudged_why,omitempty"`
}

// Any reports whether some item is ambiguous.
func (a Ambiguity) Any() bool {
	return a.RootCause || a.Layer || a.RootCauseLayer
}

// Items lists the ambiguous items by name.
func (a Ambiguity) Items() []string {
	var out []string
	if a.RootCause {
		out = append(out, "root_cause")
	}
	if a.Layer {
		out = append(out, "first_failing_layer")
	}
	if a.RootCauseLayer {
		out = append(out, "root_cause_layer")
	}
	return out
}

// Unjudged reports whether the ambiguity was left without a verdict.
func (a *Ambiguity) Unjudged() bool {
	return a != nil && a.Source == ambiguityUnjudged
}

// Settled ambiguity sources.
const (
	ambiguityCache    = "cache"
	ambiguityLive     = "live"
	ambiguityUnjudged = "unjudged"
)

// KeywordGrades is the tri-state keyword verdict of one hypothesis.
type KeywordGrades struct {
	RootCause      Grade
	Layer          Grade
	RootCauseLayer Grade
	Ambiguity      Ambiguity
}

// GradeKeywords runs the tri-state keyword pass on a scenario's final
// hypothesis. The pass/fail grades are exactly Judge's (rootCauseMatches,
// namesLayer); ambiguous replaces them only where ambiguityOf finds a reason.
func GradeKeywords(sc Scenario, text string) KeywordGrades {
	a := ambiguityOf(sc, text)
	a.KeywordRootCause = rootCauseMatches(sc.Expect, text)
	a.KeywordLayer = keywordLayer(sc, sc.Expect.FirstFailingLayer, text)
	a.KeywordRootCauseLayer = keywordLayer(sc, sc.Expect.RootCauseLayer, text)
	return KeywordGrades{
		RootCause:      triGrade(a.RootCause, a.KeywordRootCause),
		Layer:          triGrade(a.Layer, a.KeywordLayer),
		RootCauseLayer: triGrade(a.RootCauseLayer, a.KeywordRootCauseLayer),
		Ambiguity:      a,
	}
}

// keywordLayer is layerGraded for a text: true when the scenario sets no layer.
func keywordLayer(sc Scenario, layer, text string) bool {
	return layer == "" || namesLayer(layerTermsFor(sc.Pipeline, layer), text)
}

func triGrade(ambiguous, ok bool) Grade {
	switch {
	case ambiguous:
		return GradeAmbiguous
	case ok:
		return GradePass
	}
	return GradeFail
}

// ambiguityOf finds the ambiguous items of text (see the file comment). Only
// a diagnose scenario has items to grade; a layer the scenario does not set
// is never ambiguous.
func ambiguityOf(sc Scenario, text string) Ambiguity {
	var a Ambiguity
	if sc.Expect.outcome() != OutcomeDiagnose {
		return a
	}
	var first, rcLayer []string
	if l := sc.Expect.FirstFailingLayer; l != "" {
		first = layerTermsFor(sc.Pipeline, l)
	}
	if l := sc.Expect.RootCauseLayer; l != "" {
		rcLayer = layerTermsFor(sc.Pipeline, l)
	}
	for n, clause := range clauseSplit.Split(text, -1) {
		toks := tokenize(clause)
		if len(toks) == 0 {
			continue
		}
		where := fmt.Sprintf("clause %d", n+1)
		trigger := clauseTrigger(toks)
		rawNeg := anyNegator(toks)
		if trigger != "" {
			if t := firstPresent(toks, flatGroups(sc.Expect.RootCause)); t != "" {
				a.RootCause = true
				a.Reasons = append(a.Reasons, fmt.Sprintf("%s: %q with root cause term %q", where, trigger, t))
			}
			if row := presentRow(toks, sc.Expect.RootCauseForbidden); row != nil {
				a.RootCause = true
				a.Reasons = append(a.Reasons, fmt.Sprintf("%s: %q with forbidden row [%s]", where, trigger, strings.Join(row, " + ")))
			}
			if t := firstPresent(toks, first); t != "" {
				a.Layer = true
				a.Reasons = append(a.Reasons, fmt.Sprintf("%s: %q with first-failing-layer term %q", where, trigger, t))
			}
			if t := firstPresent(toks, rcLayer); t != "" {
				a.RootCauseLayer = true
				a.Reasons = append(a.Reasons, fmt.Sprintf("%s: %q with root-cause-layer term %q", where, trigger, t))
			}
		}
		if rawNeg != "" && matchForbidden(sc.Expect.RootCauseForbidden, clause) {
			a.RootCause = true
			a.Reasons = append(a.Reasons, fmt.Sprintf("%s: forbidden row fires with negator %q", where, rawNeg))
		}
	}
	return a
}

// clauseTrigger returns the first denial span (forbiddenDenials, after its
// "cannot rule out" cancelling) or hedge marker of a clause, or "".
func clauseTrigger(toks []string) string {
	if spans := forbiddenDenials(toks); len(spans) > 0 {
		return strings.Join(toks[spans[0].start:spans[0].end], " ")
	}
	for _, m := range hedgeMarkers {
		if len(findTerm(toks, m)) > 0 {
			return m
		}
	}
	return ""
}

// anyNegator returns the first negator or rule-out in toks before any
// cancelling, or "".
func anyNegator(toks []string) string {
	for i := range toks {
		if l := phraseAt(toks, i, ruleOuts); l > 0 {
			return strings.Join(toks[i:i+l], " ")
		}
		if l := phraseAt(toks, i, forbiddenNegators); l > 0 {
			return strings.Join(toks[i:i+l], " ")
		}
	}
	return ""
}

// firstPresent returns the first of terms that occurs in toks, or "".
func firstPresent(toks []string, terms []string) string {
	for _, t := range terms {
		if len(findTerm(toks, t)) > 0 {
			return t
		}
	}
	return ""
}

// presentRow returns the first non-empty row whose every term occurs in toks.
func presentRow(toks []string, rows [][]string) []string {
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		all := true
		for _, t := range row {
			if len(findTerm(toks, t)) == 0 {
				all = false
				break
			}
		}
		if all {
			return row
		}
	}
	return nil
}

func flatGroups(groups [][]string) []string {
	var out []string
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}
