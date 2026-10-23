package evals

// Negation for root_cause_forbidden (task #17). A forbidden row fires when
// every one of its terms occurs in one clause at a position no denial covers.
// Before #17 any negator anywhere in the clause skipped the whole clause, so
// "the cause is the reset to latest, not the schema" told no story, "can't
// rule out X" denied X, and "ruled out X" asserted X.
//
// A term is denied by:
//   - a negator before it, up to the next contrast word ("do not reset to
//     latest", "ruled out the consumer crash"); or
//   - a predicate denial after it, within negWindow words and with no clause
//     marker (so, because, and, which, ...) between them: an auxiliary verb
//     followed by a negator ("resetting to latest is not an option",
//     "order-sync wasn't the cause", "X cannot be the cause"), or a rule-out
//     or "against" ("X was ruled out", "a reset to latest is advised against").
//
// A term is also cleared by an exoneration after it (task Q2g review, item
// 7): a copula and a verdict that the thing is in order ("Schema Registry is
// healthy", "the plugin itself loaded fine", "topic.prefix is fine", "the
// registry is untouched"), or a theory called wrong ("the network partition
// theory is wrong"). A copula exoneration reaches only the words just before
// it ("the slot was lost, Connect is fine" still tells "slot was lost"). An
// exoneration is not a negator: it does not make its clause ambiguous
// (tristate.go's clauseTrigger reads forbiddenDenials only), since the copula
// must touch the term it clears.
//
// Not denials: "not only" / "not just" ("not only X but also Y" asserts X),
// and a negated rule-out ("cannot rule out X", "X has not been ruled out"):
// the two cancel, and X stays asserted. "no longer" stays a negator here, as
// before #17 ("order-sync no longer crashes" tells no crash story).
//
// Skipping is the cautious answer for a false "told" fails a correct run,
// so anything the pre-#17 rule skipped because of a negator in scope is still
// skipped; only negators that do not reach the term stopped counting.

import (
	"regexp"
	"strings"
)

var (
	forbiddenNegators = []string{"not", "never", "no", "t", "cannot", "without", "against", "avoid*",
		"instead of", "rather than", "neither", "nor"}
	// forbiddenExonerators say that the thing named before their copula is in order.
	// No "ok" or "correct": "restarting the connector is ok" and "the
	// diagnosis that X is correct" endorse X.
	forbiddenExonerators = []string{"healthy", "fine", "untouched", "unaffected", "unchanged", "intact",
		"unrelated", "working normally", "running normally"}
	// exonerationCopulas link a subject to an exonerator: the auxiliaries
	// plus verbs of state.
	exonerationCopulas = []string{"look", "looks", "looked", "seem", "seems", "seemed", "stay", "stays",
		"stayed", "remain", "remains", "remained", "loaded", "loads"}
	// exonerationFillers may stand between the subject and its copula, or
	// between the copula and the exonerator: "the plugin itself loaded
	// fine", "the registry is still healthy".
	exonerationFillers = map[string]bool{"itself": true, "themselves": true, "still": true, "also": true,
		"all": true, "both": true, "too": true, "entirely": true, "completely": true}
	// theoryNouns before a copula and "wrong" call the story itself wrong.
	theoryNouns = []string{"theory", "story", "hypothesis", "explanation", "idea"}
	wrongWords  = []string{"wrong", "incorrect", "false", "a red herring"}
	// ruleOuts deny what follows ("ruled out X") or, as a predicate, what
	// precedes ("X was ruled out").
	ruleOuts = []string{"rule out", "rules out", "ruled out", "ruling out", "excluded", "exclude", "excludes"}
	// forbiddenContrast ends a forward denial: "not X but Y" leaves Y. "yet"
	// is left out: "not yet reset" is still a denial.
	forbiddenContrast = map[string]bool{"but": true, "however": true, "although": true, "though": true,
		"whereas": true, "actually": true}
	// auxVerbs before a negator make it a predicate denial of the subject
	// before them. Contractions arrive split: isn't -> isn t.
	auxVerbs = map[string]bool{"is": true, "s": true, "are": true, "re": true, "was": true, "were": true,
		"be": true, "been": true, "being": true, "isn": true, "aren": true, "wasn": true, "weren": true,
		"do": true, "does": true, "did": true, "don": true, "doesn": true, "didn": true,
		"has": true, "have": true, "had": true, "hasn": true, "haven": true, "hadn": true,
		"can": true, "could": true, "couldn": true, "should": true, "shouldn": true, "will": true,
		"would": true, "wouldn": true, "won": true, "may": true, "might": true, "must": true}
)

// denialSpan is one negator or rule-out in a clause: toks[start:end].
type denialSpan struct {
	start, end int
	ruleOut    bool
	// exonerate marks a copula exoneration ("is fine") or, with theory, a
	// wrong theory ("theory is wrong"): both clear only words before them.
	exonerate, theory bool
}

// forbiddenDenials lists the denials of one clause, after cancelling each
// negated rule-out together with its negator.
func forbiddenDenials(toks []string) []denialSpan {
	var spans []denialSpan
	for i := 0; i < len(toks); i++ {
		if l := phraseAt(toks, i, ruleOuts); l > 0 {
			spans = append(spans, denialSpan{start: i, end: i + l, ruleOut: true})
			i += l - 1
			continue
		}
		l := phraseAt(toks, i, forbiddenNegators)
		if l == 0 {
			continue
		}
		if toks[i] == "not" && i+1 < len(toks) && (toks[i+1] == "only" || toks[i+1] == "just") {
			continue
		}
		spans = append(spans, denialSpan{start: i, end: i + l})
		i += l - 1
	}
	// A negator up to two words before a rule-out ("cannot rule out", "can't
	// yet rule out", "not been ruled out") cancels it, and both drop.
	drop := make([]bool, len(spans))
	for r, s := range spans {
		if !s.ruleOut {
			continue
		}
		for n := r - 1; n >= 0; n-- {
			if spans[n].ruleOut || s.start-spans[n].end > 2 {
				break
			}
			drop[n], drop[r] = true, true
			break
		}
	}
	var out []denialSpan
	for i, s := range spans {
		if !drop[i] {
			out = append(out, s)
		}
	}
	return out
}

// exonerations lists the exoneration spans of one clause (see exonerationAt).
func exonerations(toks []string) []denialSpan {
	var spans []denialSpan
	for i := 0; i < len(toks); i++ {
		if sp, ok := exonerationAt(toks, i); ok {
			spans = append(spans, sp)
			i = sp.end - 1
		}
	}
	return spans
}

// exonerationAt reports whether an exoneration starts at toks[i]: a theory
// noun, a copula and a wrong word; or a copula, optional fillers and an
// exonerator not followed by "if", "except" or "until" (a conditional or
// partial verdict).
func exonerationAt(toks []string, i int) (denialSpan, bool) {
	if l := phraseAt(toks, i, theoryNouns); l > 0 && i+l < len(toks) && isCopula(toks[i+l]) {
		if w := phraseAt(toks, i+l+1, wrongWords); w > 0 {
			return denialSpan{start: i, end: i + l + 1 + w, exonerate: true, theory: true}, true
		}
	}
	if !isCopula(toks[i]) {
		return denialSpan{}, false
	}
	j := i + 1
	for j < len(toks) && exonerationFillers[toks[j]] {
		j++
	}
	l := phraseAt(toks, j, forbiddenExonerators)
	if l == 0 {
		return denialSpan{}, false
	}
	// "fine if", "fine except", "fine until" are partial verdicts; "fine to
	// drop" endorses an action.
	if k := j + l; k < len(toks) && (toks[k] == "if" || toks[k] == "except" || toks[k] == "until" || toks[k] == "to") {
		return denialSpan{}, false
	}
	return denialSpan{start: i, end: j + l, exonerate: true}, true
}

func isCopula(t string) bool {
	return auxVerbs[t] || phraseAt([]string{t}, 0, exonerationCopulas) > 0
}

// exonerationReaches reports whether the exoneration d clears the term
// toks[s:e]. A theory clears the nouns that name it ("the network partition
// theory"): at most two words between, none of them a determiner, verb or
// conjunction. A copula clears a term right before it, or one filler away,
// unless the term is the object of an action ("dropping the slot is fine",
// "restart the connector, it is fine"): then the verdict endorses the action.
func exonerationReaches(toks []string, d denialSpan, s, e int) bool {
	if d.start < e {
		return false
	}
	if d.theory {
		if d.start-e > 2 {
			return false
		}
		for i := e; i < d.start; i++ {
			if !theoryModifier(toks[i]) {
				return false
			}
		}
		return true
	}
	switch d.start - e {
	case 0:
	case 1:
		if !exonerationFillers[toks[e]] {
			return false
		}
	default:
		return false
	}
	return !actionSubject(toks, s, e)
}

// nonGerunds end in "ing" and name no action.
var nonGerunds = map[string]bool{"nothing": true, "something": true, "everything": true, "anything": true,
	"including": true, "existing": true, "remaining": true, "during": true, "string": true, "thing": true}

// theoryModifier reports whether t may stand between a term and the theory
// noun it names: a plain word, not a determiner, auxiliary, conjunction,
// negator or verb form ("-ing", "-ed").
func theoryModifier(t string) bool {
	switch t {
	case "the", "a", "an", "this", "that", "and", "or", "but", "so", "because":
		return false
	}
	return !auxVerbs[t] && !strings.HasSuffix(t, "ing") && !strings.HasSuffix(t, "ed") &&
		phraseAt([]string{t}, 0, forbiddenNegators) == 0
}

// actionSubject reports whether the term toks[s:e] is or names an action: a
// gerund in it or in the three words before it ("dropping the slot"), or a
// "to" before it ("to restart the connector").
func actionSubject(toks []string, s, e int) bool {
	for i := max(0, s-3); i < e; i++ {
		t := toks[i]
		if strings.HasSuffix(t, "ing") && !nonGerunds[t] {
			return true
		}
		if t == "to" {
			return true
		}
	}
	return false
}

// forbiddenDenied reports whether a denial in spans covers toks[s:e].
func forbiddenDenied(toks []string, spans []denialSpan, s, e int) bool {
	for _, d := range spans {
		if d.exonerate {
			if exonerationReaches(toks, d, s, e) {
				return true
			}
			continue
		}
		switch {
		case d.end <= s:
			if !contrastBetween(toks, d.end, s) {
				return true
			}
		case d.start >= e && d.start-e < negWindow:
			// A clause marker between them means the negator denies another
			// verb: "the slot has become inactive so the snapshot cannot
			// progress" still tells the inactive-slot story.
			if contrastBetween(toks, e, d.start) || markerBetween(toks, e, d.start) {
				continue
			}
			if d.ruleOut || toks[d.start] == "against" || toks[d.start] == "cannot" ||
				(d.start > 0 && auxVerbs[toks[d.start-1]]) {
				return true
			}
		}
	}
	return false
}

// predicateMarkers start a new predicate: a negator after one is not a
// predicate denial of a term before it.
var predicateMarkers = map[string]bool{"so": true, "because": true,
	"since": true, "therefore": true, "thus": true, "then": true, "while": true, "when": true, "until": true}

func markerBetween(toks []string, from, to int) bool {
	for i := from; i < to; i++ {
		if predicateMarkers[toks[i]] {
			return true
		}
	}
	return false
}

func contrastBetween(toks []string, from, to int) bool {
	for i := from; i < to; i++ {
		if forbiddenContrast[toks[i]] {
			return true
		}
	}
	return false
}

// termAsserted reports whether term occurs in toks inside comma part p (see
// commaParts) at a position no denial in spans covers.
func termAsserted(toks []string, part []int, p int, spans []denialSpan, term string) bool {
	want, _, ok := termTokens(term)
	if !ok {
		return false
	}
	for _, at := range findTerm(toks, term) {
		if part[at] == p && part[at+len(want)-1] == p && !forbiddenDenied(toks, spans, at, at+len(want)) {
			return true
		}
	}
	return false
}

// partSplit is a comma that ends a part of a clause; "1,000" is one number.
var partSplit = regexp.MustCompile(`,(\s|$)`)

// commaParts tokenises a clause and numbers each token's comma part. A comma
// ends a part only after a predicate of its own (an auxiliary verb: "nothing
// is lost, the slot is still there"); after a bare noun phrase it is a list
// or an aside ("order-sync, Connect and the registry crashed", "the
// consumer, which many suspected, crashed") and the part goes on. The tokens
// are tokenize(clause): a comma never joins two tokens.
func commaParts(clause string) (toks []string, part []int, n int) {
	cur := 0
	for _, p := range partSplit.Split(clause, -1) {
		pt := tokenize(p)
		for _, t := range pt {
			toks = append(toks, t)
			part = append(part, cur)
		}
		for _, t := range pt {
			if auxVerbs[t] {
				cur++
				break
			}
		}
	}
	if len(part) > 0 {
		n = part[len(part)-1] + 1
	}
	return toks, part, n
}

// matchForbidden reports whether text tells one of the forbidden stories: a
// row fires when all its terms are asserted (see above) in one clause. No
// rows never match. A story that is denied in one clause and told again in
// another still fires.
func matchForbidden(rows [][]string, text string) bool {
	for _, clause := range clauseSplit.Split(text, -1) {
		toks, part, n := commaParts(clause)
		if len(toks) == 0 {
			continue
		}
		// Denials reach across commas ("no lag, so reset to latest" tells
		// no story), but a row's terms must share one comma part:
		// "nothing is lost, the slot is still there" does not say "lost the
		// slot".
		spans := append(forbiddenDenials(toks), exonerations(toks)...)
		for _, row := range rows {
			if len(row) == 0 {
				continue
			}
			for p := 0; p < n; p++ {
				all := true
				for _, term := range row {
					if !termAsserted(toks, part, p, spans, term) {
						all = false
						break
					}
				}
				if all {
					return true
				}
			}
		}
	}
	return false
}
