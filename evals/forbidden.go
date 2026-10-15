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
//   - a predicate denial after it, within negWindow words: an auxiliary verb
//     followed by a negator ("resetting to latest is not an option",
//     "order-sync wasn't the cause", "X cannot be the cause"), or a rule-out
//     or "against" ("X was ruled out", "a reset to latest is advised against").
//
// Not denials: "not only" / "not just" ("not only X but also Y" asserts X),
// and a negated rule-out ("cannot rule out X", "X has not been ruled out"):
// the two cancel, and X stays asserted. "no longer" stays a negator here, as
// before #17 ("order-sync no longer crashes" tells no crash story).
//
// Skipping is the cautious answer for a false "told" fails a correct run,
// so anything the pre-#17 rule skipped because of a negator in scope is still
// skipped; only negators that do not reach the term stopped counting.

var (
	forbiddenNegators = []string{"not", "never", "no", "t", "cannot", "without", "against", "avoid*",
		"instead of", "rather than", "neither", "nor"}
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

// forbiddenDenied reports whether a denial in spans covers toks[s:e].
func forbiddenDenied(toks []string, spans []denialSpan, s, e int) bool {
	for _, d := range spans {
		switch {
		case d.end <= s:
			if !contrastBetween(toks, d.end, s) {
				return true
			}
		case d.start >= e && d.start-e < negWindow:
			if contrastBetween(toks, e, d.start) {
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

func contrastBetween(toks []string, from, to int) bool {
	for i := from; i < to; i++ {
		if forbiddenContrast[toks[i]] {
			return true
		}
	}
	return false
}

// termAsserted reports whether term occurs in toks at a position no denial in
// spans covers.
func termAsserted(toks []string, spans []denialSpan, term string) bool {
	want, _, ok := termTokens(term)
	if !ok {
		return false
	}
	for _, at := range findTerm(toks, term) {
		if !forbiddenDenied(toks, spans, at, at+len(want)) {
			return true
		}
	}
	return false
}

// matchForbidden reports whether text tells one of the forbidden stories: a
// row fires when all its terms are asserted (see above) in one clause. No
// rows never match. A story that is denied in one clause and told again in
// another still fires.
func matchForbidden(rows [][]string, text string) bool {
	for _, clause := range clauseSplit.Split(text, -1) {
		toks := tokenize(clause)
		if len(toks) == 0 {
			continue
		}
		spans := forbiddenDenials(toks)
		for _, row := range rows {
			if len(row) == 0 {
				continue
			}
			all := true
			for _, term := range row {
				if !termAsserted(toks, spans, term) {
					all = false
					break
				}
			}
			if all {
				return true
			}
		}
	}
	return false
}
