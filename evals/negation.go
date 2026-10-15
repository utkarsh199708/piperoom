package evals

import (
	"regexp"
	"strings"
)

// Negation for agreement scoring. One place holds the negators, the words
// that end a negation's reach, the words that clear a term, and the window.
//
// forbidden.go's matchForbidden does not call this: it asks "is this wrong
// story told?", where a false yes fails a correct run, so it keeps "no longer"
// as a negator and lets a negator reach to the next contrast word, not a
// fixed window. Agreement asks "is this label term stated?", where a missed
// negation is the unsafe error, so it looks at the words around each term and
// reads "no longer carry amount" as a fact.

// clauseSplitFine adds commas and dashes to score.go's clause split, so a
// denial can stay in its own clause: "not the importer, it is the poll timeout".
var clauseSplitFine = regexp.MustCompile(`[,—–]`)

var (
	// contrastWords end a negation's reach: "not X but Y" leaves Y positive.
	contrastWords = map[string]bool{"but": true, "however": true, "actually": true, "although": true, "though": true, "whereas": true, "yet": true}
	// clearedWords after a term clear it: "Connect is healthy".
	clearedWords = map[string]bool{"healthy": true, "fine": true, "ok": true, "okay": true, "normal": true, "unaffected": true,
		"unrelated": true, "irrelevant": true, "excluded": true, "ruled": true, "innocent": true, "stable": true}
	// backRefs after a contrast word keep the clearing scan going: "the task
	// failed but it was fine" is about the task; "...but the consumer is fine"
	// is not.
	backRefs = map[string]bool{"it": true, "this": true, "that": true, "they": true, "he": true, "she": true}
	// verdictNouns end a "is not the problem" predicate.
	verdictNouns = map[string]bool{"problem": true, "cause": true, "issue": true, "culprit": true, "reason": true, "answer": true, "one": true, "it": true}
	beVerbs      = map[string]bool{"is": true, "was": true, "are": true, "were": true, "isn": true, "wasn": true, "s": true}
)

// negWindow is how many words a negator or clearing word reaches.
const negWindow = 5

// clause is one comma/semicolon-delimited piece of a hypothesis plus what
// its sentence says about it from outside.
type clause struct {
	toks []string
	// carryNeg: an earlier clause of the sentence ended on a negator
	// ("It is not, as the on-call assumed, the compat mode"), which still
	// reaches this clause up to its first contrast word.
	carryNeg bool
	// laterNeg: a later clause of the sentence is a bare predicate denial
	// ("The bulk import, which many suspected, is not the problem").
	laterNeg bool
}

// clauseTokens tokenises text into clauses and resolves, per sentence, the
// negations that reach across commas.
func clauseTokens(text string) []clause {
	var out []clause
	for _, sent := range clauseSplit.Split(text, -1) {
		start := len(out)
		for _, f := range clauseSplitFine.Split(sent, -1) {
			if t := tokenize(f); len(t) > 0 {
				out = append(out, clause{toks: t})
			}
		}
		sc := out[start:]
		pending := false
		for i := range sc {
			sc[i].carryNeg = pending
			pending = (pending && !hasContrast(sc[i].toks)) || endsOnNegator(sc[i].toks)
		}
		for j := range sc {
			if clearsBackRef(sc[j].toks) && j > 0 {
				// "The task failed, but this is fine now": the clearing
				// word is about the clause before the comma.
				sc[j-1].laterNeg = true
			}
			if !barePredicateDenial(sc[j].toks) {
				continue
			}
			// The denial is about its own subject only: walk back over
			// relative or parenthetical clauses ("which was restarted"),
			// mark the clause that holds the subject, and stop, so a true
			// cause stated earlier stays positive.
			for i := j - 1; i >= 0 && !hasContrast(sc[i].toks); i-- {
				sc[i].laterNeg = true
				if !relativeStart[sc[i].toks[0]] {
					break
				}
			}
		}
	}
	return out
}

// relativeStart opens a clause that hangs on the subject before it.
var relativeStart = map[string]bool{"which": true, "who": true, "whose": true, "that": true, "as": true, "where": true}

// clearsBackRef: "but this is fine now", a contrast word, a back-reference
// and a clearing word close behind.
func clearsBackRef(toks []string) bool {
	if len(toks) < 3 || !contrastWords[toks[0]] || !backRefs[toks[1]] {
		return false
	}
	for i := 2; i < len(toks) && i < 2+negWindow; i++ {
		if clearedWords[toks[i]] {
			return true
		}
	}
	return false
}

func hasContrast(toks []string) bool {
	for _, t := range toks {
		if contrastWords[t] {
			return true
		}
	}
	return false
}

// endsOnNegator: a negator in the last two words ("It is not,").
func endsOnNegator(toks []string) bool {
	for i := len(toks) - 1; i >= 0 && i >= len(toks)-2; i-- {
		if negatorAt(toks, i) {
			return true
		}
	}
	return false
}

// barePredicateDenial: a clause with no subject of its own that denies a
// verdict: "is not the problem", "wasn't the cause".
func barePredicateDenial(toks []string) bool {
	if len(toks) == 0 || !beVerbs[toks[0]] {
		return false
	}
	neg := false
	for i := 1; i < len(toks) && i < 6; i++ {
		neg = neg || negatorAt(toks, i)
		if neg && verdictNouns[toks[i]] {
			return true
		}
	}
	return false
}

// negatorAt reports whether toks[i] is a negator in its context.
func negatorAt(toks []string, i int) bool {
	next := ""
	if i+1 < len(toks) {
		next = toks[i+1]
	}
	switch t := toks[i]; {
	case t == "no":
		return next != "longer" // "no longer carry amount" states a fact
	case t == "not" && (next == "only" || next == "just"):
		return false // "not only X but also Y" asserts X
	case t == "not", t == "never", t == "t", t == "without", t == "against", t == "neither", t == "nor", strings.HasPrefix(t, "avoid"):
		return true
	case t == "instead":
		return next == "of"
	case t == "rather":
		return next == "than"
	}
	return false
}

// occurrence is one place a term occurs and whether a negation reaches it.
type occurrence struct {
	start, end int
	negated    bool
}

// findAgreeTerm lists the occurrences of term in one clause. A term is negated by
// a negator in the negWindow words before it or a clearing word in the
// negWindow words after it (neither scan passes a contrast word, except that
// the clearing scan follows "but it ..."), by a negator that ended an earlier
// clause of the sentence, or by a later bare denial clause.
func findAgreeTerm(c clause, term string) []occurrence {
	toks := c.toks
	want, prefix, ok := termTokens(term)
	if !ok {
		return nil
	}
	firstContrast := len(toks)
	for i, t := range toks {
		if contrastWords[t] {
			firstContrast = i
			break
		}
	}
	var out []occurrence
	for i := 0; i+len(want) <= len(toks); i++ {
		hit := true
		for j, w := range want {
			if j == len(want)-1 && prefix {
				hit = strings.HasPrefix(toks[i+j], w)
			} else {
				hit = toks[i+j] == w
			}
			if !hit {
				break
			}
		}
		if !hit {
			continue
		}
		o := occurrence{start: i, end: i + len(want)}
		o.negated = c.laterNeg || (c.carryNeg && i < firstContrast)
		for k := i - 1; !o.negated && k >= 0 && k >= i-negWindow; k-- {
			if contrastWords[toks[k]] {
				break
			}
			o.negated = negatorAt(toks, k)
		}
		for k, seen := o.end, 0; !o.negated && k < len(toks) && seen < negWindow; k, seen = k+1, seen+1 {
			if contrastWords[toks[k]] {
				if k+1 < len(toks) && backRefs[toks[k+1]] {
					continue
				}
				break
			}
			o.negated = clearedWords[toks[k]]
		}
		out = append(out, o)
	}
	return out
}

// termHits reports whether term occurs un-negated in any clause (hit) and
// whether it occurs at all only negated (neg).
func termHits(clauses []clause, term string) (hit, neg bool) {
	for _, c := range clauses {
		for _, o := range findAgreeTerm(c, term) {
			if o.negated {
				neg = true
			} else {
				return true, false
			}
		}
	}
	return false, neg
}
