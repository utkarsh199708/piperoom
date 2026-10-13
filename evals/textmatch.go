package evals

import (
	"regexp"
	"strings"

	"piperoom/internal/owners"
)

// This file grades an agent's own words: whether a hypothesis states the
// expected root cause and names a layer, and whether a message calls the
// pipeline healthy. It is a deterministic floor, not an understanding of
// English: -judge claude (judge.go) is the authoritative grader for root
// cause and layers, and these rules aim to fail the common ways a text denies
// what it mentions without failing the common ways a correct agent writes.
//
// Text is cut in four levels:
//   - clause: clauseSplit (; ! newline, and . or ? before a space or the end).
//     A clause that ends in "?" is a question and states nothing.
//   - fragment: a clause cut at : ( ) [ ] and before a contrast word (but,
//     while, whereas, although, though, however).
//   - back segment: a fragment cut at ", and", "so" and "which". A denial
//     that follows what it denies ("... is ruled out") reaches back to the
//     start of its back segment, so a plain list ("Connect, the slot and the
//     registry are healthy") stays one unit.
//   - sub: a back segment cut at "," and "and". A denial before what it
//     denies ("not a Connect problem") reaches forward to the end of its sub.

// fragmentSplit, backSplit and subSplit cut a clause into fragments, back
// segments and subs (see above).
var (
	fragmentSplit = regexp.MustCompile(`(?i)[:()\[\]]|\s(?:but|while|whereas|although|though|however)\s`)
	backSplit     = regexp.MustCompile(`(?i),\s*and\s|,?\s+(?:so|which)\s`)
	subSplit      = regexp.MustCompile(`(?i),|\sand\s`)
)

// clauseToks is one clause's tokens with, per token, the back segment and the
// sub it sits in (ids increase along the clause).
type clauseToks struct {
	toks []string
	back []int
	sub  []int
}

// clausesOf cuts text into clauses, drops questions, and tokenizes the rest
// with their scope units.
func clausesOf(text string) []clauseToks {
	var out []clauseToks
	add := func(clause string) {
		var c clauseToks
		backID, subID := 0, 0
		for _, frag := range fragmentSplit.Split(clause, -1) {
			for _, back := range backSplit.Split(frag, -1) {
				backID++
				for _, sub := range subSplit.Split(back, -1) {
					subID++
					for _, t := range tokenize(sub) {
						c.toks = append(c.toks, t)
						c.back = append(c.back, backID)
						c.sub = append(c.sub, subID)
					}
				}
			}
		}
		if len(c.toks) > 0 {
			out = append(out, c)
		}
	}
	start := 0
	for _, m := range clauseSplit.FindAllStringIndex(text, -1) {
		if text[m[0]] != '?' {
			add(text[start:m[0]])
		}
		start = m[1]
	}
	add(text[start:])
	return out
}

// phraseAt returns the token length of the first phrase in list that starts
// at position i of toks, or 0.
func phraseAt(toks []string, i int, list []string) int {
	for _, p := range list {
		want, prefix, ok := termTokens(p)
		if !ok || i+len(want) > len(toks) {
			continue
		}
		hit := true
		for k, w := range want {
			t := toks[i+k]
			if k == len(want)-1 && prefix {
				hit = strings.HasPrefix(t, w)
			} else {
				hit = t == w
			}
			if !hit {
				break
			}
		}
		if hit {
			return len(want)
		}
	}
	return 0
}

// Denial vocabulary for root-cause and layer terms. It is narrower than
// matchForbidden's negators on purpose: "without amount", "no longer include
// amount" and "cannot deserialize the record" describe the failure, they do
// not deny it.
var (
	// clauseDenials deny everything after them to the end of the clause: "I
	// doubt that ...", "unlikely ...", "I don't think ...".
	clauseDenials = []string{"doubt*", "unlikely", "t think", "not think", "t believe", "not believe"}
	// denialNegators turn into clause denials when directly followed by a
	// clauseExtender: "not because ...", "it is not the case that ...", "not a
	// case where ...", "not that someone stopped it".
	denialNegators  = []string{"not", "t", "no", "never"}
	clauseExtenders = []string{"because", "due", "caused", "the case", "a case", "that"}
	// strongNegators deny the rest of their sub: "no amount change", "nobody
	// touched amount", "instead of the connector".
	strongNegators = []string{"no", "nobody", "neither", "nor", "instead of", "rather than"}
	// weakNegators deny the rest of their sub only when a noun phrase or "be"
	// follows ("not a Connect problem", "it can't be amount"); before a verb
	// they describe the failure ("cannot register the schema", "can't decode
	// the record", "is not active").
	weakNegators     = []string{"not", "t", "never", "cannot"}
	nounStarters     = []string{"a", "an", "the", "be", "been", "being", "any", "this", "these", "those", "related"}
	irregularPassive = []string{"read", "built", "made", "sent", "kept", "found", "run", "set", "put", "seen", "done"}
	// backDenials deny what comes before them in their back segment: "an
	// ALTER TABLE ... is ruled out", "amount isn't the problem".
	backDenials = []string{"ruled out", "not the cause", "not the problem", "not the issue", "not the culprit",
		"not to blame", "not at fault", "not involved", "t the cause", "t the problem", "t the issue",
		"t to blame", "red herring"}
	// exonerators clear a layer named before them in their back segment
	// ("Connect is healthy"), unless a negator stands just before them
	// ("Connect is not healthy").
	exonerators = []string{"healthy", "fine", "unrelated", "unaffected", "working normally", "running normally"}
)

// denied marks, per token of c, whether a denial covers it. With layers set,
// exonerators clear what precedes them too.
func denied(c clauseToks, layers bool) []bool {
	n := len(c.toks)
	neg := make([]bool, n)
	back := func(j int) {
		for i := j - 1; i >= 0 && c.back[i] == c.back[j]; i-- {
			neg[i] = true
		}
	}
	for j := 0; j < n; j++ {
		if l := phraseAt(c.toks, j, clauseDenials); l > 0 {
			for i := j + l; i < n; i++ {
				neg[i] = true
			}
		}
		if l := phraseAt(c.toks, j, denialNegators); l > 0 && phraseAt(c.toks, j+l, clauseExtenders) > 0 {
			for i := j + l; i < n; i++ {
				neg[i] = true
			}
		}
		if l := forwardDenial(c.toks, j); l > 0 {
			for i := j + l; i < n && c.sub[i] == c.sub[j]; i++ {
				neg[i] = true
			}
		}
		if phraseAt(c.toks, j, backDenials) > 0 {
			back(j)
		}
		if layers && phraseAt(c.toks, j, exonerators) > 0 && !negatorBefore(c.toks, j, 2) {
			back(j)
		}
	}
	return neg
}

// forwardDenial returns the length of a strong negator at j, or of a weak one
// followed by a noun phrase or "be" (not a passive "be read", "be decoded"),
// or 0. "no longer" and "not only/just" are not denials.
func forwardDenial(toks []string, j int) int {
	if l := phraseAt(toks, j, strongNegators); l > 0 {
		if toks[j] == "no" && j+1 < len(toks) && toks[j+1] == "longer" {
			return 0
		}
		return l
	}
	l := phraseAt(toks, j, weakNegators)
	if l == 0 || j+l >= len(toks) || phraseAt(toks, j+l, nounStarters) == 0 {
		return 0
	}
	if s := toks[j+l]; (s == "be" || s == "been" || s == "being") && j+l+1 < len(toks) {
		next := toks[j+l+1]
		if strings.HasSuffix(next, "ed") || strings.HasSuffix(next, "en") || phraseAt(toks, j+l+1, irregularPassive) > 0 {
			return 0
		}
	}
	return l
}

// negatorBefore reports whether a negator (not, n't, no, never) sits in the
// window words before position j.
func negatorBefore(toks []string, j, window int) bool {
	for i := max(0, j-window); i < j; i++ {
		if phraseAt(toks, i, denialNegators) > 0 {
			return true
		}
	}
	return false
}

// matchGroups reports whether every group has a term that occurs in text
// where no denial covers it (see denied). Terms match whole words over a
// normalised token stream (see tokenize), not as substrings: "rebalanc" does
// not match "PreparingRebalance", "rebalanc*" does. A term that starts with a
// negator ("no longer", "not avro", "never reached") is its own phrase: its
// negator does not deny it.
func matchGroups(groups [][]string, text string) bool {
	if len(groups) == 0 {
		return false
	}
	cls := clausesOf(text)
	for _, g := range groups {
		if !occursUndenied(cls, g, false) {
			return false
		}
	}
	return true
}

// occursUndenied reports whether one of terms occurs in a clause at a
// position no denial (or, with layers, no exoneration) covers.
func occursUndenied(cls []clauseToks, terms []string, layers bool) bool {
	for _, c := range cls {
		neg := denied(c, layers)
		for _, term := range terms {
			for _, i := range findTerm(c.toks, term) {
				if !neg[i] {
					return true
				}
			}
		}
	}
	return false
}

// layerTerms are the words that name each pipeline layer (events.Layers) in a
// hypothesis. Kafka itself is matched by its parts, not the word "kafka",
// because "Kafka Connect" would otherwise name both layers. Postgres also
// covers the source-side objects a root cause lives in (DDL, migrations,
// publications, table ownership, WAL).
var layerTerms = map[string][]string{
	"postgres": {"postgres*", "replication slot", "source table", "source db", "source database", "transaction*",
		"ddl", "alter table", "drop column", "migration*", "publication*", "table owner*", "wal"},
	"kafka-connect":   {"connect", "connector*", "debezium"},
	"schema-registry": {"schema registry", "registry", "subject*"},
	"kafka":           {"topic*", "broker*", "partition*", "retention", "log start"},
	"consumer":        {"consumer*"},
	"warehouse":       {"warehouse*", "sink table"},
}

// layerTermsFor adds the pipeline's own names to a layer's words: its
// consumer groups name the consumer ("order-sync is stuck"), its connectors
// Connect and its sink tables the warehouse.
func layerTermsFor(p owners.Pipeline, layer string) []string {
	terms := append([]string(nil), layerTerms[layer]...)
	switch layer {
	case "consumer":
		terms = append(terms, p.ConsumerGroups...)
	case "kafka-connect":
		terms = append(terms, p.Connectors...)
	case "warehouse":
		terms = append(terms, p.SinkTables...)
	}
	return terms
}

// namesLayer reports whether text names a layer through one of terms, at a
// position no denial or exoneration covers.
func namesLayer(terms []string, text string) bool {
	return occursUndenied(clausesOf(text), terms, true)
}

// healthyTerms say there is nothing to fix.
var healthyTerms = []string{"healthy", "recovered", "resolved", "back to normal", "all clear",
	"nothing to fix", "nothing needs fixing", "nothing needs to be fixed",
	"no fix needed", "no fix is needed", "no fix required", "no fix is required",
	"no action needed", "no action is needed", "no action required", "no action is required",
	"no action necessary", "no action is necessary", "no issues", "no problems",
	"stale alert", "stale re notification"}

// healthyNegators cancel a healthy phrase anywhere else in its sub: "there is
// no sign it is healthy", "can't confirm it is healthy", "has not recovered".
var healthyNegators = []string{"not", "t", "no", "never", "nobody", "without"}

// faultTerms are current faults. One that is affirmed, current and not a
// question cancels a healthy verdict in the same text: "Connect is healthy but
// order-sync is still stuck".
var faultTerms = []string{"stuck", "fail*", "error*", "crash*", "broken", "break*", "is down", "are down",
	"went down", "stopped", "paused", "lagging", "falling behind", "still behind", "degraded", "unhealthy"}

// faultNegators make a fault term a non-fault when they sit in the three words
// before it: "no errors", "not stuck any more", "no longer lagging",
// "nothing is broken".
var faultNegators = []string{"not", "t", "no", "never", "no longer", "nothing", "none", "without", "nobody"}

// pastMarkers in a fault's sub make it history, not a current fault: "order-sync
// was stuck until 09:24", "task 0 failed earlier and was restarted".
var pastMarkers = []string{"was", "were", "had", "earlier", "before", "until", "previously", "ago",
	"recovered", "resolved", "fixed"}

// saysHealthy reports whether text says the pipeline is healthy or that there
// is nothing to fix: a healthy phrase, not in a question, with no negator
// elsewhere in its sub and not followed by "yet"; and no bare "no" answer.
func saysHealthy(text string) bool {
	found := false
	for _, c := range clausesOf(text) {
		for j := range c.toks {
			l := phraseAt(c.toks, j, healthyTerms)
			if l == 0 {
				continue
			}
			if j+l < len(c.toks) && c.toks[j+l] == "yet" {
				continue
			}
			ok := true
			for i := range c.toks {
				if c.sub[i] == c.sub[j] && (i < j || i >= j+l) && phraseAt(c.toks, i, healthyNegators) > 0 {
					ok = false
					break
				}
			}
			found = found || ok
		}
		if bareNo(c) {
			return false
		}
	}
	return found
}

// bareNo reports whether a sub of the clause is only a negator: "Healthy: no."
func bareNo(c clauseToks) bool {
	for i := 0; i < len(c.toks); {
		j := i
		all := true
		for ; j < len(c.toks) && c.sub[j] == c.sub[i]; j++ {
			if c.toks[j] != "no" && c.toks[j] != "not" && c.toks[j] != "nope" {
				all = false
			}
		}
		if all {
			return true
		}
		i = j
	}
	return false
}

// hasFault reports whether text states a current fault (see faultTerms).
func hasFault(text string) bool {
	for _, c := range clausesOf(text) {
		for j := range c.toks {
			if phraseAt(c.toks, j, faultTerms) == 0 {
				continue
			}
			negated := false
			for i := max(0, j-3); i < j; i++ {
				if c.sub[i] == c.sub[j] && phraseAt(c.toks, i, faultNegators) > 0 {
					negated = true
				}
			}
			past := false
			for i := range c.toks {
				if c.sub[i] == c.sub[j] && phraseAt(c.toks, i, pastMarkers) > 0 {
					past = true
				}
			}
			if !negated && !past {
				return true
			}
		}
	}
	return false
}

// healthyVerdict reports whether text calls the pipeline healthy and states
// no current fault.
func healthyVerdict(text string) bool {
	return saysHealthy(text) && !hasFault(text)
}

// questionEnd matches a "?" that ends a sentence: before a space, the end, or
// closing punctuation. A "?" inside a URL query is not a question.
var questionEnd = regexp.MustCompile(`\?["')\]]*(\s|$)`)

// asksQuestion reports whether text asks a question.
func asksQuestion(text string) bool {
	return questionEnd.MatchString(text)
}
