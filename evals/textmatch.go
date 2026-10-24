package evals

import (
	"fmt"
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
	// "ruled out the registry" too: backDenials only reach backwards.
	strongNegators = []string{"no", "nobody", "neither", "nor", "instead of", "rather than",
		"rule out", "rules out", "ruled out", "ruling out"}
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
	// conjunctionBarriers end a denial's reach, as faultNegatorBarriers end
	// a fault negator's: the clause they open has its own polarity ("the slot
	// is not the cause because the connector failed").
	conjunctionBarriers = []string{"because", "when"}
	// prepositionBarriers end it only as conjunctions, after a verb in their
	// sub: "no consumer read them before they were deleted" states the
	// deletion, while in "no events since the restart were lost" "since" is a
	// preposition and the denial reaches on (see isBarrier).
	prepositionBarriers = []string{"since", "after", "before", "until"}
	// barrierVerbs are the verbs isBarrier sees without a part-of-speech
	// tagger: auxiliaries, modals and irregular pasts, besides words ending
	// in -ed. A present-tense verb ("no events reach the warehouse since the
	// slot was dropped") is not seen, so that "since" stays a preposition.
	barrierVerbs = []string{"is", "was", "were", "are", "am", "s", "has", "had", "have", "did", "does", "do",
		"be", "been", "being", "t", "will", "would", "can", "could", "should", "may", "might", "must",
		"read", "built", "made", "sent", "kept", "found", "run", "ran", "set", "put", "seen", "saw", "done",
		"got", "went", "came", "took", "fell", "left", "began", "broke", "caught", "reset"}
)

// isBarrier reports whether toks[b] of c ends the reach of a denial whose
// negator ends before from: a conjunctionBarrier always; a
// prepositionBarrier when a verb (see barrierVerb) stands between the
// negator and it, so it opens a clause rather than a noun phrase ("no
// changes before the outage were lost"). Unless strict, a verb before the
// negator also counts when another follows the barrier ("it's not the
// registry since the connector crashed"), but not when a noun phrase ends
// the sub ("there are no errors since the connector restart").
func isBarrier(c clauseToks, from, b int, strict bool) bool {
	if phraseAt(c.toks, b, conjunctionBarriers) > 0 {
		return true
	}
	if phraseAt(c.toks, b, prepositionBarriers) == 0 {
		return false
	}
	before := false
	for i := b - 1; i >= 0 && c.sub[i] == c.sub[b]; i-- {
		if barrierVerb(c.toks, i) {
			if i >= from {
				return true
			}
			before = true
		}
	}
	if strict || !before {
		return false
	}
	for i := b + 1; i < len(c.toks) && c.sub[i] == c.sub[b]; i++ {
		if barrierVerb(c.toks, i) {
			return true
		}
	}
	return false
}

// barrierVerb reports whether toks[i] is a verb isBarrier sees: one of
// barrierVerbs ("s" only after a pronoun: "it's", not "Tuesday's"), or a
// word ending in -ed that no determiner precedes ("no unprocessed records"
// is a noun phrase, "nobody consumed the topic" a clause).
func barrierVerb(toks []string, i int) bool {
	t := toks[i]
	if t == "s" {
		return i > 0 && phraseAt(toks, i-1, pronounsBeforeS) > 0
	}
	if phraseAt(toks, i, barrierVerbs) > 0 {
		return true
	}
	return len(t) > 3 && strings.HasSuffix(t, "ed") && i > 0 && phraseAt(toks, i-1, determiners) == 0
}

// pronounsBeforeS are the words whose "'s" is "is" or "has"; determiners
// start a noun phrase, so an -ed word after one is an adjective.
var (
	pronounsBeforeS = []string{"it", "there", "that", "what", "here", "who", "he", "she"}
	determiners     = []string{"no", "the", "a", "an", "any", "all", "these", "those", "its", "their", "every", "some", "of"}
)

// Loss words state that data went away. Unlike other root-cause words, a
// negator a few words back denies them even before a verb: "records were not
// lost" says nothing was lost, where "cannot read the slot" still names the
// slot. lossQuantifiers ("nothing was lost", "none of the changes were
// lost") reach back over the whole sub, as a fault negator does
// (faultNegated); lossCounters ("zero data loss", "without any data loss")
// only the lossCounterWindow words before the loss word; lossVerbNegators
// ("were not lost", "has not yet been deleted") the lossVerbWindow words
// before it, and only across lossVerbFillers, so "the changes that order-sync
// never read were deleted" still states the deletion. None reaches past a
// barrier (isBarrier). "no" is a strong negator already (see denied).
var (
	lossWords        = []string{"lost", "loss*", "lose*", "losing", "delet*", "purg*", "expir*", "gone", "skipped", "discard*", "remov*", "dropped", "aged"}
	lossQuantifiers  = []string{"nothing", "none", "nobody"}
	lossCounters     = []string{"without", "zero"}
	lossVerbNegators = []string{"not", "t", "never"}
	// lossVerbFillers may stand between a lossVerbNegator and its loss word:
	// "has not yet been deleted", "was never actually lost". Any other word
	// is the negated verb ("never read", "weren't consumed", "can't
	// restore") and the loss word after it is affirmed.
	lossVerbFillers = []string{"be", "been", "being", "yet", "actually", "really", "ever", "even"}
)

// lossVerbWindow is how many words before a loss word a lossVerbNegator
// reaches; lossCounterWindow is the same for lossCounters ("zero data
// loss", "without any data loss", but not "zero-lag consumer still lost").
const (
	lossVerbWindow    = 3
	lossCounterWindow = 3
)

// onlyFillers reports whether every token of toks[from:to] is a lossVerbFiller.
func onlyFillers(toks []string, from, to int) bool {
	for i := from; i < to; i++ {
		if phraseAt(toks, i, lossVerbFillers) == 0 {
			return false
		}
	}
	return true
}

// lossNegated reports whether the occurrence of term at i holds a loss word
// that a negator denies (see lossWords).
func lossNegated(c clauseToks, i int, term string) bool {
	want, _, ok := termTokens(term)
	if !ok {
		return false
	}
	for k := i; k < i+len(want) && k < len(c.toks); k++ {
		if phraseAt(c.toks, k, lossWords) == 0 {
			continue
		}
		prep := -1 // nearest prepositionBarrier between a negator and the loss word
		for j := k - 1; j >= 0 && c.sub[j] == c.sub[k]; j-- {
			if phraseAt(c.toks, j, conjunctionBarriers) > 0 {
				break
			}
			if phraseAt(c.toks, j, prepositionBarriers) > 0 {
				if prep < 0 {
					prep = j
				}
				continue
			}
			if j >= i && j < i+len(want) {
				continue // the term's own words ("can no longer be replayed")
			}
			neg := phraseAt(c.toks, j, lossQuantifiers) > 0 || prep >= 0 && c.toks[j] == "no" && !affirmsDoubt(c.toks, j) ||
				k-j <= lossCounterWindow && phraseAt(c.toks, j, lossCounters) > 0 ||
				k-j <= lossVerbWindow && phraseAt(c.toks, j, lossVerbNegators) > 0 && !notAllOrOnly(c.toks, j) &&
					onlyFillers(c.toks, j+1, k)
			if neg {
				return prep < 0 || !isBarrier(c, j+1, prep, true)
			}
		}
	}
	return false
}

// notAllOrOnly reports whether the negator at j starts "not all", "not
// only" or "not just": they assert part of what follows ("not all changes
// were lost").
func notAllOrOnly(toks []string, j int) bool {
	if j+1 >= len(toks) || toks[j] != "not" {
		return false
	}
	next := toks[j+1]
	return next == "all" || next == "only" || next == "just"
}

// denied marks, per token of c, whether a denial covers it. With layers set,
// exonerators clear what precedes them in their own sub: "Connect is healthy"
// clears Connect, but "order-sync is stuck on partition 1, partitions 0 and 2
// are fine" still names the consumer and partition 1.
func denied(c clauseToks, layers bool) []bool {
	n := len(c.toks)
	neg := make([]bool, n)
	back := func(j int) {
		for i := j - 1; i >= 0 && c.back[i] == c.back[j]; i-- {
			neg[i] = true
		}
	}
	for j := 0; j < n; j++ {
		if l := phraseAt(c.toks, j, clauseDenials); l > 0 && !(j > 0 && affirmsDoubt(c.toks, j-1)) {
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
			for i := j + l; i < n && c.sub[i] == c.sub[j] && !isBarrier(c, j+l, i, false); i++ {
				neg[i] = true
			}
		}
		if phraseAt(c.toks, j, backDenials) > 0 {
			back(j)
		}
		if layers && phraseAt(c.toks, j, exonerators) > 0 && !negatorBefore(c.toks, j, 2) {
			for i := j - 1; i >= 0 && c.sub[i] == c.sub[j]; i-- {
				neg[i] = true
			}
		}
	}
	return neg
}

// forwardDenial returns the length of a strong negator at j, or of a weak one
// followed by a noun phrase or "be" (not a passive "be read", "be decoded"),
// or 0. "no longer" and "not only/just" are not denials.
func forwardDenial(toks []string, j int) int {
	if l := phraseAt(toks, j, strongNegators); l > 0 {
		if toks[j] == "no" && j+1 < len(toks) && toks[j+1] == "longer" || affirmsDoubt(toks, j) {
			return 0
		}
		// "cannot rule out the registry" keeps it a candidate.
		if strings.HasPrefix(toks[j], "rul") && (negatorBefore(toks, j, 2) || j > 0 && toks[j-1] == "cannot") {
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

// affirmsDoubt reports whether toks[j] starts "no doubt", "without doubt" or
// "no question": they affirm what follows ("there is no doubt the slot was
// dropped"), they do not deny it.
func affirmsDoubt(toks []string, j int) bool {
	if j+1 >= len(toks) || (toks[j] != "no" && toks[j] != "without") {
		return false
	}
	next := toks[j+1]
	return strings.HasPrefix(next, "doubt") || (toks[j] == "no" && next == "question")
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
	return occursUndeniedOutside(cls, terms, nil, layers)
}

// occursUndeniedOutside is occursUndenied that also skips a term found inside
// a longer occurrence of one of shadows (see layerShadows).
func occursUndeniedOutside(cls []clauseToks, terms, shadows []string, layers bool) bool {
	for _, c := range cls {
		neg := denied(c, layers)
		for _, term := range terms {
			for _, i := range findTerm(c.toks, term) {
				// The verb ("could not connect to the broker") is no Kafka Connect.
				if term == "connect" && i+1 < len(c.toks) && c.toks[i+1] == "to" {
					continue
				}
				if shadowed(c.toks, i, term, shadows) {
					continue
				}
				if !neg[i] && (layers || !lossNegated(c, i, term)) {
					return true
				}
			}
		}
	}
	return false
}

// layerTerms are the words that name each pipeline layer (events.IncidentLayers) in a
// hypothesis. Kafka itself is matched by its parts, not the word "kafka",
// because "Kafka Connect" would otherwise name both layers. Postgres also
// covers the source-side objects a root cause lives in (DDL, migrations,
// publications, table ownership, WAL).
var layerTerms = map[string][]string{
	"postgres": {"postgres*", "replication slot", "source table", "source db", "source database", "transaction*",
		"ddl", "alter table", "drop column", "migration*", "publication*", "table owner*", "wal",
		"postgre sql", // tokenize splits "PostgreSQL" into "postgre sql"
		// The other setup sources (U4d, U7d, U9d): owners.yaml has no source kind
		// for them yet, so their source database is graded as this layer.
		// Whole words only: a topic prefix or pipeline id such as "mongo" or
		// "orders-mssql" must not name the source layer.
		"sql server", "sqlserver", "mongodb", "mongo db", "mongod", "oracle"},
	"kafka-connect":   {"connect", "connector*", "debezium"},
	"schema-registry": {"schema registry", "registry", "subject*"},
	"kafka":           {"topic*", "broker*", "partition*", "retention", "log start"},
	"consumer":        {"consumer*"},
	"warehouse":       {"warehouse*", "sink table"},
	// The MySQL source (Q0 item 7, Q8j). tokenize splits "MySQL" into "my sql".
	"mysql": {"mysql*", "my sql", "binlog*", "binary log*", "gtid*"},
}

// layerTermsFor adds the pipeline's own names to a layer's words: its
// source tables name Postgres ("no SELECT on public.orders", as its sink
// tables name the warehouse), its consumer groups the consumer ("order-sync
// is stuck"), its topics (topic_prefix.source_table, "pg.public.orders/1")
// Kafka, its connectors Connect and its sink tables the warehouse. Grants and
// privileges are not Postgres words: Kafka ACL stories use them too.
func layerTermsFor(p owners.Pipeline, layer string) []string {
	terms := append([]string(nil), layerTerms[layer]...)
	switch layer {
	case "postgres":
		terms = append(terms, p.SourceTables...)
	case "consumer":
		terms = append(terms, p.ConsumerGroups...)
	case "kafka":
		terms = append(terms, topicNames(p)...)
	case "kafka-connect":
		terms = append(terms, p.Connectors...)
	case "warehouse":
		terms = append(terms, p.SinkTables...)
	}
	return terms
}

// topicNames are the pipeline's topics, topic_prefix.source_table.
func topicNames(p owners.Pipeline) []string {
	if p.TopicPrefix == "" {
		return nil
	}
	var out []string
	for _, t := range p.SourceTables {
		out = append(out, p.TopicPrefix+"."+t)
	}
	return out
}

// layerShadows are the longer names a layer's words must not be read from
// inside: a source table inside its topic ("public.orders" in
// "pg.public.orders") names Kafka, not Postgres.
func layerShadows(p owners.Pipeline, layer string) []string {
	if layer == "postgres" {
		return topicNames(p)
	}
	return nil
}

// shadowed reports whether term, found at i in toks, lies inside an
// occurrence of a longer phrase of shadows.
func shadowed(toks []string, i int, term string, shadows []string) bool {
	want, _, ok := termTokens(term)
	if !ok {
		return false
	}
	for _, s := range shadows {
		st, _, ok := termTokens(s)
		if !ok || len(st) <= len(want) {
			continue
		}
		for _, k := range findTerm(toks, s) {
			if k <= i && i+len(want) <= k+len(st) {
				return true
			}
		}
	}
	return false
}

// namesLayer reports whether text names a layer through one of terms, at a
// position no denial or exoneration covers.
func namesLayer(terms []string, text string) bool {
	return occursUndenied(clausesOf(text), terms, true)
}

// namesPipelineLayer is namesLayer with the pipeline's names
// (layerTermsFor) and without the ones inside a longer name (layerShadows).
func namesPipelineLayer(p owners.Pipeline, layer, text string) bool {
	return occursUndeniedOutside(clausesOf(text), layerTermsFor(p, layer), layerShadows(p, layer), true)
}

// healthyTerms say there is nothing to fix.
var healthyTerms = []string{"healthy", "recovered", "resolved", "back to normal", "all clear",
	"nothing to fix", "nothing needs fixing", "nothing needs to be fixed",
	"no fix needed", "no fix is needed", "no fix required", "no fix is required",
	"no action needed", "no action is needed", "no action required", "no action is required",
	"no action necessary", "no action is necessary", "no issues", "no problems",
	"stale alert", "stale re notification", "fine", "all good", "nothing is wrong", "operating normally",
	"running normally", "caught up", "nothing to do"}

// healthyCaveats in a healthy phrase's clause, or "until" after it, make it a
// conditional or partial verdict: "healthy except task 0", "healthy apart from
// partition 1", "no action needed until the DBA restores ownership", "it would
// be healthy if order-sync were restarted".
var healthyCaveats = []string{"except", "apart from", "other than", "otherwise", "would", "if"}

// healthyNegators cancel a healthy phrase anywhere else in its sub: "there is
// no sign it is healthy", "can't confirm it is healthy", "has not recovered".
var healthyNegators = []string{"not", "t", "no", "never", "nobody", "without"}

// faultTerms are current faults. One that is affirmed, current and not a
// question cancels a healthy verdict in the same text: "Connect is healthy but
// order-sync is still stuck".
var faultTerms = []string{"stuck", "fail*", "error*", "crash*", "broken", "break*", "is down", "are down",
	"went down", "stopped", "paused", "lagging", "behind", "degraded", "unhealthy", "stale", "wedged"}

// alertWords in a fault's sub make it about the alert, not the pipeline: "this
// alert is stale".
var alertWords = []string{"alert*", "notification*", "re notification*", "page"}

// shortDurations next to "behind" or "stale" are normal replication delay,
// not a fault: "warehouse 1 minute behind", "warehouse 1m stale", "stale for
// 1m". Longer ones ("stale for 3h", "stale 41m") stay faults.
var shortDurations = []string{"1 minute", "1 min", "2 minutes", "2 min", "1m", "2m", "seconds", "a few seconds"}

// delayTerms are the fault terms a short duration turns into normal delay.
// "behind" takes the duration before it; "stale" takes it on either side,
// since the warehouse freshness tool's own summary says "stale for 1m" and the
// model repeats that wording ("warehouse 1m stale", "stale for 1m", "stale 1m",
// "1 minute stale").
var delayTerms = map[string]bool{"behind": true, "stale": true}

// shortDelay reports whether the fault term at j is a delayTerm with a short
// duration right before it, or, for "stale", right after it with an optional
// "for" or "by" in between. Punctuation between them does not matter
// ("stale (1m)", "freshness: 1m stale"), as for "behind".
func shortDelay(c clauseToks, j int) bool {
	if !delayTerms[c.toks[j]] {
		return false
	}
	for i := max(0, j-3); i < j; i++ {
		if l := phraseAt(c.toks, i, shortDurations); l > 0 && i+l == j {
			return true
		}
	}
	if c.toks[j] != "stale" {
		return false
	}
	k := j + 1
	if k < len(c.toks) && (c.toks[k] == "for" || c.toks[k] == "by") {
		k++
	}
	return k < len(c.toks) && phraseAt(c.toks, k, shortDurations) > 0
}

// faultNegators make a fault term a non-fault when they sit before it in its
// sub: "no errors", "not stuck any more", "no longer lagging", "nothing is
// broken", and further back too, as a healthy phrase's negators reach its
// whole sub: "no component is currently failing", "two reads don't prove the
// restart loop has stopped" (healthy-after-recovery, run 39994729845). A
// negator after the fault term denies something else: "task 0 is failing with
// no retries left".
var faultNegators = []string{"not", "t", "no", "never", "no longer", "nothing", "none", "without", "nobody"}

// faultNegatorBarriers end a negator's reach back across a sub: in "Kafka
// never got the change because the connector is failing" the fault after
// "because" is affirmed whatever came before it.
var faultNegatorBarriers = []string{"because", "since", "after", "when"}

// exampleMarkers before a fault term in its sub make it an example of what to
// look for, not a fault the agent observed: "check the pod's restart reason,
// for example running out of memory or a failing health check".
var exampleMarkers = []string{"for example", "for instance"}

// pastMarkers in a fault's sub make it history, not a current fault: "order-sync
// was stuck until 09:24", "task 0 failed earlier and was restarted",
// "transient lag during a pod crash-loop" (transient: it did not last).
var pastMarkers = []string{"was", "were", "had", "earlier", "before", "until", "previously", "ago",
	"recovered", "resolved", "fixed", "transient*"}

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
			caveat := false
			for i := range c.toks {
				if phraseAt(c.toks, i, healthyCaveats) > 0 || (i >= j+l && c.toks[i] == "until") {
					caveat = true
				}
			}
			if caveat {
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
	_, _, found := faultClause(text)
	return found
}

// faultClause finds the first current fault in text and returns the fault
// term and the part of the sentence (the sub) that holds it, so a report can
// say what cancelled a healthy verdict: "stale" in "warehouse 41m stale".
func faultClause(text string) (term, clause string, found bool) {
	for _, c := range clausesOf(text) {
		for j := range c.toks {
			if phraseAt(c.toks, j, faultTerms) == 0 || isLabel(subToks(c, j)) {
				continue
			}
			negated := faultNegated(c, j)
			past, alert := false, false
			for i := range c.toks {
				if c.sub[i] == c.sub[j] && phraseAt(c.toks, i, pastMarkers) > 0 {
					past = true
				}
				if c.sub[i] == c.sub[j] && phraseAt(c.toks, i, alertWords) > 0 {
					alert = true
				}
			}
			// A zero count just before is no fault: "0 errors", "zero
			// failed tasks". Only directly before: "from 0 to 500 errors" is.
			// tokenize splits "10.0" into "10", "0": a zero after a number
			// is a decimal's fraction, not a zero count.
			if j > 0 && c.sub[j-1] == c.sub[j] && (c.toks[j-1] == "0" || c.toks[j-1] == "zero") &&
				!(c.toks[j-1] == "0" && j > 1 && c.sub[j-2] == c.sub[j] && isDigits(c.toks[j-2])) {
				negated = true
			}
			if shortDelay(c, j) {
				negated = true
			}
			if !negated && !past && !alert {
				return c.toks[j], strings.Join(subToks(c, j), " "), true
			}
		}
	}
	return "", "", false
}

// faultNegated reports whether a negator or an example marker stands before
// the fault term at j in its sub, with no barrier between them. "no doubt"
// and "no question" affirm what follows, they do not negate it.
func faultNegated(c clauseToks, j int) bool {
	for i := j - 1; i >= 0 && c.sub[i] == c.sub[j]; i-- {
		if phraseAt(c.toks, i, faultNegatorBarriers) > 0 {
			return false
		}
		if phraseAt(c.toks, i, exampleMarkers) > 0 {
			return true
		}
		if phraseAt(c.toks, i, faultNegators) > 0 && !affirmsDoubt(c.toks, i) {
			return true
		}
	}
	return false
}

// healthyNote explains in a few words how text grades for a healthy verdict,
// for the failed-runs section of the report: the fault that cancels it, or
// that it has no healthy phrase. Empty when text is a healthy verdict.
func healthyNote(text string) string {
	if term, clause, ok := faultClause(text); ok {
		return fmt.Sprintf("fault %q in %q", term, clause)
	}
	if !saysHealthy(text) {
		return "no healthy phrase"
	}
	return ""
}

// isDigits reports whether s is a non-empty run of ASCII digits.
func isDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}

// isLabel reports whether toks is only a field label: the prompt asks every
// hypothesis to name the "failing component" (internal/agent/prompt.go), so
// "Failing component: order-sync, caught up" names a slot, not a fault. The
// label must be a sub of its own (it ends at ":" or ","); "the failing
// component is order-sync" is still a fault.
func isLabel(toks []string) bool {
	return phraseAt(toks, 0, faultLabels) == len(toks) && len(toks) > 0
}

// faultLabels are the hypothesis field labels that contain a fault word.
var faultLabels = []string{"first failing component", "failing component", "first failing layer", "failing layer"}

// subToks returns the tokens of the sub that holds token j.
func subToks(c clauseToks, j int) []string {
	var out []string
	for i := range c.toks {
		if c.sub[i] == c.sub[j] {
			out = append(out, c.toks[i])
		}
	}
	return out
}

// healthyVerdict reports whether text calls the pipeline healthy and states
// no current fault.
func healthyVerdict(text string) bool {
	return saysHealthy(text) && !hasFault(text)
}

// questionEnd matches a "?" that ends a sentence: before a space, the end, or
// closing punctuation or markdown (" ' ) ] * _ ” ’). A "?" inside a URL query
// is not a question.
var questionEnd = regexp.MustCompile(`\?["')\]*_”’]*(\s|$)`)

// asksQuestion reports whether text asks a question.
func asksQuestion(text string) bool {
	return questionEnd.MatchString(text)
}
