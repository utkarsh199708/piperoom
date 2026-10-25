package agent

import (
	"regexp"
	"strings"
)

// Rule 1 of the hypothesis guard (symptomguard.go) needs a narrower question
// than KAN-408's healthyClaim (readfailure.go): not "does the text call
// anything healthy" but "does the text say the incident, or the pipeline as
// a whole, needs nothing". healthyClaim matched component findings that every
// correct downstream hypothesis contains ("the connector is healthy, so the
// problem is downstream"), and in live runs on 2026-10-25 rule 1 refused
// good-faith hypotheses about real consumer faults in 3 of 5 rooms. A
// component's health says nothing about the symptom the watcher still sees,
// so only a whole-pipeline verdict is checked against it.
//
// The matcher reuses readfailure.go's clause cutting, caveats and negators so
// both checks read a sentence the same way; only the subject test is new.

// verdictPhraseRE matches wording that is a verdict on the incident by itself,
// whatever its subject: "nothing to fix", "no action needed", "false alarm".
// A phrase followed by a component ("nothing is wrong with the connector") is
// narrowed to that component by verdictPhraseNarrowed.
var verdictPhraseRE = regexp.MustCompile(`\b(?:nothing to fix|nothing needs fixing|nothing needs to be fixed|nothing to do|` +
	`no fix needed|no fix is needed|no fix required|no fix is required|no fixes needed|` +
	`no action needed|no action is needed|no action required|no action is required|` +
	`no further action|no further actions|no actions needed|no actions required|` +
	`all clear|all good|false alarm|false positive|` +
	`no faults? found|no faults?|nothing is wrong|nothing wrong|nothing is broken|` +
	`no issues|no issues found|no problems|no problems found)\b`)

// verdictHealthyRE matches a health word that needs a subject to mean
// anything: "the pipeline is healthy" is a verdict, "the consumer is healthy"
// is a component finding.
var verdictHealthyRE = regexp.MustCompile(`\b(?:healthy|recovered|resolved|back to normal|fine|` +
	`operating normally|running normally|caught up)\b`)

// verdictNounRE finds the nouns that decide a health word's subject. Its
// matches are classified by verdictNounKind.
var verdictNounRE = regexp.MustCompile(`\b(?:all layers|all components|all systems|all checks|every layer|every component|end to end|` +
	`everything else|everything|pipelines?|system|incident|alert|room|` +
	`it|this|they|` +
	`sources?|slots?|connectors?|connect|kafka|topics?|consumers?|groups?|warehouse|sinks?|registry|schema|` +
	`upstream|downstream|tasks?|workers?|brokers?|db|database|postgres|debezium|publication|tables?|` +
	`partitions?|replicas?|replication|wal|lag|dlq|snowflake|bigquery|databricks|mysql|mongodb|offsets?|` +
	`producers?|cluster|nodes?|services?)\b`)

// nounKind is what a subject noun makes of the health word after it.
type nounKind int

const (
	nounComponent nounKind = iota
	nounWhole
	nounPronoun
)

// verdictNounKind classifies one verdictNounRE match.
func verdictNounKind(n string) nounKind {
	switch n {
	case "all layers", "all components", "all systems", "all checks", "every layer", "every component", "end to end",
		"everything", "pipeline", "pipelines", "system", "incident", "alert", "room":
		return nounWhole
	case "it", "this", "they":
		return nounPronoun
	}
	// "everything else" is everything but something: a partial claim.
	return nounComponent
}

// verdictFiller are the words a health word's subject may be made of and
// still be a bare verdict ("Healthy now.", "Verdict: recovered.").
var verdictFiller = map[string]bool{
	"now": true, "is": true, "are": true, "was": true, "were": true, "looks": true, "look": true, "seems": true,
	"seem": true, "has": true, "have": true, "been": true, "remains": true, "remain": true, "fully": true,
	"again": true, "so": true, "back": true, "all": true, "the": true, "verdict": true, "status": true,
	"conclusion": true, "summary": true, "hypothesis": true, "update": true, "result": true, "overall": true,
	"final": true, "current": true, "currently": true, "confirmed": true, "yes": true, "itself": true,
	"completely": true, "entirely": true, "and": true,
}

// verdictLinking are the verbs that say the subject sits before them: with
// one and no noun in the same sub, the subject is in an earlier sub
// ("everything, including the connector, is healthy").
var verdictLinking = map[string]bool{
	"is": true, "are": true, "was": true, "were": true, "looks": true, "look": true, "seems": true,
	"seem": true, "has": true, "have": true, "remains": true, "remain": true,
}

// verdictPrepositions narrow a verdict phrase to what follows them.
var verdictPrepositions = map[string]bool{"with": true, "in": true, "on": true, "for": true, "at": true, "from": true, "to": true, "about": true}

// verdictExtraCaveats join claimCaveats for this matcher: a contrast in the
// same clause ("healthy now, but the consumer is still down") makes it no
// clean verdict.
var verdictExtraCaveats = []string{" but ", " however", " although ", " though ", " while ", " not yet ", " still "}

// verdictCitationRE drops step citations "(s_12)" / "(steps s_3, s_4)", whose
// commas would cut a clause into subs.
var verdictCitationRE = regexp.MustCompile(`\([^()]*\bs_[a-z0-9_]+[^()]*\)`)

// verdictMarkupRE drops markdown emphasis and code marks.
var verdictMarkupRE = regexp.MustCompile("\\*\\*|__|`|~~|\\*")

// verdictBulletRE drops a list marker or heading mark at a line start.
var verdictBulletRE = regexp.MustCompile(`(?m)^[ \t]*(?:[-•+]|#{1,6}|\d+[.)])[ \t]+`)

// verdictWordRE cuts a text into words.
var verdictWordRE = regexp.MustCompile(`[a-z0-9_']+`)

// pipelineVerdictHealthy reports whether text says the incident or the whole
// pipeline needs nothing: is healthy, recovered, has nothing to fix. A health
// word whose subject is a component ("the connector is healthy") is a
// finding, not a verdict; a clause with a caveat or a contrast, a negated
// word and a question state no verdict. Rule 1 only: healthyClaim keeps
// KAN-408's wider reading.
func pipelineVerdictHealthy(text string) bool {
	t := strings.ToLower(text)
	t = verdictCitationRE.ReplaceAllString(t, "")
	t = verdictMarkupRE.ReplaceAllString(t, "")
	t = verdictBulletRE.ReplaceAllString(t, "")
	var prior strings.Builder // the text before the current clause, for pronouns
	for _, clause := range claimClauses(t) {
		if !clause.question && !verdictCaveated(clause.text) && clauseVerdict(clause.text, prior.String()) {
			return true
		}
		prior.WriteString(clause.text)
		prior.WriteString(". ")
	}
	return false
}

// verdictCaveated reports a caveat or contrast anywhere in the clause.
func verdictCaveated(clause string) bool {
	c := " " + clause + " "
	for _, w := range claimCaveats {
		if strings.Contains(c, w) {
			return true
		}
	}
	for _, w := range verdictExtraCaveats {
		if w == " still " && (strings.Contains(c, "still healthy") || strings.Contains(c, "still fine")) {
			continue
		}
		if strings.Contains(c, w) {
			return true
		}
	}
	return false
}

// clauseVerdict looks for an unnegated whole-pipeline verdict in one clause.
func clauseVerdict(clause, prior string) bool {
	subs := claimSubs(clause)
	for i, sub := range subs {
		for _, loc := range verdictPhraseRE.FindAllStringIndex(sub, -1) {
			if verdictNegated(sub, loc) {
				continue
			}
			if verdictPhraseNarrowed(sub[loc[1]:]) {
				continue
			}
			return true
		}
		for _, loc := range verdictHealthyRE.FindAllStringIndex(sub, -1) {
			if verdictNegated(sub, loc) {
				continue
			}
			earlier := prior + strings.Join(subs[:i], ", ")
			if healthySubjectWhole(sub[:loc[0]], earlier) {
				return true
			}
		}
	}
	return false
}

// verdictNegated applies healthyClaim's negation test to one match.
func verdictNegated(sub string, loc []int) bool {
	before := sub[:loc[0]]
	for _, n := range claimNegators {
		if strings.Contains(before, n) {
			return true
		}
	}
	return strings.HasPrefix(strings.TrimSpace(sub[loc[1]:]), "yet")
}

// verdictPhraseNarrowed reports a verdict phrase aimed at a component: the
// words right after it are a component ("nothing to fix upstream") or a
// preposition with a component ("nothing is wrong with the connector").
func verdictPhraseNarrowed(after string) bool {
	if i := strings.IndexAny(after, ",:;"); i >= 0 {
		after = after[:i]
	}
	words := verdictWordRE.FindAllString(after, 5)
	if len(words) == 0 {
		return false
	}
	if !verdictPrepositions[words[0]] {
		return nounKindOf(words[0]) == nounComponent
	}
	rest := strings.Join(words[1:], " ")
	if m := verdictNounRE.FindString(rest); m != "" {
		return verdictNounKind(m) == nounComponent
	}
	return false
}

// nounKindOf classifies a single word; a non-noun reads as whole (no
// narrowing).
func nounKindOf(w string) nounKind {
	if verdictNounRE.FindString(w) != w {
		return nounWhole
	}
	return verdictNounKind(w)
}

// healthySubjectWhole decides whether a health word, preceded in its sub by
// before and in the text by earlier, speaks of the whole pipeline.
func healthySubjectWhole(before, earlier string) bool {
	if nouns := verdictNounRE.FindAllString(before, -1); len(nouns) > 0 {
		last := nouns[len(nouns)-1]
		switch verdictNounKind(last) {
		case nounWhole:
			return true
		case nounComponent:
			return false
		}
		// A pronoun takes the subject of the nearest noun before it.
		i := strings.LastIndex(before, last)
		return antecedentWhole(earlier + " " + before[:i])
	}
	linking := false
	for _, w := range verdictWordRE.FindAllString(before, -1) {
		if !verdictFiller[w] {
			// A subject the matcher does not know is not a pipeline verdict.
			return false
		}
		if verdictLinking[w] {
			linking = true
		}
	}
	if !linking {
		// No subject and no verb: a bare verdict ("Healthy now.").
		return true
	}
	// "..., is healthy": the subject is in an earlier sub of the clause.
	return antecedentWhole(earlier)
}

// antecedentWhole reports whether the last subject noun of text is whole;
// with none, the pronoun or verb has only the incident to refer to.
func antecedentWhole(text string) bool {
	nouns := verdictNounRE.FindAllString(text, -1)
	for i := len(nouns) - 1; i >= 0; i-- {
		switch verdictNounKind(nouns[i]) {
		case nounWhole:
			return true
		case nounComponent:
			return false
		}
	}
	return true
}
