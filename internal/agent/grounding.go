package agent

import (
	"sort"
	"strconv"
	"strings"

	"piperoom/internal/events"
)

const (
	// maxUnbackedTerms and maxUnbackedTermBytes bound events.AgentHypothesisPayload.UnbackedTerms.
	maxUnbackedTerms     = 20
	maxUnbackedTermBytes = 80
)

// groundTerms returns the identifier-like names and numbers in text that none
// of the cited step summaries show. It marks, it never refuses: a hypothesis is
// a claim about specific evidence, so a term that only lives in an uncited
// step is unbacked until the agent cites that step. The matching rules are
// the hand-off check's (candidateNames, candidateNumbers: dates, durations,
// percentages, approximations and magnitudes are exempt; a byte size only has
// to be within 10 % of a number in the steps), against the cited summaries
// only. Terms are de-duplicated, in text order, at most maxUnbackedTerms, each
// cut to maxUnbackedTermBytes on a rune boundary.
func groundTerms(text string, cited []string) []string {
	idx := textIndex(strings.Join(cited, "\n"))
	low := strings.ToLower(text)

	type found struct {
		term string
		pos  int
	}
	var all []found
	seen := map[string]bool{}
	add := func(term string) {
		k := strings.ToLower(term)
		if seen[k] {
			return
		}
		seen[k] = true
		pos := strings.Index(low, k)
		if pos < 0 {
			pos = len(low)
		}
		all = append(all, found{term, pos})
	}
	for _, name := range candidateNames(text) {
		if !strings.Contains(idx.text, strings.ToLower(name)) {
			add(name)
		}
	}
	for _, n := range candidateNumbers(text) {
		ok := idx.hasNumber(n.value)
		if !ok && n.size {
			ok = idx.hasNear(n.value)
		}
		if !ok {
			add(n.value)
		}
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].pos < all[j].pos })
	var out []string
	for _, f := range all {
		if len(out) == maxUnbackedTerms {
			break
		}
		out = append(out, cutBytes(f.term, maxUnbackedTermBytes))
	}
	return out
}

// textIndex builds the part of a logIndex that name and number matching uses,
// from plain text.
func textIndex(text string) logIndex {
	idx := logIndex{text: strings.ToLower(text)}
	idx.textNC = thousandsGroup.ReplaceAllStringFunc(idx.text, func(m string) string { return strings.ReplaceAll(m, ",", "") })
	idx.nums = numbersIn(idx.textNC)
	return idx
}

// cutBytes shortens s to at most n bytes on a rune boundary.
func cutBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && s8(s[n]) {
		n--
	}
	return s[:n]
}

// unbackedNote is the sentence appended to the hypothesis summary so the model
// can fix itself on its next call. "" when every term is backed.
func unbackedNote(terms []string) string {
	if len(terms) == 0 {
		return ""
	}
	quoted := make([]string, len(terms))
	for i, t := range terms {
		quoted[i] = "`" + strings.ReplaceAll(t, "`", "'") + "`"
	}
	verb := "terms are"
	if len(terms) == 1 {
		verb = "term is"
	}
	return "; " + strconv.Itoa(len(terms)) + " " + verb + " not in the cited steps: " + strings.Join(quoted, ", ") +
		". Cite the step that shows them or drop them."
}

// toEventViolations converts hand-off violations to their wire form, capped at
// maxHandoffViolations with each Detail cut to maxViolationDetailBytes. It
// returns a non-nil slice so "checked and clean" differs from "not checked".
func toEventViolations(vs []HandoffViolation) []events.HandoffViolation {
	out := make([]events.HandoffViolation, 0, len(vs))
	for _, v := range vs {
		if len(out) == maxHandoffViolations {
			break
		}
		out = append(out, events.HandoffViolation{Rule: v.Rule, Field: v.Field, Detail: cutBytes(v.Detail, maxViolationDetailBytes)})
	}
	return out
}

const (
	maxHandoffViolations    = 20
	maxViolationDetailBytes = 200
)
