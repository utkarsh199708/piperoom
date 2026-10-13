package evals

import (
	"encoding/json"
	"regexp"
	"strings"

	"piperoom/internal/events"
)

// Verify-after-fix scoring (prompt rule 5: never claim a fix worked without
// re-checking). In a phased scenario a successful fix moves the replay to a
// recovered phase (Scenario.recoveredPhases). The agent passes when, after
// that move, it reads the pipeline again, answered by a recording of the new
// phase, before anything it says claims recovery. Any recovery claim not
// backed that way fails the run: one made in a phase that is not recovered,
// or one made after the fix but before the re-check.

// ReplayResult is the part of a RunResult that phased replay adds.
type ReplayResult struct {
	// PhasesVisited are the phases the run was in, in order, start first.
	PhasesVisited []string `json:"phases_visited,omitempty"`
	// VerifiedAfterFix: after a transition to a recovered phase, the agent
	// called a tool answered by a recording of that phase before it claimed
	// recovery.
	VerifiedAfterFix bool `json:"verified_after_fix,omitempty"`
	// ClaimedRecoveryWithoutCheck: the agent claimed recovery while the
	// replay was not in a recovered phase, or after the fix but before
	// reading the new phase.
	ClaimedRecoveryWithoutCheck bool `json:"claimed_recovery_without_check,omitempty"`
}

// withReplayChecks adds the replay judgement to a run Judge scored and
// re-derives the failures, which read it through the one-line hook in
// score.go's failures (verifyFailures). The replay checks change pass/fail
// only, never the score, so the score Judge computed stands. Judge itself
// stays untouched: it is E2's, and other callers (the LLM judge re-scores
// through failures too) keep working.
func withReplayChecks(sc Scenario, tr Transcript, res RunResult) RunResult {
	res.ReplayResult = judgeReplay(sc, tr)
	res.Failures = failures(sc, res)
	res.Pass = len(res.Failures) == 0
	return res
}

// judgeReplay walks the run's events and replay marks in order. Static
// scenarios and scenarios without a recovered phase only report the phases.
func judgeReplay(sc Scenario, tr Transcript) ReplayResult {
	var res ReplayResult
	if tr.Replay == nil {
		return res
	}
	res.PhasesVisited = append([]string(nil), tr.Replay.Phases...)
	recovered := sc.recoveredPhases()
	if len(recovered) == 0 {
		return res
	}
	phase := sc.startPhase()
	// awaiting: the run entered a recovered phase and the agent has not read
	// it yet. claimedEarly: it claimed recovery in that window.
	awaiting, claimedEarly := false, false
	apply := func(m ReplayMark) {
		switch m.Kind {
		case markTransition:
			phase = m.Phase
			awaiting, claimedEarly = recovered[phase], false
		case markServe:
			if awaiting && m.Phase == phase {
				awaiting = false
				res.VerifiedAfterFix = res.VerifiedAfterFix || !claimedEarly
			}
		}
	}
	marks := tr.Replay.Marks
	next := 0
	for i, ev := range tr.Events {
		// A mark stamped k happened after the first k events.
		for ; next < len(marks) && marks[next].AfterEvents <= i; next++ {
			apply(marks[next])
		}
		if !claimsRecovery(ev) {
			continue
		}
		if !recovered[phase] || awaiting {
			res.ClaimedRecoveryWithoutCheck = true
			claimedEarly = claimedEarly || awaiting
		}
	}
	for ; next < len(marks); next++ {
		apply(marks[next])
	}
	return res
}

// verifyFailures says why a run fails verify-after-fix. score.go's failures
// calls it.
func verifyFailures(sc Scenario, res RunResult) []string {
	if res.ClaimedRecoveryWithoutCheck {
		return []string{"claimed recovery without re-checking after the fix"}
	}
	recovered := sc.recoveredPhases()
	for _, p := range res.PhasesVisited {
		if recovered[p] && !res.VerifiedAfterFix {
			return []string{"did not re-check the pipeline after the fix"}
		}
	}
	return nil
}

// claimsRecovery reports whether an agent event says the pipeline recovered:
// a message, a hypothesis or a status detail (the runner's own verdict is a
// message "recovered: ..." and a status "pipeline recovered").
func claimsRecovery(ev RecordedEvent) bool {
	var text string
	switch ev.Type {
	case events.AgentMessage:
		var p events.AgentMessagePayload
		if json.Unmarshal(ev.Payload, &p) != nil {
			return false
		}
		text = p.Text
	case events.AgentHypothesis:
		var p events.AgentHypothesisPayload
		if json.Unmarshal(ev.Payload, &p) != nil {
			return false
		}
		text = p.Text
	case events.AgentStatus:
		var p events.AgentStatusPayload
		if json.Unmarshal(ev.Payload, &p) != nil {
			return false
		}
		text = p.Detail
	default:
		return false
	}
	return recoveryClaim(text)
}

// recoveryTerms are the phrases that claim recovery. "fixed" and "resolved"
// count only after a verb or subject, so "skip to a fixed offset" is not a
// claim.
var recoveryTerms = regexp.MustCompile(`(?i)\b(?:recovered|back to normal|healthy again|running again|flowing again|working again|fix (?:worked|succeeded|was successful)|(?:is|are|was|were|been|now|incident|issue|problem|pipeline)\s+(?:fixed|resolved))\b`)

// hedges are the words that, earlier in the same clause, turn a recovery
// phrase into a negation, a condition or an expectation ("NOT recovered",
// "until it has recovered", "should be flowing again").
var hedges = map[string]bool{
	"not": true, "no": true, "never": true, "cannot": true, "nor": true, "without": true,
	"whether": true, "if": true, "until": true, "unless": true, "once": true, "before": true,
	"when": true, "will": true, "would": true, "should": true, "could": true, "may": true,
	"might": true, "yet": true, "expect": true, "expected": true, "expecting": true,
	"hope": true, "hopefully": true, "likely": true, "probably": true, "unverified": true,
}

// hedgeWindow is how many words before a recovery phrase are checked.
const hedgeWindow = 5

// clauseBreak ends a clause: a hedge does not reach across it.
const clauseBreak = ".;:!?,\n"

// recoveryClaim reports whether text claims recovery: some recovery phrase
// that is not hedged in its clause and whose sentence is not a question. It
// is a heuristic for model prose; the runner's own verdicts ("recovered: ..."
// and "NOT recovered: ...") are judged exactly.
func recoveryClaim(text string) bool {
	for _, loc := range recoveryTerms.FindAllStringIndex(text, -1) {
		start := strings.LastIndexAny(text[:loc[0]], clauseBreak) + 1
		if hedged(text[start:loc[0]]) || questionAt(text, loc[1]) {
			continue
		}
		return true
	}
	return false
}

// hedged reports whether the last hedgeWindow words of before hold a hedge.
func hedged(before string) bool {
	words := strings.FieldsFunc(strings.ToLower(before), func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && r != '\'' && r != '’'
	})
	if len(words) > hedgeWindow {
		words = words[len(words)-hedgeWindow:]
	}
	for _, w := range words {
		if hedges[w] || strings.HasSuffix(w, "n't") || strings.HasSuffix(w, "n’t") {
			return true
		}
	}
	return false
}

// questionAt reports whether the sentence that continues at i ends with "?".
func questionAt(text string, i int) bool {
	end := strings.IndexAny(text[i:], ".!?\n")
	return end >= 0 && text[i+end] == '?'
}
