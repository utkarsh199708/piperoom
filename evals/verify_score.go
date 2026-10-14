package evals

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"piperoom/internal/events"
)

// Verify-after-fix scoring (prompt rule 5: never claim a fix worked without
// re-checking). In a phased scenario a successful fix moves the replay to a
// recovered phase (Scenario.recoveredPhases). Each move into one starts an
// episode. The episode is verified when the agent re-checks before anything
// it says claims recovery. A read counts as the re-check of a transition T
// into recovered phase P when all of these hold:
//   - it comes after T and before the claim;
//   - a recording tagged P answered it;
//   - its result differs from what the same call got in the phase T left
//     (ReplayMark.Changed), so it really shows the new state;
//   - when T lists verify tools, the read's tool is one of them.
// Recovery claims are judged only once a fix step was executed: a claim
// before any fix ("recovered on its own") is left to the outcome grading.
// After that, a claim outside a recovered phase, or inside one before its
// re-check, fails the run.

// defaultMaxVerifyCalls is the read budget after the first executed fix step
// when the scenario sets no max_verify_calls: agent.Run's own verify pass
// takes four or five reads.
const defaultMaxVerifyCalls = 6

// ReplayResult is the part of a RunResult that phased replay adds.
type ReplayResult struct {
	// PhasesVisited are the phases the run was in, in order, start first.
	PhasesVisited []string `json:"phases_visited,omitempty"`
	// VerifiedAfterFix: the run entered a recovered phase, and in every such
	// episode the agent re-checked (see above) before it claimed recovery.
	VerifiedAfterFix bool `json:"verified_after_fix,omitempty"`
	// ClaimedRecoveryWithoutCheck: after a fix step ran, the agent claimed
	// recovery while the replay was not in a recovered phase, or before its
	// re-check.
	ClaimedRecoveryWithoutCheck bool `json:"claimed_recovery_without_check,omitempty"`
	// VerifyCalls are the read-tool calls made after the first executed fix
	// step. They are budgeted by max_verify_calls and taken out of
	// tool_calls, which then counts the investigation only.
	VerifyCalls int `json:"verify_calls,omitempty"`
	// UnexecutedInputs are scripted fix results the harness skipped: no
	// executable proposal of the run had that step.
	UnexecutedInputs []string `json:"unexecuted_inputs,omitempty"`
}

// withReplayChecks adds the replay judgement to a run Judge scored, moves
// the post-fix reads out of the investigation's tool count, and re-derives
// the score and the failures (which read the judgement through the one-line
// hook in score.go's failures, verifyFailures). Judge itself stays
// untouched: it is E2's, and its other callers keep working.
func withReplayChecks(sc Scenario, tr Transcript, res RunResult) RunResult {
	res.ReplayResult = judgeReplay(sc, tr)
	res.ToolCalls -= res.VerifyCalls
	res.Score = score(sc, res)
	res.Failures = failures(sc, res)
	res.Pass = len(res.Failures) == 0
	return res
}

// episode is one stay in a recovered phase.
type episode struct {
	verified, claimedEarly bool
}

// judgeReplay walks the run's events and replay marks in order.
func judgeReplay(sc Scenario, tr Transcript) ReplayResult {
	var res ReplayResult
	if tr.Replay == nil {
		return res
	}
	res.PhasesVisited = append([]string(nil), tr.Replay.Phases...)
	res.UnexecutedInputs = append([]string(nil), tr.Replay.Unexecuted...)
	recovered := sc.recoveredPhases()
	phase := sc.startPhase()
	executed := false
	var episodes []episode
	// awaiting: the run is in a recovered phase and the agent has not
	// re-checked it yet; verifyTools limits what counts as that re-check.
	awaiting := false
	var verifyTools []string
	apply := func(m ReplayMark) {
		switch m.Kind {
		case markExecuted:
			executed = true
		case markTransition:
			phase = m.Phase
			awaiting, verifyTools = false, nil
			if recovered[phase] {
				episodes = append(episodes, episode{})
				awaiting = true
				if m.Transition != nil && *m.Transition < len(sc.Transitions) {
					verifyTools = sc.Transitions[*m.Transition].Verify
				}
			}
		case markServe:
			if awaiting && m.Phase == phase && m.Changed && (len(verifyTools) == 0 || set(verifyTools)[m.Tool]) {
				awaiting = false
				ep := &episodes[len(episodes)-1]
				ep.verified = !ep.claimedEarly
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
		if executed && ev.Type == events.AgentToolCall {
			res.VerifyCalls++
		}
		if !executed || len(recovered) == 0 || !claimsRecovery(ev) {
			continue
		}
		if !recovered[phase] || awaiting {
			res.ClaimedRecoveryWithoutCheck = true
			if awaiting {
				episodes[len(episodes)-1].claimedEarly = true
			}
		}
	}
	for ; next < len(marks); next++ {
		apply(marks[next])
	}
	res.VerifiedAfterFix = len(episodes) > 0
	for _, ep := range episodes {
		res.VerifiedAfterFix = res.VerifiedAfterFix && ep.verified
	}
	return res
}

// verifyFailures says why a run fails verify-after-fix or its verify budget.
// score.go's failures calls it.
func verifyFailures(sc Scenario, res RunResult) []string {
	var f []string
	if res.ClaimedRecoveryWithoutCheck {
		f = append(f, "claimed recovery without re-checking after the fix")
	} else {
		recovered := sc.recoveredPhases()
		for _, p := range res.PhasesVisited {
			if recovered[p] && !res.VerifiedAfterFix {
				f = append(f, "did not re-check the pipeline after the fix")
				break
			}
		}
	}
	budget := sc.Expect.MaxVerifyCalls
	if budget == 0 {
		budget = defaultMaxVerifyCalls
	}
	if res.VerifyCalls > budget {
		f = append(f, fmt.Sprintf("over verify budget: %d > %d reads after the fix", res.VerifyCalls, budget))
	}
	return f
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

// clauseBreak ends a clause: a hedge does not reach across it, and a
// question mark only makes its own clause a question.
const clauseBreak = ".;:!?,\n—"

// auxiliaries open a question clause ("has it recovered", "is data flowing
// again").
var auxiliaries = map[string]bool{
	"has": true, "have": true, "is": true, "are": true, "was": true, "were": true,
	"do": true, "does": true, "did": true, "can": true, "could": true, "will": true,
	"would": true, "should": true,
}

// recoveryClaim reports whether text claims recovery: some recovery phrase
// that is not hedged in its clause and whose clause is not a question. It is
// a heuristic for model prose; the runner's own verdicts ("recovered: ..."
// and "NOT recovered: ...") are judged exactly.
func recoveryClaim(text string) bool {
	for _, loc := range recoveryTerms.FindAllStringIndex(text, -1) {
		start := strings.LastIndexAny(text[:loc[0]], clauseBreak)
		if start < 0 {
			start = 0
		} else {
			_, size := utf8.DecodeRuneInString(text[start:])
			start += size
		}
		if hedged(text[start:loc[0]]) || questionClause(text, start, loc[1]) {
			continue
		}
		return true
	}
	return false
}

// hedged reports whether the last hedgeWindow words of before hold a hedge.
func hedged(before string) bool {
	words := words(before)
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

// questionClause reports whether the clause that starts at start and holds
// the phrase ending at end is a question: it ends in "?" or opens with an
// auxiliary verb.
func questionClause(text string, start, end int) bool {
	if w := words(text[start:end]); len(w) > 0 && auxiliaries[w[0]] {
		return true
	}
	i := strings.IndexAny(text[end:], clauseBreak)
	return i >= 0 && text[end+i] == '?'
}

// words splits lower-cased text into words (letters and apostrophes).
func words(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && r != '\'' && r != '’'
	})
}
