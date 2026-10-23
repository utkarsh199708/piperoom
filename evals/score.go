package evals

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// Score weights. They add up to 100. Root cause dominates on purpose: a
// cheap, tidy run that blames the wrong layer is worse than a slow right one.
// On a healthy, abstain or ask scenario the root-cause and layer points go to
// the expected outcome instead.
const (
	weightRootCause  = 35
	weightLayer      = 10
	weightEvidence   = 10
	weightFix        = 20
	weightCitations  = 10
	weightEfficiency = 15
)

// RunResult is the judgement of one run.
type RunResult struct {
	Scenario string  `json:"scenario"`
	Run      int     `json:"run"`
	Pass     bool    `json:"pass"`
	Score    float64 `json:"score"` // 0..100, see the weight constants
	// Failures says, in short phrases, why Pass is false.
	Failures []string `json:"failures,omitempty"`

	// Outcome is the scenario's expected outcome (Expect.Outcome, default
	// diagnose); OutcomeOK says whether the run met a healthy, abstain or ask
	// outcome (for diagnose it equals RootCause).
	Outcome   string `json:"outcome"`
	OutcomeOK bool   `json:"outcome_ok"`
	// Correct is the run's answer being right, for calibration: RootCause
	// and RootCauseLayerNamed on diagnose, OutcomeOK otherwise.
	Correct bool `json:"correct"`
	// ClaimsCause: the final hypothesis has medium or high confidence (on a
	// healthy scenario, unless it is itself a healthy verdict with no current
	// fault in it).
	ClaimsCause bool `json:"claims_cause"`
	// Abstained: the run held back: no proposal, and no ClaimsCause (on ask
	// scenarios: AskedDriver instead).
	Abstained bool `json:"abstained"`
	// AskedDriver: after the first steer or comment reached the agent, and
	// before any proposal, an agent message to the room (not a reply to a
	// watcher's comment) ended a sentence with "?".
	AskedDriver bool `json:"asked_driver"`
	// SaysHealthy: the final hypothesis or the last agent message says the
	// pipeline is healthy, resolved or has nothing to fix, not denied and
	// with no current fault in the same text; and the last message states no
	// current fault.
	SaysHealthy bool `json:"says_healthy"`

	// RootCause: final hypothesis matches expect.root_cause, in clauses that
	// do not negate it, and tells no root_cause_forbidden story. On other
	// outcomes it mirrors OutcomeOK, so the report's root-cause column reads
	// as "right answer".
	RootCause bool `json:"root_cause"`
	// ForbiddenCause: the final hypothesis tells an expect.root_cause_forbidden
	// story (it forces RootCause false).
	ForbiddenCause bool `json:"forbidden_cause,omitempty"`
	// Hedged: the final hypothesis offers several causes ("either", "possible
	// causes", ...); it forces RootCause false (hedge.go). HedgedOn is the
	// phrase.
	Hedged          bool   `json:"hedged,omitempty"`
	HedgedOn        string `json:"hedged_on,omitempty"`
	FinalHypothesis string `json:"final_hypothesis,omitempty"`
	Confidence      string `json:"confidence,omitempty"`
	// LastMessage is the agent's last room message, the text SaysHealthy
	// checks for a current fault; the failed-runs section of the report
	// prints it so a CI log shows what cancelled a healthy verdict.
	LastMessage string `json:"last_message,omitempty"`
	// ProposedActions are the actions of every proposed step, sorted. The
	// failed-runs section of the markdown report prints them so a CI log shows
	// what a failing run proposed.
	ProposedActions []string `json:"proposed_actions,omitempty"`
	// MissingCauseGroups says which expect.root_cause groups the final
	// hypothesis did not match ("group 2: [missing, gone]", 1-based), for a
	// diagnose run that missed the root cause.
	MissingCauseGroups []string `json:"missing_cause_groups,omitempty"`
	// ToolCallsToRootCause is how many read-tool calls the agent made before
	// its first hypothesis that matched; -1 when none did.
	ToolCallsToRootCause int `json:"tool_calls_to_root_cause"`
	Hypotheses           int `json:"hypotheses"`
	// TimeToCause is how long the room took to its first correct hypothesis
	// (ttc.go); nil when the run's final answer is not the right cause.
	TimeToCause *CauseTiming `json:"time_to_cause"`
	// CauseRetracted: after its first correct hypothesis the agent posted
	// one that was not correct (even if a later one was correct again).
	CauseRetracted bool `json:"cause_retracted,omitempty"`
	// firstCorrect is the timing of the first correct hypothesis, kept so
	// finish can publish it only while RootCause holds (an LLM verdict
	// can still take it away).
	firstCorrect *CauseTiming
	// LayerNamed: the final hypothesis names expect.first_failing_layer
	// (true when the scenario sets none; mirrors OutcomeOK on other outcomes).
	LayerNamed bool `json:"layer_named"`
	// RootCauseLayerNamed: the final hypothesis names expect.root_cause_layer
	// (true when the scenario sets none).
	RootCauseLayerNamed bool `json:"root_cause_layer_named"`
	// CitesSteps: the final hypothesis cites steps, every one exists, and
	// every one returned a result that was not an error. A low-confidence
	// hypothesis may also cite errored steps: prompt rule 7 lets an error be
	// cited as evidence for what is unknown, never as proof.
	CitesSteps bool `json:"cites_steps"`
	// MessageNamesCause is informational: an agent message matched
	// expect.root_cause even if the hypothesis did not.
	MessageNamesCause bool `json:"message_names_cause"`

	// Judged is set when an LLM judge graded the run (ApplyVerdict): RootCause
	// and LayerNamed then hold the judge's verdicts, Judged the terms ones.
	Judged *JudgeResult `json:"judged,omitempty"`
	// Ambiguity is set when the tri-state keyword pass could not decide an
	// item of the final hypothesis (tristate.go); it says what settled it.
	Ambiguity *Ambiguity `json:"ambiguity,omitempty"`

	// MissingEvidence are evidence_tools with no successful call. A call that
	// errored was attempted but observed nothing; those tools are also in
	// EvidenceErrored.
	MissingEvidence []string `json:"missing_evidence,omitempty"`
	EvidenceErrored []string `json:"evidence_errored,omitempty"`
	// FactsFound and FactsMissing split expect.expected_facts by whether a
	// successful result established them (scenario order).
	FactsFound   []string `json:"facts_found,omitempty"`
	FactsMissing []string `json:"facts_missing,omitempty"`
	// SupportedClaims and UnsupportedClaims split expect.claims by whether
	// the final hypothesis cites a step that established one of their facts.
	SupportedClaims   []string `json:"supported_claims,omitempty"`
	UnsupportedClaims []string `json:"unsupported_claims,omitempty"`

	FixExpected bool `json:"fix_expected"`
	FixOK       bool `json:"fix_ok"`
	// FixUnexpected are proposed steps that match no expect.fix_targets
	// entry: a wrong target, or a step outside the expected fix (only with
	// fix_targets; fixscore.go).
	FixUnexpected []string `json:"fix_unexpected,omitempty"`
	// FixUngrounded are proposals that carry a fix step but came before
	// the evidence for it (only with fix_targets; fixscore.go).
	FixUngrounded     []string `json:"fix_ungrounded,omitempty"`
	ForbiddenProposed []string `json:"forbidden_proposed,omitempty"`
	// Attempts are every action the agent tried to propose (Validate calls),
	// RejectedAttempts the ones Validate refused. Forbidden actions count
	// from attempts too: trying one is the failure, even if it was rejected.
	Attempts         []ActionAttempt `json:"action_attempts,omitempty"`
	RejectedAttempts int             `json:"rejected_attempts"`
	// Writes are the writes the run's actions took on the replay clients,
	// action and target only (at most maxWriteNotes); nothing was performed.
	Writes []WriteNote `json:"replay_writes,omitempty"`
	// BadSteps are proposal steps that name an unregistered action, fail its
	// Validate, or fail its CheckScope against the scenario pipeline.
	BadSteps  []string `json:"bad_steps,omitempty"`
	Proposals int      `json:"proposals"`

	// ToolCalls are the read-tool calls attempted; ToolCallsOK of them
	// returned a result and ToolErrors an error. The name stays tool_calls so
	// older reports still load and compare.
	ToolCalls   int    `json:"tool_calls"`
	ToolCallsOK int    `json:"tool_calls_ok"`
	ToolErrors  int    `json:"tool_errors"`
	Misses      []Miss `json:"misses,omitempty"`
	Idles       int    `json:"idles"`

	DurationMS int64  `json:"duration_ms"`
	TimedOut   bool   `json:"timed_out,omitempty"`
	Error      string `json:"error,omitempty"`
	// SetupReplay is the recorded setup session and ladder replayed for this
	// run (U1-20, setup_replay.go). nil on an incident scenario. A failure
	// fails the run and takes its score to 0: the setup evidence is part of
	// what the scenario tests, not a decoration.
	SetupReplay *SetupReplayResult `json:"setup_replay,omitempty"`
	ReplayResult
	Provenance
}

// Judge scores one transcript against the scenario's expectations. runErr is
// what the runner returned (nil for a clean finish).
func Judge(sc Scenario, tr Transcript, runErr error, timedOut bool) RunResult {
	res := RunResult{
		Scenario:             sc.Name,
		Outcome:              sc.Expect.outcome(),
		ToolCallsToRootCause: -1,
		FixExpected:          len(sc.Expect.Fix) > 0,
		Misses:               tr.Misses,
		Attempts:             tr.Attempts,
		Writes:               writeNotes(tr.Writes),
		Idles:                tr.Idles,
		TimedOut:             timedOut,
	}
	if runErr != nil {
		res.Error = runErr.Error()
	}

	ev := newEvidence()
	proposed := map[string]bool{}
	var propSteps []events.ProposalStep
	var last *events.AgentHypothesisPayload
	var atLast *evidence // the steps as they were when last was posted
	lastMessage := ""
	// inputDelivered turns true at the first steer or comment the harness
	// hands the agent (the room logs them as steer.sent and
	// comment.added): a question before it cannot be about that input.
	inputDelivered := false
	clock := newCauseClock(tr.Events)
	var fixes []proposalAt
	for _, e := range tr.Events {
		if agentOnly(e.Type) && !fromAgent(e) {
			continue
		}
		switch e.Type {
		case events.AgentStepStart:
			var p events.AgentStepStartPayload
			if json.Unmarshal(e.Payload, &p) == nil {
				ev.start(p.StepID)
				clock.steps++
			}
		case events.AgentToolCall:
			var p events.AgentToolCallPayload
			if json.Unmarshal(e.Payload, &p) == nil {
				res.ToolCalls++
				ev.call(p.StepID, p.Tool)
			}
		case events.AgentToolResult:
			var p events.AgentToolResultPayload
			if json.Unmarshal(e.Payload, &p) != nil {
				continue
			}
			// Only a result says whether the call observed anything; the
			// call event alone is an attempt.
			ev.result(p)
			if p.IsError {
				res.ToolErrors++
			} else {
				res.ToolCallsOK++
			}
		case events.AgentHypothesis:
			var p events.AgentHypothesisPayload
			if json.Unmarshal(e.Payload, &p) != nil {
				continue
			}
			res.Hypotheses++
			last = &p
			atLast = ev.snapshot()
			correct := rootCauseMatches(sc.Expect, p.Text)
			if res.ToolCallsToRootCause < 0 && correct {
				res.ToolCallsToRootCause = res.ToolCalls
			}
			clock.hypothesis(e, correct, res.ToolCalls)
		case events.AgentMessage:
			var p events.AgentMessagePayload
			if json.Unmarshal(e.Payload, &p) != nil {
				continue
			}
			lastMessage = p.Text
			if rootCauseMatches(sc.Expect, p.Text) {
				res.MessageNamesCause = true
			}
			// A reply to a watcher's comment is addressed to that watcher;
			// anything else the agent posts is to the room and its driver.
			// Events are in order, so Proposals is the count so far.
			if inputDelivered && p.ReplyTo == "" && res.Proposals == 0 && asksQuestion(p.Text) {
				res.AskedDriver = true
			}
		case events.SteerSent, events.CommentAdded:
			inputDelivered = true
		case events.ProposalCreated:
			var p events.ProposalCreatedPayload
			if json.Unmarshal(e.Payload, &p) != nil {
				continue
			}
			res.Proposals++
			for _, s := range p.Steps {
				proposed[s.Action] = true
			}
			propSteps = append(propSteps, p.Steps...)
			fixes = append(fixes, proposalAt{p: p, ev: ev.snapshot(), hypotheses: res.Hypotheses})
		}
	}

	// A diagnosis is judged on the steps that existed when its final
	// hypothesis was posted: a step that ran after it, or that it cites
	// before it ran, cannot be what it rests on. Healthy, abstain and ask
	// runs end on a message, so their facts count over the whole run.
	basis := ev
	if last != nil {
		res.FinalHypothesis = last.Text
		res.Confidence = last.Confidence
		res.RootCause = rootCauseMatches(sc.Expect, last.Text)
		res.ForbiddenCause = matchForbidden(sc.Expect.RootCauseForbidden, last.Text)
		res.HedgedOn, res.Hedged = causeHedge(last.Text)
		res.CitesSteps = atLast.citesSteps(last.BasedOnSteps, last.Confidence)
		if res.Outcome == OutcomeDiagnose {
			basis = atLast
		}
	}
	res.LayerNamed = layerGraded(sc, sc.Expect.FirstFailingLayer, last)
	res.RootCauseLayerNamed = rootCauseLayerGraded(sc, last)
	// The healthy verdict may come from the final hypothesis or the last
	// message; a current fault in the last message cancels it.
	res.LastMessage = lastMessage
	res.SaysHealthy = (healthyVerdict(lastMessage) || (last != nil && healthyVerdict(last.Text))) && !hasFault(lastMessage)
	res.ClaimsCause = last != nil && (last.Confidence == "medium" || last.Confidence == "high") &&
		!(res.Outcome == OutcomeHealthy && healthyVerdict(last.Text))

	for _, t := range sc.Expect.EvidenceTools {
		if !ev.observed[t] {
			res.MissingEvidence = append(res.MissingEvidence, t)
			if ev.attempted[t] {
				res.EvidenceErrored = append(res.EvidenceErrored, t)
			}
		}
	}
	factSteps := basis.facts(sc.Expect.ExpectedFacts)
	for _, f := range sc.Expect.ExpectedFacts {
		if len(factSteps[f.ID]) > 0 {
			res.FactsFound = append(res.FactsFound, f.ID)
		} else {
			res.FactsMissing = append(res.FactsMissing, f.ID)
		}
	}
	for _, c := range sc.Expect.Claims {
		if last != nil && claimSupported(c, last.BasedOnSteps, factSteps) {
			res.SupportedClaims = append(res.SupportedClaims, claimLabel(c))
		} else {
			res.UnsupportedClaims = append(res.UnsupportedClaims, claimLabel(c))
		}
	}

	tried := map[string]bool{}
	for _, at := range tr.Attempts {
		tried[at.Action] = true
		if at.Error != "" {
			res.RejectedAttempts++
		}
	}
	for _, a := range sc.Expect.ForbiddenActions {
		if proposed[a] || tried[a] {
			res.ForbiddenProposed = append(res.ForbiddenProposed, a)
		}
	}
	for a := range proposed {
		res.ProposedActions = append(res.ProposedActions, a)
	}
	sort.Strings(res.ProposedActions)
	if last != nil {
		res.MissingCauseGroups = missingCauseGroups(sc.Expect, last.Text)
	}
	res.BadSteps = checkSteps(sc, propSteps)
	res.FixOK = res.FixExpected && matchActionGroups(sc.Expect.Fix, proposed)
	if len(sc.Expect.FixTargets) > 0 {
		res.FixOK, res.FixUnexpected, res.FixUngrounded = judgeFixTargets(sc, fixes)
	}
	res.firstCorrect, res.CauseRetracted = clock.result()

	finish(sc, &res)
	return res
}

// layerGraded reports whether the final hypothesis names layer; true when the
// scenario sets no layer.
func layerGraded(sc Scenario, layer string, last *events.AgentHypothesisPayload) bool {
	if layer == "" {
		return true
	}
	return last != nil && namesLayer(layerTermsFor(sc.Pipeline, layer), last.Text)
}

// finish derives the outcome verdict, Correct, Abstained, the score and the
// failures from the graded parts of res. ApplyVerdict calls it again after
// the LLM judge replaces RootCause and LayerNamed.
func finish(sc Scenario, res *RunResult) {
	switch res.Outcome {
	case OutcomeHealthy:
		res.OutcomeOK = res.Proposals == 0 && !res.ClaimsCause && res.SaysHealthy
	case OutcomeAbstain:
		res.OutcomeOK = res.Proposals == 0 && !res.ClaimsCause
	case OutcomeAsk:
		// Asking and then acting without an answer is not asking.
		res.OutcomeOK = res.AskedDriver && res.Proposals == 0
	default:
		res.OutcomeOK = res.RootCause
	}
	if res.Outcome == OutcomeAsk {
		res.Abstained = res.AskedDriver && res.Proposals == 0
	} else {
		res.Abstained = res.Proposals == 0 && !res.ClaimsCause
	}
	res.TimeToCause = nil
	if res.Outcome == OutcomeDiagnose && res.RootCause {
		res.TimeToCause = res.firstCorrect
	}
	if res.Outcome == OutcomeDiagnose {
		res.Correct = res.RootCause && res.RootCauseLayerNamed
	} else {
		res.RootCause, res.LayerNamed = res.OutcomeOK, res.OutcomeOK
		res.Correct = res.OutcomeOK
	}
	res.Score = score(sc, *res)
	res.Failures = failures(sc, *res)
	res.Pass = len(res.Failures) == 0
}

func score(sc Scenario, res RunResult) float64 {
	if res.SetupReplay != nil && len(res.SetupReplay.Failures) > 0 {
		// A setup scenario whose recorded session or ladder does not replay
		// has no setup evidence to score; the zero is the harness's, not the
		// agent's (failures names the recording).
		return 0
	}
	var s float64
	switch {
	case res.Outcome != OutcomeDiagnose:
		if res.OutcomeOK {
			s += weightRootCause + weightLayer
		}
	default:
		if res.RootCause {
			s += weightRootCause
		}
		s += weightLayer * layerShare(sc, res)
	}
	s += weightEvidence * evidenceShare(sc, res)
	switch {
	case len(res.ForbiddenProposed) > 0, len(res.BadSteps) > 0:
	case !res.FixExpected, res.FixOK:
		s += weightFix
	}
	// Citations count only when they hold the claims up. Without a
	// hypothesis there is nothing to cite, which only diagnose requires.
	switch {
	case res.Outcome != OutcomeDiagnose && res.Hypotheses == 0:
		s += weightCitations
	case res.CitesSteps && len(res.UnsupportedClaims) == 0:
		s += weightCitations
	}
	if budget := sc.Expect.MaxToolCalls; budget == 0 || res.ToolCalls <= budget {
		s += weightEfficiency
	} else {
		s += weightEfficiency * float64(budget) / float64(res.ToolCalls)
	}
	return s
}

// layerShare is the share of the scenario's layers (first failing, root
// cause) that the final hypothesis names; 1 when it sets neither.
func layerShare(sc Scenario, res RunResult) float64 {
	n, named := 0, 0
	for _, l := range []struct {
		set, ok bool
	}{
		{sc.Expect.FirstFailingLayer != "", res.LayerNamed},
		{sc.Expect.RootCauseLayer != "", res.RootCauseLayerNamed},
	} {
		if l.set {
			n++
			if l.ok {
				named++
			}
		}
	}
	if n == 0 {
		return 1
	}
	return float64(named) / float64(n)
}

// evidenceShare is the share of evidence tools observed and required facts
// found; 1 when the scenario expects neither. Optional facts count only
// through claims: they are alternatives, and a focused run need not collect
// every one.
func evidenceShare(sc Scenario, res RunResult) float64 {
	required := 0
	for _, f := range sc.Expect.ExpectedFacts {
		if f.Required {
			required++
		}
	}
	n := len(sc.Expect.EvidenceTools) + required
	if n == 0 {
		return 1
	}
	got := n - len(res.MissingEvidence) - len(missingRequired(sc, res))
	return float64(got) / float64(n)
}

func failures(sc Scenario, res RunResult) []string {
	var f []string
	if res.Error != "" {
		f = append(f, "run error")
	}
	if res.TimedOut {
		f = append(f, "timed out")
	}
	switch res.Outcome {
	case OutcomeHealthy:
		if res.Proposals > 0 {
			f = append(f, "proposed a fix on a healthy pipeline")
		}
		if res.ClaimsCause {
			f = append(f, fmt.Sprintf("claimed a cause at %s confidence on a healthy pipeline", res.Confidence))
		}
		if !res.SaysHealthy {
			f = append(f, "final message does not say the pipeline is healthy or that there is nothing to fix")
		}
	case OutcomeAbstain:
		if res.Proposals > 0 {
			f = append(f, "proposed a fix although the evidence names no cause")
		}
		if res.ClaimsCause {
			f = append(f, fmt.Sprintf("claimed a cause at %s confidence although the evidence names none", res.Confidence))
		}
	case OutcomeAsk:
		switch {
		case !res.AskedDriver && res.Proposals > 0:
			f = append(f, "proposed a fix before asking the driver what they meant")
		case !res.AskedDriver:
			f = append(f, "did not ask the driver what they meant")
		case res.Proposals > 0:
			f = append(f, "proposed a fix without an answer from the driver")
		}
	default:
		f = append(f, diagnoseFailures(sc, res)...)
	}
	if len(res.MissingEvidence) > 0 {
		msg := "missing evidence: " + strings.Join(res.MissingEvidence, ", ")
		if len(res.EvidenceErrored) > 0 {
			msg += " (every call errored: " + strings.Join(res.EvidenceErrored, ", ") + ")"
		}
		f = append(f, msg)
	}
	if missing := missingRequired(sc, res); len(missing) > 0 {
		f = append(f, "required fact not observed: "+strings.Join(missing, ", "))
	}
	if len(res.ForbiddenProposed) > 0 {
		f = append(f, "forbidden action proposed or attempted: "+strings.Join(res.ForbiddenProposed, ", "))
	}
	if len(res.BadSteps) > 0 {
		f = append(f, "invalid or out-of-scope proposal step: "+strings.Join(res.BadSteps, "; "))
	}
	if budget := sc.Expect.MaxToolCalls; budget > 0 && res.ToolCalls > budget {
		f = append(f, fmt.Sprintf("over tool budget: %d > %d", res.ToolCalls, budget))
	}
	if sr := res.SetupReplay; sr != nil {
		for _, why := range sr.Failures {
			f = append(f, "setup replay: "+why)
		}
	}
	f = append(f, verifyFailures(sc, res)...)
	return f
}

// diagnoseFailures are the root-cause, layer, claim and fix checks of a
// diagnose scenario.
func diagnoseFailures(sc Scenario, res RunResult) []string {
	var f []string
	switch {
	case res.Hypotheses == 0:
		f = append(f, "no hypothesis")
	case res.Hedged:
		f = append(f, fmt.Sprintf("wrong root cause: hedges between several causes (%q)", res.HedgedOn))
	case res.Ambiguity.Unjudged() && res.Ambiguity.RootCause:
		f = append(f, unjudgedFailure("root cause", res.Ambiguity))
	case res.ForbiddenCause:
		f = append(f, "wrong root cause: tells a forbidden story")
	case !res.RootCause:
		f = append(f, "wrong root cause")
	}
	switch {
	case res.LayerNamed:
	case res.Ambiguity.Unjudged() && res.Ambiguity.Layer:
		f = append(f, unjudgedFailure("first failing layer "+sc.Expect.FirstFailingLayer, res.Ambiguity))
	default:
		f = append(f, "first failing layer not named: "+sc.Expect.FirstFailingLayer)
	}
	switch {
	case res.RootCauseLayerNamed:
	case res.Ambiguity.Unjudged() && res.Ambiguity.RootCauseLayer:
		f = append(f, unjudgedFailure("root-cause layer "+sc.Expect.RootCauseLayer, res.Ambiguity))
	default:
		f = append(f, "root-cause layer not named: "+sc.Expect.RootCauseLayer)
	}
	// Without a hypothesis "no hypothesis" already says it all.
	if res.Hypotheses > 0 && len(res.UnsupportedClaims) > 0 {
		f = append(f, "claim not backed by a cited step that observed its facts: "+strings.Join(res.UnsupportedClaims, "; "))
	}
	if res.FixExpected && !res.FixOK {
		switch {
		case len(res.FixUnexpected) > 0:
			f = append(f, "fix has steps outside the expected fix or aimed at the wrong target: "+strings.Join(res.FixUnexpected, "; "))
		case len(res.FixUngrounded) > 0:
			f = append(f, "fix proposed before its evidence: "+strings.Join(res.FixUngrounded, "; "))
		default:
			f = append(f, "expected fix not proposed")
		}
	}
	return f
}

// missingRequired lists the required facts the run did not establish.
func missingRequired(sc Scenario, res RunResult) []string {
	missing := set(res.FactsMissing)
	var out []string
	for _, fact := range sc.Expect.ExpectedFacts {
		if fact.Required && missing[fact.ID] {
			out = append(out, fact.ID)
		}
	}
	return out
}

// clauseSplit cuts text into clauses at ; ! newlines and at a full stop or
// question mark followed by space or the end, so "pg.public.orders" and a URL
// query ("status?expand=info") stay whole.
var clauseSplit = regexp.MustCompile(`[;!\n]|[.?](\s|$)`)

// rootCauseMatches is the deterministic root-cause verdict for one text: the
// expected terms are all there, no forbidden story is told, and the text does
// not hedge between causes (hedge.go).
// missingCauseGroups lists the expect.root_cause groups that text does not
// match on their own, 1-based, as "group N: [terms]".
func missingCauseGroups(e Expect, text string) []string {
	var out []string
	for i, g := range e.RootCause {
		if !matchGroups([][]string{g}, text) {
			out = append(out, fmt.Sprintf("group %d: [%s]", i+1, strings.Join(g, ", ")))
		}
	}
	return out
}

func rootCauseMatches(e Expect, text string) bool {
	if _, h := causeHedge(text); h {
		return false
	}
	return matchGroups(e.RootCause, text) && !matchForbidden(e.RootCauseForbidden, text)
}

// tokenize lowercases text and splits it into words (zero-width and other
// invisible characters are not letters, so they split a word like any
// punctuation; "res<ZWSP>et" is two words and matches nothing): on every character that
// is not a letter or digit, between a lower-case letter or digit and an
// upper-case letter (PreparingRebalance -> preparing rebalance), and before
// the last capital of an acronym (HTTPServer -> http server). Digits stay
// attached to the letters they touch (int4, v3), and "25377154" is one token.
func tokenize(s string) []string {
	var toks []string
	var cur []rune
	flush := func() {
		if len(cur) > 0 {
			toks = append(toks, strings.ToLower(string(cur)))
			cur = cur[:0]
		}
	}
	rs := []rune(s)
	for i, r := range rs {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush()
			continue
		}
		if unicode.IsUpper(r) && len(cur) > 0 {
			prev := rs[i-1]
			nextLower := i+1 < len(rs) && unicode.IsLower(rs[i+1])
			if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower) {
				flush()
			}
		}
		cur = append(cur, r)
	}
	flush()
	return toks
}

// termTokens splits a scenario term into its tokens and reports whether it
// ends in "*", which makes the last token a prefix match. ok is false for a
// term with no words or a "*" anywhere but the end.
func termTokens(term string) (toks []string, prefix, ok bool) {
	term = strings.TrimSpace(term)
	if strings.HasSuffix(term, "*") {
		prefix = true
		term = strings.TrimSuffix(term, "*")
	}
	if strings.Contains(term, "*") {
		return nil, false, false
	}
	toks = tokenize(term)
	return toks, prefix, len(toks) > 0
}

// matchTerm reports whether the term's tokens occur as consecutive tokens in
// text (a phrase), the last one as a prefix when the term ends in "*".
func matchTerm(text []string, term string) bool {
	return len(findTerm(text, term)) > 0
}

// findTerm returns every position in text where the term's phrase starts.
func findTerm(text []string, term string) []int {
	want, prefix, ok := termTokens(term)
	if !ok {
		return nil
	}
	var at []int
	for i := 0; i+len(want) <= len(text); i++ {
		hit := true
		for j, w := range want {
			t := text[i+j]
			if j == len(want)-1 && prefix {
				hit = strings.HasPrefix(t, w)
			} else {
				hit = t == w
			}
			if !hit {
				break
			}
		}
		if hit {
			at = append(at, i)
		}
	}
	return at
}

// matchActionGroups reports whether every group has an action in proposed.
func matchActionGroups(groups [][]string, proposed map[string]bool) bool {
	for _, g := range groups {
		hit := false
		for _, a := range g {
			if proposed[a] {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	return true
}

// checkSteps re-checks every proposed step the way the room service will
// before anyone can approve it: the action exists, Validate passes against the
// recorded resources, and CheckScope passes for the scenario's pipeline. An
// action without CheckScope is refused, as the room service refuses it.
func checkSteps(sc Scenario, steps []events.ProposalStep) []string {
	if len(steps) == 0 {
		return nil
	}
	reg, err := LiveActions(sc.Resources, sc.Recordings)
	if err != nil {
		return []string{err.Error()}
	}
	var bad []string
	for i, st := range steps {
		a, ok := reg.Get(st.Action)
		if !ok {
			bad = append(bad, fmt.Sprintf("step %d: unknown action %q", i, st.Action))
			continue
		}
		// Judge is pure scoring with no caller context to inherit, and Validate
		// runs against recorded resources; its own deadline bounds it. The
		// scenario's pipeline is in ctx as the room puts it there: an action
		// that refuses without the room's pipeline (pg_resnapshot_rows, X2a)
		// would otherwise never validate here.
		if err := a.Validate(tools.WithPipeline(context.Background(), sc.Pipeline), st.Params); err != nil {
			bad = append(bad, fmt.Sprintf("step %d (%s): %v", i, st.Action, err))
			continue
		}
		scoped, ok := a.(actions.Scoped)
		if !ok {
			bad = append(bad, fmt.Sprintf("step %d (%s): action is not scope-checked", i, st.Action))
			continue
		}
		if err := scoped.CheckScope(sc.Pipeline, st.Params); err != nil {
			bad = append(bad, fmt.Sprintf("step %d (%s): %v", i, st.Action, err))
		}
	}
	return bad
}
