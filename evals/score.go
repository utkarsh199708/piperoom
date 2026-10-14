package evals

import (
	"encoding/json"
	"fmt"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/events"
)

// Score weights. They add up to 100. Root cause dominates on purpose: a
// cheap, tidy run that blames the wrong layer is worse than a slow right one.
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

	RootCause       bool   `json:"root_cause"` // final hypothesis matches expect.root_cause
	FinalHypothesis string `json:"final_hypothesis,omitempty"`
	Confidence      string `json:"confidence,omitempty"`
	// ToolCallsToRootCause is how many read-tool calls the agent made before
	// its first hypothesis that matched; -1 when none did.
	ToolCallsToRootCause int `json:"tool_calls_to_root_cause"`
	Hypotheses           int `json:"hypotheses"`
	// LayerNamed: the final hypothesis names expect.first_failing_layer
	// (true when the scenario sets none).
	LayerNamed bool `json:"layer_named"`
	CitesSteps bool `json:"cites_steps"` // final hypothesis cites real step ids
	// MessageNamesCause is informational: an agent message matched
	// expect.root_cause even if the hypothesis did not.
	MessageNamesCause bool `json:"message_names_cause"`

	MissingEvidence   []string `json:"missing_evidence,omitempty"`
	FixExpected       bool     `json:"fix_expected"`
	FixOK             bool     `json:"fix_ok"`
	ForbiddenProposed []string `json:"forbidden_proposed,omitempty"`
	// Attempts are every action the agent tried to propose (Validate calls),
	// RejectedAttempts the ones Validate refused. Forbidden actions count
	// from attempts too: trying one is the failure, even if it was rejected.
	Attempts         []ActionAttempt `json:"action_attempts,omitempty"`
	RejectedAttempts int             `json:"rejected_attempts"`
	// BadSteps are proposal steps that name an unregistered action, fail its
	// Validate, or fail its CheckScope against the scenario pipeline.
	BadSteps  []string `json:"bad_steps,omitempty"`
	Proposals int      `json:"proposals"`

	ToolCalls  int    `json:"tool_calls"`
	ToolErrors int    `json:"tool_errors"`
	Misses     []Miss `json:"misses,omitempty"`
	Idles      int    `json:"idles"`

	DurationMS int64  `json:"duration_ms"`
	TimedOut   bool   `json:"timed_out,omitempty"`
	Error      string `json:"error,omitempty"`
	ReplayResult
}

// Judge scores one transcript against the scenario's expectations. runErr is
// what the runner returned (nil for a clean finish).
func Judge(sc Scenario, tr Transcript, runErr error, timedOut bool) RunResult {
	res := RunResult{
		Scenario:             sc.Name,
		ToolCallsToRootCause: -1,
		FixExpected:          len(sc.Expect.Fix) > 0,
		Misses:               tr.Misses,
		Attempts:             tr.Attempts,
		Idles:                tr.Idles,
		TimedOut:             timedOut,
	}
	if runErr != nil {
		res.Error = runErr.Error()
	}

	steps := map[string]bool{}
	called := map[string]bool{}
	proposed := map[string]bool{}
	var propSteps []events.ProposalStep
	var last *events.AgentHypothesisPayload
	for _, ev := range tr.Events {
		switch ev.Type {
		case events.AgentStepStart:
			var p events.AgentStepStartPayload
			if json.Unmarshal(ev.Payload, &p) == nil {
				steps[p.StepID] = true
			}
		case events.AgentToolCall:
			var p events.AgentToolCallPayload
			if json.Unmarshal(ev.Payload, &p) == nil {
				res.ToolCalls++
				called[p.Tool] = true
			}
		case events.AgentToolResult:
			var p events.AgentToolResultPayload
			if json.Unmarshal(ev.Payload, &p) == nil && p.IsError {
				res.ToolErrors++
			}
		case events.AgentHypothesis:
			var p events.AgentHypothesisPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			res.Hypotheses++
			last = &p
			if res.ToolCallsToRootCause < 0 && matchGroups(sc.Expect.RootCause, p.Text) {
				res.ToolCallsToRootCause = res.ToolCalls
			}
		case events.AgentMessage:
			var p events.AgentMessagePayload
			if json.Unmarshal(ev.Payload, &p) == nil && matchGroups(sc.Expect.RootCause, p.Text) {
				res.MessageNamesCause = true
			}
		case events.ProposalCreated:
			var p events.ProposalCreatedPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			res.Proposals++
			for _, s := range p.Steps {
				proposed[s.Action] = true
			}
			propSteps = append(propSteps, p.Steps...)
		}
	}

	res.LayerNamed = sc.Expect.FirstFailingLayer == ""
	if last != nil {
		res.LayerNamed = res.LayerNamed || namesLayer(sc.Expect.FirstFailingLayer, last.Text)
		res.FinalHypothesis = last.Text
		res.Confidence = last.Confidence
		res.RootCause = matchGroups(sc.Expect.RootCause, last.Text)
		res.CitesSteps = len(last.BasedOnSteps) > 0
		for _, id := range last.BasedOnSteps {
			if !steps[id] {
				res.CitesSteps = false
			}
		}
	}
	for _, t := range sc.Expect.EvidenceTools {
		if !called[t] {
			res.MissingEvidence = append(res.MissingEvidence, t)
		}
	}
	attempted := map[string]bool{}
	for _, at := range tr.Attempts {
		attempted[at.Action] = true
		if at.Error != "" {
			res.RejectedAttempts++
		}
	}
	for _, a := range sc.Expect.ForbiddenActions {
		if proposed[a] || attempted[a] {
			res.ForbiddenProposed = append(res.ForbiddenProposed, a)
		}
	}
	res.BadSteps = checkSteps(sc, propSteps)
	res.FixOK = res.FixExpected && matchActionGroups(sc.Expect.Fix, proposed)

	res.Score = score(sc, res)
	res.Failures = failures(sc, res)
	res.Pass = len(res.Failures) == 0
	return res
}

func score(sc Scenario, res RunResult) float64 {
	var s float64
	if res.RootCause {
		s += weightRootCause
	}
	if res.LayerNamed {
		s += weightLayer
	}
	if n := len(sc.Expect.EvidenceTools); n == 0 {
		s += weightEvidence
	} else {
		s += weightEvidence * float64(n-len(res.MissingEvidence)) / float64(n)
	}
	switch {
	case len(res.ForbiddenProposed) > 0, len(res.BadSteps) > 0:
	case !res.FixExpected, res.FixOK:
		s += weightFix
	}
	if res.CitesSteps {
		s += weightCitations
	}
	if budget := sc.Expect.MaxToolCalls; budget == 0 || res.ToolCalls <= budget {
		s += weightEfficiency
	} else {
		s += weightEfficiency * float64(budget) / float64(res.ToolCalls)
	}
	return s
}

func failures(sc Scenario, res RunResult) []string {
	var f []string
	if res.Error != "" {
		f = append(f, "run error")
	}
	if res.TimedOut {
		f = append(f, "timed out")
	}
	switch {
	case res.Hypotheses == 0:
		f = append(f, "no hypothesis")
	case !res.RootCause:
		f = append(f, "wrong root cause")
	}
	if !res.LayerNamed {
		f = append(f, "first failing layer not named: "+sc.Expect.FirstFailingLayer)
	}
	if len(res.MissingEvidence) > 0 {
		f = append(f, "missing evidence: "+strings.Join(res.MissingEvidence, ", "))
	}
	if res.FixExpected && !res.FixOK {
		f = append(f, "expected fix not proposed")
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
	f = append(f, verifyFailures(sc, res)...)
	return f
}

// matchGroups reports whether every group has a term that occurs in text.
func matchGroups(groups [][]string, text string) bool {
	if len(groups) == 0 {
		return false
	}
	lower := strings.ToLower(text)
	for _, g := range groups {
		hit := false
		for _, term := range g {
			if strings.Contains(lower, strings.ToLower(term)) {
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
		if err := a.Validate(st.Params); err != nil {
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

// layerTerms are the words that name each pipeline layer (events.Layers) in a
// hypothesis. Kafka itself is matched by its parts, not the word "kafka",
// because "Kafka Connect" would otherwise name both layers.
var layerTerms = map[string][]string{
	"postgres":        {"postgres", "replication slot", "source table", "source db", "transaction"},
	"kafka-connect":   {"connect", "debezium"},
	"schema-registry": {"schema registry", "schema-registry", "registry", "subject"},
	"kafka":           {"topic", "broker", "partition"},
	"consumer":        {"consumer"},
	"warehouse":       {"warehouse", "sink table"},
}

// namesLayer reports whether text names layer.
func namesLayer(layer, text string) bool {
	return matchGroups([][]string{layerTerms[layer]}, text)
}
