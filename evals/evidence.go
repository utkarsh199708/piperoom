package evals

import (
	"strings"

	"piperoom/internal/events"
)

// stepRecord is what one read-tool step produced.
type stepRecord struct {
	tool string
	// ok: a result arrived and it was not an error. A step that only started,
	// or whose call failed, observed nothing.
	ok      bool
	summary string
}

// evidence is what a run's read-tool steps observed, built from the events in
// order.
type evidence struct {
	steps map[string]*stepRecord
	order []string // step ids in the order they first appeared
	// attempted is every tool called; observed only those with at least one
	// successful result.
	attempted map[string]bool
	observed  map[string]bool
}

func newEvidence() *evidence {
	return &evidence{steps: map[string]*stepRecord{}, attempted: map[string]bool{}, observed: map[string]bool{}}
}

func (e *evidence) step(id string) *stepRecord {
	st, ok := e.steps[id]
	if !ok {
		st = &stepRecord{}
		e.steps[id] = st
		e.order = append(e.order, id)
	}
	return st
}

// snapshot copies the steps seen so far. Judge takes one at each hypothesis,
// so the final hypothesis is judged on the steps that existed when it was
// made, not on ones that ran after it.
func (e *evidence) snapshot() *evidence {
	c := &evidence{steps: make(map[string]*stepRecord, len(e.steps)), order: append([]string(nil), e.order...),
		attempted: map[string]bool{}, observed: map[string]bool{}}
	for id, st := range e.steps {
		cp := *st
		c.steps[id] = &cp
	}
	for k, v := range e.attempted {
		c.attempted[k] = v
	}
	for k, v := range e.observed {
		c.observed[k] = v
	}
	return c
}

func (e *evidence) start(id string) { e.step(id) }

func (e *evidence) call(id, tool string) {
	e.attempted[tool] = true
	e.step(id).tool = tool
}

func (e *evidence) result(p events.AgentToolResultPayload) {
	st := e.step(p.StepID)
	st.tool = p.Tool
	st.ok = !p.IsError
	st.summary = p.Summary
	if st.ok {
		e.observed[p.Tool] = true
	}
}

// citesSteps reports whether cited is non-empty and every cited step exists
// and succeeded. At low confidence an errored step may be cited too: prompt
// rule 7 lets an error be evidence for what is unknown, never proof.
func (e *evidence) citesSteps(cited []string, confidence string) bool {
	if len(cited) == 0 {
		return false
	}
	for _, id := range cited {
		st, ok := e.steps[id]
		if !ok || (!st.ok && confidence != "low") {
			return false
		}
	}
	return true
}

// facts returns, per fact id, the set of steps that established it: the step
// ran the fact's tool, succeeded, and its summary holds every match term.
func (e *evidence) facts(facts []Fact) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, f := range facts {
		for _, id := range e.order {
			st := e.steps[id]
			if st.ok && st.tool == f.Tool && factIn(f, st.summary) {
				if out[f.ID] == nil {
					out[f.ID] = map[string]bool{}
				}
				out[f.ID][id] = true
			}
		}
	}
	return out
}

// factIn reports whether every match term of f occurs in summary, as whole
// words (matchTerm). Tool output is data, not prose, so there is no negation
// handling: "active=false" is the fact, not a denial.
func factIn(f Fact, summary string) bool {
	if len(f.Match) == 0 {
		return false
	}
	toks := tokenize(summary)
	for _, term := range f.Match {
		if !matchTerm(toks, term) {
			return false
		}
	}
	return true
}

// claimSupported reports whether one of the cited steps established one of
// the claim's facts. An errored step establishes nothing, so citing one never
// supports a claim.
func claimSupported(c Claim, cited []string, factSteps map[string]map[string]bool) bool {
	for _, id := range cited {
		for _, f := range c.Facts {
			if factSteps[f][id] {
				return true
			}
		}
	}
	return false
}

// claimLabel names a claim in a RunResult: "root_cause: f_a, f_b".
func claimLabel(c Claim) string {
	return c.About + ": " + strings.Join(c.Facts, ", ")
}
