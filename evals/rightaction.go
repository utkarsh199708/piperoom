package evals

import (
	"encoding/json"
	"fmt"
	"strings"

	"piperoom/internal/events"
)

// The "right action proposed" score (P3g-2).
//
// fix_ok asks whether every expect.fix group appears somewhere among the
// proposals. That passes a run that proposes the snapshot before the table is
// in the publication, which on a real stack snapshots nothing new. This score
// is stricter about the order: the agent's proposal steps, taken in the order
// they were proposed (proposal by proposal, step by step), must contain the
// expect.fix groups as an ordered subsequence: a step for group 1, then a
// later step for group 2, and so on. With expect.fix_targets a step counts
// for its group only when it also aims at the action's target. Other steps
// may sit in between; fix_targets already fails a run on those.
//
// It is reported next to the other scores and does not change pass, score or
// fix_ok, so the pass rate of the scenarios written before it stays what it
// was.

// RightAction is the right-action judgement of one run; nil on a run whose
// scenario expects no fix.
type RightAction struct {
	// OK: every expect.fix group was met, in order, by a proposed step.
	OK bool `json:"ok"`
	// Matched are the steps that met the groups, in group order, as
	// "<proposal id> step <i>: <action>".
	Matched []string `json:"matched,omitempty"`
	// Missing is the first group no later step met, its actions joined
	// with "|".
	Missing string `json:"missing,omitempty"`
}

// judgeRightAction walks the agent's proposals in event order and matches
// the scenario's fix groups against their steps in order. A greedy match is
// exact here: taking the earliest step that meets a group never stops a
// later group from finding its own step.
func judgeRightAction(sc Scenario, evs []RecordedEvent) *RightAction {
	if len(sc.Expect.Fix) == 0 {
		return nil
	}
	type at struct {
		label string
		step  events.ProposalStep
	}
	var steps []at
	for _, e := range evs {
		if e.Type != events.ProposalCreated || !fromAgent(e) {
			continue
		}
		var p events.ProposalCreatedPayload
		if json.Unmarshal(e.Payload, &p) != nil {
			continue
		}
		for i, st := range p.Steps {
			steps = append(steps, at{label: fmt.Sprintf("%s step %d: %s", p.ProposalID, i, st.Action), step: st})
		}
	}
	ra := &RightAction{}
	next := 0
	for _, g := range sc.Expect.Fix {
		found := -1
		for i := next; i < len(steps) && found < 0; i++ {
			if meetsGroup(sc, g, steps[i].step) {
				found = i
			}
		}
		if found < 0 {
			ra.Missing = strings.Join(g, "|")
			return ra
		}
		ra.Matched = append(ra.Matched, steps[found].label)
		next = found + 1
	}
	ra.OK = true
	return ra
}

// meetsGroup reports whether st is one of the group's actions and, with
// fix_targets, aims at that action's target.
func meetsGroup(sc Scenario, group []string, st events.ProposalStep) bool {
	for _, a := range group {
		if st.Action != a {
			continue
		}
		if len(sc.Expect.FixTargets) == 0 {
			return true
		}
		return stepTargets(sc.Expect.FixTargets, st)
	}
	return false
}
