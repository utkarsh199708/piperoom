package evals

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/events"
)

// The fix path, with expect.fix_targets (README "Fix targets").
//
// expect.fix names actions; that alone rewards a proposal that restarts the
// wrong task, or one that lists every action it can think of. fix_targets
// says, per action, what the step must aim at: params the step must contain,
// matched like a recording's input (every listed key present with an equal
// value; one target per action). With fix_targets set:
//
//   - every action of every expect.fix group has a target, and an action
//     counts toward its group only through a step that matches its target;
//   - every proposed step must match some target. An action in fix_targets
//     but in no fix group is an allowed extra (a "restore compatibility"
//     step); anything else is unexpected and fails the fix;
//   - every proposal holding a fix step must come after the evidence: a
//     hypothesis was posted and every required expected_fact was
//     established by a step that ran before the proposal. A correct fix
//     guessed from the alert, before any evidence, is not a found fix, and
//     approvers would have seen it as one.

// proposalAt is a proposal and what the run knew when it was made.
type proposalAt struct {
	p          events.ProposalCreatedPayload
	ev         *evidence
	hypotheses int
}

// stepTargets reports whether a step matches its action's target.
func stepTargets(targets map[string]map[string]any, st events.ProposalStep) bool {
	want, ok := targets[st.Action]
	if !ok {
		return false
	}
	return paramsMatch(want, st.Params)
}

// judgeFixTargets grades the fix with fix_targets: ok, the unexpected steps
// and the proposals made before their evidence.
func judgeFixTargets(sc Scenario, props []proposalAt) (bool, []string, []string) {
	targets := sc.Expect.FixTargets
	hit := map[string]bool{}
	var unexpected, ungrounded []string
	for _, pa := range props {
		hasFix := false
		for i, st := range pa.p.Steps {
			if !stepTargets(targets, st) {
				unexpected = append(unexpected, fmt.Sprintf("%s step %d: %s %s", pa.p.ProposalID, i, st.Action, compactJSON(st.Params)))
				continue
			}
			hasFix = true
			hit[st.Action] = true
		}
		if !hasFix {
			continue
		}
		var missing []string
		if pa.hypotheses == 0 {
			missing = append(missing, "no hypothesis yet")
		}
		facts := pa.ev.facts(sc.Expect.ExpectedFacts)
		for _, f := range sc.Expect.ExpectedFacts {
			if f.Required && len(facts[f.ID]) == 0 {
				missing = append(missing, f.ID+" not yet observed")
			}
		}
		if len(missing) > 0 {
			ungrounded = append(ungrounded, fmt.Sprintf("%s (%s)", pa.p.ProposalID, strings.Join(missing, ", ")))
		}
	}
	ok := matchActionGroups(sc.Expect.Fix, hit) && len(unexpected) == 0 && len(ungrounded) == 0
	return ok, unexpected, ungrounded
}

// checkFixTargets validates fix_targets against the expect block and the
// live action schemas.
func (s Scenario) checkFixTargets(areg *actions.Registry) []error {
	t := s.Expect.FixTargets
	if len(t) == 0 {
		return nil
	}
	var errs []error
	if len(s.Expect.Fix) == 0 {
		errs = append(errs, fmt.Errorf("expect.fix_targets: needs expect.fix (the groups the targets belong to)"))
	}
	for _, g := range s.Expect.Fix {
		for _, a := range g {
			if _, ok := t[a]; !ok {
				errs = append(errs, fmt.Errorf("expect.fix_targets: no target for fix action %q; with fix_targets every fix action names its target", a))
			}
		}
	}
	required := 0
	for _, f := range s.Expect.ExpectedFacts {
		if f.Required {
			required++
		}
	}
	if required == 0 {
		errs = append(errs, fmt.Errorf("expect.fix_targets: needs at least one required expected_fact, or a fix guessed before any evidence would pass"))
	}
	names := make([]string, 0, len(t))
	for a := range t {
		names = append(names, a)
	}
	sort.Strings(names)
	for _, a := range names {
		act, ok := areg.Get(a)
		if !ok {
			errs = append(errs, fmt.Errorf("expect.fix_targets: unknown action %q", a))
			continue
		}
		if len(t[a]) == 0 {
			errs = append(errs, fmt.Errorf("expect.fix_targets.%s: empty target; name what the step must aim at", a))
		}
		props, _ := act.ParamsSchema()["properties"].(map[string]any)
		for _, k := range sortedKeys(t[a]) {
			if _, ok := props[k]; !ok {
				errs = append(errs, fmt.Errorf("expect.fix_targets.%s: %q is not a parameter of the action", a, k))
			}
		}
		if _, err := json.Marshal(t[a]); err != nil {
			errs = append(errs, fmt.Errorf("expect.fix_targets.%s: %v", a, err))
		}
	}
	return errs
}
