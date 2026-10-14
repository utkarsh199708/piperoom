package evals

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Phased replay. A static scenario answers a call the same way every time, so
// it cannot show lag that changes, a pipeline that recovers after an approved
// fix, or whether the agent re-checks before it claims recovery. A phased
// scenario names the states the recorded stack goes through (phases, start
// phase first) and when the run moves between them (transitions): when the
// harness executes a proposal step, or after a tool has been called n times.
// A recording with a phase answers only in that phase; one without answers in
// every phase. A recording with a sequence answers successive matching calls
// with successive observations, and the last one repeats.

// Transition moves the run to phase To when On happens.
type Transition struct {
	On TransitionOn `yaml:"on"`
	To string       `yaml:"to"`
	// From, when set, limits the transition to runs in that phase. An
	// after_calls transition needs it in a scenario that can be fixed, so
	// time passing cannot undo a fix.
	From string `yaml:"from"`
	// Verify, when set, lists the only tools whose reads count as the
	// re-check after this transition into a recovered phase (see
	// verify_score.go). Empty means any tool.
	Verify []string `yaml:"verify"`
}

// TransitionOn is what fires a transition: exactly one of Action and
// AfterCalls. Transitions are checked in file order and the first that
// matches fires; a transition to the phase the run is already in does
// nothing.
type TransitionOn struct {
	// Action fires the transition when the harness executes a proposal step
	// with this action (an action_result input that names it, see
	// Input.Action).
	Action string `yaml:"action"`
	// OK, when set, also requires that step outcome. Absent matches any
	// outcome, unknown included.
	OK *bool `yaml:"ok"`
	// AfterCalls fires the transition once, right after the n-th call of the
	// tool in the run (misses included), so the n-th call is still answered
	// in the old phase. It stands for time passing.
	AfterCalls *AfterCalls `yaml:"after_calls"`
}

// AfterCalls counts calls of one tool.
type AfterCalls struct {
	Tool string `yaml:"tool"`
	N    int    `yaml:"n"`
}

// Observation is one result in a recording's sequence, with the same meaning
// as the top-level fields of a Recording.
type Observation struct {
	Summary   string `yaml:"summary"`
	Data      any    `yaml:"data"`
	Truncated bool   `yaml:"truncated"`
	Error     string `yaml:"error"`
}

// recordingResults returns every result a recording can give: its sequence,
// or its top-level fields as one result. Anything that reads what a
// recording shows (such as E2's factRecorded, which must find a fact that
// appears only in a sequence element) must go through it.
func recordingResults(r Recording) []Observation {
	if r.Sequence != nil {
		return r.Sequence
	}
	return []Observation{{Summary: r.Summary, Data: r.Data, Truncated: r.Truncated, Error: r.Error}}
}

// startPhase is the phase a run starts in; "" for a static scenario.
func (s Scenario) startPhase() string {
	if len(s.Phases) == 0 {
		return ""
	}
	return s.Phases[0]
}

// recoveredPhases are the phases a successful fix leads to: the targets of
// action transitions that require ok: true. After the run enters one, the
// agent must read the new state before it claims recovery (verify_score.go).
func (s Scenario) recoveredPhases() map[string]bool {
	out := map[string]bool{}
	for _, t := range s.Transitions {
		if t.On.Action != "" && t.On.OK != nil && *t.On.OK {
			out[t.To] = true
		}
	}
	return out
}

// onKey identifies a trigger, to find transitions that can never fire because
// an earlier one with the same trigger always wins.
func (t Transition) onKey() string {
	o := t.On
	from := "from " + t.From + " "
	switch {
	case o.Action != "" && o.OK == nil:
		return from + "action " + o.Action + " ok=any"
	case o.Action != "":
		return fmt.Sprintf("%saction %s ok=%v", from, o.Action, *o.OK)
	case o.AfterCalls != nil:
		return fmt.Sprintf("%safter_calls %s %d", from, o.AfterCalls.Tool, o.AfterCalls.N)
	}
	return ""
}

// validatePhases checks phases, transitions and the phase of every recording.
func (s Scenario) validatePhases(toolInputs map[string][]string, actionNames []string) []error {
	var errs []error
	known := map[string]bool{}
	for i, p := range s.Phases {
		switch {
		case strings.TrimSpace(p) == "":
			errs = append(errs, fmt.Errorf("phases[%d]: blank name", i))
		case known[p]:
			errs = append(errs, fmt.Errorf("phases[%d]: duplicate phase %q", i, p))
		}
		known[p] = true
	}
	if len(s.Phases) == 0 && len(s.Transitions) > 0 {
		errs = append(errs, errors.New("transitions need phases: the first phase is where a run starts"))
	}
	isAction := set(actionNames)
	seen := map[string]int{}
	anyOK := map[string]int{}
	for i, t := range s.Transitions {
		where := fmt.Sprintf("transitions[%d]", i)
		if !known[t.To] {
			errs = append(errs, fmt.Errorf("%s: to unknown phase %q", where, t.To))
		}
		if t.From != "" && !known[t.From] {
			errs = append(errs, fmt.Errorf("%s: from unknown phase %q", where, t.From))
		}
		for _, tool := range t.Verify {
			if _, ok := toolInputs[tool]; !ok {
				errs = append(errs, fmt.Errorf("%s: verify: unknown tool %q", where, tool))
			}
		}
		if t.On.AfterCalls != nil && t.From == "" && len(s.recoveredPhases()) > 0 {
			errs = append(errs, fmt.Errorf("%s: an after_calls transition needs from in a scenario with a recovered phase, or time passing could undo the fix", where))
		}
		on := t.On
		anyKey := t.From + "|" + on.Action
		shadowed := false
		switch {
		case on.Action != "" && on.AfterCalls != nil:
			errs = append(errs, fmt.Errorf("%s: on has both action and after_calls; use one", where))
		case on.Action != "":
			if !isAction[on.Action] {
				errs = append(errs, fmt.Errorf("%s: on.action: unknown action %q", where, on.Action))
			}
			if j, ok := anyOK[anyKey]; ok {
				shadowed = true
				errs = append(errs, fmt.Errorf("%s: never fires: transitions[%d] matches %s with any outcome first", where, j, on.Action))
			} else if on.OK == nil {
				anyOK[anyKey] = i
			}
		case on.AfterCalls != nil:
			if on.OK != nil {
				errs = append(errs, fmt.Errorf("%s: on.ok applies only to on.action", where))
			}
			if _, ok := toolInputs[on.AfterCalls.Tool]; !ok {
				errs = append(errs, fmt.Errorf("%s: on.after_calls: unknown tool %q", where, on.AfterCalls.Tool))
			}
			if on.AfterCalls.N < 1 {
				errs = append(errs, fmt.Errorf("%s: on.after_calls.n must be at least 1", where))
			}
		default:
			errs = append(errs, fmt.Errorf("%s: on needs action or after_calls", where))
		}
		if k := t.onKey(); k != "" && !shadowed {
			if j, dup := seen[k]; dup {
				errs = append(errs, fmt.Errorf("%s: never fires: same trigger as transitions[%d] (%s)", where, j, k))
			} else {
				seen[k] = i
			}
		}
	}
	for i, r := range s.Recordings {
		if r.Phase == "" || known[r.Phase] {
			continue
		}
		if len(s.Phases) == 0 {
			errs = append(errs, fmt.Errorf("recordings[%d] (%s): phase %q, but the scenario declares no phases", i, r.Tool, r.Phase))
		} else {
			errs = append(errs, fmt.Errorf("recordings[%d] (%s): unknown phase %q (want one of %s)", i, r.Tool, r.Phase, strings.Join(s.Phases, ", ")))
		}
	}
	if s.Expect.MaxVerifyCalls < 0 {
		errs = append(errs, errors.New("expect.max_verify_calls is negative"))
	}
	errs = append(errs, s.checkStatusPhase()...)
	return errs
}

// checkStatusPhase keeps action validation on the start phase. Validate of
// connect_resume_connector reads the first connect_status recording with data
// for the connector (connectStatuses), and proposals are made before any fix
// ran, so that recording must not belong to a later phase.
func (s Scenario) checkStatusPhase() []error {
	start := s.startPhase()
	seen := map[string]bool{}
	var errs []error
	for i, r := range s.Recordings {
		name, _ := r.Input["connector"].(string)
		if r.Tool != "connect_status" || name == "" || seen[name] || recordingData(r) == nil {
			continue
		}
		seen[name] = true
		if r.Phase != "" && r.Phase != start {
			errs = append(errs, fmt.Errorf("recordings[%d] (connect_status): action validation reads the first connect_status recording of %q, and it is in phase %q, not the start phase %q: put the start phase's recording first", i, name, r.Phase, start))
		}
	}
	return errs
}

// recordingData is the data action validation reads from a recording: the
// top-level data, or the first observation's.
func recordingData(r Recording) any {
	if res := recordingResults(r); len(res) > 0 {
		return res[0].Data
	}
	return nil
}

// checkSequence validates a recording with a sequence.
func checkSequence(i int, r Recording) []error {
	where := fmt.Sprintf("recordings[%d] (%s)", i, r.Tool)
	if len(r.Sequence) == 0 {
		return []error{fmt.Errorf("%s: sequence is empty", where)}
	}
	var errs []error
	if r.Summary != "" || r.Data != nil || r.Error != "" || r.Truncated {
		errs = append(errs, fmt.Errorf("%s: has a sequence and a top-level summary, data, error or truncated: put every result in the sequence", where))
	}
	for j, o := range r.Sequence {
		if o.Summary == "" && o.Error == "" {
			errs = append(errs, fmt.Errorf("%s: sequence[%d]: needs summary or error", where, j))
		}
		if o.Data != nil {
			if _, err := json.Marshal(o.Data); err != nil {
				errs = append(errs, fmt.Errorf("%s: sequence[%d]: data does not convert to JSON: %v", where, j, err))
			}
		}
	}
	return errs
}

// checkExecutedInput validates the action fields of one input.
func checkExecutedInput(i int, in Input, isAction map[string]bool) []error {
	if in.Action == "" {
		var errs []error
		if in.OK != nil {
			errs = append(errs, fmt.Errorf("inputs[%d]: ok needs action", i))
		}
		if in.Params != nil {
			errs = append(errs, fmt.Errorf("inputs[%d]: params needs action", i))
		}
		return errs
	}
	var errs []error
	if in.OK == nil {
		// The room always knows a step's outcome once it ran; a scripted
		// fix must say it.
		errs = append(errs, fmt.Errorf("inputs[%d]: an executed step (action) needs ok: true or false", i))
	}
	if in.Kind != "action_result" {
		errs = append(errs, fmt.Errorf("inputs[%d]: action applies only to kind action_result, not %q", i, in.Kind))
	}
	if !isAction[in.Action] {
		errs = append(errs, fmt.Errorf("inputs[%d]: unknown action %q", i, in.Action))
	}
	return errs
}

// Warnings are problems that do not make a scenario invalid but probably make
// it wrong: phases nothing leads to, and action transitions no input can fire.
func (s Scenario) Warnings() []string {
	var w []string
	if len(s.Phases) == 0 {
		return w
	}
	reach := map[string]bool{s.Phases[0]: true}
	for _, t := range s.Transitions {
		reach[t.To] = true
	}
	for _, p := range s.Phases[1:] {
		if !reach[p] {
			w = append(w, fmt.Sprintf("phase %q is unreachable: no transition leads to it", p))
		}
	}
	for i, t := range s.Transitions {
		on := t.On
		if on.Action == "" || s.executes(on) {
			continue
		}
		w = append(w, fmt.Sprintf("transitions[%d]: no input executes %s with a matching ok, so it fires only when that step runs before another executed step of the same proposal", i, on.Action))
	}
	return w
}

// executes reports whether some input executes on.Action with an outcome on
// accepts.
func (s Scenario) executes(on TransitionOn) bool {
	for _, in := range s.Inputs {
		if in.Action != on.Action {
			continue
		}
		if on.OK == nil || (in.OK != nil && *in.OK == *on.OK) {
			return true
		}
	}
	return false
}
