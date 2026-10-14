package evals

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// RecordedEvent is one event the agent produced during a run.
type RecordedEvent struct {
	Type    events.Type     `json:"type"`
	Payload json.RawMessage `json:"payload"`
	At      time.Time       `json:"at"`
}

// Transcript is everything one run produced, in order. Proposals appear in
// Events as proposal.created, the way the room service would log them.
type Transcript struct {
	Events []RecordedEvent `json:"events"`
	Misses []Miss          `json:"misses,omitempty"`
	// Attempts are every action Validate call, including the rejected ones
	// that never became a proposal.
	Attempts []ActionAttempt `json:"action_attempts,omitempty"`
	Idles    int             `json:"idles"`
	// Replay is what a phased or executing scenario's replay did: phases,
	// executed steps, transitions and phase-tagged answers. Nil for a static
	// scenario that executed nothing.
	Replay *ReplayTrace `json:"replay,omitempty"`
}

// recorder is the run's Sink and Proposals: it keeps every event in order,
// and every proposal so the executor can run their steps.
type recorder struct {
	mu        sync.Mutex
	events    []RecordedEvent
	proposals []events.ProposalCreatedPayload
	nextID    int
	now       func() time.Time
}

func newRecorder() *recorder { return &recorder{now: time.Now} }

// Emit implements agent.Sink.
func (r *recorder) Emit(_ context.Context, typ events.Type, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("evals: marshal %s payload: %w", typ, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, RecordedEvent{Type: typ, Payload: b, At: r.now()})
	return nil
}

// Create implements agent.Proposals. It stores the proposal and never runs
// anything; the agent sees the same kind of reply the room service gives.
func (r *recorder) Create(ctx context.Context, p events.ProposalCreatedPayload) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	r.mu.Lock()
	r.nextID++
	p.ProposalID = "p_" + strconv.Itoa(r.nextID)
	r.mu.Unlock()
	if err := r.Emit(ctx, events.ProposalCreated, p); err != nil {
		return "", err
	}
	r.mu.Lock()
	r.proposals = append(r.proposals, p)
	r.mu.Unlock()
	return p.ProposalID, nil
}

func (r *recorder) snapshot() []RecordedEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RecordedEvent(nil), r.events...)
}

// count is how many events have been recorded so far.
func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// createdProposals returns the proposals created so far, oldest first.
func (r *recorder) createdProposals() []events.ProposalCreatedPayload {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]events.ProposalCreatedPayload(nil), r.proposals...)
}

// scriptedInbox gives the agent the scenario's inputs one per idle. Once they
// run out it reports agent.ErrInboxClosed, which ends agent.Run cleanly.
//
// An input that names an action is an executed fix step: on its idle the
// executor turns it into what the room would deliver (approvals, then step
// results). If the action was never proposed there is nothing to execute;
// the input is skipped and the next one is delivered on the same idle.
type scriptedInbox struct {
	mu      sync.Mutex
	queue   []scriptedInput
	pending []agent.HumanInput
	idles   int
	exec    *executor // nil: inputs are delivered as written
	// log, when set, records each steer and comment as the room log would
	// (steer.sent, comment.added) at the moment the agent receives it, so the
	// transcript shows what the agent had been told when it spoke (the ask
	// outcome counts only questions after the first one). Approval and
	// action results have no event of their own here: theirs need a proposal.
	log agent.Sink
}

// scriptedInput is one scenario input waiting for its idle.
type scriptedInput struct {
	index  int // position in the scenario's inputs
	in     agent.HumanInput
	action string
	ok     *bool
	params map[string]any
	from   string // the approver's name for an executed step
}

func newScriptedInbox(inputs []agent.HumanInput, log agent.Sink) *scriptedInbox {
	in := &scriptedInbox{log: log}
	for i, h := range inputs {
		in.queue = append(in.queue, scriptedInput{index: i, in: h})
	}
	return in
}

// newScenarioInbox scripts sc's inputs, executing the ones that name an
// action through x and logging delivered steers and comments to log.
func newScenarioInbox(sc Scenario, x *executor, log agent.Sink) *scriptedInbox {
	in := newScriptedInbox(sc.humanInputs(), log)
	for i := range in.queue {
		in.queue[i].action = sc.Inputs[i].Action
		in.queue[i].ok = sc.Inputs[i].OK
		in.queue[i].from = sc.Inputs[i].From
		in.queue[i].params = sc.Inputs[i].Params
	}
	in.exec = x
	return in
}

// Drain implements agent.Inbox.
func (in *scriptedInbox) Drain(ctx context.Context) ([]agent.HumanInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	in.mu.Lock()
	out := in.pending
	in.pending = nil
	in.mu.Unlock()
	if in.log != nil {
		for _, h := range out {
			if err := in.logInput(ctx, h); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// logInput records one delivered input in the room log's shape.
func (in *scriptedInbox) logInput(ctx context.Context, h agent.HumanInput) error {
	switch h.Kind {
	case "steer":
		return in.log.Emit(ctx, events.SteerSent, events.SteerSentPayload{Text: h.Text})
	case "comment":
		return in.log.Emit(ctx, events.CommentAdded, events.CommentAddedPayload{
			CommentID: h.CommentID, StepID: h.StepID, Text: h.Text, MentionsAgent: true,
		})
	}
	return nil
}

// Wait implements agent.Inbox. The agent calls it only when it is idle with
// nothing pending, so each call is one idle.
func (in *scriptedInbox) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if len(in.pending) > 0 {
		return nil
	}
	in.idles++
	for len(in.queue) > 0 {
		next := in.queue[0]
		in.queue = in.queue[1:]
		if next.action == "" || in.exec == nil {
			in.pending = append(in.pending, next.in)
			return nil
		}
		if got := in.exec.run(next); len(got) > 0 {
			in.pending = append(in.pending, got...)
			return nil
		}
	}
	return agent.ErrInboxClosed
}

// executor plays the room and the action executor for scripted action
// results. Nothing is written anywhere: executing a step means delivering
// what the room would and telling the replayer, whose transitions then move
// the recorded stack to its next phase. It mirrors the room's rules: a
// proposal whose steps the room refuses at creation (unknown action, failed
// Validate or CheckScope; internal/room service_proposals.go) never runs, a
// proposal runs all its steps in order once it is approved, and the executor
// stops at the first step that does not succeed (service_executor.go).
type executor struct {
	sc  Scenario
	rec *recorder
	rp  *replayer

	mu      sync.Mutex
	done    map[string]bool // "<proposal id>/<step index>" already executed
	stopped map[string]bool // proposals whose execution ended at a failed step
	refused map[string]bool // proposals the room would have refused
}

func newExecutor(sc Scenario, rec *recorder, rp *replayer) *executor {
	return &executor{sc: sc, rec: rec, rp: rp, done: map[string]bool{}, stopped: map[string]bool{}, refused: map[string]bool{}}
}

// run executes the proposal an input names: the newest proposal of the run
// that the room would accept, that has not stopped at a failed step, and
// that has a not yet executed step with the input's action. It returns the
// inputs the room would deliver: one approval per required layer, then one
// action_result per executed step, formatted as internal/room formats them.
// It returns nil when nothing could be executed; the skip is reported.
func (x *executor) run(si scriptedInput) []agent.HumanInput {
	props := x.rec.createdProposals()
	x.mu.Lock()
	defer x.mu.Unlock()
	for p := len(props) - 1; p >= 0; p-- {
		id := props[p].ProposalID
		if x.stopped[id] || x.isRefused(props[p]) {
			continue
		}
		for i, st := range props[p].Steps {
			if st.Action == si.action && !x.done[stepKey(id, i)] {
				return x.execute(props[p], i, si)
			}
		}
	}
	x.rp.skip(fmt.Sprintf("inputs[%d]: no executable proposal of this run has a %s step left to run", si.index, si.action))
	return nil
}

// isRefused reports whether the room would have refused p when the agent
// proposed it, so it can never run. The caller holds x.mu.
func (x *executor) isRefused(p events.ProposalCreatedPayload) bool {
	r, ok := x.refused[p.ProposalID]
	if !ok {
		r = len(checkSteps(x.sc, p.Steps)) > 0
		x.refused[p.ProposalID] = r
	}
	return r
}

func stepKey(proposalID string, step int) string {
	return proposalID + "/" + strconv.Itoa(step)
}

// execute runs p as the room's executor would once every layer approved it:
// the steps in order, earlier ones succeeding, the named one with the
// scripted outcome, and the later ones succeeding too, unless a step fails,
// where it stops. The caller holds x.mu.
func (x *executor) execute(p events.ProposalCreatedPayload, step int, si scriptedInput) []agent.HumanInput {
	var out []agent.HumanInput
	approver := events.Actor{Kind: events.ActorHuman, ID: "approver", Name: "approver"}
	if si.from != "" {
		approver.ID, approver.Name = si.from, si.from
	}
	approved := true
	seen := map[string]bool{}
	for _, st := range p.Steps {
		if st.Layer == "" || seen[st.Layer] {
			continue
		}
		seen[st.Layer] = true
		out = append(out, agent.HumanInput{
			Seq: si.in.Seq, Actor: approver, Kind: "approval_result",
			Text:       fmt.Sprintf("layer %q approved proposal %s", st.Layer, p.ProposalID),
			ProposalID: p.ProposalID, Layer: st.Layer, Approved: &approved,
		})
	}
	system := events.Actor{Kind: events.ActorSystem, ID: "executor", Name: "executor"}
	// run binds only proposals with no step run yet (a proposal runs all its
	// steps at once, or stops), so every step here is new.
	for i, st := range p.Steps {
		x.done[stepKey(p.ProposalID, i)] = true
		ok, output := true, "succeeded"
		switch {
		case i == step && si.params != nil && !paramsMatch(si.params, st.Params):
			// Not the step the scenario scripted (another task, another
			// connector): the target says no, and the stack does not move.
			ok, output = false, fmt.Sprintf("actions: %s %s: not found", st.Action, compactJSON(st.Params))
			x.rp.executedWrongParams(st.Action)
		case i == step:
			ok, output = si.ok != nil && *si.ok, si.in.Text
			x.rp.executed(st.Action, &ok)
		default:
			x.rp.executed(st.Action, &ok)
		}
		stepOK, idx := ok, i
		out = append(out, agent.HumanInput{
			Seq: si.in.Seq, Actor: system, Kind: "action_result",
			Text: fmt.Sprintf("proposal %s step %d: %s", p.ProposalID, i, output),
			OK:   &stepOK, ProposalID: p.ProposalID, StepIndex: &idx,
		})
		if !ok {
			x.stopped[p.ProposalID] = true
			break
		}
	}
	return out
}

// paramsMatch reports whether every scripted param is in the step's params
// with an equal value (the recording input rule).
func paramsMatch(want map[string]any, params json.RawMessage) bool {
	var got map[string]any
	if err := json.Unmarshal(params, &got); err != nil {
		return false
	}
	return matches(want, got)
}

// compactJSON renders step params for a result text.
func compactJSON(raw json.RawMessage) string {
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		return string(raw)
	}
	return b.String()
}

func (in *scriptedInbox) idleCount() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.idles
}
