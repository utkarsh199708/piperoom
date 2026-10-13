package evals

import (
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
}

// scriptedInput is one scenario input waiting for its idle.
type scriptedInput struct {
	index  int // position in the scenario's inputs
	in     agent.HumanInput
	action string
	ok     *bool
	from   string // the approver's name for an executed step
}

func newScriptedInbox(inputs []agent.HumanInput) *scriptedInbox {
	in := &scriptedInbox{}
	for i, h := range inputs {
		in.queue = append(in.queue, scriptedInput{index: i, in: h})
	}
	return in
}

// newScenarioInbox scripts sc's inputs, executing the ones that name an
// action through x.
func newScenarioInbox(sc Scenario, x *executor) *scriptedInbox {
	in := newScriptedInbox(sc.humanInputs())
	for i := range in.queue {
		in.queue[i].action = sc.Inputs[i].Action
		in.queue[i].ok = sc.Inputs[i].OK
		in.queue[i].from = sc.Inputs[i].From
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
	defer in.mu.Unlock()
	out := in.pending
	in.pending = nil
	return out, nil
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
// the recorded stack to its next phase.
type executor struct {
	rec *recorder
	rp  *replayer

	mu   sync.Mutex
	done map[string]bool // "<proposal id>/<step index>" already executed
}

func newExecutor(rec *recorder, rp *replayer) *executor {
	return &executor{rec: rec, rp: rp, done: map[string]bool{}}
}

// run executes the step an input names: the first not yet executed step with
// that action in the newest proposal that has one. Like the room's executor
// it runs the proposal's steps in order, so the earlier unexecuted steps run
// first and succeed. It returns the inputs the room would deliver: one
// approval per required layer (on the proposal's first execution), then one
// action_result per executed step, formatted as internal/room formats them.
// It returns nil when nothing could be executed.
func (x *executor) run(si scriptedInput) []agent.HumanInput {
	props := x.rec.createdProposals()
	x.mu.Lock()
	defer x.mu.Unlock()
	for p := len(props) - 1; p >= 0; p-- {
		for i, st := range props[p].Steps {
			if st.Action == si.action && !x.done[stepKey(props[p].ProposalID, i)] {
				return x.execute(props[p], i, si)
			}
		}
	}
	x.rp.skip(fmt.Sprintf("inputs[%d]: no proposal of this run has a %s step left to execute", si.index, si.action))
	return nil
}

func stepKey(proposalID string, step int) string {
	return proposalID + "/" + strconv.Itoa(step)
}

// execute runs steps of p up to and including step. The caller holds x.mu.
func (x *executor) execute(p events.ProposalCreatedPayload, step int, si scriptedInput) []agent.HumanInput {
	var out []agent.HumanInput
	first := true
	for i := range p.Steps {
		if x.done[stepKey(p.ProposalID, i)] {
			first = false
		}
	}
	if first {
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
	}
	system := events.Actor{Kind: events.ActorSystem, ID: "executor", Name: "executor"}
	for i := 0; i <= step; i++ {
		key := stepKey(p.ProposalID, i)
		if x.done[key] {
			continue
		}
		x.done[key] = true
		// An earlier step had to succeed for the executor to reach this one.
		succeeded := true
		stepOK, output := &succeeded, "succeeded"
		if i == step {
			stepOK, output = nil, si.in.Text
			if si.ok != nil {
				v := *si.ok
				stepOK = &v
			}
		}
		x.rp.executed(p.Steps[i].Action, stepOK)
		idx := i
		out = append(out, agent.HumanInput{
			Seq: si.in.Seq, Actor: system, Kind: "action_result",
			Text: fmt.Sprintf("proposal %s step %d: %s", p.ProposalID, i, output),
			OK:   stepOK, ProposalID: p.ProposalID, StepIndex: &idx,
		})
	}
	return out
}

func (in *scriptedInbox) idleCount() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.idles
}
