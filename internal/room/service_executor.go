package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

// execQueue serializes proposal execution per room: approvals released while
// another proposal is still running wait here instead of starting a second
// executor (review item 10: one action at a time per room).
type execQueue struct {
	mu    sync.Mutex
	items []store.ProposalRow
	wake  chan struct{}
}

func newExecQueue() *execQueue {
	return &execQueue{wake: make(chan struct{}, 1)}
}

// push appends without blocking and signals the worker. Called with s.mu held.
func (q *execQueue) push(p store.ProposalRow) {
	q.mu.Lock()
	q.items = append(q.items, p)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// pop returns the next queued proposal, if any.
func (q *execQueue) pop() (store.ProposalRow, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return store.ProposalRow{}, false
	}
	p := q.items[0]
	q.items = q.items[1:]
	return p, true
}

// enqueueExecutionLocked queues an approved proposal and lazily starts the
// room's single executor worker. Caller must hold s.mu.
func (s *Service) enqueueExecutionLocked(st *roomState, p store.ProposalRow) {
	if s.closed {
		return
	}
	if st.exec == nil {
		st.exec = newExecQueue()
		execCtx, cancel := context.WithCancel(s.ctx)
		st.execCancel = cancel
		s.wg.Add(1)
		go s.runExecutor(execCtx, p.RoomID, st.exec)
	}
	st.exec.push(p)
}

// runExecutor drains the room's queue one proposal at a time until its room
// context ends (the room closes) or the service lifetime ends.
func (s *Service) runExecutor(ctx context.Context, roomID string, q *execQueue) {
	defer s.wg.Done()
	for {
		for {
			p, ok := q.pop()
			if !ok {
				break
			}
			if err := s.executeProposal(p); err != nil && !errors.Is(err, context.Canceled) {
				s.log.Warn("room: executor stopped", "room", roomID, "proposal", p.ProposalID, "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-q.wake:
		}
	}
}

// stepStatus is one state of the explicit per-step state machine.
type stepStatus int

const (
	stepPending stepStatus = iota
	stepStarted
	stepDone
	stepFailed
)

// stepTracker is the explicit step state machine of one executor run:
// pending -> started -> done|failed. Every transition is checked; an invalid
// one returns ErrStepOrder instead of silently continuing (review item b).
type stepTracker struct {
	states []stepStatus
}

func newStepTracker(n int) *stepTracker {
	return &stepTracker{states: make([]stepStatus, n)}
}

// claim moves step i from pending to started.
func (t *stepTracker) claim(i int) error {
	if i < 0 || i >= len(t.states) {
		return fmt.Errorf("room: step %d out of range: %w", i, ErrStepOrder)
	}
	if t.states[i] != stepPending {
		return fmt.Errorf("room: step %d already started: %w", i, ErrStepOrder)
	}
	t.states[i] = stepStarted
	return nil
}

// finish moves step i from started to done or failed. Finishing a step that
// was never started (or already finished) is an order violation.
func (t *stepTracker) finish(i int, ok bool) error {
	if i < 0 || i >= len(t.states) {
		return fmt.Errorf("room: step %d out of range: %w", i, ErrStepOrder)
	}
	if t.states[i] != stepStarted {
		return fmt.Errorf("room: step %d is not started: %w", i, ErrStepOrder)
	}
	if ok {
		t.states[i] = stepDone
	} else {
		t.states[i] = stepFailed
	}
	return nil
}

// resultWriteTimeout bounds the result writes (step finish, proposal status,
// action.executed event) that must complete even while the service shuts down.
const resultWriteTimeout = 5 * time.Second

// resultCtx returns a context for result writes. It is detached from the
// service lifetime context (context.WithoutCancel) so Close cancelling s.ctx
// mid-run cannot leave a step "started" forever, and carries a short deadline
// so a hung database cannot block shutdown indefinitely (review item 3).
func (s *Service) resultCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(s.ctx), resultWriteTimeout)
}

// executeProposal runs the approved steps in order, stopping at the first
// failure. Each step is claimed in the store (status "started") BEFORE it
// runs, so a crash or a duplicate executor can never run it twice.
//
// Every stop path records an outcome: the step (when claimed) is finished as
// "failed", an action.executed event with OK=false is emitted and the proposal
// status becomes "failed", so the snapshot sees the result instead of a
// proposal stuck open (review item 13). Result writes use resultCtx and
// therefore survive a concurrent Close (review item 3).
//
// A non-nil return means the executor itself could not proceed (decode error,
// step order violation, store failure): that is an operational problem, not a
// failed action. A failing action is a normal outcome recorded as
// action.executed plus proposal status "failed".
func (s *Service) executeProposal(p store.ProposalRow) error {
	ctx := s.ctx

	// One executor per approved proposal across replicas
	// (docs/design/replicas.md section 5): the claim comes before anything
	// that writes, so a losing executor leaves no trace. With an error the
	// claim may or may not have landed; nothing runs, and if it landed the
	// stale-claim sweep records the outcome later.
	claimed, err := s.claimExecution(ctx, p.ProposalID)
	if err != nil {
		return fmt.Errorf("room: proposal %s: claim: %w", p.ProposalID, err)
	}
	if !claimed {
		s.log.Info("room: proposal already claimed by another executor; not run here", "proposal", p.ProposalID)
		return nil
	}

	// approved is the hash the approvers approved (the queued row, filled at
	// approval time). It is never replaced by anything read later, and every
	// event below carries it (p.ParamsHash is not reassigned).
	approved := p.ParamsHash
	var steps []events.ProposalStep
	if err := json.Unmarshal(p.Steps, &steps); err != nil {
		s.failProposal(p.RoomID, p.ProposalID, 0, p.ParamsHash, "cannot decode steps: "+err.Error())
		return fmt.Errorf("room: decode proposal %s steps: %w", p.ProposalID, err)
	}
	// CreateProposal refuses a proposal with no steps, so a row that decodes to
	// none (steps edited to null or []) was changed after creation. Without
	// this the loop below would not run, no step would be re-checked, and the
	// proposal would be marked "executed" (security audit 2026-10, A-PH-3).
	if len(steps) == 0 {
		s.failProposal(p.RoomID, p.ProposalID, 0, p.ParamsHash, paramsHashMismatchOutput)
		return nil
	}
	tracker := newStepTracker(len(steps))

	for i := range steps {
		if !s.isRoomOpen(ctx, p.RoomID) {
			s.failProposal(p.RoomID, p.ProposalID, i, p.ParamsHash, "room closed before this step ran")
			return nil
		}
		// Refresh the executor claim: a claim the stale sweep declared dead
		// (and recorded as failed) must not run another step.
		if ok, err := s.touchExecution(ctx, p.ProposalID); err != nil {
			return fmt.Errorf("room: proposal %s: refresh claim before step %d: %w", p.ProposalID, i, err)
		} else if !ok {
			s.log.Warn("room: executor claim was declared dead; step not run", "proposal", p.ProposalID, "step", i)
			return nil
		}
		if err := tracker.claim(i); err != nil {
			return fmt.Errorf("room: proposal %s: %w", p.ProposalID, err)
		}
		// Persist the claim before any side effect. ErrStepOrder here means the
		// step was already claimed by an earlier executor (crash + restart):
		// never run it again. Finish the stale claim and record the outcome so
		// the proposal does not stay open. Any other error (shutdown, store
		// failure) leaves the claim state unknown, so no outcome is recorded:
		// the proposal stays open and is re-evaluated after the next restart.
		if err := s.store.StartProposalStep(ctx, p.ProposalID, i); err != nil {
			if errors.Is(err, ErrStepOrder) {
				output := fmt.Sprintf("step %d was already claimed by an earlier run: %v", i, err)
				s.finishClaimedStep(p.ProposalID, i, output)
				s.failProposal(p.RoomID, p.ProposalID, i, p.ParamsHash, output)
				return fmt.Errorf("room: proposal %s: claim step %d: %w", p.ProposalID, i, err)
			}
			return fmt.Errorf("room: proposal %s: start step %d: %w", p.ProposalID, i, err)
		}

		// Design section 3.2: re-read the row and recompute the hash right
		// before the write. The step that runs is the one the fresh row holds
		// and the hash covers, not the copy queued at approval time.
		step, why := s.recheckStep(ctx, p.ProposalID, approved, i)
		if why != "" {
			s.stopOnStep(tracker, p, i, why)
			return nil
		}

		action, ok := s.actions.Get(step.Action)
		if !ok {
			s.stopOnStep(tracker, p, i, "unknown action "+step.Action)
			return nil
		}
		if err := action.Validate(step.Params); err != nil {
			s.stopOnStep(tracker, p, i, "validation failed: "+err.Error())
			return nil
		}
		// Re-check the scope right before the write, per docs/api.md: an
		// approved proposal must not write outside the room's pipeline.
		if err := s.scopeCheck(s.roomPipeline(ctx, p.RoomID), action, step.Params); err != nil {
			s.stopOnStep(tracker, p, i, "scope check failed: "+err.Error())
			return nil
		}

		sctx, cancel := context.WithTimeout(ctx, stepTimeout)
		out, err := action.Execute(sctx, step.Params)
		cancel()
		output := out
		if err != nil {
			output = err.Error()
		}
		if err := s.finishStep(p.RoomID, p.ProposalID, p.ParamsHash, tracker, i, err == nil, output); err != nil {
			// The local or store transition failed: finish the store row
			// directly (best effort) and record the outcome anyway, so the
			// proposal never stays open without an event.
			s.finishClaimedStep(p.ProposalID, i, output)
			s.failProposal(p.RoomID, p.ProposalID, i, p.ParamsHash, output)
			return err
		}
		if err != nil {
			s.setProposalStatus(p.ProposalID, "failed")
			return nil
		}
	}
	s.setProposalStatus(p.ProposalID, "executed")
	return nil
}

// stopOnStep finishes a claimed step as failed (which emits its
// action.executed event) and marks the proposal failed. If the store finish
// fails the outcome event is still emitted best-effort (review item 4), so the
// room UI never learns the step stopped but not why.
func (s *Service) stopOnStep(t *stepTracker, p store.ProposalRow, i int, output string) {
	if err := s.finishStep(p.RoomID, p.ProposalID, p.ParamsHash, t, i, false, output); err != nil {
		s.log.Warn("room: record step stop failed", "proposal", p.ProposalID, "step", i, "err", err)
		s.finishClaimedStep(p.ProposalID, i, output)
		s.failStep(p.RoomID, p.ProposalID, i, p.ParamsHash, output)
	}
	s.setProposalStatus(p.ProposalID, "failed")
}

// paramsHashMismatchOutput is the step output when the hash check refuses to
// run a step (design section 3.2).
const paramsHashMismatchOutput = "params hash mismatch"

// recheckStep is the executor's integrity check (design section 3.2). It
// re-reads the proposal row and recomputes the hash from what the row holds
// now. The step runs only if the recomputed hash equals BOTH the row's stored
// params_hash AND approved, the hash the approvers approved, which the executor
// took from the approval itself (the queued row) and keeps in memory. The
// second comparison is what stops someone who edits the steps and the stored
// hash together after the last approval. A row with no stored hash is a
// mismatch: the approve path fills the hash of an old row before it queues the
// proposal, so the executor never lets a row vouch for itself.
//
// It returns the step to run (taken from that same fresh row) and, when the
// step must not run, the failure output.
func (s *Service) recheckStep(ctx context.Context, proposalID, approved string, i int) (events.ProposalStep, string) {
	cur, err := s.store.GetProposal(ctx, proposalID)
	if err != nil {
		return events.ProposalStep{}, "cannot re-read the proposal before running the step: " + err.Error()
	}
	got, steps, err := rowParamsHash(cur)
	if err != nil {
		return events.ProposalStep{}, paramsHashMismatchOutput + " (cannot recompute: " + err.Error() + ")"
	}
	if approved == "" || cur.ParamsHash == "" || got != cur.ParamsHash || got != approved || i >= len(steps) {
		return events.ProposalStep{}, paramsHashMismatchOutput
	}
	// Every approval must be for the hash that runs. The last approver's echo
	// is checked against the row, but an earlier approver approved the row as
	// it was then: steps and stored hash edited together between two
	// approvals would otherwise run on the first approver's name (security
	// audit 2026-10, A-PH-1).
	apps, err := s.store.ListApprovals(ctx, proposalID)
	if err != nil {
		return events.ProposalStep{}, "cannot read the approvals before running the step: " + err.Error()
	}
	for _, a := range apps {
		if a.Decision == "approved" && a.ParamsHash != approved {
			return events.ProposalStep{}, paramsHashMismatchOutput
		}
	}
	return steps[i], ""
}

// finishStep checks the local transition, records the step result in the store
// and emits the action.executed event. The store write and the event use
// resultCtx so they complete even while the service shuts down (review item 3).
func (s *Service) finishStep(roomID, proposalID, paramsHash string, t *stepTracker, i int, ok bool, output string) error {
	output = sanitizeAgentText(output) // action output is tool text
	status := "done"
	if !ok {
		status = "failed"
	}
	if err := t.finish(i, ok); err != nil {
		return fmt.Errorf("room: proposal %s step %d: %w", proposalID, i, err)
	}
	rctx, cancel := s.resultCtx()
	defer cancel()
	if err := s.store.FinishProposalStep(rctx, proposalID, i, status, output); err != nil {
		return fmt.Errorf("room: proposal %s step %d: %w", proposalID, i, err)
	}
	// The DB state is authoritative; a failed event append must not undo it.
	if err := s.executorEmit(rctx, roomID, events.ActionExecuted, events.ActionExecutedPayload{
		ProposalID: proposalID, StepIndex: i, OK: ok, Output: output, ParamsHash: paramsHash,
	}); err != nil {
		s.log.Warn("room: emit action.executed failed", "proposal", proposalID, "step", i, "err", err)
	}
	return nil
}

// failProposal records a proposal failure with its outcome event (review item
// 13): a failing action.executed for the given step plus proposal status
// "failed". It is used on stop paths where the local tracker did not (or could
// not) finish a step: decode error, closed room, failed claim.
func (s *Service) failProposal(roomID, proposalID string, stepIndex int, paramsHash, output string) {
	s.failStep(roomID, proposalID, stepIndex, paramsHash, output)
	s.setProposalStatus(proposalID, "failed")
}

// failStep emits a failing action.executed for a step that was never claimed
// (so there is no step state to finish).
func (s *Service) failStep(roomID, proposalID string, i int, paramsHash, output string) {
	output = sanitizeAgentText(output)
	rctx, cancel := s.resultCtx()
	defer cancel()
	if err := s.executorEmit(rctx, roomID, events.ActionExecuted, events.ActionExecutedPayload{
		ProposalID: proposalID, StepIndex: i, OK: false, Output: output, ParamsHash: paramsHash,
	}); err != nil {
		s.log.Warn("room: emit action.executed failed", "proposal", proposalID, "step", i, "err", err)
	}
}

// finishClaimedStep marks a claimed store step "failed" without a local
// tracker transition. It is used when the claim belongs to an earlier run (or
// to this run but the local transition failed): ErrStepOrder simply means
// there is no "started" row to finish. The write uses resultCtx (review item 3).
func (s *Service) finishClaimedStep(proposalID string, i int, output string) {
	output = sanitizeAgentText(output)
	rctx, cancel := s.resultCtx()
	defer cancel()
	if err := s.store.FinishProposalStep(rctx, proposalID, i, "failed", output); err != nil && !errors.Is(err, ErrStepOrder) {
		s.log.Warn("room: finish claimed step failed", "proposal", proposalID, "step", i, "err", err)
	}
}

// scopeCheck enforces actions.Scoped for a step against the room's pipeline.
// It fails closed: when no owners config is loaded there is nothing to scope
// against, so the action is refused instead of running unchecked (LEAD-ARCH
// round 4, item 3 removed the "--demo skips scope" bypass).
func (s *Service) scopeCheck(pipelineID string, a actions.Action, params json.RawMessage) error {
	sc, ok := a.(actions.Scoped)
	if !ok {
		return fmt.Errorf("action %q does not support scope checks", a.Name())
	}
	if s.owners == nil {
		return fmt.Errorf("action %q cannot be scope-checked: no owners config is loaded", a.Name())
	}
	p, ok := s.owners.Pipeline(pipelineID)
	if !ok {
		return fmt.Errorf("pipeline %q is not configured", pipelineID)
	}
	if err := sc.CheckScope(p, params); err != nil {
		return fmt.Errorf("action %q: %w", a.Name(), err)
	}
	return nil
}

// roomPipeline returns the pipeline of a cached room ("" when unknown).
func (s *Service) roomPipeline(ctx context.Context, roomID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return ""
	}
	return st.snap.PipelineID
}

// setProposalStatus persists a proposal status using resultCtx, so the write
// survives a concurrent Close.
func (s *Service) setProposalStatus(proposalID, status string) {
	rctx, cancel := s.resultCtx()
	defer cancel()
	if err := s.store.SetProposalStatus(rctx, proposalID, status); err != nil {
		s.log.Warn("room: set proposal status failed", "proposal", proposalID, "status", status, "err", err)
	}
}

func (s *Service) isRoomOpen(ctx context.Context, roomID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.rooms[roomID]
	return st != nil && st.snap.Status == "open"
}
