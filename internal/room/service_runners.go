package room

import (
	"context"
	"errors"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// EmitSetup is the runner registry's audit sink and the escape hatch for a
// protocol refusal that happens before a code resolves (a version mismatch at
// Pair or Hello): it appends setup.* events to the room's log when the event
// names a room (loading the room into the cache first when it is not), and
// logs (never loses) an attempt whose room is unknown, such as a bad pairing
// code or a global rate limit, with its safe outcome and source. The code and
// the token never reach this method.
func (s *Service) EmitSetup(ctx context.Context, roomID string, typ events.Type, payload any) error {
	if roomID == "" {
		outcome, source := pairingAuditFields(payload)
		s.log.Info("room: setup pairing audit (no room)", "type", typ, "outcome", outcome, "source", source)
		return nil
	}
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		outcome, source := pairingAuditFields(payload)
		s.log.Warn("room: setup audit dropped: room unavailable", "type", typ, "room", roomID, "outcome", outcome, "source", source, "err", err)
		return err
	}
	defer s.unlockRoom(st)
	_, err = s.emitLocked(ctx, st, roomID, typ, systemActor, payload)
	return err
}

// pairingAuditFields extracts the safe outcome and source from a setup audit
// payload that is a pairing attempt; both are "" for the other setup events.
func pairingAuditFields(payload any) (outcome, source string) {
	if p, ok := payload.(events.SetupPairingPayload); ok {
		return p.Outcome, p.Source
	}
	return "", ""
}

// IssuePairingCode issues a single-use pairing code bound to (tenant, roomID,
// creator), valid for protocol.CodeTTL. POST /api/setup (U1-5b) is its caller.
func (s *Service) IssuePairingCode(tenant, roomID string, creator events.Actor) (string, error) {
	return s.runners.IssuePairingCode(tenant, roomID, creator)
}

// PairRunner validates one pairing attempt (a Pair frame) and returns its
// outcome. It emits setup.pairing for the attempt.
func (s *Service) PairRunner(ctx context.Context, code, source string) (PairResult, error) {
	return s.runners.PairCode(ctx, code, source)
}

// AttachRunner registers a paired runner (after Hello) and consumes its code.
// It emits setup.runner_attached, and setup.pairing {outcome: replaced} when it
// replaces a runner of the same (tenant, room, creator). Ops waiting on the
// replaced runner can no longer be reported by it: they end as ErrRunnerLost.
func (s *Service) AttachRunner(ctx context.Context, code string, rn Runner) error {
	if err := s.runners.Attach(ctx, code, rn); err != nil {
		return err
	}
	s.runnerExec.DropRoom(rn.RoomID)
	return nil
}

// BindRunnerWriter attaches the live runner socket to the runner record the
// token names, so Execute can reach it. It reports false when the attachment
// was replaced or lost meanwhile.
func (s *Service) BindRunnerWriter(tenant, roomID, creatorID, token string, w RunnerWriter) bool {
	return s.runners.BindWriter(tenant, roomID, creatorID, token, w)
}

// ReconnectRunner reattaches a runner that presents its token (not a pairing
// code), rotates the token to newToken with the given idle expiry and emits
// setup.runner_attached again. The outcome is events.PairingOK, PairingBadCode
// or PairingExpired; an unknown or expired token changes nothing. Neither
// token is logged or put in an event.
func (s *Service) ReconnectRunner(ctx context.Context, token, newToken string, expires time.Time, source string) (Runner, string) {
	return s.runners.Reconnect(ctx, token, newToken, expires, source)
}

// RunnerLookup returns the runner bound to (tenant, roomID, creatorID) while a
// socket is attached.
func (s *Service) RunnerLookup(tenant, roomID, creatorID string) (*Runner, bool) {
	return s.runners.Runner(tenant, roomID, creatorID)
}

// RunnerLost detaches the runner bound to (tenant, roomID, creatorID) and emits
// setup.runner_lost. token is the attachment identity; a loss reported by a
// superseded socket is a no-op. The runner may reattach with its token until
// that expires.
func (s *Service) RunnerLost(ctx context.Context, tenant, roomID, creatorID, token string) {
	s.runners.Lost(ctx, tenant, roomID, creatorID, token)
}

// RunnerTouch updates the runner's last-seen time after a frame arrives.
func (s *Service) RunnerTouch(tenant, roomID, creatorID, token string) {
	s.runners.Touch(tenant, roomID, creatorID, token)
}

// RunnerRefused records that the runner refused an op: it wakes the waiting
// Execute with the *protocol.RefusedError (a duplicate_step refusal for a
// re-sent op is not an outcome and keeps it waiting) and emits
// setup.runner_refused once for the current attachment. A frame from a
// superseded attachment is ignored.
func (s *Service) RunnerRefused(ctx context.Context, tenant, roomID, creatorID, token string, ref protocol.Refusal) {
	payload := events.SetupRunnerRefusedPayload{
		Op:         ref.Op,
		Reason:     ref.Reason,
		ProposalID: ref.ProposalID,
		StepIndex:  ref.StepIndex,
	}
	if !s.runners.Refuse(ctx, tenant, roomID, creatorID, token, payload) {
		return
	}
	s.runnerExec.DeliverRefusal(roomID, ref)
}

// RunnerResult hands one FrameResult to the op waiting for it or records it as
// the step's result when that call is gone. A result for an op this server
// never sent is ignored; neither the op id nor the result is logged whole.
func (s *Service) RunnerResult(ctx context.Context, tenant, roomID, creatorID, token string, res protocol.OpResult) {
	if !s.runners.Current(tenant, roomID, creatorID, token) {
		return
	}
	s.runnerExec.DeliverResult(ctx, roomID, res)
}

// RunnerResume reconciles the first frame after a token reconnect: done ops are
// delivered or recorded, running ops without a waiter are recorded as unknown,
// and an op absent from Resume is re-sent at most once; a running or done op is
// never re-sent. A Resume from a superseded attachment is ignored.
func (s *Service) RunnerResume(ctx context.Context, tenant, roomID, creatorID, token string, res protocol.Resume) {
	if !s.runners.Current(tenant, roomID, creatorID, token) {
		return
	}
	s.runnerExec.ReconcileResume(ctx, roomID, res)
}

// RunnerExecutor returns the executor a setup action or remote client uses to
// reach the runner attached to its room (protocol.RunnerExecutor).
func (s *Service) RunnerExecutor() protocol.RunnerExecutor {
	return s.runnerExec
}

// applyRunnerResult records an OpResult no Execute call waits for any more as
// the step's result, exactly once: the store's started->done transition is
// the guard, so a duplicate or a step already recorded by the executor is
// ignored. The event carries the runner's receipt (actions.Receipted).
func (s *Service) applyRunnerResult(ctx context.Context, roomID string, req protocol.OpRequest, res protocol.OpResult) {
	receipt := res.Receipt
	s.recordRunnerOutcome(roomID, req, res.OK, res.Summary, false, &receipt)
}

// applyRunnerUnknown records that a step's outcome is unknown: the runner
// reported it still running after its socket dropped, or the request ended
// while the op was in flight. Exactly once, like applyRunnerResult.
func (s *Service) applyRunnerUnknown(ctx context.Context, roomID string, req protocol.OpRequest) {
	s.recordRunnerOutcome(roomID, req, false, runnerLostOutput, true, nil)
}

// runnerLostOutput is the step output when a runner op's outcome is unknown.
const runnerLostOutput = "outcome unknown: the runner socket dropped while this step was in flight; check the target before you retry with a new proposal"

// recordRunnerOutcome finishes a step the runner executed but whose Execute
// caller no longer records it, and emits its action.executed once. A step that
// is not started any more (finished by the ordinary executor, or by an earlier
// reconciliation) is left alone.
func (s *Service) recordRunnerOutcome(roomID string, req protocol.OpRequest, ok bool, output string, unknown bool, receipt *events.SetupReceipt) {
	if req.ProposalID == "" || req.StepIndex == nil {
		return // a T0 op has no step to record on the server
	}
	i := *req.StepIndex
	status := "done"
	if !ok {
		status = "failed"
	}
	output = maskExecOutput(output)
	rctx, cancel := s.resultCtx()
	defer cancel()
	if err := s.finishStepRow(rctx, req.ProposalID, i, status, output); err != nil {
		if errors.Is(err, ErrStepOrder) {
			// Already finished: a duplicate result, or the executor recorded
			// the step before this outcome arrived. Never record it twice.
			return
		}
		s.log.Warn("room: record reconciled runner result failed",
			"room", roomID, "proposal", req.ProposalID, "step", i, "err", maskExecErr(err))
		return
	}
	if err := s.executorEmit(rctx, roomID, events.ActionExecuted, events.ActionExecutedPayload{
		ProposalID:     req.ProposalID,
		StepIndex:      i,
		OK:             ok,
		Output:         output,
		ParamsHash:     req.ParamsHash,
		OutcomeUnknown: unknown,
		PlanStepIndex:  s.setupPlanIndexOf(rctx, roomID, req.ProposalID, i),
		Receipt:        receipt,
	}); err != nil {
		s.log.Warn("room: emit reconciled action.executed failed",
			"room", roomID, "proposal", req.ProposalID, "step", i, "err", maskExecErr(err))
	}
	if !ok {
		s.setProposalStatus(req.ProposalID, "failed")
	}
}
