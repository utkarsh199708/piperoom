package room

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Withdrawals (Q0, QA G8/G9): proposal.withdrawn ends an open proposal
// without running it. The driver withdraws through the route, the agent its
// own proposals through agent.ProposalWithdrawer, and the close every
// proposal still open.
//
// Who wins against the last approval: in one process the room's lock orders
// the withdrawal and the approval, and a fully approved proposal is refused
// (proposal_running). Across replicas the snapshot can lag, so the store
// decides: WithdrawProposal changes the row only while no executor claimed it
// and no step started, and the executor's claim needs the row still open, so
// exactly one of the two lands. The event follows the row.

// maxWithdrawReason caps the withdrawer's reason, in characters (docs/api.md).
const maxWithdrawReason = 500

// errProposalWithdrawn is 409 proposal_withdrawn: approve, reject or withdraw
// of a withdrawn proposal.
func errProposalWithdrawn() *Error {
	return &Error{Status: 409, Code: "proposal_withdrawn", Msg: "the proposal was withdrawn; it takes no more decisions"}
}

// errProposalRunning is 409 proposal_running: the proposal is fully approved
// or the executor has claimed one of its steps, so it can no longer be
// withdrawn.
func errProposalRunning() *Error {
	return &Error{Status: 409, Code: "proposal_running", Msg: "the proposal is fully approved or running; it can no longer be withdrawn"}
}

// errNotOwnProposal answers the agent's withdrawal of a proposal it did not
// make. The message is shown to the model.
func errNotOwnProposal() *Error {
	return &Error{Status: 403, Code: "not_allowed", Msg: "the agent can withdraw only proposals it made; ask the driver to withdraw this one"}
}

// WithdrawProposal is the driver's POST /api/rooms/{id}/proposals/{pid}/withdraw:
// it writes proposal.withdrawn with the driver as actor. The agent reads it as
// a declined approval_result (withdrawnInput). reason is optional, typed text
// of at most maxWithdrawReason characters.
func (s *Service) WithdrawProposal(ctx context.Context, user User, roomID, proposalID, reason string) (events.Event, error) {
	// Typed text: refused before any read or write.
	if err := rejectUnstorable(reason); err != nil {
		return events.Event{}, err
	}
	if err := checkLen("reason", reason, maxWithdrawReason); err != nil {
		return events.Event{}, err
	}
	// Shared store: fold what other replicas wrote, reading outside the lock.
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	if !s.roomOpen(st) {
		return events.Event{}, roomClosed("room is closed")
	}
	if !s.isDriver(st, user.ID) {
		return events.Event{}, ErrNotDriver()
	}
	e, err := s.withdrawLocked(ctx, st, roomID, proposalID, s.humanActor(user), reason, false)
	if err != nil {
		return events.Event{}, err
	}
	s.log.Info("room: proposal withdrawn by the driver", "room", roomID, "proposal", proposalID, "user", user.ID)
	// A driver action renews the driver lease (docs/api.md).
	s.touchDriverLease(ctx, st, roomID)
	return e, nil
}

// Withdraw implements agent.ProposalWithdrawer for the room's agent run: only
// a proposal the agent made in this room, only while it is open, and fenced
// like Create, so a run that lost its lease withdraws nothing. reason is
// model text: cleaned and cut to maxWithdrawReason, never refused.
func (p proposalCreator) Withdraw(ctx context.Context, proposalID, reason string) error {
	return p.s.agentWithdraw(ctx, p.roomID, p.run, proposalID, reason)
}

var _ agent.ProposalWithdrawer = proposalCreator{}

func (s *Service) agentWithdraw(ctx context.Context, roomID string, run uint64, proposalID, reason string) error {
	reason = clipChars(strings.TrimSpace(sanitizeAgentText(reason)), maxWithdrawReason)
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return err
	}
	defer s.unlockRoom(st)
	if err := s.fenceLocked(roomID, st, run); err != nil {
		return err
	}
	if !s.roomOpen(st) {
		return roomClosed("room is closed")
	}
	if st.snap.Shadow {
		return validation("a shadow room has no proposals")
	}
	if _, err := s.withdrawLocked(ctx, st, roomID, proposalID, events.AgentActor, reason, true); err != nil {
		return err
	}
	s.log.Info("room: proposal withdrawn by the agent", "room", roomID, "proposal", proposalID)
	return nil
}

// withdrawLocked withdraws an open proposal of the open room roomID as actor.
// own limits it to a proposal the agent made. It answers 404 for a proposal of
// another room, 409 proposal_withdrawn, already_decided (rejected, executed
// or failed) or proposal_running. The row is changed first (the store decides
// against an executor's claim) and put back when the event cannot be written,
// so the log and the row agree. Caller holds st's lock.
func (s *Service) withdrawLocked(ctx context.Context, st *roomState, roomID, proposalID string, actor events.Actor, reason string, own bool) (events.Event, error) {
	row, err := s.store.GetProposal(ctx, proposalID)
	if err != nil {
		return events.Event{}, mapStoreErr(err)
	}
	if row.RoomID != roomID {
		return events.Event{}, notFound("proposal not found")
	}
	switch row.Status {
	case "open":
	case store.ProposalWithdrawn:
		return events.Event{}, errProposalWithdrawn()
	default:
		return events.Event{}, alreadyDecided("proposal already decided")
	}
	prop := findOpenProposal(st.snap, proposalID)
	if prop == nil {
		// The row is open but the log decided it (a step result whose status
		// write was lost): it is not open any more.
		return events.Event{}, alreadyDecided("proposal already decided")
	}
	if own && !prop.byAgent {
		return events.Event{}, errNotOwnProposal()
	}
	// Fully approved means queued for the executor or running.
	if fullyApproved(*prop) {
		return events.Event{}, errProposalRunning()
	}
	if err := s.store.WithdrawProposal(ctx, proposalID); err != nil {
		return events.Event{}, mapWithdrawErr(err)
	}
	e, err := s.emitLocked(ctx, st, roomID, events.ProposalWithdrawn, actor, events.ProposalWithdrawnPayload{
		ProposalID: proposalID, Reason: reason,
	})
	if err != nil {
		// No event, no withdrawal: everyone reads the log, so the proposal
		// must stay approvable. A fresh deadline, as ctx may be why the
		// append failed.
		rctx, cancel := s.resultCtx()
		if rerr := s.store.SetProposalStatus(rctx, proposalID, "open"); rerr != nil {
			s.log.Warn("room: put back a proposal whose withdrawal was not logged", "proposal", proposalID, "err", rerr)
		}
		cancel()
		return events.Event{}, err
	}
	return e, nil
}

// withdrawOnCloseLocked writes proposal.withdrawn {reason: "room closed"} with
// the system actor for every proposal still open when the room closed, right
// after room.closed (QA G8), so a closed room shows no proposal waiting for
// approval. A fully approved proposal is the executor's: it records the
// outcome itself ("room closed before this step ran", or the step's result),
// and so is one the store says an executor claimed. The store counts these
// events like room.closed, so a full log takes them too. The writes use a
// fresh deadline detached from the request: room.closed is already in the log.
// A shadow room has no proposals. Caller holds st's lock.
func (s *Service) withdrawOnCloseLocked(ctx context.Context, st *roomState, roomID string) {
	// room.closed already emptied OpenProposals; openAtClose is what it
	// found open.
	if st.snap.Shadow || len(st.snap.openAtClose) == 0 {
		return
	}
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), resultWriteTimeout)
	defer cancel()
	for _, p := range append([]Proposal(nil), st.snap.openAtClose...) {
		if fullyApproved(p) {
			continue
		}
		if err := s.store.WithdrawProposal(wctx, p.ProposalID); err != nil {
			if !errors.Is(err, store.ErrProposalRunning) && !errors.Is(err, store.ErrProposalDecided) && !errors.Is(err, store.ErrProposalWithdrawn) {
				s.log.Warn("room: withdraw proposal at close failed", "room", roomID, "proposal", p.ProposalID, "err", err)
			}
			continue
		}
		if _, err := s.emitLocked(wctx, st, roomID, events.ProposalWithdrawn, systemActor, events.ProposalWithdrawnPayload{
			ProposalID: p.ProposalID, Reason: events.ProposalWithdrawnRoomClosed,
		}); err != nil {
			// The row says withdrawn and the room is closed, so nothing can
			// approve or run it; only the log lacks the withdrawal.
			s.log.Warn("room: log withdrawal at close failed", "room", roomID, "proposal", p.ProposalID, "err", err)
		}
	}
}

// withdrawnInput is what the agent reads of the driver's withdrawal: an
// approval_result that did not approve, saying it was withdrawn and why
// (docs/api.md). The agent's own withdrawal and the close's are not fed: the
// agent made the first, and the second ends its run.
func withdrawnInput(e events.Event) *agent.HumanInput {
	if e.Actor.Kind != events.ActorHuman {
		return nil
	}
	p, ok := decode[events.ProposalWithdrawnPayload](e)
	if !ok {
		return nil
	}
	text := "the driver withdrew your proposal; it will not run and takes no more approvals"
	if reason := strings.TrimSpace(p.Reason); reason != "" {
		text += ". Reason: " + reason
	}
	approved := false
	return &agent.HumanInput{
		Seq: e.Seq, Actor: e.Actor, Kind: "approval_result", Text: text,
		ProposalID: p.ProposalID, Approved: &approved,
	}
}

// findOpenProposal returns the snapshot's open proposal with this id, or nil.
func findOpenProposal(snap RoomSnapshot, proposalID string) *Proposal {
	for i := range snap.OpenProposals {
		if snap.OpenProposals[i].ProposalID == proposalID {
			return &snap.OpenProposals[i]
		}
	}
	return nil
}

// mapWithdrawErr turns a store refusal into the API answer.
func mapWithdrawErr(err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return notFound("proposal not found")
	case errors.Is(err, store.ErrProposalWithdrawn):
		return errProposalWithdrawn()
	case errors.Is(err, store.ErrProposalRunning):
		return errProposalRunning()
	case errors.Is(err, store.ErrProposalDecided):
		return alreadyDecided("proposal already decided")
	}
	return fmt.Errorf("room: withdraw proposal: %w", err)
}
