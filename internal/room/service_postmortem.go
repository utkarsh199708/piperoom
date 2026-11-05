package room

import (
	"context"
	"errors"
	"time"

	"piperoom/internal/events"
)

// postmortemTimeout bounds the closing summary's model call, like a hand-off's.
const postmortemTimeout = handoffSummaryTimeout

// postmortemAppendTimeout bounds the postmortem.* append.
const postmortemAppendTimeout = 10 * time.Second

// postmortemRestartWindow is how long after a close a restarted server still
// writes the events-only draft of a room that has none.
const postmortemRestartWindow = 10 * time.Minute

var errPostmortemDecided = errors.New("room: postmortem already drafted")

// sweepPostmortems gives every room closed within the restart window that has
// neither postmortem event the events-only draft, so a crash during the
// drafting leaves no room without one. No model call after a restart (Load).
// It reads the published snapshots: the drafter takes the room's lock itself.
func (s *Service) sweepPostmortems() {
	for id, sn := range s.publishedRooms() {
		if sn.Shadow || sn.Status != "closed" || sn.Postmortem.Status != PostmortemDrafting || sn.ClosedAt == nil {
			continue
		}
		if s.now().Sub(*sn.ClosedAt) > postmortemRestartWindow {
			continue
		}
		if !s.addWorker() {
			return
		}
		go s.draftPostmortem(id, false)
	}
}

// draftPostmortem writes postmortem.drafted (or .failed) for a closed room. The
// caller reserved its place in wg (addWorker). It runs outside every lock on
// the service context, after room.closed, so closing never waits on the model.
// allowModel is false after a restart.
func (s *Service) draftPostmortem(roomID string, allowModel bool) {
	defer s.wg.Done()
	evs, err := s.allEvents(s.ctx, roomID)
	if err != nil {
		if s.ctx.Err() == nil {
			s.log.Warn("room: postmortem: read events failed", "room", roomID, "err", err)
		}
		return
	}
	typ, payload := s.choosePostmortem(s.ctx, roomID, evs, allowModel)
	if s.ctx.Err() != nil {
		return
	}
	actx, cancel := context.WithTimeout(s.ctx, postmortemAppendTimeout)
	defer cancel()
	s.syncRoom(actx, roomID)
	st := s.lockCachedRoom(roomID)
	if st == nil {
		return
	}
	defer s.unlockRoom(st)
	// A checked append: of two replicas that both drafted, one wins.
	_, err = s.emitCheckedLocked(actx, st, roomID, func() error {
		if st.snap.Postmortem.Status == PostmortemDrafted || st.snap.Postmortem.Status == PostmortemFailed {
			return errPostmortemDecided
		}
		return nil
	}, typ, systemActor, payload)
	if err != nil && !errors.Is(err, errPostmortemDecided) {
		// The GET still renders; the snapshot stays "drafting" until restart.
		s.log.Warn("room: append postmortem event failed", "room", roomID, "type", typ, "err", err)
	}
}

// choosePostmortem picks the source of the draft's prose (F2 item 13): the
// newest hand-off written after the last proposal, action and hypothesis, else
// one model call, else none.
func (s *Service) choosePostmortem(ctx context.Context, roomID string, evs []events.Event, allowModel bool) (events.Type, any) {
	var lastWork int64
	var handoffSeq int64
	var handoffID string
	for _, e := range evs {
		switch e.Type {
		case events.ProposalCreated, events.ActionExecuted, events.AgentHypothesis:
			lastWork = max(lastWork, e.Seq)
		case events.HandoffCreated:
			if p, ok := decode[events.HandoffCreatedPayload](e); ok {
				handoffSeq, handoffID = e.Seq, p.HandoffID
			}
		}
	}
	if handoffID != "" && handoffSeq > lastWork {
		return events.PostmortemDrafted, events.PostmortemDraftedPayload{Source: "handoff", SummaryRef: handoffID}
	}
	// A forecast room closed before anyone asked its agent has nothing for a
	// model to summarise, and must spend nothing (X3c).
	if s.writePostmortem == nil || !allowModel || quietLog(evs) {
		return events.PostmortemDrafted, events.PostmortemDraftedPayload{Source: "events"}
	}
	wctx, cancel := context.WithTimeout(ctx, postmortemTimeout)
	defer cancel()
	draft, err := s.writePostmortem(wctx, evs, s.opts)
	if err != nil {
		// Fixed codes only: error text can quote room content.
		reason := events.HandoffFailedSummary
		if ctx.Err() == nil && wctx.Err() == nil {
			s.log.Warn("room: postmortem model call failed", "room", roomID, "err", err)
		} else if ctx.Err() == nil {
			s.log.Warn("room: postmortem model call timed out", "room", roomID)
		}
		return events.PostmortemFailed, events.PostmortemFailedPayload{Reason: reason}
	}
	sum := sanitizeHandoff(draft.Summary)
	if sum.WhatBroke == "" && sum.Why == "" {
		s.log.Warn("room: postmortem model call returned an empty summary", "room", roomID)
		return events.PostmortemFailed, events.PostmortemFailedPayload{Reason: events.HandoffFailedSummary}
	}
	var viol []string
	for _, v := range draft.Violations {
		viol = append(viol, truncateDetail(sanitizeAgentText(v)))
	}
	return events.PostmortemDrafted, events.PostmortemDraftedPayload{Source: "model", Summary: &sum, Violations: viol}
}
