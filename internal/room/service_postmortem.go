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

// sweepPostmortemsLocked gives every room closed within the restart window that
// has neither postmortem event the events-only draft, so a crash during the
// drafting leaves no room without one. No model call after a restart. Caller
// holds s.mu (Load).
func (s *Service) sweepPostmortemsLocked() {
	if s.closed {
		return
	}
	for id, st := range s.rooms {
		sn := st.snap
		if sn.Shadow || sn.Status != "closed" || sn.Postmortem.Status != PostmortemDrafting || sn.ClosedAt == nil {
			continue
		}
		if s.now().Sub(*sn.ClosedAt) > postmortemRestartWindow {
			continue
		}
		s.wg.Add(1)
		go s.draftPostmortem(id, false)
	}
}

// draftPostmortem writes postmortem.drafted (or .failed) for a closed room. The
// caller did wg.Add(1). It runs outside s.mu on the service context, after
// room.closed, so closing never waits on the model. allowModel is false after a
// restart.
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
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.rooms[roomID]
	if st == nil {
		return
	}
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
// newest hand-off written after the last proposal, action, hypothesis and fix
// note, else one model call, else none. A fix note counts as work: a person
// fixed the problem after the hand-off, so its open items and risks are stale
// (QA G10).
func (s *Service) choosePostmortem(ctx context.Context, roomID string, evs []events.Event, allowModel bool) (events.Type, any) {
	var lastWork int64
	var handoffSeq int64
	var handoffID string
	for _, e := range evs {
		switch e.Type {
		case events.ProposalCreated, events.ActionExecuted, events.AgentHypothesis:
			lastWork = max(lastWork, e.Seq)
		case events.CommentAdded:
			if p, ok := decode[events.CommentAddedPayload](e); ok && p.Kind == events.CommentKindFixNote {
				lastWork = max(lastWork, e.Seq)
			}
		case events.HandoffCreated:
			if p, ok := decode[events.HandoffCreatedPayload](e); ok {
				handoffSeq, handoffID = e.Seq, p.HandoffID
			}
		}
	}
	if handoffID != "" && handoffSeq > lastWork {
		return events.PostmortemDrafted, events.PostmortemDraftedPayload{Source: "handoff", SummaryRef: handoffID}
	}
	if s.writePostmortem == nil || !allowModel {
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
