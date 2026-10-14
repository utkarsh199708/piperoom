package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// handoffSummaryTimeout bounds the hand-off summary generation. docs/api.md
// allows up to 3 minutes for this route.
const handoffSummaryTimeout = 3 * time.Minute

// handoffListMax caps the number of items in each hand-off list (review item
// 12: a runaway model must not dump raw tool output into the log).
const handoffListMax = 10

// sanitizeHandoff bounds every agent-written summary string and list before it
// is stored, so raw tool output blobs cannot leak into the log or the UI.
func sanitizeHandoff(s events.HandoffSummary) events.HandoffSummary {
	s.WhatBroke = truncateDetail(sanitizeAgentText(s.WhatBroke))
	s.Why = truncateDetail(sanitizeAgentText(s.Why))
	s.Done = sanitizeHandoffList(s.Done)
	s.RuledOut = sanitizeHandoffList(s.RuledOut)
	s.Open = sanitizeHandoffList(s.Open)
	s.Risks = sanitizeHandoffList(s.Risks)
	return s
}

func sanitizeHandoffList(in []string) []string {
	if len(in) > handoffListMax {
		in = in[:handoffListMax]
	}
	out := make([]string, 0, len(in))
	for _, item := range in {
		out = append(out, truncateDetail(sanitizeAgentText(item)))
	}
	return out
}

// handoffFailTimeout bounds the handoff.failed append. It runs on a context
// detached from the request: a client that went away must not leave the
// request in flight until its expires_at.
const handoffFailTimeout = 10 * time.Second

// HandoffInFlight answers 409 handoff_in_progress while a hand-off request of
// the room is in flight (docs/api.md, Hand-off requests), and nil otherwise,
// also for a room it does not know (the route's own checks answer that). The
// HTTP layer calls it before it reads the body, so the 409 comes before every
// other check but visibility; CreateHandoff checks again atomically with its
// own handoff.requested append.
func (s *Service) HandoffInFlight(ctx context.Context, roomID string) error {
	s.syncRoom(ctx, roomID)
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return nil
	}
	if st.snap.handoffInFlight(s.now()) {
		return handoffInProgress()
	}
	return nil
}

// CreateHandoff asks the agent for a shift summary and emits handoff.created.
//
// The request is logged first (handoff.requested, under s.mu and as a checked
// append, so of two concurrent requests exactly one gets in and the other
// answers 409 handoff_in_progress) and ends with handoff.created or
// handoff.failed for the same request_id. The summary is written outside s.mu
// and may take minutes; meanwhile the fold of handoff.requested keeps every
// other request out, on this replica and, through the log, on the others.
func (s *Service) CreateHandoff(ctx context.Context, user User, roomID, toID string, stayAsWatcher bool) (events.Event, error) {
	if err := s.HandoffInFlight(ctx, roomID); err != nil {
		return events.Event{}, err
	}
	to, err := s.LookupPerson(ctx, toID)
	if err != nil {
		return events.Event{}, err
	}

	s.syncRoom(ctx, roomID)
	s.mu.Lock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		s.mu.Unlock()
		return events.Event{}, err
	}
	now := s.now().UTC()
	requestID := NewID("hr_")
	expiresAt := now.Add(handoffSummaryTimeout)
	_, err = s.emitCheckedLocked(ctx, st, roomID, func() error {
		if st.snap.handoffInFlight(s.now()) {
			return handoffInProgress()
		}
		if !s.roomOpen(st) {
			return roomClosed("room is closed")
		}
		if !s.isDriver(st, user.ID) {
			return ErrNotDriver()
		}
		if to.ID == user.ID {
			return validation("cannot hand off to yourself")
		}
		return s.checkTargetVisible(to, st.snap.PipelineID)
	}, events.HandoffRequested, s.humanActor(user), events.HandoffRequestedPayload{
		RequestID:     requestID,
		To:            s.humanActor(to),
		StayAsWatcher: stayAsWatcher,
		ExpiresAt:     expiresAt,
	})
	s.mu.Unlock()
	if err != nil {
		return events.Event{}, err
	}

	e, err := s.completeHandoff(ctx, user, to, roomID, requestID, stayAsWatcher)
	if err != nil {
		s.failHandoff(ctx, roomID, requestID, handoffFailReason(err))
		return events.Event{}, err
	}
	return e, nil
}

// errSummary marks a failed summary call, for handoffFailReason.
var errSummary = errors.New("summary failed")

// completeHandoff writes the summary and appends handoff.created for
// requestID.
func (s *Service) completeHandoff(ctx context.Context, user, to User, roomID, requestID string, stayAsWatcher bool) (events.Event, error) {
	roomEvents, err := s.allEvents(ctx, roomID)
	if err != nil {
		return events.Event{}, fmt.Errorf("room: handoff: read events: %w", err)
	}
	// The same budget handoff.requested announced as expires_at. A relative
	// timeout, not a deadline at expiresAt: that time is from s.now, which
	// tests (and nothing else) set to another clock.
	wctx, cancel := context.WithTimeout(ctx, handoffSummaryTimeout)
	summary, err := s.writeHandoff(wctx, roomEvents, s.opts)
	cancel()
	if err != nil {
		return events.Event{}, fmt.Errorf("room: handoff: write summary: %w: %w", errSummary, err)
	}
	summary = sanitizeHandoff(summary)

	s.mu.Lock()
	defer s.mu.Unlock()
	st2 := s.rooms[roomID]
	if st2 == nil {
		return events.Event{}, notFound("room not found")
	}
	if !s.roomOpen(st2) {
		return events.Event{}, roomClosed("room is closed")
	}
	if !s.isDriver(st2, user.ID) {
		return events.Event{}, ErrNotDriver()
	}

	handoffID := NewID("h_")
	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		return events.Event{}, fmt.Errorf("room: handoff: marshal summary: %w", err)
	}
	// Row first; if the event append fails we remove the row (compensation).
	if err := s.store.CreateHandoff(ctx, store.HandoffRow{
		HandoffID:     handoffID,
		RoomID:        roomID,
		FromID:        user.ID,
		FromName:      user.Name,
		ToID:          to.ID,
		ToName:        to.Name,
		Summary:       summaryJSON,
		StayAsWatcher: stayAsWatcher,
	}); err != nil {
		return events.Event{}, err
	}
	e, err := s.emitCheckedLocked(ctx, st2, roomID, func() error {
		if !s.roomOpen(st2) {
			return roomClosed("room is closed")
		}
		if !s.isDriver(st2, user.ID) {
			return ErrNotDriver()
		}
		return nil
	}, events.HandoffCreated, s.humanActor(user), events.HandoffCreatedPayload{
		HandoffID:     handoffID,
		RequestID:     requestID,
		To:            s.humanActor(to),
		Summary:       summary,
		StayAsWatcher: stayAsWatcher,
	})
	if err != nil {
		if delErr := s.store.DeleteHandoff(ctx, handoffID); delErr != nil {
			s.log.Warn("room: compensate failed handoff row", "handoff", handoffID, "err", delErr)
		}
		return events.Event{}, err
	}
	s.touchDriverLease(ctx, st2, roomID)
	// A new handoff supersedes every older pending one (review item 10):
	// accepting a superseded handoff is stale, never driver-moving.
	s.afterAppend(roomID, "void older handoffs", s.store.VoidHandoffs(ctx, roomID, handoffID), "handoff", handoffID)
	return e, nil
}

// handoffFailReason maps why a request ended to the fixed handoff.failed
// reason; never error text, which can quote room content.
func handoffFailReason(err error) string {
	var re *Error
	switch {
	case errors.Is(err, errSummary):
		return events.HandoffFailedSummary
	case errors.As(err, &re) && re.Code == codeNotDriver:
		return events.HandoffFailedNotDriver
	case errors.As(err, &re) && re.Code == codeRoomClosed:
		return events.HandoffFailedClosed
	}
	return events.HandoffFailedInternal
}

// failHandoff appends handoff.failed for requestID, ending the request. If
// that append fails too, the request still ends at its expires_at.
func (s *Service) failHandoff(ctx context.Context, roomID, requestID, reason string) {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handoffFailTimeout)
	defer cancel()
	s.syncRoom(fctx, roomID)
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.rooms[roomID]
	if st == nil {
		return
	}
	if _, err := s.emitLocked(fctx, st, roomID, events.HandoffFailed, systemActor, events.HandoffFailedPayload{
		RequestID: requestID, Reason: reason,
	}); err != nil {
		s.log.Warn("room: append handoff.failed; the request ends at its expiry", "room", roomID, "request", requestID, "err", err)
	}
}

// AcceptHandoff completes a hand-off: it moves the driver role
// (control.granted) and emits handoff.accepted. Only the handoff recipient may
// accept, and only while the handoff's creator is still the driver, or while
// the recipient drives because an earlier accept of it moved the role but
// failed before handoff.accepted (the retry then finishes it).
func (s *Service) AcceptHandoff(ctx context.Context, user User, handoffID string) (events.Event, error) {
	s.syncHandoffRoom(ctx, handoffID)
	s.mu.Lock()
	defer s.mu.Unlock()

	h, err := s.store.GetHandoff(ctx, handoffID)
	if err != nil {
		return events.Event{}, mapStoreErr(err)
	}
	if err := s.checkVisibleLocked(ctx, user, h.RoomID); err != nil {
		return events.Event{}, err
	}
	if h.Status == "voided" {
		// A handoff is voided when a newer one supersedes it (review item 10):
		// accepting it can never move the driver role.
		return events.Event{}, staleHandoff("the handoff was voided by a newer one")
	}
	if h.Status != "pending" {
		return events.Event{}, alreadyDecided("handoff already accepted")
	}
	if h.ToID != user.ID {
		return events.Event{}, notRecipient("only the handoff recipient can accept")
	}
	st, err := s.getStateLocked(ctx, h.RoomID)
	if err != nil {
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		return events.Event{}, roomClosed("room is closed")
	}
	// A stale hand-off (the creator is no longer the driver) must not move the
	// role. The recipient may already drive because an earlier accept of
	// this hand-off moved the role and then failed: the log decides.
	if !s.isDriver(st, h.FromID) {
		if s.isDriver(st, h.ToID) {
			return s.finishHandoffLocked(ctx, st, user, h)
		}
		return events.Event{}, staleHandoff("the handoff creator is no longer the driver")
	}
	to := s.humanActor(User{ID: h.ToID, Name: h.ToName})
	lease := s.now().UTC().Add(DriverLease)

	// Checked append: two accepts on two replicas, or a control change
	// elsewhere, cannot both move the role (docs/design/replicas.md 9.1).
	if _, err := s.emitCheckedLocked(ctx, st, h.RoomID, func() error {
		if !s.roomOpen(st) {
			return roomClosed("room is closed")
		}
		if !s.isDriver(st, h.FromID) {
			return staleHandoff("the handoff creator is no longer the driver")
		}
		return nil
	}, events.ControlGranted, s.humanActor(user), events.ControlGrantedPayload{
		To: to, LeaseUntil: lease,
	}); err != nil {
		return events.Event{}, err
	}
	// The role has moved, so nothing below undoes the accept (afterAppend).
	// The row is marked only after handoff.accepted is logged: if that append
	// fails, the row stays pending and the recipient's retry finishes the
	// accept (finishHandoffLocked).
	s.afterAppend(h.RoomID, "persist accepted driver", s.store.UpdateDriver(ctx, h.RoomID, h.ToID, h.ToName, lease), "handoff", handoffID)
	e, err := s.emitLocked(ctx, st, h.RoomID, events.HandoffAccepted, s.humanActor(user), events.HandoffAcceptedPayload{
		HandoffID: handoffID,
	})
	if err != nil {
		return events.Event{}, err
	}
	s.afterAppend(h.RoomID, "mark handoff accepted", s.store.SetHandoffStatus(ctx, handoffID, "accepted"), "handoff", handoffID)
	s.afterAppend(h.RoomID, "void other handoffs", s.store.VoidHandoffs(ctx, h.RoomID, handoffID), "handoff", handoffID)
	return e, nil
}

// finishHandoffLocked answers an accept of a pending hand-off whose recipient
// already drives the room. The handoffs row is written after the events, best
// effort, so the log says how far an earlier accept got:
//   - handoff.accepted is logged: only the row write failed. The row is
//     brought in line and the accept is a repeat (already_decided), as it is
//     when the row says accepted.
//   - the recipient's own control.granted is logged after handoff.created but
//     handoff.accepted is not: the earlier accept moved the role and failed
//     before it finished. The retry finishes it and returns handoff.accepted.
//   - neither: someone else gave the recipient the role (a direct grant), so
//     the hand-off is stale as before.
//
// Other pending hand-offs are not voided here: one created after the role
// moved belongs to the new driver and is live. Caller holds s.mu.
func (s *Service) finishHandoffLocked(ctx context.Context, st *roomState, user User, h store.HandoffRow) (events.Event, error) {
	took, accepted, err := s.handoffInLog(ctx, h)
	if err != nil {
		return events.Event{}, fmt.Errorf("room: accept handoff: read events: %w", err)
	}
	if accepted {
		s.afterAppend(h.RoomID, "mark handoff accepted", s.store.SetHandoffStatus(ctx, h.HandoffID, "accepted"), "handoff", h.HandoffID)
		return events.Event{}, alreadyDecided("handoff already accepted")
	}
	if !took {
		return events.Event{}, staleHandoff("the handoff creator is no longer the driver")
	}
	e, err := s.emitCheckedLocked(ctx, st, h.RoomID, func() error {
		if !s.roomOpen(st) {
			return roomClosed("room is closed")
		}
		if !s.isDriver(st, h.ToID) {
			return staleHandoff("the handoff recipient is no longer the driver")
		}
		return nil
	}, events.HandoffAccepted, s.humanActor(user), events.HandoffAcceptedPayload{
		HandoffID: h.HandoffID,
	})
	if err != nil {
		return events.Event{}, err
	}
	s.afterAppend(h.RoomID, "mark handoff accepted", s.store.SetHandoffStatus(ctx, h.HandoffID, "accepted"), "handoff", h.HandoffID)
	return e, nil
}

// handoffInLog reads the room log for hand-off h: took is true when, after its
// handoff.created, the recipient granted the role to themselves (the
// control.granted an accept writes); accepted is true when a handoff.accepted
// for it is logged.
func (s *Service) handoffInLog(ctx context.Context, h store.HandoffRow) (took, accepted bool, err error) {
	evs, err := s.allEvents(ctx, h.RoomID)
	if err != nil {
		return false, false, err
	}
	created := false
	for _, e := range evs {
		switch e.Type {
		case events.HandoffCreated:
			if p, ok := decode[events.HandoffCreatedPayload](e); ok && p.HandoffID == h.HandoffID {
				created = true
			}
		case events.ControlGranted:
			if p, ok := decode[events.ControlGrantedPayload](e); ok && created && e.Actor.ID == h.ToID && p.To.ID == h.ToID {
				took = true
			}
		case events.HandoffAccepted:
			if p, ok := decode[events.HandoffAcceptedPayload](e); ok && p.HandoffID == h.HandoffID {
				accepted = true
			}
		}
	}
	return took, accepted, nil
}

func (s *Service) allEvents(ctx context.Context, roomID string) ([]events.Event, error) {
	var out []events.Event
	after := int64(0)
	for {
		evs, err := s.store.Since(ctx, roomID, after, 1000)
		if err != nil {
			return nil, err
		}
		if len(evs) == 0 {
			return out, nil
		}
		out = append(out, evs...)
		after = evs[len(evs)-1].Seq
		if len(evs) < 1000 {
			return out, nil
		}
	}
}
