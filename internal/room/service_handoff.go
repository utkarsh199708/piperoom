package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
//
// A summary the agent cannot write (a model error, a timeout, an incomplete
// summary) ends the request with handoff.failed {summary_failed} and answers
// 503 summary_failed, so the driver can retry or write the summary themselves
// (CreateHandoffManual). Only a store failure is a 500.
func (s *Service) CreateHandoff(ctx context.Context, user User, roomID, toID string, stayAsWatcher bool) (events.Event, error) {
	return s.createHandoff(ctx, user, roomID, toID, stayAsWatcher, nil)
}

// CreateHandoffManual is CreateHandoff with a summary the driver wrote (QA G4:
// the agent's attempt failed, or the driver knows better). No model call is
// made and handoff.created has manual: true. Every other rule of the request
// is the same: handoff.requested, the in-flight rule, the voiding of older
// hand-offs, accept. The summary is validated before anything is written
// (400 validation) and its Violations are never set: no check ran.
func (s *Service) CreateHandoffManual(ctx context.Context, user User, roomID, toID string, stayAsWatcher bool, summary events.HandoffSummary) (events.Event, error) {
	return s.createHandoff(ctx, user, roomID, toID, stayAsWatcher, &summary)
}

// createHandoff is the shared body; manual is nil for an agent-written summary.
func (s *Service) createHandoff(ctx context.Context, user User, roomID, toID string, stayAsWatcher bool, manual *events.HandoffSummary) (events.Event, error) {
	if err := s.HandoffInFlight(ctx, roomID); err != nil {
		return events.Event{}, err
	}
	if manual != nil {
		clean, err := normalizeManualSummary(*manual)
		if err != nil {
			return events.Event{}, err
		}
		manual = &clean
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

	e, err := s.completeHandoff(ctx, user, to, roomID, requestID, stayAsWatcher, manual)
	if err != nil {
		s.failHandoff(ctx, roomID, requestID, handoffFailReason(err))
		return events.Event{}, err
	}
	return e, nil
}

// summaryFailed is 503 summary_failed: the agent could not write the hand-off
// summary (docs/api.md, error table). It carries no cause: model and tool
// errors can quote room content, so the cause goes to the server log only.
func summaryFailed() *Error {
	return &Error{
		Status: 503, Code: events.HandoffFailedSummary,
		Msg: "the agent could not write the hand-off summary; try again, or write the summary yourself and send it with the hand-off",
	}
}

// Limits of a driver-written summary (docs/api.md, POST /api/rooms/{id}/handoffs).
const (
	maxManualSummaryChars = 1000 // what_broke and why
	maxManualItemChars    = 500  // one item of done, ruled_out, open or risks
	maxManualListItems    = 20   // items per list
)

// normalizeManualSummary validates a driver-written summary and returns the
// form that is stored: trimmed text, blank list items dropped, lists never
// nil, no Violations. It refuses with 400 validation and never quotes the text.
func normalizeManualSummary(in events.HandoffSummary) (events.HandoffSummary, error) {
	var out events.HandoffSummary
	var err error
	if out.WhatBroke, err = manualText("what_broke", in.WhatBroke); err != nil {
		return events.HandoffSummary{}, err
	}
	if out.Why, err = manualText("why", in.Why); err != nil {
		return events.HandoffSummary{}, err
	}
	for _, l := range []struct {
		name string
		in   []string
		out  *[]string
	}{
		{"done", in.Done, &out.Done},
		{"ruled_out", in.RuledOut, &out.RuledOut},
		{"open", in.Open, &out.Open},
		{"risks", in.Risks, &out.Risks},
	} {
		if *l.out, err = manualList(l.name, l.in); err != nil {
			return events.HandoffSummary{}, err
		}
	}
	return out, nil
}

// manualText checks one required text of a manual summary.
func manualText(field, s string) (string, error) {
	if err := rejectUnstorable(s); err != nil {
		return "", err
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return "", validation(field + " is required")
	}
	if err := checkLen(field, s, maxManualSummaryChars); err != nil {
		return "", err
	}
	return s, nil
}

// manualList checks one list of a manual summary. A blank item is dropped, not
// refused: a form that sends one empty line should not fail the hand-off.
func manualList(field string, in []string) ([]string, error) {
	if err := rejectUnstorable(in...); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if err := checkLen(field+" item", item, maxManualItemChars); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if len(out) > maxManualListItems {
		return nil, validation(fmt.Sprintf("%s has %d items, the limit is %d", field, len(out), maxManualListItems))
	}
	return out, nil
}

// agentSummary asks the agent for the hand-off summary of roomID. Every
// failure of the writer (a model error, a timeout, a summary without
// what_broke) is summaryFailed; only a failure to read the log is an internal
// error.
func (s *Service) agentSummary(ctx context.Context, roomID, requestID string) (events.HandoffSummary, error) {
	roomEvents, err := s.allEvents(ctx, roomID)
	if err != nil {
		return events.HandoffSummary{}, fmt.Errorf("room: handoff: read events: %w", err)
	}
	// The same budget handoff.requested announced as expires_at. A relative
	// timeout, not a deadline at expiresAt: that time is from s.now, which
	// tests (and nothing else) set to another clock.
	wctx, cancel := context.WithTimeout(ctx, handoffSummaryTimeout)
	summary, err := s.writeHandoff(wctx, roomEvents, s.opts)
	cancel()
	if err != nil {
		s.log.Warn("room: hand-off summary failed", "room", roomID, "request", requestID, "err", err)
		return events.HandoffSummary{}, summaryFailed()
	}
	summary = sanitizeHandoff(summary)
	// The writer is meant to refuse an incomplete summary itself; a hand-off
	// with no headline would still reach the recipient, so check here too.
	if strings.TrimSpace(summary.WhatBroke) == "" {
		s.log.Warn("room: hand-off summary has no what_broke", "room", roomID, "request", requestID)
		return events.HandoffSummary{}, summaryFailed()
	}
	return summary, nil
}

// completeHandoff gets the summary (the driver's, or the agent's) and appends
// handoff.created for requestID.
func (s *Service) completeHandoff(ctx context.Context, user, to User, roomID, requestID string, stayAsWatcher bool, manual *events.HandoffSummary) (events.Event, error) {
	var summary events.HandoffSummary
	if manual != nil {
		summary = *manual
	} else {
		var err error
		if summary, err = s.agentSummary(ctx, roomID, requestID); err != nil {
			return events.Event{}, err
		}
	}

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
		Manual:        manual != nil,
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
	if err := s.store.VoidHandoffs(ctx, roomID, handoffID); err != nil {
		s.log.Warn("room: void older handoffs failed", "room", roomID, "handoff", handoffID, "err", err)
	}
	return e, nil
}

// handoffFailReason maps why a request ended to the fixed handoff.failed
// reason; never error text, which can quote room content.
func handoffFailReason(err error) string {
	var re *Error
	switch {
	case errors.As(err, &re) && re.Code == events.HandoffFailedSummary:
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
// accept, and only while the handoff's creator is still the driver.
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
	// role.
	if !s.isDriver(st, h.FromID) {
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
	if err := s.store.UpdateDriver(ctx, h.RoomID, h.ToID, h.ToName, lease); err != nil {
		s.log.Warn("room: persist accepted driver failed", "room", h.RoomID, "err", err)
	}
	if err := s.store.SetHandoffStatus(ctx, handoffID, "accepted"); err != nil {
		return events.Event{}, err
	}
	if err := s.store.VoidHandoffs(ctx, h.RoomID, handoffID); err != nil {
		s.log.Warn("room: void other handoffs failed", "room", h.RoomID, "err", err)
	}
	return s.emitLocked(ctx, st, h.RoomID, events.HandoffAccepted, s.humanActor(user), events.HandoffAcceptedPayload{
		HandoffID: handoffID,
	})
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
