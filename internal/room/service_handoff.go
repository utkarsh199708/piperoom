package room

import (
	"context"
	"encoding/json"
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

// CreateHandoff asks the agent for a shift summary and emits handoff.created.
func (s *Service) CreateHandoff(ctx context.Context, user User, roomID, toID string, stayAsWatcher bool) (events.Event, error) {
	to, err := s.LookupPerson(ctx, toID)
	if err != nil {
		return events.Event{}, err
	}

	// Shared store: fold what other replicas wrote, reading outside s.mu.
	s.syncRoom(ctx, roomID)
	s.mu.Lock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		s.mu.Unlock()
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		s.mu.Unlock()
		return events.Event{}, roomClosed("room is closed")
	}
	if !s.isDriver(st, user.ID) {
		s.mu.Unlock()
		return events.Event{}, ErrNotDriver()
	}
	if to.ID == user.ID {
		s.mu.Unlock()
		return events.Event{}, validation("cannot hand off to yourself")
	}
	if err := s.checkTargetVisible(to, st.snap.PipelineID); err != nil {
		s.mu.Unlock()
		return events.Event{}, err
	}
	s.mu.Unlock()

	roomEvents, err := s.allEvents(ctx, roomID)
	if err != nil {
		return events.Event{}, fmt.Errorf("room: handoff: read events: %w", err)
	}
	wctx, cancel := context.WithTimeout(ctx, handoffSummaryTimeout)
	summary, err := s.writeHandoff(wctx, roomEvents, s.opts)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return events.Event{}, fmt.Errorf("room: handoff: write summary: %w", err)
		}
		// The model is down, out of credentials or timed out. The shift
		// change must still happen (QA sweep 2026-10-14): hand over with a
		// summary built from the log alone, marked as such. The model error
		// is logged here and never shown in the room.
		s.log.Warn("room: handoff summary from the model failed; using the auto summary", "room", roomID, "err", err)
		summary = autoHandoffSummary(roomEvents)
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
	if err := s.store.VoidHandoffs(ctx, roomID, handoffID); err != nil {
		s.log.Warn("room: void older handoffs failed", "room", roomID, "handoff", handoffID, "err", err)
	}
	return e, nil
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

// autoSummaryMark starts the what_broke of a hand-off summary written without
// the model, so the recipient knows nobody reasoned over it.
const autoSummaryMark = "auto summary (model unavailable)"

// autoHandoffSummary is the hand-off summary when the model cannot write one:
// built from the room log only, deterministically. It carries the room title,
// the latest hypothesis, the proposals still waiting for a decision and the
// most recent investigation steps.
func autoHandoffSummary(roomEvents []events.Event) events.HandoffSummary {
	var snap RoomSnapshot
	var steps []string
	for _, e := range roomEvents {
		snap = Apply(snap, e)
		if e.Type == events.AgentStepStart {
			if p, ok := decode[events.AgentStepStartPayload](e); ok && strings.TrimSpace(p.Title) != "" {
				steps = append(steps, p.Title)
			}
		}
	}
	sum := events.HandoffSummary{
		WhatBroke: autoSummaryMark,
		Why:       "No hypothesis yet.",
		Done:      []string{},
		RuledOut:  []string{},
		Open:      []string{},
		Risks:     []string{"This summary was not written by the agent; read the room log before acting."},
	}
	if t := strings.TrimSpace(snap.Title); t != "" {
		sum.WhatBroke = autoSummaryMark + ": " + t
	}
	if h := snap.Hypothesis; h != nil && strings.TrimSpace(h.Text) != "" {
		conf := h.Confidence
		if conf == "" {
			conf = "unknown"
		}
		sum.Why = fmt.Sprintf("Latest hypothesis (%s confidence): %s", conf, h.Text)
	}
	// The most recent steps, newest last.
	if len(steps) > handoffListMax {
		steps = steps[len(steps)-handoffListMax:]
	}
	for _, t := range steps {
		sum.Done = append(sum.Done, "Step: "+t)
	}
	for _, p := range snap.OpenProposals {
		var waiting []string
		for _, layer := range p.RequiredLayers {
			if p.Approvals[layer] != "approved" {
				waiting = append(waiting, layer)
			}
		}
		item := "Proposal " + p.ProposalID + ": " + p.Title
		if len(waiting) > 0 {
			item += " (waiting for " + strings.Join(waiting, ", ") + ")"
		}
		sum.Open = append(sum.Open, item)
	}
	if n := len(snap.PendingSuggestions); n > 0 {
		sum.Open = append(sum.Open, fmt.Sprintf("%d pending suggestion(s) not yet sent to the agent", n))
	}
	return sum
}
