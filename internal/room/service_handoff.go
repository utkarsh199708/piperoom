package room

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// CreateHandoff asks the agent for a shift summary and emits handoff.created.
func (s *Service) CreateHandoff(ctx context.Context, user User, roomID, toID string, stayAsWatcher bool) (events.Event, error) {
	to, ok := s.users.Get(toID)
	if !ok {
		return events.Event{}, validation("unknown user to hand off to")
	}

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
	s.mu.Unlock()

	roomEvents, err := s.allEvents(ctx, roomID)
	if err != nil {
		return events.Event{}, fmt.Errorf("room: handoff: read events: %w", err)
	}
	wctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	summary, err := s.writeHandoff(wctx, roomEvents, s.opts)
	cancel()
	if err != nil {
		return events.Event{}, fmt.Errorf("room: handoff: write summary: %w", err)
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
	e, err := s.emitLocked(ctx, st2, roomID, events.HandoffCreated, humanActor(user), events.HandoffCreatedPayload{
		HandoffID:     handoffID,
		To:            humanActor(to),
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
	return e, nil
}

// AcceptHandoff completes a hand-off: it moves the driver role
// (control.granted) and emits handoff.accepted. Only the handoff recipient may
// accept, and only while the handoff's creator is still the driver.
func (s *Service) AcceptHandoff(ctx context.Context, user User, handoffID string) (events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, err := s.store.GetHandoff(ctx, handoffID)
	if err != nil {
		return events.Event{}, mapStoreErr(err)
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
	to := events.Actor{Kind: events.ActorHuman, ID: h.ToID, Name: h.ToName}
	lease := s.now().UTC().Add(DriverLease)

	if _, err := s.emitLocked(ctx, st, h.RoomID, events.ControlGranted, humanActor(user), events.ControlGrantedPayload{
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
	return s.emitLocked(ctx, st, h.RoomID, events.HandoffAccepted, humanActor(user), events.HandoffAcceptedPayload{
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
