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
	if err := s.store.CreateHandoff(ctx, store.HandoffRow{
		HandoffID:     handoffID,
		RoomID:        roomID,
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
	if err == nil {
		s.touchDriverLease(ctx, st2, roomID)
	}
	return e, err
}

// AcceptHandoff completes a hand-off: it moves the driver role
// (control.granted) and emits handoff.accepted. Only the handoff recipient may
// accept.
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
	oldDriver := st.snap.Driver
	to := events.Actor{Kind: events.ActorHuman, ID: h.ToID, Name: h.ToName}
	lease := s.now().UTC().Add(DriverLease)

	if _, err := s.emitLocked(ctx, st, h.RoomID, events.ControlGranted, humanActor(user), events.ControlGrantedPayload{
		To: to, LeaseUntil: lease,
	}); err != nil {
		return events.Event{}, err
	}
	_ = s.store.UpdateDriver(ctx, h.RoomID, h.ToID, h.ToName, lease)
	if !h.StayAsWatcher && oldDriver != nil {
		st.snap.Members = removeMember(st.snap.Members, oldDriver.ID)
	}

	if err := s.store.SetHandoffStatus(ctx, handoffID, "accepted"); err != nil {
		return events.Event{}, err
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
