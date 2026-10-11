package room

import (
	"context"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// AddComment posts a comment; "@agent" in the text sets mentions_agent.
func (s *Service) AddComment(ctx context.Context, user User, roomID, stepID, text string) (events.Event, error) {
	if strings.TrimSpace(text) == "" {
		return events.Event{}, validation("text is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		return events.Event{}, roomClosed("room is closed")
	}
	payload := events.CommentAddedPayload{
		CommentID:     NewID("c_"),
		StepID:        stepID,
		Text:          text,
		MentionsAgent: strings.Contains(text, "@agent"),
	}
	return s.emitLocked(ctx, st, roomID, events.CommentAdded, humanActor(user), payload)
}

// Steer sends an instruction. The driver's steer goes straight to the agent
// (steer.sent); anyone else's becomes a pending suggestion (steer.suggested).
func (s *Service) Steer(ctx context.Context, user User, roomID, text string) (events.Event, error) {
	if strings.TrimSpace(text) == "" {
		return events.Event{}, validation("text is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		return events.Event{}, roomClosed("room is closed")
	}
	if s.isDriver(st, user.ID) {
		e, err := s.emitLocked(ctx, st, roomID, events.SteerSent, humanActor(user), events.SteerSentPayload{Text: text})
		if err == nil {
			s.touchDriverLease(ctx, st, roomID)
		}
		return e, err
	}
	suggestionID := NewID("g_")
	e, err := s.emitLocked(ctx, st, roomID, events.SteerSuggested, humanActor(user), events.SteerSuggestedPayload{
		SuggestionID: suggestionID,
		Text:         text,
	})
	if err != nil {
		return events.Event{}, err
	}
	if err := s.store.CreateSuggestion(ctx, store.SuggestionRow{
		SuggestionID: suggestionID,
		RoomID:       roomID,
		FromID:       user.ID,
		FromName:     user.Name,
		Text:         text,
	}); err != nil {
		return events.Event{}, err
	}
	return e, nil
}

// SendSuggestion lets the driver forward a pending suggestion to the agent.
func (s *Service) SendSuggestion(ctx context.Context, user User, roomID, suggestionID string) (events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		return events.Event{}, roomClosed("room is closed")
	}
	if !s.isDriver(st, user.ID) {
		return events.Event{}, ErrNotDriver()
	}
	sugg, err := s.store.GetSuggestion(ctx, suggestionID)
	if err != nil {
		return events.Event{}, mapStoreErr(err)
	}
	if sugg.RoomID != roomID || sugg.Status != "pending" {
		return events.Event{}, notFound("suggestion not found")
	}
	if err := s.store.SetSuggestionStatus(ctx, suggestionID, "sent"); err != nil {
		return events.Event{}, err
	}
	e, err := s.emitLocked(ctx, st, roomID, events.SteerSent, humanActor(user), events.SteerSentPayload{
		SuggestionID: suggestionID,
		Text:         sugg.Text,
	})
	if err == nil {
		s.touchDriverLease(ctx, st, roomID)
	}
	return e, err
}

// RequestControl asks the current driver for control.
func (s *Service) RequestControl(ctx context.Context, user User, roomID string) (events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		return events.Event{}, roomClosed("room is closed")
	}
	return s.emitLocked(ctx, st, roomID, events.ControlRequested, humanActor(user), events.ControlRequestedPayload{})
}

// GrantControl moves the driver role: the driver can grant, or anyone when
// there is no driver or the lease has expired.
func (s *Service) GrantControl(ctx context.Context, user User, roomID, toID string) (events.Event, error) {
	to, ok := s.users.Get(toID)
	if !ok {
		return events.Event{}, validation("unknown user to grant control to")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		return events.Event{}, roomClosed("room is closed")
	}
	if !s.leaseExpired(st) && !s.isDriver(st, user.ID) {
		return events.Event{}, ErrNotDriver()
	}
	lease := s.now().UTC().Add(DriverLease)
	e, err := s.emitLocked(ctx, st, roomID, events.ControlGranted, humanActor(user), events.ControlGrantedPayload{
		To:         humanActor(to),
		LeaseUntil: lease,
	})
	if err != nil {
		return events.Event{}, err
	}
	_ = s.store.UpdateDriver(ctx, roomID, to.ID, to.Name, lease)
	return e, nil
}
