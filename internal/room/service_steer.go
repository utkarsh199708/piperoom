package room

import (
	"context"
	"regexp"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// agentMention matches "@agent" as a whole word, case-insensitively: it must
// start the text or follow a non-word character, and must not run on into more
// letters, so "@agentless" or "text@agent" do not wake the agent.
var agentMention = regexp.MustCompile(`(?i)(^|\W)@agent\b`)

// mentionsAgent reports whether a comment addresses the agent.
func mentionsAgent(text string) bool {
	return agentMention.MatchString(text)
}

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
		MentionsAgent: mentionsAgent(text),
	}
	e, err := s.emitLocked(ctx, st, roomID, events.CommentAdded, humanActor(user), payload)
	if err == nil && payload.MentionsAgent {
		s.ensureAgentLocked(ctx, roomID, st)
	}
	return e, err
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
			s.ensureAgentLocked(ctx, roomID, st)
		}
		return e, err
	}
	suggestionID := NewID("g_")
	// Row first; if the event append fails we remove the row (compensation).
	if err := s.store.CreateSuggestion(ctx, store.SuggestionRow{
		SuggestionID: suggestionID,
		RoomID:       roomID,
		FromID:       user.ID,
		FromName:     user.Name,
		Text:         text,
	}); err != nil {
		return events.Event{}, err
	}
	e, err := s.emitLocked(ctx, st, roomID, events.SteerSuggested, humanActor(user), events.SteerSuggestedPayload{
		SuggestionID: suggestionID,
		Text:         text,
	})
	if err != nil {
		if delErr := s.store.DeleteSuggestion(ctx, suggestionID); delErr != nil {
			s.log.Warn("room: compensate failed suggestion row", "suggestion", suggestionID, "err", delErr)
		}
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
	if sugg.RoomID != roomID {
		return events.Event{}, notFound("suggestion not found")
	}
	// A second send of an already-sent suggestion is a conflict, not a missing
	// row (docs/api.md:51).
	if sugg.Status != "pending" {
		return events.Event{}, alreadyDecided("suggestion already sent")
	}
	if err := s.store.SetSuggestionStatus(ctx, suggestionID, "sent"); err != nil {
		return events.Event{}, err
	}
	e, err := s.emitLocked(ctx, st, roomID, events.SteerSent, humanActor(user), events.SteerSentPayload{
		SuggestionID: suggestionID,
		Text:         sugg.Text,
	})
	if err != nil {
		// Revert the side-table write so the suggestion stays pending.
		if revErr := s.store.SetSuggestionStatus(ctx, suggestionID, "pending"); revErr != nil {
			s.log.Warn("room: revert suggestion status failed", "suggestion", suggestionID, "err", revErr)
		}
		return events.Event{}, err
	}
	s.touchDriverLease(ctx, st, roomID)
	s.ensureAgentLocked(ctx, roomID, st)
	return e, nil
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
// there is no driver or the lease has expired. A non-driver while another
// user's lease is still live gets 409 lease_held (docs/api.md).
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
	if !s.isDriver(st, user.ID) && !s.leaseExpired(st) {
		return events.Event{}, leaseHeld("another user's driver lease is still live")
	}
	lease := s.now().UTC().Add(DriverLease)
	e, err := s.emitLocked(ctx, st, roomID, events.ControlGranted, humanActor(user), events.ControlGrantedPayload{
		To:         humanActor(to),
		LeaseUntil: lease,
	})
	if err != nil {
		return events.Event{}, err
	}
	if err := s.store.UpdateDriver(ctx, roomID, to.ID, to.Name, lease); err != nil {
		s.log.Warn("room: persist granted driver failed", "room", roomID, "err", err)
	}
	return e, nil
}
