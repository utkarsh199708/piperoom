package room

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// agentMention matches "@agent" as a whole word, case-insensitively: it must
// start the text or follow a non-word character, and must not run on into more
// letters, so "@agentless" or "text@agent" do not wake the agent.
var agentMention = regexp.MustCompile(`(?i)(^|\W)@agent\b`)

// errNoLongerDriver ends a driver's checked steer when another replica moved
// the driver role between the check and the write; the steer then becomes a
// suggestion.
var errNoLongerDriver = errors.New("room: no longer the driver")

// mentionsAgent reports whether a comment addresses the agent.
func mentionsAgent(text string) bool {
	return agentMention.MatchString(text)
}

// AddComment posts a comment; "@agent" in the text sets mentions_agent.
func (s *Service) AddComment(ctx context.Context, user User, roomID, stepID, text string) (events.Event, error) {
	return s.AddCommentKeyed(ctx, user, roomID, stepID, text, "")
}

// AddCommentKeyed is AddComment with the request's idempotency key
// (docs/api.md, Idempotent posts); "" is no key. A key the caller already used
// in the room answers the stored event, or 409 client_id_reused for another
// body, before any other check.
func (s *Service) AddCommentKeyed(ctx context.Context, user User, roomID, stepID, text, clientID string) (events.Event, error) {
	if err := checkClientID(clientID); err != nil {
		return events.Event{}, err
	}
	same := sameComment(stepID, text)
	if e, done, err := s.repeatOf(ctx, roomID, user.ID, clientID, same); done {
		return e, err
	}
	e, err := s.addComment(ctx, user, roomID, stepID, text, clientID)
	if err != nil {
		return s.raceLoser(ctx, roomID, user.ID, clientID, same, err)
	}
	return e, nil
}

func (s *Service) addComment(ctx context.Context, user User, roomID, stepID, text, clientID string) (events.Event, error) {
	if strings.TrimSpace(text) == "" {
		return events.Event{}, validation("text is required")
	}
	if err := rejectUnstorable(stepID, text); err != nil {
		return events.Event{}, err
	}
	if err := checkLen("text", text, MaxMessageChars); err != nil {
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
	payload := events.CommentAddedPayload{
		CommentID:     NewID("c_"),
		StepID:        stepID,
		Text:          text,
		MentionsAgent: mentionsAgent(text),
		ClientID:      clientID,
	}
	e, err := s.emitLocked(ctx, st, roomID, events.CommentAdded, s.humanActor(user), payload)
	if err == nil && payload.MentionsAgent {
		s.ensureAgentLocked(roomID, st)
	}
	return e, err
}

// Steer sends an instruction. The driver's steer goes straight to the agent
// (steer.sent); anyone else's becomes a pending suggestion (steer.suggested).
func (s *Service) Steer(ctx context.Context, user User, roomID, text string) (events.Event, error) {
	return s.SteerKeyed(ctx, user, roomID, text, "")
}

// SteerKeyed is Steer with the request's idempotency key, under the rules of
// AddCommentKeyed. The key spans both routes. A repeat appends nothing, wakes
// nothing and renews no lease; a steer that landed as steer.sent stays that
// event even after the driver changed.
func (s *Service) SteerKeyed(ctx context.Context, user User, roomID, text, clientID string) (events.Event, error) {
	if err := checkClientID(clientID); err != nil {
		return events.Event{}, err
	}
	same := sameSteer(text)
	if e, done, err := s.repeatOf(ctx, roomID, user.ID, clientID, same); done {
		return e, err
	}
	e, err := s.steer(ctx, user, roomID, text, clientID)
	if err != nil {
		// A lost race on a suggestion has already removed its row (the
		// compensation in steer).
		return s.raceLoser(ctx, roomID, user.ID, clientID, same, err)
	}
	return e, nil
}

func (s *Service) steer(ctx context.Context, user User, roomID, text, clientID string) (events.Event, error) {
	if strings.TrimSpace(text) == "" {
		return events.Event{}, validation("text is required")
	}
	if err := rejectUnstorable(text); err != nil {
		return events.Event{}, err
	}
	if err := checkLen("text", text, MaxMessageChars); err != nil {
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
	if s.isDriver(st, user.ID) {
		// Checked append: if another replica moved the driver role in
		// between, the steer is not logged as the driver's
		// (docs/design/replicas.md 9.1); it becomes a suggestion below.
		e, err := s.emitCheckedLocked(ctx, st, roomID, func() error {
			if !s.roomOpen(st) {
				return roomClosed("room is closed")
			}
			if !s.isDriver(st, user.ID) {
				return errNoLongerDriver
			}
			return nil
		}, events.SteerSent, s.humanActor(user), events.SteerSentPayload{Text: text, ClientID: clientID})
		if err == nil {
			s.touchDriverLease(ctx, st, roomID)
			s.ensureAgentLocked(roomID, st)
			return e, nil
		}
		if !errors.Is(err, errNoLongerDriver) {
			return e, err
		}
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
	e, err := s.emitLocked(ctx, st, roomID, events.SteerSuggested, s.humanActor(user), events.SteerSuggestedPayload{
		SuggestionID: suggestionID,
		Text:         text,
		ClientID:     clientID,
	})
	if err != nil {
		dctx, cancel := compensateCtx(ctx)
		defer cancel()
		if delErr := s.store.DeleteSuggestion(dctx, suggestionID); delErr != nil {
			s.log.Warn("room: compensate failed suggestion row", "suggestion", suggestionID, "err", delErr)
		}
		return events.Event{}, err
	}
	return e, nil
}

// SendSuggestion lets the driver forward a pending suggestion to the agent.
func (s *Service) SendSuggestion(ctx context.Context, user User, roomID, suggestionID string) (events.Event, error) {
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
	sugg, err := s.store.GetSuggestion(ctx, suggestionID)
	if err != nil {
		return events.Event{}, mapStoreErr(err)
	}
	if sugg.RoomID != roomID {
		// The same answer as a missing id (mapStoreErr): another room's id,
		// maybe of a hidden pipeline, must not be told apart (tenancy.md §6).
		return events.Event{}, notFound("not found")
	}
	// A second send of an already-sent suggestion is a conflict, not a missing
	// row (docs/api.md:51). A declined one is decided too, and the snapshot
	// check catches a steer.declined whose row status write was lost.
	if sugg.Status != "pending" || !hasPendingSuggestion(st.snap, suggestionID) {
		return events.Event{}, alreadyDecided("suggestion already decided")
	}
	if err := s.store.SetSuggestionStatus(ctx, suggestionID, "sent"); err != nil {
		return events.Event{}, err
	}
	e, err := s.emitCheckedLocked(ctx, st, roomID, func() error {
		if !s.roomOpen(st) {
			return roomClosed("room is closed")
		}
		if !s.isDriver(st, user.ID) {
			return ErrNotDriver()
		}
		// A decline (or send) another replica logged in between, or one
		// whose status write failed, leaves the row pending; the log decides.
		if !hasPendingSuggestion(st.snap, suggestionID) {
			return alreadyDecided("suggestion already decided")
		}
		return nil
	}, events.SteerSent, s.humanActor(user), events.SteerSentPayload{
		SuggestionID: suggestionID,
		From:         suggester(st.snap, suggestionID, sugg),
		Text:         sugg.Text,
	})
	if err != nil {
		// Revert the side-table write so the suggestion stays pending, unless
		// another decision already closed it: the row must not read pending again.
		if isAlreadyDecided(err) {
			return events.Event{}, err
		}
		dctx, cancel := compensateCtx(ctx)
		defer cancel()
		if revErr := s.store.SetSuggestionStatus(dctx, suggestionID, "pending"); revErr != nil {
			s.log.Warn("room: revert suggestion status failed", "suggestion", suggestionID, "err", revErr)
		}
		return events.Event{}, err
	}
	s.touchDriverLease(ctx, st, roomID)
	s.ensureAgentLocked(roomID, st)
	return e, nil
}

// RequestControl asks the current driver for control.
func (s *Service) RequestControl(ctx context.Context, user User, roomID string) (events.Event, error) {
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
	return s.emitLocked(ctx, st, roomID, events.ControlRequested, s.humanActor(user), events.ControlRequestedPayload{})
}

// GrantControl moves the driver role: the driver can grant, or anyone when
// there is no driver or the lease has expired. A non-driver while another
// user's lease is still live gets 409 lease_held (docs/api.md).
func (s *Service) GrantControl(ctx context.Context, user User, roomID, toID string) (events.Event, error) {
	to, err := s.LookupPerson(ctx, toID)
	if err != nil {
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
	if !s.isDriver(st, user.ID) && !s.leaseExpired(st) {
		return events.Event{}, leaseHeld("another user's driver lease is still live")
	}
	if err := s.checkTargetVisible(to, st.snap.PipelineID); err != nil {
		return events.Event{}, err
	}
	lease := s.now().UTC().Add(DriverLease)
	// Checked append: two people taking control on two replicas at once get
	// one driver; the second re-checks against the first's grant
	// (docs/design/replicas.md 9.1).
	e, err := s.emitCheckedLocked(ctx, st, roomID, func() error {
		if !s.roomOpen(st) {
			return roomClosed("room is closed")
		}
		if !s.isDriver(st, user.ID) && !s.leaseExpired(st) {
			return leaseHeld("another user's driver lease is still live")
		}
		return nil
	}, events.ControlGranted, s.humanActor(user), events.ControlGrantedPayload{
		To:         s.humanActor(to),
		LeaseUntil: lease,
	})
	if err != nil {
		return events.Event{}, err
	}
	s.afterAppend(roomID, "persist granted driver", s.store.UpdateDriver(ctx, roomID, to.ID, to.Name, lease))
	return e, nil
}
