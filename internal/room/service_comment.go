package room

import (
	"context"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/events"
)

// fixNoteWindow is how long after a close a fix note is still accepted: long
// enough for the person who fixed it by hand to write down what they did, short
// enough that a closed room stops changing.
const fixNoteWindow = 24 * time.Hour

// closerLookback is how many events from the end of the log are read first when
// looking for room.closed. A closed room's log ends with a handful of events
// (post-mortem, withdrawals, fix notes), so the close is almost always in this
// window and a long log is not read in full.
const closerLookback = 200

// errFixNoteNotAllowed is 403 not_allowed: the room is closed and the person is
// neither its closer nor a member of its pipeline (the rule of the label route).
func errFixNoteNotAllowed() *Error {
	return &Error{Status: 403, Code: "not_allowed", Msg: "a fix note on a closed room needs the person who closed it or a member of its pipeline"}
}

// AddCommentKind posts a comment of the given kind: "" for a plain comment
// ("@agent" in the text sets mentions_agent) or events.CommentKindFixNote, a
// person's note of what they did outside Piperoom (QA G10), which the
// post-mortem lists under "What was done". Any other kind is 400 validation.
//
// A plain comment needs an open room. A fix note may also go to a closed room,
// for 24 hours after the close, from the person who closed it or a member of
// its pipeline.
func (s *Service) AddCommentKind(ctx context.Context, user User, roomID, stepID, text, kind string) (events.Event, error) {
	switch kind {
	case "", events.CommentKindFixNote:
	default:
		return events.Event{}, validation(`kind must be empty or "fix_note"`)
	}
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
	open := s.roomOpen(st)
	if !open {
		if kind != events.CommentKindFixNote {
			return events.Event{}, roomClosed("room is closed")
		}
		if err := s.checkFixNoteClosedLocked(ctx, st, user); err != nil {
			return events.Event{}, err
		}
	}
	payload := events.CommentAddedPayload{
		CommentID: NewID("c_"),
		StepID:    stepID,
		Text:      text,
		// No agent runs in a closed room: the flag would promise an answer.
		MentionsAgent: open && mentionsAgent(text),
		Kind:          kind,
	}
	e, err := s.emitLocked(ctx, st, roomID, events.CommentAdded, s.humanActor(user), payload)
	if err == nil && payload.MentionsAgent {
		s.ensureAgentLocked(roomID, st)
	}
	return e, err
}

// checkFixNoteClosedLocked decides whether user may add a fix note to the
// closed room st: within fixNoteWindow of the close (409 room_closed after
// that), and a member of the room's pipeline or the person who closed it (403
// not_allowed otherwise). Caller holds st's lock.
func (s *Service) checkFixNoteClosedLocked(ctx context.Context, st *roomState, user User) error {
	if st.snap.ClosedAt == nil {
		return roomClosed("room is closed")
	}
	if s.now().Sub(*st.snap.ClosedAt) > fixNoteWindow {
		return roomClosed("the room closed more than 24 hours ago; fix notes are no longer accepted")
	}
	// Membership first: it needs no read of the log.
	if _, ok := s.canGradeShadow(st.snap.PipelineID, user); ok {
		return nil
	}
	closer, found, err := s.roomCloser(ctx, st.snap.ID, st.snap.LastSeq)
	if err != nil {
		return fmt.Errorf("room: fix note: find who closed the room: %w", err)
	}
	if found && closer.Kind == events.ActorHuman && closer.ID == user.ID {
		return nil
	}
	return errFixNoteNotAllowed()
}

// roomCloser returns the actor of room.closed, reading the end of the log first
// (lastSeq is the room's newest seq) and the whole log only when the close is
// further back than closerLookback events.
func (s *Service) roomCloser(ctx context.Context, roomID string, lastSeq int64) (events.Actor, bool, error) {
	if from := lastSeq - closerLookback; from > 0 {
		a, found, err := s.closerAfter(ctx, roomID, from)
		if err != nil || found {
			return a, found, err
		}
	}
	return s.closerAfter(ctx, roomID, 0)
}

// closerAfter pages through the events after seq and returns the actor of the
// first room.closed.
func (s *Service) closerAfter(ctx context.Context, roomID string, after int64) (events.Actor, bool, error) {
	const page = 1000
	for {
		evs, err := s.store.Since(ctx, roomID, after, page)
		if err != nil {
			return events.Actor{}, false, err
		}
		for _, e := range evs {
			if e.Type == events.RoomClosed {
				return e.Actor, true, nil
			}
		}
		if len(evs) < page {
			return events.Actor{}, false, nil
		}
		after = evs[len(evs)-1].Seq
	}
}
