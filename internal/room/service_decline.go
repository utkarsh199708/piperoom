package room

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"piperoom/internal/events"
)

// maxDeclineReason caps the driver's note on a declined suggestion, in
// characters (runes), per docs/api.md.
const maxDeclineReason = 500

// DeclineSuggestion lets the driver close a pending suggestion without sending
// it to the agent (steer.declined). A suggestion that is no longer pending,
// sent or declined, answers 409 already_decided exactly like a second send, so
// a retry after a lost response is safe; nothing is appended twice.
func (s *Service) DeclineSuggestion(ctx context.Context, user User, roomID, suggestionID, reason string) (events.Event, error) {
	// Typed text: refused before any read or write, like a steer's text.
	if err := rejectUnstorable(reason); err != nil {
		return events.Event{}, err
	}
	if utf8.RuneCountInString(reason) > maxDeclineReason {
		return events.Event{}, validation(fmt.Sprintf("reason is longer than %d characters", maxDeclineReason))
	}
	// Shared store: fold what other replicas wrote, reading outside s.mu.
	s.syncRoom(ctx, roomID)
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
	if sugg.Status != "pending" {
		return events.Event{}, alreadyDecided("suggestion already decided")
	}
	// The event is written first and the side-table status after it: the log
	// is the truth, and the pending check below reads the snapshot folded from
	// it. With several replicas the checked append re-runs that check after
	// catching up, so a send or decline that landed elsewhere in between turns
	// this one into 409 already_decided instead of a second decision, and
	// nothing has to be rolled back in the suggestions table.
	e, err := s.emitCheckedLocked(ctx, st, roomID, func() error {
		if !s.roomOpen(st) {
			return roomClosed("room is closed")
		}
		if !s.isDriver(st, user.ID) {
			return ErrNotDriver()
		}
		if !hasPendingSuggestion(st.snap, suggestionID) {
			return alreadyDecided("suggestion already decided")
		}
		return nil
	}, events.SteerDeclined, s.humanActor(user), events.SteerDeclinedPayload{
		SuggestionID: suggestionID,
		Reason:       reason,
	})
	if err != nil {
		return events.Event{}, err
	}
	// A failed status write leaves the row saying pending; the snapshot check
	// in send and decline still refuses the suggestion, so this only warns.
	s.afterAppend(roomID, "mark suggestion declined", s.store.SetSuggestionStatus(ctx, suggestionID, "declined"), "suggestion", suggestionID)
	// A driver action renews the driver lease (docs/api.md), as send does.
	s.touchDriverLease(ctx, st, roomID)
	return e, nil
}

// isAlreadyDecided reports whether err is the 409 already_decided answer.
func isAlreadyDecided(err error) bool {
	var re *Error
	return errors.As(err, &re) && re.Code == codeAlreadyDecided
}

// hasPendingSuggestion reports whether the snapshot still lists the suggestion
// as pending, i.e. no steer.sent or steer.declined for it has been folded.
func hasPendingSuggestion(snap RoomSnapshot, suggestionID string) bool {
	for _, sg := range snap.PendingSuggestions {
		if sg.SuggestionID == suggestionID {
			return true
		}
	}
	return false
}
