package room

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// MaxClientIDLen is the longest client_id a steer or comment may carry
// (docs/api.md, Idempotent posts).
const MaxClientIDLen = 64

var clientIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidClientID reports whether id is a well-formed idempotency key: 1–64
// characters of [A-Za-z0-9_-]. The empty string (no key) is not one.
func ValidClientID(id string) bool {
	return clientIDPattern.MatchString(id)
}

func checkClientID(id string) error {
	if id == "" || ValidClientID(id) {
		return nil
	}
	return validation(fmt.Sprintf("client_id must be 1-%d characters of A-Z, a-z, 0-9, _ or -", MaxClientIDLen))
}

// errClientIDReused is 409 client_id_reused: the key was used for another
// route or another body.
func errClientIDReused() *Error {
	return &Error{Status: 409, Code: "client_id_reused", Msg: "client_id was already used for a different request in this room"}
}

// sameRequest reports whether a stored event answers the current request.
type sameRequest func(events.Event) bool

func sameComment(stepID, text string) sameRequest {
	return func(e events.Event) bool {
		if e.Type != events.CommentAdded {
			return false
		}
		p, err := events.Decode[events.CommentAddedPayload](e)
		// Byte-for-byte text; absent and "" step_id are the same (docs/api.md).
		return err == nil && p.Text == text && p.StepID == stepID
	}
}

func sameSteer(text string) sameRequest {
	return func(e events.Event) bool {
		switch e.Type {
		case events.SteerSent:
			p, err := events.Decode[events.SteerSentPayload](e)
			// A forward never carries a key, so it is never this request.
			return err == nil && p.SuggestionID == "" && p.Text == text
		case events.SteerSuggested:
			p, err := events.Decode[events.SteerSuggestedPayload](e)
			return err == nil && p.Text == text
		}
		return false
	}
}

// keyedPayload reads the key of any payload; nil when it has none.
type keyedPayload struct {
	ClientID *string `json:"client_id"`
}

// eventByClientID finds the event the caller's key was used for, or
// store.ErrNotFound. A store without the index (a test fake) is scanned.
func (s *Service) eventByClientID(ctx context.Context, roomID, userID, clientID string) (events.Event, error) {
	if ids, ok := s.store.(store.ClientIDs); ok {
		return ids.EventByClientID(ctx, roomID, userID, clientID)
	}
	const page = 1000
	var after int64
	for {
		evs, err := s.store.Since(ctx, roomID, after, page)
		if err != nil {
			return events.Event{}, err
		}
		for _, e := range evs {
			if e.Actor.ID != userID {
				continue
			}
			p, err := events.Decode[keyedPayload](e)
			if err == nil && p.ClientID != nil && *p.ClientID == clientID {
				return e, nil
			}
		}
		if len(evs) < page {
			return events.Event{}, fmt.Errorf("room: event by client_id: %w", store.ErrNotFound)
		}
		after = evs[len(evs)-1].Seq
	}
}

// repeatOf is the repeat check of a keyed request: done is true when the key
// was used already, with the stored event (same request) or
// 409 client_id_reused (another route or body). A key not used yet, or no
// key, is not done and the request goes on to its own checks.
func (s *Service) repeatOf(ctx context.Context, roomID, userID, clientID string, same sameRequest) (e events.Event, done bool, err error) {
	if clientID == "" {
		return events.Event{}, false, nil
	}
	e, err = s.eventByClientID(ctx, roomID, userID, clientID)
	if errors.Is(err, store.ErrNotFound) {
		// Unused key, or no such room: the route's own checks answer.
		return events.Event{}, false, nil
	}
	if err != nil {
		return events.Event{}, true, fmt.Errorf("room: look up client_id: %w", err)
	}
	if !same(e) {
		return events.Event{}, true, errClientIDReused()
	}
	return e, true, nil
}

// raceLoser answers a keyed append the store refused because another request
// with the same key landed first (store.ErrClientIDTaken): as a repeat of the
// winner. store.ErrLogFull is checked the same way: Postgres checks the cap
// before the index, so a loser whose winner took the room's last slot sees a
// full log. Any other error is returned as it is.
func (s *Service) raceLoser(ctx context.Context, roomID, userID, clientID string, same sameRequest, appendErr error) (events.Event, error) {
	if clientID == "" || !(errors.Is(appendErr, store.ErrClientIDTaken) || errors.Is(appendErr, store.ErrLogFull)) {
		return events.Event{}, appendErr
	}
	e, done, err := s.repeatOf(ctx, roomID, userID, clientID, same)
	if err != nil {
		return events.Event{}, err
	}
	if !done {
		// A full log with an unused key, or a winner whose room was deleted
		// in between: the append's own answer.
		return events.Event{}, appendErr
	}
	return e, nil
}
