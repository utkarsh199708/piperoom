package room

import (
	"context"
	"fmt"
	"unicode/utf8"

	"piperoom/internal/events"
)

// maxVerdictNote caps the note of a hypothesis verdict, in characters.
const maxVerdictNote = 500

// DecideHypothesis records a member's confirm or dispute of the agent's current
// hypothesis (hypothesis.verdict). Any member may; the verdict is domain
// knowledge that usually sits with a layer owner, not the driver. The same
// person repeating the same verdict on the same hypothesis answers 409
// already_decided; a changed mind is a new event.
func (s *Service) DecideHypothesis(ctx context.Context, user User, roomID, verdict, note string) (events.Event, error) {
	if verdict != "confirmed" && verdict != "disputed" {
		return events.Event{}, validation(`verdict must be "confirmed" or "disputed"`)
	}
	if err := rejectUnstorable(note); err != nil {
		return events.Event{}, err
	}
	if utf8.RuneCountInString(note) > maxVerdictNote {
		return events.Event{}, validation(fmt.Sprintf("note is longer than %d characters", maxVerdictNote))
	}
	s.syncRoom(ctx, roomID)
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	// The checked append re-runs this after catching up with other replicas,
	// so two replicas cannot both pass the duplicate check.
	var hypSeq int64 // set by the first check; the payload carries it
	check := func() error {
		if !s.roomOpen(st) {
			return roomClosed("room is closed")
		}
		// A shadow room's hypothesis stays hidden until the reveal.
		if st.snap.Shadow {
			return errRoomOpen()
		}
		h := st.snap.Hypothesis
		if h == nil {
			return noHypothesis()
		}
		for _, v := range h.Verdicts {
			if v.By.ID == user.ID && v.Verdict == verdict {
				return alreadyDecided("you already gave this verdict on the current hypothesis")
			}
		}
		if hypSeq != 0 && h.Seq != hypSeq {
			// A new hypothesis landed on another replica while we checked:
			// the verdict would answer the wrong one. The client may retry.
			return errConflict()
		}
		hypSeq = h.Seq
		return nil
	}
	if err := check(); err != nil {
		return events.Event{}, err
	}
	e, err := s.emitCheckedLocked(ctx, st, roomID, check, events.HypothesisVerdict, s.humanActor(user), events.HypothesisVerdictPayload{Verdict: verdict, Note: note, HypothesisSeq: hypSeq})
	if err != nil {
		return events.Event{}, err
	}
	if s.isDriver(st, user.ID) {
		s.touchDriverLease(ctx, st, roomID)
	}
	// Like an @agent comment, the verdict wakes the agent.
	s.ensureAgentLocked(ctx, roomID, st)
	return e, nil
}
