package room

import (
	"context"
	"time"
)

// Test-only knobs for the external room_test package. Each must be set right
// after New, before a room starts an agent, so no goroutine reads the field
// while it changes.

// SetRetryAfterForTest replaces the wait between failed agent and shadow
// attempts (time.After(backoff) in production).
func SetRetryAfterForTest(s *Service, f func(d time.Duration) <-chan time.Time) { s.retryAfter = f }

// SetTriageWaitForTest sets how long a run waits for its first look.
func SetTriageWaitForTest(s *Service, d time.Duration) { s.triageWait = d }

// TriageWaitForTest is the wait New derived from Deps.TriagePassTimeout.
func TriageWaitForTest(s *Service) time.Duration { return s.triageWait }

// AgentStartsForTest is how many agent supervisors the room has started (its
// run generation). A steer starts one under the room's lock before it
// returns, so the count is final when the steer is, unlike the runs a
// supervisor makes later in its own goroutine.
func AgentStartsForTest(s *Service, roomID string) (uint64, error) {
	st, err := s.lockRoom(context.Background(), roomID)
	if err != nil {
		return 0, err
	}
	defer s.unlockRoom(st)
	return st.agentRun, nil
}
