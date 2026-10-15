package room

import "time"

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
