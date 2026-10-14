package room

import "time"

// HandoffInFlightAt exposes the snapshot's hand-off request fold to the
// external tests.
func HandoffInFlightAt(s RoomSnapshot, now time.Time) bool { return s.handoffInFlight(now) }
