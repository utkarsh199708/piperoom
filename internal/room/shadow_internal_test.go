package room

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/store/memstore"
)

// beginShadowAttempt is the re-check right after gate.acquire: with the slot in
// hand, no attempt starts on a closed room. End to end this only matters when
// a slot is granted and the close lands before the supervisor wakes (acquire's
// select can see both ready), which a test cannot schedule, so it is pinned
// here.
func TestBeginShadowAttemptRefusesClosedRoom(t *testing.T) {
	s := New(Deps{Store: memstore.New()})
	t.Cleanup(s.Close)
	st := &roomState{snap: NewSnapshot()}
	st.snap.Status = "open"
	if !s.beginShadowAttempt(st) || !st.shadowInAttempt {
		t.Fatal("an open room must start the attempt and mark it running")
	}
	s.endShadowAttempt(st)
	if st.shadowInAttempt {
		t.Fatal("endShadowAttempt must clear the mark")
	}
	at := time.Now()
	st.snap.Status, st.snap.ClosedAt = "closed", &at
	if s.beginShadowAttempt(st) || st.shadowInAttempt {
		t.Fatal("a closed room must not start an attempt")
	}
}

// closeShadowLocked gives grace only to a running attempt; a queued or
// backing-off run gets its cancel back at once.
func TestCloseShadowGraceOnlyForRunningAttempt(t *testing.T) {
	s := New(Deps{Store: memstore.New(), ShadowGrace: time.Hour})
	t.Cleanup(s.Close)
	cancelled := false
	cancel := context.CancelFunc(func() { cancelled = true })

	idle := &roomState{snap: NewSnapshot(), shadowActive: true}
	if got := s.closeShadowLocked(context.Background(), "r", idle, cancel); got == nil {
		t.Fatal("a run between attempts must be cancelled at close")
	}
	running := &roomState{snap: NewSnapshot(), shadowActive: true, shadowInAttempt: true}
	if got := s.closeShadowLocked(context.Background(), "r", running, cancel); got != nil || running.graceTimer == nil {
		t.Fatal("a running attempt must get the grace timer, not a cancel")
	}
	running.graceTimer.Stop()
	if cancelled {
		t.Fatal("nothing should have been cancelled yet")
	}
}
