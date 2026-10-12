package watch

import "time"

// Clock is the time source of the watcher. Tests pass a fake so that "for",
// cooldown and the 15-minute "still firing" rule run without sleeping.
type Clock interface {
	Now() time.Time
	// NewTimer returns a timer that fires once after d.
	NewTimer(d time.Duration) Timer
}

// Timer is the part of *time.Timer the watcher uses.
type Timer interface {
	// C delivers the time when the timer fires.
	C() <-chan time.Time
	// Reset re-arms the timer to fire after d. Call it only after the timer
	// fired or was stopped.
	Reset(d time.Duration)
	// Stop disarms the timer.
	Stop()
}

// RealClock returns the wall clock.
func RealClock() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time   { return r.t.C }
func (r realTimer) Reset(d time.Duration) { r.t.Reset(d) }
func (r realTimer) Stop()                 { r.t.Stop() }
