package demo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
)

// Presenter controls for the scripted demo (KAN-398). The --demo server builds
// one Controls, runs Controls.Run as the room's agent and mounts the four HTTP
// routes on it. The controls are dev-only: no other server can build them, so
// they never reach production. Each control is a plain in-memory signal to the
// room's run, and each one appends an agent.message so the log stays truthful.

// A Controls is the room's agent runner in --demo (a bound method value).
var _ agent.Runner = (*Controls)(nil).Run

// errDemoIdle is 409 demo_idle: the room has no scripted run to control (not
// started yet, or the run ended and the supervisor did not restart it).
func errDemoIdle() *room.Error {
	return &room.Error{Status: 409, Code: "demo_idle", Msg: "no scripted demo run is active in this room"}
}

// errDemoFinished is 409 demo_finished: the scripted story has played out and
// is waiting for approvals, so there is no step to skip, pause or resume.
func errDemoFinished() *room.Error {
	return &room.Error{Status: 409, Code: "demo_finished", Msg: "the scripted story has finished; reset to replay it from step 1"}
}

// Controls drives the scripted demo: skip to the next step, pause and resume
// the steps, and reset the room's fake clients and the story. It implements
// agent.Runner and is safe for concurrent use by the HTTP handlers and the
// run goroutine. A nil *Controls is not usable.
type Controls struct {
	fakes *Fakes
	// delay is the step delay; tests shorten it. Zero means stepDelay.
	delay time.Duration

	mu    sync.Mutex
	rooms map[string]*runState
}

// NewControls returns the presenter controls for the --demo server. fakes,
// when not nil, is the client pair the demo actions run over; reset puts it
// back to the state the story starts from.
func NewControls(fakes *Fakes) *Controls {
	return &Controls{fakes: fakes, delay: stepDelay, rooms: map[string]*runState{}}
}

// Run implements agent.Runner. It registers the room's control state, replays
// the story, and restarts it whenever the driver resets.
func (c *Controls) Run(ctx context.Context, alert agent.Alert, deps agent.Deps, opts agent.Options) error {
	rc := newRunState()
	if deps.Sink != nil {
		rc.setEmitter(func(typ events.Type, payload any) error {
			return deps.Sink.Emit(ctx, typ, payload)
		})
	}
	c.register(alert.RoomID, rc)
	defer c.unregister(alert.RoomID, rc)
	return runStory(ctx, rc, c.fakes, deps, c.delay)
}

// Skip advances the scripted story to the next step without waiting for the
// current step's remaining delay.
func (c *Controls) Skip(roomID string) error {
	rc := c.state(roomID)
	if rc == nil {
		return errDemoIdle()
	}
	return rc.skip()
}

// Pause holds the story before the next step. The step in flight finishes.
func (c *Controls) Pause(roomID string) error {
	rc := c.state(roomID)
	if rc == nil {
		return errDemoIdle()
	}
	return rc.pause()
}

// Resume lets a paused story continue with the step after the hold.
func (c *Controls) Resume(roomID string) error {
	rc := c.state(roomID)
	if rc == nil {
		return errDemoIdle()
	}
	return rc.resume()
}

// Reset restores the fake clients (when the server wired them) and replays the
// story from step 1 in the same room. The current run is not cancelled: the
// running goroutine rewinds, so the room keeps one agent run.
func (c *Controls) Reset(roomID string) error {
	rc := c.state(roomID)
	if rc == nil {
		return errDemoIdle()
	}
	return rc.reset()
}

func (c *Controls) state(roomID string) *runState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rooms[roomID]
}

func (c *Controls) register(roomID string, rc *runState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rooms[roomID] = rc
}

// unregister removes the room's state only when this run still owns it: a
// newer run of the same room (a restart) keeps its own entry.
func (c *Controls) unregister(roomID string, rc *runState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rooms[roomID] == rc {
		delete(c.rooms, roomID)
	}
}

// runWait is the outcome of one scripted wait.
type runWait int

const (
	// waitDone: the context ended or the room closed; the run returns nil.
	waitDone runWait = iota
	// waitReady: the story may go on.
	waitReady
	// waitReset: a presenter asked to replay from step 1.
	waitReset
)

// runState is one room's scripted run: its pause, skip and reset signals. The
// zero value is not usable; use newRunState.
type runState struct {
	// wake is signalled on every control change; capacity one, so signals
	// coalesce.
	wake chan struct{}

	mu     sync.Mutex
	emit   func(events.Type, any) error
	paused bool
	// fast is the number of skips waiting for a step start: each step start
	// consumes one. While it is positive every wait returns at once, so a skip
	// fast-forwards to the next step start.
	fast int
	// resets counts replay requests; the run drains it when it rewinds.
	resets int
	// done is true once the story reaches its end (the fix proposal) and only
	// the idle wait is left: skip, pause and resume have nothing to act on.
	done bool
	// step is the 1-based index of the step in flight, 0 before the first.
	step int
}

func newRunState() *runState {
	return &runState{wake: make(chan struct{}, 1)}
}

func (r *runState) setEmitter(emit func(events.Type, any) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.emit = emit
}

// emitMessage appends one presenter message. Caller holds r.mu (so a control
// event lands before the run can react to it) and r.emit may be nil (the
// package-level Run has no sink of its own to promise).
func (r *runState) emitMessage(text string) error {
	if r.emit == nil {
		return nil
	}
	return r.emit(events.AgentMessage, events.AgentMessagePayload{Text: text})
}

func (r *runState) skip() error {
	r.mu.Lock()
	if r.done {
		r.mu.Unlock()
		return errDemoFinished()
	}
	msg := "demo: skipped to the end of the story"
	if r.step < len(story) {
		msg = fmt.Sprintf("demo: skipped to step %d", r.step+1)
	}
	if err := r.emitMessage(msg); err != nil {
		r.mu.Unlock()
		return err
	}
	r.fast++
	r.mu.Unlock()
	r.signal()
	return nil
}

func (r *runState) pause() error {
	r.mu.Lock()
	if r.done {
		r.mu.Unlock()
		return errDemoFinished()
	}
	if r.paused {
		r.mu.Unlock()
		return nil
	}
	if err := r.emitMessage("demo: paused — the scripted steps are on hold until resume"); err != nil {
		r.mu.Unlock()
		return err
	}
	r.paused = true
	r.mu.Unlock()
	r.signal()
	return nil
}

func (r *runState) resume() error {
	r.mu.Lock()
	if r.done {
		r.mu.Unlock()
		return errDemoFinished()
	}
	if !r.paused {
		r.mu.Unlock()
		return nil
	}
	if err := r.emitMessage("demo: resumed"); err != nil {
		r.mu.Unlock()
		return err
	}
	r.paused = false
	r.mu.Unlock()
	r.signal()
	return nil
}

func (r *runState) reset() error {
	r.mu.Lock()
	if err := r.emitMessage("demo: reset — the fake clients are back at their starting state; replaying from step 1"); err != nil {
		r.mu.Unlock()
		return err
	}
	r.resets++
	r.mu.Unlock()
	r.signal()
	return nil
}

func (r *runState) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *runState) resetPending() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.resets > 0
}

// takeReset drains every pending replay request: several presses before the
// run noticed collapse into one replay.
func (r *runState) takeReset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resets = 0
}

// restartStory clears the control state for a replay from step 1.
func (r *runState) restartStory() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paused = false
	r.fast = 0
	r.done = false
	r.step = 0
}

// markIdle records that the steps are over and only the idle wait is left.
func (r *runState) markIdle() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done = true
}

// beginStep records the step that is starting and consumes one skip request.
func (r *runState) beginStep(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.step = n
	if r.fast > 0 {
		r.fast--
	}
}

// hold sleeps for d. A skip returns at once, a reset ends the replay
// (waitReset), and a cancelled context or a closed room ends the run.
func (r *runState) hold(ctx context.Context, d time.Duration) runWait {
	if ctx.Err() != nil {
		return waitDone
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		r.mu.Lock()
		switch {
		case r.resets > 0:
			r.mu.Unlock()
			return waitReset
		case r.fast > 0:
			r.mu.Unlock()
			return waitReady
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return waitDone
		case <-timer.C:
			return waitReady
		case <-r.wake:
		}
	}
}

// boundary holds the story before the next step while paused. A skip while
// paused lets exactly one step through and holds again before the one after.
func (r *runState) boundary(ctx context.Context) (res runWait, stillPaused bool) {
	if ctx.Err() != nil {
		return waitDone, false
	}
	for {
		r.mu.Lock()
		switch {
		case r.resets > 0:
			r.mu.Unlock()
			return waitReset, false
		case !r.paused:
			r.mu.Unlock()
			return waitReady, false
		case r.fast > 0:
			r.mu.Unlock()
			return waitReady, true
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return waitDone, false
		case <-r.wake:
		}
	}
}
