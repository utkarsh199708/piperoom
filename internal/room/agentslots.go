package room

import (
	"context"
	"fmt"
	"sync"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// DefaultMaxAgents is the cap on concurrent agent runs when Deps.MaxAgents is
// not set. One Connect failure can open dozens of rooms at once; without a
// cap each of them would start an Opus run.
const DefaultMaxAgents = 3

// slotWaiter is one room's place in the agent queue.
type slotWaiter struct {
	ready   chan struct{} // closed when a slot is granted
	granted bool          // guarded by agentSlots.mu
}

// agentSlots is the global limit on concurrent agent runs across all rooms. A
// room that wants to run asks for a slot with request; when none is free it
// waits in a FIFO queue and is granted one as slots are released. It never
// calls back into the Service, so it is safe to use while holding Service.mu.
type agentSlots struct {
	mu    sync.Mutex
	max   int
	inUse int
	queue []*slotWaiter
}

func newAgentSlots(max int) *agentSlots {
	if max < 1 {
		max = DefaultMaxAgents
	}
	return &agentSlots{max: max}
}

// request takes a slot if one is free and nobody is queued ahead, otherwise it
// joins the back of the queue. ahead is the number of rooms queued before the
// caller.
func (a *agentSlots) request() (w *slotWaiter, ahead int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w = &slotWaiter{ready: make(chan struct{})}
	if a.inUse < a.max && len(a.queue) == 0 {
		a.inUse++
		w.granted = true
		close(w.ready)
		return w, 0
	}
	ahead = len(a.queue)
	a.queue = append(a.queue, w)
	return w, ahead
}

// isWaiting reports whether w is still queued (not yet granted).
func (a *agentSlots) isWaiting(w *slotWaiter) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return !w.granted
}

// release returns one slot and grants it to the head of the queue.
func (a *agentSlots) release() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inUse--
	a.pumpLocked()
}

// cancel withdraws w: a queued waiter leaves the queue; a waiter that was
// granted a slot in the meantime gives it back. Either way w is done.
func (a *agentSlots) cancel(w *slotWaiter) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if w.granted {
		a.inUse--
		a.pumpLocked()
		return
	}
	for i, q := range a.queue {
		if q == w {
			a.queue = append(a.queue[:i], a.queue[i+1:]...)
			return
		}
	}
}

func (a *agentSlots) pumpLocked() {
	for a.inUse < a.max && len(a.queue) > 0 {
		w := a.queue[0]
		a.queue[0] = nil
		a.queue = a.queue[1:]
		w.granted = true
		a.inUse++
		close(w.ready)
	}
}

// stats returns slots in use and rooms queued (tests, logging).
func (a *agentSlots) stats() (inUse, queued int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inUse, len(a.queue)
}

// agentGate is one room's handle on the global slots. The room holds a slot
// while its agent can call the model and gives it back while the agent waits
// for input, so rooms parked on a human do not starve rooms that need to run.
// gatedInbox drives it.
type agentGate struct {
	s      *Service
	st     *roomState
	roomID string

	mu   sync.Mutex
	held bool
	w    *slotWaiter // the pending request, shared by concurrent acquires
}

func (g *agentGate) isHeld() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.held
}

// acquire blocks until the room holds a slot or ctx ends. While the room has to
// wait it shows agent.status "queued". Concurrent callers share one queue
// entry, so a room is never queued twice.
func (g *agentGate) acquire(ctx context.Context) error {
	g.mu.Lock()
	if g.held {
		g.mu.Unlock()
		return nil
	}
	w, owner, ahead := g.w, false, 0
	if w == nil {
		w, ahead = g.s.slots.request()
		g.w, owner = w, true
		if ahead == 0 && !g.s.slots.isWaiting(w) {
			g.held, g.w = true, nil
			g.mu.Unlock()
			return nil
		}
	}
	g.mu.Unlock()

	shown := false
	if owner {
		shown = g.s.emitQueued(g, w, ahead)
	}
	select {
	case <-w.ready:
	case <-ctx.Done():
		g.mu.Lock()
		if g.w == w {
			g.s.slots.cancel(w)
			g.w = nil
		}
		g.mu.Unlock()
		return ctx.Err()
	}
	g.mu.Lock()
	if g.w == w {
		g.held, g.w = true, nil
	}
	g.mu.Unlock()
	if shown {
		g.s.emitSlotAcquired(ctx, g)
	}
	return nil
}

// release gives the slot back. It is a no-op when none is held, so callers can
// call it on every exit path.
func (g *agentGate) release() {
	g.mu.Lock()
	if !g.held {
		g.mu.Unlock()
		return
	}
	g.held = false
	g.mu.Unlock()
	g.s.slots.release()
}

// emitQueued writes agent.status "queued" for a room that has to wait. It
// checks, under s.mu, that the room is still open and still waiting: the
// agent's own "working" status needs s.mu too, so a late "queued" can never
// land after it. Reports whether the event was written.
func (s *Service) emitQueued(g *agentGate, w *slotWaiter, ahead int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.roomOpen(g.st) || !s.slots.isWaiting(w) {
		return false
	}
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	_, err := s.emitLocked(ctx, g.st, g.roomID, events.AgentStatus, events.AgentActor, events.AgentStatusPayload{
		State:  "queued",
		Detail: fmt.Sprintf("waiting for an agent slot (%d ahead)", ahead),
	})
	if err != nil {
		s.log.Warn("room: emit queued status failed", "room", g.roomID, "err", err)
		return false
	}
	return true
}

// emitSlotAcquired replaces a "queued" status with "working" once the slot is
// granted, so the room never keeps showing "queued" for a run that has begun.
// The agent emits its own, more specific status right after.
func (s *Service) emitSlotAcquired(ctx context.Context, g *agentGate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil || s.closed || !s.roomOpen(g.st) {
		return
	}
	ectx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	if _, err := s.emitLocked(ectx, g.st, g.roomID, events.AgentStatus, events.AgentActor, events.AgentStatusPayload{
		State: "working", Detail: "agent slot acquired",
	}); err != nil {
		s.log.Warn("room: emit slot acquired status failed", "room", g.roomID, "err", err)
	}
}

// gatedInbox is the agent's Inbox and the only place a slot is given back or
// taken again after the run has started. The runner calls Wait only when it has
// nothing to do, and a model turn only after a Drain that returned input (or
// after a Wait that ended with an error, see Wait). So the room holds a slot
// exactly while the runner can call the model. Nothing here reads the status
// strings the agent emits: those can say "done" while the agent still works.
type gatedInbox struct {
	in *inbox
	g  *agentGate
	// runCtx is the supervisor context: it outlives the per-call contexts the
	// runner passes in (for example the step-result wait deadline).
	runCtx context.Context
}

// Drain implements agent.Inbox. It takes the input first, then makes sure the
// room holds a slot before the runner sees it, so there is no window in which
// input reaches a turn without a slot. If no slot can be had (ctx ended while
// queued) the input goes back to the front of the inbox, which outlives the
// run, and the error is returned.
func (gi gatedInbox) Drain(ctx context.Context) ([]agent.HumanInput, error) {
	items, err := gi.in.Drain(ctx)
	if err != nil || len(items) == 0 {
		return items, err
	}
	if gi.g.isHeld() {
		return items, nil
	}
	if err := gi.g.acquire(ctx); err != nil {
		gi.in.pushFront(items)
		return nil, err
	}
	return items, nil
}

// Wait implements agent.Inbox. The runner is idle here, so the slot goes back
// to the pool before blocking. If the wait ends with an error while the run is
// still alive, the runner is about to carry on without new input (the
// step-result wait ran out); it takes a slot again first. Input that wakes the
// wait is slotted by the Drain that follows.
func (gi gatedInbox) Wait(ctx context.Context) error {
	gi.g.release()
	err := gi.in.Wait(ctx)
	if err != nil && gi.runCtx.Err() == nil {
		if aerr := gi.g.acquire(gi.runCtx); aerr != nil {
			return aerr
		}
	}
	return err
}
