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
// while its agent works and gives it back when the agent goes idle, so rooms
// parked on a human do not starve rooms that need to run. The agent's inbox
// (gatedInbox) takes a slot again before new input reaches a parked agent.
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

// gatedInbox is the agent's Inbox. Draining pending input while the room holds
// no slot first waits for one, so a parked agent cannot start a model turn
// outside the cap. With nothing pending it returns at once.
type gatedInbox struct {
	in *inbox
	g  *agentGate
}

// Drain implements agent.Inbox.
func (gi gatedInbox) Drain(ctx context.Context) ([]agent.HumanInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !gi.g.isHeld() && gi.in.hasPending() {
		if err := gi.g.acquire(ctx); err != nil {
			return nil, err
		}
	}
	return gi.in.Drain(ctx)
}

// Wait implements agent.Inbox.
func (gi gatedInbox) Wait(ctx context.Context) error { return gi.in.Wait(ctx) }
