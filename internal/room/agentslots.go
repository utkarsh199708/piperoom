package room

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

// DefaultMaxAgents is the cap on concurrent agent runs on one replica when
// Deps.MaxAgents is not set. One Connect failure can open dozens of rooms at
// once; without a cap each of them would start an Opus run.
const DefaultMaxAgents = 3

// slotWaiter is one room's place in the agent queue.
type slotWaiter struct {
	ready    chan struct{} // closed when a slot is granted
	pipeline string
	granted  bool // guarded by agentSlots.mu
}

// agentSlots is this replica's limit on concurrent agent runs across all its
// rooms (PIPEROOM_MAX_AGENTS). A room that wants to run asks for a slot with
// request; when none is free it waits in a queue and is granted one as slots
// are released. The queue is round-robin across pipelines and FIFO within a
// pipeline, so one pipeline with many rooms cannot starve another
// (docs/design/replicas.md section 4.4). It never calls back into the Service,
// so it is safe to use while holding Service.mu.
type agentSlots struct {
	mu    sync.Mutex
	max   int
	inUse int
	// queues holds the waiters of each pipeline in arrival order; order is
	// the rotation of pipelines that have waiters, and next is the index in
	// order of the pipeline served next.
	queues map[string][]*slotWaiter
	order  []string
	next   int
	queued int
	// closed is set by shutdown: a closing service grants no slot, so a room
	// queued behind a run that Close just cancelled never starts its own.
	closed bool
}

func newAgentSlots(max int) *agentSlots {
	if max < 1 {
		max = DefaultMaxAgents
	}
	return &agentSlots{max: max, queues: map[string][]*slotWaiter{}}
}

// request takes a slot if one is free and nobody is queued, otherwise it joins
// its pipeline's queue. ahead is the number of rooms that will be granted a
// slot before the caller, in round-robin order.
func (a *agentSlots) request(pipeline string) (w *slotWaiter, ahead int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w = &slotWaiter{ready: make(chan struct{}), pipeline: pipeline}
	if !a.closed && a.inUse < a.max && a.queued == 0 {
		a.inUse++
		w.granted = true
		close(w.ready)
		return w, 0
	}
	if len(a.queues[pipeline]) == 0 {
		// A pipeline that had no waiter joins the rotation just before the
		// pipeline served next: it waits one full round at most, and never
		// jumps ahead of pipelines already waiting their turn.
		a.order = append(a.order, "")
		copy(a.order[a.next+1:], a.order[a.next:])
		a.order[a.next] = pipeline
		a.next++
		if a.next >= len(a.order) {
			a.next = 0
		}
	}
	a.queues[pipeline] = append(a.queues[pipeline], w)
	a.queued++
	for i, q := range a.grantOrderLocked() {
		if q == w {
			ahead = i
			break
		}
	}
	return w, ahead
}

// grantOrderLocked returns every queued waiter in the order pump would grant
// them. Caller holds a.mu.
func (a *agentSlots) grantOrderLocked() []*slotWaiter {
	out := make([]*slotWaiter, 0, a.queued)
	if len(a.order) == 0 {
		return out
	}
	taken := make(map[string]int, len(a.order))
	for len(out) < a.queued {
		before := len(out)
		for i := 0; i < len(a.order); i++ {
			p := a.order[(a.next+i)%len(a.order)]
			if k := taken[p]; k < len(a.queues[p]) {
				out = append(out, a.queues[p][k])
				taken[p] = k + 1
			}
		}
		if len(out) == before {
			break // never spin under the lock, whatever the counters say
		}
	}
	return out
}

// isWaiting reports whether w is still queued (not yet granted).
func (a *agentSlots) isWaiting(w *slotWaiter) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return !w.granted
}

// release returns one slot and grants it to the next waiter.
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
	q := a.queues[w.pipeline]
	for i, x := range q {
		if x == w {
			a.queues[w.pipeline] = append(q[:i:i], q[i+1:]...)
			a.queued--
			if len(a.queues[w.pipeline]) == 0 {
				a.dropPipelineLocked(w.pipeline)
			}
			return
		}
	}
}

// dropPipelineLocked takes a pipeline with no waiters out of the rotation,
// keeping next on the pipeline that was due. Caller holds a.mu.
func (a *agentSlots) dropPipelineLocked(p string) {
	delete(a.queues, p)
	for i, x := range a.order {
		if x != p {
			continue
		}
		a.order = append(a.order[:i], a.order[i+1:]...)
		if i < a.next {
			a.next--
		}
		if a.next >= len(a.order) {
			a.next = 0
		}
		return
	}
}

// pumpLocked grants free slots, one pipeline at a time in rotation. Caller
// holds a.mu.
func (a *agentSlots) pumpLocked() {
	for !a.closed && a.inUse < a.max && a.queued > 0 {
		p := a.order[a.next]
		q := a.queues[p]
		w := q[0]
		q[0] = nil
		a.queues[p] = q[1:]
		a.queued--
		if len(a.queues[p]) == 0 {
			a.dropPipelineLocked(p) // next now points at the pipeline after p
		} else {
			a.next = (a.next + 1) % len(a.order)
		}
		w.granted = true
		a.inUse++
		close(w.ready)
	}
}

// shutdown stops granting slots. Waiters stay queued until their context
// ends and they cancel; slots in use are still released as usual.
func (a *agentSlots) shutdown() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
}

// stats returns slots in use and rooms queued (tests, logging).
func (a *agentSlots) stats() (inUse, queued int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inUse, a.queued
}

// globalSlots is the cap across replicas (PIPEROOM_MAX_AGENTS_GLOBAL), taken
// after this replica's own slot. coordSlots implements it on the store; tests
// use a fake.
type globalSlots interface {
	// take tries to take a slot for the room; false means the cap is full.
	take(ctx context.Context, roomID string) (bool, error)
	// give returns the room's slot.
	give(roomID string)
	// limit is the cap, for the queued status text.
	limit() int
}

// globalSlotRetry is how often a room waiting for a global slot tries again;
// after a database error it backs off up to globalSlotMaxBackoff.
const (
	globalSlotRetry      = time.Second
	globalSlotMaxBackoff = 10 * time.Second
)

// agentGate is one room's handle on the slots. The room holds a slot while
// its agent can call the model and gives it back while the agent waits for
// input, so rooms parked on a human do not starve rooms that need to run.
// gatedInbox drives it.
type agentGate struct {
	s        *Service
	st       *roomState
	roomID   string
	pipeline string

	mu   sync.Mutex
	held bool
	w    *slotWaiter // the pending request, shared by concurrent acquires

	// gmu serializes the global part of acquire; globalHeld is guarded by it.
	gmu        sync.Mutex
	globalHeld bool
}

func (g *agentGate) isHeld() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.held
}

// acquire blocks until the room holds a slot (this replica's, then the
// global one when a global cap is set) or ctx ends. While the room has to wait
// it shows agent.status "queued". Concurrent callers share one queue entry,
// so a room is never queued twice.
func (g *agentGate) acquire(ctx context.Context) error {
	shown, err := g.acquireLocal(ctx)
	if err != nil {
		return err
	}
	// Close sets closed before it cancels the service context, and the
	// cancel reaches the rooms one at a time: a room granted the slot that a
	// cancelled room just gave back can still see a live context. It must not
	// start a run on a closing service.
	if g.s.isClosed() {
		g.release()
		return context.Canceled
	}
	if g.s.global != nil {
		got, err := g.acquireGlobal(ctx, shown)
		if err != nil {
			g.release()
			return err
		}
		shown = shown || got
	}
	if shown {
		g.s.emitSlotAcquired(ctx, g)
	}
	return nil
}

// acquireLocal is acquire on this replica's slots. shown reports whether a
// "queued" status was written while waiting.
func (g *agentGate) acquireLocal(ctx context.Context) (shown bool, err error) {
	g.mu.Lock()
	if g.held {
		g.mu.Unlock()
		return false, nil
	}
	w, owner, ahead := g.w, false, 0
	if w == nil {
		w, ahead = g.s.slots.request(g.pipeline)
		g.w, owner = w, true
		if ahead == 0 && !g.s.slots.isWaiting(w) {
			g.held, g.w = true, nil
			g.mu.Unlock()
			return false, nil
		}
	}
	g.mu.Unlock()

	if owner {
		shown = g.s.emitQueued(g, func() bool { return g.s.slots.isWaiting(w) },
			fmt.Sprintf("waiting for an agent slot (%d ahead)", ahead))
	}
	select {
	case <-w.ready:
	case <-ctx.Done():
	}
	// A grant and a cancel can arrive together (the service closing frees a
	// slot and cancels every queued room at once): a cancelled room gives the
	// slot back instead of starting a run on a dead context.
	if err := ctx.Err(); err != nil {
		g.mu.Lock()
		if g.w == w {
			g.s.slots.cancel(w)
			g.w = nil
		}
		g.mu.Unlock()
		return shown, err
	}
	g.mu.Lock()
	if g.w == w {
		g.held, g.w = true, nil
	}
	g.mu.Unlock()
	return shown, nil
}

// acquireGlobal takes a slot under the cross-replica cap, retrying every
// globalSlotRetry while the cap is full. shown reports whether it wrote a
// "queued" status (localShown: one was written already for this wait).
func (g *agentGate) acquireGlobal(ctx context.Context, localShown bool) (shown bool, err error) {
	g.gmu.Lock()
	defer g.gmu.Unlock()
	if g.globalHeld {
		return false, nil
	}
	backoff := globalSlotRetry
	for {
		ok, err := g.s.global.take(ctx, g.roomID)
		wait := globalSlotRetry
		switch {
		case err != nil && (ctx.Err() != nil || errors.Is(err, store.ErrLeaseLost)):
			return shown, err
		case err != nil:
			// A database error says nothing about the cap: retry with
			// backoff rather than leave the room with an agent that never
			// runs (docs/design/replicas.md 4.4).
			g.s.log.Warn("room: take global agent slot failed; retrying", "room", g.roomID, "in", backoff.String(), "err", err)
			wait = backoff
			backoff = min(2*backoff, globalSlotMaxBackoff)
		case ok:
			g.globalHeld = true
			return shown, nil
		default:
			backoff = globalSlotRetry
			if !shown && !localShown {
				shown = g.s.emitQueued(g, func() bool { return true },
					fmt.Sprintf("waiting for an agent slot (all %d across the servers are in use)", g.s.global.limit()))
			}
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return shown, ctx.Err()
		case <-t.C:
		}
	}
}

// release gives the slots back. It is a no-op when none is held, so callers
// can call it on every exit path.
func (g *agentGate) release() {
	g.gmu.Lock()
	if g.globalHeld {
		g.globalHeld = false
		g.s.global.give(g.roomID)
	}
	g.gmu.Unlock()
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
func (s *Service) emitQueued(g *agentGate, waiting func() bool, detail string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.roomOpen(g.st) || !waiting() {
		return false
	}
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	_, err := s.emitLocked(ctx, g.st, g.roomID, events.AgentStatus, events.AgentActor, events.AgentStatusPayload{
		State:  "queued",
		Detail: detail,
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
// taken again after the run has started. The runner calls the run-level Wait
// only when it has nothing to do, and starts a model turn from input only after
// a Drain that returned it. So the room holds a slot while the runner can call
// the model (the bounded step-result wait counts as working, see Wait). Nothing here reads the status
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

// Wait implements agent.Inbox. Only the open-ended, run-level wait gives the
// slot back: agent.Run passes the supervisor context unchanged to it, and it
// ends only when input arrives (the Drain after it takes a slot again) or the
// run ends. Any other wait uses a context derived from it, such as the
// bounded step-result wait (at most 45 s, agent status "working"), and keeps
// the slot. That wait can end by deadline, or return at once on a stale wake
// token that Drain never consumes, and the runner then calls the model with
// nothing more to Drain. Giving the slot up there would leave that call
// without one; it costs a slot for at most the bound.
func (gi gatedInbox) Wait(ctx context.Context) error {
	if ctx.Done() == gi.runCtx.Done() {
		gi.g.release()
	}
	return gi.in.Wait(ctx)
}
