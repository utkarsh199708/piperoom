package room

import (
	"context"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/store/memstore"
)

// gateHandle is what a fake runner passes back to the test: the agent-facing
// inbox and the context the runner was started with.
type gateHandle struct {
	roomID string
	inbox  gatedInbox
	ctx    context.Context
}

// newGateService builds a service whose runner hands each room's gated inbox to
// the test and then blocks until the room's context ends.
func newGateService(t *testing.T, maxAgents int) (*Service, chan gateHandle) {
	t.Helper()
	handles := make(chan gateHandle, 16)
	run := func(ctx context.Context, alert agent.Alert, deps agent.Deps, _ agent.Options) error {
		gi, ok := deps.Inbox.(gatedInbox)
		if !ok {
			t.Errorf("deps.Inbox is %T, want gatedInbox", deps.Inbox)
			return nil
		}
		handles <- gateHandle{roomID: alert.RoomID, inbox: gi, ctx: ctx}
		<-ctx.Done()
		return nil
	}
	svc := New(Deps{Store: memstore.New(), Users: DefaultUsers(), Run: run, MaxAgents: maxAgents})
	t.Cleanup(svc.Close)
	return svc, handles
}

func nextHandle(t *testing.T, ch chan gateHandle) gateHandle {
	t.Helper()
	select {
	case h := <-ch:
		return h
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not start")
		return gateHandle{}
	}
}

func mustCreate(t *testing.T, svc *Service) string {
	t.Helper()
	u, ok := svc.LookupUser("arjun")
	if !ok {
		t.Fatal("no user arjun")
	}
	snap, err := svc.CreateRoom(context.Background(), u, "Incident", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	return snap.ID
}

func pollUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within 5s")
}

// T4: a pusher and a drainer race. Every non-empty Drain must hand over its
// input with the room's slot held, even while another party competes for the
// only slot and the drainer keeps giving its slot back.
func TestGatedInboxDrainAlwaysHoldsSlot(t *testing.T) {
	svc, handles := newGateService(t, 1)
	roomID := mustCreate(t, svc)
	h := nextHandle(t, handles)
	gi := h.inbox
	arjun, _ := svc.LookupUser("arjun")

	const total = 300
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// A competitor takes and returns the slot, so acquire has to queue.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			w, _ := svc.slots.request()
			select {
			case <-w.ready:
			case <-stop:
				svc.slots.cancel(w)
				return
			}
			time.Sleep(50 * time.Microsecond)
			svc.slots.release()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			if _, err := svc.Steer(context.Background(), arjun, roomID, "go"); err != nil {
				t.Errorf("steer: %v", err)
				return
			}
		}
	}()

	got := 0
	deadline := time.Now().Add(20 * time.Second)
	for got < total && time.Now().Before(deadline) {
		items, err := gi.Drain(h.ctx)
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
		if len(items) == 0 {
			wctx, cancel := context.WithTimeout(h.ctx, time.Second)
			_ = gi.in.Wait(wctx)
			cancel()
			continue
		}
		if !gi.g.isHeld() {
			t.Fatalf("Drain returned %d inputs without the room holding a slot", len(items))
		}
		got += len(items)
		// Like an agent going back to waiting: the slot goes back.
		gi.g.release()
	}
	close(stop)
	wg.Wait()
	if got < total {
		t.Fatalf("drained %d of %d inputs", got, total)
	}
	if inUse, queued := svc.slots.stats(); inUse != 0 || queued != 0 {
		t.Fatalf("slots after the stress: in use %d, queued %d, want 0 and 0", inUse, queued)
	}
}

// T5: the room closes while its agent is queued inside Drain. The queue entry
// goes away and the input it had taken is back at the head of the inbox.
func TestGatedInboxDrainQueuedThenRoomClosed(t *testing.T) {
	svc, handles := newGateService(t, 1)
	arjun, _ := svc.LookupUser("arjun")

	b := mustCreate(t, svc)
	hb := nextHandle(t, handles)
	hb.inbox.g.release() // B's agent is idle: no slot

	_ = mustCreate(t, svc) // A takes the only slot
	nextHandle(t, handles)

	for _, text := range []string{"first", "second"} {
		if _, err := svc.Steer(context.Background(), arjun, b, text); err != nil {
			t.Fatalf("steer: %v", err)
		}
	}

	type result struct {
		items []agent.HumanInput
		err   error
	}
	res := make(chan result, 1)
	go func() {
		items, err := hb.inbox.Drain(hb.ctx)
		res <- result{items, err}
	}()
	pollUntil(t, func() bool { _, q := svc.slots.stats(); return q == 1 })

	if _, err := svc.CloseRoom(context.Background(), arjun, b, "closed while queued"); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case r := <-res:
		if r.err == nil || len(r.items) != 0 {
			t.Fatalf("Drain = %v, %v; want no input and an error", r.items, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Drain did not return after the room closed")
	}
	if inUse, queued := svc.slots.stats(); inUse != 1 || queued != 0 {
		t.Fatalf("slots: in use %d, queued %d; want 1 (room A) and 0", inUse, queued)
	}
	svc.mu.Lock()
	in := svc.rooms[b].inbox
	svc.mu.Unlock()
	if n := in.pending(); n != 2 {
		t.Fatalf("inbox has %d inputs, want the 2 that were taken", n)
	}
	items, err := in.Drain(context.Background())
	if err != nil || len(items) != 2 || items[0].Text != "first" || items[1].Text != "second" {
		t.Fatalf("inputs came back as %+v (err %v), want first then second", items, err)
	}
}

// A Wait that ends by deadline (the agent's step-result wait) is followed by a
// model call with no new input, so Wait must hand back a slot before returning.
func TestGatedInboxWaitDeadlineTakesSlotBack(t *testing.T) {
	svc, handles := newGateService(t, 1)
	arjun, _ := svc.LookupUser("arjun")

	_ = mustCreate(t, svc) // B holds the only slot
	hb := nextHandle(t, handles)
	a := mustCreate(t, svc) // A queues
	hb.inbox.g.release()    // B waits: A gets the slot
	nextHandle(t, handles)

	wctx, cancel := context.WithTimeout(hb.ctx, 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- hb.inbox.Wait(wctx) }()

	// The deadline passes, but Wait must not return before B has a slot again.
	pollUntil(t, func() bool { _, q := svc.slots.stats(); return q == 1 })
	select {
	case err := <-done:
		t.Fatalf("Wait returned (%v) before it had a slot", err)
	case <-time.After(150 * time.Millisecond):
	}
	if _, err := svc.CloseRoom(context.Background(), arjun, a, "free the slot"); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Wait should report the deadline")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return once a slot was free")
	}
	if !hb.inbox.g.isHeld() {
		t.Fatal("room must hold a slot after its wait deadline")
	}
}
