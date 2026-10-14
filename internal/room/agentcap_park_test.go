package room_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// T1: an agent reports "done" and then keeps calling the model (the real agent
// does after a recovered verdict). The status must not give its slot away: a
// second room stays queued until the first one really parks.
func TestAgentCapDoneStatusKeepsSlotWhileAgentKeepsTurning(t *testing.T) {
	cr := newCapRunner()
	run := func(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
		_ = d.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "done", Detail: "pipeline recovered"})
		return cr.run(ctx, a, d, o) // goes on "turning" until released
	}
	svc, _ := newCapService(t, 1, run)

	r1 := createRoom(t, svc, "arjun").ID
	cr.waitStarted(t, 1)
	r2 := createRoom(t, svc, "arjun").ID
	waitQueued(t, svc, r2)
	settle()
	if got := cr.startedIDs(); !slices.Equal(got, []string{r1}) {
		t.Fatalf("r2 ran while r1 was still turning: started %v", got)
	}
	cr.finish(r1)
	cr.waitStarted(t, 2)
	if m := cr.maxConcurrent(); m != 1 {
		t.Fatalf("max concurrent turners = %d, want 1", m)
	}
	cr.finish(r2)
}

// T2: an agent parked "done" is woken, drains one input, and then waits again
// without emitting any status. Its slot must go to the queued room.
func TestAgentCapWaitWithoutStatusFreesSlot(t *testing.T) {
	cr := newCapRunner()
	var first sync.Once
	proceed := make(chan struct{})
	drained := make(chan int, 1)
	run := func(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
		isR1 := false
		first.Do(func() { isR1 = true })
		if !isR1 {
			return cr.run(ctx, a, d, o)
		}
		_ = d.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "done", Detail: "pipeline recovered"})
		if err := d.Inbox.Wait(ctx); err != nil {
			return nil
		}
		in, err := d.Inbox.Drain(ctx)
		if err != nil {
			return nil
		}
		drained <- len(in)
		select {
		case <-proceed:
		case <-ctx.Done():
			return nil
		}
		_ = d.Inbox.Wait(ctx) // no status: just waits again
		<-ctx.Done()
		return nil
	}
	svc, _ := newCapService(t, 1, run)
	arjun := mustUser(t, svc, "arjun")

	r1 := createRoom(t, svc, "arjun").ID // parks at once, freeing its slot
	waitFor(t, 5*time.Second, func() bool { return agentState(t, svc, r1).State == "done" })
	r2 := createRoom(t, svc, "arjun").ID // takes the freed slot
	cr.waitStarted(t, 1)
	if _, err := svc.Steer(context.Background(), arjun, r1, "wake"); err != nil {
		t.Fatalf("steer: %v", err)
	}
	waitQueued(t, svc, r1) // the cap is full, so r1 queues for its turn
	r3 := createRoom(t, svc, "arjun").ID
	waitFor(t, 5*time.Second, func() bool { return agentState(t, svc, r3).State == "queued" })

	cr.finish(r2)
	select {
	case n := <-drained:
		if n != 1 {
			t.Fatalf("r1 drained %d inputs, want 1", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("r1 never got its slot")
	}
	settle()
	if got := cr.startedIDs(); !slices.Equal(got, []string{r2}) {
		t.Fatalf("r3 must wait while r1 holds the slot, started %v", got)
	}
	close(proceed) // r1 waits again, silently
	cr.waitStarted(t, 2)
	if got := cr.startedIDs(); !slices.Equal(got, []string{r2, r3}) {
		t.Fatalf("started %v, want r3 after r1 waited", got)
	}
	cr.finish(r3)
}

// failStatusStore fails every agent.status append once armed, like a database
// that is down for that write.
type failStatusStore struct {
	store.Store
	fail atomic.Bool
}

func (f *failStatusStore) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	if f.fail.Load() && typ == events.AgentStatus {
		return events.Event{}, errors.New("append failed")
	}
	return f.Store.Append(ctx, roomID, typ, actor, payload)
}

// T3: the agent's status append fails. The slot is not tied to statuses, so it
// is neither leaked nor lost: the room frees it when its agent waits.
func TestAgentCapStatusAppendFailureDoesNotLeakSlot(t *testing.T) {
	cr := newCapRunner()
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	fs := &failStatusStore{Store: memstore.NewWithClock(c.Now)}
	var first sync.Once
	emitErr := make(chan error, 1)
	goOn := make(chan struct{})
	r1Started := make(chan struct{})
	run := func(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
		isR1 := false
		first.Do(func() { isR1 = true })
		if !isR1 {
			return cr.run(ctx, a, d, o)
		}
		close(r1Started)
		<-goOn
		emitErr <- d.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle"})
		_ = d.Inbox.Wait(ctx)
		<-ctx.Done()
		return nil
	}
	svc := room.New(room.Deps{Store: fs, Users: room.DefaultUsers(), Run: run, Now: c.Now, MaxAgents: 1})
	t.Cleanup(svc.Close)

	createRoom(t, svc, "arjun")
	<-r1Started
	r2 := createRoom(t, svc, "arjun").ID
	settle()
	if n := cr.starts(r2); n != 0 {
		t.Fatal("r2 ran while r1 held the only slot")
	}
	fs.fail.Store(true)
	close(goOn)
	if err := <-emitErr; err == nil {
		t.Fatal("the status append was meant to fail")
	}
	cr.waitStarted(t, 1) // r1 waited, so r2 got the slot despite the failed append
	if got := cr.startedIDs(); !slices.Equal(got, []string{r2}) {
		t.Fatalf("started %v, want r2", got)
	}
	cr.finish(r2)
}
