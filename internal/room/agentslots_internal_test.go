package room

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/store/memstore"
)

// grantedOrder releases one slot at a time and returns the names of the
// waiters in the order they were granted.
func grantedOrder(t *testing.T, a *agentSlots, names map[*slotWaiter]string) []string {
	t.Helper()
	var order []string
	pending := len(names)
	for pending > 0 {
		a.release()
		got := ""
		for w, n := range names {
			select {
			case <-w.ready:
				got = n
				delete(names, w)
			default:
			}
		}
		if got == "" {
			t.Fatalf("a release granted nothing; %d still queued", pending)
		}
		order = append(order, got)
		pending--
	}
	return order
}

// One pipeline with many queued rooms does not starve the others: grants go
// round-robin across pipelines, FIFO within each, and the "ahead" count a
// room is told is its true place in that order.
func TestAgentSlotsRoundRobinAcrossPipelines(t *testing.T) {
	a := newAgentSlots(1)
	if _, ahead := a.request("noisy"); ahead != 0 {
		t.Fatalf("first request ahead=%d", ahead)
	}
	names := map[*slotWaiter]string{}
	aheads := map[string]int{}
	add := func(pipeline, name string) {
		w, ahead := a.request(pipeline)
		names[w] = name
		aheads[name] = ahead
	}
	for i := 1; i <= 5; i++ {
		add("noisy", fmt.Sprintf("n%d", i))
	}
	add("quiet1", "q1")
	add("quiet2", "q2")
	add("noisy", "n6")
	add("quiet1", "q1b")
	if inUse, queued := a.stats(); inUse != 1 || queued != 9 {
		t.Fatalf("stats = %d in use, %d queued", inUse, queued)
	}
	want := []string{"n1", "q1", "q2", "n2", "q1b", "n3", "n4", "n5", "n6"}
	for i, n := range want {
		if aheads[n] > i {
			t.Errorf("%s was told %d ahead but is granted at position %d", n, aheads[n], i)
		}
	}
	// q2 joined after q1 and n1..n5: it waits for n1 and q1 only.
	if aheads["q1"] != 1 || aheads["q2"] != 2 {
		t.Fatalf("ahead q1=%d q2=%d, want 1 and 2", aheads["q1"], aheads["q2"])
	}
	if got := grantedOrder(t, a, names); !slices.Equal(got, want) {
		t.Fatalf("grant order %v, want %v", got, want)
	}
}

// A queued room that leaves keeps the rotation intact; the same pipeline
// alone is plain FIFO.
func TestAgentSlotsCancelKeepsRotation(t *testing.T) {
	a := newAgentSlots(1)
	a.request("p")
	names := map[*slotWaiter]string{}
	wA1, _ := a.request("a")
	names[wA1] = "a1"
	wB1, _ := a.request("b")
	wC1, _ := a.request("c")
	names[wC1] = "c1"
	wA2, _ := a.request("a")
	names[wA2] = "a2"
	a.cancel(wB1) // b leaves the rotation entirely
	if _, queued := a.stats(); queued != 3 {
		t.Fatalf("queued %d after cancel, want 3", queued)
	}
	if got, want := grantedOrder(t, a, names), []string{"a1", "c1", "a2"}; !slices.Equal(got, want) {
		t.Fatalf("grant order %v, want %v", got, want)
	}

	f := newAgentSlots(1)
	f.request("x")
	names = map[*slotWaiter]string{}
	for i := 0; i < 4; i++ {
		w, ahead := f.request("x")
		if ahead != i {
			t.Fatalf("fifo ahead=%d, want %d", ahead, i)
		}
		names[w] = fmt.Sprint(i)
	}
	if got := grantedOrder(t, f, names); !slices.Equal(got, []string{"0", "1", "2", "3"}) {
		t.Fatalf("one pipeline is not FIFO: %v", got)
	}
}

// fakeGlobal is a cross-replica cap of max slots.
type fakeGlobal struct {
	mu    sync.Mutex
	max   int
	held  map[string]bool
	takes int
}

func (f *fakeGlobal) take(_ context.Context, roomID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.takes++
	if f.held[roomID] {
		return true, nil
	}
	if len(f.held) >= f.max {
		return false, nil
	}
	f.held[roomID] = true
	return true, nil
}

func (f *fakeGlobal) give(roomID string) {
	f.mu.Lock()
	delete(f.held, roomID)
	f.mu.Unlock()
}

func (f *fakeGlobal) limit() int { return f.max }

func (f *fakeGlobal) holding() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.held)
}

// With a free local slot but the global cap reached, a room waits, shows that
// it waits for a slot across the servers, and runs once the holder is done.
func TestAgentSlotsGlobalCap(t *testing.T) {
	var mu sync.Mutex
	started := map[string]chan struct{}{}
	finish := map[string]chan struct{}{}
	run := func(ctx context.Context, a agent.Alert, deps agent.Deps, _ agent.Options) error {
		mu.Lock()
		ch, done := started[a.RoomID], finish[a.RoomID]
		mu.Unlock()
		close(ch)
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
	st := memstore.New()
	s := New(Deps{Store: st, Users: DefaultUsers(), Run: run, MaxAgents: 5})
	t.Cleanup(s.Close)
	g := &fakeGlobal{max: 1, held: map[string]bool{}}
	s.global = g // the cap across replicas; local slots stay plentiful
	arjun, _ := s.LookupUser("arjun")

	// The runner looks its channels up by room id, which CreateRoom picks:
	// open holds mu from CreateRoom until the channels are registered, so
	// the run (which reads them under mu) always finds them.
	open := func() string {
		mu.Lock()
		defer mu.Unlock()
		snap, err := s.CreateRoom(context.Background(), arjun, "t", "sev3", "orders")
		if err != nil {
			t.Fatal(err)
		}
		started[snap.ID] = make(chan struct{})
		finish[snap.ID] = make(chan struct{})
		return snap.ID
	}
	r1 := open()
	select {
	case <-started[r1]:
	case <-time.After(5 * time.Second):
		t.Fatal("first room never ran")
	}
	r2 := open()
	waitStatus := func(room, state, detail string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			snap, err := s.GetRoom(context.Background(), room)
			if err == nil && snap.AgentStatus.State == state && strings.Contains(snap.AgentStatus.Detail, detail) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		snap, _ := s.GetRoom(context.Background(), room)
		t.Fatalf("room %s status %+v, want %s %q", room, snap.AgentStatus, state, detail)
	}
	waitStatus(r2, "queued", "all 1 across the servers")
	select {
	case <-started[r2]:
		t.Fatal("second room ran while the global cap was full")
	case <-time.After(2 * globalSlotRetry):
	}
	if inUse, _ := s.slots.stats(); inUse != 2 {
		t.Fatalf("local slots in use = %d, want 2 (the cap that binds is the global one)", inUse)
	}
	close(finish[r1])
	select {
	case <-started[r2]:
	case <-time.After(5 * time.Second):
		t.Fatal("second room never ran after the first gave its global slot back")
	}
	waitStatus(r2, "working", "agent slot acquired")
	if n := g.holding(); n != 1 {
		t.Fatalf("global slots held = %d, want 1", n)
	}
	close(finish[r2])
	deadline := time.Now().Add(5 * time.Second)
	for g.holding() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := g.holding(); n != 0 {
		t.Fatalf("global slots held after both runs ended = %d", n)
	}
}

// Review mutant M16: a room granted the slot that another room just gave
// back while the service is closing never starts a run, whatever its context
// says (Close cancels the rooms one at a time).
func TestAgentGateRefusesOnClosingService(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	started := map[string]int{}
	run := func(ctx context.Context, a agent.Alert, _ agent.Deps, _ agent.Options) error {
		mu.Lock()
		started[a.RoomID]++
		mu.Unlock()
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}
	s := New(Deps{Store: memstore.New(), Users: DefaultUsers(), Run: run, MaxAgents: 1})
	arjun, _ := s.LookupUser("arjun")
	waitFor := func(cond func() bool) {
		deadline := time.Now().Add(5 * time.Second)
		for !cond() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
	}
	r1, err := s.CreateRoom(context.Background(), arjun, "a", "sev3", "orders")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(func() bool { mu.Lock(); defer mu.Unlock(); return started[r1.ID] == 1 })
	r2, err := s.CreateRoom(context.Background(), arjun, "b", "sev3", "orders")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(func() bool { _, q := s.slots.stats(); return q == 1 })
	if inUse, q := s.slots.stats(); inUse != 1 || q != 1 {
		mu.Lock()
		t.Fatalf("setup: in use %d, queued %d, started %v", inUse, q, started)
	}
	// Close has begun (closed is set) but no context is cancelled yet: r1
	// finishes and gives its slot to the queued r2.
	s.reg.Lock()
	s.closed = true
	s.reg.Unlock()
	close(release)
	time.Sleep(300 * time.Millisecond)
	s.cancel()
	s.wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if started[r2.ID] != 0 {
		t.Fatal("a room started a run on a closing service")
	}
}
