package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// capRunner is a fake agent runner that records when runs start and how many
// run at once, and holds each run until the test releases that room.
type capRunner struct {
	mu      sync.Mutex
	cur     int
	max     int
	started []string       // room ids in start order (one entry per run start)
	count   map[string]int // run starts per room
	release map[string]chan struct{}
	failing map[string]string // room id -> "error" | "panic": fail its first run
	// startCh, when set, receives the room id of every run start, so a test
	// waits on the start itself instead of polling.
	startCh chan string
}

func newCapRunner() *capRunner {
	return &capRunner{
		count:   map[string]int{},
		release: map[string]chan struct{}{},
		failing: map[string]string{},
	}
}

func (c *capRunner) releaseCh(id string) chan struct{} {
	ch, ok := c.release[id]
	if !ok {
		ch = make(chan struct{})
		c.release[id] = ch
	}
	return ch
}

// finish lets the room's run return.
func (c *capRunner) finish(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	close(c.releaseCh(id))
}

func (c *capRunner) run(ctx context.Context, alert agent.Alert, _ agent.Deps, _ agent.Options) error {
	id := alert.RoomID
	c.mu.Lock()
	c.cur++
	if c.cur > c.max {
		c.max = c.cur
	}
	c.started = append(c.started, id)
	c.count[id]++
	mode := ""
	if c.count[id] == 1 {
		mode = c.failing[id]
	}
	ch := c.releaseCh(id)
	notify := c.startCh
	c.mu.Unlock()
	if notify != nil {
		notify <- id
	}
	defer func() {
		c.mu.Lock()
		c.cur--
		c.mu.Unlock()
	}()
	switch mode {
	case "error":
		return errors.New("fake agent failure")
	case "panic":
		panic("fake agent panic")
	}
	select {
	case <-ch:
	case <-ctx.Done():
	}
	return nil
}

func (c *capRunner) startedIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.started)
}

func (c *capRunner) maxConcurrent() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.max
}

func (c *capRunner) starts(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count[id]
}

func (c *capRunner) waitStarted(t *testing.T, n int) {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool { return len(c.startedIDs()) >= n })
}

func newCapService(t *testing.T, maxAgents int, run agent.Runner) (*room.Service, *memstore.Store) {
	t.Helper()
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Run: run, Now: c.Now, MaxAgents: maxAgents,
	})
	t.Cleanup(svc.Close)
	return svc, st
}

func agentState(t *testing.T, svc *room.Service, roomID string) room.AgentStatus {
	t.Helper()
	snap, err := svc.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatalf("get room: %v", err)
	}
	return snap.AgentStatus
}

func waitQueued(t *testing.T, svc *room.Service, roomID string) room.AgentStatus {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool { return agentState(t, svc, roomID).State == "queued" })
	return agentState(t, svc, roomID)
}

// settle gives goroutines a moment to (wrongly) act before a negative check.
func settle() { time.Sleep(100 * time.Millisecond) }

func TestAgentCapRunsOnlyMaxAndQueuesFIFO(t *testing.T) {
	cr := newCapRunner()
	svc, _ := newCapService(t, 2, cr.run)

	r1 := createRoom(t, svc, "arjun").ID
	cr.waitStarted(t, 1)
	r2 := createRoom(t, svc, "arjun").ID
	cr.waitStarted(t, 2)
	r3 := createRoom(t, svc, "arjun").ID
	s3 := waitQueued(t, svc, r3)
	r4 := createRoom(t, svc, "arjun").ID
	s4 := waitQueued(t, svc, r4)

	if want := "waiting for an agent slot (0 ahead)"; s3.Detail != want {
		t.Fatalf("r3 detail = %q, want %q", s3.Detail, want)
	}
	if want := "waiting for an agent slot (1 ahead)"; s4.Detail != want {
		t.Fatalf("r4 detail = %q, want %q", s4.Detail, want)
	}
	settle()
	if got := cr.startedIDs(); !slices.Equal(got, []string{r1, r2}) {
		t.Fatalf("with cap 2 only r1 and r2 may run, started %v", got)
	}

	// A slot frees: the head of the queue (r3) starts, not r4.
	cr.finish(r1)
	cr.waitStarted(t, 3)
	settle()
	if got := cr.startedIDs(); !slices.Equal(got, []string{r1, r2, r3}) {
		t.Fatalf("after r1 finished want r3 next, started %v", got)
	}
	if st := agentState(t, svc, r3); st.State == "queued" {
		t.Fatalf("r3 is running but still shows queued: %+v", st)
	}
	if st := agentState(t, svc, r4); st.State != "queued" {
		t.Fatalf("r4 should still be queued: %+v", st)
	}

	cr.finish(r2)
	cr.waitStarted(t, 4)
	if got := cr.startedIDs(); !slices.Equal(got, []string{r1, r2, r3, r4}) {
		t.Fatalf("start order = %v", got)
	}
	if m := cr.maxConcurrent(); m != 2 {
		t.Fatalf("max concurrent runs = %d, want 2", m)
	}
	cr.finish(r3)
	cr.finish(r4)
}

func TestAgentCapClosingQueuedRoomRemovesIt(t *testing.T) {
	cr := newCapRunner()
	svc, _ := newCapService(t, 1, cr.run)

	r1 := createRoom(t, svc, "arjun").ID
	cr.waitStarted(t, 1)
	r2 := createRoom(t, svc, "arjun").ID
	waitQueued(t, svc, r2)
	r3 := createRoom(t, svc, "arjun").ID
	if d := waitQueued(t, svc, r3).Detail; !strings.Contains(d, "(1 ahead)") {
		t.Fatalf("r3 detail = %q, want 1 ahead", d)
	}

	if _, err := svc.CloseRoom(context.Background(), mustUser(t, svc, "arjun"), r2, "not needed"); err != nil {
		t.Fatalf("close r2: %v", err)
	}
	cr.finish(r1)
	cr.waitStarted(t, 2)
	settle()
	if got := cr.startedIDs(); !slices.Equal(got, []string{r1, r3}) {
		t.Fatalf("closed r2 must be skipped; started %v", got)
	}
	if n := cr.starts(r2); n != 0 {
		t.Fatalf("closed room ran %d times", n)
	}
	cr.finish(r3)
}

func TestAgentCapClosingRunningRoomFreesSlot(t *testing.T) {
	cr := newCapRunner()
	svc, _ := newCapService(t, 1, cr.run)

	r1 := createRoom(t, svc, "arjun").ID
	cr.waitStarted(t, 1)
	r2 := createRoom(t, svc, "arjun").ID
	waitQueued(t, svc, r2)

	// Closing r1 cancels its run's context; the slot must come back.
	if _, err := svc.CloseRoom(context.Background(), mustUser(t, svc, "arjun"), r1, "done"); err != nil {
		t.Fatalf("close r1: %v", err)
	}
	cr.waitStarted(t, 2)
	if got := cr.startedIDs(); !slices.Equal(got, []string{r1, r2}) {
		t.Fatalf("started %v", got)
	}
	cr.finish(r2)
}

func TestAgentCapDuplicateWakesDoNotDoubleQueue(t *testing.T) {
	cr := newCapRunner()
	svc, st := newCapService(t, 1, cr.run)
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")

	r1 := createRoom(t, svc, "arjun").ID
	cr.waitStarted(t, 1)
	r2 := createRoom(t, svc, "arjun").ID
	waitQueued(t, svc, r2)
	r3 := createRoom(t, svc, "arjun").ID
	waitQueued(t, svc, r3)

	// Wake the already queued r2 many times: steers and @agent mentions.
	for i := 0; i < 3; i++ {
		if _, err := svc.Steer(ctx, arjun, r2, fmt.Sprintf("look again %d", i)); err != nil {
			t.Fatalf("steer: %v", err)
		}
		if _, err := svc.AddComment(ctx, arjun, r2, "", "@agent please start"); err != nil {
			t.Fatalf("comment: %v", err)
		}
	}

	cr.finish(r1)
	cr.waitStarted(t, 2)
	cr.finish(r2)
	cr.waitStarted(t, 3)
	settle()
	// A second queue entry for r2 would have started it again before or after r3.
	if got := cr.startedIDs(); !slices.Equal(got, []string{r1, r2, r3}) {
		t.Fatalf("start order = %v, want r1 r2 r3 with r2 once", got)
	}
	queuedEvents := 0
	for _, e := range allEvents(t, st, r2) {
		if e.Type != events.AgentStatus {
			continue
		}
		if p, ok := decodeStatus(e); ok && p.State == "queued" {
			queuedEvents++
		}
	}
	if queuedEvents != 1 {
		t.Fatalf("r2 got %d queued statuses, want 1", queuedEvents)
	}
	cr.finish(r3)
}

func decodeStatus(e events.Event) (events.AgentStatusPayload, bool) {
	var p events.AgentStatusPayload
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return p, false
	}
	return p, true
}

func TestAgentCapRestartRespectsCap(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	const rooms = 6

	// The old process opened many rooms.
	svc1 := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun, Now: c.Now, MaxAgents: rooms})
	ids := make([]string, 0, rooms)
	for i := 0; i < rooms; i++ {
		ids = append(ids, createRoom(t, svc1, "arjun").ID)
	}
	svc1.Close() // the process dies; every room stays open in the store

	// The new process loads them under cap 2.
	cr := newCapRunner()
	svc2 := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: cr.run, Now: c.Now, MaxAgents: 2})
	t.Cleanup(svc2.Close)
	if err := svc2.Load(context.Background()); err != nil {
		t.Fatalf("load: %v", err)
	}
	settle()
	if got := cr.startedIDs(); len(got) != 0 {
		t.Fatalf("Load must not start runs, started %v", got)
	}

	// Everyone steers their room at once: only 2 run, the rest queue.
	arjun := mustUser(t, svc2, "arjun")
	for _, id := range ids {
		if _, err := svc2.Steer(context.Background(), arjun, id, "continue"); err != nil {
			t.Fatalf("steer %s: %v", id, err)
		}
	}
	cr.waitStarted(t, 2)
	settle()
	if n := len(cr.startedIDs()); n != 2 {
		t.Fatalf("after restart %d runs started, cap is 2", n)
	}
	queued := 0
	for _, id := range ids {
		if agentState(t, svc2, id).State == "queued" {
			queued++
		}
	}
	if queued != rooms-2 {
		t.Fatalf("queued rooms = %d, want %d", queued, rooms-2)
	}

	// Drain them one by one; the cap holds all the way.
	for i := 0; i < rooms; i++ {
		cr.waitStarted(t, i+1)
		cr.finish(cr.startedIDs()[i])
	}
	if m := cr.maxConcurrent(); m > 2 {
		t.Fatalf("max concurrent runs = %d, cap is 2", m)
	}
	if n := len(cr.startedIDs()); n != rooms {
		t.Fatalf("%d rooms ran, want %d", n, rooms)
	}
}

func TestAgentCapFailedRunReleasesSlot(t *testing.T) {
	for _, mode := range []string{"error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			cr := newCapRunner()
			// The room ids are not known before creation, so the first room
			// the runner sees gets the failure on its first run.
			var once sync.Once
			runFirstFails := func(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
				once.Do(func() {
					cr.mu.Lock()
					cr.failing[a.RoomID] = mode
					cr.mu.Unlock()
				})
				return cr.run(ctx, a, d, o)
			}
			svc, _ := newCapService(t, 1, runFirstFails)

			r1 := createRoom(t, svc, "arjun").ID
			cr.waitStarted(t, 1)
			r2 := createRoom(t, svc, "arjun").ID

			// r1 failed at once; its slot goes to r2 without waiting for r1's
			// retry backoff.
			cr.waitStarted(t, 2)
			if got := cr.startedIDs(); !slices.Equal(got[:2], []string{r1, r2}) {
				t.Fatalf("started %v", got)
			}
			if m := cr.maxConcurrent(); m != 1 {
				t.Fatalf("max concurrent runs = %d, want 1", m)
			}

			// r1's retry queues behind r2 and runs once r2 is done.
			cr.finish(r2)
			cr.waitStarted(t, 3)
			if got := cr.startedIDs(); got[2] != r1 {
				t.Fatalf("r1 retry should run third, started %v", got)
			}
			cr.finish(r1)
		})
	}
}

// parkingAgent is a fake agent that, like the real one, reports idle between
// episodes and waits for input. It records episode starts and holds each
// episode until the test lets it finish.
type parkingAgent struct {
	mu       sync.Mutex
	episodes int
	proceed  chan struct{}
}

func (p *parkingAgent) run(ctx context.Context, _ agent.Alert, deps agent.Deps, _ agent.Options) error {
	for {
		_ = deps.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle"})
		for {
			in, err := deps.Inbox.Drain(ctx)
			if err != nil {
				return nil
			}
			if len(in) > 0 {
				break
			}
			if err := deps.Inbox.Wait(ctx); err != nil {
				return nil
			}
		}
		p.mu.Lock()
		p.episodes++
		p.mu.Unlock()
		_ = deps.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"})
		select {
		case <-p.proceed:
		case <-ctx.Done():
			return nil
		}
	}
}

func (p *parkingAgent) episodeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.episodes
}

func TestAgentCapIdleAgentGivesUpSlotAndRequeuesOnInput(t *testing.T) {
	pa := &parkingAgent{proceed: make(chan struct{})}
	cr := newCapRunner()
	var first sync.Once
	var parkedRoom string
	var pmu sync.Mutex
	run := func(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
		isParking := false
		first.Do(func() {
			isParking = true
			pmu.Lock()
			parkedRoom = a.RoomID
			pmu.Unlock()
		})
		if isParking {
			return pa.run(ctx, a, d, o)
		}
		return cr.run(ctx, a, d, o)
	}
	svc, _ := newCapService(t, 1, run)
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")

	r1 := createRoom(t, svc, "arjun").ID // the parking agent: goes idle at once
	waitFor(t, 5*time.Second, func() bool { pmu.Lock(); defer pmu.Unlock(); return parkedRoom == r1 })
	waitFor(t, 5*time.Second, func() bool { return agentState(t, svc, r1).State == "idle" })

	// r1 is idle, so r2 takes the only slot.
	r2 := createRoom(t, svc, "arjun").ID
	cr.waitStarted(t, 1)
	if got := cr.startedIDs(); got[0] != r2 {
		t.Fatalf("started %v, want r2 first", got)
	}

	// Steering the idle r1 while the cap is full queues it; it must not run.
	if _, err := svc.Steer(ctx, arjun, r1, "wake up"); err != nil {
		t.Fatalf("steer: %v", err)
	}
	waitQueued(t, svc, r1)
	settle()
	if n := pa.episodeCount(); n != 0 {
		t.Fatalf("idle agent ran %d episodes outside the cap", n)
	}

	// r2 finishes, r1 gets the slot and runs its episode.
	cr.finish(r2)
	waitFor(t, 5*time.Second, func() bool { return pa.episodeCount() == 1 })
	close(pa.proceed)
}

func TestAgentCapServiceCloseWithQueuedRooms(t *testing.T) {
	cr := newCapRunner()
	// Buffered past every possible start, so a wrong start never blocks the
	// runner and is still counted below.
	cr.startCh = make(chan string, 16)
	svc, st := newCapService(t, 1, cr.run)
	first := createRoom(t, svc, "arjun").ID
	if got := <-cr.startCh; got != first {
		t.Fatalf("first run started for %s, want %s", got, first)
	}
	for i := 0; i < 5; i++ {
		r := createRoom(t, svc, "arjun").ID
		waitQueuedEvent(t, st, r)
	}
	done := make(chan struct{})
	go func() { svc.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung with queued rooms")
	}
	// Close waited for every supervisor, so any run that started is already
	// in the channel.
	close(cr.startCh)
	for id := range cr.startCh {
		t.Errorf("run started for queued room %s during Close", id)
	}
	if n := len(cr.startedIDs()); n != 1 {
		t.Fatalf("%d runs started, want 1", n)
	}
}

// waitQueuedEvent blocks until the room's log holds agent.status "queued",
// reading the store's subscription rather than polling the snapshot.
func waitQueuedEvent(t *testing.T, st *memstore.Store, roomID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := st.Subscribe(ctx, roomID, 0)
	if err != nil {
		t.Fatalf("subscribe %s: %v", roomID, err)
	}
	for e := range ch {
		if e.Type != events.AgentStatus {
			continue
		}
		var p events.AgentStatusPayload
		if json.Unmarshal(e.Payload, &p) == nil && p.State == "queued" {
			return
		}
	}
	t.Fatalf("room %s: no queued agent.status before %v", roomID, ctx.Err())
}

// AgentStats and OpenRooms feed the /metrics gauges: one run holding the only
// slot, one room queued behind it, two open rooms; closing the queued room
// takes it out of both.
func TestAgentStatsAndOpenRooms(t *testing.T) {
	cr := newCapRunner()
	cr.startCh = make(chan string, 4)
	svc, st := newCapService(t, 1, cr.run)
	if n := svc.OpenRooms(); n != 0 {
		t.Fatalf("OpenRooms = %d before any room", n)
	}
	createRoom(t, svc, "arjun")
	<-cr.startCh
	queued := createRoom(t, svc, "arjun").ID
	waitQueuedEvent(t, st, queued)
	if inUse, q := svc.AgentStats(); inUse != 1 || q != 1 {
		t.Fatalf("AgentStats = %d in use, %d queued; want 1, 1", inUse, q)
	}
	if n := svc.OpenRooms(); n != 2 {
		t.Fatalf("OpenRooms = %d, want 2", n)
	}
	if _, err := svc.CloseRoom(context.Background(), mustUser(t, svc, "arjun"), queued, "dup"); err != nil {
		t.Fatalf("close: %v", err)
	}
	if n := svc.OpenRooms(); n != 1 {
		t.Fatalf("OpenRooms after close = %d, want 1", n)
	}
}
