package room_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// fakeTriager writes one agent.triage through the room's sink, like
// agent.Endpoint.Triage, and records its calls.
type fakeTriager struct {
	mu    sync.Mutex
	calls []string
	block chan struct{} // when set, each pass waits for it
}

func (f *fakeTriager) triage(ctx context.Context, a agent.Alert, d agent.Deps, _ agent.Options) (agent.TriageResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, a.RoomID)
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return agent.TriageResult{}, ctx.Err()
		}
	}
	p := events.AgentTriagePayload{Status: "ok", Headline: "task 0 FAILED", SuspectedLayer: "kafka-connect", Confidence: "medium", EvidenceRefs: []string{}, Model: "haiku"}
	if err := d.Sink.Emit(ctx, events.AgentTriage, p); err != nil {
		return agent.TriageResult{}, err
	}
	return agent.TriageResult{Payload: p, Steps: []agent.TriageStep{}}, nil
}

func (f *fakeTriager) n() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// alertRun records the alert each run got and holds the run until ctx ends.
type alertRun struct {
	mu     sync.Mutex
	alerts map[string]agent.Alert
	inbox  map[string]agent.Inbox
}

func (r *alertRun) run(ctx context.Context, a agent.Alert, d agent.Deps, _ agent.Options) error {
	r.mu.Lock()
	r.alerts[a.RoomID] = a
	r.inbox[a.RoomID] = d.Inbox
	r.mu.Unlock()
	_ = d.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle"})
	<-ctx.Done()
	return nil
}

func (r *alertRun) get(id string) (agent.Alert, agent.Inbox, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.alerts[id]
	return a, r.inbox[id], ok
}

func newTriageService(t *testing.T, maxAgents int, tr *fakeTriager, now func() time.Time) (*room.Service, *alertRun) {
	t.Helper()
	r := &alertRun{alerts: map[string]agent.Alert{}, inbox: map[string]agent.Inbox{}}
	st := memstore.New()
	if now != nil {
		st = memstore.NewWithClock(now)
	}
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: r.run, Triage: tr.triage,
		TriagePassTimeout: time.Second, MaxAgents: maxAgents, Now: now})
	t.Cleanup(svc.Close)
	return svc, r
}

func openGeneric(t *testing.T, svc *room.Service, key string, raw json.RawMessage) (room.RoomSnapshot, bool) {
	t.Helper()
	snap, created, err := svc.OpenAlertRoom(context.Background(), alertin.Alert{
		Source: alertin.Generic, Status: alertin.StatusFiring, Title: "orders stale", Severity: "sev2",
		PipelineID: "orders", DedupKey: "generic:" + key, Raw: raw,
	})
	if err != nil {
		t.Fatalf("OpenAlertRoom: %v", err)
	}
	return snap, created
}

func roomTriage(t *testing.T, svc *room.Service, id string) *room.Triage {
	t.Helper()
	snap, err := svc.GetRoom(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return snap.Triage
}

func TestTriageRunsBeforeSlot(t *testing.T) {
	tr := &fakeTriager{}
	svc, runs := newTriageService(t, 1, tr, nil)
	first, _ := openGeneric(t, svc, "a", nil)
	waitFor(t, 2*time.Second, func() bool { _, _, ok := runs.get(first.ID); return ok })
	// The only slot is held: the second room queues, but its first look runs.
	second, _ := openGeneric(t, svc, "b", nil)
	waitFor(t, 2*time.Second, func() bool { return roomTriage(t, svc, second.ID) != nil })
	if _, _, ok := runs.get(second.ID); ok {
		t.Fatal("the second room's agent ran without a slot")
	}
	if got := roomTriage(t, svc, second.ID); got.Headline != "task 0 FAILED" || got.Status != "ok" {
		t.Fatalf("triage = %+v", got)
	}
	// The slot frees: the queued run starts with the first look in its alert.
	// An alert room has no driver: take control, then close.
	if _, err := svc.GrantControl(context.Background(), mustUser(t, svc, "arjun"), first.ID, "arjun"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CloseRoom(context.Background(), mustUser(t, svc, "arjun"), first.ID, "done"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { _, _, ok := runs.get(second.ID); return ok })
	a, _, _ := runs.get(second.ID)
	if a.Triage == nil || a.Triage.Payload.Headline != "task 0 FAILED" {
		t.Fatalf("run alert triage = %+v", a.Triage)
	}
	if tr.n() != 2 {
		t.Fatalf("passes = %d, want one per room", tr.n())
	}
}

func TestTriageWaitIsBounded(t *testing.T) {
	tr := &fakeTriager{block: make(chan struct{})}
	defer close(tr.block)
	svc, runs := newTriageService(t, 2, tr, nil)
	// The wait is TriagePassTimeout 1 s + the 5 s margin; the test then
	// shortens it so it need not sit out six seconds.
	if got := room.TriageWaitForTest(svc); got != 6*time.Second {
		t.Fatalf("triage wait = %s, want 6s", got)
	}
	room.SetTriageWaitForTest(svc, 100*time.Millisecond)
	snap, _ := openGeneric(t, svc, "a", nil)
	// The run starts without the pass once the wait is over.
	waitFor(t, 5*time.Second, func() bool { _, _, ok := runs.get(snap.ID); return ok })
	if a, _, _ := runs.get(snap.ID); a.Triage != nil {
		t.Fatalf("run got a first look that never finished: %+v", a.Triage)
	}
}

func TestTriageSkippedForShadowAndRestoredRooms(t *testing.T) {
	tr := &fakeTriager{}
	env := newShadowEnv(t, shadowCfg{})
	env.svc.Close()
	env.deps.Triage = tr.triage
	env.svc = room.New(env.deps)
	t.Cleanup(env.svc.Close)
	env.open(t, "shadow-1")
	time.Sleep(100 * time.Millisecond)
	if tr.n() != 0 {
		t.Fatalf("a shadow room got %d first looks", tr.n())
	}

	// Restored: a room opened before a restart gets no first look when its
	// agent starts again.
	st := memstore.New()
	r := &alertRun{alerts: map[string]agent.Alert{}, inbox: map[string]agent.Inbox{}}
	deps := room.Deps{Store: st, Users: room.DefaultUsers(), Run: r.run}
	svc1 := room.New(deps)
	snap, err := svc1.CreateRoom(context.Background(), mustUser(t, svc1, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	svc1.Close()
	deps.Triage = tr.triage
	svc2 := room.New(deps)
	t.Cleanup(svc2.Close)
	if err := svc2.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := svc2.Steer(context.Background(), mustUser(t, svc2, "arjun"), snap.ID, "look again"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { _, _, ok := r.get(snap.ID); return ok })
	if tr.n() != 0 {
		t.Fatalf("a restored room got %d first looks", tr.n())
	}
}

func TestAlertUpdateReachesNextTurn(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)}
	tr := &fakeTriager{}
	svc, runs := newTriageService(t, 2, tr, c.Now)
	raw := json.RawMessage(`{"source":"watcher","signals":[{"signal":"slot_inactive","slot":"debezium_orders","active":false}]}`)
	snap, _ := openGeneric(t, svc, "a", raw)
	waitFor(t, 2*time.Second, func() bool { _, _, ok := runs.get(snap.ID); return ok })
	_, inbox, _ := runs.get(snap.ID)

	// Two re-fires: the second update replaces the first.
	for range 2 {
		if _, created := openGeneric(t, svc, "a", raw); created {
			t.Fatal("a re-fire opened a second room")
		}
	}
	wctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := inbox.Wait(wctx); err == nil {
		t.Fatal("an alert update woke the agent")
	}
	got, err := inbox.Drain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Kind != "alert_update" || got[0].Text != "source=watcher\nsignal=slot_inactive active=false slot=debezium_orders" {
		t.Fatalf("drained = %+v", got)
	}
	if tr.n() != 1 {
		t.Fatalf("passes = %d; a re-fire within 15 min must not re-run it", tr.n())
	}

	// 15 minutes later, with the agent idle (alertRun says so), a re-fire
	// runs the pass again.
	c.Advance(16 * time.Minute)
	openGeneric(t, svc, "a", raw)
	waitFor(t, 2*time.Second, func() bool { return tr.n() == 2 })
}

func TestPostRoomEvidenceFeedsAlertUpdate(t *testing.T) {
	tr := &fakeTriager{}
	svc, runs := newTriageService(t, 2, tr, nil)
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { _, _, ok := runs.get(snap.ID); return ok })
	_, inbox, _ := runs.get(snap.ID)
	if err := svc.PostRoomNote(context.Background(), snap.ID, "still firing"); err != nil {
		t.Fatal(err)
	}
	if got, _ := inbox.Drain(context.Background()); len(got) != 0 {
		t.Fatalf("a plain note reached the agent: %+v", got)
	}
	ev := json.RawMessage(`{"source":"watcher","signals":[{"signal":"consumer_lag","group":"order-sync","total_lag":1471,"note":"FREE TEXT"}]}`)
	if err := svc.PostRoomEvidence(context.Background(), snap.ID, "still firing", ev); err != nil {
		t.Fatal(err)
	}
	got, _ := inbox.Drain(context.Background())
	if len(got) != 1 || got[0].Kind != "alert_update" || got[0].Text != "source=watcher\nsignal=consumer_lag group=order-sync total_lag=1471" {
		t.Fatalf("drained = %+v", got)
	}
}
