package room

import (
	"context"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// leaseCoord is fakeCoord with owner-aware leases: a room's lease belongs to
// the first replica that takes it.
type leaseCoord struct {
	*fakeCoord
	mu     sync.Mutex
	owners map[string]string
}

func (l *leaseCoord) AcquireRoomLease(_ context.Context, roomID, owner, _ string, _ time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if o, ok := l.owners[roomID]; ok && o != owner {
		return false, nil
	}
	l.owners[roomID] = owner
	return true, nil
}
func (l *leaseCoord) RenewRoomLeases(_ context.Context, _ string, ids []string, _ time.Duration) ([]string, error) {
	return ids, nil
}
func (l *leaseCoord) ReleaseRoomLease(context.Context, string, string) error { return nil }
func (l *leaseCoord) TakeAgentSlot(context.Context, string, string, int) (bool, error) {
	return true, nil
}
func (l *leaseCoord) ReleaseAgentSlot(context.Context, string, string) error { return nil }

// twoReplicas is replica B, which opens rooms and runs their agents, and
// replica A (the watcher's leader), on one store with owner-aware leases.
func twoReplicas(t *testing.T, runB func(context.Context, agent.Alert, agent.Deps, agent.Options) error) (a, b *Service) {
	t.Helper()
	c := &testClock{t: leaseT0}
	ms := memstore.NewWithClock(c.Now)
	shared := &leaseCoord{fakeCoord: &fakeCoord{Store: ms, leases: map[string]store.RoomLease{}}, owners: map[string]string{}}
	mk := func(id string, run func(context.Context, agent.Alert, agent.Deps, agent.Options) error) *Service {
		s := New(Deps{
			Store: shared, Users: DefaultUsers(), Now: c.Now, ReplicaID: id,
			Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}},
			Run:    run,
		})
		t.Cleanup(s.Close)
		return s
	}
	b = mk("replica-b", runB)
	a = mk("replica-a", func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
		t.Error("replica A must not run the agent")
		<-ctx.Done()
		return nil
	})
	return a, b
}

func nextDeps(t *testing.T, ch <-chan agent.Deps, what string) agent.Deps {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: no agent run started on B", what)
		return agent.Deps{}
	}
}

func wantNewIncident(t *testing.T, d agent.Deps) {
	t.Helper()
	wctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := d.Inbox.Wait(wctx); err != nil {
		t.Fatalf("B's agent never got the new incident: %v", err)
	}
	ins, err := d.Inbox.Drain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, in := range ins {
		if in.NewIncident && in.Kind == "alert_update" && in.Text != "" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("inputs = %+v, want exactly one new-incident alert_update", ins)
	}
}

// QA F21: the watcher's leader (A) joins a new incident into a room whose
// agent lease B holds. The log is the store-backed inbox: B folds A's note
// and its parked agent wakes with the new incident.
func TestNewIncidentReachesTheLeaseOwner(t *testing.T) {
	ctx := context.Background()
	depsB := make(chan agent.Deps, 4)
	a, b := twoReplicas(t, func(ctx context.Context, _ agent.Alert, d agent.Deps, _ agent.Options) error {
		depsB <- d
		<-ctx.Done()
		return nil
	})
	snap, err := b.CreateRoom(ctx, leaseUser(t, b, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	d := nextDeps(t, depsB, "first run")
	_, _ = d.Inbox.Drain(ctx)

	if err := a.PostIncidentJSON(ctx, snap.ID, "watcher: orders: consumer lag (new signal)", nil); err != nil {
		t.Fatal(err)
	}
	b.syncRoom(ctx, snap.ID)
	wantNewIncident(t, d)

	// A plain note or evidence join is not a new incident.
	if err := a.PostRoomNote(ctx, snap.ID, "watcher: still firing"); err != nil {
		t.Fatal(err)
	}
	b.syncRoom(ctx, snap.ID)
	if ins, _ := d.Inbox.Drain(ctx); len(ins) != 0 {
		t.Fatalf("a plain note reached the agent: %+v", ins)
	}
}

// QA F21: B's run ended on its own (it holds the lease, its supervisor is
// gone). Folding A's new incident starts a fresh run on B with the input.
func TestNewIncidentRestartsAnEndedRunOnTheLeaseOwner(t *testing.T) {
	ctx := context.Background()
	depsB := make(chan agent.Deps, 4)
	var runs sync.Mutex
	n := 0
	a, b := twoReplicas(t, func(ctx context.Context, _ agent.Alert, d agent.Deps, _ agent.Options) error {
		runs.Lock()
		n++
		first := n == 1
		runs.Unlock()
		depsB <- d
		if first {
			return nil // ends by itself
		}
		<-ctx.Done()
		return nil
	})
	snap, err := b.CreateRoom(ctx, leaseUser(t, b, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	_ = nextDeps(t, depsB, "first run")
	deadline := time.Now().Add(5 * time.Second)
	for {
		b.mu.Lock()
		st := b.rooms[snap.ID]
		done := st != nil && st.agentDone == st.agentRun && st.agentRun > 0
		b.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the first run never ended")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := a.PostIncidentJSON(ctx, snap.ID, "watcher: orders: connector down (new signal)", nil); err != nil {
		t.Fatal(err)
	}
	b.syncRoom(ctx, snap.ID)
	wantNewIncident(t, nextDeps(t, depsB, "after the new incident"))
}
