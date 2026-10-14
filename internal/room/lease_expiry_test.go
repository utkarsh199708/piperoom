package room

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

var leaseT0 = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)

// leaseRig is one Service with a hand-fed lease ticker. tick runs one check of
// the loop and returns when it has finished.
type leaseRig struct {
	t      *testing.T
	svc    *Service
	ticks  chan time.Time
	done   chan struct{}
	stops  chan struct{}
	period time.Duration
}

func newLeaseRig(t *testing.T, st store.Store, c *testClock) *leaseRig {
	t.Helper()
	r := &leaseRig{t: t, ticks: make(chan time.Time), done: make(chan struct{}), stops: make(chan struct{}, 1)}
	r.svc = New(Deps{
		Store:  st,
		Users:  DefaultUsers(),
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}},
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		Now: c.Now,
		LeaseTicker: func(d time.Duration) (<-chan time.Time, func()) {
			r.period = d
			return r.ticks, func() { r.stops <- struct{}{} }
		},
	})
	r.svc.leaseTickDone = func() { r.done <- struct{}{} }
	t.Cleanup(r.svc.Close)
	return r
}

func (r *leaseRig) load() {
	r.t.Helper()
	if err := r.svc.Load(context.Background()); err != nil {
		r.t.Fatalf("load: %v", err)
	}
}

func (r *leaseRig) tick() {
	r.t.Helper()
	r.ticks <- time.Time{}
	<-r.done
}

func leaseUser(t *testing.T, svc *Service, id string) User {
	t.Helper()
	u, ok := svc.LookupUser(id)
	if !ok {
		t.Fatalf("no user %q", id)
	}
	return u
}

// expiries returns the control.lease_expired payloads in the room's log.
func expiries(t *testing.T, st store.Store, roomID string) []events.ControlLeaseExpiredPayload {
	t.Helper()
	evs, err := st.Since(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out []events.ControlLeaseExpiredPayload
	for _, e := range evs {
		if e.Type != events.ControlLeaseExpired {
			continue
		}
		if e.Actor != systemActor {
			t.Fatalf("control.lease_expired actor = %+v, want the system actor", e.Actor)
		}
		p, err := events.Decode[events.ControlLeaseExpiredPayload](e)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

func mustRoom(t *testing.T, svc *Service, roomID string) RoomSnapshot {
	t.Helper()
	snap, err := svc.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestLeaseExpiryOncePerLeaseAcrossRestarts(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: leaseT0}
	ms := memstore.NewWithClock(c.Now)

	// The room is opened by a server that then stops before any check: the
	// next server emits once.
	first := newLeaseRig(t, ms, c)
	arjun := leaseUser(t, first.svc, "arjun")
	snap, err := first.svc.CreateRoom(ctx, arjun, "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	roomID := snap.ID
	lease := leaseT0.Add(DriverLease)
	first.svc.Close()

	r := newLeaseRig(t, ms, c)
	r.load()

	c.Advance(DriverLease - time.Second)
	r.tick()
	// Read after a tick: the loop set it before its first receive.
	if r.period != LeaseCheckEvery {
		t.Fatalf("ticker period = %v, want %v", r.period, LeaseCheckEvery)
	}
	if got := expiries(t, ms, roomID); len(got) != 0 {
		t.Fatalf("before expiry: %d events, want 0", len(got))
	}

	c.Advance(time.Second) // exactly lease_until: the lease has passed
	r.tick()
	got := expiries(t, ms, roomID)
	if len(got) != 1 {
		t.Fatalf("after expiry: %d events, want 1", len(got))
	}
	if got[0].Driver.ID != "arjun" || !got[0].LeaseUntil.Equal(lease) {
		t.Fatalf("payload = %+v, want driver arjun lease_until %v", got[0], lease)
	}
	snap = mustRoom(t, r.svc, roomID)
	if snap.Driver == nil || snap.Driver.ID != "arjun" || !snap.Driver.LeaseExpired {
		t.Fatalf("driver after expiry = %+v, want arjun still driving with lease_expired", snap.Driver)
	}
	b, _ := json.Marshal(snap.Driver)
	if !strings.Contains(string(b), `"lease_expired":true`) {
		t.Fatalf("driver JSON = %s, want lease_expired true", b)
	}

	// Later ticks do not repeat it.
	c.Advance(time.Minute)
	r.tick()
	r.tick()
	if n := len(expiries(t, ms, roomID)); n != 1 {
		t.Fatalf("later ticks: %d events, want 1", n)
	}

	// A restart folds the event from the log and does not repeat it.
	r.svc.Close()
	r2 := newLeaseRig(t, ms, c)
	r2.load()
	r2.tick()
	if n := len(expiries(t, ms, roomID)); n != 1 {
		t.Fatalf("after restart: %d events, want 1", n)
	}
	if snap := mustRoom(t, r2.svc, roomID); !snap.Driver.LeaseExpired {
		t.Fatal("after restart the snapshot lost the lease_expired signal")
	}

	// The driver acts: the renewal is logged (control.granted to arjun) and
	// ends the signal; the new lease expires again with its own event.
	if _, err := r2.svc.Steer(ctx, arjun, roomID, "check the connector"); err != nil {
		t.Fatal(err)
	}
	snap = mustRoom(t, r2.svc, roomID)
	if snap.Driver.LeaseExpired || snap.Driver.ID != "arjun" {
		t.Fatalf("driver after renewal = %+v, want arjun without the signal", snap.Driver)
	}
	renewed := snap.Driver.LeaseUntil
	if !renewed.Equal(c.Now().Add(DriverLease)) {
		t.Fatalf("renewed lease = %v, want %v", renewed, c.Now().Add(DriverLease))
	}
	r2.tick()
	if n := len(expiries(t, ms, roomID)); n != 1 {
		t.Fatalf("after renewal: %d events, want 1", n)
	}
	c.Advance(DriverLease)
	r2.tick()
	got = expiries(t, ms, roomID)
	if len(got) != 2 || !got[1].LeaseUntil.Equal(renewed) {
		t.Fatalf("second lease: %+v, want a second event with lease_until %v", got, renewed)
	}
}

func TestLeaseExpiryNotForClosedNoDriverOrReplacedDriver(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: leaseT0}
	ms := memstore.NewWithClock(c.Now)
	r := newLeaseRig(t, ms, c)
	r.load()
	arjun := leaseUser(t, r.svc, "arjun")
	other := otherUser(t, r.svc, "arjun")

	closed, err := r.svc.CreateRoom(ctx, arjun, "closed", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.svc.CloseRoom(ctx, arjun, closed.ID, "fixed"); err != nil {
		t.Fatal(err)
	}
	alert, _, err := r.svc.CreateRoomFromAlert(ctx, "alert", "sev2", "orders", json.RawMessage(`{"title":"lag"}`))
	if err != nil {
		t.Fatal(err)
	}
	if alert.Driver != nil {
		t.Fatalf("alert room has a driver: %+v", alert.Driver)
	}
	taken, err := r.svc.CreateRoom(ctx, arjun, "taken", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}

	// The lease ran out; another user takes control before the check.
	c.Advance(DriverLease + time.Minute)
	if _, err := r.svc.GrantControl(ctx, other, taken.ID, other.ID); err != nil {
		t.Fatalf("take control after expiry: %v", err)
	}
	r.tick()
	for _, id := range []string{closed.ID, alert.ID, taken.ID} {
		if got := expiries(t, ms, id); len(got) != 0 {
			t.Fatalf("room %s: %+v, want no control.lease_expired", id, got)
		}
	}
	if snap := mustRoom(t, r.svc, taken.ID); snap.Driver.ID != other.ID || snap.Driver.LeaseExpired {
		t.Fatalf("taken room driver = %+v", snap.Driver)
	}
}

// otherUser returns a directory user other than not.
func otherUser(t *testing.T, svc *Service, not string) User {
	t.Helper()
	for _, u := range svc.AllUsers() {
		if u.ID != not {
			return u
		}
	}
	t.Fatal("need a second user")
	return User{}
}

// An action with 25 minutes or more left renews the lease on the rooms row
// only. The event waits for that enforced lease but names the logged one, the
// value clients folding the log know.
func TestLeaseExpiryAfterUnloggedRenewal(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: leaseT0}
	ms := memstore.NewWithClock(c.Now)
	r := newLeaseRig(t, ms, c)
	r.load()
	arjun := leaseUser(t, r.svc, "arjun")
	snap, err := r.svc.CreateRoom(ctx, arjun, "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	logged := leaseT0.Add(DriverLease)
	c.Advance(2 * time.Minute)
	if _, err := r.svc.Steer(ctx, arjun, snap.ID, "look at lag"); err != nil {
		t.Fatal(err)
	}
	enforced := c.Now().Add(DriverLease)

	c.Advance(DriverLease - 2*time.Minute) // past the logged lease only
	r.tick()
	if n := len(expiries(t, ms, snap.ID)); n != 0 {
		t.Fatalf("before the enforced lease: %d events, want 0", n)
	}
	c.Advance(enforced.Sub(c.Now()))
	r.tick()
	got := expiries(t, ms, snap.ID)
	if len(got) != 1 || !got[0].LeaseUntil.Equal(logged) {
		t.Fatalf("events = %+v, want one naming the logged lease %v", got, logged)
	}
	if s := mustRoom(t, r.svc, snap.ID); !s.Driver.LeaseExpired {
		t.Fatal("snapshot does not show the signal")
	}
	r.tick()
	if n := len(expiries(t, ms, snap.ID)); n != 1 {
		t.Fatalf("repeat tick: %d events, want 1", n)
	}
}

func TestLeaseExpiryLoopStopsOnClose(t *testing.T) {
	before := runtime.NumGoroutine()
	c := &testClock{t: leaseT0}
	r := newLeaseRig(t, memstore.NewWithClock(c.Now), c)
	r.load()
	r.tick() // the loop is running
	r.svc.Close()
	select {
	case <-r.stops:
	default:
		t.Fatal("Close returned but the lease ticker was not stopped")
	}
	for i := 0; i < 10000 && runtime.NumGoroutine() > before; i++ {
		runtime.Gosched()
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutines: %d after Close, %d before", n, before)
	}
	// Load after Close starts nothing.
	r2 := newLeaseRig(t, memstore.NewWithClock(c.Now), c)
	r2.svc.Close()
	_ = r2.svc.Load(context.Background())
	select {
	case r2.ticks <- time.Time{}:
		t.Fatal("a lease loop runs after Close")
	default:
	}
}

// fakeCoord is a memstore that claims to be shared by several replicas, with
// room leases the test sets. Coordinator methods the lease check does not use
// are left nil and panic if called.
type fakeCoord struct {
	*memstore.Store
	store.Coordinator
	leases map[string]store.RoomLease
}

func (f *fakeCoord) RoomLeases(context.Context) (map[string]store.RoomLease, error) {
	return f.leases, nil
}

func (f *fakeCoord) ExpireRoomLeases(context.Context, string) error { return nil }

func (f *fakeCoord) AppendIfLast(ctx context.Context, roomID string, expectSeq int64, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	row, err := f.Store.GetRoom(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	if row.LastSeq != expectSeq {
		return events.Event{}, fmt.Errorf("fake: last seq %d, want %d: %w", row.LastSeq, expectSeq, store.ErrSeqConflict)
	}
	return f.Store.Append(ctx, roomID, typ, actor, payload)
}

func TestLeaseExpiryReplicas(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: leaseT0}
	ms := memstore.NewWithClock(c.Now)
	// Open three rooms on a single-process service, then look at them from
	// a replica of a shared store.
	setup := newLeaseRig(t, ms, c)
	arjun := leaseUser(t, setup.svc, "arjun")
	var ids []string
	for i := 0; i < 3; i++ {
		snap, err := setup.svc.CreateRoom(ctx, arjun, fmt.Sprintf("r%d", i), "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, snap.ID)
	}
	setup.svc.Close()
	heldElsewhere, heldHere, unheld := ids[0], ids[1], ids[2]

	fc := &fakeCoord{Store: ms, leases: map[string]store.RoomLease{
		heldElsewhere: {RoomID: heldElsewhere, Owner: "replica-b"},
		heldHere:      {RoomID: heldHere, Owner: "replica-a"},
	}}
	svc := New(Deps{Store: fc, Users: DefaultUsers(), Now: c.Now, ReplicaID: "replica-a"})
	t.Cleanup(svc.Close)
	if svc.coord == nil {
		t.Fatal("fake store not seen as a coordinator")
	}
	for _, id := range ids {
		mustRoom(t, svc, id)
	}
	c.Advance(DriverLease)
	svc.checkLeases(ctx)
	if n := len(expiries(t, ms, heldElsewhere)); n != 0 {
		t.Fatalf("room held by another replica: %d events, want 0", n)
	}
	for _, id := range []string{heldHere, unheld} {
		if n := len(expiries(t, ms, id)); n != 1 {
			t.Fatalf("room %s: %d events, want 1", id, n)
		}
	}
	svc.checkLeases(ctx)
	if n := len(expiries(t, ms, unheld)); n != 1 {
		t.Fatalf("second check: %d events, want 1", n)
	}

	// Another replica wrote the event first: this one folds it on its
	// conditional append's conflict and writes nothing.
	svc2 := New(Deps{Store: fc, Users: DefaultUsers(), Now: c.Now, ReplicaID: "replica-c"})
	t.Cleanup(svc2.Close)
	mustRoom(t, svc2, heldElsewhere)
	fc.leases = map[string]store.RoomLease{}
	if _, err := ms.Append(ctx, heldElsewhere, events.ControlLeaseExpired, systemActor, events.ControlLeaseExpiredPayload{
		Driver: events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: arjun.Name}, LeaseUntil: leaseT0.Add(DriverLease),
	}); err != nil {
		t.Fatal(err)
	}
	svc2.emitLeaseExpired(ctx, heldElsewhere) // its cached snapshot is one event behind
	if n := len(expiries(t, ms, heldElsewhere)); n != 1 {
		t.Fatalf("after another replica's event: %d events, want 1", n)
	}
}

func TestApplyLeaseExpired(t *testing.T) {
	lease := leaseT0.Add(DriverLease)
	arjun := events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun", Verified: true}
	maya := events.Actor{Kind: events.ActorHuman, ID: "maya", Name: "Maya", Verified: true}
	ev := func(seq int64, typ events.Type, actor events.Actor, p any) events.Event {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		return events.Event{Seq: seq, RoomID: "r1", Type: typ, Actor: actor, Payload: b}
	}
	created := ev(1, events.RoomCreated, arjun, events.RoomCreatedPayload{Title: "t", Severity: "sev2", PipelineID: "orders"})
	granted := func(seq int64, to events.Actor, until time.Time) events.Event {
		return ev(seq, events.ControlGranted, to, events.ControlGrantedPayload{To: to, LeaseUntil: until})
	}
	expired := func(seq int64, d events.Actor, until time.Time) events.Event {
		return ev(seq, events.ControlLeaseExpired, systemActor, events.ControlLeaseExpiredPayload{Driver: d, LeaseUntil: until})
	}
	cases := []struct {
		name    string
		log     []events.Event
		driver  string
		expired bool
	}{
		{"no event", []events.Event{created, granted(2, arjun, lease)}, "arjun", false},
		{"current lease", []events.Event{created, granted(2, arjun, lease), expired(3, arjun, lease)}, "arjun", true},
		{"older lease", []events.Event{created, granted(2, arjun, lease.Add(time.Hour)), expired(3, arjun, lease)}, "arjun", false},
		{"other driver", []events.Event{created, granted(2, arjun, lease), expired(3, maya, lease)}, "arjun", false},
		{"no driver", []events.Event{created, expired(2, arjun, lease)}, "", false},
		{"renewal clears", []events.Event{created, granted(2, arjun, lease), expired(3, arjun, lease), granted(4, arjun, lease.Add(time.Hour))}, "arjun", false},
		{"new driver clears", []events.Event{created, granted(2, arjun, lease), expired(3, arjun, lease), granted(4, maya, lease.Add(time.Hour))}, "maya", false},
		{"stale event after renewal", []events.Event{created, granted(2, arjun, lease), granted(3, arjun, lease.Add(time.Hour)), expired(4, arjun, lease)}, "arjun", false},
		{"bad payload", []events.Event{created, granted(2, arjun, lease), {Seq: 3, RoomID: "r1", Type: events.ControlLeaseExpired, Actor: systemActor, Payload: json.RawMessage(`"x"`)}}, "arjun", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := NewSnapshot()
			for _, e := range tc.log {
				snap = Apply(snap, e)
			}
			if tc.driver == "" {
				if snap.Driver != nil {
					t.Fatalf("driver = %+v, want none", snap.Driver)
				}
				return
			}
			if snap.Driver == nil || snap.Driver.ID != tc.driver || snap.Driver.LeaseExpired != tc.expired {
				t.Fatalf("driver = %+v, want %s lease_expired=%v", snap.Driver, tc.driver, tc.expired)
			}
		})
	}

	// Apply does not modify its input snapshot.
	in := Apply(Apply(NewSnapshot(), created), granted(2, arjun, lease))
	out := Apply(in, expired(3, arjun, lease))
	if in.Driver.LeaseExpired || !out.Driver.LeaseExpired {
		t.Fatalf("input modified or output unmarked: in=%+v out=%+v", in.Driver, out.Driver)
	}
	// Nothing else moves: the driver keeps role and lease.
	if out.Driver.LeaseUntil != in.Driver.LeaseUntil || len(out.Members) != len(in.Members) || out.Members[0].Role != "driver" {
		t.Fatalf("lease_expired moved state: in=%+v out=%+v", in, out)
	}
}
