package room

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// Sole-member driver (P2b-2, docs/api.md item 4).

var soleT0 = time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)

func soleOwners() *owners.Owners {
	on := true
	return &owners.Owners{Pipelines: []owners.Pipeline{
		{ID: "orders", SourceTables: []string{"public.orders"}},
		{ID: "shadowed", SourceTables: []string{"public.shadowed"}, ShadowSpec: &owners.ShadowSpec{Enabled: &on}},
	}}
}

func soleDeps(st store.Store, c *testClock) Deps {
	return Deps{Store: st, Users: DefaultUsers(), Owners: soleOwners(), Run: idleRun, Now: c.Now,
		ViewerGroups: []string{"viewers"}}
}

func newSoleService(t *testing.T, st store.Store, c *testClock) *Service {
	t.Helper()
	svc := New(soleDeps(st, c))
	t.Cleanup(svc.Close)
	if err := svc.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	return svc
}

func openSoleAlertRoom(t *testing.T, svc *Service, pipelineID, key string) string {
	t.Helper()
	snap, created, err := svc.OpenAlertRoom(context.Background(), alertin.Alert{
		Source: alertin.Generic, Status: alertin.StatusFiring, Title: "lag", Severity: "sev2",
		PipelineID: pipelineID, DedupKey: "generic:" + key, Raw: json.RawMessage(`{"alert":"lag"}`),
	})
	if err != nil || !created {
		t.Fatalf("open alert room: created=%v err=%v", created, err)
	}
	if snap.Driver != nil {
		t.Fatalf("alert room opened with driver %+v, want none", snap.Driver)
	}
	return snap.ID
}

// soleGrants returns the control.granted events in the room's log.
func grantsIn(t *testing.T, st store.Store, roomID string) []events.Event {
	t.Helper()
	evs, err := st.Since(context.Background(), roomID, 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	var out []events.Event
	for _, e := range evs {
		if e.Type == events.ControlGranted {
			out = append(out, e)
		}
	}
	return out
}

func viewer() User {
	return User{ID: "vic", Name: "Vic", Groups: []string{"viewers"}}
}

func mustJoin(t *testing.T, svc *Service, u User, roomID string) (events.Event, bool) {
	t.Helper()
	e, granted, err := svc.JoinRoom(context.Background(), u, roomID)
	if err != nil {
		t.Fatalf("join %s: %v", u.ID, err)
	}
	return e, granted
}

func TestSoleMemberFirstJoinGetsDriver(t *testing.T) {
	c := &testClock{t: soleT0}
	ms := memstore.NewWithClock(c.Now)
	svc := newSoleService(t, ms, c)
	ctx := context.Background()
	roomID := openSoleAlertRoom(t, svc, "orders", "a")
	priya := leaseUser(t, svc, "priya")
	arjun := leaseUser(t, svc, "arjun")

	e, granted := mustJoin(t, svc, priya, roomID)
	if !granted {
		t.Fatal("first member join: not granted")
	}
	p, ok := decode[events.ControlGrantedPayload](e)
	if !ok || p.Reason != events.ControlReasonSoleMember || p.To.ID != "priya" || !p.To.Verified ||
		!p.LeaseUntil.Equal(soleT0.Add(DriverLease)) {
		t.Fatalf("grant payload = %+v, want sole_member to priya until %s", p, soleT0.Add(DriverLease))
	}
	if e.Actor != systemActor {
		t.Fatalf("grant actor = %+v, want the system", e.Actor)
	}
	snap, err := svc.GetRoom(ctx, roomID)
	if err != nil || snap.Driver == nil || snap.Driver.ID != "priya" {
		t.Fatalf("driver after join = %+v (err %v), want priya", snap.Driver, err)
	}
	if row, err := ms.GetRoom(ctx, roomID); err != nil || row.DriverID != "priya" {
		t.Fatalf("room row driver = %q (err %v), want priya persisted", row.DriverID, err)
	}

	// A second member, the driver again, and a viewer: nothing changes.
	for _, u := range []User{arjun, priya, viewer()} {
		if _, granted := mustJoin(t, svc, u, roomID); granted {
			t.Fatalf("%s's join granted control, want nothing", u.ID)
		}
	}
	if g := grantsIn(t, ms, roomID); len(g) != 1 {
		t.Fatalf("control.granted events = %d, want 1", len(g))
	}
	if snap, _ := svc.GetRoom(ctx, roomID); snap.Driver.ID != "priya" {
		t.Fatalf("driver after later joins = %s, want priya", snap.Driver.ID)
	}
	if n := svc.SoleMemberGrants(); n != 1 {
		t.Fatalf("SoleMemberGrants = %d, want 1", n)
	}

	// A lapsed lease is still a room that had a driver: a later member
	// takes control by asking, not by walking in.
	c.Advance(DriverLease + time.Minute)
	if _, granted := mustJoin(t, svc, arjun, roomID); granted {
		t.Fatal("join after the sole member's lease lapsed granted control")
	}
	if n := svc.SoleMemberGrants(); n != 1 {
		t.Fatalf("SoleMemberGrants = %d, want 1", n)
	}
}

func TestSoleMemberViewerNeverDrives(t *testing.T) {
	c := &testClock{t: soleT0}
	ms := memstore.NewWithClock(c.Now)
	svc := newSoleService(t, ms, c)
	roomID := openSoleAlertRoom(t, svc, "orders", "v")

	if _, granted := mustJoin(t, svc, viewer(), roomID); granted {
		t.Fatal("viewer's join granted control")
	}
	if g := grantsIn(t, ms, roomID); len(g) != 0 {
		t.Fatalf("control.granted after a viewer joined = %d, want 0", len(g))
	}
	// The first member after the viewer is still the first member.
	if _, granted := mustJoin(t, svc, leaseUser(t, svc, "arjun"), roomID); !granted {
		t.Fatal("member after a viewer: not granted")
	}
	if n := svc.SoleMemberGrants(); n != 1 {
		t.Fatalf("SoleMemberGrants = %d, want 1", n)
	}
}

func TestSoleMemberNotForOtherRooms(t *testing.T) {
	c := &testClock{t: soleT0}
	ms := memstore.NewWithClock(c.Now)
	svc := newSoleService(t, ms, c)
	ctx := context.Background()
	priya := leaseUser(t, svc, "priya")
	arjun := leaseUser(t, svc, "arjun")

	// A room opened by hand: not an alert room, and its creator drives.
	hand, err := svc.CreateRoom(ctx, arjun, "by hand", "sev3", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, granted := mustJoin(t, svc, priya, hand.ID); granted {
		t.Fatal("join of a hand-opened room granted control")
	}

	// An alert room someone already took control of.
	taken := openSoleAlertRoom(t, svc, "orders", "taken")
	if _, err := svc.GrantControl(ctx, arjun, taken, "arjun"); err != nil {
		t.Fatal(err)
	}
	if _, granted := mustJoin(t, svc, priya, taken); granted {
		t.Fatal("join of a room with a driver granted control")
	}

	// A shadow room: the agent runs silently and nobody is made driver.
	shadow := openSoleAlertRoom(t, svc, "shadowed", "shadow")
	if snap, _ := svc.GetRoom(ctx, shadow); !snap.Shadow {
		t.Fatal("fixture: shadowed pipeline did not open a shadow room")
	}
	if _, granted := mustJoin(t, svc, priya, shadow); granted {
		t.Fatal("join of a shadow room granted control")
	}

	// A control.requested is pending: the server does not decide for it.
	requested := openSoleAlertRoom(t, svc, "orders", "requested")
	if _, err := svc.RequestControl(ctx, arjun, requested); err != nil {
		t.Fatal(err)
	}
	if _, granted := mustJoin(t, svc, priya, requested); granted {
		t.Fatal("join with a control.requested pending granted control")
	}

	for _, id := range []string{hand.ID, shadow, requested} {
		for _, e := range grantsIn(t, ms, id) {
			if p, _ := decode[events.ControlGrantedPayload](e); p.Reason != "" {
				t.Fatalf("room %s: grant with reason %q, want none", id, p.Reason)
			}
		}
	}
	if g := grantsIn(t, ms, shadow); len(g) != 0 {
		t.Fatalf("shadow room control.granted = %d, want 0", len(g))
	}
	if g := grantsIn(t, ms, requested); len(g) != 0 {
		t.Fatalf("requested room control.granted = %d, want 0", len(g))
	}
	if n := svc.SoleMemberGrants(); n != 0 {
		t.Fatalf("SoleMemberGrants = %d, want 0", n)
	}
}

// A room loaded from the log knows it came from an alert; a closed one gets
// nobody.
func TestSoleMemberAfterRestartAndClosed(t *testing.T) {
	c := &testClock{t: soleT0}
	ms := memstore.NewWithClock(c.Now)
	ctx := context.Background()
	first := New(soleDeps(ms, c))
	if err := first.Load(ctx); err != nil {
		t.Fatal(err)
	}
	open := openSoleAlertRoom(t, first, "orders", "open")
	closed := openSoleAlertRoom(t, first, "orders", "closed")
	first.Close()
	if _, err := ms.Append(ctx, closed, events.RoomClosed, systemActor, events.RoomClosedPayload{Resolution: "gone"}); err != nil {
		t.Fatal(err)
	}

	svc := newSoleService(t, ms, c)
	kabir := leaseUser(t, svc, "kabir")
	if _, granted := mustJoin(t, svc, kabir, closed); granted {
		t.Fatal("join of a closed room granted control")
	}
	if _, granted := mustJoin(t, svc, kabir, open); !granted {
		t.Fatal("join of a reloaded driverless alert room: not granted")
	}
}

// A steer or comment is not a join: a member's steer in a driverless alert
// room stays a suggestion (TestSteerWithNoDriverQueuesSuggestion pins it),
// and neither grants control.
func TestSoleMemberNotOnSteerOrComment(t *testing.T) {
	c := &testClock{t: soleT0}
	ms := memstore.NewWithClock(c.Now)
	svc := newSoleService(t, ms, c)
	ctx := context.Background()
	priya := leaseUser(t, svc, "priya")
	roomID := openSoleAlertRoom(t, svc, "orders", "steer")
	if e, err := svc.Steer(ctx, priya, roomID, "check the connector"); err != nil || e.Type != events.SteerSuggested {
		t.Fatalf("steer = %s (err %v), want %s", e.Type, err, events.SteerSuggested)
	}
	if _, err := svc.AddComment(ctx, priya, roomID, "", "looking"); err != nil {
		t.Fatal(err)
	}
	if g := grantsIn(t, ms, roomID); len(g) != 0 {
		t.Fatalf("control.granted after a steer and a comment = %d, want 0", len(g))
	}
	// The join still makes them the driver afterwards.
	if _, granted := mustJoin(t, svc, priya, roomID); !granted {
		t.Fatal("join after a steer: not granted")
	}
}

// serialLeaseStore makes the fake's conditional append one step, as the
// Postgres statement is: leaseStore's check and append are two calls.
type serialLeaseStore struct {
	*leaseStore
	amu sync.Mutex
}

func (s *serialLeaseStore) AppendIfLast(ctx context.Context, roomID string, expectSeq int64, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	s.amu.Lock()
	defer s.amu.Unlock()
	return s.leaseStore.AppendIfLast(ctx, roomID, expectSeq, typ, actor, payload)
}

// Two first joiners on two replicas of one shared store: exactly one becomes
// the driver, and the loser's replica sees the winner.
func TestSoleMemberConcurrentReplicasGrantOnce(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: soleT0}
	ls := newLeaseStore()
	const rooms = 8
	const staleRoom = rooms
	// Opened on a single-process service over the same rows, then served by
	// two replicas.
	opener := New(soleDeps(ls.gatedStore.Store, c))
	if err := opener.Load(ctx); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := range rooms + 1 {
		ids = append(ids, openSoleAlertRoom(t, opener, "orders", fmt.Sprintf("race-%d", i)))
	}
	opener.Close()
	stale := ids[staleRoom]
	ids = ids[:rooms]

	shared := &serialLeaseStore{leaseStore: ls}
	replica := func(name string) *Service {
		d := soleDeps(shared, c)
		d.ReplicaID = name
		d.ReplicaTimings = ReplicaTimings{LeaseTTL: time.Minute, LeaseRenew: 100 * time.Millisecond,
			TakeoverScan: time.Hour, ExecSweep: time.Hour}
		s := New(d)
		t.Cleanup(s.Close)
		if err := s.Load(ctx); err != nil {
			t.Fatal(err)
		}
		if s.coord == nil {
			t.Fatal("fixture: shared store not seen as a coordinator")
		}
		return s
	}
	a, b := replica("replica-a"), replica("replica-b")
	priya, arjun := leaseUser(t, a, "priya"), leaseUser(t, b, "arjun")

	for _, id := range ids {
		start := make(chan struct{})
		var wg sync.WaitGroup
		var mu sync.Mutex
		winners := 0
		for _, j := range []struct {
			s *Service
			u User
		}{{a, priya}, {b, arjun}} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, granted, err := j.s.JoinRoom(ctx, j.u, id)
				if err != nil {
					t.Errorf("room %s join %s: %v", id, j.u.ID, err)
					return
				}
				if granted {
					mu.Lock()
					winners++
					mu.Unlock()
				}
			}()
		}
		close(start)
		wg.Wait()
		if winners != 1 {
			t.Fatalf("room %s: %d joiners became the driver, want 1", id, winners)
		}
		g := grantsIn(t, ls, id)
		if len(g) != 1 {
			t.Fatalf("room %s: control.granted events = %d, want 1", id, len(g))
		}
		// Each replica, after a command syncs it, names the same driver.
		want, _ := decode[events.ControlGrantedPayload](g[0])
		for _, s := range []*Service{a, b} {
			if _, granted, err := s.JoinRoom(ctx, leaseUser(t, s, "kabir"), id); err != nil || granted {
				t.Fatalf("room %s: a third joiner on %s: granted=%v err=%v", id, s.replica, granted, err)
			}
			if p := s.published(id); p == nil || p.Driver == nil || p.Driver.ID != want.To.ID {
				t.Fatalf("room %s: %s's driver = %+v, want %s", id, s.replica, p, want.To.ID)
			}
		}
	}
	if n := a.SoleMemberGrants() + b.SoleMemberGrants(); n != rooms {
		t.Fatalf("SoleMemberGrants a+b = %d, want %d (one per room)", n, rooms)
	}

	// The re-check at append time, deterministically: replica a decides on
	// a snapshot that predates b's grant (it holds the room's lock, so
	// nothing folds b's event), and its conditional append must lose.
	st, err := a.lockRoom(ctx, stale)
	if err != nil {
		t.Fatal(err)
	}
	if _, granted, err := b.JoinRoom(ctx, arjun, stale); err != nil || !granted {
		t.Fatalf("b's join: granted=%v err=%v, want granted", granted, err)
	}
	if !soleMemberRoom(&st.snap) {
		t.Fatal("fixture: a's snapshot already saw b's grant")
	}
	_, granted, err := a.claimSoleMemberLocked(ctx, st, stale, priya)
	driver := st.snap.Driver
	a.unlockRoom(st)
	if err != nil || granted {
		t.Fatalf("a's stale claim: granted=%v err=%v, want nothing", granted, err)
	}
	if driver == nil || driver.ID != "arjun" {
		t.Fatalf("a's driver after losing = %+v, want arjun (caught up)", driver)
	}
	if g := grantsIn(t, ls, stale); len(g) != 1 {
		t.Fatalf("stale room control.granted = %d, want 1", len(g))
	}
	if n := a.SoleMemberGrants() + b.SoleMemberGrants(); n != rooms+1 {
		t.Fatalf("SoleMemberGrants a+b = %d, want %d", n, rooms+1)
	}
}

// The fold that decides it: room.created from an alert, control.requested
// while driverless, and the first control.granted.
func TestSoleMemberRoomFold(t *testing.T) {
	ev := func(seq int64, typ events.Type, actor events.Actor, payload any) events.Event {
		b, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return events.Event{RoomID: "r_1", Seq: seq, Type: typ, Actor: actor, Payload: b}
	}
	human := events.Actor{Kind: events.ActorHuman, ID: "priya", Name: "Priya"}
	cases := []struct {
		name string
		evs  []events.Event
		want bool
	}{
		{"alert room", []events.Event{ev(1, events.RoomCreated, systemActor, events.RoomCreatedPayload{AlertSource: "generic"})}, true},
		{"pre-source alert room", []events.Event{ev(1, events.RoomCreated, systemActor, events.RoomCreatedPayload{Alert: json.RawMessage(`{}`)})}, true},
		{"hand room", []events.Event{ev(1, events.RoomCreated, human, events.RoomCreatedPayload{Title: "x"})}, false},
		{"shadow", []events.Event{ev(1, events.RoomCreated, systemActor, events.RoomCreatedPayload{AlertSource: "generic", Shadow: true})}, false},
		{"requested", []events.Event{
			ev(1, events.RoomCreated, systemActor, events.RoomCreatedPayload{AlertSource: "generic"}),
			ev(2, events.ControlRequested, human, events.ControlRequestedPayload{}),
		}, false},
		{"granted", []events.Event{
			ev(1, events.RoomCreated, systemActor, events.RoomCreatedPayload{AlertSource: "generic"}),
			ev(2, events.ControlGranted, human, events.ControlGrantedPayload{To: human, LeaseUntil: soleT0}),
		}, false},
		{"closed", []events.Event{
			ev(1, events.RoomCreated, systemActor, events.RoomCreatedPayload{AlertSource: "generic"}),
			ev(2, events.RoomClosed, human, events.RoomClosedPayload{Resolution: "done"}),
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := NewSnapshot()
			for _, e := range tc.evs {
				snap = Apply(snap, e)
			}
			if got := soleMemberRoom(&snap); got != tc.want {
				t.Fatalf("soleMemberRoom = %v, want %v", got, tc.want)
			}
		})
	}
}
