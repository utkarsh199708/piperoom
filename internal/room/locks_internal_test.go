package room

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// Tests for the room service's locking: a slow database call for one room
// must not hold up reads of, or commands on, any other room; and the cache
// must stay equal to the log under concurrent load.

// gatedStore is a memstore that is slow for one room: while it is armed,
// Append and AppendIfLast for that room wait before they write, and Since
// for it waits after it has read, until the test releases it (or the call's
// context ends). Calls for every other room go straight through. Armed with
// ops, only those calls wait: "append", "since", "appended" (an Append that
// has written), "close" (CloseRoom) and "driver" (UpdateDriver). Room "*"
// is every room.
type gatedStore struct {
	*memstore.Store
	mu      sync.Mutex
	room    string
	ops     map[string]bool
	gate    chan struct{}
	entered chan string
	loads   map[string]int // Since(room, 0, ...) calls: rebuilds of each room
}

func newGatedStore() *gatedStore {
	return &gatedStore{Store: memstore.New(), entered: make(chan string, 64), loads: map[string]int{}}
}

// arm makes roomID slow until release is called: the calls ops names, or
// Append and Since without ops. release is idempotent.
func (g *gatedStore) arm(roomID string, ops ...string) (release func()) {
	if len(ops) == 0 {
		ops = []string{"append", "since"}
	}
	gate := make(chan struct{})
	g.mu.Lock()
	g.room, g.gate, g.ops = roomID, gate, map[string]bool{}
	for _, op := range ops {
		g.ops[op] = true
	}
	g.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			if g.gate == gate {
				g.room, g.gate = "", nil
			}
			g.mu.Unlock()
			close(gate)
		})
	}
}

// hold waits while roomID is armed and reports op as held.
func (g *gatedStore) hold(ctx context.Context, op, roomID string) error {
	g.mu.Lock()
	gate := g.gate
	if (roomID != g.room && g.room != "*") || !g.ops[op] {
		gate = nil
	}
	g.mu.Unlock()
	if gate == nil {
		return nil
	}
	select {
	case g.entered <- op:
	default:
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitHeld waits until a call of op is held.
func (g *gatedStore) waitHeld(t *testing.T, op string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got := <-g.entered:
			if got == op {
				return
			}
		case <-deadline:
			t.Fatalf("no %s call was held", op)
		}
	}
}

func (g *gatedStore) loadsOf(roomID string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.loads[roomID]
}

func (g *gatedStore) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	if err := g.hold(ctx, "append", roomID); err != nil {
		return events.Event{}, err
	}
	e, err := g.Store.Append(ctx, roomID, typ, actor, payload)
	if err == nil {
		_ = g.hold(ctx, "appended", roomID) // written either way
	}
	return e, err
}

func (g *gatedStore) CloseRoom(ctx context.Context, roomID string, closedAt time.Time) error {
	if err := g.hold(ctx, "close", roomID); err != nil {
		return err
	}
	return g.Store.CloseRoom(ctx, roomID, closedAt)
}

func (g *gatedStore) UpdateDriver(ctx context.Context, roomID, driverID, driverName string, leaseUntil time.Time) error {
	if err := g.hold(ctx, "driver", roomID); err != nil {
		return err
	}
	return g.Store.UpdateDriver(ctx, roomID, driverID, driverName, leaseUntil)
}

func (g *gatedStore) Since(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	if afterSeq == 0 {
		g.mu.Lock()
		g.loads[roomID]++
		g.mu.Unlock()
	}
	evs, err := g.Store.Since(ctx, roomID, afterSeq, limit)
	if herr := g.hold(ctx, "since", roomID); herr != nil {
		return nil, herr
	}
	return evs, err
}

// within fails the test unless f returns, without an error, within d. f runs
// on its own goroutine: a call stuck behind a lock does not hang the test, it
// ends when the test releases the store.
func within(t *testing.T, d time.Duration, what string, f func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-time.After(d):
		t.Fatalf("%s did not return within %s", what, d)
	}
}

// tenantOwners turns tenancy on, so CheckRoomVisible has to look the room up:
// arjun and priya see pipeline orders, kabir sees nothing.
func tenantOwners() *owners.Owners {
	return &owners.Owners{
		Tenants: map[string]owners.Tenant{"a": {Members: owners.Principals{
			{Kind: owners.KindUser, Name: "arjun"}, {Kind: owners.KindUser, Name: "priya"},
		}}},
		Pipelines: []owners.Pipeline{{ID: "orders", Tenant: "a", SourceTables: []string{"public.orders"}}},
	}
}

func idleRun(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
	<-ctx.Done()
	return nil
}

func newLockService(t *testing.T, st store.Store) *Service {
	t.Helper()
	svc := New(Deps{Store: st, Users: DefaultUsers(), Owners: tenantOwners(), Run: idleRun})
	t.Cleanup(svc.Close)
	return svc
}

func lockUser(t *testing.T, svc *Service, id string) User {
	t.Helper()
	u, ok := svc.LookupUser(id)
	if !ok {
		t.Fatalf("no user %q", id)
	}
	return u
}

func createRooms(t *testing.T, svc *Service, n int) []string {
	t.Helper()
	arjun := lockUser(t, svc, "arjun")
	ids := make([]string, n)
	for i := range ids {
		snap, err := svc.CreateRoom(context.Background(), arjun, fmt.Sprintf("room %d", i), "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = snap.ID
	}
	return ids
}

// stallAppend starts a comment on roomID whose append the store holds, and
// returns once it is held. The comment finishes after release; wait returns
// its error.
func stallAppend(t *testing.T, svc *Service, st *gatedStore, roomID string) (release func(), wait func() error) {
	t.Helper()
	release = st.arm(roomID)
	t.Cleanup(release)
	done := make(chan error, 1)
	go func() {
		_, err := svc.AddComment(context.Background(), lockUser(t, svc, "arjun"), roomID, "", "held by a slow database")
		done <- err
	}()
	st.waitHeld(t, "append")
	return release, func() error {
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			return errors.New("the held comment did not finish after release")
		}
	}
}

// While one room's append is stuck in the database, reads of other rooms, of
// the room list and of the stuck room itself answer at once: from what each
// room last published, not from behind the stuck command.
func TestSlowAppendReadsDoNotWait(t *testing.T) {
	st := newGatedStore()
	svc := newLockService(t, st)
	ids := createRooms(t, svc, 2)
	a, b := ids[0], ids[1]
	before, err := svc.GetRoom(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	release, wait := stallAppend(t, svc, st, a)
	ctx := context.Background()

	within(t, time.Second, "GetRoom(B)", func() error {
		snap, err := svc.GetRoom(ctx, b)
		if err == nil && snap.ID != b {
			err = fmt.Errorf("got room %s", snap.ID)
		}
		return err
	})
	within(t, time.Second, "ListRooms", func() error {
		list, err := svc.ListRooms(ctx)
		if err == nil && len(list) != 2 {
			err = fmt.Errorf("listed %d rooms, want 2", len(list))
		}
		return err
	})
	within(t, time.Second, "CheckRoomVisible(B)", func() error {
		if err := svc.CheckRoomVisible(ctx, lockUser(t, svc, "priya"), b); err != nil {
			return err
		}
		if err := svc.CheckRoomVisible(ctx, lockUser(t, svc, "kabir"), b); !isNotFound(err) {
			return fmt.Errorf("kabir: err = %v, want not found", err)
		}
		return nil
	})
	within(t, time.Second, "GetRoom(A)", func() error {
		snap, err := svc.GetRoom(ctx, a)
		if err == nil && snap.LastSeq != before.LastSeq {
			err = fmt.Errorf("last seq %d, want %d (the stuck comment is not published yet)", snap.LastSeq, before.LastSeq)
		}
		return err
	})
	within(t, time.Second, "Events(B)", func() error {
		_, last, err := svc.Events(ctx, b, 0, 10)
		if err == nil && last == 0 {
			err = errors.New("last seq 0")
		}
		return err
	})

	release()
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	after, err := svc.GetRoom(ctx, a)
	if err != nil || after.LastSeq != before.LastSeq+1 {
		t.Fatalf("after release: last seq %d (err %v), want %d", after.LastSeq, err, before.LastSeq+1)
	}
}

// While one room's append is stuck in the database, commands on other rooms
// go through, and the room list socket hears about them.
func TestSlowAppendOtherRoomCommandsDoNotWait(t *testing.T) {
	st := newGatedStore()
	svc := newLockService(t, st)
	ids := createRooms(t, svc, 2)
	a, b := ids[0], ids[1]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := svc.SubscribeRooms(ctx)
	release, wait := stallAppend(t, svc, st, a)
	arjun := lockUser(t, svc, "arjun")

	var seq int64
	within(t, time.Second, "AddComment(B)", func() error {
		e, err := svc.AddComment(ctx, arjun, b, "", "room B is fine")
		seq = e.Seq
		return err
	})
	within(t, time.Second, "Steer(B)", func() error {
		e, err := svc.Steer(ctx, arjun, b, "look at the connector")
		if err == nil && e.Type != events.SteerSent {
			err = fmt.Errorf("steer logged as %s, want the driver's steer.sent", e.Type)
		}
		return err
	})
	within(t, time.Second, "room list update for B", func() error {
		for up := range updates {
			if up.Room != nil && up.Room.ID == b && up.Room.LastSeq >= seq {
				return nil
			}
		}
		return errors.New("updates closed")
	})

	release()
	if err := wait(); err != nil {
		t.Fatal(err)
	}
}

// loadedLater opens n rooms through one service and returns a second service
// on the same store that has cached none of them, as after a restart without
// Load.
func loadedLater(t *testing.T, st *gatedStore, n int) (*Service, []string) {
	t.Helper()
	first := New(Deps{Store: st, Users: DefaultUsers(), Owners: tenantOwners(), Run: idleRun})
	ids := createRooms(t, first, n)
	first.Close()
	return newLockService(t, st), ids
}

// While one room's load is stuck in the database, a cached room still reads
// at once.
func TestSlowLoadCachedReadsDoNotWait(t *testing.T) {
	st := newGatedStore()
	svc, ids := loadedLater(t, st, 2)
	a, b := ids[0], ids[1]
	ctx := context.Background()
	if _, err := svc.GetRoom(ctx, b); err != nil { // B is cached from here on
		t.Fatal(err)
	}
	release := st.arm(a)
	t.Cleanup(release)
	loaded := make(chan error, 1)
	go func() {
		_, err := svc.GetRoom(ctx, a)
		loaded <- err
	}()
	st.waitHeld(t, "since")

	within(t, time.Second, "GetRoom(B)", func() error {
		_, err := svc.GetRoom(ctx, b)
		return err
	})
	within(t, time.Second, "CheckRoomVisible(B)", func() error {
		return svc.CheckRoomVisible(ctx, lockUser(t, svc, "arjun"), b)
	})

	release()
	select {
	case err := <-loaded:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GetRoom(A) did not finish after release")
	}
}

// While one room's load is stuck in the database, the room list still
// answers.
func TestSlowLoadListRoomsDoesNotWait(t *testing.T) {
	st := newGatedStore()
	svc, ids := loadedLater(t, st, 2)
	a, b := ids[0], ids[1]
	ctx := context.Background()
	if _, err := svc.GetRoom(ctx, b); err != nil {
		t.Fatal(err)
	}
	release := st.arm(a)
	t.Cleanup(release)
	loaded := make(chan error, 1)
	go func() {
		_, err := svc.GetRoom(ctx, a)
		loaded <- err
	}()
	st.waitHeld(t, "since")

	within(t, time.Second, "ListRooms", func() error {
		list, err := svc.ListRooms(ctx)
		for _, r := range list {
			if r.ID == b {
				return err
			}
		}
		if err == nil {
			err = fmt.Errorf("room B missing from %d rooms", len(list))
		}
		return err
	})

	release()
	if err := <-loaded; err != nil {
		t.Fatal(err)
	}
}

// Two reads of a room that is not cached share one rebuild from the log.
func TestSlowLoadSharedRebuild(t *testing.T) {
	st := newGatedStore()
	svc, ids := loadedLater(t, st, 1)
	a := ids[0]
	ctx := context.Background()
	base := st.loadsOf(a)
	release := st.arm(a)
	t.Cleanup(release)
	type got struct {
		snap RoomSnapshot
		err  error
	}
	res := make(chan got, 2)
	read := func() {
		snap, err := svc.GetRoom(ctx, a)
		res <- got{snap, err}
	}
	go read()
	st.waitHeld(t, "since")
	go read()
	time.Sleep(50 * time.Millisecond) // let the second read find the load in flight
	release()
	var seqs []int64
	for range 2 {
		select {
		case r := <-res:
			if r.err != nil {
				t.Fatal(r.err)
			}
			seqs = append(seqs, r.snap.LastSeq)
		case <-time.After(5 * time.Second):
			t.Fatal("a read did not finish")
		}
	}
	if seqs[0] != seqs[1] || seqs[0] == 0 {
		t.Fatalf("reads saw last seq %v, want the same room", seqs)
	}
	if n := st.loadsOf(a) - base; n != 1 {
		t.Fatalf("%d rebuilds for two reads, want 1", n)
	}
}

// A room that retention purges while it is being loaded does not come back:
// it ends up out of the cache, and OnRoomDeleted runs once.
func TestLoadRacingPurgeDoesNotResurrect(t *testing.T) {
	st := newGatedStore()
	first := New(Deps{Store: st, Users: DefaultUsers(), Owners: tenantOwners(), Run: idleRun})
	a := createRooms(t, first, 1)[0]
	first.Close()
	var deleted atomic.Int32
	svc := New(Deps{Store: st, Users: DefaultUsers(), Owners: tenantOwners(), Run: idleRun,
		OnRoomDeleted: func(roomID string, _ events.Event) {
			if roomID == a {
				deleted.Add(1)
			}
		}})
	t.Cleanup(svc.Close)
	ctx := context.Background()

	release := st.arm(a)
	t.Cleanup(release)
	loaded := make(chan struct{})
	go func() {
		defer close(loaded)
		_, _ = svc.GetRoom(ctx, a) // either answer is fine; the cache is what counts
	}()
	st.waitHeld(t, "since") // the log is read; the load has not finished

	// Retention's purge: the store delete, then the eviction.
	if _, _, err := st.DeleteRoom(ctx, store.RoomDeletion{RoomID: a, DeletedBy: store.RetentionActor, DeletedAt: time.Now().UTC(), Reason: "retention"}, -1); err != nil {
		t.Fatal(err)
	}
	evicted := make(chan struct{})
	go func() {
		defer close(evicted)
		svc.evictRooms([]string{a}, func(string, int64) events.Event { return events.Event{} })
	}()
	time.Sleep(50 * time.Millisecond) // let the eviction reach the load
	release()
	for _, ch := range []chan struct{}{loaded, evicted} {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("load or eviction did not finish")
		}
	}

	if svc.cached(a) != nil {
		t.Fatal("the purged room is cached again")
	}
	list, err := svc.ListRooms(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range list {
		if r.ID == a {
			t.Fatal("the purged room is listed again")
		}
	}
	if _, err := svc.GetRoom(ctx, a); !isNotFound(err) {
		t.Fatalf("GetRoom after the purge: err = %v, want not found", err)
	}
	time.Sleep(50 * time.Millisecond) // a second call would come from a goroutine
	if n := deleted.Load(); n != 1 {
		t.Fatalf("OnRoomDeleted ran %d times, want 1", n)
	}
}

// Fifty copies of one alert at the same moment open one room; the others
// join it.
func TestAlertDedupConcurrent(t *testing.T) {
	svc := New(Deps{Store: memstore.New(), Users: DefaultUsers(), Run: idleRun})
	t.Cleanup(svc.Close)
	a := alertin.Alert{
		Source: alertin.Generic, Status: alertin.StatusFiring, Title: "Lag", Severity: "sev2",
		PipelineID: "orders", DedupKey: "generic:lag", Raw: []byte(`{"title":"Lag"}`),
	}
	const n = 50
	start := make(chan struct{})
	type got struct {
		id      string
		created bool
		err     error
	}
	res := make(chan got, n)
	for range n {
		go func() {
			<-start
			snap, created, err := svc.OpenAlertRoom(context.Background(), a)
			res <- got{snap.ID, created, err}
		}()
	}
	close(start)
	ids := map[string]int{}
	created := 0
	for range n {
		r := <-res
		if r.err != nil {
			t.Fatal(r.err)
		}
		ids[r.id]++
		if r.created {
			created++
		}
	}
	if len(ids) != 1 || created != 1 {
		t.Fatalf("%d alerts opened %d rooms (%d reported created), want 1", n, len(ids), created)
	}
	list, err := svc.ListRooms(context.Background())
	if err != nil || len(list) != 1 {
		t.Fatalf("listed %d rooms (err %v), want 1", len(list), err)
	}
}

// leaseStore is a gatedStore that says it is shared (store.Coordinator), with
// the agent leases in memory: enough for the lease loop, the watchdog and the
// follower. It counts the renewals of each room's lease. Coordinator methods
// the test does not reach are left nil and panic if called.
type leaseStore struct {
	*gatedStore
	store.Coordinator
	lmu     sync.Mutex
	leases  map[string]store.RoomLease
	renewed map[string]int
	watch   store.AppendWatch
	// takenOver counts the leases taken over through TakeOverRoomLease.
	takenOver map[string]int
	// stale are the proposals StaleProposals reports, each interrupted once;
	// rows are proposal rows GetProposal returns ahead of the store's.
	stale       []store.StaleProposal
	interrupted map[string]bool
	rows        map[string]store.ProposalRow
}

func newLeaseStore() *leaseStore {
	return &leaseStore{gatedStore: newGatedStore(), leases: map[string]store.RoomLease{}, renewed: map[string]int{},
		takenOver: map[string]int{}, interrupted: map[string]bool{}, rows: map[string]store.ProposalRow{}}
}

// expireTo hands roomID's lease to another replica that has stopped renewing
// it: expired, so the takeover scan takes it.
func (l *leaseStore) expireTo(roomID, owner string) {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	cur := l.leases[roomID]
	cur.RoomID, cur.Owner, cur.ExpiresAt = roomID, owner, time.Now().Add(-time.Second)
	l.leases[roomID] = cur
}

func (l *leaseStore) takeovers(roomID string) int {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	return l.takenOver[roomID]
}

func (l *leaseStore) TakeOverRoomLease(_ context.Context, roomID, owner string, ttl time.Duration) (bool, error) {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	cur, ok := l.leases[roomID]
	if !ok || cur.ExpiresAt.After(time.Now()) {
		return false, nil
	}
	cur.Owner, cur.ExpiresAt = owner, time.Now().Add(ttl)
	l.leases[roomID] = cur
	l.takenOver[roomID]++
	return true, nil
}

func (l *leaseStore) InterruptProposal(_ context.Context, proposalID, _ string, _, _ time.Duration) (bool, error) {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	if l.interrupted[proposalID] {
		return false, nil
	}
	for _, sp := range l.stale {
		if sp.ProposalID == proposalID {
			l.interrupted[proposalID] = true
			return true, nil
		}
	}
	return false, nil
}

func (l *leaseStore) GetProposal(ctx context.Context, proposalID string) (store.ProposalRow, error) {
	l.lmu.Lock()
	row, ok := l.rows[proposalID]
	l.lmu.Unlock()
	if ok {
		return row, nil
	}
	return l.Store.GetProposal(ctx, proposalID)
}

func (l *leaseStore) AcquireRoomLease(_ context.Context, roomID, owner, pipelineID string, ttl time.Duration) (bool, error) {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	now := time.Now()
	if cur, ok := l.leases[roomID]; ok && cur.Owner != owner && cur.ExpiresAt.After(now) {
		return false, nil
	}
	l.leases[roomID] = store.RoomLease{RoomID: roomID, Owner: owner, PipelineID: pipelineID, ExpiresAt: now.Add(ttl)}
	return true, nil
}

func (l *leaseStore) RenewRoomLeases(_ context.Context, owner string, roomIDs []string, ttl time.Duration) ([]string, error) {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	now := time.Now()
	var out []string
	for _, id := range roomIDs {
		cur, ok := l.leases[id]
		if !ok || cur.Owner != owner || !cur.ExpiresAt.After(now) {
			continue
		}
		cur.ExpiresAt = now.Add(ttl)
		l.leases[id] = cur
		l.renewed[id]++
		out = append(out, id)
	}
	return out, nil
}

func (l *leaseStore) renewals(roomID string) int {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	return l.renewed[roomID]
}

func (l *leaseStore) lease(roomID string) (store.RoomLease, bool) {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	cur, ok := l.leases[roomID]
	return cur, ok
}

func (l *leaseStore) ReleaseRoomLease(_ context.Context, roomID, owner string) error {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	if l.leases[roomID].Owner == owner {
		delete(l.leases, roomID)
	}
	return nil
}

func (l *leaseStore) ExpireRoomLeases(_ context.Context, owner string) error {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	for id, cur := range l.leases {
		if cur.Owner == owner {
			cur.ExpiresAt = time.Now()
			l.leases[id] = cur
		}
	}
	return nil
}

func (l *leaseStore) ExpiredRoomLeases(context.Context, int) ([]store.RoomLease, error) {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	var out []store.RoomLease
	for _, cur := range l.leases {
		if !cur.ExpiresAt.After(time.Now()) {
			out = append(out, cur)
		}
	}
	return out, nil
}

func (l *leaseStore) RoomLeases(context.Context) (map[string]store.RoomLease, error) {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	out := make(map[string]store.RoomLease, len(l.leases))
	for id, cur := range l.leases {
		out[id] = cur
	}
	return out, nil
}

func (l *leaseStore) StaleProposals(context.Context, time.Duration, time.Duration, int) ([]store.StaleProposal, error) {
	l.lmu.Lock()
	defer l.lmu.Unlock()
	var out []store.StaleProposal
	for _, sp := range l.stale {
		if !l.interrupted[sp.ProposalID] {
			out = append(out, sp)
		}
	}
	return out, nil
}

func (l *leaseStore) WatchAppends(_ context.Context, w store.AppendWatch) {
	l.lmu.Lock()
	l.watch = w
	l.lmu.Unlock()
}

// appendElsewhere appends an event as another replica would, and sends the
// notification the follower acts on.
func (l *leaseStore) appendElsewhere(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	e, err := l.Store.Append(ctx, roomID, typ, actor, payload)
	if err != nil {
		return e, err
	}
	l.lmu.Lock()
	w := l.watch
	l.lmu.Unlock()
	if w.OnAppend != nil {
		w.OnAppend(roomID, e.Seq)
	}
	return e, nil
}

func (l *leaseStore) AppendIfLast(ctx context.Context, roomID string, expectSeq int64, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	if err := l.hold(ctx, "append", roomID); err != nil {
		return events.Event{}, err
	}
	row, err := l.Store.GetRoom(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	if row.LastSeq != expectSeq {
		return events.Event{}, fmt.Errorf("lease store: last seq %d, want %d: %w", row.LastSeq, expectSeq, store.ErrSeqConflict)
	}
	return l.Store.Append(ctx, roomID, typ, actor, payload)
}

// inputRun is an agent that records every input it drains and whether its
// run is still going.
type inputRun struct {
	mu     sync.Mutex
	inputs map[string][]agent.HumanInput
	live   map[string]context.Context
}

func (r *inputRun) run(ctx context.Context, a agent.Alert, d agent.Deps, _ agent.Options) error {
	r.mu.Lock()
	r.live[a.RoomID] = ctx
	r.mu.Unlock()
	for {
		if err := d.Inbox.Wait(ctx); err != nil {
			return nil
		}
		items, err := d.Inbox.Drain(ctx)
		if err != nil {
			return nil
		}
		r.mu.Lock()
		r.inputs[a.RoomID] = append(r.inputs[a.RoomID], items...)
		r.mu.Unlock()
	}
}

func (r *inputRun) running(roomID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	ctx, ok := r.live[roomID]
	return ok && ctx.Err() == nil
}

func (r *inputRun) got(roomID, text string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, in := range r.inputs[roomID] {
		if in.Text == text {
			return true
		}
	}
	return false
}

// With a shared store, a room whose command is stuck in the database does not
// hold up the other rooms' leases: they are renewed, their agents keep
// running, and the follower keeps feeding them what other replicas append.
func TestSharedStoreStalledRoomKeepsOthersLeased(t *testing.T) {
	ls := newLeaseStore()
	runs := &inputRun{inputs: map[string][]agent.HumanInput{}, live: map[string]context.Context{}}
	svc := New(Deps{
		Store: ls, Users: DefaultUsers(), Owners: tenantOwners(), Run: runs.run, ReplicaID: "replica-a",
		ReplicaTimings: ReplicaTimings{LeaseTTL: 600 * time.Millisecond, LeaseRenew: 100 * time.Millisecond, TakeoverScan: time.Hour, ExecSweep: time.Hour},
	})
	t.Cleanup(svc.Close)
	ctx := context.Background()
	if err := svc.Load(ctx); err != nil {
		t.Fatal(err)
	}
	ids := createRooms(t, svc, 2)
	a, b := ids[0], ids[1]
	pollUntil(t, func() bool { return runs.running(a) && runs.running(b) })

	release, wait := stallAppend(t, svc, ls.gatedStore, a)
	// Wake the follower for A while it is stuck, as another replica's append
	// to A would: the follower must not wait for A, or B's steer below would
	// never reach B's agent.
	pa, err := svc.GetRoom(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	ls.lmu.Lock()
	w := ls.watch
	ls.lmu.Unlock()
	w.OnAppend(a, pa.LastSeq+1)
	renewedBefore := ls.renewals(b)
	time.Sleep(1300 * time.Millisecond) // more than two lease TTLs

	if n := ls.renewals(b) - renewedBefore; n < 5 {
		t.Fatalf("B's lease was renewed %d times while A was stuck, want it kept renewed", n)
	}
	if l, ok := ls.lease(b); !ok || l.Owner != "replica-a" || !l.ExpiresAt.After(time.Now()) {
		t.Fatalf("B's lease = %+v (held %v), want held by replica-a and live", l, ok)
	}
	if !runs.running(b) {
		t.Fatal("B's agent was stopped while A was stuck")
	}
	priya := lockUser(t, svc, "priya")
	if _, err := ls.appendElsewhere(ctx, b, events.SteerSent, svc.humanActor(priya), events.SteerSentPayload{Text: "from replica b"}); err != nil {
		t.Fatal(err)
	}
	within(t, time.Second, "the follower feeding B", func() error {
		for !runs.got(b, "from replica b") {
			time.Sleep(5 * time.Millisecond)
		}
		return nil
	})

	release()
	if err := wait(); err != nil {
		t.Fatal(err)
	}
}

// chatRun is an agent that answers every input with a message, so agent
// appends race the commands of the stress test.
func chatRun(ctx context.Context, _ agent.Alert, d agent.Deps, _ agent.Options) error {
	for {
		if err := d.Inbox.Wait(ctx); err != nil {
			return nil
		}
		items, err := d.Inbox.Drain(ctx)
		if err != nil {
			return nil
		}
		for range items {
			if err := d.Sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: "noted"}); err != nil {
				return nil
			}
		}
	}
}

// Many people working many rooms at once, with closes, deletes, list reads
// and a list subscriber: nothing panics or deadlocks (run it with -race), and
// once it is quiet every cached room equals its log.
func TestRoomLocksStress(t *testing.T) {
	users, err := ParseUsers([]byte("users:\n" +
		"  - id: arjun\n    name: Arjun\n    layers: [kafka]\n" +
		"  - id: priya\n    name: Priya\n    layers: [postgres]\n" +
		"  - id: ada\n    name: Ada\n    admin: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	ms := memstore.New()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := New(Deps{Store: ms, Users: users, Run: chatRun, MaxAgents: 2, Logger: quiet})
	t.Cleanup(svc.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	people := []User{lockUser(t, svc, "arjun"), lockUser(t, svc, "priya")}
	ada := lockUser(t, svc, "ada")

	var mu sync.Mutex
	var rooms []string
	open := func(u User) {
		snap, err := svc.CreateRoom(ctx, u, "stress", "sev3", "orders")
		if err == nil {
			mu.Lock()
			rooms = append(rooms, snap.ID)
			mu.Unlock()
		}
	}
	pick := func(r *rand.Rand) string {
		mu.Lock()
		defer mu.Unlock()
		return rooms[r.IntN(len(rooms))]
	}
	for range 6 {
		open(people[0])
	}

	go func() { // a list socket that keeps reading
		for range svc.SubscribeRooms(ctx) {
		}
	}()

	const workers, ops = 8, 400
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 7))
			for range ops {
				u := people[r.IntN(len(people))]
				id := pick(r)
				switch n := r.IntN(100); {
				case n < 25:
					_, _ = svc.AddComment(ctx, u, id, "", "a comment")
				case n < 35:
					_, _ = svc.AddComment(ctx, u, id, "", "@agent what next?")
				case n < 50:
					_, _ = svc.Steer(ctx, u, id, "check the lag")
				case n < 58:
					other := people[r.IntN(len(people))]
					_, _ = svc.GrantControl(ctx, u, id, other.ID)
				case n < 63:
					_, _ = svc.RequestControl(ctx, u, id)
				case n < 75:
					_, _ = svc.GetRoom(ctx, id)
				case n < 83:
					_, _ = svc.ListRooms(ctx)
				case n < 90:
					_, _, _ = svc.Events(ctx, id, 0, 50)
				case n < 93:
					_, _ = svc.CloseRoom(ctx, u, id, "done")
				case n < 94:
					_, _ = svc.DeleteRoom(ctx, ada, id, "stress", false)
				default:
					open(u)
				}
			}
		}()
	}
	wg.Wait()

	// Agents may still be answering; every room must settle on its log.
	deadline := time.Now().Add(10 * time.Second)
	for {
		bad := cacheVsLog(ctx, svc, ms)
		if bad == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(bad)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// cacheVsLog compares every cached room, as published, with a fold of its
// log and returns the first difference ("" when there is none).
func cacheVsLog(ctx context.Context, svc *Service, ms *memstore.Store) string {
	for id, st := range svc.cachedRooms() {
		pub := st.pub.Load()
		if pub == nil {
			return fmt.Sprintf("room %s: cached but never published", id)
		}
		got := *pub
		row, err := ms.GetRoom(ctx, id)
		if err != nil {
			return fmt.Sprintf("room %s: cached but not in the store: %v", id, err)
		}
		want, err := svc.rebuild(ctx, id)
		if err != nil {
			return fmt.Sprintf("room %s: rebuild: %v", id, err)
		}
		switch {
		case got.LastSeq != row.LastSeq:
			return fmt.Sprintf("room %s: cached last seq %d, store %d", id, got.LastSeq, row.LastSeq)
		case got.Status != want.snap.Status:
			return fmt.Sprintf("room %s: cached status %q, log %q", id, got.Status, want.snap.Status)
		case driverID(got) != driverID(want.snap):
			return fmt.Sprintf("room %s: cached driver %q, log %q", id, driverID(got), driverID(want.snap))
		}
	}
	return ""
}

func driverID(s RoomSnapshot) string {
	if s.Driver == nil {
		return ""
	}
	return s.Driver.ID
}

// A close holds the room's lock from room.closed until it has stopped the
// executor: isRoomOpen waits for it and then says closed, so no step starts
// after room.closed is in the log.
func TestIsRoomOpenWaitsForClose(t *testing.T) {
	st := newGatedStore()
	svc := newLockService(t, st)
	a := createRooms(t, svc, 1)[0]
	release := st.arm(a, "close")
	t.Cleanup(release)
	closed := make(chan error, 1)
	go func() {
		_, err := svc.CloseRoom(context.Background(), lockUser(t, svc, "arjun"), a, "fixed")
		closed <- err
	}()
	st.waitHeld(t, "close") // room.closed is logged; the close is not done

	open := make(chan bool, 1)
	go func() { open <- svc.isRoomOpen(context.Background(), a) }()
	select {
	case got := <-open:
		t.Fatalf("isRoomOpen = %v while the close was in progress, want it to wait", got)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case got := <-open:
		if got {
			t.Fatal("isRoomOpen = true after room.closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("isRoomOpen did not return after the close")
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

// A room being opened for a pipeline is found by OpenRoomForPipeline, which
// waits for it, so the watcher does not open a second room beside it.
func TestOpenRoomForPipelineWaitsForCreate(t *testing.T) {
	st := newGatedStore()
	svc := newLockService(t, st)
	release := st.arm("*", "append")
	t.Cleanup(release)
	created := make(chan string, 1)
	go func() {
		snap, err := svc.CreateRoom(context.Background(), lockUser(t, svc, "arjun"), "slow", "sev2", "orders")
		if err != nil {
			t.Error(err)
		}
		created <- snap.ID
	}()
	st.waitHeld(t, "append") // cached, room.created not written yet

	type found struct {
		id string
		ok bool
	}
	res := make(chan found, 1)
	go func() {
		id, ok := svc.OpenRoomForPipeline("orders")
		res <- found{id, ok}
	}()
	select {
	case f := <-res:
		t.Fatalf("OpenRoomForPipeline = %+v while the room was being opened, want it to wait", f)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	id := <-created
	select {
	case f := <-res:
		if !f.ok || f.id != id {
			t.Fatalf("OpenRoomForPipeline = %+v, want the room being opened (%s)", f, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OpenRoomForPipeline did not return after the create")
	}
}

// An event already in the store is never above Events' last_seq, even while
// the command that appended it has not published it yet.
func TestEventsLastSeqCoversEventsInFlight(t *testing.T) {
	st := newGatedStore()
	svc := newLockService(t, st)
	a := createRooms(t, svc, 1)[0]
	ctx := context.Background()
	before, err := svc.GetRoom(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	base := before.LastSeq
	release := st.arm(a, "appended")
	t.Cleanup(release)
	done := make(chan error, 1)
	go func() {
		_, err := svc.AddComment(ctx, lockUser(t, svc, "arjun"), a, "", "written, not published")
		done <- err
	}()
	st.waitHeld(t, "appended")

	within(t, time.Second, "Events with the new event", func() error {
		evs, last, err := svc.Events(ctx, a, 0, 100)
		switch {
		case err != nil:
			return err
		case len(evs) == 0 || evs[len(evs)-1].Seq != base+1:
			return fmt.Errorf("%d events, want the new one (seq %d) last", len(evs), base+1)
		case last < base+1:
			return fmt.Errorf("last_seq %d below the event %d it returned", last, base+1)
		}
		return nil
	})
	within(t, time.Second, "Events a page short of the new event", func() error {
		evs, last, err := svc.Events(ctx, a, 0, int(base))
		switch {
		case err != nil:
			return err
		case int64(len(evs)) != base:
			return fmt.Errorf("%d events, want %d", len(evs), base)
		case last < base+1:
			return fmt.Errorf("last_seq %d, want at least %d: a client paging to it would stop early", last, base+1)
		}
		return nil
	})
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// With a shared store, a room whose command is stuck in the database after
// its append landed (here: the driver lease write of a steer) does not hold
// up the room list or another room's hand-off check: neither may wait for the
// stuck room's lock to fold it.
func TestSharedStoreSlowCommandReadsDoNotWait(t *testing.T) {
	ls := newLeaseStore()
	svc := New(Deps{
		Store: ls, Users: DefaultUsers(), Owners: tenantOwners(), Run: idleRun, ReplicaID: "replica-a",
		ReplicaTimings: ReplicaTimings{TakeoverScan: time.Hour, ExecSweep: time.Hour},
	})
	t.Cleanup(svc.Close)
	ctx := context.Background()
	if err := svc.Load(ctx); err != nil {
		t.Fatal(err)
	}
	ids := createRooms(t, svc, 2)
	a, b := ids[0], ids[1]
	release := ls.arm(a, "driver")
	t.Cleanup(release)
	done := make(chan error, 1)
	go func() {
		_, err := svc.Steer(ctx, lockUser(t, svc, "arjun"), a, "check the lag")
		done <- err
	}()
	ls.waitHeld(t, "driver") // steer.sent is in the store; A's lock is held

	within(t, time.Second, "ListRooms", func() error {
		list, err := svc.ListRooms(ctx)
		if err == nil && len(list) != 2 {
			err = fmt.Errorf("listed %d rooms, want 2", len(list))
		}
		return err
	})
	within(t, time.Second, "HandoffInFlight(B)", func() error {
		return svc.HandoffInFlight(ctx, b)
	})

	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the held steer did not finish")
	}
}

// newStalledLeaseRig opens rooms C and B on a replica of a shared store with
// short lease timings, takeover scans and sweeps every 200 ms, and both agents
// running.
func newStalledLeaseRig(t *testing.T) (svc *Service, ls *leaseStore, runs *inputRun, c, b string) {
	t.Helper()
	ls = newLeaseStore()
	runs = &inputRun{inputs: map[string][]agent.HumanInput{}, live: map[string]context.Context{}}
	svc = New(Deps{
		Store: ls, Users: DefaultUsers(), Owners: tenantOwners(), Run: runs.run, ReplicaID: "replica-a",
		ReplicaTimings: ReplicaTimings{LeaseTTL: 600 * time.Millisecond, LeaseRenew: 100 * time.Millisecond,
			TakeoverScan: 200 * time.Millisecond, ExecSweep: 200 * time.Millisecond},
	})
	t.Cleanup(svc.Close)
	if err := svc.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	ids := createRooms(t, svc, 2)
	c, b = ids[0], ids[1]
	pollUntil(t, func() bool { return runs.running(c) && runs.running(b) })
	return svc, ls, runs, c, b
}

// keepsLeased checks, over two lease TTLs, that room b's lease stays renewed
// and its agent running.
func keepsLeased(t *testing.T, ls *leaseStore, runs *inputRun, b string) {
	t.Helper()
	before := ls.renewals(b)
	time.Sleep(1300 * time.Millisecond) // more than two lease TTLs
	if n := ls.renewals(b) - before; n < 5 {
		t.Fatalf("B's lease was renewed %d times while C was stuck, want it kept renewed", n)
	}
	if l, ok := ls.lease(b); !ok || l.Owner != "replica-a" || !l.ExpiresAt.After(time.Now()) {
		t.Fatalf("B's lease = %+v (held %v), want held by replica-a and live", l, ok)
	}
	if !runs.running(b) {
		t.Fatal("B's agent was stopped while C was stuck")
	}
}

// A room stuck in a slow call whose lease this replica then takes over (it had
// expired while the room was stuck) is adopted on its own goroutine: the
// takeover waiting for the room's lock does not stop the other rooms' leases
// from being renewed.
func TestSharedStoreStalledRoomTakenOverKeepsOthersLeased(t *testing.T) {
	svc, ls, runs, c, b := newStalledLeaseRig(t)
	release, wait := stallAppend(t, svc, ls.gatedStore, c)
	ls.expireTo(c, "replica-x")
	pollUntil(t, func() bool { return ls.takeovers(c) > 0 })
	keepsLeased(t, ls, runs, b)
	release()
	if err := wait(); err != nil {
		t.Fatal(err)
	}
}

// A stale proposal whose room is stuck in a slow call is recorded on its own
// goroutine: the sweep waiting for that room does not stop the other rooms'
// leases from being renewed, and the outcome is still written once the room
// is free (the interruption is committed; skipping it would lose it).
func TestSharedStoreStaleProposalOfStalledRoomKeepsOthersLeased(t *testing.T) {
	svc, ls, runs, c, b := newStalledLeaseRig(t)
	release, wait := stallAppend(t, svc, ls.gatedStore, c)
	ls.lmu.Lock()
	ls.rows["p_stale"] = store.ProposalRow{ProposalID: "p_stale", RoomID: c, Status: "open", Steps: []byte("[]")}
	ls.stale = []store.StaleProposal{{ProposalID: "p_stale", RoomID: c, ClaimedBy: "replica-x"}}
	ls.lmu.Unlock()
	pollUntil(t, func() bool {
		ls.lmu.Lock()
		defer ls.lmu.Unlock()
		return ls.interrupted["p_stale"]
	})
	keepsLeased(t, ls, runs, b)
	release()
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	waitFor := func() bool {
		evs, err := ls.Store.Since(context.Background(), c, 0, 1000)
		if err != nil {
			return false
		}
		for _, e := range evs {
			if p, ok := decode[events.ActionExecutedPayload](e); ok && e.Type == events.ActionExecuted && p.ProposalID == "p_stale" && p.OutcomeUnknown {
				return true
			}
		}
		return false
	}
	pollUntil(t, waitFor)
}

// A re-list that finds a room busy leaves it for the follower, which brings it
// up to its row once it is free: a driver lease another replica renewed (on
// the row only) reaches the cache too.
func TestSharedStoreBusyRoomAdoptsRowLeaseLater(t *testing.T) {
	svc, ls, _, c, _ := newStalledLeaseRig(t)
	ctx := context.Background()
	release, wait := stallAppend(t, svc, ls.gatedStore, c)
	later := time.Now().Add(2 * DriverLease).UTC().Truncate(time.Second)
	if err := ls.Store.UpdateDriver(ctx, c, "arjun", "Arjun", later); err != nil {
		t.Fatal(err)
	}
	ls.lmu.Lock()
	w := ls.watch
	ls.lmu.Unlock()
	w.OnResync()
	time.Sleep(100 * time.Millisecond) // the re-list finds C busy
	release()
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	pollUntil(t, func() bool {
		snap, err := svc.GetRoom(ctx, c)
		return err == nil && snap.Driver != nil && snap.Driver.LeaseUntil.Equal(later)
	})
}
