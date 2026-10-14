//go:build integration

package store_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

// openReplica opens one more store on schema with its own pool: one
// "replica" of the server in this test process.
func openReplica(t *testing.T, schema string, opts store.Options) *store.PG {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	poolCfg, err := pgxpool.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	poolCfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("build pool: %v", err)
	}
	s := store.NewPoolOptions(pool, cfg.RoomDBURL, slog.Default(), opts)
	t.Cleanup(s.Close)
	return s
}

// sql runs one statement on schema from a fresh connection.
func execSQL(t *testing.T, schema, q string, args ...any) {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cc, err := pgx.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cc.RuntimeParams["search_path"] = schema
	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// terminateApp kills every backend with this application_name.
func terminateApp(t *testing.T, app string) int {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var n int
	if err := conn.QueryRow(ctx,
		`SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = $1`, app).Scan(&n); err != nil {
		t.Fatalf("terminate %s: %v", app, err)
	}
	return n
}

type term struct {
	replica    int
	start, end time.Time
}

// Three replicas run the same loop under the elector: exactly one runs it.
// Killing the leader's lock connection hands the loop to another replica, and
// the old loop's context is cancelled before the new loop starts.
func TestElectorOneLeaderTakeover(t *testing.T) {
	s0, _, schema := requireStoreSchema(t)
	stores := []*store.PG{s0, openReplica(t, schema, store.Options{}), openReplica(t, schema, store.Options{})}
	electors := make([]*store.PGElector, len(stores))
	for i, s := range stores {
		// Replica names unique per run: leader connections are found by
		// application_name, and other packages' tests run electors at once.
		e := s.Elector(fmt.Sprintf("%s-r%d", schema, i))
		// The others retry faster than the leader checks its connection, so
		// without the settle delay a new loop would start while the old one
		// still runs; Settle > Check + CheckTimeout is what prevents it.
		e.Retry, e.Check, e.CheckTimeout, e.Settle = 300*time.Millisecond, time.Second, 300*time.Millisecond, 1500*time.Millisecond
		electors[i] = e
	}

	var (
		mu        sync.Mutex
		terms     []*term
		active    atomic.Int32
		maxActive atomic.Int32
	)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i, e := range electors {
		wg.Add(1)
		go func(i int, e *store.PGElector) {
			defer wg.Done()
			e.Run(ctx, "retention", func(lctx context.Context) {
				n := active.Add(1)
				for {
					m := maxActive.Load()
					if n <= m || maxActive.CompareAndSwap(m, n) {
						break
					}
				}
				tm := &term{replica: i, start: time.Now()}
				mu.Lock()
				terms = append(terms, tm)
				mu.Unlock()
				<-lctx.Done()
				mu.Lock()
				tm.end = time.Now()
				mu.Unlock()
				active.Add(-1)
			})
		}(i, e)
	}
	defer func() {
		cancel()
		wg.Wait()
	}()

	waitTerms := func(n int, within time.Duration) []*term {
		t.Helper()
		deadline := time.Now().Add(within)
		for time.Now().Before(deadline) {
			mu.Lock()
			got := len(terms)
			cp := append([]*term(nil), terms...)
			mu.Unlock()
			if got >= n {
				return cp
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("only %d leadership terms after %s, want %d", len(terms), within, n)
		return nil
	}
	first := waitTerms(1, 15*time.Second)[0]
	// Several retry periods: nobody else may start.
	time.Sleep(2 * time.Second)
	mu.Lock()
	if len(terms) != 1 {
		mu.Unlock()
		t.Fatalf("%d leadership terms with a healthy leader, want 1", len(terms))
	}
	mu.Unlock()

	killed := time.Now()
	if n := terminateApp(t, electors[first.replica].AppName("retention")); n != 1 {
		t.Fatalf("terminated %d lock backends, want 1", n)
	}
	all := waitTerms(2, 30*time.Second)
	second := all[1]
	mu.Lock()
	oldEnd := first.end
	mu.Unlock()
	if oldEnd.IsZero() {
		t.Fatal("the old leader's loop context was never cancelled")
	}
	if !oldEnd.Before(second.start) {
		t.Fatalf("old loop cancelled at %s, new loop started at %s: they overlapped", oldEnd.Format(time.StampMilli), second.start.Format(time.StampMilli))
	}
	if second.start.Sub(killed) > 30*time.Second {
		t.Fatalf("takeover took %s, want < 30s", second.start.Sub(killed))
	}
	if m := maxActive.Load(); m != 1 {
		t.Fatalf("%d loops ran at once, want 1", m)
	}
	t.Logf("leader r%d killed; r%d took over after %s", first.replica, second.replica, second.start.Sub(killed).Round(time.Millisecond))
}

func TestRoomLeaseLifecycle(t *testing.T) {
	a, _, schema := requireStoreSchema(t)
	b := openReplica(t, schema, store.Options{})
	roomID := createTestRoom(t, a)
	ctx := context.Background()

	if ok, err := a.AcquireRoomLease(ctx, roomID, "A", "orders", time.Minute); err != nil || !ok {
		t.Fatalf("A acquire: ok=%v err=%v", ok, err)
	}
	if ok, err := a.AcquireRoomLease(ctx, roomID, "A", "orders", time.Minute); err != nil || !ok {
		t.Fatalf("A acquire again (its own): ok=%v err=%v", ok, err)
	}
	if ok, err := b.AcquireRoomLease(ctx, roomID, "B", "orders", time.Minute); err != nil || ok {
		t.Fatalf("B acquire of A's live lease: ok=%v err=%v, want false", ok, err)
	}
	if ok, err := b.TakeOverRoomLease(ctx, roomID, "B", time.Minute); err != nil || ok {
		t.Fatalf("B take over a live lease: ok=%v err=%v, want false", ok, err)
	}
	got, err := a.RenewRoomLeases(ctx, "A", []string{roomID, "r_missing"}, time.Minute)
	if err != nil || len(got) != 1 || got[0] != roomID {
		t.Fatalf("A renew: %v %v", got, err)
	}
	if got, err := b.RenewRoomLeases(ctx, "B", []string{roomID}, time.Minute); err != nil || len(got) != 0 {
		t.Fatalf("B renew of A's lease: %v %v, want none", got, err)
	}
	if _, err := a.AcquireRoomLease(ctx, "r_missing", "A", "orders", time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("acquire on a missing room: %v, want ErrNotFound", err)
	}

	// A stops renewing: the lease runs out, B takes it over, A's renew finds
	// it gone.
	if _, err := a.RenewRoomLeases(ctx, "A", []string{roomID}, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if got, err := a.RenewRoomLeases(ctx, "A", []string{roomID}, time.Minute); err != nil || len(got) != 0 {
		t.Fatalf("renew after expiry: %v %v, want none (an expired lease is lost)", got, err)
	}
	exp, err := b.ExpiredRoomLeases(ctx, 10)
	if err != nil || len(exp) != 1 || exp[0].RoomID != roomID || exp[0].Owner != "A" {
		t.Fatalf("expired leases: %+v %v", exp, err)
	}
	if ok, err := b.TakeOverRoomLease(ctx, roomID, "B", time.Minute); err != nil || !ok {
		t.Fatalf("B take over: ok=%v err=%v", ok, err)
	}
	if ok, err := a.TakeOverRoomLease(ctx, roomID, "A", time.Minute); err != nil || ok {
		t.Fatalf("second take over: ok=%v err=%v, want false", ok, err)
	}
	all, err := a.RoomLeases(ctx)
	if err != nil || all[roomID].Owner != "B" {
		t.Fatalf("leases: %+v %v", all, err)
	}

	// Graceful shutdown of B hands the room over at once.
	if err := b.ExpireRoomLeases(ctx, "B"); err != nil {
		t.Fatal(err)
	}
	if ok, err := a.AcquireRoomLease(ctx, roomID, "A", "orders", time.Minute); err != nil || !ok {
		t.Fatalf("acquire after B expired its leases: ok=%v err=%v", ok, err)
	}
	if err := b.ReleaseRoomLease(ctx, roomID, "B"); err != nil {
		t.Fatal(err)
	}
	if all, _ := a.RoomLeases(ctx); all[roomID].Owner != "A" {
		t.Fatalf("B released A's lease: %+v", all[roomID])
	}
	if err := a.ReleaseRoomLease(ctx, roomID, "A"); err != nil {
		t.Fatal(err)
	}
	if all, _ := a.RoomLeases(ctx); len(all) != 0 {
		t.Fatalf("lease left after release: %+v", all)
	}
}

// Many rooms on three replicas race for two global slots: exactly two win.
func TestAgentSlotGlobalCap(t *testing.T) {
	s0, _, schema := requireStoreSchema(t)
	stores := []*store.PG{s0, openReplica(t, schema, store.Options{}), openReplica(t, schema, store.Options{})}
	ctx := context.Background()
	const rooms, max = 9, 2
	ids := make([]string, rooms)
	for i := range ids {
		ids[i] = createTestRoom(t, s0)
		owner := fmt.Sprintf("R%d", i%3)
		if ok, err := stores[i%3].AcquireRoomLease(ctx, ids[i], owner, "orders", time.Minute); err != nil || !ok {
			t.Fatalf("acquire: %v %v", ok, err)
		}
	}
	var won atomic.Int32
	var wg sync.WaitGroup
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok, err := stores[i%3].TakeAgentSlot(ctx, ids[i], fmt.Sprintf("R%d", i%3), max)
			if err != nil {
				t.Errorf("take slot: %v", err)
			}
			if ok {
				won.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if won.Load() != max {
		t.Fatalf("%d rooms got a global slot, want %d", won.Load(), max)
	}
	leases, _ := s0.RoomLeases(ctx)
	var holder string
	for id, l := range leases {
		if l.Slot {
			holder = id
			break
		}
	}
	owner := leases[holder].Owner
	var spare string
	for id, l := range leases {
		if !l.Slot && l.Owner == owner {
			spare = id
			break
		}
	}
	idx := map[string]int{"R0": 0, "R1": 1, "R2": 2}[owner]
	if ok, _ := stores[idx].TakeAgentSlot(ctx, spare, owner, max); ok {
		t.Fatal("a third slot was granted while two are held")
	}
	if err := stores[idx].ReleaseAgentSlot(ctx, holder, owner); err != nil {
		t.Fatal(err)
	}
	if ok, err := stores[idx].TakeAgentSlot(ctx, spare, owner, max); err != nil || !ok {
		t.Fatalf("slot after a release: ok=%v err=%v", ok, err)
	}
	// A slot on a lease the caller does not hold is refused.
	if _, err := stores[idx].TakeAgentSlot(ctx, spare, "nobody", max); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("slot on someone else's lease: %v, want ErrLeaseLost", err)
	}
	// Expired leases stop counting.
	execSQL(t, schema, `UPDATE room_agent_leases SET expires_at = now() - interval '1 second' WHERE slot`)
	n := 0
	for i := range ids {
		if ok, _ := stores[i%3].TakeAgentSlot(ctx, ids[i], fmt.Sprintf("R%d", i%3), max); ok {
			n++
		}
	}
	if n != max {
		t.Fatalf("after the holders expired, %d slots were granted, want %d", n, max)
	}
}

// Fifty claims across three replicas: exactly one executor gets the proposal.
func TestClaimProposalExactlyOnce(t *testing.T) {
	s0, _, schema := requireStoreSchema(t)
	stores := []*store.PG{s0, openReplica(t, schema, store.Options{}), openReplica(t, schema, store.Options{})}
	roomID := createTestRoom(t, s0)
	createTestProposal(t, s0, roomID, "p_claim")
	ctx := context.Background()
	var won atomic.Int32
	var winner atomic.Value
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			owner := fmt.Sprintf("R%d-%d", i%3, i)
			ok, err := stores[i%3].ClaimProposal(ctx, "p_claim", owner)
			if err != nil {
				t.Errorf("claim: %v", err)
			}
			if ok {
				won.Add(1)
				winner.Store(owner)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if won.Load() != 1 {
		t.Fatalf("%d claims won, want 1", won.Load())
	}
	w := winner.Load().(string)
	if ok, err := s0.TouchProposalClaim(ctx, "p_claim", w); err != nil || !ok {
		t.Fatalf("winner touch: %v %v", ok, err)
	}
	if ok, _ := s0.TouchProposalClaim(ctx, "p_claim", "someone-else"); ok {
		t.Fatal("a loser could refresh the claim")
	}
	// A fresh claim is not stale.
	if sp, err := s0.StaleProposals(ctx, time.Minute, time.Minute, 10); err != nil || len(sp) != 0 {
		t.Fatalf("stale proposals with a fresh claim: %+v %v", sp, err)
	}
	if ok, _ := s0.InterruptProposal(ctx, "p_claim", "sweeper", time.Minute, time.Minute); ok {
		t.Fatal("a fresh claim was interrupted")
	}
	// The claimer stops: its claim ages; exactly one replica interrupts.
	execSQL(t, schema, `UPDATE proposals SET claimed_at = now() - interval '10 minutes' WHERE proposal_id = 'p_claim'`)
	sp, err := s0.StaleProposals(ctx, time.Minute, time.Minute, 10)
	if err != nil || len(sp) != 1 || sp[0].ProposalID != "p_claim" || sp[0].ClaimedBy != w || sp[0].RoomID != roomID {
		t.Fatalf("stale proposals: %+v %v", sp, err)
	}
	var interrupted atomic.Int32
	for i := 0; i < 9; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if ok, err := stores[i%3].InterruptProposal(ctx, "p_claim", fmt.Sprintf("sweeper-%d", i), time.Minute, time.Minute); err != nil {
				t.Errorf("interrupt: %v", err)
			} else if ok {
				interrupted.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if interrupted.Load() != 1 {
		t.Fatalf("%d interruptions, want 1", interrupted.Load())
	}
	row, _ := s0.GetProposal(ctx, "p_claim")
	if row.Status != "failed" {
		t.Fatalf("status %q, want failed", row.Status)
	}
	if ok, _ := s0.TouchProposalClaim(ctx, "p_claim", w); ok {
		t.Fatal("the old claimer could still refresh an interrupted claim")
	}
}

// An approved proposal nobody claimed is stale only after the last approval
// is old enough, and only when every required layer approved.
func TestStaleUnclaimedProposal(t *testing.T) {
	s, _, schema := requireStoreSchema(t)
	roomID := createTestRoom(t, s)
	ctx := context.Background()
	if err := s.CreateProposal(ctx, store.ProposalRow{
		ProposalID: "p_two", RoomID: roomID, Title: "fix", Status: "open",
		RequiredLayers: []string{"postgres", "kafka"}, Steps: []byte(`[{"action":"a","layer":"postgres"}]`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordApproval(ctx, store.ApprovalRow{ProposalID: "p_two", Layer: "postgres", Decision: "approved"}); err != nil {
		t.Fatal(err)
	}
	execSQL(t, schema, `UPDATE approvals SET decided_at = now() - interval '10 minutes'`)
	if sp, _ := s.StaleProposals(ctx, time.Minute, time.Minute, 10); len(sp) != 0 {
		t.Fatalf("half-approved proposal listed as stale: %+v", sp)
	}
	if err := s.RecordApproval(ctx, store.ApprovalRow{ProposalID: "p_two", Layer: "kafka", Decision: "approved"}); err != nil {
		t.Fatal(err)
	}
	if sp, _ := s.StaleProposals(ctx, time.Minute, time.Minute, 10); len(sp) != 0 {
		t.Fatalf("just-approved proposal listed as stale: %+v", sp)
	}
	// The interrupt re-checks the rule itself: a caller with a stale list
	// (or none) cannot interrupt a proposal that is not stale.
	if ok, err := s.InterruptProposal(ctx, "p_two", "sweeper", time.Minute, time.Minute); err != nil || ok {
		t.Fatalf("interrupt of a just-approved proposal: ok=%v err=%v, want false", ok, err)
	}
	execSQL(t, schema, `UPDATE approvals SET decided_at = now() - interval '10 minutes'`)
	sp, err := s.StaleProposals(ctx, time.Minute, time.Minute, 10)
	if err != nil || len(sp) != 1 || sp[0].ClaimedBy != "" {
		t.Fatalf("stale unclaimed: %+v %v", sp, err)
	}
	if ok, err := s.InterruptProposal(ctx, "p_two", "sweeper", time.Minute, time.Minute); err != nil || !ok {
		t.Fatalf("interrupt: %v %v", ok, err)
	}
	if ok, _ := s.ClaimProposal(ctx, "p_two", "late-executor"); ok {
		t.Fatal("an executor claimed an interrupted proposal")
	}
}

// A subscriber on replica B gets events appended on replica A: through NOTIFY,
// and with B's LISTEN off through the poll fallback alone.
func TestSubscribeAcrossReplicas(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts store.Options
	}{
		{"notify", store.Options{}},
		{"poll fallback", store.Options{NoListen: true, PollInterval: 300 * time.Millisecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _, schema := requireStoreSchema(t)
			b := openReplica(t, schema, tc.opts)
			roomID := createTestRoom(t, a)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			appendComment(ctx, t, a, roomID, 0)
			// Two subscribers on B share the room feed; one starts behind it.
			ch1, err := b.Subscribe(ctx, roomID, 0)
			if err != nil {
				t.Fatal(err)
			}
			ch2, err := b.Subscribe(ctx, roomID, 1)
			if err != nil {
				t.Fatal(err)
			}
			const n = 100
			go func() {
				for i := 1; i <= n; i++ {
					if _, err := a.Append(ctx, roomID, events.CommentAdded, commentActor(), events.CommentAddedPayload{CommentID: fmt.Sprintf("c_%d", i), Text: "x"}); err != nil {
						t.Errorf("append: %v", err)
						return
					}
				}
			}()
			for i, tc := range []struct {
				ch    <-chan events.Event
				first int64
			}{{ch1, 1}, {ch2, 2}} {
				want := tc.first
				for want <= n+1 {
					select {
					case e := <-tc.ch:
						if e.Seq != want {
							t.Fatalf("subscriber %d: seq %d, want %d", i, e.Seq, want)
						}
						want++
					case <-ctx.Done():
						t.Fatalf("subscriber %d: timed out at seq %d", i, want)
					}
				}
			}
		})
	}
}

// While LISTEN is up, an append whose notification never arrives (a socket
// that died without an error) still reaches subscribers through the safety
// poll (docs/design/replicas.md section 6.1).
func TestSafetyPollWhileListening(t *testing.T) {
	a, _, schema := requireStoreSchema(t)
	b := openReplica(t, schema, store.Options{PollInterval: 100 * time.Millisecond, SafetyPoll: 500 * time.Millisecond})
	roomID := createTestRoom(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ch, err := b.Subscribe(ctx, roomID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.WaitHubListening(ctx); err != nil {
		t.Fatalf("replica b never started listening: %v", err)
	}
	// An event written without pg_notify, as if its notification was lost.
	// One statement, like Append's one transaction: written as two, a feed
	// whose first read fell between them took last_seq 1 as its starting
	// tail before event 1 existed, and its subscriber then waited for an
	// event the feed would never report (about 6 runs in 100 failed).
	execSQL(t, schema, `WITH r AS (UPDATE rooms SET last_seq = 1 WHERE id = $1 RETURNING id)
		INSERT INTO room_events (room_id, seq, type, actor, payload, at)
		SELECT r.id, 1, 'comment.added', '{"kind":"human","id":"arjun","name":"Arjun"}', '{"comment_id":"c_silent","text":"x"}', now() FROM r`, roomID)
	select {
	case e := <-ch:
		if e.Seq != 1 {
			t.Fatalf("seq %d, want 1", e.Seq)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("an event with a lost notification never arrived while listening (listening now: %v)", b.HubListening())
	}
	if !b.HubListening() {
		t.Fatal("LISTEN dropped during the test; the delivery may have come from the fallback poll")
	}
}

func TestLockKeyExcludes(t *testing.T) {
	a, _, schema := requireStoreSchema(t)
	b := openReplica(t, schema, store.Options{})
	ctx := context.Background()
	unlock, err := a.LockKey(ctx, "alert:orders|k")
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan time.Time, 1)
	go func() {
		u2, err := b.LockKey(ctx, "alert:orders|k")
		if err != nil {
			t.Errorf("second lock: %v", err)
			got <- time.Time{}
			return
		}
		got <- time.Now()
		u2()
	}()
	select {
	case <-got:
		t.Fatal("the second replica got a held key lock")
	case <-time.After(500 * time.Millisecond):
	}
	released := time.Now()
	unlock()
	select {
	case at := <-got:
		if at.Before(released) {
			t.Fatal("lock granted before the release")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second replica never got the lock")
	}
	// A short wait is bounded by its context.
	unlock, err = a.LockKey(ctx, "alert:orders|k")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	sctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := b.LockKey(sctx, "alert:orders|k"); err == nil {
		t.Fatal("lock granted while held")
	}
}
