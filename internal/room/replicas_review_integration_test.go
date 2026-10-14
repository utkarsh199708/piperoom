//go:build integration

// Regression tests for the PR 121 review (lr121): each one reproduced a bug
// before its fix. Plus tests that kill the review's surviving mutants.
package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

type lrClock struct{ off atomic.Int64 }

func (c *lrClock) now() time.Time      { return time.Now().Add(time.Duration(c.off.Load())) }
func (c *lrClock) add(d time.Duration) { c.off.Add(int64(d)) }

func lrEvents(t *testing.T, s *Service, room string) []events.Event {
	t.Helper()
	evs, err := s.store.Since(context.Background(), room, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return evs
}

// Review blocker 1: two people take control at the same instant on two
// replicas with no live driver lease. One driver, and the rooms row agrees
// with the log.
func TestReplicasTwoDriversTakeControl(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	clk := &lrClock{}
	withClock := func(d *Deps) { d.Now = clk.now }
	a := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r0", log, nil, withClock)
	b := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r1", log, nil, withClock)
	ctx := context.Background()
	const n = 40
	var rooms []string
	for i := 0; i < n; i++ {
		snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), fmt.Sprintf("r%d", i), "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		rooms = append(rooms, snap.ID)
	}
	clk.add(31 * time.Minute) // arjun's lease lapses everywhere
	both, rowMismatch, logDrivers := 0, 0, 0
	for _, room := range rooms {
		start := make(chan struct{})
		var wg sync.WaitGroup
		var errA, errB error
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _, errA = a.GrantControl(ctx, user(t, a, "priya"), room, "priya") }()
		go func() { defer wg.Done(); <-start; _, errB = b.GrantControl(ctx, user(t, b, "meera"), room, "meera") }()
		close(start)
		wg.Wait()
		if errA == nil && errB == nil {
			both++
		}
		grants := 0
		var last string
		for _, e := range lrEvents(t, a, room) {
			if e.Type == events.ControlGranted && e.Seq > 2 {
				p, _ := decode[events.ControlGrantedPayload](e)
				grants++
				last = p.To.ID
			}
		}
		if grants != 1 {
			logDrivers++
		}
		row, err := a.store.GetRoom(ctx, room)
		if err != nil {
			t.Fatal(err)
		}
		if row.DriverID != last {
			rowMismatch++
		}
	}
	if both > 0 || logDrivers > 0 || rowMismatch > 0 {
		t.Fatalf("%d/%d rooms granted control twice (%d with two grants in the log, %d rows disagree with the log)", both, n, logDrivers, rowMismatch)
	}
}

// Review blocker 1: the old driver (lease lapsed) steers on A while someone
// takes control on B. The steer never lands as steer.sent after the grant.
func TestReplicasStaleDriverSteer(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	clk := &lrClock{}
	withClock := func(d *Deps) { d.Now = clk.now }
	a := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r0", log, nil, withClock)
	b := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r1", log, nil, withClock)
	ctx := context.Background()
	const n = 40
	var rooms []string
	for i := 0; i < n; i++ {
		snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), fmt.Sprintf("r%d", i), "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		rooms = append(rooms, snap.ID)
	}
	clk.add(31 * time.Minute)
	bad := 0
	for _, room := range rooms {
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, _ = a.Steer(ctx, user(t, a, "arjun"), room, "restart the connector")
		}()
		go func() { defer wg.Done(); <-start; _, _ = b.GrantControl(ctx, user(t, b, "priya"), room, "priya") }()
		close(start)
		wg.Wait()
		var grantSeq, steerSeq int64
		for _, e := range lrEvents(t, a, room) {
			if e.Type == events.ControlGranted {
				p, _ := decode[events.ControlGrantedPayload](e)
				if p.To.ID == "priya" && grantSeq == 0 {
					grantSeq = e.Seq
				}
			}
			if e.Type == events.SteerSent && e.Actor.ID == "arjun" {
				steerSeq = e.Seq
			}
		}
		if grantSeq > 0 && steerSeq > grantSeq {
			bad++
		}
	}
	if bad > 0 {
		t.Fatalf("%d/%d rooms logged a driver steer from a user who was no longer the driver", bad, n)
	}
}

// stallStore stalls the executor's claimed step start until released, as a
// replica frozen between the claim refresh and the step start.
type stallStore struct {
	*store.PG
	stalled chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *stallStore) StartClaimedStep(ctx context.Context, proposalID string, i int, owner string) error {
	s.once.Do(func() { close(s.stalled) })
	<-s.release
	return s.PG.StartClaimedStep(ctx, proposalID, i, owner)
}

// Review blocker 2: a step whose executor stalled is declared unknown by the
// sweep; when the stall ends, the step must not run and nothing more is
// written.
func TestReplicasStepNeverRunsAfterDeclaredUnknown(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	act := &itAction{}
	ss := &stallStore{PG: pgReplicaStore(t, schema, store.Options{}), stalled: make(chan struct{}), release: make(chan struct{})}
	a := newReplica(t, ss, "r0", log, act)
	b := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r1", log, act)
	ctx := context.Background()
	snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	room := snap.ID
	pid, err := a.CreateProposal(ctx, room, events.ProposalCreatedPayload{
		Title: "restart", Risk: "low", Rollback: "restart again",
		Steps:          []events.ProposalStep{{Action: "it_fix", Layer: "kafka", Params: json.RawMessage(`{}`)}},
		RequiredLayers: []string{"kafka"},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	row, _ := a.store.GetProposal(ctx, pid)
	if _, err := a.ApproveProposal(ctx, user(t, a, "arjun"), pid, "kafka", row.ParamsHash); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ss.stalled:
	case <-time.After(10 * time.Second):
		t.Fatal("the executor never reached the claimed step start")
	}
	executed := func() []events.ActionExecutedPayload {
		var out []events.ActionExecutedPayload
		for _, e := range lrEvents(t, b, room) {
			if e.Type == events.ActionExecuted {
				p, _ := decode[events.ActionExecutedPayload](e)
				out = append(out, p)
			}
		}
		return out
	}
	eventually(t, 10*time.Second, "sweep records the outcome", func() bool { return len(executed()) == 1 })
	close(ss.release)
	time.Sleep(2 * time.Second)
	evs := executed()
	if act.calls.Load() != 0 {
		t.Fatalf("the step ran %d time(s) after its outcome was declared unknown", act.calls.Load())
	}
	if len(evs) != 1 || evs[0].OK || !evs[0].OutcomeUnknown {
		t.Fatalf("action.executed events %+v, want exactly one with ok=false outcome_unknown=true", evs)
	}
	if p, _ := a.store.GetProposal(ctx, pid); p.Status != "failed" {
		t.Fatalf("status %q, want failed", p.Status)
	}
}

// Review blocker 3: approved proposals wait in the room's executor queue
// behind a running one, longer than both stale rules; their live replica must
// not have them declared dead. Each step (1.5 s) is shorter than the claim
// stale time (2.5 s), the last proposal waits 3 s: only the claim at enqueue
// plus the refresh of queued claims keeps it alive.
func TestReplicasQueuedProposalNotDeclaredDead(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	act := &itAction{delay: 1500 * time.Millisecond}
	a := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r0", log, act, func(d *Deps) {
		tm := fastTimings
		tm.ClaimStale = 2500 * time.Millisecond
		d.ReplicaTimings = tm
	})
	ctx := context.Background()
	snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	room := snap.ID
	var pids []string
	for n := 1; n <= 3; n++ {
		pid, err := a.CreateProposal(ctx, room, events.ProposalCreatedPayload{
			Title: fmt.Sprintf("p%d", n), Risk: "low", Rollback: "restart the task again",
			Steps:          []events.ProposalStep{{Action: "it_fix", Layer: "kafka", Params: json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))}},
			RequiredLayers: []string{"kafka"},
		}, events.AgentActor)
		if err != nil {
			t.Fatal(err)
		}
		pids = append(pids, pid)
	}
	for _, pid := range pids {
		row, _ := a.store.GetProposal(ctx, pid)
		if _, err := a.ApproveProposal(ctx, user(t, a, "arjun"), pid, "kafka", row.ParamsHash); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 20*time.Second, "all proposals executed", func() bool {
		for _, pid := range pids {
			if r, _ := a.store.GetProposal(ctx, pid); r.Status != "executed" {
				return false
			}
		}
		return true
	})
	if n := act.calls.Load(); n != 3 {
		t.Fatalf("action calls %d, want 3", n)
	}
	for _, e := range lrEvents(t, a, room) {
		if e.Type == events.ActionExecuted {
			if p, _ := decode[events.ActionExecutedPayload](e); !p.OK {
				t.Fatalf("a live replica's proposal was failed: %+v", p)
			}
		}
	}
}

// slotErrStore fails the first global slot take, as a transient DB error.
type slotErrStore struct {
	*store.PG
	failed atomic.Bool
}

func (s *slotErrStore) TakeAgentSlot(ctx context.Context, roomID, owner string, max int) (bool, error) {
	if s.failed.CompareAndSwap(false, true) {
		return false, errors.New("injected: connection reset")
	}
	return s.PG.TakeAgentSlot(ctx, roomID, owner, max)
}

// Review blocker 5: one transient error while taking a global slot must not
// leave the room without an agent.
func TestReplicasGlobalSlotErrorRetried(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	ss := &slotErrStore{PG: pgReplicaStore(t, schema, store.Options{})}
	a := newReplica(t, ss, "r0", log, nil, func(d *Deps) { d.MaxAgentsGlobal = 3 })
	ctx := context.Background()
	snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "the agent runs after the slot error", func() bool { return len(log.runsOf(snap.ID)) == 1 })
}

// touchLies is a store whose claim refresh reports the claim gone while the
// database still holds it, to pin the executor's own check of the refresh
// (review mutant M3: the claimed step start would let the step run).
type touchLies struct {
	*store.PG
	lie atomic.Bool
}

func (s *touchLies) TouchProposalClaim(ctx context.Context, proposalID, owner string) (bool, error) {
	if s.lie.Load() {
		return false, nil
	}
	return s.PG.TouchProposalClaim(ctx, proposalID, owner)
}

func TestReplicasExecutorObeysClaimRefresh(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	act := &itAction{}
	tl := &touchLies{PG: pgReplicaStore(t, schema, store.Options{})}
	a := newReplica(t, tl, "r0", log, act)
	ctx := context.Background()
	snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	pid, err := a.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
		Title: "restart", Risk: "low", Rollback: "restart again",
		Steps:          []events.ProposalStep{{Action: "it_fix", Layer: "kafka", Params: json.RawMessage(`{}`)}},
		RequiredLayers: []string{"kafka"},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	row, _ := a.store.GetProposal(ctx, pid)
	tl.lie.Store(true)
	if err := a.executeProposal(row, false); err != nil {
		t.Fatal(err)
	}
	if n := act.calls.Load(); n != 0 {
		t.Fatalf("the step ran %d time(s) although the claim refresh said the claim was gone", n)
	}
}

// Review mutant M6: a renew that reports the lease gone stops the agent at
// once, long before the watchdog's deadline.
func TestReplicasRenewReportingLossStopsAgent(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	slow := func(d *Deps) {
		tm := fastTimings
		tm.LeaseTTL = 60 * time.Second // the watchdog alone would wait ~50 s
		tm.LeaseRenew = 300 * time.Millisecond
		tm.TakeoverScan = time.Hour
		d.ReplicaTimings = tm
	}
	a := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r0", log, nil, slow)
	ctx := context.Background()
	snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "agent on r0", func() bool { return len(log.runsOf(snap.ID)) == 1 })
	execOn(t, schema, `UPDATE room_agent_leases SET owner = 'someone-else' WHERE room_id = $1`, snap.ID)
	eventually(t, 3*time.Second, "r0 stops its agent after a renew reports the lease gone", func() bool {
		return !log.runsOf(snap.ID)[0].end.IsZero()
	})
}

// Review mutant M18: with a shared store, Load writes the restart notice only
// into a room that shows a working agent but holds no lease; an idle room
// without a lease and a room whose lease another replica holds get nothing.
func TestReplicasLoadRestartNotice(t *testing.T) {
	schema := pgSchema(t)
	st := pgReplicaStore(t, schema, store.Options{})
	ctx := context.Background()
	mk := func(id, state string) {
		if err := st.CreateRoom(ctx, store.Room{ID: id, Title: id, Severity: "sev3", PipelineID: "orders", Status: "open", OpenedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Append(ctx, id, events.RoomCreated, systemActor, events.RoomCreatedPayload{Title: id, Severity: "sev3", PipelineID: "orders"}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Append(ctx, id, events.AgentStatus, events.AgentActor, events.AgentStatusPayload{State: state}); err != nil {
			t.Fatal(err)
		}
	}
	mk("r_working", "working")
	mk("r_idle", "idle")
	mk("r_leased", "working")
	if ok, _ := st.AcquireRoomLease(ctx, "r_leased", "elsewhere", "orders", time.Minute); !ok {
		t.Fatal("acquire")
	}
	log := newAgentLog()
	s := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r0", log, nil)
	notice := func(room string) bool {
		for _, e := range lrEvents(t, s, room) {
			if p, _ := decode[events.AgentStatusPayload](e); e.Type == events.AgentStatus && p.Detail == restartDetail {
				return true
			}
		}
		return false
	}
	if !notice("r_working") {
		t.Fatal("a working agent with no lease got no restart notice")
	}
	if notice("r_idle") || notice("r_leased") {
		t.Fatalf("restart notice where none is due: idle=%v leased=%v", notice("r_idle"), notice("r_leased"))
	}
}
