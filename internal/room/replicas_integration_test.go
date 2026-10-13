//go:build integration

// Multi-replica tests (docs/design/replicas.md): two or three room services in
// one process, each with its own pool, on one throwaway Postgres schema. They
// skip when the room database is unreachable.
package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
	"piperoom/migrations"
)

// fastTimings keeps every lease and sweep period short so a test sees
// expiry and takeover in seconds.
var fastTimings = ReplicaTimings{
	LeaseTTL:       3 * time.Second,
	LeaseRenew:     time.Second,
	TakeoverScan:   300 * time.Millisecond,
	ExecSweep:      300 * time.Millisecond,
	ClaimStale:     2 * time.Second,
	UnclaimedStale: 2 * time.Second,
}

// pgSchema makes a fresh schema with every migration applied and returns
// its name; it is dropped at cleanup.
func pgSchema(t *testing.T) string {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		t.Skipf("room Postgres unreachable at %s: %v", cfg.RoomDBURL, err)
	}
	defer conn.Close(ctx)
	schema := testenv.Name("replicas")
	if _, err := conn.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Skipf("cannot create schema: %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		if c, err := pgx.Connect(dctx, cfg.RoomDBURL); err == nil {
			_, _ = c.Exec(dctx, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
			_ = c.Close(dctx)
		}
	})
	cc, err := pgx.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	cc.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cc)
	defer db.Close()
	if _, err := migrations.Up(ctx, db); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	return schema
}

// pgReplicaStore opens one replica's store (its own pool) on schema.
func pgReplicaStore(t *testing.T, schema string, opts store.Options) *store.PG {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pc, err := pgxpool.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	pc.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewPoolOptions(pool, cfg.RoomDBURL, slog.Default(), opts)
	t.Cleanup(st.Close)
	return st
}

func execOn(t *testing.T, schema, q string, args ...any) {
	t.Helper()
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cc, err := pgx.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	cc.RuntimeParams["search_path"] = schema
	c, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// agentLog records every agent run of every replica.
type agentLog struct {
	mu     sync.Mutex
	runs   []*agentRunRec
	inputs map[string][]string // replica/room -> input texts
}

type agentRunRec struct {
	replica, room string
	start, end    time.Time
}

func newAgentLog() *agentLog { return &agentLog{inputs: map[string][]string{}} }

// runner is the fake agent of one replica: it says it is working, then reads
// its inbox until its context ends.
func (l *agentLog) runner(replica string) agent.Runner {
	return func(ctx context.Context, a agent.Alert, deps agent.Deps, _ agent.Options) error {
		rec := &agentRunRec{replica: replica, room: a.RoomID, start: time.Now()}
		l.mu.Lock()
		l.runs = append(l.runs, rec)
		l.mu.Unlock()
		defer func() {
			l.mu.Lock()
			rec.end = time.Now()
			l.mu.Unlock()
		}()
		_ = deps.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working", Detail: "investigating"})
		for {
			items, err := deps.Inbox.Drain(ctx)
			if err != nil {
				return nil
			}
			l.mu.Lock()
			for _, in := range items {
				l.inputs[replica+"/"+a.RoomID] = append(l.inputs[replica+"/"+a.RoomID], in.Text)
			}
			l.mu.Unlock()
			if err := deps.Inbox.Wait(ctx); err != nil {
				return nil
			}
		}
	}
}

func (l *agentLog) runsOf(room string) []agentRunRec {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []agentRunRec
	for _, r := range l.runs {
		if r.room == room {
			out = append(out, *r)
		}
	}
	return out
}

func (l *agentLog) inputsOf(replica, room string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.inputs[replica+"/"+room]...)
}

// itAction is a scoped action that counts its executions.
type itAction struct {
	calls atomic.Int32
	delay time.Duration
}

func (a *itAction) Name() string                   { return "it_fix" }
func (a *itAction) Layer() string                  { return "kafka" }
func (a *itAction) Describe() string               { return "it_fix" }
func (a *itAction) ParamsSchema() map[string]any   { return map[string]any{"type": "object"} }
func (a *itAction) Validate(json.RawMessage) error { return nil }
func (a *itAction) CheckScope(owners.Pipeline, json.RawMessage) error {
	return nil
}
func (a *itAction) Execute(ctx context.Context, _ json.RawMessage) (string, error) {
	a.calls.Add(1)
	select {
	case <-time.After(a.delay):
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return "fixed", nil
}

func itOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}}
}

type replicaOpt func(*Deps)

// newReplica builds one replica's room service on st and loads it.
func newReplica(t *testing.T, st store.Store, name string, log *agentLog, act actions.Action, opts ...replicaOpt) *Service {
	t.Helper()
	reg := actions.NewRegistry()
	if act != nil {
		if err := reg.Register(act); err != nil {
			t.Fatal(err)
		}
	}
	d := Deps{
		Store:          st,
		Actions:        reg,
		Users:          DefaultUsers(),
		Owners:         itOwners(),
		Run:            log.runner(name),
		ReplicaID:      name,
		ReplicaTimings: fastTimings,
	}
	for _, o := range opts {
		o(&d)
	}
	s := New(d)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.Load(ctx); err != nil {
		t.Fatalf("load %s: %v", name, err)
	}
	t.Cleanup(s.Close)
	return s
}

func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("not within %s: %s", within, what)
}

func user(t *testing.T, s *Service, id string) User {
	t.Helper()
	u, ok := s.LookupUser(id)
	if !ok {
		t.Fatalf("no user %s", id)
	}
	return u
}

// One room's agent runs on exactly one replica: steers and @agent comments
// sent to the others reach the owner's inbox through the log, and a burst of
// @agent comments on three replicas for a room with no agent starts one run.
func TestReplicasOneAgentPerRoom(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	var svcs []*Service
	for i := 0; i < 3; i++ {
		svcs = append(svcs, newReplica(t, pgReplicaStore(t, schema, store.Options{}), fmt.Sprintf("r%d", i), log, nil))
	}
	a, b, c := svcs[0], svcs[1], svcs[2]
	ctx := context.Background()
	arjun := user(t, a, "arjun")

	snap, err := a.CreateRoom(ctx, arjun, "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	room := snap.ID
	eventually(t, 5*time.Second, "agent started on r0", func() bool { return len(log.runsOf(room)) == 1 })

	// Driver steers from two replicas race on the conditional append; after
	// three tries the spec answers 409 conflict (the caller retries), so a
	// steer that got 409 sent no input and is not expected in the inbox.
	var wg sync.WaitGroup
	var sent atomic.Int64
	for i := 0; i < 10; i++ {
		for _, s := range []*Service{b, c} {
			wg.Add(1)
			go func(s *Service, i int) {
				defer wg.Done()
				_, err := s.Steer(ctx, arjun, room, fmt.Sprintf("steer %s %d", s.replica, i))
				var re *Error
				switch {
				case err == nil:
					sent.Add(1)
				case errors.As(err, &re) && re.Code == "conflict":
				default:
					t.Errorf("steer on %s: %v", s.replica, err)
				}
				sent.Add(1) // the comment below
				if _, err := s.AddComment(ctx, user(t, s, "priya"), room, "", fmt.Sprintf("@agent look %s %d", s.replica, i)); err != nil {
					t.Errorf("comment on %s: %v", s.replica, err)
				}
			}(s, i)
		}
	}
	wg.Wait()
	accepted := int(sent.Load())
	if accepted <= 20 {
		t.Fatalf("only %d of 40 inputs were accepted: no driver steer got through", accepted)
	}
	eventually(t, 10*time.Second, fmt.Sprintf("the owner's inbox gets all %d accepted inputs", accepted), func() bool { return len(log.inputsOf("r0", room)) == accepted })
	time.Sleep(500 * time.Millisecond)
	if runs := log.runsOf(room); len(runs) != 1 || runs[0].replica != "r0" {
		t.Fatalf("runs = %+v, want exactly one on r0", runs)
	}
	// Three replicas wrote to the room at once; each one's snapshot is still
	// exactly the fold of the whole log (no event skipped in a seq gap).
	// Postgres keeps microseconds; the replica that wrote an event folded
	// its nanoseconds (as a single server always has), so times are compared
	// at the precision the log keeps.
	norm := func(s RoomSnapshot) []byte {
		s.OpenedAt = s.OpenedAt.Truncate(time.Microsecond)
		if s.Driver != nil {
			// A driver lease renewal that is not logged lives only on the
			// rooms row: a replica adopts it before its next command
			// (syncRoom), while a GET answers from the cache.
			d := *s.Driver
			d.LeaseUntil = time.Time{}
			s.Driver = &d
		}
		b, _ := json.Marshal(s)
		return b
	}
	want, _, err := a.rebuild(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON := norm(want)
	for _, s := range svcs {
		// Reads answer from the cache the follower keeps: give it a moment.
		var gotJSON []byte
		deadline := time.Now().Add(5 * time.Second)
		for {
			got, err := s.GetRoom(ctx, room)
			if err != nil {
				t.Fatal(err)
			}
			if gotJSON = norm(got); string(gotJSON) == string(wantJSON) || time.Now().After(deadline) {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("snapshot on %s differs from the log:\n got %s\nwant %s", s.replica, gotJSON, wantJSON)
		}
	}

	// A room whose agent runs nowhere (opened by another process that
	// stopped): @agent on all three replicas at once starts exactly one run.
	st0 := a.store
	idle := "r_idle_" + strings.ToLower(NewID(""))
	if err := st0.CreateRoom(ctx, store.Room{ID: idle, Title: "idle", Severity: "sev3", PipelineID: "orders", Status: "open", OpenedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if _, err := st0.Append(ctx, idle, events.RoomCreated, systemActor, events.RoomCreatedPayload{Title: "idle", Severity: "sev3", PipelineID: "orders"}); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(s *Service, i int) {
			defer wg.Done()
			<-start
			if _, err := s.AddComment(ctx, user(t, s, "priya"), idle, "", fmt.Sprintf("@agent wake %d", i)); err != nil {
				t.Errorf("comment: %v", err)
			}
		}(svcs[i%3], i)
	}
	close(start)
	wg.Wait()
	eventually(t, 5*time.Second, "one run for the idle room", func() bool { return len(log.runsOf(idle)) >= 1 })
	time.Sleep(time.Second)
	runs := log.runsOf(idle)
	if len(runs) != 1 {
		t.Fatalf("%d runs for one room, want 1: %+v", len(runs), runs)
	}
	owner := runs[0].replica
	eventually(t, 10*time.Second, "the owner gets all 30 mentions", func() bool { return len(log.inputsOf(owner, idle)) == 30 })
}

// faultyCoord is a replica whose lease writes fail (renew, acquire, take
// over) while the rest of its database access works, as when it is cut off
// from the leases but still serves the room: it must stop its agent before
// the lease can expire, and only then may another replica take the room over.
type faultyCoord struct {
	*store.PG
	failRenew atomic.Bool
}

var errInjected = fmt.Errorf("injected lease write failure")

func (f *faultyCoord) RenewRoomLeases(ctx context.Context, owner string, ids []string, ttl time.Duration) ([]string, error) {
	if f.failRenew.Load() {
		return nil, errInjected
	}
	return f.PG.RenewRoomLeases(ctx, owner, ids, ttl)
}

func (f *faultyCoord) AcquireRoomLease(ctx context.Context, roomID, owner, pipelineID string, ttl time.Duration) (bool, error) {
	if f.failRenew.Load() {
		return false, errInjected
	}
	return f.PG.AcquireRoomLease(ctx, roomID, owner, pipelineID, ttl)
}

func (f *faultyCoord) TakeOverRoomLease(ctx context.Context, roomID, owner string, ttl time.Duration) (bool, error) {
	if f.failRenew.Load() {
		return false, errInjected
	}
	return f.PG.TakeOverRoomLease(ctx, roomID, owner, ttl)
}

// The owner stops renewing: its agent stops before the lease expires, another
// replica takes the lease over and resumes the agent, with a steer the old
// owner never saw in its inbox.
func TestReplicasLeaseTakeover(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	fa := &faultyCoord{PG: pgReplicaStore(t, schema, store.Options{})}
	// The action is registered so a late proposal from r0 would be valid:
	// only the fence may refuse it.
	a := newReplica(t, fa, "r0", log, &itAction{})
	b := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r1", log, &itAction{}, func(d *Deps) { d.ResumeOnTakeover = true })
	ctx := context.Background()
	arjun := user(t, a, "arjun")

	snap, err := a.CreateRoom(ctx, arjun, "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	room := snap.ID
	eventually(t, 5*time.Second, "agent on r0", func() bool { return len(log.runsOf(room)) == 1 })
	// Several renew periods with a healthy owner: nothing moves.
	time.Sleep(2 * fastTimings.LeaseTTL)
	if runs := log.runsOf(room); len(runs) != 1 || !runs[0].end.IsZero() {
		t.Fatalf("runs with a healthy owner = %+v", runs)
	}

	fa.failRenew.Store(true)
	stopped := time.Now()
	eventually(t, 2*fastTimings.LeaseTTL, "r0 stops its agent", func() bool { return !log.runsOf(room)[0].end.IsZero() })
	if since := time.Since(stopped); since > fastTimings.LeaseTTL {
		t.Fatalf("r0 stopped its agent %s after the renewals failed; the lease TTL is %s", since, fastTimings.LeaseTTL)
	}
	// A steer while nobody runs the agent: it waits in the log.
	if _, err := b.Steer(ctx, arjun, room, "check the slot"); err != nil {
		t.Fatal(err)
	}
	eventually(t, 4*fastTimings.LeaseTTL, "r1 resumes the agent", func() bool { return len(log.runsOf(room)) == 2 })
	runs := log.runsOf(room)
	if runs[1].replica != "r1" {
		t.Fatalf("second run on %s, want r1", runs[1].replica)
	}
	if !runs[0].end.Before(runs[1].start) {
		t.Fatalf("old run ended %s, new run started %s: two agents at once", runs[0].end.Format(time.StampMilli), runs[1].start.Format(time.StampMilli))
	}
	eventually(t, 5*time.Second, "the resumed run gets the steer", func() bool {
		for _, in := range log.inputsOf("r1", room) {
			if in == "check the slot" {
				return true
			}
		}
		return false
	})
	// The old owner's agent may no longer write.
	// The old owner's run (generation 1) may no longer write: no events and
	// no proposals.
	if err := a.agentEmit(ctx, room, 1, events.AgentMessage, events.AgentMessagePayload{Text: "late"}); err == nil {
		t.Fatal("the replica that lost the lease could still write agent events")
	}
	if pid, err := (proposalCreator{s: a, roomID: room, run: 1}).Create(ctx, events.ProposalCreatedPayload{
		Title: "late fix", Risk: "low", Rollback: "restart the task again",
		Steps:          []events.ProposalStep{{Action: "it_fix", Layer: "kafka", Params: json.RawMessage(`{}`)}},
		RequiredLayers: []string{"kafka"},
	}); err == nil {
		t.Fatalf("the replica that lost the lease created proposal %s", pid)
	}
	// Everyone sees the resumed agent working.
	eventually(t, 5*time.Second, "r0 sees the resumed agent", func() bool {
		got, err := a.GetRoom(ctx, room)
		return err == nil && got.AgentStatus.State == "working"
	})
}

// With PIPEROOM_RESUME_ON_TAKEOVER off (the default) a takeover pauses the
// agent, as a restart does on main: notice, no new run, lease dropped.
func TestReplicasTakeoverPausesByDefault(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	fa := &faultyCoord{PG: pgReplicaStore(t, schema, store.Options{})}
	a := newReplica(t, fa, "r0", log, nil)
	b := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r1", log, nil, func(d *Deps) { d.ResumeOnTakeover = false })
	ctx := context.Background()
	snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	room := snap.ID
	eventually(t, 5*time.Second, "agent on r0", func() bool { return len(log.runsOf(room)) == 1 })
	fa.failRenew.Store(true)
	eventually(t, 4*fastTimings.LeaseTTL, "r1 pauses the room", func() bool {
		for _, e := range lrEvents(t, b, room) {
			if e.Type == events.AgentStatus {
				if p, _ := decode[events.AgentStatusPayload](e); p.Detail == restartDetail {
					return true
				}
			}
		}
		return false
	})
	time.Sleep(time.Second)
	if n := len(log.runsOf(room)); n != 1 {
		t.Fatalf("%d runs; with the flag off a takeover must not start one", n)
	}
	if leases, _ := b.coord.RoomLeases(ctx); len(leases) != 0 {
		t.Fatalf("lease kept after a pause: %+v", leases)
	}
}

// An approved proposal executes exactly once across replicas: the approving
// replica's executor and 50 more executor attempts spread over three
// replicas race for it.
func TestReplicasExecutorExactlyOnce(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	act := &itAction{delay: 300 * time.Millisecond}
	var svcs []*Service
	for i := 0; i < 3; i++ {
		svcs = append(svcs, newReplica(t, pgReplicaStore(t, schema, store.Options{}), fmt.Sprintf("r%d", i), log, act))
	}
	a := svcs[0]
	ctx := context.Background()
	snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	room := snap.ID
	pid, err := a.CreateProposal(ctx, room, events.ProposalCreatedPayload{
		Title: "restart", Risk: "low", Rollback: "restart the task again",
		Steps: []events.ProposalStep{
			{Action: "it_fix", Layer: "kafka", Params: json.RawMessage(`{"n":1}`)},
			{Action: "it_fix", Layer: "kafka", Params: json.RawMessage(`{"n":2}`)},
		},
		RequiredLayers: []string{"kafka"},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	row, err := a.store.GetProposal(ctx, pid)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(s *Service) {
			defer wg.Done()
			<-start
			if err := s.executeProposal(row, false); err != nil {
				t.Errorf("execute on %s: %v", s.replica, err)
			}
		}(svcs[i%3])
	}
	close(start)
	b := svcs[1]
	if _, err := b.ApproveProposal(ctx, user(t, b, "arjun"), pid, "kafka", row.ParamsHash); err != nil {
		t.Fatalf("approve: %v", err)
	}
	wg.Wait()
	eventually(t, 10*time.Second, "proposal executed", func() bool {
		p, err := a.store.GetProposal(ctx, pid)
		return err == nil && p.Status == "executed"
	})
	time.Sleep(500 * time.Millisecond)
	if n := act.calls.Load(); n != 2 {
		t.Fatalf("the action ran %d times for 2 steps, want 2", n)
	}
	evs, err := a.store.Since(ctx, room, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	okSteps := map[int]int{}
	for _, e := range evs {
		if e.Type != events.ActionExecuted {
			continue
		}
		p, _ := decode[events.ActionExecutedPayload](e)
		if !p.OK {
			t.Fatalf("a losing executor wrote a failure: %+v", p)
		}
		okSteps[p.StepIndex]++
	}
	if okSteps[0] != 1 || okSteps[1] != 1 || len(okSteps) != 2 {
		t.Fatalf("action.executed per step = %v, want one each for steps 0 and 1", okSteps)
	}
	p, _ := a.store.GetProposal(ctx, pid)
	if p.Status != "executed" {
		t.Fatalf("status %q", p.Status)
	}
}

// An executor that claimed a proposal and stopped mid-step: nothing happens
// while its claim is fresh; once it is stale exactly one replica records the
// step's outcome as unknown, and nobody runs it again.
func TestReplicasStaleClaimOutcomeUnknown(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	act := &itAction{}
	var svcs []*Service
	for i := 0; i < 3; i++ {
		svcs = append(svcs, newReplica(t, pgReplicaStore(t, schema, store.Options{}), fmt.Sprintf("r%d", i), log, act))
	}
	a := svcs[0]
	ctx := context.Background()
	snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	room := snap.ID
	pid, err := a.CreateProposal(ctx, room, events.ProposalCreatedPayload{
		Title: "restart", Risk: "low", Rollback: "restart the task again",
		Steps:          []events.ProposalStep{{Action: "it_fix", Layer: "kafka", Params: json.RawMessage(`{}`)}},
		RequiredLayers: []string{"kafka"},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	// A replica that no longer exists claimed it and started step 0.
	execOn(t, schema, `UPDATE proposals SET claimed_by = 'dead-replica', claimed_at = now() WHERE proposal_id = $1`, pid)
	execOn(t, schema, `INSERT INTO proposal_steps (proposal_id, step_index, status) VALUES ($1, 0, 'started')`, pid)
	countFailures := func() int {
		evs, err := a.store.Since(ctx, room, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range evs {
			if e.Type == events.ActionExecuted {
				p, _ := decode[events.ActionExecutedPayload](e)
				if p.OK || !strings.Contains(p.Output, "outcome unknown") {
					t.Fatalf("unexpected action.executed %+v", p)
				}
				n++
			}
		}
		return n
	}
	// Fresh claim: the sweeps (every 300 ms on three replicas) leave it.
	time.Sleep(fastTimings.ClaimStale / 2)
	if n := countFailures(); n != 0 {
		t.Fatalf("a live claim was interrupted (%d events)", n)
	}
	eventually(t, 4*fastTimings.ClaimStale, "the stale claim is recorded", func() bool { return countFailures() > 0 })
	time.Sleep(time.Second)
	if n := countFailures(); n != 1 {
		t.Fatalf("%d outcome events, want 1", n)
	}
	if n := act.calls.Load(); n != 0 {
		t.Fatalf("the step ran %d times after the crash, want 0", n)
	}
	p, _ := a.store.GetProposal(ctx, pid)
	if p.Status != "failed" {
		t.Fatalf("status %q, want failed", p.Status)
	}
	// Every replica's snapshot shows it decided.
	for _, s := range svcs {
		eventually(t, 5*time.Second, "snapshot drops the proposal on "+s.replica, func() bool {
			got, err := s.GetRoom(ctx, room)
			return err == nil && len(got.OpenProposals) == 0
		})
	}
}

// Each replica's cache follows the log: control taken on A counts on B at
// once; a close on B stops the agent on A; the same alert sent to both at once
// opens one room.
func TestReplicasCacheFollowsLog(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	var closedOnA atomic.Int32
	a := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r0", log, nil, func(d *Deps) {
		d.OnRoomClosed = func(context.Context, string, string, string) { closedOnA.Add(1) }
	})
	// B hears no notifications at all (no LISTEN, no poll in the test's
	// time): whatever B knows about A's writes comes from reading the log
	// before it acts, which is the guard under test.
	b := newReplica(t, pgReplicaStore(t, schema, store.Options{NoListen: true, PollInterval: time.Hour}), "r1", log, nil)
	ctx := context.Background()
	arjun, priya := user(t, a, "arjun"), user(t, a, "priya")

	snap, err := a.CreateRoom(ctx, arjun, "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	room := snap.ID
	eventually(t, 5*time.Second, "agent on r0", func() bool { return len(log.runsOf(room)) == 1 })
	if _, err := b.GetRoom(ctx, room); err != nil {
		t.Fatal(err)
	}
	// arjun hands control to priya on A; priya steers on B right away.
	if _, err := a.GrantControl(ctx, arjun, room, "priya"); err != nil {
		t.Fatal(err)
	}
	ev, err := b.Steer(ctx, priya, room, "as driver")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Type != events.SteerSent {
		t.Fatalf("priya's steer on B became %s: B did not see the control change", ev.Type)
	}
	// priya closes on B: the agent on A stops and A's room-closed hook runs.
	if _, err := b.CloseRoom(ctx, priya, room, "fixed"); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "agent on r0 stopped", func() bool { return !log.runsOf(room)[0].end.IsZero() })
	eventually(t, 5*time.Second, "hook on r0", func() bool { return closedOnA.Load() == 1 })
	leases, err := a.coord.RoomLeases(ctx)
	if err != nil || len(leases) != 0 {
		t.Fatalf("leases after close: %+v %v", leases, err)
	}

	// The same alert reaches both replicas at the same moment.
	alert := alertin.Alert{Source: alertin.Alertmanager, Status: alertin.StatusFiring, Title: "Lag", Severity: "sev2", PipelineID: "orders", DedupKey: "alertmanager:lag", Raw: json.RawMessage(`{"a":1}`)}
	var ids [2]string
	var created atomic.Int32
	var wg sync.WaitGroup
	for i, s := range []*Service{a, b} {
		wg.Add(1)
		go func(i int, s *Service) {
			defer wg.Done()
			got, isNew, err := s.OpenAlertRoom(ctx, alert)
			if err != nil {
				t.Errorf("open alert on %s: %v", s.replica, err)
				return
			}
			ids[i] = got.ID
			if isNew {
				created.Add(1)
			}
		}(i, s)
	}
	wg.Wait()
	if created.Load() != 1 || ids[0] != ids[1] {
		t.Fatalf("one alert on two replicas: created=%d rooms %v, want one room", created.Load(), ids)
	}
	// And a repeat later on the other replica joins it too.
	got, isNew, err := b.OpenAlertRoom(ctx, alert)
	if err != nil || isNew || got.ID != ids[0] {
		t.Fatalf("repeat: %s new=%v err=%v", got.ID, isNew, err)
	}
	// B's watcher view: the open room for the pipeline is that alert room.
	if id, ok := b.OpenRoomForPipeline("orders"); !ok || id != ids[0] {
		t.Fatalf("OpenRoomForPipeline on B = %q %v, want %q", id, ok, ids[0])
	}
}
