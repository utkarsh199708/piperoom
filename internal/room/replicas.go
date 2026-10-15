package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Multi-replica coordination of the room service (docs/design/replicas.md).
// Everything here is off when the store is not a store.Coordinator (memstore):
// that store belongs to one process, so the cache is the truth, every lease is
// this process's, and the code paths below return early.

// Production timings (docs/design/replicas.md section 10).
const (
	DefaultLeaseTTL     = 30 * time.Second
	DefaultLeaseRenew   = 10 * time.Second
	DefaultTakeoverScan = 10 * time.Second
	DefaultExecSweep    = 30 * time.Second
	// DefaultClaimStale is when an executor claim counts as dead: no refresh
	// for one step timeout plus a margin for the result writes.
	DefaultClaimStale = stepTimeout + 2*time.Minute
	// DefaultUnclaimedStale is how long a fully approved proposal may wait for
	// an executor to claim it.
	DefaultUnclaimedStale = 2 * time.Minute
)

// maxPendingInputs caps the human inputs a room keeps for priming the inbox
// of a replica that becomes its owner.
const maxPendingInputs = 50

// coordTimeout bounds each coordination statement.
const coordTimeout = 5 * time.Second

// ReplicaTimings tunes the coordination periods. Zero fields take the
// Default* values; tests shorten them.
type ReplicaTimings struct {
	LeaseTTL       time.Duration
	LeaseRenew     time.Duration
	TakeoverScan   time.Duration
	ExecSweep      time.Duration
	ClaimStale     time.Duration
	UnclaimedStale time.Duration
}

func (t ReplicaTimings) withDefaults() ReplicaTimings {
	def := func(d, v time.Duration) time.Duration {
		if d > 0 {
			return d
		}
		return v
	}
	t.LeaseTTL = def(t.LeaseTTL, DefaultLeaseTTL)
	t.LeaseRenew = def(t.LeaseRenew, DefaultLeaseRenew)
	t.TakeoverScan = def(t.TakeoverScan, DefaultTakeoverScan)
	t.ExecSweep = def(t.ExecSweep, DefaultExecSweep)
	t.ClaimStale = def(t.ClaimStale, DefaultClaimStale)
	t.UnclaimedStale = def(t.UnclaimedStale, DefaultUnclaimedStale)
	return t
}

// leaseGrace is how long before the TTL ends the owner stops the agent when no
// renew has confirmed the lease: one renew period.
func (t ReplicaTimings) leaseGrace() time.Duration {
	if g := t.LeaseTTL - t.LeaseRenew; g > 0 {
		return g
	}
	return t.LeaseTTL / 2
}

// ReplicaID returns the name this service uses in leases and claims ("" when
// the store is not shared).
func (s *Service) ReplicaID() string { return s.replica }

func (s *Service) coordCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(s.ctx), coordTimeout)
}

// ---- global slots ----

// coordSlots is globalSlots on the store's leases.
type coordSlots struct {
	s   *Service
	max int
}

func (c coordSlots) take(ctx context.Context, roomID string) (bool, error) {
	tctx, cancel := context.WithTimeout(ctx, coordTimeout)
	defer cancel()
	return c.s.coord.TakeAgentSlot(tctx, roomID, c.s.replica, c.max)
}

func (c coordSlots) give(roomID string) {
	ctx, cancel := c.s.coordCtx()
	defer cancel()
	if err := c.s.coord.ReleaseAgentSlot(ctx, roomID, c.s.replica); err != nil {
		// The slot stops counting when the lease expires or is renewed
		// without it; a failed release only delays another room.
		c.s.log.Warn("room: release global agent slot failed", "room", roomID, "err", err)
	}
}

func (c coordSlots) limit() int { return c.max }

// ---- leases ----

// claimLeaseLocked makes sure this replica owns the room's agent lease before
// it starts the agent. With a single-process store it always succeeds. A
// newly taken lease primes the inbox with the input the room's log holds for
// the agent (docs/design/replicas.md section 4.2). Caller holds st's lock.
func (s *Service) claimLeaseLocked(roomID string, st *roomState) bool {
	if s.coord == nil || st.leased {
		return true
	}
	ctx, cancel := s.coordCtx()
	defer cancel()
	start := time.Now()
	ok, err := s.coord.AcquireRoomLease(ctx, roomID, s.replica, st.snap.PipelineID, s.timing.LeaseTTL)
	if err != nil {
		s.log.Warn("room: acquire agent lease failed; agent not started here", "room", roomID, "err", err)
		return false
	}
	if !ok {
		s.log.Debug("room: agent runs on another replica", "room", roomID)
		return false
	}
	s.holdLeaseLocked(st, start)
	return true
}

// setLeased sets leased and its lock-free mirror. Caller holds st's lock.
func (st *roomState) setLeased(v bool) {
	st.leased = v
	st.leasedHint.Store(v)
}

// leaseDeadline is leaseValid as a time.
func (st *roomState) leaseDeadline() time.Time {
	return time.Unix(0, st.leaseValid.Load())
}

// holdLeaseLocked records a lease this replica just took (at start) and primes
// the inbox. Caller holds st's lock.
func (s *Service) holdLeaseLocked(st *roomState, start time.Time) {
	st.setLeased(true)
	st.leaseValid.Store(start.Add(s.timing.leaseGrace()).UnixNano())
	if !st.snap.Shadow {
		st.inbox.reset(st.pending)
	}
}

// dropLeaseLocked gives the room's lease back (deletes the row) when this
// replica holds it. Caller holds st's lock.
func (s *Service) dropLeaseLocked(roomID string, st *roomState) {
	if s.coord == nil || !st.leased {
		return
	}
	st.setLeased(false)
	ctx, cancel := s.coordCtx()
	defer cancel()
	if err := s.coord.ReleaseRoomLease(ctx, roomID, s.replica); err != nil {
		// The lease then expires on its own and the takeover scan finds the
		// room closed (or the agent idle) and deletes it.
		s.log.Warn("room: release agent lease failed", "room", roomID, "err", err)
	}
}

// loseLeaseLocked stops the room's agent on this replica because its lease is
// gone or can no longer be confirmed: another replica may own the room
// already, or will once the lease expires. Caller holds st's lock.
func (s *Service) loseLeaseLocked(roomID string, st *roomState, why string) {
	if !st.leased {
		return
	}
	st.setLeased(false)
	s.log.Warn("room: lost the agent lease; stopping the agent here", "room", roomID, "replica", s.replica, "why", why)
	if st.agentCancel != nil {
		st.agentCancel()
		st.agentCancel = nil
		st.agentRun++
	}
	if st.graceTimer != nil {
		st.graceTimer.Stop()
		st.graceTimer = nil
	}
}

// startCoordination starts the lease, takeover, sweep and follower goroutines
// once, after Load. No-op with a single-process store.
func (s *Service) startCoordination() {
	if s.coord == nil {
		return
	}
	s.coordOnce.Do(func() {
		if !s.addWorkers(4) {
			return
		}
		s.coord.WatchAppends(s.ctx, store.AppendWatch{OnAppend: s.follow.noteAppend, OnResync: s.follow.noteResync})
		go func() {
			defer s.wg.Done()
			s.runFollower()
		}()
		go func() {
			defer s.wg.Done()
			s.runLeaseWatchdog()
		}()
		go func() {
			defer s.wg.Done()
			s.runLeaseRenewal()
		}()
		go func() {
			defer s.wg.Done()
			s.runCoordination()
		}()
	})
}

// runLeaseRenewal renews this replica's leases and refreshes its queued
// claims until the service closes. It has its own goroutine, and takes no
// room lock it would wait for: whatever else stalls (a takeover or a sweep
// waiting for a room stuck in a slow call), the leases of every other room
// stay renewed, so no other replica takes them over.
func (s *Service) runLeaseRenewal() {
	renew := time.NewTicker(s.timing.LeaseRenew)
	defer renew.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-renew.C:
			s.renewLeases()
			s.touchQueuedClaims()
		}
	}
}

// runCoordination takes over expired leases and sweeps stale executor claims
// until the service closes.
func (s *Service) runCoordination() {
	scan := time.NewTicker(s.timing.TakeoverScan)
	defer scan.Stop()
	sweep := time.NewTicker(s.timing.ExecSweep)
	defer sweep.Stop()
	s.takeoverScan()
	s.sweepStaleProposals()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-scan.C:
			s.takeoverScan()
		case <-sweep.C:
			s.sweepStaleProposals()
		}
	}
}

// renewLeases extends every lease this replica holds, in one statement. A
// lease the store did not extend is lost: its agent stops here at once.
func (s *Service) renewLeases() {
	// The lease state is read without the room locks, so a room stuck in a
	// slow call keeps its lease confirmed like every other.
	var ids []string
	held := map[string]*roomState{}
	for id, st := range s.cachedRooms() {
		if st.leasedHint.Load() {
			ids = append(ids, id)
			held[id] = st
		}
	}
	if len(ids) == 0 {
		return
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(s.ctx, min(coordTimeout, s.timing.LeaseRenew))
	renewed, err := s.coord.RenewRoomLeases(ctx, s.replica, ids, s.timing.LeaseTTL)
	cancel()
	if err != nil {
		if s.ctx.Err() == nil {
			// The watchdog stops the agents if this goes on.
			s.log.Warn("room: renew agent leases failed", "rooms", len(ids), "err", err)
		}
		return
	}
	ok := make(map[string]bool, len(renewed))
	for _, id := range renewed {
		ok[id] = true
	}
	valid := start.Add(s.timing.leaseGrace()).UnixNano()
	for _, id := range ids {
		st := held[id]
		if ok[id] {
			st.leaseValid.Store(valid)
			continue
		}
		// A lost lease stops the agent, under the room's lock. A room busy
		// in a slow call is left to the watchdog: its agent's writes are
		// refused once the deadline passes (fenceLocked).
		if st.mu.TryLock() {
			if !st.gone {
				s.loseLeaseLocked(id, st, "the lease expired or another replica took it")
			}
			s.unlockRoom(st)
		}
	}
}

// touchQueuedClaims refreshes the claims of approved proposals that wait in
// this replica's executor queues behind a running one, so the stale sweep
// never mistakes a live replica's queue for a dead one (docs/design/replicas.md
// section 5). A claim that is gone is left for the executor, which re-checks it
// before every step.
func (s *Service) touchQueuedClaims() {
	var ids []string
	for id := range s.cachedRooms() {
		// A room busy in a slow call is skipped: the claims go stale only
		// after minutes, and the next renew period tries again.
		st, _ := s.tryLockCachedRoom(id)
		if st == nil {
			continue
		}
		if st.exec != nil {
			ids = append(ids, st.exec.queuedIDs()...)
		}
		s.unlockRoom(st)
	}
	for _, id := range ids {
		if s.ctx.Err() != nil {
			return
		}
		if _, err := s.touchExecution(s.ctx, id); err != nil && s.ctx.Err() == nil {
			s.log.Warn("room: refresh a queued proposal's claim failed", "proposal", id, "err", err)
		}
	}
}

// runLeaseWatchdog stops the agent of every room whose lease no renew has
// confirmed in time, before the lease can expire and another replica take it.
func (s *Service) runLeaseWatchdog() {
	every := max(s.timing.LeaseRenew/10, 50*time.Millisecond)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		for id, st := range s.cachedRooms() {
			if !st.leasedHint.Load() || !now.After(st.leaseDeadline()) {
				continue
			}
			// A busy room is tried again at the next tick; meanwhile its
			// agent's writes are refused (fenceLocked).
			if st.mu.TryLock() {
				if !st.gone && st.leased && now.After(st.leaseDeadline()) {
					s.loseLeaseLocked(id, st, "no renew confirmed the lease in time")
				}
				s.unlockRoom(st)
			}
		}
	}
}

// takeoverScan takes over the leases of replicas that stopped renewing them
// and adopts those rooms.
func (s *Service) takeoverScan() {
	ctx, cancel := context.WithTimeout(s.ctx, coordTimeout)
	leases, err := s.coord.ExpiredRoomLeases(ctx, 100)
	cancel()
	if err != nil {
		if s.ctx.Err() == nil {
			s.log.Warn("room: list expired agent leases failed", "err", err)
		}
		return
	}
	for _, l := range leases {
		if s.ctx.Err() != nil {
			return
		}
		start := time.Now()
		ctx, cancel := context.WithTimeout(s.ctx, coordTimeout)
		ok, err := s.coord.TakeOverRoomLease(ctx, l.RoomID, s.replica, s.timing.LeaseTTL)
		cancel()
		if err != nil {
			s.log.Warn("room: take over agent lease failed", "room", l.RoomID, "err", err)
			continue
		}
		if !ok {
			continue // another replica was faster
		}
		s.log.Info("room: took over the agent lease", "room", l.RoomID, "from", l.Owner, "replica", s.replica)
		// Adopting locks the room, which may be stuck in a slow call: on its
		// own goroutine, so the scan goes on. Closing: the lease taken here
		// expires again and another replica adopts the room.
		if s.addWorker() {
			go func(roomID string) {
				defer s.wg.Done()
				s.adoptRoom(roomID, start)
			}(l.RoomID)
		}
	}
}

// adoptRoom decides what a replica that just took over a room's lease does
// (docs/design/replicas.md section 4.3): with PIPEROOM_RESUME_ON_TAKEOVER on,
// resume an agent that was working (at most maxResumesPerHour per room);
// otherwise, as on a restart, pause it; finish a shadow run; drop the lease of
// a closed room.
func (s *Service) adoptRoom(roomID string, start time.Time) {
	ctx, cancel := context.WithTimeout(s.ctx, 2*coordTimeout)
	defer cancel()
	s.syncRoom(ctx, roomID) // the room as the old owner left it
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		s.log.Warn("room: adopt room: load failed; the lease will expire again", "room", roomID, "err", err)
		return
	}
	defer s.unlockRoom(st)
	if st.leased && st.agentCancel != nil {
		return // already ours and running (a renew raced the scan)
	}
	s.holdLeaseLocked(st, start)
	switch {
	case s.isClosed():
		return
	case !s.roomOpen(st):
		s.dropLeaseLocked(roomID, st)
	case st.snap.Shadow:
		// A shadow run is one-shot: it never restarts (docs/design/shadow.md
		// 3.7); its row is marked interrupted like the Load sweep does.
		s.interruptShadowRunLocked(ctx, roomID, st)
		s.dropLeaseLocked(roomID, st)
	case (agentWasWorking(st) || len(st.pending) > 0) && s.resumeAllowedLocked(ctx, roomID):
		if _, err := s.emitLocked(ctx, st, roomID, events.AgentMessage, systemActor, events.AgentMessagePayload{
			Text: "the server that ran this room's agent stopped; the agent resumes on another server",
		}); err != nil {
			s.log.Warn("room: adopt room: resume notice failed", "room", roomID, "err", err)
		}
		// The new run has no memory of the old one: tell it which fixes
		// are already proposed, so it does not propose them again.
		if in := openProposalsInput(st); in != nil {
			st.inbox.push(*in)
		}
		s.startAgentLocked(roomID, st.snap.Title, st.snap.PipelineID, st.alertRaw, st)
	default:
		if err := s.noteRestartLocked(ctx, roomID, st); err != nil {
			s.log.Warn("room: adopt room: pause notice failed", "room", roomID, "err", err)
		}
		s.dropLeaseLocked(roomID, st)
	}
}

// maxResumesPerHour caps resumes of one room's agent after takeovers, so a
// run that kills its replica cannot crash-loop across the cluster.
const maxResumesPerHour = 3

// resumeAllowedLocked reports whether this takeover may resume the room's
// agent: PIPEROOM_RESUME_ON_TAKEOVER is on and the room has resumed fewer than
// maxResumesPerHour times in the last hour (counted on the lease row, so the
// cap holds across replicas). Caller holds the room's lock.
func (s *Service) resumeAllowedLocked(ctx context.Context, roomID string) bool {
	if !s.resumeOnTakeover {
		return false
	}
	ok, err := s.coord.NoteResume(ctx, roomID, s.replica, maxResumesPerHour, time.Hour)
	if err != nil {
		s.log.Warn("room: adopt room: count resume failed; pausing instead", "room", roomID, "err", err)
		return false
	}
	if !ok {
		s.log.Warn("room: adopt room: resume cap reached; pausing the agent", "room", roomID, "max_per_hour", maxResumesPerHour)
	}
	return ok
}

// openProposalsInput lists the room's open proposals for a resumed run, or
// nil when there are none.
func openProposalsInput(st *roomState) *agent.HumanInput {
	if len(st.snap.OpenProposals) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("this run resumes an investigation another server started; these fixes are already proposed and open (do not propose them again):")
	for _, p := range st.snap.OpenProposals {
		fmt.Fprintf(&b, " %s (%s);", p.ProposalID, p.Title)
	}
	return &agent.HumanInput{Seq: st.snap.LastSeq, Actor: systemActor, Kind: "comment", Text: b.String()}
}

// agentWasWorking reports whether the room's last agent status says a run
// was in progress.
func agentWasWorking(st *roomState) bool {
	switch st.snap.AgentStatus.State {
	case "working", "queued":
		return true
	}
	return false
}

// interruptShadowRunLocked marks a queued or running shadow run interrupted
// and reveals a closed room, as the Load sweep does for one room. Caller
// holds st's lock.
func (s *Service) interruptShadowRunLocked(ctx context.Context, roomID string, st *roomState) {
	if s.shadow == nil {
		return
	}
	run, err := s.shadow.GetShadowRun(ctx, roomID)
	if err == nil && (run.Status == store.ShadowQueued || run.Status == store.ShadowRunning) {
		now := s.now().UTC()
		run.Status = store.ShadowInterrupted
		run.FinishedAt = &now
		run.FinishedAfterClose = st.snap.ClosedAt != nil && now.After(*st.snap.ClosedAt)
		if err := s.shadow.UpdateShadowRun(ctx, run); err != nil {
			s.log.Warn("room: mark shadow run interrupted failed", "room", roomID, "err", err)
		}
	}
	if !s.roomOpen(st) {
		s.revealLocked(ctx, roomID, st)
	}
}

// ---- executor claims ----

// claimExecution claims a proposal for this replica's executor. With a
// single-process store there is nothing to claim. ok false: another executor
// has it, run nothing.
func (s *Service) claimExecution(ctx context.Context, proposalID string) (bool, error) {
	if s.coord == nil {
		return true, nil
	}
	cctx, cancel := context.WithTimeout(ctx, coordTimeout)
	defer cancel()
	return s.coord.ClaimProposal(cctx, proposalID, s.replica)
}

// touchExecution refreshes this replica's claim before a step. false: the
// claim was declared dead and taken by the stale sweep, so the step must not
// run.
func (s *Service) touchExecution(ctx context.Context, proposalID string) (bool, error) {
	if s.coord == nil {
		return true, nil
	}
	cctx, cancel := context.WithTimeout(ctx, coordTimeout)
	defer cancel()
	return s.coord.TouchProposalClaim(cctx, proposalID, s.replica)
}

// sweepStaleProposals records the outcome of proposals whose executor
// stopped (docs/design/replicas.md section 5). Each one is interrupted by one
// conditional UPDATE, so exactly one replica writes its event. A step that was
// started is never run again: its outcome is unknown and recorded as failed.
func (s *Service) sweepStaleProposals() {
	ctx, cancel := context.WithTimeout(s.ctx, coordTimeout)
	stale, err := s.coord.StaleProposals(ctx, s.timing.ClaimStale, s.timing.UnclaimedStale, 100)
	cancel()
	if err != nil {
		if s.ctx.Err() == nil {
			s.log.Warn("room: list stale proposals failed", "err", err)
		}
		return
	}
	for _, sp := range stale {
		if s.ctx.Err() != nil {
			return
		}
		// The recording locks the room, which may be stuck in a slow call,
		// so it runs on its own goroutine. Its place is reserved before the
		// interruption commits: an interruption this replica won must be
		// recorded, never skipped, so none is won while closing.
		if !s.addWorker() {
			return
		}
		ctx, cancel := context.WithTimeout(s.ctx, coordTimeout)
		ok, err := s.coord.InterruptProposal(ctx, sp.ProposalID, s.replica, s.timing.ClaimStale, s.timing.UnclaimedStale)
		cancel()
		if err != nil || !ok {
			s.wg.Done()
			if err != nil {
				s.log.Warn("room: interrupt stale proposal failed", "proposal", sp.ProposalID, "err", err)
			}
			continue
		}
		s.log.Warn("room: proposal's executor stopped; outcome recorded as failed", "proposal", sp.ProposalID, "room", sp.RoomID, "claimed_by", sp.ClaimedBy)
		go func(sp store.StaleProposal) {
			defer s.wg.Done()
			s.recordInterruption(sp)
		}(sp)
	}
}

// recordInterruption writes the outcome of a proposal the sweep interrupted:
// every started step is finished as failed and one failing action.executed
// names the step that had not finished. The reads (the proposal, the log scan
// for the step) and the step writes happen outside the room's lock, which only
// the append takes: InterruptProposal already decided that this replica
// records the outcome, so nothing here races another writer of it.
func (s *Service) recordInterruption(sp store.StaleProposal) {
	ctx, cancel := s.resultCtx()
	defer cancel()
	snap, err := s.readRoom(ctx, sp.RoomID)
	if err != nil {
		s.log.Warn("room: record interruption: load room failed", "room", sp.RoomID, "err", err)
		return
	}
	var p Proposal
	found := false
	for _, op := range snap.OpenProposals {
		if op.ProposalID == sp.ProposalID {
			p, found = op, true
			break
		}
	}
	if !found {
		row, err := s.store.GetProposal(ctx, sp.ProposalID)
		if err != nil {
			s.log.Warn("room: record interruption: read proposal failed", "proposal", sp.ProposalID, "err", err)
			return
		}
		p = Proposal{ProposalID: row.ProposalID, ParamsHash: row.ParamsHash}
		_ = json.Unmarshal(row.Steps, &p.Steps) // no steps: only the event is written
	}
	next := s.interruptedStep(ctx, sp.RoomID, p)
	output := fmt.Sprintf("outcome unknown: the server running this proposal stopped before it recorded the result of step %d; check the target before you retry with a new proposal", next+1)
	if sp.ClaimedBy == "" {
		output = "the proposal was approved, but the server that should have run it stopped before it started; create a new proposal to retry"
	}
	for i := range p.Steps {
		s.finishClaimedStep(sp.ProposalID, i, output)
	}
	st, err := s.lockRoom(ctx, sp.RoomID)
	if err != nil {
		s.log.Warn("room: record interruption: load room failed", "room", sp.RoomID, "err", err)
		return
	}
	defer s.unlockRoom(st)
	if _, err := s.emitLocked(ctx, st, sp.RoomID, events.ActionExecuted, executorActor, events.ActionExecutedPayload{
		ProposalID: sp.ProposalID, StepIndex: next, OK: false, ParamsHash: p.ParamsHash, Output: output,
		// Claimed: the step may have run. Never claimed: it did not.
		OutcomeUnknown: sp.ClaimedBy != "",
	}); err != nil {
		s.log.Warn("room: emit interruption failed", "proposal", sp.ProposalID, "err", err)
	}
}

// ---- following the log ----

// follower collects append notifications for runFollower.
type follower struct {
	mu     sync.Mutex
	dirty  map[string]int64 // room -> highest notified seq
	resync bool
	// syncs are rooms a re-list found busy: each is brought up to its row
	// (trySyncRoom), its driver lease included, once its lock is free.
	syncs map[string]bool
	wake  chan struct{}
}

func newFollower() *follower {
	return &follower{dirty: map[string]int64{}, syncs: map[string]bool{}, wake: make(chan struct{}, 1)}
}

func (f *follower) noteSync(roomID string) {
	f.mu.Lock()
	f.syncs[roomID] = true
	f.mu.Unlock()
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *follower) noteAppend(roomID string, seq int64) {
	f.mu.Lock()
	if seq > f.dirty[roomID] {
		f.dirty[roomID] = seq
	}
	f.mu.Unlock()
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *follower) noteResync() {
	f.mu.Lock()
	f.resync = true
	f.mu.Unlock()
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *follower) take() (dirty map[string]int64, resync bool, syncs map[string]bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dirty, resync, syncs = f.dirty, f.resync, f.syncs
	f.dirty, f.resync, f.syncs = map[string]int64{}, false, map[string]bool{}
	return dirty, resync, syncs
}

// syncLater queues a busy room for trySyncRoom after a short delay
// (followLater's sibling for rooms a re-list could not bring up to date).
func (s *Service) syncLater(roomID string) {
	time.AfterFunc(followRetry, func() { s.follow.noteSync(roomID) })
}

// runFollower folds what other replicas append into this replica's cache, so
// the owner's inbox gets their steers, a close elsewhere stops the agent here,
// and new rooms enter the alert index (docs/design/replicas.md section 6.2).
func (s *Service) runFollower() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.follow.wake:
		}
		dirty, resync, syncs := s.follow.take()
		if resync {
			ctx, cancel := context.WithTimeout(s.ctx, 2*coordTimeout)
			busy, err := s.refresh(ctx)
			if err != nil && s.ctx.Err() == nil {
				s.log.Warn("room: refresh the room cache failed", "err", err)
			}
			cancel()
			for id := range busy {
				syncs[id] = true
			}
		}
		for roomID := range syncs {
			if s.ctx.Err() != nil {
				return
			}
			// Not even read while busy: the command holding it may be stuck
			// in the store.
			if st := s.cached(roomID); st != nil && !st.mu.TryLock() {
				s.syncLater(roomID)
				continue
			} else if st != nil {
				st.mu.Unlock()
			}
			ctx, cancel := context.WithTimeout(s.ctx, coordTimeout)
			if s.trySyncRoom(ctx, roomID) {
				s.syncLater(roomID)
			}
			cancel()
		}
		for roomID, seq := range dirty {
			if s.ctx.Err() != nil {
				return
			}
			s.followRoom(roomID, seq)
		}
	}
}

// followRetry is how long the follower waits before it tries again a room
// whose lock is busy or whose load is in flight.
const followRetry = 100 * time.Millisecond

// followLater re-queues a room the follower could not fold now. The follower
// serves every room, so it never waits for one: a room stuck in a slow call
// would hold up the steers, closes and new rooms of all the others.
func (s *Service) followLater(roomID string, seq int64) {
	time.AfterFunc(followRetry, func() { s.follow.noteAppend(roomID, seq) })
}

// followRoom folds a room's events up to (at least) seq. The read happens
// outside the room's lock and only the fold takes it; a room whose lock is
// busy, or whose load is in flight, is re-queued (followLater).
func (s *Service) followRoom(roomID string, seq int64) {
	ctx, cancel := context.WithTimeout(s.ctx, coordTimeout)
	defer cancel()
	if s.isClosed() {
		return
	}
	st := s.cached(roomID)
	if st == nil {
		// A room another replica opened: load it so the alert index, the
		// room list and OpenRoomForPipeline know it. A purged room is not
		// found and stays out. The log is read outside every lock (a long
		// room must not hold up every other one). A load already in flight
		// may have read the log before this append: try again after it.
		loaded, _, err := s.loadRoom(ctx, roomID, false)
		switch {
		case err != nil && !isNotFound(err) && s.ctx.Err() == nil:
			s.log.Debug("room: follow: load new room failed", "room", roomID, "err", err)
		case err == nil && loaded == nil:
			s.followLater(roomID, seq)
		}
		return
	}
	p := st.pub.Load()
	if p == nil {
		s.followLater(roomID, seq) // still being opened here
		return
	}
	if st.stale.Load() {
		s.rebuildStale(ctx, roomID, st, seq)
		return
	}
	if seq <= p.LastSeq {
		return // our own append, already folded
	}
	// A room in the middle of a command is not even read now: the command
	// may be stuck in the store.
	if !st.mu.TryLock() {
		s.followLater(roomID, seq)
		return
	}
	after := st.snap.LastSeq
	s.unlockRoom(st)
	if seq <= after {
		return
	}
	evs, err := s.store.Since(ctx, roomID, after, 1000)
	if errors.Is(err, store.ErrNotFound) {
		// Deleted by another replica (an admin or its retention) after the
		// append that woke us.
		if !st.mu.TryLock() {
			s.followLater(roomID, seq)
			return
		}
		if !st.gone && !s.isClosed() {
			s.evictLocked(roomID, st)
		}
		s.unlockRoom(st)
		return
	}
	if err != nil {
		if s.ctx.Err() == nil {
			s.log.Warn("room: follow: read events failed", "room", roomID, "err", err)
		}
		return
	}
	if !st.mu.TryLock() {
		s.followLater(roomID, seq)
		return
	}
	if !st.gone && !s.isClosed() {
		s.foldFetchedLocked(ctx, roomID, st, evs)
	}
	s.unlockRoom(st)
	if len(evs) == 1000 {
		s.follow.noteAppend(roomID, seq) // more to read
	}
}

// rebuildRetry is how long a stale room waits before its rebuild is tried
// again: the read that failed is the whole log, not one page.
const rebuildRetry = time.Second

// rebuildLater queues the rebuild of a stale room on the follower.
func (s *Service) rebuildLater(roomID string, seq int64) {
	time.AfterFunc(rebuildRetry, func() { s.follow.noteAppend(roomID, seq) })
}

// rebuildStale replaces the snapshot of a stale room (fillGapLocked) with one
// folded from the whole log, as a restart would. The log is read outside the
// lock; a failed read or a busy lock is tried again later. Events folded by
// the rebuild do not feed the agent inbox: the pending input it carries is
// what the next agent run starts from, as for a room loaded from the log.
func (s *Service) rebuildStale(ctx context.Context, roomID string, st *roomState, seq int64) {
	l, err := s.rebuild(ctx, roomID)
	if errors.Is(err, store.ErrNotFound) {
		return // deleted; the follower's next read or refresh evicts it
	}
	if err != nil {
		if s.ctx.Err() == nil {
			s.log.Warn("room: rebuild stale room failed; trying again", "room", roomID, "err", err)
			s.rebuildLater(roomID, seq)
		}
		return
	}
	if !st.mu.TryLock() {
		s.followLater(roomID, seq)
		return
	}
	defer s.unlockRoom(st)
	if st.gone || s.isClosed() {
		return
	}
	if l.snap.LastSeq < st.snap.LastSeq {
		// Read before an append of ours that the snapshot already has.
		s.followLater(roomID, seq)
		return
	}
	wasOpen := s.roomOpen(st)
	full := st.snap.LogCap == LogCapFull
	st.snap, st.pending, st.logBytes = l.snap, l.pending, l.logBytes
	if full {
		st.snap.LogCap = LogCapFull // the in-process stop of logFullLocked
	}
	st.changed = true
	st.stale.Store(false)
	if wasOpen && !s.roomOpen(st) {
		s.closedElsewhereLocked(ctx, roomID, st)
	}
}

// loadNewRoom loads a room this replica has not cached yet (loadRoom: outside
// every lock, shared with any other load of it). A purged room is not found
// and stays out.
func (s *Service) loadNewRoom(ctx context.Context, roomID string) {
	if s.isClosed() {
		return
	}
	if _, _, err := s.loadRoom(ctx, roomID, true); err != nil && !isNotFound(err) && s.ctx.Err() == nil {
		s.log.Debug("room: follow: load new room failed", "room", roomID, "err", err)
	}
}

// foldFetchedLocked folds events read outside the lock, skipping those the
// snapshot already has. If they do not continue the snapshot (it moved on and
// back is impossible; a gap means a newer read is needed) the room catches up
// from the store instead. Caller holds st's lock.
func (s *Service) foldFetchedLocked(ctx context.Context, roomID string, st *roomState, evs []events.Event) {
	for _, e := range evs {
		if e.Seq <= st.snap.LastSeq {
			continue
		}
		if e.Seq != st.snap.LastSeq+1 {
			s.catchUpLocked(ctx, roomID, st)
			return
		}
		s.foldLocked(ctx, roomID, st, e, true)
	}
}

// adoptRowLeaseLocked takes the driver lease from the rooms row when it is
// later than the snapshot's: renewals are persisted there and only some are
// logged. Caller holds st's lock.
func (s *Service) adoptRowLeaseLocked(st *roomState, row store.Room) {
	if !rowLeaseLater(&st.snap, row) {
		return
	}
	nd := *st.snap.Driver
	nd.LeaseUntil = row.DriverLeaseUntil.UTC()
	st.snap.Driver = &nd
	st.changed = true
}

// rowLeaseLater reports whether the rooms row holds a later lease for the
// snapshot's driver than the snapshot has.
func rowLeaseLater(snap *RoomSnapshot, row store.Room) bool {
	d := snap.Driver
	return d != nil && row.DriverID != "" && row.DriverID == d.ID &&
		row.DriverLeaseUntil != nil && row.DriverLeaseUntil.After(d.LeaseUntil)
}

// catchUpLocked folds every event after the snapshot. Caller holds st's lock.
func (s *Service) catchUpLocked(ctx context.Context, roomID string, st *roomState) {
	for {
		evs, err := s.store.Since(ctx, roomID, st.snap.LastSeq, 1000)
		if err != nil {
			s.log.Warn("room: catch up from the log failed", "room", roomID, "err", err)
			return
		}
		for _, e := range evs {
			if e.Seq != st.snap.LastSeq+1 {
				s.log.Warn("room: catch up: seq gap in the log", "room", roomID, "have", st.snap.LastSeq, "got", e.Seq)
				return
			}
			s.foldLocked(ctx, roomID, st, e, true)
		}
		if len(evs) < 1000 {
			return
		}
	}
}

// foldLocked applies one event to the cached room: snapshot, the pending input
// kept for a future owner, and the inbox (when this replica owns the agent).
// remote is true for an event another replica appended; when it closes the
// room, this replica stops what it runs for the room. Caller holds st's lock.
func (s *Service) foldLocked(ctx context.Context, roomID string, st *roomState, e events.Event, remote bool) {
	wasOpen := s.roomOpen(st)
	st.snap = Apply(st.snap, e)
	// The store counts the same bytes (len of the payload JSON) for the
	// room's log cap, whichever replica appended the event.
	st.logBytes += int64(len(e.Payload))
	s.setDeadline(&st.snap)
	trackPending(st, e)
	s.feedInbox(st, e)
	st.changed = true
	if remote && wasOpen && !s.roomOpen(st) {
		s.closedElsewhereLocked(ctx, roomID, st)
	}
}

// trackPending keeps the human input logged after the agent's last own event.
// A status is not counted as the agent's: the service writes "queued" and
// "working" for it, and a run may die between draining a steer and acting.
func trackPending(st *roomState, e events.Event) {
	if e.Actor.Kind == events.ActorAgent {
		if e.Type != events.AgentStatus {
			st.pending = nil
		}
		return
	}
	in := inputFor(e)
	if in == nil {
		return
	}
	st.pending = append(st.pending, *in)
	if n := len(st.pending); n > maxPendingInputs {
		st.pending = append([]agent.HumanInput(nil), st.pending[n-maxPendingInputs:]...)
	}
}

// closedElsewhereLocked does on this replica what CloseRoom does on the one
// that closed the room: stop the agent (a shadow run gets its grace), stop the
// executor, free the alert key, give the lease back, and tell the room-closed
// hook (asynchronously: the hook must not run under the room's lock). Caller
// holds st's lock.
func (s *Service) closedElsewhereLocked(ctx context.Context, roomID string, st *roomState) {
	s.releaseAlertKeyLocked(roomID, st)
	cancel := st.agentCancel
	if st.snap.Shadow {
		// Only the replica that runs the shadow run applies its close rules
		// (grace, then the reveal in finishShadow). The closing replica
		// revealed already if the run had ended; every other replica must not
		// reveal again.
		cancel = nil
		if st.shadowActive {
			cancel = s.closeShadowLocked(ctx, roomID, st, st.agentCancel)
		}
	} else {
		st.agentCancel = nil
		s.dropLeaseLocked(roomID, st)
	}
	if cancel != nil {
		cancel()
	}
	if st.execCancel != nil {
		st.execCancel()
	}
	if s.onRoomClosed != nil && s.addWorker() {
		pipelineID, key := st.snap.PipelineID, hookDedupKey(st.snap.PipelineID, s.dedupKeyOf(st))
		go func() {
			defer s.wg.Done()
			s.onRoomClosed(s.ctx, roomID, pipelineID, key)
		}()
	}
}

// evictLocked forgets a room that is gone from the store, and tells the
// OnRoomDeleted hook (goneElsewhereLocked). Caller holds st's lock.
func (s *Service) evictLocked(roomID string, st *roomState) {
	if st.agentCancel != nil {
		st.agentCancel()
		st.agentCancel = nil
	}
	if st.execCancel != nil {
		st.execCancel()
	}
	st.setLeased(false)
	st.gone = true
	s.uncacheRoom(roomID, st)
	s.markListDirty(roomID)
	s.goneElsewhereLocked(roomID)
}

// goneElsewhereLocked hands the room.deleted frame of a room another replica
// deleted to the OnRoomDeleted hook, on its own goroutine: the frame comes
// from the room's audit row, a store read that must not run under the room's
// lock. When the row cannot be read the hook gets the zero Event and the
// sockets get only the close. Caller holds the room's lock.
func (s *Service) goneElsewhereLocked(roomID string) {
	if s.onRoomDeleted == nil || !s.addWorker() {
		return
	}
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithTimeout(s.ctx, coordTimeout)
		deleted, _ := s.DeletedEvent(ctx, roomID)
		cancel()
		s.roomDeleted(roomID, deleted)
	}()
}

// refreshAll brings the whole cache up to the store: rooms other replicas
// opened are loaded, rooms that are behind are folded, rooms that are gone
// are evicted. Every read happens outside every lock (one ListRooms, then one
// Since per room that is behind and one rebuild per new room), and what each
// room needs is judged from what it last published, so only a room that has
// to be folded, evicted or inserted is locked: a cache that is current costs
// no lock at all.
func (s *Service) refreshAll(ctx context.Context) error {
	_, err := s.refresh(ctx)
	return err
}

// refresh is refreshAll. A room whose lock is busy (a command in flight, which
// may be stuck in the store) is not waited for: it is left as it is and
// returned in busy, for the follower to bring up to its row once it is free
// (trySyncRoom: the fold, the driver lease, or the eviction refresh skipped).
func (s *Service) refresh(ctx context.Context) (busy map[string]bool, err error) {
	if s.coord == nil {
		return nil, s.loadMissing(ctx)
	}
	busy = map[string]bool{}
	// tryLock locks st for refresh, or records it busy.
	tryLock := func(id string, st *roomState) bool {
		if !st.mu.TryLock() {
			busy[id] = true
			return false
		}
		if st.gone {
			st.mu.Unlock()
			return false
		}
		return true
	}
	// Only rooms cached before the list is read may be evicted for missing
	// from it: a room opened here meanwhile is not in the list yet.
	before := s.cachedRooms()
	rows, err := s.store.ListRooms(ctx)
	if err != nil {
		return nil, fmt.Errorf("room: refresh: %w", err)
	}
	type behind struct {
		st    *roomState
		after int64
	}
	type leaseRow struct {
		st  *roomState
		row store.Room
	}
	stale := map[string]behind{}
	var adopt []leaseRow
	var missing []store.Room
	seen := make(map[string]bool, len(rows))
	for _, r := range rows {
		seen[r.ID] = true
		st := s.cached(r.ID)
		if st == nil {
			if r.LastSeq > 0 {
				missing = append(missing, r)
			}
			continue
		}
		p := st.pub.Load()
		if p == nil {
			continue // being opened here; it publishes itself
		}
		if r.LastSeq > p.LastSeq {
			stale[r.ID] = behind{st: st, after: p.LastSeq}
		}
		if rowLeaseLater(p, r) {
			adopt = append(adopt, leaseRow{st, r})
		}
	}
	for id, st := range before {
		if seen[id] || !tryLock(id, st) {
			continue
		}
		s.evictLocked(id, st)
		s.unlockRoom(st)
	}
	for _, a := range adopt {
		if !tryLock(a.row.ID, a.st) {
			continue
		}
		s.adoptRowLeaseLocked(a.st, a.row)
		s.unlockRoom(a.st)
	}

	fetched := make(map[string][]events.Event, len(stale))
	for id, b := range stale {
		// A busy room is not even read: its command may be stuck in the
		// store, and the command folds what it finds when it appends.
		if !tryLock(id, b.st) {
			continue
		}
		s.unlockRoom(b.st)
		evs, err := s.store.Since(ctx, id, b.after, 1000)
		if err != nil {
			s.log.Warn("room: refresh: read events failed", "room", id, "err", err)
			continue
		}
		fetched[id] = evs
	}
	if s.isClosed() {
		return busy, nil
	}
	for id, evs := range fetched {
		b := stale[id]
		if !tryLock(id, b.st) {
			continue
		}
		s.foldFetchedLocked(ctx, id, b.st, evs)
		s.unlockRoom(b.st)
	}
	// New rooms are loaded outside every lock, like any load (loadRoom).
	for _, r := range missing {
		if _, _, err := s.loadRoom(ctx, r.ID, false); err != nil && !isNotFound(err) {
			s.log.Warn("room: refresh: rebuild failed", "room", r.ID, "err", err)
		}
	}
	return busy, nil
}

// loadMissing is refreshAll with a single-process store: it loads every room
// of the store that is not cached yet, each outside every lock (loadRows), so
// a current cache costs no lock.
func (s *Service) loadMissing(ctx context.Context) error {
	rows, err := s.store.ListRooms(ctx)
	if err != nil {
		return fmt.Errorf("room: load: %w", err)
	}
	return s.loadRows(ctx, rows, false, false)
}

// ---- alert dedup across replicas ----

// alertLockStripes is how many in-process alert key locks there are.
const alertLockStripes = 64

// newAlertLocks makes the in-process alert key stripes (Service.alertLocks).
func newAlertLocks() [alertLockStripes]chan struct{} {
	var l [alertLockStripes]chan struct{}
	for i := range l {
		l[i] = make(chan struct{}, 1)
	}
	return l
}

// lockAlertKey takes the lock on an alert index key, so two alerts with the
// same key at once open one room: first this process's (striped) lock, then,
// with a shared store, the cluster-wide one, so two replicas do not either.
// The unlock releases them in reverse order. Waiting for either ends with ctx.
// It returns a no-op unlock for no key.
func (s *Service) lockAlertKey(ctx context.Context, indexKey string) (func(), error) {
	if indexKey == "" {
		return func() {}, nil
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(indexKey))
	sem := s.alertLocks[h.Sum32()%alertLockStripes]
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("room: lock alert key: %w", ctx.Err())
	}
	release := func() { <-sem }
	if s.coord == nil {
		return release, nil
	}
	unlock, err := s.coord.LockKey(ctx, "alert:"+indexKey)
	if err != nil {
		release()
		return nil, fmt.Errorf("room: lock alert key: %w", err)
	}
	return func() {
		unlock()
		release()
	}, nil
}

// prepareAlertKeyRoom runs before OpenAlertRoom locks a room, with the alert
// key lock held: it folds what other replicas appended to the room this
// replica indexes for indexKey, and finds (and caches) the open room another
// replica opened for it, whose id it returns ("" when there is none). Every
// store read happens here, outside any room lock. No-op with a single-process
// store.
func (s *Service) prepareAlertKeyRoom(ctx context.Context, indexKey string) string {
	if s.coord == nil || indexKey == "" {
		return ""
	}
	known, _ := s.alertRoomFor(indexKey)
	if known != "" {
		s.syncRoom(ctx, known)
	}
	row, err := s.store.LastRoomByDedupKey(ctx, indexKey)
	if err != nil || row.Status != "open" || row.ID == known {
		return ""
	}
	if s.cached(row.ID) != nil {
		s.syncRoom(ctx, row.ID)
	} else {
		s.loadNewRoom(ctx, row.ID)
	}
	return row.ID
}

// openAlertRoomSharedLocked indexes here the open room another replica opened
// for indexKey (roomID, found by prepareAlertKeyRoom), whose own index this
// replica may not know yet. Caller holds the alert key lock.
func (s *Service) openAlertRoomSharedLocked(indexKey, roomID string) (string, bool) {
	if roomID == "" {
		return "", false
	}
	st := s.cached(roomID)
	if st == nil {
		return "", false
	}
	if p := st.pub.Load(); p == nil || p.Status != "open" {
		return "", false
	}
	s.setAlertKey(roomID, st, indexKey, true)
	return roomID, true
}

func isNotFound(err error) bool {
	var re *Error
	return errors.As(err, &re) && re.Code == "not_found"
}

// ---- conditional append (docs/design/replicas.md section 9.1) ----

// checkedAppendTries is how often a checked command re-reads the log and
// re-checks after another replica appended first, before it answers 409.
const checkedAppendTries = 3

// emitCheckedLocked appends an event whose rule (check) must still hold when
// the event lands. With a shared store the append is conditional on the
// snapshot's last seq: when another replica appended in between, the room
// catches up, check runs again, and the append is retried, at most
// checkedAppendTries times, then 409 conflict. A check that fails returns its
// error. With a single-process store, the room's lock already makes check and
// append one step. Caller holds st's lock.
func (s *Service) emitCheckedLocked(ctx context.Context, st *roomState, roomID string, check func() error, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	if s.coord == nil {
		if err := check(); err != nil {
			return events.Event{}, err
		}
		return s.emitLocked(ctx, st, roomID, typ, actor, payload)
	}
	for try := 0; try < checkedAppendTries; try++ {
		if err := check(); err != nil {
			return events.Event{}, err
		}
		e, err := s.coord.AppendIfLast(ctx, roomID, st.snap.LastSeq, typ, actor, payload)
		if err == nil {
			s.foldLocked(ctx, roomID, st, e, false)
			return e, nil
		}
		if !errors.Is(err, store.ErrSeqConflict) {
			return events.Event{}, fmt.Errorf("room: append %s: %w", typ, err)
		}
		s.catchUpLocked(ctx, roomID, st)
	}
	return events.Event{}, errConflict()
}

// syncRoom brings a cached room up to the log before a command, with every
// read made outside the room's lock (only the fold takes it): the room row,
// and the missing events when the row is ahead. A room another replica's
// retention deleted is evicted (the command then answers not found). A read
// error leaves the cache as it is; the command's own write fails on the same
// outage, and a checked command (emitCheckedLocked) catches up on its own
// conflict. GETs do not call it: they answer from the cache the follower
// keeps.
func (s *Service) syncRoom(ctx context.Context, roomID string) {
	s.syncRoomWith(ctx, roomID, false)
}

// trySyncRoom is syncRoom for a background loop: it never waits for the room's
// lock, and reports busy when the room was locked, so the loop can skip it.
func (s *Service) trySyncRoom(ctx context.Context, roomID string) (busy bool) {
	return s.syncRoomWith(ctx, roomID, true)
}

func (s *Service) syncRoomWith(ctx context.Context, roomID string, try bool) (busy bool) {
	if s.coord == nil {
		return false
	}
	lockCached := func() *roomState {
		if !try {
			return s.lockCachedRoom(roomID)
		}
		st, b := s.tryLockCachedRoom(roomID)
		busy = busy || b
		return st
	}
	lockState := func(st *roomState) bool {
		if !try {
			s.lockState(st)
			return true
		}
		if st.mu.TryLock() {
			return true
		}
		busy = true
		return false
	}
	row, err := s.store.GetRoom(ctx, roomID)
	if errors.Is(err, store.ErrNotFound) {
		if st := lockCached(); st != nil {
			s.evictLocked(roomID, st)
			s.unlockRoom(st)
		}
		return busy
	}
	if err != nil {
		s.log.Debug("room: sync: read room row failed; using the cached state", "room", roomID, "err", err)
		return false
	}
	st := lockCached()
	if st == nil {
		return busy
	}
	if st.stale.Load() {
		// Rebuilt before the command rather than folded: the snapshot
		// skipped events (fillGapLocked).
		s.unlockRoom(st)
		s.rebuildStale(ctx, roomID, st, row.LastSeq)
		return busy
	}
	s.adoptRowLeaseLocked(st, row)
	after := st.snap.LastSeq
	s.unlockRoom(st)
	for after < row.LastSeq {
		evs, err := s.store.Since(ctx, roomID, after, 1000)
		if err != nil || len(evs) == 0 {
			return busy
		}
		if !lockState(st) {
			return busy
		}
		// A room evicted or a service closed meanwhile folds nothing more, so
		// the next read would fetch the same page again: stop. So does a pass
		// that did not advance (a gap the catch-up could not fill), instead of
		// re-reading a full page until ctx ends.
		if st.gone || s.isClosed() {
			s.unlockRoom(st)
			return busy
		}
		before := after
		s.foldFetchedLocked(ctx, roomID, st, evs)
		after = st.snap.LastSeq
		s.unlockRoom(st)
		if len(evs) < 1000 || after <= before {
			return busy
		}
	}
	return busy
}

// syncProposalRoom is syncRoom for the room of a proposal (approve, reject).
func (s *Service) syncProposalRoom(ctx context.Context, proposalID string) {
	if s.coord == nil {
		return
	}
	if p, err := s.store.GetProposal(ctx, proposalID); err == nil {
		s.syncRoom(ctx, p.RoomID)
	}
}

// syncHandoffRoom is syncRoom for the room of a hand-off (accept).
func (s *Service) syncHandoffRoom(ctx context.Context, handoffID string) {
	if s.coord == nil {
		return
	}
	if h, err := s.store.GetHandoff(ctx, handoffID); err == nil {
		s.syncRoom(ctx, h.RoomID)
	}
}
