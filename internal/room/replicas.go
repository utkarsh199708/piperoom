package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// the agent (docs/design/replicas.md section 4.2). Caller holds s.mu.
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

// holdLeaseLocked records a lease this replica just took (at start) and primes
// the inbox. Caller holds s.mu.
func (s *Service) holdLeaseLocked(st *roomState, start time.Time) {
	st.leased = true
	st.leaseValid = start.Add(s.timing.leaseGrace())
	if !st.snap.Shadow {
		st.inbox.reset(st.pending)
	}
}

// dropLeaseLocked gives the room's lease back (deletes the row) when this
// replica holds it. Caller holds s.mu.
func (s *Service) dropLeaseLocked(roomID string, st *roomState) {
	if s.coord == nil || !st.leased {
		return
	}
	st.leased = false
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
// already, or will once the lease expires. Caller holds s.mu.
func (s *Service) loseLeaseLocked(roomID string, st *roomState, why string) {
	if !st.leased {
		return
	}
	st.leased = false
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
		s.mu.Lock()
		closed := s.closed
		if !closed {
			s.wg.Add(3)
		}
		s.mu.Unlock()
		if closed {
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
			s.runCoordination()
		}()
	})
}

// runCoordination renews this replica's leases, takes over expired ones and
// sweeps stale executor claims until the service closes.
func (s *Service) runCoordination() {
	renew := time.NewTicker(s.timing.LeaseRenew)
	defer renew.Stop()
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
		case <-renew.C:
			s.renewLeases()
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
	s.mu.Lock()
	var ids []string
	for id, st := range s.rooms {
		if st.leased {
			ids = append(ids, id)
		}
	}
	s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		st := s.rooms[id]
		if st == nil || !st.leased {
			continue
		}
		if ok[id] {
			st.leaseValid = start.Add(s.timing.leaseGrace())
		} else {
			s.loseLeaseLocked(id, st, "the lease expired or another replica took it")
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
		s.mu.Lock()
		for id, st := range s.rooms {
			if st.leased && now.After(st.leaseValid) {
				s.loseLeaseLocked(id, st, "no renew confirmed the lease in time")
			}
		}
		s.mu.Unlock()
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
		s.adoptRoom(l.RoomID, start)
	}
}

// adoptRoom decides what a replica that just took over a room's lease does
// (docs/design/replicas.md section 4.3): resume an agent that was working,
// pause one that was idle, finish a shadow run, or drop the lease of a closed
// room.
func (s *Service) adoptRoom(roomID string, start time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, 2*coordTimeout)
	defer cancel()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		s.log.Warn("room: adopt room: load failed; the lease will expire again", "room", roomID, "err", err)
		return
	}
	if st.leased && st.agentCancel != nil {
		return // already ours and running (a renew raced the scan)
	}
	s.holdLeaseLocked(st, start)
	switch {
	case s.closed:
		return
	case !s.roomOpen(st):
		s.dropLeaseLocked(roomID, st)
	case st.snap.Shadow:
		// A shadow run is one-shot: it never restarts (docs/design/shadow.md
		// 3.7); its row is marked interrupted like the Load sweep does.
		s.interruptShadowRunLocked(ctx, roomID, st)
		s.dropLeaseLocked(roomID, st)
	case agentWasWorking(st) || len(st.pending) > 0:
		if _, err := s.emitLocked(ctx, st, roomID, events.AgentMessage, systemActor, events.AgentMessagePayload{
			Text: "the server that ran this room's agent stopped; the agent resumes on another server",
		}); err != nil {
			s.log.Warn("room: adopt room: resume notice failed", "room", roomID, "err", err)
		}
		s.startAgentLocked(roomID, st.snap.Title, st.snap.PipelineID, s.rawAlert(ctx, roomID), st)
	default:
		if err := s.noteRestartLocked(ctx, roomID, st); err != nil {
			s.log.Warn("room: adopt room: pause notice failed", "room", roomID, "err", err)
		}
		s.dropLeaseLocked(roomID, st)
	}
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
// holds s.mu.
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
		ctx, cancel := context.WithTimeout(s.ctx, coordTimeout)
		ok, err := s.coord.InterruptProposal(ctx, sp.ProposalID, s.replica, s.timing.ClaimStale, s.timing.UnclaimedStale)
		cancel()
		if err != nil {
			s.log.Warn("room: interrupt stale proposal failed", "proposal", sp.ProposalID, "err", err)
			continue
		}
		if !ok {
			continue
		}
		s.log.Warn("room: proposal's executor stopped; outcome recorded as failed", "proposal", sp.ProposalID, "room", sp.RoomID, "claimed_by", sp.ClaimedBy)
		s.recordInterruption(sp)
	}
}

// recordInterruption writes the outcome of a proposal the sweep interrupted:
// every started step is finished as failed and one failing action.executed
// names the step that had not finished.
func (s *Service) recordInterruption(sp store.StaleProposal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := s.resultCtx()
	defer cancel()
	st, err := s.getStateLocked(ctx, sp.RoomID)
	if err != nil {
		s.log.Warn("room: record interruption: load room failed", "room", sp.RoomID, "err", err)
		return
	}
	var p Proposal
	found := false
	for _, op := range st.snap.OpenProposals {
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
	output := fmt.Sprintf("outcome unknown: the server running this proposal stopped before it recorded the result of step %d; check the target before you retry with a new proposal", next)
	if sp.ClaimedBy == "" {
		output = "the proposal was approved, but the server that should have run it stopped before it started; create a new proposal to retry"
	}
	for i := range p.Steps {
		s.finishClaimedStep(sp.ProposalID, i, output)
	}
	if _, err := s.emitLocked(ctx, st, sp.RoomID, events.ActionExecuted, executorActor, events.ActionExecutedPayload{
		ProposalID: sp.ProposalID, StepIndex: next, OK: false, ParamsHash: p.ParamsHash, Output: output,
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
	wake   chan struct{}
}

func newFollower() *follower {
	return &follower{dirty: map[string]int64{}, wake: make(chan struct{}, 1)}
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

func (f *follower) take() (map[string]int64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, r := f.dirty, f.resync
	f.dirty, f.resync = map[string]int64{}, false
	return d, r
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
		dirty, resync := s.follow.take()
		if resync {
			s.refreshAll()
		}
		for roomID, seq := range dirty {
			if s.ctx.Err() != nil {
				return
			}
			s.followRoom(roomID, seq)
		}
	}
}

// followRoom folds a room's events up to (at least) seq. The read happens
// outside s.mu; only the fold holds it.
func (s *Service) followRoom(roomID string, seq int64) {
	ctx, cancel := context.WithTimeout(s.ctx, coordTimeout)
	defer cancel()
	s.mu.Lock()
	st, cached := s.rooms[roomID]
	var after int64
	if cached {
		after = st.snap.LastSeq
	}
	if s.closed {
		s.mu.Unlock()
		return
	}
	if !cached {
		// A room another replica opened: load it so the alert index, the
		// room list and OpenRoomForPipeline know it. A purged room is not
		// found and stays out.
		if _, err := s.getStateLocked(ctx, roomID); err != nil && !isNotFound(err) {
			s.log.Debug("room: follow: load new room failed", "room", roomID, "err", err)
		}
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	if seq <= after {
		return // our own append, already folded
	}
	evs, err := s.store.Since(ctx, roomID, after, 1000)
	if err != nil {
		if s.ctx.Err() == nil {
			s.log.Warn("room: follow: read events failed", "room", roomID, "err", err)
		}
		return
	}
	s.mu.Lock()
	if s.rooms[roomID] == st && !s.closed {
		s.foldFetchedLocked(ctx, roomID, st, evs)
	}
	s.mu.Unlock()
	if len(evs) == 1000 {
		s.follow.noteAppend(roomID, seq) // more to read
	}
}

// foldFetchedLocked folds events read outside the lock, skipping those the
// snapshot already has. If they do not continue the snapshot (it moved on and
// back is impossible; a gap means a newer read is needed) the room catches up
// from the store instead. Caller holds s.mu.
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

// syncLocked brings a cached room up to the log before a request uses it:
// events another replica appended are folded, a room another replica's
// retention deleted is evicted (not found), and the driver lease persisted on
// the rooms row is taken over. A store error leaves the cached state in use
// (the command's own append would fail on the same outage). Caller holds s.mu.
func (s *Service) syncLocked(ctx context.Context, roomID string, st *roomState) error {
	if s.coord == nil {
		return nil
	}
	row, err := s.store.GetRoom(ctx, roomID)
	if errors.Is(err, store.ErrNotFound) {
		s.evictLocked(roomID, st)
		return notFound("not found")
	}
	if err != nil {
		s.log.Debug("room: sync: read room row failed; using the cached state", "room", roomID, "err", err)
		return nil
	}
	if row.LastSeq > st.snap.LastSeq {
		s.catchUpLocked(ctx, roomID, st)
	}
	s.adoptRowLeaseLocked(st, row)
	return nil
}

// adoptRowLeaseLocked takes the driver lease from the rooms row when it is
// later than the snapshot's: renewals are persisted there and only some are
// logged. Caller holds s.mu.
func (s *Service) adoptRowLeaseLocked(st *roomState, row store.Room) {
	d := st.snap.Driver
	if d == nil || row.DriverID == "" || row.DriverID != d.ID || row.DriverLeaseUntil == nil {
		return
	}
	if row.DriverLeaseUntil.After(d.LeaseUntil) {
		nd := *d
		nd.LeaseUntil = row.DriverLeaseUntil.UTC()
		st.snap.Driver = &nd
	}
}

// catchUpLocked folds every event after the snapshot. Caller holds s.mu.
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
// room, this replica stops what it runs for the room. Caller holds s.mu.
func (s *Service) foldLocked(ctx context.Context, roomID string, st *roomState, e events.Event, remote bool) {
	wasOpen := s.roomOpen(st)
	st.snap = Apply(st.snap, e)
	trackPending(st, e)
	s.feedInbox(st, e)
	if remote && wasOpen && !s.roomOpen(st) {
		s.closedElsewhereLocked(ctx, roomID, st)
	}
}

// trackPending keeps the human input logged after the agent's last own event.
// A status is not counted as the agent's: the service writes "queued" and
// "working" for it, and a run may die between draining a steer and acting.
func trackPending(st *roomState, e events.Event) {
	if e.Actor == events.AgentActor {
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
// hook (asynchronously: the hook must not run under s.mu). Caller holds s.mu.
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
	if s.onRoomClosed != nil && !s.closed {
		pipelineID, key := st.snap.PipelineID, hookDedupKey(st.snap.PipelineID, st.dedupKey)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.onRoomClosed(s.ctx, roomID, pipelineID, key)
		}()
	}
}

// evictLocked forgets a room that is gone from the store. Caller holds s.mu.
func (s *Service) evictLocked(roomID string, st *roomState) {
	if st.agentCancel != nil {
		st.agentCancel()
		st.agentCancel = nil
	}
	if st.execCancel != nil {
		st.execCancel()
	}
	st.leased = false
	if st.dedupKey != "" && s.alertRooms[st.dedupKey] == roomID {
		delete(s.alertRooms, st.dedupKey)
	}
	delete(s.rooms, roomID)
}

// refreshAll brings the whole cache up to the store (after a LISTEN gap, and
// every poll interval while LISTEN is down).
func (s *Service) refreshAll() {
	ctx, cancel := context.WithTimeout(s.ctx, 2*coordTimeout)
	defer cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if err := s.refreshAllLocked(ctx); err != nil && s.ctx.Err() == nil {
		s.log.Warn("room: refresh the room cache failed", "err", err)
	}
}

// refreshAllLocked loads rooms other replicas opened, folds cached rooms that
// are behind, and evicts rooms that are gone, from one ListRooms read. Caller
// holds s.mu.
func (s *Service) refreshAllLocked(ctx context.Context) error {
	rows, err := s.store.ListRooms(ctx)
	if err != nil {
		return fmt.Errorf("room: refresh: %w", err)
	}
	seen := make(map[string]bool, len(rows))
	for _, r := range rows {
		seen[r.ID] = true
		st, ok := s.rooms[r.ID]
		if !ok {
			continue
		}
		if r.LastSeq > st.snap.LastSeq {
			s.catchUpLocked(ctx, r.ID, st)
		}
		s.adoptRowLeaseLocked(st, r)
	}
	for id, st := range s.rooms {
		if !seen[id] {
			s.evictLocked(id, st)
		}
	}
	return s.loadRowsLocked(ctx, rows, false)
}

// refreshCacheLocked makes the cache whole before a read of every room: it
// loads rooms not cached yet and, with a shared store, also folds rooms that
// are behind and evicts rooms that are gone. Caller holds s.mu.
func (s *Service) refreshCacheLocked(ctx context.Context) error {
	if s.coord == nil {
		return s.ensureAllLoadedLocked(ctx, false)
	}
	return s.refreshAllLocked(ctx)
}

// ---- alert dedup across replicas ----

// lockAlertKey takes the cluster-wide lock on an alert index key, so two
// replicas that get the same alert at once open one room. It returns a no-op
// unlock with a single-process store or no key.
func (s *Service) lockAlertKey(ctx context.Context, indexKey string) (func(), error) {
	if s.coord == nil || indexKey == "" {
		return func() {}, nil
	}
	unlock, err := s.coord.LockKey(ctx, "alert:"+indexKey)
	if err != nil {
		return nil, fmt.Errorf("room: lock alert key: %w", err)
	}
	return unlock, nil
}

// openAlertRoomSharedLocked finds the open room for indexKey that another
// replica opened (its own index may not know it yet) and indexes it here.
// Caller holds s.mu and the alert key lock.
func (s *Service) openAlertRoomSharedLocked(ctx context.Context, indexKey string) (string, bool) {
	if s.coord == nil || indexKey == "" {
		return "", false
	}
	row, err := s.store.LastRoomByDedupKey(ctx, indexKey)
	if err != nil || row.Status != "open" {
		return "", false
	}
	st, err := s.getStateLocked(ctx, row.ID)
	if err != nil || !s.roomOpen(st) {
		return "", false
	}
	st.dedupKey = indexKey
	s.alertRooms[indexKey] = row.ID
	return row.ID, true
}

func isNotFound(err error) bool {
	var re *Error
	return errors.As(err, &re) && re.Code == "not_found"
}
