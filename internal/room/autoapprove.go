package room

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Auto-approve policy (P0 item 4, tasks/P2-solo-ceremony.md P2b-4).
//
// A pipeline's owners.yaml `auto_approve:` list names actions a proposal may
// be approved for without a person. When every step of a new agent proposal
// is on that list and its preview passed, the system appends
// proposal.scheduled {run_at = now + PIPEROOM_AUTO_APPROVE_DELAY}. Until
// run_at the driver or an owner of a required layer may cancel it
// (proposal.cancelled), and any person's approve or approve-all stops it.
// At run_at the policy appends one approval.given {grant: "policy"} per
// required layer and hands the proposal to the executor like the last
// approve; with write mode off the executor runs nothing (P0 item 1).
//
// No timer per proposal: the countdown lives in the log. The fold
// (foldScheduled, foldScheduleDecision) says whether a countdown is pending,
// and one loop per replica (runAutoApprove) looks for pending countdowns
// whose run_at has passed, like the driver lease check. So a restart reloads
// the countdown with the room, and with several replicas whichever replica
// gets there first fires it. The fire path is authoritative: under the
// room's lock it checks the room is open, the countdown pending (no
// proposal.cancelled, no person's approval after proposal.scheduled), due,
// the preview still good, the policy still covering every step and the
// stored proposal still hashing to the params_hash shown; each approval is a
// conditional append (emitCheckedLocked) of that same check, and the
// approvals row of each layer is written first (one decision per layer), so
// two replicas never both approve a layer.

// AutoApproveCheckEvery is how often the policy loop looks for countdowns
// that ran out. A fix runs at most this long after its run_at.
const AutoApproveCheckEvery = time.Second

// autoApproveCheckTimeout bounds one check: the reads and the appends.
const autoApproveCheckTimeout = 30 * time.Second

// maxCancelReason caps the reason typed with a cancel, in characters.
const maxCancelReason = 500

// errNotAutoApprovable is the schedule check's refusal: the proposal does
// not qualify for (or no longer needs) a policy countdown.
var errNotAutoApprovable = errors.New("room: proposal not eligible for the auto-approve policy")

// errScheduleGone is the fire check's refusal: the countdown was cancelled,
// stopped by a person, is not due, or the room or the proposal changed.
var errScheduleGone = errors.New("room: policy countdown no longer due")

// notScheduled is 409 not_scheduled (docs/api.md): no countdown of the
// proposal is pending.
func notScheduled(msg string) *Error {
	return &Error{Status: 409, Code: "not_scheduled", Msg: msg}
}

// isSchemaAction reports a schema_* action, which the policy never covers in
// this wave (tasks/P2-solo-ceremony.md, Out of scope). cmd/server refuses
// such an entry at start; this keeps the rule for a config that did not go
// through it.
func isSchemaAction(name string) bool { return strings.HasPrefix(name, "schema_") }

// policyActions is the set of actions the pipeline's auto_approve: list lets
// the policy approve: empty when the policy is off (PIPEROOM_AUTO_APPROVE_DELAY
// = 0), the pipeline unknown, or the list empty.
func (s *Service) policyActions(pipelineID string) map[string]bool {
	out := map[string]bool{}
	for _, a := range s.autoApproveFor(pipelineID) {
		out[a] = true
	}
	return out
}

// autoApproveFor is the room view's mode.auto_approve: the pipeline's
// auto_approve: list in file order, [] when none or when the policy is off;
// never nil.
func (s *Service) autoApproveFor(pipelineID string) []string {
	out := []string{}
	if s.autoDelay <= 0 || s.owners == nil {
		return out
	}
	p, ok := s.owners.Pipeline(pipelineID)
	if !ok {
		return out
	}
	for _, a := range p.AutoApproveList() {
		if !isSchemaAction(a) {
			out = append(out, a)
		}
	}
	return out
}

// soloFor is the room view's mode.solo: u may approve every layer of the
// pipeline's kind alone (events.LayersOf), as owner or backup by the rules
// approve applies (authorizeLayer), or is PIPEROOM_SOLO_USER. The driver's
// fallback on unowned layers does not count: it is a per-proposal right.
func (s *Service) soloFor(u User, pipelineID string) bool {
	if s.IsSolo(u.ID) {
		return true
	}
	layers := events.LayersOf(s.pipelineKind(pipelineID))
	if len(layers) == 0 {
		return false // fail closed, like approve-all: "every layer of nothing" is not solo
	}
	for _, l := range layers {
		if _, err := s.authorizeLayer(pipelineID, u, l, "", false); err != nil {
			return false
		}
	}
	return true
}

// previewAllOK reports whether the proposal's preview says every step would
// be accepted: one preview per step, none failed, and no dry run that said
// no (or was skipped: then it did not say yes).
func previewAllOK(p Proposal) bool {
	pv := p.Preview
	if pv == nil || len(pv.Steps) != len(p.Steps) || len(p.Steps) == 0 {
		return false
	}
	for i, sp := range pv.Steps {
		if sp.StepIndex != i || sp.Error != "" || sp.Preview == nil {
			return false
		}
		if sp.Preview.DryRun != nil && !sp.Preview.DryRun.OK {
			return false
		}
	}
	return true
}

// policyCovers reports whether every step's action is on the pipeline's
// auto_approve: list.
func (s *Service) policyCovers(pipelineID string, steps []events.ProposalStep) bool {
	acts := s.policyActions(pipelineID)
	if len(acts) == 0 || len(steps) == 0 {
		return false
	}
	for _, st := range steps {
		if !acts[st.Action] {
			return false
		}
	}
	return true
}

// scheduleCheckLocked is the rule for appending proposal.scheduled: the
// policy is on, write mode is on, the room is open and not a shadow room,
// the proposal is an open agent proposal with no countdown yet and no
// decision on any layer, its preview passed, and the policy covers every
// step. Caller holds st's lock.
func (s *Service) scheduleCheckLocked(st *roomState, proposalID string) error {
	if s.autoDelay <= 0 || s.writeOff {
		return errNotAutoApprovable
	}
	if !s.roomOpen(st) || st.deleting || st.snap.Shadow || s.proposalsDisabled(st) {
		return errNotAutoApprovable
	}
	p, ok := openProposal(st.snap, proposalID)
	if !ok || p.origin == events.ProposalOriginDriver || p.sched.set {
		return errNotAutoApprovable
	}
	for _, l := range p.RequiredLayers {
		if p.Approvals[l] != "pending" {
			return errNotAutoApprovable
		}
	}
	if !previewAllOK(p) || !s.policyCovers(st.snap.PipelineID, p.Steps) {
		return errNotAutoApprovable
	}
	return nil
}

// maybeScheduleLocked appends the proposal's proposal.scheduled when it
// qualifies (scheduleCheckLocked). previewProposal calls it right after
// proposal.previewed, so the countdown starts once the approvers can see
// what the fix would do. Caller holds st's lock.
func (s *Service) maybeScheduleLocked(ctx context.Context, st *roomState, roomID, proposalID string) {
	if s.scheduleCheckLocked(st, proposalID) != nil {
		return
	}
	runAt := s.now().UTC().Add(s.autoDelay)
	_, err := s.emitCheckedLocked(ctx, st, roomID, func() error {
		return s.scheduleCheckLocked(st, proposalID)
	}, events.ProposalScheduled, systemActor, events.ProposalScheduledPayload{
		ProposalID: proposalID, RunAt: runAt, Policy: events.PolicyAutoApprove, CancelUntil: runAt,
	})
	switch {
	case errors.Is(err, errNotAutoApprovable):
	case err != nil:
		// Nothing is scheduled: the proposal waits for people, as without
		// the policy.
		s.log.Warn("room: auto-approve: append proposal.scheduled failed; the proposal waits for manual approval", "room", roomID, "proposal", proposalID, "err", err)
	default:
		s.log.Info("room: auto-approve countdown started", "room", roomID, "proposal", proposalID, "run_at", runAt)
	}
}

// fireCheckLocked is the fire path's rule, run before the first approval
// and again as the condition of each one: the room is open, the proposal open
// with a pending countdown (not cancelled, no person's approval since) that
// is due, its preview still good and the policy still covering every step,
// and no layer decided otherwise than by the policy. It returns the proposal
// as folded. Caller holds st's lock.
func (s *Service) fireCheckLocked(st *roomState, proposalID string) (Proposal, error) {
	if !s.roomOpen(st) || st.deleting {
		return Proposal{}, errScheduleGone
	}
	p, ok := openProposal(st.snap, proposalID)
	// Fully approved: the policy is done; the executor (or write mode) has it.
	if !ok || !p.sched.pending() || fullyApproved(p) || s.now().Before(p.sched.runAt) {
		return Proposal{}, errScheduleGone
	}
	if !previewAllOK(p) || !s.policyCovers(st.snap.PipelineID, p.Steps) {
		return Proposal{}, errScheduleGone
	}
	for _, l := range p.RequiredLayers {
		if d := p.Approvals[l]; d != "pending" && d != "approved" {
			return Proposal{}, errScheduleGone
		}
	}
	return p, nil
}

// scheduleDue lists the proposals of a published snapshot whose countdown is
// pending and due, sorted.
func (s *Service) scheduleDue(snap *RoomSnapshot) []string {
	if snap.Status != "open" {
		return nil
	}
	now := s.now()
	var out []string
	for _, p := range snap.OpenProposals {
		if p.sched.pending() && !fullyApproved(p) && !now.Before(p.sched.runAt) {
			out = append(out, p.ProposalID)
		}
	}
	sort.Strings(out)
	return out
}

// startAutoApprove starts the policy loop once, after Load, when the policy
// is on. It stops when the service lifetime ends (Close).
func (s *Service) startAutoApprove() {
	if s.autoDelay <= 0 {
		return
	}
	s.autoOnce.Do(func() {
		if !s.addWorker() {
			return
		}
		newTicker := s.autoTicker
		if newTicker == nil {
			newTicker = func(d time.Duration) (<-chan time.Time, func()) {
				t := time.NewTicker(d)
				return t.C, t.Stop
			}
		}
		tick, stop := newTicker(AutoApproveCheckEvery)
		go func() {
			defer s.wg.Done()
			defer stop()
			s.runAutoApprove(tick)
		}()
	})
}

// runAutoApprove checks the countdowns at every tick until Close.
func (s *Service) runAutoApprove(tick <-chan time.Time) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-tick:
		}
		ctx, cancel := context.WithTimeout(s.ctx, autoApproveCheckTimeout)
		s.checkSchedules(ctx)
		cancel()
		if s.autoTickDone != nil {
			s.autoTickDone()
		}
	}
}

// checkSchedules fires every countdown that ran out. With a shared store a
// room whose agent lease another replica holds is left to that replica, as
// for the driver lease check; a room nobody holds is fired by whichever
// replica gets there first, and the approvals rows plus the conditional
// appends make sure each layer is approved once.
func (s *Service) checkSchedules(ctx context.Context) {
	if s.isClosed() || s.autoDelay <= 0 {
		return
	}
	due := map[string][]string{}
	for id, p := range s.publishedRooms() {
		if ids := s.scheduleDue(p); len(ids) > 0 {
			due[id] = ids
		}
	}
	if len(due) == 0 {
		return
	}
	rooms := make([]string, 0, len(due))
	for id := range due {
		rooms = append(rooms, id)
	}
	sort.Strings(rooms)

	var held map[string]store.RoomLease
	if s.coord != nil {
		cctx, cancel := context.WithTimeout(ctx, coordTimeout)
		leases, err := s.coord.RoomLeases(cctx)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				s.log.Warn("room: auto-approve: list agent leases failed; retry at the next check", "err", err)
			}
			return
		}
		held = leases
	}
	for _, id := range rooms {
		if ctx.Err() != nil {
			return
		}
		if l, ok := held[id]; ok && l.Owner != s.replica {
			continue // the replica holding the room fires it
		}
		if s.trySyncRoom(ctx, id) {
			continue
		}
		for _, pid := range due[id] {
			s.firePolicy(ctx, id, pid)
		}
	}
}

// firePolicy approves one due proposal by policy, every pending required
// layer in order, and queues it for the executor. A busy room, a failed read
// or an append that loses to another replica leaves it for the next check,
// which finds it done or carries on with the layers still pending.
func (s *Service) firePolicy(ctx context.Context, roomID, proposalID string) {
	if s.isClosed() {
		return
	}
	st, _ := s.tryLockCachedRoom(roomID)
	if st == nil {
		return
	}
	defer s.unlockRoom(st)
	prop, err := s.fireCheckLocked(st, proposalID)
	if err != nil {
		return
	}
	row, err := s.store.GetProposal(ctx, proposalID)
	if err != nil {
		s.log.Warn("room: auto-approve: read proposal failed; retry at the next check", "room", roomID, "proposal", proposalID, "err", err)
		return
	}
	if why := s.policyRowMismatch(roomID, st.snap.PipelineID, row, prop); why != "" {
		s.skipPolicyOnce(roomID, proposalID, why)
		return
	}
	a := approver{actor: systemActor, detail: events.GrantPolicy, check: func() error {
		_, err := s.fireCheckLocked(st, proposalID)
		return err
	}}
	n := 0
	for _, layer := range row.RequiredLayers {
		if approvalFor(st.snap, proposalID, layer) != "pending" {
			continue
		}
		if _, err := s.appendApprovalLocked(ctx, st, row, layer, a); err != nil {
			var re *Error
			if errors.Is(err, errScheduleGone) || (errors.As(err, &re) && re.Code == codeAlreadyDecided) {
				s.log.Info("room: auto-approve: countdown decided elsewhere; policy stops", "room", roomID, "proposal", proposalID, "layer", layer, "err", err)
				return
			}
			s.log.Warn("room: auto-approve: approval failed; retry at the next check", "room", roomID, "proposal", proposalID, "layer", layer, "err", err)
			return
		}
		n++
	}
	if allApproved(st.snap, proposalID) {
		s.log.Info("room: auto-approve policy approved the proposal", "room", roomID, "proposal", proposalID, "layers", n)
		s.enqueueExecutionLocked(st, row)
	}
}

// policyRowMismatch re-reads the stored proposal at fire time: it must be
// the room's, still open, carry the params_hash shown with the countdown,
// hash to it from its stored steps (what the executor runs), and its steps
// must still be covered by the policy. Any difference is the reason the
// policy does not approve; "" when there is none.
func (s *Service) policyRowMismatch(roomID, pipelineID string, row store.ProposalRow, p Proposal) string {
	if row.RoomID != roomID {
		return "proposal row belongs to another room"
	}
	if row.Status != "open" {
		return fmt.Sprintf("proposal row is %q, not open", row.Status)
	}
	if row.ParamsHash == "" || row.ParamsHash != p.ParamsHash {
		return "params_hash changed since the countdown started"
	}
	h, steps, err := rowParamsHash(row)
	if err != nil || h != row.ParamsHash {
		return "stored steps no longer hash to the params_hash"
	}
	if !s.policyCovers(pipelineID, steps) {
		return "the policy no longer covers every stored step"
	}
	return ""
}

// skipPolicyOnce logs, once per proposal and process, why a due countdown
// was not fired. The proposal stays open for manual approval.
func (s *Service) skipPolicyOnce(roomID, proposalID, why string) {
	s.autoSkipMu.Lock()
	seen := s.autoSkip[proposalID]
	if s.autoSkip == nil {
		s.autoSkip = map[string]bool{}
	}
	s.autoSkip[proposalID] = true
	s.autoSkipMu.Unlock()
	if !seen {
		s.log.Warn("room: auto-approve: countdown ran out but the policy does not approve; the proposal waits for manual approval", "room", roomID, "proposal", proposalID, "why", why)
	}
}

// CancelProposal is POST /api/proposals/{pid}/cancel (P0 item 4): the
// driver or an owner of any required layer stops the proposal's pending
// policy countdown with proposal.cancelled. The proposal stays open for
// manual approval. 409 not_scheduled when no countdown is pending (none was
// started, it was cancelled or stopped, or the policy already ran).
func (s *Service) CancelProposal(ctx context.Context, user User, proposalID, reason string) (events.Event, error) {
	if err := rejectUnstorable(reason); err != nil {
		return events.Event{}, err
	}
	if utf8.RuneCountInString(reason) > maxCancelReason {
		return events.Event{}, validation(fmt.Sprintf("reason is longer than %d characters", maxCancelReason))
	}
	s.syncProposalRoom(ctx, proposalID)
	row, st, err := s.lockProposalRoom(ctx, user, proposalID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	check := func() error {
		if !s.roomOpen(st) {
			return roomClosed("room is closed")
		}
		if err := s.mayCancelLocked(st, row, user); err != nil {
			return err
		}
		p, ok := openProposal(st.snap, proposalID)
		if !ok || !p.sched.cancellable() {
			return notScheduled("no auto-approve countdown of this proposal is pending")
		}
		return nil
	}
	if err := check(); err != nil {
		return events.Event{}, err
	}
	actor := s.humanActor(user)
	ev, err := s.emitCheckedLocked(ctx, st, row.RoomID, check, events.ProposalCancelled, actor, events.ProposalCancelledPayload{
		ProposalID: proposalID, By: actor, Reason: reason,
	})
	if err != nil {
		return events.Event{}, err
	}
	s.log.Info("room: auto-approve countdown cancelled", "room", row.RoomID, "proposal", proposalID, "user", user.ID)
	return ev, nil
}

// mayCancelLocked is the cancel rule: the room's driver, or a person who may
// approve one of the proposal's required layers (owner, backup, solo user).
// Caller holds st's lock.
func (s *Service) mayCancelLocked(st *roomState, row store.ProposalRow, user User) error {
	if s.isDriver(st, user.ID) {
		return nil
	}
	for _, l := range row.RequiredLayers {
		if _, err := s.authorizeLayer(st.snap.PipelineID, user, l, "", false); err == nil {
			return nil
		}
	}
	return notLayerOwner("only the room's driver or an owner of a required layer may cancel the countdown")
}
