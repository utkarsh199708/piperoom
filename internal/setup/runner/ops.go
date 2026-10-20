package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// errDuplicateStep marks the duplicate refusal internally; runOp turns it into
// a RefuseDuplicateStep refusal.
var errDuplicateStep = errors.New("setup: step already started")

// tierRank orders tiers T0..T3; an unknown tier string ranks as T0.
func tierRank(t string) int {
	switch t {
	case protocol.TierT3:
		return 3
	case protocol.TierT2:
		return 2
	case protocol.TierT1:
		return 1
	default:
		return 0
	}
}

// classRank orders classes local < shared < production; an unknown or empty
// class ranks as local, so it never raises a floor and never lowers one.
func classRank(c string) int {
	switch c {
	case protocol.ClassProduction:
		return 2
	case protocol.ClassShared:
		return 1
	default:
		return 0
	}
}

// maxClass returns the higher of two classes.
func maxClass(a, b string) string {
	if classRank(a) >= classRank(b) {
		return a
	}
	return b
}

// inflightOp is one op the runner is executing or has executed whose outcome
// the server may not have learned yet; Resume reports it.
type inflightOp struct {
	status protocol.OpStatus
	cancel context.CancelFunc
}

// applyTargets seeds (or raises) the runner's own class floors from the
// targets the server declared. A floor is only ever raised, never lowered, so
// a server that re-sends a lower class on a reconnect cannot move a production
// target back to shared.
func (r *Runner) applyTargets(targets []events.SetupTarget) {
	r.opMu.Lock()
	defer r.opMu.Unlock()
	for _, t := range targets {
		if t.Kind == "" {
			continue
		}
		r.floors[t.Kind] = maxClass(r.floors[t.Kind], t.Class)
	}
}

// raiseFloor raises the floor for one target kind; a lower incoming class is
// ignored. This is how a later wave applies setup.target_classed it is told of.
func (r *Runner) raiseFloor(kind, class string) {
	r.opMu.Lock()
	defer r.opMu.Unlock()
	r.floors[kind] = maxClass(r.floors[kind], class)
}

// effectiveClass raises req.Class to the runner's own floor for the target the
// op reaches, so a server that sends Class "shared" for a production target
// still meets the guard.
func (r *Runner) effectiveClass(req *protocol.OpRequest, target string) string {
	r.opMu.Lock()
	floor := r.floors[target]
	r.opMu.Unlock()
	return maxClass(req.Class, floor)
}

// tokenExpired reports whether the runner's token is past its idle expiry.
func (r *Runner) tokenExpired() bool {
	return !r.now().Before(r.tokenExpiry)
}

// storeToken records the token (never logged, never printed) and its idle
// expiry. The server's ExpiresAt is honoured but never kept longer than
// protocol.TokenIdle from now, so the runner's own clock bounds a token even
// against a server that lies.
func (r *Runner) storeToken(tok *protocol.Token) error {
	if tok.Token == "" {
		return errors.New("setup: empty token")
	}
	r.token = tok.Token
	exp := tok.ExpiresAt
	limit := r.now().Add(protocol.TokenIdle)
	if exp.IsZero() || exp.After(limit) {
		exp = limit
	}
	r.tokenExpiry = exp
	return nil
}

// setInflight registers an op as running (or updates an existing entry) and
// keeps its cancel func for a FrameCancel.
func (r *Runner) setInflight(st *protocol.OpStatus, cancel context.CancelFunc) {
	r.opMu.Lock()
	defer r.opMu.Unlock()
	if cur, ok := r.inflight[st.ID]; ok {
		cur.status.State = st.State
		return
	}
	r.inflight[st.ID] = &inflightOp{status: *st, cancel: cancel}
}

// markDone records the op's result; a failed write later leaves the done
// status in place so the next Resume carries the result.
func (r *Runner) markDone(id string, result *protocol.OpResult) {
	r.opMu.Lock()
	defer r.opMu.Unlock()
	if op, ok := r.inflight[id]; ok {
		op.status.State = protocol.StateDone
		op.status.Result = result
	}
}

// buildResume collects the ops the server may not have learned about: running
// ops stay (they may need reporting again), done ops are reported once and
// dropped. The list is sorted by ID for determinism.
func (r *Runner) buildResume() protocol.Resume {
	r.opMu.Lock()
	defer r.opMu.Unlock()
	res := protocol.Resume{InFlight: []protocol.OpStatus{}}
	for id, op := range r.inflight {
		res.InFlight = append(res.InFlight, op.status)
		if op.status.State == protocol.StateDone {
			delete(r.inflight, id)
		}
	}
	sort.Slice(res.InFlight, func(i, j int) bool { return res.InFlight[i].ID < res.InFlight[j].ID })
	return res
}

// runOp handles one FrameOp in its own goroutine: it applies the runner's
// safety checks (floor, proposal fields, guard, params_hash, duplicate, T3
// confirmation), executes through the catalogue, and reports Result or Refused.
func (r *Runner) runOp(ctx context.Context, req *protocol.OpRequest) {
	opCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if !req.Deadline.IsZero() && !r.now().Before(req.Deadline) {
		r.refuse(ctx, req, protocol.RefuseDeadline)
		return
	}
	if !req.Deadline.IsZero() {
		var dcancel context.CancelFunc
		opCtx, dcancel = context.WithDeadline(opCtx, req.Deadline)
		defer dcancel()
	}

	// Unknown op and the tier floor come first: nothing below the op's floor
	// runs, and an op the catalogue does not know is refused outright.
	floor, ok := r.catalogue.Floor(req.Op)
	if !ok {
		r.refuse(ctx, req, protocol.RefuseUnknownOp)
		return
	}
	if tierRank(req.Tier) < tierRank(floor) {
		r.refuse(ctx, req, protocol.RefuseTierBelowFloor)
		return
	}

	target := r.catalogue.Target(*req)
	eff := r.effectiveClass(req, target)
	t1 := tierRank(req.Tier) >= tierRank(protocol.TierT1)

	// T1+ requests must name the proposal step they execute.
	if t1 && (req.ProposalID == "" || req.StepIndex == nil || req.ParamsHash == "") {
		r.refuse(ctx, req, protocol.RefuseNoProposal)
		return
	}

	// The guard runs on the raised class before anything else, for every op.
	if r.catalogue.GuardForbidden(req.Op, eff) {
		r.refuse(ctx, req, protocol.RefuseGuard)
		return
	}

	if t1 {
		hash, err := r.proposalHash(req)
		if err != nil || hash != req.ParamsHash {
			r.refuse(ctx, req, protocol.RefuseParamsHash)
			return
		}

		// Every T3 op needs a y on the runner's terminal.
		if req.Tier == protocol.TierT3 {
			ok, err := r.confirmT3(ctx, req)
			if err != nil {
				return // ctx ended; the op does not run
			}
			if !ok {
				r.refuse(ctx, req, protocol.RefuseNotConfirmed)
				return
			}
		}

		if err := r.markStarted(req, target, eff, cancel); err != nil {
			// The only error markStarted returns is the duplicate refusal.
			if errors.Is(err, errDuplicateStep) {
				r.refuse(ctx, req, protocol.RefuseDuplicateStep)
				return
			}
			r.log.Error("marking op started failed", "op", req.Op, "err", err)
			return
		}
		r.printOp(req, target)
	} else {
		r.setInflight(&protocol.OpStatus{ID: req.ID, State: protocol.StateRunning}, cancel)
	}

	result, err := r.catalogue.Execute(opCtx, *req)
	result.ID = req.ID
	if err != nil {
		result.OK = false
		if result.Summary == "" {
			result.Summary = "setup: execution failed"
		}
		r.log.Debug("op failed", "op", req.Op, "id", req.ID)
	}
	r.markDone(req.ID, &result)

	if t1 {
		if lerr := r.logEntry(opLogEntry{
			ID: req.ID, Op: req.Op, Target: target, ProposalID: req.ProposalID,
			StepIndex: req.StepIndex, Tier: req.Tier, Class: eff,
			State: opLogDone, Result: &result, At: r.now(),
		}, false); lerr != nil {
			r.log.Error("op log write failed", "op", req.Op, "err", lerr)
		}
	}

	if werr := r.writeFrame(ctx, protocol.Frame{Type: protocol.FrameResult, Result: &result}); werr != nil {
		// The result stays in the in-flight set; the next Resume carries it.
		r.log.Debug("writing result failed", "op", req.Op, "id", req.ID, "err", werr)
	}
}

// proposalHash returns the params_hash the runner recomputes for req: over the
// frozen proposal it was shown (the first request of a proposal carries it, and
// the runner caches it for the later requests of the same proposal).
func (r *Runner) proposalHash(req *protocol.OpRequest) (string, error) {
	r.opMu.Lock()
	if len(req.Proposal) > 0 {
		var p events.ProposalCreatedPayload
		if err := json.Unmarshal(req.Proposal, &p); err != nil {
			r.opMu.Unlock()
			return "", fmt.Errorf("setup: decoding frozen proposal: %w", err)
		}
		if p.ProposalID != req.ProposalID {
			r.opMu.Unlock()
			return "", errors.New("setup: frozen proposal id does not match the request")
		}
		r.proposals[req.ProposalID] = p
	}
	p, ok := r.proposals[req.ProposalID]
	r.opMu.Unlock()
	if !ok {
		return "", errors.New("setup: no frozen proposal to recompute params_hash")
	}
	return ComputeParamsHash(r.roomID, p.ProposalID, p.Title, p.Risk, p.Rollback, p.RequiredLayers, p.Steps)
}

// markStarted checks the duplicate ledger and records the op as started before
// it runs. The duplicate set is in memory and, through the op log, on disk, so
// the refusal survives a restart.
func (r *Runner) markStarted(req *protocol.OpRequest, target, eff string, cancel context.CancelFunc) error {
	// Load the durable ledger before the duplicate check, so a step started by
	// a previous run of this runner is refused even after a restart.
	if err := r.ensureOpLog(); err != nil {
		return err
	}
	key := stepKey{proposalID: req.ProposalID, step: *req.StepIndex}
	r.opMu.Lock()
	if _, dup := r.started[key]; dup {
		r.opMu.Unlock()
		return errDuplicateStep
	}
	r.started[key] = struct{}{}
	r.opMu.Unlock()

	// The durable "started" marker is best effort: the in-memory set above
	// still protects against a duplicate within this process, and a disk
	// failure is logged rather than blocking every op.
	if err := r.logEntry(opLogEntry{
		ID: req.ID, Op: req.Op, Target: target, ProposalID: req.ProposalID,
		StepIndex: req.StepIndex, Tier: req.Tier, Class: eff,
		State: opLogStarted, At: r.now(),
	}, true); err != nil {
		r.log.Error("op log write failed; started marker is in memory only", "op", req.Op, "err", err)
	}
	r.setInflight(&protocol.OpStatus{
		ID: req.ID, ProposalID: req.ProposalID, StepIndex: req.StepIndex, State: protocol.StateRunning,
	}, cancel)
	return nil
}

// refuse reports a Refusal frame and appends it to the op log.
func (r *Runner) refuse(ctx context.Context, req *protocol.OpRequest, reason string) {
	target := r.catalogue.Target(*req)
	ref := &protocol.Refusal{ID: req.ID, Op: req.Op, Reason: reason, ProposalID: req.ProposalID, StepIndex: req.StepIndex}
	if err := r.writeFrame(ctx, protocol.Frame{Type: protocol.FrameRefused, Refused: ref}); err != nil {
		r.log.Debug("writing refusal failed", "op", req.Op, "reason", reason, "err", err)
	}
	if lerr := r.logEntry(opLogEntry{
		ID: req.ID, Op: req.Op, Target: target, ProposalID: req.ProposalID,
		StepIndex: req.StepIndex, Tier: req.Tier, Class: r.effectiveClass(req, target),
		State: opLogRefused, Reason: reason, At: r.now(),
	}, false); lerr != nil {
		r.log.Debug("op log write failed", "op", req.Op, "err", lerr)
	}
}

// logEntry appends one entry to the op log, opening it on first use. A write
// failure is returned to the caller; it never aborts the runner. After the
// runner has finished (closeOpLog), appends are no-ops.
func (r *Runner) logEntry(e opLogEntry, sync bool) error {
	if err := r.ensureOpLog(); err != nil {
		return err
	}
	r.opMu.Lock()
	defer r.opMu.Unlock()
	if r.opLog == nil {
		return nil
	}
	return r.opLog.append(e, sync)
}

// closeOpLog closes the op log once the runner is done; late appends from op
// goroutines that outlive Run are dropped, never re-opening the file.
func (r *Runner) closeOpLog() {
	r.opMu.Lock()
	defer r.opMu.Unlock()
	r.opLogDone = true
	if r.opLog != nil {
		_ = r.opLog.close()
		r.opLog = nil
	}
}

// ensureOpLog opens the op log and loads the durable duplicate set, once.
func (r *Runner) ensureOpLog() error {
	r.opMu.Lock()
	defer r.opMu.Unlock()
	if r.opLogDone || r.opLog != nil {
		return nil
	}
	l, err := openOpLog(r.projectDir)
	if err != nil {
		return err
	}
	keys, err := loadStarted(r.projectDir)
	if err != nil {
		_ = l.close()
		return err
	}
	for k := range keys {
		r.started[k] = struct{}{}
	}
	r.opLog = l
	return nil
}

// printOp prints every T1+ op on the runner's terminal (op, target, proposal
// id, step), a witness the person holds that does not depend on the server.
// The op, target and proposal id are server text and are scrubbed.
func (r *Runner) printOp(req *protocol.OpRequest, target string) {
	if tierRank(req.Tier) < tierRank(protocol.TierT1) {
		return
	}
	step := -1
	if req.StepIndex != nil {
		step = *req.StepIndex
	}
	fmt.Fprintf(r.stdout, "op %s target=%s proposal=%s step=%d\n",
		scrub(req.Op, maxPrintedFieldLen), scrub(target, maxPrintedFieldLen),
		scrub(req.ProposalID, maxPrintedFieldLen), step)
}

// confirmT3 asks for a y/N before a T3 op runs. "N" (or any answer other than
// y/yes) is RefuseNotConfirmed; a ctx that ends first means the op does not
// run at all.
func (r *Runner) confirmT3(ctx context.Context, req *protocol.OpRequest) (bool, error) {
	fmt.Fprintf(r.stdout, "op %s is T3; it runs only after your confirmation\n", scrub(req.Op, maxPrintedFieldLen))
	return r.ask(ctx, "confirm? [y/N] ")
}

// cancelOp cancels one in-flight op (a FrameCancel); the executor's context is
// cancelled and its result (OK false) is reported when it returns.
func (r *Runner) cancelOp(id string) {
	r.opMu.Lock()
	op := r.inflight[id]
	r.opMu.Unlock()
	if op != nil && op.cancel != nil {
		op.cancel()
	}
}
