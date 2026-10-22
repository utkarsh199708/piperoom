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

// refuseStartPersist is the reason the runner refuses a T1+ op whose durable
// "started" marker could not be written or synced: running it would break the
// exactly-once ledger, since the step could be replayed after a restart. This
// reason is not yet one of protocol.Refuse*; see the contract change request
// in the PR for adding it there.
const refuseStartPersist = "start_persist"

// refuseStoreUnavailable is the reason the runner refuses an op when the
// secret store cannot be opened: without the store there is no redaction set
// and no receipt key, so no OpResult could pass SanitizeResult or carry an
// OutMAC. Refusing keeps the invariant that every OpResult leaves the runner
// redacted and MAC'd; nothing is marked started, so the server may send the
// step again once the store is writable. Like refuseStartPersist, this reason
// is not yet one of protocol.Refuse*.
const refuseStoreUnavailable = "store_unavailable"

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
	r.setInflightLocked(st, cancel)
}

// setInflightLocked is setInflight with opMu already held.
func (r *Runner) setInflightLocked(st *protocol.OpStatus, cancel context.CancelFunc) {
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
// ops stay (they may need reporting again), done ops are reported once. It also
// returns the ids of the done ops it included; the caller deletes them only
// after the Resume is delivered, so a failed write keeps the results in place
// for the next Resume. The list is sorted by ID for determinism.
func (r *Runner) buildResume() (protocol.Resume, []string) {
	r.opMu.Lock()
	defer r.opMu.Unlock()
	res := protocol.Resume{InFlight: []protocol.OpStatus{}}
	var done []string
	for id, op := range r.inflight {
		res.InFlight = append(res.InFlight, op.status)
		if op.status.State == protocol.StateDone {
			done = append(done, id)
		}
	}
	sort.Slice(res.InFlight, func(i, j int) bool { return res.InFlight[i].ID < res.InFlight[j].ID })
	return res, done
}

// dropDone removes the done ops that a delivered Resume carried, and only
// those: a result that finished after the snapshot was taken stays in the
// in-flight set for the next Resume.
func (r *Runner) dropDone(ids []string) {
	if len(ids) == 0 {
		return
	}
	r.opMu.Lock()
	defer r.opMu.Unlock()
	for _, id := range ids {
		if op, ok := r.inflight[id]; ok && op.status.State == protocol.StateDone {
			delete(r.inflight, id)
		}
	}
}

// runOp handles one FrameOp in its own goroutine: it applies the runner's
// safety checks (floor, proposal fields, guard, params_hash, duplicate, T3
// confirmation), executes through the catalogue (or the runner's built-in
// secret_set), redacts the result literally under the secret store and mints
// its OutMAC, and reports Result or Refused.
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
	// runs, and an op neither the catalogue nor the runner knows is refused
	// outright.
	floor, ok := r.floorOf(req.Op)
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

	// A catalogue that can allow-list a request's target refuses a host
	// outside the room's targets (and malformed params) before the guard. A
	// built-in op is not in the catalogue, so it has no target to check.
	if tc, ok := r.catalogue.(TargetChecker); ok && !isBuiltinOp(req.Op) {
		if reason := tc.CheckTarget(*req); reason != "" {
			r.refuse(ctx, req, reason)
			return
		}
	}

	// The guard runs on the raised class before anything else, for every op.
	if r.catalogue.GuardForbidden(req.Op, eff) {
		r.refuse(ctx, req, protocol.RefuseGuard)
		return
	}

	// The secret store must be open before anything runs: every result is
	// redacted literally and carries an OutMAC under the store's key, and an
	// op whose output could not be redacted must not run at all. The check is
	// before markStarted, so a step is not burned on a store failure.
	store, err := r.secretStore()
	if err != nil {
		r.log.Error("secret store unavailable", "op", req.Op, "err", err)
		r.refuse(ctx, req, refuseStoreUnavailable)
		return
	}

	if t1 {
		hash, err := r.proposalHash(req)
		if err != nil || hash != req.ParamsHash {
			r.refuse(ctx, req, protocol.RefuseParamsHash)
			return
		}
		// A matching hash only proves the cached frozen proposal is the one
		// the approver saw; it does not say the request is asking to execute
		// that step. Bind the request to the frozen step before anything runs.
		if err := r.checkStep(req); err != nil {
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
			if errors.Is(err, errDuplicateStep) {
				r.refuse(ctx, req, protocol.RefuseDuplicateStep)
				return
			}
			// A start record that cannot be durably persisted means the op
			// must not run: without it a destructive step could run and later
			// be replayed after a restart. Refuse rather than execute.
			r.log.Error("marking op started failed", "op", req.Op, "err", err)
			r.refuse(ctx, req, refuseStartPersist)
			return
		}
		r.printOp(req, target)
	} else {
		r.setInflight(&protocol.OpStatus{ID: req.ID, State: protocol.StateRunning}, cancel)
	}

	// The secret store was opened above, before the step was marked started.
	result, err := r.executeOp(opCtx, *req)
	result.ID = req.ID
	if err != nil {
		result.OK = false
		if result.Summary == "" {
			result.Summary = "setup: execution failed"
		}
		r.log.Debug("op failed", "op", req.Op, "id", req.ID)
	}
	// Literal redaction before anything leaves the runner, then the receipt
	// MAC over exactly the bytes the server receives.
	result = store.SanitizeResult(result)
	result.Receipt.OutMAC = store.ResultMAC(result.OK, result.Summary, result.Data)
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

// executeOp runs one op: a built-in (secret_set) in the runner, everything
// else through the catalogue.
func (r *Runner) executeOp(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error) {
	if isBuiltinOp(req.Op) {
		return r.executeBuiltin(ctx, req)
	}
	return r.catalogue.Execute(ctx, req)
}

// frozenProposal returns the frozen proposal the runner cached for req,
// caching it from req.Proposal on the first request of a proposal. A later
// request may still carry a Proposal, but it is only accepted when its
// canonical hash matches the cached proposal; otherwise it is refused (the
// caller reports RefuseParamsHash) and the cache is left untouched, so a
// request cannot swap the frozen proposal — and with it the hash checkStep
// binds to — for a different one.
func (r *Runner) frozenProposal(req *protocol.OpRequest) (events.ProposalCreatedPayload, error) {
	r.opMu.Lock()
	defer r.opMu.Unlock()

	cached, ok := r.proposals[req.ProposalID]
	if !ok {
		if len(req.Proposal) == 0 {
			return events.ProposalCreatedPayload{}, errors.New("setup: no frozen proposal to recompute params_hash")
		}
		p, err := decodeProposal(req)
		if err != nil {
			return events.ProposalCreatedPayload{}, err
		}
		r.proposals[req.ProposalID] = p
		return p, nil
	}

	if len(req.Proposal) > 0 {
		p, err := decodeProposal(req)
		if err != nil {
			return events.ProposalCreatedPayload{}, err
		}
		same, err := r.sameProposal(cached, p)
		if err != nil {
			return events.ProposalCreatedPayload{}, err
		}
		if !same {
			return events.ProposalCreatedPayload{}, errors.New("setup: proposal differs from the frozen one")
		}
	}
	return cached, nil
}

// decodeProposal parses the frozen proposal a request carries and checks its
// id against the request's.
func decodeProposal(req *protocol.OpRequest) (events.ProposalCreatedPayload, error) {
	var p events.ProposalCreatedPayload
	if err := json.Unmarshal(req.Proposal, &p); err != nil {
		return events.ProposalCreatedPayload{}, fmt.Errorf("setup: decoding frozen proposal: %w", err)
	}
	if p.ProposalID != req.ProposalID {
		return events.ProposalCreatedPayload{}, errors.New("setup: frozen proposal id does not match the request")
	}
	return p, nil
}

// sameProposal reports whether two payloads have the same canonical hash — the
// params_hash the approver saw. Only the fields that hash covers matter; the
// rest (Origin, FallbackLayers, ParamsHash) do not change what a step
// executes, so they are ignored, exactly as params_hash ignores them.
func (r *Runner) sameProposal(a, b events.ProposalCreatedPayload) (bool, error) {
	ha, err := ComputeParamsHash(r.roomID, a.ProposalID, a.Title, a.Risk, a.Rollback, a.RequiredLayers, a.Steps)
	if err != nil {
		return false, err
	}
	hb, err := ComputeParamsHash(r.roomID, b.ProposalID, b.Title, b.Risk, b.Rollback, b.RequiredLayers, b.Steps)
	if err != nil {
		return false, err
	}
	return ha == hb, nil
}

// proposalHash returns the params_hash the runner recomputes for req: over the
// frozen proposal it was shown (the first request of a proposal carries it, and
// the runner caches it for the later requests of the same proposal).
func (r *Runner) proposalHash(req *protocol.OpRequest) (string, error) {
	p, err := r.frozenProposal(req)
	if err != nil {
		return "", err
	}
	return ComputeParamsHash(r.roomID, p.ProposalID, p.Title, p.Risk, p.Rollback, p.RequiredLayers, p.Steps)
}

// checkStep binds a T1+ request to the frozen proposal step it names: the step
// index must be in range, the op must be the catalogue's op for that step's
// action, and the params must be the step's params under the hash's own
// canonicalization. It returns an error the caller turns into RefuseParamsHash.
func (r *Runner) checkStep(req *protocol.OpRequest) error {
	p, err := r.frozenProposal(req)
	if err != nil {
		return err
	}
	step := *req.StepIndex
	if step < 0 || step >= len(p.Steps) {
		return errors.New("setup: step index outside the frozen proposal")
	}
	frozen := p.Steps[step]
	action, ok := r.catalogue.StepAction(req.Op)
	if !ok && isBuiltinOp(req.Op) {
		// secret_set is executed by the runner itself; its catalogue action
		// name is its op name.
		action, ok = req.Op, true
	}
	if !ok {
		return errors.New("setup: op is not in the catalogue")
	}
	if action != frozen.Action {
		return errors.New("setup: op does not execute the frozen step's action")
	}
	if !stepParamsEqual(req.Params, frozen.Params) {
		return errors.New("setup: params differ from the frozen step")
	}
	return nil
}

// stepParamsEqual reports whether the request's params are the same JSON as
// the frozen step's params under the canonicalization params_hash uses, so a
// request cannot run a step with different parameters while quoting the
// approved hash.
func stepParamsEqual(a, b json.RawMessage) bool {
	ca, err := canonicalParams(a)
	if err != nil {
		return false
	}
	cb, err := canonicalParams(b)
	if err != nil {
		return false
	}
	return string(ca) == string(cb)
}

// markStarted checks the duplicate ledger and records the op as started before
// it runs. The durable "started" marker is written and synced before the
// in-memory ledger is updated, so an op runs only once its start survives a
// restart; a write or sync failure fails the op (the caller refuses it) instead
// of running a step that could later be replayed.
func (r *Runner) markStarted(req *protocol.OpRequest, target, eff string, cancel context.CancelFunc) error {
	r.opMu.Lock()
	defer r.opMu.Unlock()

	// Load the durable ledger before the duplicate check, so a step started by
	// a previous run of this runner is refused even after a restart.
	if err := r.ensureOpLogLocked(); err != nil {
		return err
	}
	key := stepKey{proposalID: req.ProposalID, step: *req.StepIndex}
	if _, dup := r.started[key]; dup {
		return errDuplicateStep
	}
	if r.opLog == nil {
		return errors.New("setup: op log is closed")
	}

	// The durable marker comes first; the in-memory set below is only updated
	// after it is on disk.
	if err := r.opLog.append(opLogEntry{
		ID: req.ID, Op: req.Op, Target: target, ProposalID: req.ProposalID,
		StepIndex: req.StepIndex, Tier: req.Tier, Class: eff,
		State: opLogStarted, At: r.now(),
	}, true); err != nil {
		return fmt.Errorf("setup: persisting started marker: %w", err)
	}
	r.started[key] = struct{}{}

	r.setInflightLocked(&protocol.OpStatus{
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
	r.opMu.Lock()
	defer r.opMu.Unlock()
	if err := r.ensureOpLogLocked(); err != nil {
		return err
	}
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

// ensureOpLogLocked opens the op log and loads the durable duplicate set,
// once. opMu must be held.
func (r *Runner) ensureOpLogLocked() error {
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
