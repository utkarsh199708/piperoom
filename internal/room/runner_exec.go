package room

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"piperoom/internal/setup/protocol"
)

// ErrRunnerLost reports that the runner socket dropped while an op was in
// flight and the runner did not reconnect before the request ended. The op's
// outcome is unknown: it may have run. The room's executor records the step
// with outcome_unknown instead of a plain failure (docs/design/
// setup-agent.md §2.1, docs/api.md runner socket rules).
var ErrRunnerLost = errors.New("room: setup runner lost with an op in flight")

// Limits of the executor's memory of finished ops. A setup room runs tens of
// steps, so these caps are far above any real room and only bound a hostile
// or very long-lived runner.
const (
	maxKnownOps      = 512
	maxSettledSteps  = 1024
	runnerWriteLimit = 10 * time.Second
)

// RunnerResults is where the executor records outcomes no Execute call waits
// for any more: a done result the runner reported after its socket dropped
// (Resume) or after the caller's request ended. Each is recorded at most once
// per (proposal, step). Nil callbacks drop the outcome.
type RunnerResults struct {
	// Result records res as the step's result.
	Result func(ctx context.Context, roomID string, req protocol.OpRequest, res protocol.OpResult)
	// Unknown records that the step's outcome is unknown (the runner reported
	// it still running, or the request ended while the op was in flight).
	Unknown func(ctx context.Context, roomID string, req protocol.OpRequest)
}

// pendingOp is one sent OpRequest whose Execute call is still waiting.
type pendingOp struct {
	roomID string
	req    protocol.OpRequest
	ch     chan runnerOutcome
	resent bool
}

// runnerOutcome is what a waiting Execute receives: a result or the runner's
// refusal.
type runnerOutcome struct {
	res protocol.OpResult
	err error
}

// RunnerExecutor dispatches OpRequests to the runner attached to a room and
// reconciles its Result and Resume frames. It implements
// protocol.RunnerExecutor (docs/design/setup-agent.md §2.2):
//
//   - Execute sends one frame and waits for its result until ctx or
//     req.Deadline ends; without an attached runner it answers ErrNoRunner.
//   - A result reaches the waiting Execute exactly once; a duplicate, or a
//     result for an op this executor never sent, is ignored.
//   - A result that arrives after its Execute returned is recorded through
//     RunnerResults exactly once (the store's step transition is the second
//     guard).
//   - On Resume an op the runner reports running or done is never re-sent; a
//     done result is delivered or recorded, a running op without a waiter is
//     recorded as unknown; an op the runner never saw (not in Resume) is
//     re-sent at most once. An unknown op id or state in Resume changes
//     nothing.
//
// The executor is safe for concurrent use.
type RunnerExecutor struct {
	reg *RunnerRegistry
	out RunnerResults
	log *slog.Logger

	mu      sync.Mutex
	pending map[string]*pendingOp         // op id -> waiting Execute
	known   map[string]protocol.OpRequest // op id -> request whose Execute ended, for a late result
	order   []string                      // FIFO of known ids for maxKnownOps
	settled map[string]struct{}           // (proposal, step) already recorded
	settle  []string                      // FIFO of settled keys for maxSettledSteps
}

// NewRunnerExecutor builds an executor over the runner registry. out may be
// empty; log may be nil.
func NewRunnerExecutor(reg *RunnerRegistry, out RunnerResults, log *slog.Logger) *RunnerExecutor {
	if log == nil {
		log = slog.Default()
	}
	return &RunnerExecutor{
		reg:     reg,
		out:     out,
		log:     log,
		pending: map[string]*pendingOp{},
		known:   map[string]protocol.OpRequest{},
		settled: map[string]struct{}{},
	}
}

// Execute implements protocol.RunnerExecutor. It sends req to the runner
// attached to roomID and waits for its result, the runner's refusal, or the
// end of ctx (which may carry req.Deadline). A runner that is not attached at
// the call answers protocol.ErrNoRunner; one that drops while the op is in
// flight is waited out until ctx ends, because a reconnect can report the op
// done. When ctx ends first the error wraps ctx.Err() and, if no runner is
// attached, ErrRunnerLost.
func (e *RunnerExecutor) Execute(ctx context.Context, roomID string, req protocol.OpRequest) (protocol.OpResult, error) {
	if req.ID == "" {
		return protocol.OpResult{}, fmt.Errorf("room: runner execute %q: empty op id", req.Op)
	}
	if !req.Deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, req.Deadline)
		defer cancel()
	}
	p := &pendingOp{roomID: roomID, req: req, ch: make(chan runnerOutcome, 1)}
	e.mu.Lock()
	if _, dup := e.pending[req.ID]; dup {
		e.mu.Unlock()
		return protocol.OpResult{}, fmt.Errorf("room: runner execute %q: duplicate op id", req.Op)
	}
	e.pending[req.ID] = p
	e.mu.Unlock()

	if err := e.reg.WriteToRoom(ctx, roomID, protocol.Frame{Type: protocol.FrameOp, Op: &req}); err != nil {
		if errors.Is(err, protocol.ErrNoRunner) {
			e.forgetPending(req.ID, false)
			return protocol.OpResult{}, fmt.Errorf("room: runner execute %q: %w", req.Op, protocol.ErrNoRunner)
		}
		// The frame may or may not have reached the runner. Keep waiting: a
		// reconnect's Resume re-sends an op absent from it at most once.
		e.log.Warn("room: write op to runner failed; waiting for a reconnect",
			"room", roomID, "op", req.Op, "id", req.ID, "err", err)
	}

	select {
	case o := <-p.ch:
		return o.res, o.err
	case <-ctx.Done():
		// A result that raced the cancellation wins over it. DeliverResult
		// sends under e.mu, so holding it here makes the choice exactly once:
		// either the buffered result is taken, or the waiter is forgotten.
		e.mu.Lock()
		select {
		case o := <-p.ch:
			e.mu.Unlock()
			return o.res, o.err
		default:
		}
		if cur := e.pending[req.ID]; cur != nil {
			delete(e.pending, req.ID)
			e.rememberLocked(req)
		}
		e.mu.Unlock()
		if !e.reg.ConnectedForRoom(roomID) {
			return protocol.OpResult{}, fmt.Errorf("room: runner execute %q: %w: %w", req.Op, ErrRunnerLost, ctx.Err())
		}
		// Still attached: ask the runner to stop the op (best effort).
		e.cancelOp(roomID, req.ID)
		return protocol.OpResult{}, fmt.Errorf("room: runner execute %q: %w", req.Op, ctx.Err())
	}
}

// forgetPending removes the waiter of op id. keep remembers the request when
// its Execute ended without an outcome, so a result that arrives later is
// recorded once (remember=false when there was no runner to send it to).
func (e *RunnerExecutor) forgetPending(id string, keep bool) {
	e.mu.Lock()
	p := e.pending[id]
	delete(e.pending, id)
	if keep && p != nil {
		e.rememberLocked(p.req)
	}
	e.mu.Unlock()
}

// cancelOp asks the runner to stop op id. It uses its own short context: the
// caller's has just ended.
func (e *RunnerExecutor) cancelOp(roomID, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), runnerWriteLimit)
	defer cancel()
	if err := e.reg.WriteToRoom(ctx, roomID, protocol.Frame{Type: protocol.FrameCancel, Cancel: &protocol.Cancel{ID: id}}); err != nil {
		e.log.Debug("room: cancel write to runner failed", "room", roomID, "id", id, "err", err)
	}
}

// DeliverResult hands one FrameResult to the Execute call waiting for it, or
// records it as an orphan result when that call has already ended. A result
// for an op this executor never sent, and a duplicate of one already recorded,
// change nothing.
func (e *RunnerExecutor) DeliverResult(ctx context.Context, roomID string, res protocol.OpResult) {
	e.mu.Lock()
	p := e.pending[res.ID]
	if p != nil {
		// The channel is buffered (capacity one) and this is its only
		// sender, so the send never blocks and happens before any Execute
		// can decide to give up.
		delete(e.pending, res.ID)
		p.ch <- runnerOutcome{res: res}
		e.mu.Unlock()
		return
	}
	req, known := e.known[res.ID]
	if known {
		e.forgetKnownLocked(res.ID)
	}
	e.mu.Unlock()

	if !known {
		e.log.Info("room: runner result for an unknown op ignored", "room", roomID)
		return
	}
	if !e.markSettled(req) {
		return
	}
	if e.out.Result != nil {
		e.out.Result(ctx, roomID, req, res)
	}
}

// DeliverRefusal hands the runner's refusal to the Execute call waiting for
// the op. A duplicate_step refusal for an op this executor re-sent is not an
// outcome: the runner already had the op (it was started before the drop) and
// its real result follows, so the waiter stays. A refusal for an op nobody
// waits for changes nothing here: httpapi has already emitted
// setup.runner_refused for it.
func (e *RunnerExecutor) DeliverRefusal(ref protocol.Refusal) {
	e.mu.Lock()
	if p := e.pending[ref.ID]; p != nil && !(ref.Reason == protocol.RefuseDuplicateStep && p.resent) {
		// Same handshake as DeliverResult: send under e.mu so a waiter that
		// is giving up either takes this outcome or is already gone.
		delete(e.pending, ref.ID)
		e.forgetKnownLocked(ref.ID)
		p.ch <- runnerOutcome{err: &protocol.RefusedError{Refusal: ref}}
	}
	e.mu.Unlock()
}

// ReconcileResume applies the runner's first frame after a token reconnect
// (docs/design/setup-agent.md §2.2). A done op is delivered to its waiting
// Execute or, when that call is gone, recorded as the step's result exactly
// once; a running op with no waiter is recorded as unknown exactly once; an op
// absent from Resume that the server sent is re-sent at most once. A running
// or done op is never re-sent.
func (e *RunnerExecutor) ReconcileResume(ctx context.Context, roomID string, res protocol.Resume) {
	seen := make(map[string]bool, len(res.InFlight))
	for _, st := range res.InFlight {
		if st.ID == "" {
			continue
		}
		seen[st.ID] = true
		switch st.State {
		case protocol.StateDone:
			if st.Result == nil || st.Result.ID == "" {
				// Done without a result is not a usable outcome. Keep waiting
				// (a real result may follow) and never re-send.
				e.log.Warn("room: runner resume reports a done op without a result; ignored", "room", roomID)
				continue
			}
			e.DeliverResult(ctx, roomID, *st.Result)
		case protocol.StateRunning:
			e.markRunning(ctx, roomID, st.ID)
		default:
			// An unknown state is data from a newer peer: never acted on,
			// never re-sent.
			e.log.Info("room: runner resume with an unknown op state ignored", "room", roomID)
		}
	}

	e.mu.Lock()
	var resend []protocol.OpRequest
	for id, p := range e.pending {
		if p.roomID != roomID || seen[id] || p.resent {
			continue
		}
		p.resent = true
		resend = append(resend, p.req)
	}
	e.mu.Unlock()
	for _, req := range resend {
		req := req
		if err := e.reg.WriteToRoom(ctx, roomID, protocol.Frame{Type: protocol.FrameOp, Op: &req}); err != nil {
			// A later Resume may not come; the waiter still ends at its ctx.
			e.log.Warn("room: re-send of an unseen op failed", "room", roomID, "op", req.Op, "err", err)
		}
	}
}

// markRunning records a running op whose Execute call is gone as unknown,
// exactly once. While a waiter exists the op simply keeps running; Resume is
// not an outcome.
func (e *RunnerExecutor) markRunning(ctx context.Context, roomID, id string) {
	e.mu.Lock()
	p := e.pending[id]
	req, known := e.known[id]
	if known {
		e.forgetKnownLocked(id)
	}
	e.mu.Unlock()
	if p != nil || !known {
		return
	}
	if !e.markSettled(req) {
		return
	}
	if e.out.Unknown != nil {
		e.out.Unknown(ctx, roomID, req)
	}
}

// DropRoom ends every waiting Execute for roomID (a new pairing replaced the
// runner that held their ops, so it cannot report them). The errors wrap
// ErrRunnerLost: the ops' outcome is unknown.
func (e *RunnerExecutor) DropRoom(roomID string) {
	e.mu.Lock()
	for id, p := range e.pending {
		if p.roomID != roomID {
			continue
		}
		delete(e.pending, id)
		p.ch <- runnerOutcome{err: fmt.Errorf("room: runner execute %q: %w", p.req.Op, ErrRunnerLost)}
	}
	e.mu.Unlock()
}

// rememberLocked records a request whose Execute ended without an outcome.
// Caller holds e.mu.
func (e *RunnerExecutor) rememberLocked(req protocol.OpRequest) {
	if _, ok := e.known[req.ID]; ok {
		return
	}
	e.known[req.ID] = req
	e.order = append(e.order, req.ID)
	for len(e.order) > maxKnownOps {
		old := e.order[0]
		e.order = e.order[1:]
		delete(e.known, old)
	}
}

// forgetKnownLocked drops a request from the late-result memory. Caller holds
// e.mu.
func (e *RunnerExecutor) forgetKnownLocked(id string) {
	if _, ok := e.known[id]; !ok {
		return
	}
	delete(e.known, id)
	for i, v := range e.order {
		if v == id {
			e.order = append(e.order[:i], e.order[i+1:]...)
			break
		}
	}
}

// stepKey names one proposal step. T0 requests carry neither field and are
// never settled.
func stepKey(req protocol.OpRequest) (string, bool) {
	if req.ProposalID == "" || req.StepIndex == nil {
		return "", false
	}
	return fmt.Sprintf("%s\x00%d", req.ProposalID, *req.StepIndex), true
}

// markSettled reports whether the request's step has not been recorded yet and
// marks it now. It is the executor's exactly-once guard for orphan outcomes
// (the store's started->done transition is the second).
func (e *RunnerExecutor) markSettled(req protocol.OpRequest) bool {
	key, ok := stepKey(req)
	if !ok {
		return true
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, done := e.settled[key]; done {
		return false
	}
	e.settled[key] = struct{}{}
	e.settle = append(e.settle, key)
	for len(e.settle) > maxSettledSteps {
		old := e.settle[0]
		e.settle = e.settle[1:]
		delete(e.settled, old)
	}
	return true
}
