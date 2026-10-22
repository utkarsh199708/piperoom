package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// Counted verification of a recovery (X2c). A proposal with a recovery step
// (actions.ClassRecovery: pg_resnapshot_rows, kafka_replay_range) re-emits
// data a fault lost, so new rows at the sink (the F4 data-flow check) say
// nothing about it. The step's own Verify says how much of the gap it set out
// to repair is still open. The agent calls it on the actions.RecoveryVerify*
// schedule (first 15 s after the step, the wait doubling up to 60 s, until
// After is 0 or 10 min after the step), writes one agent.recovery_check per
// counted call and one agent.verification with the outcome at the end, and
// posts one message citing the numbers. No model call is made for the count:
// the model reads the verdict as a [verification] note, like an F4 verdict.
//
// Layer checks still run for the proposal's fix steps (a connector restart
// next to a replay), and the proposal is recovered only when they pass and
// the outcome is events.RecoveryRecovered.

// recoveryStatusDetail is the status detail while the counts are polled.
const recoveryStatusDetail = "checking recovery counts"

// recoveryStep is one recovery step of a proposal and what its polls found.
type recoveryStep struct {
	index int
	step  events.ProposalStep
	v     actions.Verifier
	// last is the newest counted call; nil until one succeeded.
	last *actions.VerifyResult
	// unsupported is set when Verify answered an error no later poll can
	// cure (cannotCount): the source or sink cannot count, or the call is
	// refused, so the outcome is unverifiable.
	unsupported bool
	// lastErr is the newest failed call's error, scrubbed, for the evidence.
	lastErr string
}

// done reports whether the step needs no more polls.
func (s *recoveryStep) done() bool {
	return s.unsupported || (s.last != nil && s.last.After == 0)
}

// recoverySteps returns the steps of a proposal whose action is a recovery
// action of the run's registry, in step order. None means a fix proposal.
func (r *runner) recoverySteps(steps []events.ProposalStep) []*recoveryStep {
	if r.deps.Actions == nil {
		return nil
	}
	var out []*recoveryStep
	for i, s := range steps {
		a, ok := r.deps.Actions.Get(s.Action)
		if !ok || actions.ClassOf(a) != actions.ClassRecovery {
			continue
		}
		// Registry.Register refuses a recovery action without Verifier.
		if v, ok := a.(actions.Verifier); ok {
			out = append(out, &recoveryStep{index: i, step: s, v: v})
		}
	}
	return out
}

// nowTime is the runner's clock: a test clock that also tells the time
// drives the poll schedule; the real clock is time.Now.
func (r *runner) nowTime() time.Time {
	if c, ok := r.clk.(interface{ Now() time.Time }); ok {
		return c.Now()
	}
	return time.Now()
}

// executedAt is the time the last step ran, which Verify counts from: the
// action.executed event's time (HumanInput.At), else the time the agent first
// read the result (never earlier), else now. A time past the agent's clock
// (another replica's clock ahead of this one) is now, so the first poll is
// never pushed later than the schedule says.
func (r *runner) executedAt(in HumanInput, id string, step int) time.Time {
	now := r.nowTime()
	if !in.At.IsZero() {
		return minTime(in.At, now)
	}
	if at, ok := r.proposals.seenAt(id, step); ok {
		return minTime(at, now)
	}
	return now
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// recoveryPlan returns the times of the polls of one recovery verification:
// first after first, then the wait doubling up to maxWait, every one at most
// timeout after executedAt. Polls already due at now (the agent saw the
// result late) fold into one poll at now, so a late start never bursts; a
// plan always has at least one poll.
func recoveryPlan(executedAt, now time.Time, first, maxWait, timeout time.Duration) []time.Time {
	deadline := executedAt.Add(timeout)
	var due []time.Time
	if first > 0 {
		t, wait := executedAt.Add(first), first
		for !t.After(deadline) {
			due = append(due, t)
			// A zero cap would never move t: stop rather than spin.
			if wait = min(wait*2, maxWait); wait <= 0 {
				break
			}
			t = t.Add(wait)
		}
	}
	past := 0
	for past < len(due) && !due[past].After(now) {
		past++
	}
	if past > 0 || len(due) == 0 {
		due = append([]time.Time{now}, due[past:]...)
	}
	return due
}

// actionScope scopes ctx for an action's Verify. Actions need the room's
// whole pipeline (tools.WithPipeline), which only the room service resolves
// from owners.yaml: a run ctx that carries the alert's own pipeline keeps it.
// Anything else gets the alert's pipeline id only, which Verify refuses
// (tools.ErrNoScope): another pipeline's scope is never used.
func (r *runner) actionScope(ctx context.Context) context.Context {
	if p, ok := tools.PipelineFrom(ctx); ok && p.ID != "" && p.ID == r.alert.PipelineID {
		return ctx
	}
	return r.toolScope(ctx)
}

// callVerify makes one Verify call under the room's pipeline scope and the
// tool deadline. The error is scrubbed of secrets; a result that breaks the
// Verifier's rules (After outside [0, Before], an unknown unit) is an error,
// never a count.
func (r *runner) callVerify(ctx context.Context, st *recoveryStep, executedAt time.Time) (res actions.VerifyResult, err error) {
	vctx, cancel := context.WithTimeout(r.actionScope(ctx), r.timing.toolTimeout)
	defer cancel()
	defer func() {
		if p := recover(); p != nil {
			res, err = actions.VerifyResult{}, fmt.Errorf("agent: verify %s panicked: %v", st.step.Action, scrubPanic(p))
		}
	}()
	res, err = st.v.Verify(vctx, st.step.Params, executedAt)
	if err != nil {
		return actions.VerifyResult{}, r.scrubToolError(st.step.Action, err)
	}
	switch {
	case res.Before < 0 || res.After < 0 || res.After > res.Before:
		return actions.VerifyResult{}, fmt.Errorf("agent: verify %s: counts out of range (before %d, after %d)", st.step.Action, res.Before, res.After)
	case res.Unit != events.RecoveryUnitRows && res.Unit != events.RecoveryUnitRecords:
		return actions.VerifyResult{}, fmt.Errorf("agent: verify %s: unknown unit %q", st.step.Action, clipLine(res.Unit))
	}
	return res, nil
}

// pollRecovery polls every step's Verify on the plan until each step counts
// After 0, one answers clients.ErrNotSupported, or the plan ends. It returns
// one evidence line per call and ctx's error when ctx ended first; it starts
// no goroutine, and every wait ends with ctx.
func (r *runner) pollRecovery(ctx context.Context, id string, steps []*recoveryStep, executedAt time.Time) ([]string, error) {
	plan := recoveryPlan(executedAt, r.nowTime(), r.timing.recoveryFirstPoll, r.timing.recoveryMaxPoll, r.timing.recoveryTimeout)
	deadline := executedAt.Add(r.timing.recoveryTimeout)
	var lines []string
	for n, due := range plan {
		if n > 0 && r.nowTime().After(deadline) {
			// Slow calls pushed the plan past the timeout.
			lines = append(lines, "stopped: "+r.timing.recoveryTimeout.String()+" passed since the step")
			break
		}
		if d := due.Sub(r.nowTime()); d > 0 {
			if err := r.clk.Sleep(ctx, d); err != nil {
				return lines, err
			}
		}
		poll := n + 1
		for _, st := range steps {
			if st.done() {
				continue
			}
			res, err := r.callVerify(ctx, st, executedAt)
			if cerr := ctx.Err(); cerr != nil {
				return lines, cerr
			}
			head := fmt.Sprintf("poll %d of %d, step %d (%s): ", poll, len(plan), st.index+1, st.step.Action)
			if err != nil {
				st.lastErr = err.Error()
				if cannotCount(err) {
					st.unsupported = true
					lines = append(lines, head+"cannot be counted: "+clipLine(st.lastErr))
					return lines, nil
				}
				lines = append(lines, head+"error: "+clipLine(st.lastErr))
				continue
			}
			st.last = &res
			lines = append(lines, head+countLine(res))
			r.emit(ctx, events.AgentRecoveryCheck, events.AgentRecoveryCheckPayload{
				ProposalID: id, StepIndex: st.index, Poll: poll, MaxPolls: len(plan),
				Before: res.Before, After: res.After, Unit: res.Unit,
			})
		}
		if allDone(steps) {
			break
		}
	}
	return lines, nil
}

// cannotCount reports a Verify error that polling again cannot cure: the
// source or sink cannot count (clients.ErrNotSupported), or the call is
// refused before any I/O (no or another pipeline's scope, bad params). Such a
// step is unverifiable at once instead of after the whole timeout, during
// which the run would read nothing else.
func cannotCount(err error) bool {
	return errors.Is(err, clients.ErrNotSupported) || errors.Is(err, tools.ErrNoScope) ||
		errors.Is(err, tools.ErrOutOfScope) || errors.Is(err, tools.ErrBadInput)
}

// countLine renders one counted call for the evidence.
func countLine(res actions.VerifyResult) string {
	line := fmt.Sprintf("before %d %s, after %d", res.Before, res.Unit, res.After)
	if res.Detail != "" {
		line += "; " + clipLine(res.Detail)
	}
	return line
}

func allDone(steps []*recoveryStep) bool {
	for _, st := range steps {
		if !st.done() {
			return false
		}
	}
	return true
}

// recoveryVerdict is the outcome of a finished poll loop over every step.
type recoveryVerdict struct {
	outcome       string
	before, after int64
	unit          string
}

// counted reports whether the verdict carries counts (every outcome but
// unverifiable).
func (v recoveryVerdict) counted() bool { return v.outcome != events.RecoveryUnverifiable }

// recoveryOutcome turns the steps' last counts into one outcome. A step that
// cannot count, or that never counted before the timeout, makes the whole
// proposal unverifiable. Steps of one unit are summed and judged by
// events.RecoveryOutcomeFor; steps of different units (rows next to records)
// cannot be summed, so the worst step's outcome and counts stand for the
// proposal.
func recoveryOutcome(steps []*recoveryStep) recoveryVerdict {
	for _, st := range steps {
		if st.unsupported || st.last == nil {
			return recoveryVerdict{outcome: events.RecoveryUnverifiable}
		}
	}
	unit := steps[0].last.Unit
	same := true
	var before, after int64
	for _, st := range steps {
		same = same && st.last.Unit == unit
		before += st.last.Before
		after += st.last.After
	}
	if same {
		return recoveryVerdict{outcome: events.RecoveryOutcomeFor(before, after), before: before, after: after, unit: unit}
	}
	rank := map[string]int{events.RecoveryRecovered: 0, events.RecoveryPartial: 1, events.RecoveryNotRecovered: 2}
	var worst recoveryVerdict
	for i, st := range steps {
		v := recoveryVerdict{outcome: events.RecoveryOutcomeFor(st.last.Before, st.last.After), before: st.last.Before, after: st.last.After, unit: st.last.Unit}
		if i == 0 || rank[v.outcome] > rank[worst.outcome] {
			worst = v
		}
	}
	return worst
}

// recoveryHead is the verdict line (agent.verification.verdict and the head
// of the model's note). Only a recovered outcome with every layer check
// passing starts with "recovered:"; it holds counts and fixed words only.
func recoveryHead(v recoveryVerdict, layersOK bool, timeout time.Duration) string {
	switch {
	case !v.counted():
		return "NOT recovered (unverifiable): the recovery could not be counted (the source or sink cannot count it, or no count succeeded in " + timeout.String() + "), so it is not verified.\n"
	case !layersOK:
		return fmt.Sprintf("NOT recovered: a layer check did not pass (the counts: %d %s missing before, %d after).\n", v.before, v.unit, v.after)
	case v.outcome == events.RecoveryRecovered:
		return fmt.Sprintf("recovered: the recovery count reached 0: %d %s were missing before the step, 0 are missing now.\n", v.before, v.unit)
	case v.outcome == events.RecoveryPartial:
		return fmt.Sprintf("NOT recovered (partial): %d %s were missing before the step and %d are still missing after %s.\n", v.before, v.unit, v.after, timeout)
	default:
		return fmt.Sprintf("NOT recovered (not_recovered): %d %s were missing before the step and %d are still missing after %s.\n", v.before, v.unit, v.after, timeout)
	}
}

// recoveryMessage is the room message citing the numbers, for people. It
// never says the data is back unless the proposal is recovered.
func recoveryMessage(v recoveryVerdict, layersOK bool, timeout time.Duration) string {
	var msg string
	switch {
	case !v.counted():
		msg = "Recovery check: the counts could not be read (the source or sink cannot count them, or every count failed), so I cannot say whether the data is back. This is NOT verified as recovered."
	case v.outcome == events.RecoveryRecovered:
		msg = fmt.Sprintf("Recovery check: %d %s were missing before the recovery step and 0 are missing now.", v.before, v.unit)
		if layersOK {
			msg += " The data is back."
		}
	case v.outcome == events.RecoveryPartial:
		msg = fmt.Sprintf("Recovery check: %d %s were missing before the recovery step and %d are still missing after %s. Only part of the data is back: NOT recovered.", v.before, v.unit, v.after, timeout)
	default:
		msg = fmt.Sprintf("Recovery check: %d %s were missing before the recovery step and %d are still missing after %s. The gap did not close: NOT recovered.", v.before, v.unit, v.after, timeout)
	}
	if v.counted() && !layersOK {
		msg += " A layer check did not pass, so the pipeline is NOT recovered."
	}
	return msg
}

// stepCheck is a recovery step's entry in agent.verification.checks.
func (st *recoveryStep) check() events.VerificationCheck {
	c := events.VerificationCheck{Layer: st.step.Layer, Name: st.step.Action}
	switch {
	case st.unsupported:
		c.Detail = clipLine("cannot be counted: " + st.lastErr)
	case st.last == nil:
		c.Detail = "no count succeeded"
		if st.lastErr != "" {
			c.Detail = clipLine("no count succeeded; last error: " + st.lastErr)
		}
	default:
		c.OK = st.last.After == 0
		c.Detail = clipLine(countLine(*st.last))
	}
	return c
}

// verifyRecoveryProposal verifies a proposal with recovery steps after its
// last step succeeded: the fix steps' layer checks, then the counts. It emits
// the agent.verification and the message, records the verdict, and returns
// the note for the model. executedAt is the last step's action.executed time
// (runner.executedAt; when the input does not carry it, the time the agent
// first saw the result, which is never earlier, so a count bounded by it errs
// towards "not recovered"). A ctx that ends mid-way emits no verdict.
func (r *runner) verifyRecoveryProposal(ctx context.Context, rec proposalRecord, steps []*recoveryStep, executedAt time.Time) actionOutcome {
	var fixSteps []events.ProposalStep
	for i, s := range rec.steps {
		if !isRecoveryIndex(steps, i) {
			fixSteps = append(fixSteps, s)
		}
	}
	var lines []string
	var checks []events.VerificationCheck
	layersOK := true
	for _, c := range verifyChecks(fixSteps) {
		ok, line, ids := r.runVerifyCheck(ctx, c)
		layersOK = layersOK && ok
		label := c.name
		if len(ids) > 0 {
			label += " (step " + strings.Join(ids, ", step ") + ")"
		}
		lines = append(lines, label+": "+line)
		checks = append(checks, events.VerificationCheck{Layer: c.layer, Name: c.name, OK: ok, Detail: clipLine(line)})
	}

	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working", Detail: recoveryStatusDetail})
	polls, err := r.pollRecovery(ctx, rec.id, steps, executedAt)
	if err != nil {
		r.log.Info("agent: recovery verification stopped", "proposal", rec.id, "err", err)
		return actionOutcome{broken: true}
	}
	// The detail must not outlive the polls.
	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"})
	lines = append(lines, polls...)

	v := recoveryOutcome(steps)
	for _, st := range steps {
		checks = append(checks, st.check())
	}
	recovered := layersOK && v.outcome == events.RecoveryRecovered
	head := recoveryHead(v, layersOK, r.timing.recoveryTimeout)
	p := events.AgentVerificationPayload{
		ProposalID: rec.id,
		Checks:     checks,
		Recovered:  recovered,
		Verdict:    strings.TrimSuffix(head, "\n"),
		Outcome:    v.outcome,
	}
	if v.counted() {
		before, after := v.before, v.after
		p.Before, p.After, p.Unit = &before, &after, v.unit
	}
	r.emitVerification(ctx, p)
	r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: recoveryMessage(v, layersOK, r.timing.recoveryTimeout)})
	r.proposals.markVerified(rec.id, recovered)
	if recovered {
		r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "done", Detail: "pipeline recovered"})
	}
	return actionOutcome{
		recovered: recovered,
		broken:    !recovered,
		notes:     []string{verifyTag + head + wrapToolOutput("verify", "", strings.Join(lines, "\n"))},
	}
}

// isRecoveryIndex reports whether step i of the proposal is a recovery step.
func isRecoveryIndex(steps []*recoveryStep, i int) bool {
	for _, st := range steps {
		if st.index == i {
			return true
		}
	}
	return false
}
