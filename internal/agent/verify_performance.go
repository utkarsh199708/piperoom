package agent

import (
	"context"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
)

// Throughput verification of a tuning (X6c). A proposal with a performance
// step (actions.ClassPerformance: connect_tune_throughput) changes how fast
// the pipeline moves, not whether it moves, so the F4 layer checks only say
// the restart left every task running. The step's own Verify measures the
// connector's throughput over the window before the step and the one after
// it (actions.PerformanceVerify*); the agent calls it once the after window
// has closed, retries a failed call every minute, and stops at the timeout
// (unverifiable). It shares the recovery loop's step type, scope and call
// (verify_recovery.go) and writes one agent.verification with the outcome of
// events.PerformanceOutcomeFor and one message citing the numbers; no model
// call is made for the measure, and nothing is rolled back automatically.

// performanceStatusDetail is the status detail while the windows are measured.
const performanceStatusDetail = "measuring throughput after the tuning"

// perfDone reports whether a performance step needs no more calls.
func perfDone(st *recoveryStep) bool { return st.unsupported || st.last != nil }

// callPerfVerify is callVerifyRaw with the performance rules: throughput
// never negative, the records-per-minute unit.
func (r *runner) callPerfVerify(ctx context.Context, st *recoveryStep, executedAt time.Time) (actions.VerifyResult, error) {
	res, err := r.callVerifyRaw(ctx, st, executedAt)
	if err != nil {
		return res, err
	}
	switch {
	case res.Before < 0 || res.After < 0:
		return actions.VerifyResult{}, fmt.Errorf("agent: verify %s: throughput out of range (before %d, after %d)", st.step.Action, res.Before, res.After)
	case res.Unit != events.PerformanceUnitRecordsPerMinute:
		return actions.VerifyResult{}, fmt.Errorf("agent: verify %s: unknown unit %q", st.step.Action, clipLine(res.Unit))
	}
	return res, nil
}

// pollPerformance calls every step's Verify once the after window has
// closed, then again every recoveryMaxPoll after a failed call, until each
// step measured, one cannot be measured, or perfTimeout after the step. It
// returns one evidence line per call and ctx's error when ctx ended first;
// it starts no goroutine, and every wait ends with ctx.
func (r *runner) pollPerformance(ctx context.Context, steps []*recoveryStep, executedAt time.Time) ([]string, error) {
	due := executedAt.Add(r.timing.perfSettle + r.timing.perfWindow)
	deadline := executedAt.Add(r.timing.perfTimeout)
	retry := max(r.timing.recoveryMaxPoll, time.Second)
	var lines []string
	for call := 1; ; call++ {
		if d := due.Sub(r.nowTime()); d > 0 {
			if err := r.clk.Sleep(ctx, d); err != nil {
				return lines, err
			}
		}
		for _, st := range steps {
			if perfDone(st) {
				continue
			}
			res, err := r.callPerfVerify(ctx, st, executedAt)
			if cerr := ctx.Err(); cerr != nil {
				return lines, cerr
			}
			head := fmt.Sprintf("call %d, step %d (%s): ", call, st.index+1, st.step.Action)
			if err != nil {
				st.lastErr = err.Error()
				if cannotCount(err) {
					st.unsupported = true
					lines = append(lines, head+"cannot be measured: "+clipLine(st.lastErr))
					return lines, nil
				}
				lines = append(lines, head+"error: "+clipLine(st.lastErr))
				continue
			}
			st.last = &res
			lines = append(lines, head+throughputLine(res))
		}
		if allPerfDone(steps) {
			return lines, nil
		}
		due = r.nowTime().Add(retry)
		if due.After(deadline) {
			return append(lines, "stopped: "+r.timing.perfTimeout.String()+" passed since the step"), nil
		}
	}
}

func allPerfDone(steps []*recoveryStep) bool {
	for _, st := range steps {
		if !perfDone(st) {
			return false
		}
	}
	return true
}

// throughputLine renders one measured call for the evidence: numbers and the
// action's own detail (names and times) only.
func throughputLine(res actions.VerifyResult) string {
	line := fmt.Sprintf("throughput before %d %s, after %d (%s)", res.Before, res.Unit, res.After, percentText(res.Before, res.After))
	if res.LagSlopeBefore != nil && res.LagSlopeAfter != nil {
		line += fmt.Sprintf("; lag slope %+d → %+d per min", *res.LagSlopeBefore, *res.LagSlopeAfter)
	}
	if res.Detail != "" {
		line += "; " + clipLine(res.Detail)
	}
	return line
}

// percentText is after against before in whole percent, "+15 %".
func percentText(before, after int64) string {
	if before == 0 {
		if after == 0 {
			return "no change"
		}
		return "from none"
	}
	return fmt.Sprintf("%+d %%", (after-before)*100/before)
}

// perfVerdict is the outcome of a finished measure over every step.
type perfVerdict struct {
	outcome string
	res     actions.VerifyResult // the deciding step's measure; zero when unverifiable
	why     string               // why it is unverifiable
}

// performanceOutcome judges every step with events.PerformanceOutcomeFor. A
// step that could not be measured makes the whole proposal unverifiable;
// otherwise the worst step stands for the proposal (worse, then no_change,
// then improved), with its numbers.
func performanceOutcome(steps []*recoveryStep) perfVerdict {
	for _, st := range steps {
		switch {
		case st.unsupported:
			return perfVerdict{outcome: events.PerformanceUnverifiable, why: st.lastErr}
		case st.last == nil:
			why := "no measure succeeded before the timeout"
			if st.lastErr != "" {
				why += "; last error: " + st.lastErr
			}
			return perfVerdict{outcome: events.PerformanceUnverifiable, why: why}
		}
	}
	rank := map[string]int{events.PerformanceImproved: 0, events.PerformanceNoChange: 1, events.PerformanceWorse: 2}
	var worst perfVerdict
	for i, st := range steps {
		l := st.last
		v := perfVerdict{outcome: events.PerformanceOutcomeFor(l.Before, l.After, l.LagSlopeBefore, l.LagSlopeAfter), res: *l}
		if i == 0 || rank[v.outcome] > rank[worst.outcome] {
			worst = v
		}
	}
	return worst
}

// numbers is the verdict's throughput and lag in one phrase.
func (v perfVerdict) numbers() string {
	s := fmt.Sprintf("throughput %d → %d records/min (%s)", v.res.Before, v.res.After, percentText(v.res.Before, v.res.After))
	if v.res.LagSlopeBefore != nil && v.res.LagSlopeAfter != nil {
		s += fmt.Sprintf(", lag slope %+d → %+d records/min", *v.res.LagSlopeBefore, *v.res.LagSlopeAfter)
	}
	return s
}

// performanceHead is the verdict line (agent.verification.verdict and the
// head of the model's note): counts and fixed words only. Only an improved
// tuning with every layer check passing starts with "improved:".
func performanceHead(v perfVerdict, layersOK bool, timeout time.Duration) string {
	var head string
	switch v.outcome {
	case events.PerformanceUnverifiable:
		head = "NOT verified (unverifiable): the tuning's throughput could not be measured within " + timeout.String() + " (no offsets by time, or no group_lag watch samples around the step), so its effect is unknown."
	case events.PerformanceImproved:
		head = "improved: " + v.numbers() + "."
	case events.PerformanceNoChange:
		head = "no_change: " + v.numbers() + "; under the 10 % that counts, the tuning did not change throughput."
	default:
		head = "worse: " + v.numbers() + ". The values from before the step are the rollback: the proposal's preview lists them, and the step result names them."
	}
	if !layersOK {
		head = "NOT healthy: a layer check did not pass after the tuning (it restarted the tasks). " + head
	}
	return head + "\n"
}

// performanceMessage is the room message citing the numbers, for people.
func performanceMessage(v perfVerdict, layersOK bool) string {
	var msg string
	switch v.outcome {
	case events.PerformanceUnverifiable:
		msg = "Throughput check: the tuning's effect could not be measured (no offsets by time, or no group_lag samples from the watcher around the step). It is NOT verified."
	case events.PerformanceImproved:
		msg = "Throughput check: " + v.numbers() + ". The tuning improved throughput."
	case events.PerformanceNoChange:
		msg = "Throughput check: " + v.numbers() + ". That is under the 10 % that counts: no change."
	default:
		msg = "Throughput check: " + v.numbers() + ". The tuning made it worse. The proposal card offers the values from before the step as a rollback; nothing was rolled back automatically."
	}
	if !layersOK {
		msg += " A layer check did not pass after the restart, so the pipeline is NOT healthy."
	}
	return msg
}

// perfCheck is a performance step's entry in agent.verification.checks: OK
// when it did not make throughput worse.
func (st *recoveryStep) perfCheck() events.VerificationCheck {
	c := events.VerificationCheck{Layer: st.step.Layer, Name: st.step.Action}
	switch {
	case st.unsupported:
		c.Detail = clipLine("cannot be measured: " + st.lastErr)
	case st.last == nil:
		c.Detail = "no measure succeeded"
		if st.lastErr != "" {
			c.Detail = clipLine("no measure succeeded; last error: " + st.lastErr)
		}
	default:
		l := st.last
		c.OK = events.PerformanceOutcomeFor(l.Before, l.After, l.LagSlopeBefore, l.LagSlopeAfter) != events.PerformanceWorse
		c.Detail = clipLine(throughputLine(*l))
	}
	return c
}

// verifyPerformanceProposal verifies a proposal with performance steps after
// its last step succeeded: every step's layer checks (a tuning restarts the
// tasks, which can fail them), then the throughput windows. It emits the
// agent.verification and the message, records the verdict, and returns the
// note for the model. Recovered on the event keeps its F4 meaning (every
// layer check passed); the episode parks as done only when the tuning also
// improved throughput, so the model reads a no_change, worse or
// unverifiable verdict and decides what is next (a rollback is its own
// proposal). A ctx that ends mid-way emits no verdict.
func (r *runner) verifyPerformanceProposal(ctx context.Context, rec proposalRecord, steps []*recoveryStep, executedAt time.Time) actionOutcome {
	var lines []string
	var checks []events.VerificationCheck
	layersOK := true
	for _, c := range verifyChecks(rec.steps) {
		ok, line, ids := r.runVerifyCheck(ctx, c)
		layersOK = layersOK && ok
		label := c.name
		if len(ids) > 0 {
			label += " (step " + strings.Join(ids, ", step ") + ")"
		}
		lines = append(lines, label+": "+line)
		checks = append(checks, events.VerificationCheck{Layer: c.layer, Name: c.name, OK: ok, Detail: clipLine(line)})
	}

	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working", Detail: performanceStatusDetail})
	polls, err := r.pollPerformance(ctx, steps, executedAt)
	if err != nil {
		r.log.Info("agent: performance verification stopped", "proposal", rec.id, "err", err)
		return actionOutcome{broken: true}
	}
	// The detail must not outlive the measure.
	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"})
	lines = append(lines, polls...)

	v := performanceOutcome(steps)
	if v.why != "" {
		lines = append(lines, "unverifiable: "+clipLine(v.why))
	}
	for _, st := range steps {
		checks = append(checks, st.perfCheck())
	}
	head := performanceHead(v, layersOK, r.timing.perfTimeout)
	p := events.AgentVerificationPayload{
		ProposalID: rec.id,
		Checks:     checks,
		Recovered:  layersOK,
		Verdict:    strings.TrimSuffix(head, "\n"),
		Outcome:    v.outcome,
	}
	if v.outcome != events.PerformanceUnverifiable {
		before, after := v.res.Before, v.res.After
		p.Before, p.After, p.Unit = &before, &after, v.res.Unit
		p.LagSlopeBefore, p.LagSlopeAfter = v.res.LagSlopeBefore, v.res.LagSlopeAfter
	}
	r.emitVerification(ctx, p)
	r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: performanceMessage(v, layersOK)})
	done := layersOK && v.outcome == events.PerformanceImproved
	r.proposals.markVerified(rec.id, done)
	if done {
		r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "done", Detail: "throughput improved"})
	}
	return actionOutcome{
		recovered: done,
		broken:    !done,
		notes:     []string{verifyTag + head + wrapToolOutput("verify", "", strings.Join(lines, "\n"))},
	}
}
