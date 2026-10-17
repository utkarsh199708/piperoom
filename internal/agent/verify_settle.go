package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/events"
)

// Settling after a fix (QA G6). Kafka Connect restarts a connector or task
// asynchronously: a read right after the last step ran can show the state from
// before the fix, a transition (UNASSIGNED, RESTARTING, no task yet), or a REST
// error while the cluster rebalances. The first verifier judged that single
// read and announced "NOT recovered" for a pipeline that recovered seconds
// later. The layer checks are now polled until they pass, until they have
// failed for good, or until the settle window has passed; the verdict is
// decided once, at the end of that.

// checkState is what one read of a layer check says.
type checkState int

const (
	// checkFail: the layer answered and it is not recovered.
	checkFail checkState = iota
	// checkPending: the layer is still settling, or could not be read yet.
	checkPending
	// checkPass: the layer shows the recovered state.
	checkPass
)

const (
	// verifySettleRounds is how many polls follow the first read. With the
	// default settleWait of 5 s the window is 90 s, which covers a connector
	// restart and its rebalance with room to spare, and still ends before a
	// person gives up waiting on the room.
	verifySettleRounds = 18
	// connectFailConfirm is how many reads in a row must show a Connect failure
	// before it is final. A task FAILED before the fix can still be listed as
	// FAILED while the new task starts, for a few seconds.
	connectFailConfirm = 3
)

// verifySettleDeadline is the settle window: verifySettleRounds polls,
// settleWait apart. It follows the runner's settleWait, so a test that shrinks
// that wait shrinks the window with it and needs no other knob.
func (t timing) verifySettleDeadline() time.Duration {
	return time.Duration(verifySettleRounds) * t.settleWait
}

// checkReading is one read of one check.
type checkReading struct {
	state checkState
	line  string
	// final marks a failure that needs no confirmation: the check cannot run,
	// and reading it again changes nothing.
	final bool
	// ids are the step ids of the reads that went to the room (and the step
	// ledger), for the model to cite; a silent read adds none.
	ids []string
}

// settleOutcome is the result of polling the layer checks.
type settleOutcome struct {
	// ok is true when every check passed.
	ok bool
	// aborted is true when ctx ended before a result: there is no verdict, and
	// the caller must not announce one.
	aborted bool
	// timedOut is true when the window ran out with a check still not passing.
	timedOut bool
	// checks and lines describe the last read of every check, in step order.
	checks []events.VerificationCheck
	lines  []string
	rounds int
	waited time.Duration
}

// settleLayers polls the layer checks of a proposal. Each round reads every
// check once; a passing check with settle set must pass again settleWait
// later, so a connector listed RUNNING just before it restarts does not count.
//
// It stops when
//   - every check passes in one round (ok);
//   - every check that does not pass has failed in the same way for as many
//     rounds in a row as its failConfirm asks, with nothing still pending;
//   - the window (verifySettleDeadline) has passed, or the wall-clock backstop
//     of twice that has, because reads take time too (timedOut);
//   - ctx ends (aborted).
//
// Only the first round writes its reads to the room; later rounds are silent
// (runStepEvents), and the verdict's evidence says how many reads were made.
func (r *runner) settleLayers(ctx context.Context, checks []verifyCheck) settleOutcome {
	if len(checks) == 0 {
		return settleOutcome{ok: true}
	}
	interval := r.timing.settleWait
	window := r.timing.verifySettleDeadline()
	hard, cancel := context.WithTimeout(ctx, 2*window+r.timing.toolTimeout)
	defer cancel()

	var (
		waited     time.Duration
		failStreak int
	)
	// Only round 1 writes its reads to the room, so its step ids are the ones
	// the model can cite, whatever round decides.
	stepIDs := make([][]string, len(checks))
	for round := 1; ; round++ {
		readings := make([]checkReading, len(checks))
		allPass, anyPending, need := true, false, 1
		for i, c := range checks {
			rd := r.readCheckSettled(hard, c, interval, round == 1)
			if rd.slept {
				waited += interval
			}
			readings[i] = rd.checkReading
			if round == 1 {
				stepIDs[i] = rd.ids
			}
			switch rd.state {
			case checkPass:
			case checkPending:
				allPass, anyPending = false, true
			default:
				allPass = false
				if !rd.final {
					need = max(need, c.failConfirm)
				}
			}
		}
		if ctx.Err() != nil {
			return settleOutcome{aborted: true}
		}
		out := settleOutcome{ok: allPass, rounds: round, waited: waited}
		out.checks, out.lines = describeReadings(checks, readings, stepIDs)
		if allPass {
			return out.withNote(window, failStreak)
		}
		if anyPending {
			failStreak = 0
		} else {
			failStreak++
		}
		if !anyPending && failStreak >= need {
			return out.withNote(window, failStreak)
		}
		if round > verifySettleRounds || waited+interval > window || hard.Err() != nil {
			out.timedOut = true
			return out.withNote(window, failStreak)
		}
		if round == 1 {
			// One look at the room while it waits: the agent is not stuck.
			r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working", Detail: "verifying the fix: waiting for the layers to settle"})
		}
		if err := r.clk.Sleep(hard, interval); err != nil {
			if ctx.Err() != nil {
				return settleOutcome{aborted: true}
			}
			out.timedOut = true // the wall-clock backstop ended the wait
			return out.withNote(window, failStreak)
		}
		waited += interval
	}
}

// settledReading is a reading plus whether the settle pause was taken.
type settledReading struct {
	checkReading
	slept bool
}

// readCheckSettled reads one check, and for a settle check that passes reads it
// again after interval; the second read decides. The evidence line then holds
// both reads.
func (r *runner) readCheckSettled(ctx context.Context, c verifyCheck, interval time.Duration, withEvents bool) settledReading {
	first := r.readCheckOnce(ctx, c, withEvents)
	if first.state != checkPass || !c.settle {
		return settledReading{checkReading: first}
	}
	if err := r.clk.Sleep(ctx, interval); err != nil {
		return settledReading{checkReading: checkReading{state: checkPending, line: first.line + "; settling: " + err.Error(), ids: first.ids}}
	}
	again := r.readCheckOnce(ctx, c, withEvents)
	return settledReading{
		checkReading: checkReading{state: again.state, line: first.line + "; read again after " + interval.String() + ": " + again.line, ids: append(first.ids, again.ids...)},
		slept:        true,
	}
}

// readCheckOnce reads a layer check once and classifies the answer. A tool
// error is pending: a layer that is restarting answers with errors, and a read
// that timed out may succeed next time. A check that cannot run at all is a
// failure, since waiting changes nothing.
func (r *runner) readCheckOnce(ctx context.Context, c verifyCheck, withEvents bool) checkReading {
	res, id, err := r.runStepEvents(ctx, "verify", c.name, c.input, withEvents)
	var ids []string
	if id != "" {
		ids = []string{id}
	}
	if err != nil {
		line := clipLine("error: " + err.Error())
		if errors.Is(err, errVerifyUnrunnable) {
			return checkReading{state: checkFail, line: line, final: true, ids: ids}
		}
		return checkReading{state: checkPending, line: line, ids: ids}
	}
	state := checkFail
	switch {
	case c.state != nil:
		state = c.state(res)
	case c.judge(res):
		state = checkPass
	}
	return checkReading{state: state, line: clipLine(res.Summary), ids: ids}
}

// describeReadings turns the last reading of every check into the event's
// check entries and the evidence lines the model and the legacy message get.
// stepIDs are the room steps of each check's reads: the model's line names
// them, so it can cite the re-check instead of reading the same component
// again; the room's check detail stays free of ids.
func describeReadings(checks []verifyCheck, readings []checkReading, stepIDs [][]string) ([]events.VerificationCheck, []string) {
	vc := make([]events.VerificationCheck, len(checks))
	lines := make([]string, len(checks))
	for i, c := range checks {
		vc[i] = events.VerificationCheck{Layer: c.layer, Name: c.name, OK: readings[i].state == checkPass, Detail: clipLine(readings[i].line)}
		label := c.name
		if i < len(stepIDs) && len(stepIDs[i]) > 0 {
			label += " (step " + strings.Join(stepIDs[i], ", step ") + ")"
		}
		lines[i] = label + ": " + readings[i].line
	}
	return vc, lines
}

// withNote adds the evidence line that says how the polling ended. A verdict
// reached on the first read has nothing to add, which keeps its evidence as it
// was before polling.
func (o settleOutcome) withNote(window time.Duration, failStreak int) settleOutcome {
	switch {
	case o.rounds <= 1:
		return o
	case o.ok:
		o.lines = append(o.lines, fmt.Sprintf("layer checks settled on read %d, %s after the first", o.rounds, o.waited))
	case o.timedOut:
		o.lines = append(o.lines, fmt.Sprintf("layer checks did not settle within %s (%d reads)", window, o.rounds))
	default:
		o.lines = append(o.lines, fmt.Sprintf("layer checks failed on %d reads in a row (%d reads, %s)", failStreak, o.rounds, o.waited))
	}
	return o
}
