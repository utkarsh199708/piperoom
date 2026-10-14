package loadtest

import "fmt"

// The correctness checks are pure functions of the counters, so each guard
// is unit-tested with synthetic counters that contain the failure it must
// catch (checks_test.go).

// agentLimit is the most agent runs that may call the model at once: the
// per-replica cap times the replicas, or the global cap when it is lower
// (global 0 = off).
func agentLimit(perReplica, replicas, global int) int {
	limit := perReplica * replicas
	if global > 0 && global < limit {
		limit = global
	}
	return limit
}

// agentCapCheck passes when the fake model never had more requests open at
// once than the agent limit. It is a lower bound on cap breaches: a run that
// holds a slot without calling the model is not seen.
func agentCapCheck(maxInFlight, perReplica, replicas, global int) Check {
	limit := agentLimit(perReplica, replicas, global)
	return Check{
		Name:   "agent cap respected (fake model in-flight <= limit)",
		OK:     maxInFlight <= limit,
		Detail: fmt.Sprintf("max in-flight %d, limit %d (%d per replica x %d replicas, global %d)", maxInFlight, limit, perReplica, replicas, global),
	}
}

// liteModel summarizes the fake model's counters for rooms: a room with more
// than one run start, or with two model calls open at once, had a duplicate
// agent run.
func liteModel(st ModelStats, rooms []string) ModelStatsLite {
	l := ModelStatsLite{Requests: st.Requests, BadRequests: st.BadRequests, MaxInFlight: st.MaxInFlight}
	for _, r := range rooms {
		if rs := st.Rooms[r]; rs != nil {
			if rs.RunStarts > 1 {
				l.DuplicateRuns++
			}
			if rs.MaxInFlight > 1 {
				l.ConcurrentDup++
			}
		}
	}
	return l
}

// dupRunCheck passes when no room had a duplicate agent run.
func dupRunCheck(l ModelStatsLite) Check {
	return Check{
		Name:   "no duplicate agent runs",
		OK:     l.DuplicateRuns == 0 && l.ConcurrentDup == 0,
		Detail: fmt.Sprintf("rooms with >1 run start %d, rooms with concurrent model calls %d", l.DuplicateRuns, l.ConcurrentDup),
	}
}

// execCounts folds the lt_noop execution counts of every server (total, by
// key) against the approved keys: all executions, keys run more than once,
// approved keys never run.
func execCounts(total map[string]int, approved map[string]bool) (executions, duplicates, missing int) {
	for _, v := range total {
		executions += v
		if v > 1 {
			duplicates++
		}
	}
	for k := range approved {
		if total[k] == 0 {
			missing++
		}
	}
	return executions, duplicates, missing
}

// execCheck passes when every one of rooms' proposals was approved and
// executed exactly once, by the action counters and by the room logs.
func execCheck(e ExecStats, rooms int) Check {
	return Check{
		Name: "every approved fix executed exactly once",
		OK: e.Approved == rooms && e.ApproveErr == 0 && e.DuplicateExecutions == 0 &&
			e.MissingExecutions == 0 && e.DuplicateExecEvents == 0 && e.ExecutedEvents == e.Approved,
		Detail: fmt.Sprintf("proposals %d approved %d approve errors %d; executions %d (duplicate %d, missing %d); action.executed events %d (duplicate %d)",
			e.Proposals, e.Approved, e.ApproveErr, e.Executions, e.DuplicateExecutions, e.MissingExecutions, e.ExecutedEvents, e.DuplicateExecEvents),
	}
}

// seqCheck passes when subscribers saw every event once and in order.
func seqCheck(t SubTotals) Check {
	return Check{
		Name:   "every subscriber saw every event once, in order",
		OK:     t.Gaps == 0 && t.Dups == 0 && t.MissingTail == 0 && t.Mismatches == 0,
		Detail: fmt.Sprintf("gaps %d dups %d missing-at-end %d parse mismatches %d", t.Gaps, t.Dups, t.MissingTail, t.Mismatches),
	}
}

// leakCheck passes when the servers' goroutines came back to within slack of
// the count before the subscribers connected.
func leakCheck(before, after int) Check {
	return Check{
		Name:   "no server goroutine leak after close",
		OK:     after <= before+goroutineSlack,
		Detail: fmt.Sprintf("%d before subscribers, %d after they closed", before, after),
	}
}

// goroutineSlack allows for timers and pool goroutines that come and go.
const goroutineSlack = 5

// labeled prefixes a check's name.
func labeled(label string, c Check) Check {
	c.Name = label + c.Name
	return c
}

func (r *Result) add(c Check) { r.Checks = append(r.Checks, c) }
