package watch

import (
	"fmt"
	"strconv"
	"time"
)

// The crash-loop counter of connector_down (tasks/F6-proactive-triage.md
// §Design 1). Kafka Connect restarts a failed task (by hand, by an operator
// script, or by errors.retry), so a task that keeps failing can read RUNNING
// on most ticks. The watcher keeps each task's state changes; a FAILED ->
// RUNNING change is a restart, and crashloop_restarts of them inside
// crashloop_window make the task count as down while it reads RUNNING.
//
// Honest limit: the watcher only sees the state at each tick. A task that
// fails and is restarted between two ticks (a loop faster than the watch
// interval) reads RUNNING every time and is never counted; F12's
// debezium_metrics signal is the detector for those.

// taskEvent is one observed state change of a task.
type taskEvent struct {
	at    time.Time
	state string
}

// taskHist is the memory of one connector task (key "connector/task id").
type taskHist struct {
	events []taskEvent // state changes, oldest first
	// lastTrace is the first line of the last FAILED trace, for the evidence.
	lastTrace string
	lastSeen  time.Time
}

// maxTaskEvents bounds one task's ring whatever the window and interval are.
const maxTaskEvents = 256

// loopInfo is a crash-looping task.
type loopInfo struct {
	restarts int
	trace    string
}

// taskKey is the taskHist key of a connector task.
func taskKey(connector string, id int) string { return connector + "/" + strconv.Itoa(id) }

// recordTasks adds this tick's task states to the pipeline's history and
// returns the tasks that crash-loop now, by taskKey. A connector whose status
// could not be read adds nothing (an error tick does not break the chain).
// Caller holds w.mu.
func recordTasks(now time.Time, t target, ps *pipeState, obs *observation) map[string]loopInfo {
	loop := t.cfg.ConnectorCrashLoop
	if loop.Restarts <= 0 || loop.Window <= 0 {
		return nil
	}
	if ps.taskHist == nil {
		ps.taskHist = map[string]*taskHist{}
	}
	cutoff := now.Add(-loop.Window)
	out := map[string]loopInfo{}
	for _, name := range t.connectors {
		co := obs.conns[name]
		if co == nil || co.statusErr != nil {
			continue
		}
		for _, tk := range co.status.Tasks {
			key := taskKey(name, tk.ID)
			h := ps.taskHist[key]
			if h == nil {
				h = &taskHist{}
				ps.taskHist[key] = h
			}
			h.lastSeen = now
			state := clean(tk.State, 24)
			if n := len(h.events); n == 0 || h.events[n-1].state != state {
				h.events = append(h.events, taskEvent{at: now, state: state})
			}
			if state == "FAILED" {
				if line := traceLine(tk.Trace); line != "" {
					h.lastTrace = line
				}
			}
			// Drop changes that can no longer count; keep the newest, so the
			// next change still has a "before".
			for len(h.events) > 1 && (h.events[1].at.Before(cutoff) || len(h.events) > maxTaskEvents) {
				h.events = h.events[1:]
			}
			if n := restarts(h.events, cutoff); n >= loop.Restarts {
				out[key] = loopInfo{restarts: n, trace: h.lastTrace}
			}
		}
	}
	// Forget tasks not seen for a whole window (a connector removed from
	// owners.yaml, a task count that went down).
	for key, h := range ps.taskHist {
		if h.lastSeen.Before(cutoff) {
			delete(ps.taskHist, key)
		}
	}
	return out
}

// restarts counts FAILED -> RUNNING changes at or after since.
func restarts(evs []taskEvent, since time.Time) int {
	n := 0
	for i := 1; i < len(evs); i++ {
		if evs[i-1].state == "FAILED" && evs[i].state == "RUNNING" && !evs[i].at.Before(since) {
			n++
		}
	}
	return n
}

// loopLabel is the title fragment of a crash-looping task.
func loopLabel(id int, li loopInfo, window time.Duration) string {
	return fmt.Sprintf("task %d crash-looping (%d restarts in %s)", id, li.restarts, fmtDur(window))
}
