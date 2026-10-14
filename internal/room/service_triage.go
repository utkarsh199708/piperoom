package room

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/config"
)

// The triage pass (F6, tasks/F6-proactive-triage.md §3) is a bounded first
// look that runs before a new room's agent waits for a slot, so a room queued
// behind PIPEROOM_MAX_AGENTS still shows a one-line diagnosis. It writes its
// own steps and one agent.triage through the room's sink; its result goes into
// the main run's first message (agent.Alert.Triage).

const (
	// maxTriagePasses caps first looks running at once in this process. A room
	// over it waits for a pass; it never skips one.
	maxTriagePasses = 4
	// triageWaitMargin is added to the pass's deadline for how long the main
	// run waits for its result.
	triageWaitMargin = 5 * time.Second
	// refireTriageAfter is how old the newest agent.triage must be before a
	// re-fire of the alert runs the pass again (new numbers).
	refireTriageAfter = 15 * time.Minute
)

// triageWaitFor is how long the main run waits for a pass with deadline d
// (0 = the default deadline).
func triageWaitFor(d time.Duration) time.Duration {
	if d <= 0 {
		d, _ = time.ParseDuration(config.DefaultTriagePassTimeout)
	}
	return min(d, config.MaxTriagePassTimeout) + triageWaitMargin
}

// startTriageLocked starts the pass for a room on its own goroutine and
// returns where its result arrives (closed without a value when it failed to
// run). run is the agent run whose sink the pass writes through, so the
// pass is fenced like the run. Caller holds s.mu.
func (s *Service) startTriageLocked(ctx context.Context, roomID string, run uint64, alert agent.Alert) <-chan agent.TriageResult {
	out := make(chan agent.TriageResult, 1)
	deps := agent.Deps{
		Tools:  s.toolsFor(alert.PipelineID),
		Sink:   roomSink{s: s, roomID: roomID, run: run},
		Logger: s.log,
	}
	if !s.addWorker() {
		close(out) // a closing service runs no pass
		return out
	}
	go func() {
		defer s.wg.Done()
		defer close(out)
		select {
		case s.triageSem <- struct{}{}:
			defer func() { <-s.triageSem }()
		case <-ctx.Done():
			return
		}
		res, err := s.runTriage(ctx, roomID, alert, deps)
		if err != nil {
			if ctx.Err() == nil {
				s.log.Warn("room: first look did not run", "room", roomID, "err", err)
			}
			return
		}
		out <- res
	}()
	return out
}

// runTriage calls the triager and turns a panic into an error, like runGuarded.
func (s *Service) runTriage(ctx context.Context, roomID string, alert agent.Alert, deps agent.Deps) (res agent.TriageResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("room: first look panicked", "room", roomID, "panic", r)
			err = fmt.Errorf("room: first look panicked: %v", r)
		}
	}()
	return s.triage(ctx, alert, deps, s.opts)
}

// awaitTriage waits for a pass's result, at most s.triageWait. Nil when the
// pass gave none in time.
func (s *Service) awaitTriage(ctx context.Context, roomID string, ch <-chan agent.TriageResult) *agent.TriageResult {
	t := time.NewTimer(s.triageWait)
	defer t.Stop()
	select {
	case res, ok := <-ch:
		if !ok {
			return nil
		}
		return &res
	case <-t.C:
		s.log.Warn("room: first look still running; the agent starts without it", "room", roomID)
		return nil
	case <-ctx.Done():
		return nil
	}
}

// alertUpdateLocked gives the room's agent a re-fired or joined alert's
// evidence as a quiet "alert_update" input (it never wakes an idle agent and
// replaces one still pending), and re-runs the first look when the last one
// is old and the agent is not working. seq is the room event the update came
// with. Caller holds s.mu.
func (s *Service) alertUpdateLocked(roomID string, st *roomState, seq int64, raw json.RawMessage) {
	if st.snap.Shadow || st.inbox == nil {
		return
	}
	if s.coord != nil && !st.leased {
		return // the lease owner reads it from the log (pending input)
	}
	if st.agentCancel != nil {
		st.inbox.pushQuiet(agent.HumanInput{Seq: seq, Actor: systemActor, Kind: "alert_update", Text: agent.AlertEvidenceText(raw)})
	}
	s.refireTriageLocked(roomID, st, raw)
}

// refireTriageLocked re-runs the first look for a re-fire when the newest
// agent.triage is at least refireTriageAfter old and the agent is idle or
// queued. It uses no agent slot. Caller holds s.mu.
func (s *Service) refireTriageLocked(roomID string, st *roomState, raw json.RawMessage) {
	if s.triage == nil || s.isClosed() || st.snap.Triage == nil || st.agentCancel == nil {
		return
	}
	if s.now().Sub(st.snap.Triage.At) < refireTriageAfter {
		return
	}
	if state := st.snap.AgentStatus.State; state != "idle" && state != "queued" {
		return
	}
	alert := agent.Alert{RoomID: roomID, Title: st.snap.Title, PipelineID: st.snap.PipelineID, Raw: raw}
	// A newer agent.triage lands in the log and the snapshot; the running
	// agent already has the numbers from the alert update.
	_ = s.startTriageLocked(s.ctx, roomID, st.agentRun, alert)
}
