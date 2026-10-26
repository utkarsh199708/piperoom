package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
)

// Shadow mode (docs/design/shadow.md): a room whose pipeline has
// `shadow: {enabled: true}` runs the agent once, silently, with read tools only.
// Every agent-actor event goes to the shadow log (emitLocked), and the result is
// revealed after the room closes.

// shadowWriteTimeout bounds the run-row writes the supervisor makes after its
// run context may already be cancelled (close, shutdown).
const shadowWriteTimeout = 10 * time.Second

// pipelineShadow reports whether pipelineID is a known pipeline with shadow mode
// on. Without an owners config (unit tests, nothing loaded) nothing is shadow.
func (s *Service) pipelineShadow(pipelineID string) bool {
	if s.owners == nil {
		return false
	}
	p, ok := s.owners.Pipeline(pipelineID)
	return ok && p.Shadow()
}

// createShadowRun inserts the room's run row as queued. It is idempotent, so
// openAlertRoom, the supervisor and the Load sweep may all call it. A failure is
// logged at error and returned.
func (s *Service) createShadowRun(ctx context.Context, roomID string) error {
	if s.shadow == nil {
		err := fmt.Errorf("room: shadow room %s: the store cannot hold shadow runs", roomID)
		s.log.Error("room: create shadow run failed", "room", roomID, "err", err)
		return err
	}
	if err := s.shadow.CreateShadowRun(ctx, store.ShadowRun{RoomID: roomID, Status: store.ShadowQueued}); err != nil {
		s.log.Error("room: create shadow run failed", "room", roomID, "err", err)
		return err
	}
	return nil
}

// appendShadowLocked writes one agent event of a shadow room to the shadow log.
// The event is not folded into the snapshot and not fed to the inbox. Caller
// holds the room's lock (it is called from emitLocked).
func (s *Service) appendShadowLocked(ctx context.Context, roomID string, typ events.Type, payload any) (events.Event, error) {
	if s.shadow == nil {
		return events.Event{}, fmt.Errorf("room: shadow room %s: the store cannot hold shadow events", roomID)
	}
	e, err := s.shadow.AppendShadow(ctx, roomID, typ, events.AgentActor, payload)
	if err != nil {
		return events.Event{}, fmt.Errorf("room: append shadow %s: %w", typ, err)
	}
	return e, nil
}

// shadowLogFullLocked stops a shadow run whose log reached the room's cap
// (docs/design/retention.md section 5.3): every further agent event would be
// refused, so the run is cancelled and ends with status budget, like a run
// that spent its tokens. Caller holds st's lock.
func (s *Service) shadowLogFullLocked(roomID string, st *roomState) {
	if st.shadowLogFull {
		return
	}
	st.shadowLogFull = true
	if st.agentCancel != nil {
		st.agentCancel()
	}
	s.log.Warn("room: shadow log full; shadow run stopped", "room", roomID)
}

// shadowStopStatus is the status of a shadow run whose context ended: budget
// when the log cap stopped it, interrupted otherwise (close, shutdown).
func (s *Service) shadowStopStatus(st *roomState) string {
	s.lockState(st)
	defer s.unlockRoom(st)
	if st.shadowLogFull {
		return store.ShadowBudget
	}
	return store.ShadowInterrupted
}

// updateShadowRun reads the run row, applies fn and writes it back.
func (s *Service) updateShadowRun(ctx context.Context, roomID string, fn func(*store.ShadowRun)) error {
	if s.shadow == nil {
		return fmt.Errorf("room: shadow room %s: the store cannot hold shadow runs", roomID)
	}
	r, err := s.shadow.GetShadowRun(ctx, roomID)
	if err != nil {
		return err
	}
	fn(&r)
	return s.shadow.UpdateShadowRun(ctx, r)
}

// startShadowLocked starts a shadow room's one-shot run (design 3.3). The run
// gets no actions and no proposal creator, the room's inbox is closed so it
// ends at its first end_turn, and its options are ShadowOptions. Caller holds
// st's lock and has reserved the run's place in s.wg (startAgentLocked).
func (s *Service) startShadowLocked(roomID, title, pipelineID string, raw json.RawMessage, st *roomState) {
	ctx, cancel := context.WithCancel(s.ctx)
	st.agentCancel = cancel
	st.agentRun++
	run := st.agentRun
	st.shadowActive = true
	openedAt := st.snap.OpenedAt
	gate := &agentGate{s: s, st: st, roomID: roomID, pipeline: pipelineID}
	go func() {
		defer s.wg.Done()
		defer cancel()
		defer gate.release()
		var tokens atomic.Int64
		status := s.superviseShadow(ctx, roomID, title, pipelineID, openedAt, raw, st, gate, &tokens, run)
		gate.release()
		s.finishShadow(roomID, st, status, tokens.Load())
	}()
}

// superviseShadow runs the shadow agent and returns the run's final status, or
// "" when the run row could not be created (then nothing ran and nothing is
// written). A nil return maps to ShadowHypothesised here; finishShadow lowers
// it to no_hypothesis when the run emitted none.
func (s *Service) superviseShadow(ctx context.Context, roomID, title, pipelineID string, openedAt time.Time, raw json.RawMessage, st *roomState, gate *agentGate, tokens *atomic.Int64, run uint64) string {
	// Second try (the first was at open): no row means nothing could store
	// the run's output, so no tokens are spent on it.
	cctx, ccancel := context.WithTimeout(ctx, shadowWriteTimeout)
	err := s.createShadowRun(cctx, roomID)
	ccancel()
	if err != nil {
		return ""
	}
	alert := s.newAlert(roomID, title, pipelineID, openedAt, raw)
	// Actions and Proposals stay nil: propose_fix lists "(none)" and refuses,
	// so a shadow run can never reach a write (design section 4).
	deps := agent.Deps{
		Tools:  s.toolsFor(pipelineID),
		Inbox:  gatedInbox{in: st.inbox, g: gate, runCtx: ctx},
		Sink:   roomSink{s: s, roomID: roomID, run: run},
		Logger: s.log,
		Watch:  s.watch,
		Usage:  func(used int64) { tokens.Store(used) },
	}
	backoff := agentRetryBackoff
	started := false
	for attempt := 1; ; attempt++ {
		// A close while queued or backing off cancels ctx (closeShadowLocked
		// gives grace only to a running attempt), so acquire and the backoff
		// below end at once.
		if err := gate.acquire(ctx); err != nil {
			return store.ShadowInterrupted
		}
		// Re-check under the room's lock with the slot in hand, and mark the attempt
		// running in the same critical section: a close either sees the
		// attempt running (grace) or is seen here. No attempt starts on a
		// closed room, because the room-closed hook has dropped its token
		// count and a retry would spend a fresh budget (design 3.3, M4).
		if !s.beginShadowAttempt(st) {
			return store.ShadowInterrupted // the caller releases the slot
		}
		if !started {
			started = true
			alert.PipelineMap = s.markShadowStarted(ctx, roomID, pipelineID)
		}
		err := s.runGuarded(ctx, roomID, alert, deps, s.shadowOpts)
		s.endShadowAttempt(st)
		gate.release()
		switch {
		case ctx.Err() != nil:
			return s.shadowStopStatus(st)
		case isBudgetStop(err):
			return store.ShadowBudget
		case err == nil:
			return store.ShadowHypothesised
		}
		// After the close the room-closed hook has dropped the room's token
		// count, so a retry would start a fresh budget (design 3.3, M4).
		if s.shadowRoomClosed(st) {
			s.log.Warn("room: shadow run failed after close; not retried", "room", roomID, "err", err)
			return store.ShadowInterrupted
		}
		s.log.Warn("room: shadow run returned error", "room", roomID, "attempt", attempt, "err", err)
		ectx, ecancel := context.WithTimeout(s.ctx, 5*time.Second)
		_ = s.agentEmit(ectx, roomID, run, events.AgentStatus, events.AgentStatusPayload{State: "error", Detail: truncateDetail(err.Error())})
		ecancel()
		if attempt >= maxAgentAttempts {
			s.log.Error("room: shadow run gave up after repeated failures", "room", roomID, "attempts", attempt)
			return store.ShadowError
		}
		select {
		case <-ctx.Done():
			return store.ShadowInterrupted
		case <-s.backoffAfter(backoff):
		}
		backoff *= 2
	}
}

// markShadowStarted records that the run holds its first slot (running,
// started_at), renders the topology and stores the model and the map. It
// returns the rendered map for the alert.
func (s *Service) markShadowStarted(ctx context.Context, roomID, pipelineID string) string {
	wctx, wcancel := context.WithTimeout(context.WithoutCancel(ctx), shadowWriteTimeout)
	defer wcancel()
	now := s.now().UTC()
	if err := s.updateShadowRun(wctx, roomID, func(r *store.ShadowRun) {
		r.Status = store.ShadowRunning
		r.StartedAt = &now
	}); err != nil {
		s.log.Warn("room: mark shadow run running failed", "room", roomID, "err", err)
	}
	pipelineMap := ""
	if s.topo != nil {
		tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
		if g, err := s.topo.Build(tctx, pipelineID); err == nil {
			pipelineMap = s.topo.Render(g)
		}
		tcancel()
	}
	if err := s.updateShadowRun(wctx, roomID, func(r *store.ShadowRun) {
		r.Model = sanitizeAgentText(s.shadowOpts.Model)
		r.PipelineMap = sanitizeAgentText(pipelineMap)
	}); err != nil {
		s.log.Warn("room: record shadow run model failed", "room", roomID, "err", err)
	}
	return pipelineMap
}

// beginShadowAttempt marks an attempt as running, or reports false when the
// room is closed and no attempt may start.
func (s *Service) beginShadowAttempt(st *roomState) bool {
	s.lockState(st)
	defer s.unlockRoom(st)
	if st.gone || !s.roomOpen(st) {
		return false
	}
	st.shadowInAttempt = true
	return true
}

func (s *Service) endShadowAttempt(st *roomState) {
	s.lockState(st)
	st.shadowInAttempt = false
	s.unlockRoom(st)
}

func (s *Service) shadowRoomClosed(st *roomState) bool {
	s.lockState(st)
	defer s.unlockRoom(st)
	return st.gone || !s.roomOpen(st)
}

// finishShadow writes the run's final status once, with tool_calls and tokens,
// and reveals when the room is already closed (the supervisor is second). Under
// the room's lock, so it and CloseRoom agree on who reveals.
func (s *Service) finishShadow(roomID string, st *roomState, status string, tokens int64) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), shadowWriteTimeout)
	defer cancel()
	calls := 0
	if status != "" {
		n, err := s.shadow.CountShadowEvents(ctx, roomID, events.AgentToolCall)
		if err != nil {
			s.log.Warn("room: count shadow tool calls failed", "room", roomID, "err", err)
		}
		calls = n
	}
	s.lockState(st)
	defer s.unlockRoom(st)
	st.shadowActive = false
	if st.graceTimer != nil {
		st.graceTimer.Stop()
		st.graceTimer = nil
	}
	if st.gone {
		return // deleted: its run row went with it
	}
	// A shadow run is one-shot: once it has ended, the room needs no agent
	// owner (a shadow room keeps its lease through the grace after close).
	s.dropLeaseLocked(roomID, st)
	if status != "" {
		now := s.now().UTC()
		closedAt := st.snap.ClosedAt
		if err := s.updateShadowRun(ctx, roomID, func(r *store.ShadowRun) {
			r.Status = status
			if status == store.ShadowHypothesised && len(r.FinalHypothesis) == 0 {
				r.Status = store.ShadowNoHypothesis
			}
			r.FinishedAt = &now
			r.FinishedAfterClose = closedAt != nil && now.After(*closedAt)
			r.ToolCalls = calls
			if tokens > 0 {
				r.Tokens = tokens
			}
		}); err != nil {
			s.log.Warn("room: write shadow run result failed", "room", roomID, "status", status, "err", err)
		}
	}
	if !s.roomOpen(st) {
		s.revealLocked(ctx, roomID, st)
	}
}

// closeShadowLocked applies the close rules of a shadow room (design 3.4) and
// returns the cancel func CloseRoom should call now, or nil. With a grace
// period, an attempt that is running keeps going and a timer cancels it; a run
// that is queued for a slot or waiting to retry is cancelled at once. A room
// whose run has ended is revealed here. Caller holds st's lock, after
// room.closed was appended.
func (s *Service) closeShadowLocked(ctx context.Context, roomID string, st *roomState, cancel context.CancelFunc) context.CancelFunc {
	if st.shadowActive {
		// Grace only for an attempt that is running now. A run that is
		// queued for a slot or backing off before a retry is cancelled at
		// once: it must not start an attempt on a closed room.
		if s.shadowGrace > 0 && st.shadowInAttempt && cancel != nil {
			st.graceTimer = time.AfterFunc(s.shadowGrace, cancel)
			return nil
		}
		return cancel
	}
	s.revealLocked(ctx, roomID, st)
	return cancel
}

// revealLocked appends shadow.revealed once the room is closed and its run has
// ended, then sets revealed_at. A closed room with no run row gets an error row
// first, so it still reveals and counts. It is safe to call more than once:
// the snapshot and the row both say whether the reveal was written. Caller
// holds st's lock.
func (s *Service) revealLocked(ctx context.Context, roomID string, st *roomState) {
	if s.shadow == nil || s.roomOpen(st) {
		return
	}
	if st.snap.shadowRevealed {
		// The event is in the log; a crash may have kept revealed_at unset.
		if _, err := s.shadow.MarkRevealed(ctx, roomID, s.now().UTC()); err != nil {
			s.log.Warn("room: mark shadow run revealed failed", "room", roomID, "err", err)
		}
		return
	}
	closedAt := s.now().UTC()
	if st.snap.ClosedAt != nil {
		closedAt = st.snap.ClosedAt.UTC()
	}
	run, err := s.shadow.GetShadowRun(ctx, roomID)
	if errors.Is(err, store.ErrNotFound) {
		if err := s.shadow.CreateShadowRun(ctx, store.ShadowRun{RoomID: roomID, Status: store.ShadowError, FinishedAt: &closedAt}); err != nil {
			s.log.Warn("room: upsert shadow error row failed", "room", roomID, "err", err)
			return
		}
		run, err = s.shadow.GetShadowRun(ctx, roomID)
	}
	if err != nil {
		s.log.Warn("room: read shadow run for reveal failed", "room", roomID, "err", err)
		return
	}
	if !run.Finished() || run.RevealedAt != nil {
		return
	}
	if _, err := s.emitLocked(ctx, st, roomID, events.ShadowRevealed, systemActor, revealPayload(st.snap.OpenedAt, closedAt, run)); err != nil {
		s.log.Warn("room: append shadow.revealed failed", "room", roomID, "err", err)
		return
	}
	if _, err := s.shadow.MarkRevealed(ctx, roomID, s.now().UTC()); err != nil {
		s.log.Warn("room: mark shadow run revealed failed", "room", roomID, "err", err)
	}
}

// secondsSince is t - from in whole seconds, -1 when t is unset, never below 0.
func secondsSince(from time.Time, t *time.Time) int64 {
	if t == nil {
		return -1
	}
	d := t.Sub(from)
	if d < 0 {
		return 0
	}
	return int64(d / time.Second)
}

// revealPayload holds numbers only, never the hypothesis, so the WebSocket and
// GET /events cannot un-blind a labeller (design 3.4).
func revealPayload(openedAt, closedAt time.Time, run store.ShadowRun) events.ShadowRevealedPayload {
	return events.ShadowRevealedPayload{
		Status:                 run.Status,
		TimeToFirstHypothesisS: secondsSince(openedAt, run.FirstHypothesisAt),
		TimeToFinalHypothesisS: secondsSince(openedAt, run.FinalHypothesisAt),
		HumanMTTRS:             secondsSince(openedAt, &closedAt),
		FinishedAfterClose:     run.FinishedAfterClose,
		ToolCalls:              run.ToolCalls,
		Tokens:                 run.Tokens,
	}
}

// sweepShadow is Load's shadow pass (design 3.7). No shadow run survives a
// restart and none is started again: an open room without a row gets one, a
// queued or running row becomes interrupted, and every closed room whose run
// has ended is revealed. The run rows are read and written outside the room
// locks (sweepShadowRoom); only a reveal, which appends to the room, takes
// the room's lock.
func (s *Service) sweepShadow(ctx context.Context) {
	if s.shadow == nil {
		return
	}
	// With a shared store, a room with an agent lease has its run on another
	// replica (live) or is taken over by the coordination loop (expired): not
	// this sweep's.
	var leases map[string]store.RoomLease
	if s.coord != nil {
		var err error
		if leases, err = s.coord.RoomLeases(ctx); err != nil {
			s.log.Warn("room: shadow sweep: list agent leases failed; sweep skipped", "err", err)
			return
		}
	}
	now := s.now().UTC()
	for id, st := range s.cachedRooms() {
		if _, held := leases[id]; !held {
			s.sweepShadowRoom(ctx, id, st, now)
		}
	}
	ids, err := s.shadow.UnrevealedShadowRuns(ctx)
	if err != nil {
		s.log.Warn("room: shadow sweep: list unrevealed runs failed", "err", err)
		return
	}
	for _, id := range ids {
		// Only a closed shadow room can reveal (revealLocked): the others
		// cost no wait for their lock. Shadow is fixed at create and a room
		// never reopens, so the published snapshot answers both.
		if p := s.published(id); p == nil || !p.Shadow || p.Status == "open" {
			continue
		}
		s.revealCached(ctx, id)
	}
}

// revealCached runs revealLocked on roomID when it is cached.
func (s *Service) revealCached(ctx context.Context, roomID string) {
	if st := s.lockCachedRoom(roomID); st != nil {
		if st.snap.Shadow {
			s.revealLocked(ctx, roomID, st)
		}
		s.unlockRoom(st)
	}
}

// sweepShadowRoom is sweepShadow for one room. The run row is read and
// written with no lock held; the room's lock is taken only to check that no
// run of this process is active before the row is marked interrupted, and
// for a reveal.
//
// The reveal (revealLocked) still reads the run row and appends
// shadow.revealed under the room's lock: finishShadow and CloseRoom decide
// under that lock who reveals, and finishShadow writes the run's final status
// under it, so a row read outside it could reveal stale numbers or reveal
// twice.
func (s *Service) sweepShadowRoom(ctx context.Context, id string, st *roomState, now time.Time) {
	// Shadow is fixed at create and a room never reopens, so the published
	// snapshot is enough to choose; nothing below relies on it staying
	// current but the checks made under the lock.
	p := st.pub.Load()
	if p == nil || !p.Shadow {
		return
	}
	open := p.Status == "open"
	run, err := s.shadow.GetShadowRun(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		if !open {
			s.revealCached(ctx, id) // upserts an error row and reveals
			return
		}
		if s.createShadowRun(ctx, id) != nil {
			return
		}
		run, err = s.shadow.GetShadowRun(ctx, id)
	}
	if err != nil {
		s.log.Warn("room: shadow sweep: read run failed", "room", id, "err", err)
		return
	}
	if run.Status == store.ShadowQueued || run.Status == store.ShadowRunning {
		// A run active in this process writes its own result
		// (finishShadow), and a room that has none now never gets one: a
		// shadow run starts only when its room opens. So the check is made
		// under the lock, and the write after it needs none.
		s.lockState(st)
		active := st.gone || st.shadowActive
		closedAt := st.snap.ClosedAt
		s.unlockRoom(st)
		if !active {
			run.Status = store.ShadowInterrupted
			run.FinishedAt = &now
			run.FinishedAfterClose = closedAt != nil && now.After(*closedAt)
			if err := s.shadow.UpdateShadowRun(ctx, run); err != nil {
				s.log.Warn("room: shadow sweep: mark run interrupted failed", "room", id, "err", err)
			}
		}
	}
	if !open && p.shadowRevealed && run.RevealedAt == nil {
		// The event is in the log (shadowRevealed is never unset): only
		// revealed_at is missing, a row write that needs no room lock.
		if _, err := s.shadow.MarkRevealed(ctx, id, s.now().UTC()); err != nil {
			s.log.Warn("room: mark shadow run revealed failed", "room", id, "err", err)
		}
	}
}

// ---- label, grade, read ----

// Shadow route errors (docs/api.md).
func errRoomOpen() *Error {
	return &Error{Status: 409, Code: "room_open", Msg: "the room is still open; shadow results are available after it closes"}
}

func errNotLabelled() *Error {
	return &Error{Status: 409, Code: "not_labelled", Msg: "label the room blind (POST /api/rooms/{id}/shadow/blind) before reading its shadow run"}
}

func errNotRevealed() *Error {
	return &Error{Status: 409, Code: "not_revealed", Msg: "the shadow run is not revealed yet"}
}

func errShadowNotAllowed() *Error {
	return &Error{Status: 403, Code: "not_allowed", Msg: "labelling or grading a shadow room needs a member of its pipeline"}
}

// canGradeShadow decides whether user may label or grade a room of pipelineID:
// the first layer of owners.KnownLayers that authorizeLayer grants, which is exactly
// CheckExportAccess without the auditor shortcut (design 3.5). grant is the
// string recorded in identity.grant.
func (s *Service) canGradeShadow(pipelineID string, user User) (string, bool) {
	for _, layer := range owners.KnownLayers() {
		if g, err := s.authorizeLayer(pipelineID, user, layer, "", false); err == nil {
			return g, true
		}
	}
	return "", false
}

// lockClosedShadow locks a closed shadow room: 404 for an unknown or live
// room, 409 room_open while it is open. The caller releases it with
// unlockRoom.
func (s *Service) lockClosedShadow(ctx context.Context, roomID string) (*roomState, error) {
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if !st.snap.Shadow {
		s.unlockRoom(st)
		return nil, notFound("not a shadow room")
	}
	if s.roomOpen(st) {
		s.unlockRoom(st)
		return nil, errRoomOpen()
	}
	return st, nil
}

// Label limits (docs/api.md).
const (
	maxRootCauseTerms   = 20
	maxRootCauseTermLen = 200
	maxGradeNotesLen    = 2000
)

// ShadowLabel records the blind label of a closed shadow room (POST
// /api/rooms/{id}/shadow/blind). Once per room.
func (s *Service) ShadowLabel(ctx context.Context, user User, roomID, actualLayer string, terms []string) (events.Event, error) {
	// Shared store: fold what other replicas wrote, reading outside the lock.
	s.syncRoom(ctx, roomID)
	st, err := s.lockClosedShadow(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	grant, ok := s.canGradeShadow(st.snap.PipelineID, user)
	if !ok {
		return events.Event{}, errShadowNotAllowed()
	}
	if !isLayer(actualLayer) {
		return events.Event{}, validation(fmt.Sprintf("actual_layer must be one of %s", strings.Join(events.IncidentLayers(), ", ")))
	}
	if len(terms) < 1 || len(terms) > maxRootCauseTerms {
		return events.Event{}, validation(fmt.Sprintf("root_cause_terms must hold 1 to %d terms", maxRootCauseTerms))
	}
	for _, t := range terms {
		if n := len([]rune(t)); strings.TrimSpace(t) == "" || n > maxRootCauseTermLen {
			return events.Event{}, validation(fmt.Sprintf("each root cause term must be 1 to %d characters", maxRootCauseTermLen))
		}
	}
	if err := rejectUnstorable(terms...); err != nil {
		return events.Event{}, err
	}
	if st.snap.shadowLabelled {
		return events.Event{}, alreadyDecided("this shadow room is already labelled")
	}
	ident := s.identityFor(user, grant)
	e, err := s.emitLocked(ctx, st, roomID, events.ShadowLabelled, s.humanActor(user), events.ShadowLabelledPayload{
		ActualLayer: actualLayer, RootCauseTerms: append([]string{}, terms...), Identity: &ident,
	})
	if err != nil {
		return events.Event{}, mapUnstorable(err)
	}
	s.log.Info("room: shadow room labelled", "room", roomID, "user", user.ID, "grant", grant)
	return e, nil
}

// ShadowGrade records a grade of the revealed run (POST
// /api/rooms/{id}/shadow/grade). Re-grading appends another; the last wins.
func (s *Service) ShadowGrade(ctx context.Context, user User, roomID string, layerCorrect bool, rootCause, notes string) (events.Event, error) {
	// Shared store: fold what other replicas wrote, reading outside the lock.
	s.syncRoom(ctx, roomID)
	st, err := s.lockClosedShadow(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	grant, ok := s.canGradeShadow(st.snap.PipelineID, user)
	if !ok {
		return events.Event{}, errShadowNotAllowed()
	}
	switch rootCause {
	case "yes", "partial", "no":
	default:
		return events.Event{}, validation("root_cause must be yes, partial or no")
	}
	if len([]rune(notes)) > maxGradeNotesLen {
		return events.Event{}, validation(fmt.Sprintf("notes must be at most %d characters", maxGradeNotesLen))
	}
	if err := rejectUnstorable(notes); err != nil {
		return events.Event{}, err
	}
	if !st.snap.shadowLabelled {
		return events.Event{}, errNotLabelled()
	}
	if !st.snap.shadowRevealed {
		return events.Event{}, errNotRevealed()
	}
	ident := s.identityFor(user, grant)
	e, err := s.emitLocked(ctx, st, roomID, events.ShadowGraded, s.humanActor(user), events.ShadowGradedPayload{
		LayerCorrect: layerCorrect, RootCause: rootCause, Notes: notes, Identity: &ident,
	})
	if err != nil {
		return events.Event{}, mapUnstorable(err)
	}
	s.log.Info("room: shadow room graded", "room", roomID, "user", user.ID, "grant", grant)
	return e, nil
}

func isLayer(l string) bool {
	// An actual layer is where the incident was, so the setup layer
	// (environment) is refused (U0).
	return slices.Contains(events.IncidentLayers(), l)
}

// ShadowEvent is a shadow log entry as GET /api/rooms/{id}/shadow returns it:
// the Event plus "shadow": true, with its own seq.
type ShadowEvent struct {
	events.Event
	Shadow bool `json:"shadow"`
}

// ShadowRoomEvents are the room events the eval builder needs (include=room).
// Graded is the last grade; nil fields are null.
type ShadowRoomEvents struct {
	Created  *events.Event `json:"created"`
	Labelled *events.Event `json:"labelled"`
	Graded   *events.Event `json:"graded"`
}

// ShadowView is the body of GET /api/rooms/{id}/shadow.
type ShadowView struct {
	Run    store.ShadowRun   `json:"run"`
	Events []ShadowEvent     `json:"events"`
	Room   *ShadowRoomEvents `json:"room,omitempty"`
}

// ShadowReadOptions are the include= values of GET /api/rooms/{id}/shadow.
type ShadowReadOptions struct {
	Data bool // keep tool_result.data
	Room bool // add the room's created, labelled and last graded events
}

// shadowPage is how many shadow events one store read returns.
const shadowPage = 1000

// Shadow returns a closed, labelled shadow room's run and shadow log. Before
// the blind label it is 409 not_labelled.
func (s *Service) Shadow(ctx context.Context, roomID string, opt ShadowReadOptions) (ShadowView, error) {
	st, err := s.lockClosedShadow(ctx, roomID)
	if err != nil {
		return ShadowView{}, err
	}
	labelled := st.snap.shadowLabelled
	s.unlockRoom(st)
	if !labelled {
		return ShadowView{}, errNotLabelled()
	}
	run, evs, err := s.readShadow(ctx, roomID, opt.Data)
	if err != nil {
		return ShadowView{}, err
	}
	v := ShadowView{Run: run, Events: evs}
	if opt.Room {
		r, err := s.shadowRoomEvents(ctx, roomID)
		if err != nil {
			return ShadowView{}, err
		}
		v.Room = &r
	}
	return v, nil
}

// readShadow reads the run row and the whole shadow log. Without withData,
// tool_result.data is dropped from each payload (F3).
func (s *Service) readShadow(ctx context.Context, roomID string, withData bool) (store.ShadowRun, []ShadowEvent, error) {
	if s.shadow == nil {
		return store.ShadowRun{}, nil, notFound("not a shadow room")
	}
	run, err := s.shadow.GetShadowRun(ctx, roomID)
	if err != nil {
		return store.ShadowRun{}, nil, mapStoreErr(err)
	}
	out := []ShadowEvent{}
	after := int64(0)
	for {
		evs, err := s.shadow.ShadowSince(ctx, roomID, after, shadowPage)
		if err != nil {
			return store.ShadowRun{}, nil, fmt.Errorf("room: read shadow log %s: %w", roomID, err)
		}
		for _, e := range evs {
			if !withData && e.Type == events.AgentToolResult {
				e.Payload = withoutData(e.Payload)
			}
			out = append(out, ShadowEvent{Event: e, Shadow: true})
			after = e.Seq
		}
		if len(evs) < shadowPage {
			break
		}
	}
	return run, out, nil
}

// withoutData removes the "data" member of a tool_result payload. A payload
// that is not an object is returned as is.
func withoutData(raw json.RawMessage) json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return raw
	}
	if _, ok := m["data"]; !ok {
		return raw
	}
	delete(m, "data")
	b, err := json.Marshal(m)
	if err != nil {
		return raw
	}
	return b
}

// shadowRoomEvents finds room.created, shadow.labelled and the last
// shadow.graded in the room log.
func (s *Service) shadowRoomEvents(ctx context.Context, roomID string) (ShadowRoomEvents, error) {
	var r ShadowRoomEvents
	after := int64(0)
	for {
		evs, err := s.store.Since(ctx, roomID, after, 1000)
		if err != nil {
			return r, fmt.Errorf("room: read room log %s: %w", roomID, err)
		}
		for i := range evs {
			e := evs[i]
			switch e.Type {
			case events.RoomCreated:
				r.Created = &e
			case events.ShadowLabelled:
				r.Labelled = &e
			case events.ShadowGraded:
				r.Graded = &e
			}
			after = e.Seq
		}
		if len(evs) < 1000 {
			break
		}
	}
	return r, nil
}

// ---- metrics ----

// ShadowRootCause counts the last grade's root_cause over graded rooms.
type ShadowRootCause struct {
	Yes     int `json:"yes"`
	Partial int `json:"partial"`
	No      int `json:"no"`
}

// ShadowMetricsRoom is one per_room row of GET /api/shadow/metrics.
type ShadowMetricsRoom struct {
	RoomID             string    `json:"room_id"`
	PipelineID         string    `json:"pipeline_id"`
	AlertSource        string    `json:"alert_source"`
	Status             string    `json:"status"`
	OpenedAt           time.Time `json:"opened_at"`
	ClosedAt           time.Time `json:"closed_at"`
	Labelled           bool      `json:"labelled"`
	Graded             bool      `json:"graded"`
	LayerCorrect       *bool     `json:"layer_correct"` // null until graded
	RootCause          string    `json:"root_cause"`    // "" until graded
	TimeToFirstS       int64     `json:"time_to_first_s"`
	TimeToFinalS       int64     `json:"time_to_final_s"`
	HumanMTTRS         int64     `json:"human_mttr_s"`
	ToolCalls          int       `json:"tool_calls"`
	Tokens             int64     `json:"tokens"`
	FinishedAfterClose bool      `json:"finished_after_close"`
}

// ShadowMetrics is the body of GET /api/shadow/metrics. "N of M matched" is
// LayerCorrect of Rooms; every closed shadow room counts in Rooms, errors and
// ungraded rooms included (lead decision, design 3.6).
type ShadowMetrics struct {
	Rooms              int                 `json:"rooms"`
	Labelled           int                 `json:"labelled"`
	Graded             int                 `json:"graded"`
	LayerCorrect       int                 `json:"layer_correct"`
	RootCause          ShadowRootCause     `json:"root_cause"`
	ByStatus           map[string]int      `json:"by_status"`
	MedianTimeToFirstS int64               `json:"median_time_to_first_s"`
	MedianTimeToFinalS int64               `json:"median_time_to_final_s"`
	MedianHumanMTTRS   int64               `json:"median_human_mttr_s"`
	TotalTokens        int64               `json:"total_tokens"`
	TokensAvailable    bool                `json:"tokens_available"`
	PerRoom            []ShadowMetricsRoom `json:"per_room"`
}

// shadowStatuses are every run status, so by_status always has the same keys.
var shadowStatuses = []string{
	store.ShadowQueued, store.ShadowRunning, store.ShadowHypothesised, store.ShadowNoHypothesis,
	store.ShadowError, store.ShadowBudget, store.ShadowInterrupted,
}

// ShadowMetrics aggregates every closed shadow room of pipelineID ("" = all)
// closed at or after since (zero = all).
func (s *Service) ShadowMetrics(ctx context.Context, pipelineID string, since time.Time) (ShadowMetrics, error) {
	return s.shadowMetrics(ctx, pipelineID, since, nil)
}

// shadowMetrics is ShadowMetrics over the runs whose pipeline keep accepts
// (nil = all), for the tenant filter (ShadowMetricsFor).
func (s *Service) shadowMetrics(ctx context.Context, pipelineID string, since time.Time, keep func(pipelineID string) bool) (ShadowMetrics, error) {
	m := ShadowMetrics{ByStatus: map[string]int{}, PerRoom: []ShadowMetricsRoom{},
		MedianTimeToFirstS: -1, MedianTimeToFinalS: -1, MedianHumanMTTRS: -1}
	for _, st := range shadowStatuses {
		m.ByStatus[st] = 0
	}
	if s.shadow == nil {
		return m, nil
	}
	runs, err := s.shadow.ListShadowRuns(ctx, pipelineID, since)
	if err != nil {
		return m, fmt.Errorf("room: shadow metrics: %w", err)
	}
	if keep != nil {
		runs = slices.DeleteFunc(runs, func(r store.ShadowRunRoom) bool { return !keep(r.PipelineID) })
	}
	// The published snapshots are enough: the labels and grades are folded
	// from the log, and no room's lock is waited for. A room that is not
	// cached is loaded, and one another caller is loading is waited for
	// (awaitRoom, bounded by ctx), so no run counts as unlabelled only
	// because its room was mid-load.
	snaps := make(map[string]RoomSnapshot, len(runs))
	for _, r := range runs {
		p := s.published(r.RoomID)
		if p == nil {
			st, _, err := s.awaitRoom(ctx, r.RoomID)
			switch {
			case err == nil:
				p = st.pub.Load()
			case ctx.Err() != nil:
				return m, fmt.Errorf("room: shadow metrics: %w", ctx.Err())
			case isNotFound(err):
				// Deleted since the run list was read: its row counts, with
				// no labels.
			default:
				return m, fmt.Errorf("room: shadow metrics: load %s: %w", r.RoomID, err)
			}
		}
		if p != nil {
			snaps[r.RoomID] = *p
		}
	}

	var first, final, mttr []int64
	for _, r := range runs {
		snap := snaps[r.RoomID]
		row := ShadowMetricsRoom{
			RoomID: r.RoomID, PipelineID: r.PipelineID, AlertSource: r.AlertSource, Status: r.Status,
			OpenedAt: r.OpenedAt, ClosedAt: r.ClosedAt, Labelled: snap.shadowLabelled,
			TimeToFirstS: secondsSince(r.OpenedAt, r.FirstHypothesisAt),
			TimeToFinalS: secondsSince(r.OpenedAt, r.FinalHypothesisAt),
			HumanMTTRS:   secondsSince(r.OpenedAt, &r.ClosedAt),
			ToolCalls:    r.ToolCalls, Tokens: r.Tokens, FinishedAfterClose: r.FinishedAfterClose,
		}
		m.Rooms++
		m.ByStatus[r.Status]++
		if row.Labelled {
			m.Labelled++
		}
		if g := snap.shadowGrade; g != nil {
			lc := g.LayerCorrect
			row.Graded, row.LayerCorrect, row.RootCause = true, &lc, g.RootCause
			m.Graded++
			if lc {
				m.LayerCorrect++
			}
			switch g.RootCause {
			case "yes":
				m.RootCause.Yes++
			case "partial":
				m.RootCause.Partial++
			case "no":
				m.RootCause.No++
			}
		}
		if row.TimeToFirstS >= 0 {
			first = append(first, row.TimeToFirstS)
		}
		if row.TimeToFinalS >= 0 {
			final = append(final, row.TimeToFinalS)
		}
		mttr = append(mttr, row.HumanMTTRS)
		m.TotalTokens += r.Tokens
		if r.Tokens > 0 {
			m.TokensAvailable = true
		}
		m.PerRoom = append(m.PerRoom, row)
	}
	m.MedianTimeToFirstS = median(first)
	m.MedianTimeToFinalS = median(final)
	m.MedianHumanMTTRS = median(mttr)
	return m, nil
}

// median returns the median of v in whole seconds (the mean of the two middle
// values, rounded down, for an even count), or -1 for no values.
func median(v []int64) int64 {
	if len(v) == 0 {
		return -1
	}
	c := slices.Clone(v)
	slices.Sort(c)
	n := len(c)
	if n%2 == 1 {
		return c[n/2]
	}
	return (c[n/2-1] + c[n/2]) / 2
}
