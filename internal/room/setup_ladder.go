package room

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
	"piperoom/internal/setup/plan"
	"piperoom/internal/tools"
)

// The verification ladder runner (U1-8b; docs/design/setup-agent.md §3.4).
//
// The model never reports a pipeline verified: it asks for verification
// (setup_request_verification, U1-7) and this runner, in the room service,
// does the work:
//
//  1. it draws a nonce (16 crypto/rand bytes, hex) and inserts the canary row
//     with it as a system-origin proposal, executed through the ordinary
//     approval and executor path like any other write (T1 by the setup_local
//     policy on a local source, an approval on a shared one);
//  2. it issues each rung's read itself through the room's own read tools,
//     wraps each result as agent.tool_call/agent.tool_result so the room log
//     holds the receipt, and judges the predicate on the structured Data,
//     never on the summary (plan.Ladder);
//  3. it writes setup.verified with every rung's outcome. That is the only
//     writer of the type: the agent's Sink and EmitSetup refuse it.
//
// Rung 8 is the person's: the runner asks for a change (setup.awaiting_change)
// and waits setupLadderChangeWait, then records the wait's end as a rung note.
// A timeout is not a failure: rungs 1 to 7 stay green and setup.verified
// carries the note (U1-14 gates registration on rungs 1 to 7 only).

// TypeSetupVerificationRequested is the event setup_request_verification emits
// (internal/setup/tools/verify.go, U1-7): the agent asks the room's ladder
// runner to verify and nothing more. It is not in internal/events yet; the
// U1-7 PR carries a contract change request adding
//
//	SetupVerificationRequested Type = "setup.verification_requested"
//
// beside SetupVerified. Until a lead decides, this local constant is the
// adapter that request names. The agent's Sink accepts it; setup.verified stays
// refused there (a test).
const TypeSetupVerificationRequested events.Type = "setup.verification_requested"

// setupProposalOriginSystem marks the proposals the server writes itself: the
// ladder's canary insert. internal/events names an agent and a driver origin
// only (ProposalOriginAgent, ProposalOriginDriver); the canary belongs to
// neither — no model wrote it and it is not in the setup plan — so it carries
// its own origin. The U1-8b PR carries a contract change request adding
// events.ProposalOriginSystem = "system"; until then this constant is the
// adapter. setupNeedsPlan accepts it (the canary is not a plan step).
const setupProposalOriginSystem = "system"

// The canary insert the ladder runs, by the action name the source adapter
// registers (internal/setup/remoteclients names the op; the action keeps the
// name). setupLadderCanaryName is the canary table's name: the setup creates it
// through source_create_canary_table, so the ladder only names it.
const (
	setupLadderOpInsertCanary = "source_insert_canary"
	setupLadderCanaryName     = "piperoom_canary"
)

// setupLadderTimings are a run's waits. The zero struct is replaced by
// setupLadderDefaultTimings; tests shorten the waits they cannot sit out.
type setupLadderTimings struct {
	// Approval is how long the canary proposal may wait for its approval and
	// execution before the ladder goes on without it (the person may be
	// deciding; the reads still say something).
	Approval time.Duration
	// Event is rung 5's window: the canary event must be on the topic within
	// it (design §3.4 says 60 s).
	Event time.Duration
	// Change is rung 8's window: the change the person makes (design §3.4
	// says 10 min).
	Change time.Duration
	// Poll is the pause between two reads of a waiting rung.
	Poll time.Duration
	// Step bounds one read (rule 4: a tool call gets a deadline).
	Step time.Duration
	// OpenCheck is how often a wait re-reads the room: a run whose room has
	// closed ends at the next check instead of polling out its window (rung 8
	// alone would otherwise read the pipeline for up to 10 more minutes).
	OpenCheck time.Duration
}

// setupLadderDefaultTimings is the production schedule of design §3.4.
var setupLadderDefaultTimings = setupLadderTimings{
	Approval:  10 * time.Minute,
	Event:     60 * time.Second,
	Change:    10 * time.Minute,
	Poll:      2 * time.Second,
	Step:      10 * time.Second,
	OpenCheck: 15 * time.Second,
}

// errSetupLadderInFlight is the run guard's refusal: one ladder run per room
// at a time on this replica (a second request is 409 already_decided).
var errSetupLadderInFlight = errors.New("room: a verification ladder run is already in flight for this room")

// ladderTimings returns the run's waits: the service's test override when one
// is set, the production schedule otherwise.
func (s *Service) ladderTimings() setupLadderTimings {
	if s.ladderTiming != nil {
		t := *s.ladderTiming
		if t.Poll <= 0 {
			t.Poll = setupLadderDefaultTimings.Poll
		}
		if t.Step <= 0 {
			t.Step = setupLadderDefaultTimings.Step
		}
		if t.OpenCheck <= 0 {
			t.OpenCheck = setupLadderDefaultTimings.OpenCheck
		}
		return t
	}
	return setupLadderDefaultTimings
}

// beginSetupLadder claims the room's one ladder run on this replica.
func (s *Service) beginSetupLadder(roomID string) bool {
	s.ladderMu.Lock()
	defer s.ladderMu.Unlock()
	if s.ladderRuns[roomID] {
		return false
	}
	if s.ladderRuns == nil {
		s.ladderRuns = map[string]bool{}
	}
	s.ladderRuns[roomID] = true
	return true
}

// endSetupLadder releases the room's ladder run.
func (s *Service) endSetupLadder(roomID string) {
	s.ladderMu.Lock()
	defer s.ladderMu.Unlock()
	delete(s.ladderRuns, roomID)
}

// setupLadderRunning reports whether a ladder run is in flight for roomID on
// this replica.
func (s *Service) setupLadderRunning(roomID string) bool {
	s.ladderMu.Lock()
	defer s.ladderMu.Unlock()
	return s.ladderRuns[roomID]
}

// RequestSetupVerification is POST /api/rooms/{id}/setup/verify: the driver
// asks for the ladder now, the person's equivalent of the agent's
// setup_request_verification. It answers before the run (202): the run writes
// its outcome as setup.verified. A room without a paired runner is 409
// no_runner (the reads reach the pipeline through the runner's clients) and a
// second request while a run is in flight is 409 already_decided.
func (s *Service) RequestSetupVerification(ctx context.Context, user User, roomID string) error {
	// lockSetupRoom applies every shared rule: setup on (404 setup_disabled), a
	// setup room (400 not_setup_room), open (409 room_closed), driver only
	// (403 not_driver) and the tenant check (404).
	st, err := s.lockSetupRoom(ctx, user, roomID)
	if err != nil {
		return err
	}
	if !s.runners.ConnectedForRoom(roomID) {
		s.unlockRoom(st)
		return noRunner()
	}
	s.unlockRoom(st)
	if err := s.startSetupLadder(roomID); err != nil {
		if errors.Is(err, errSetupLadderInFlight) {
			return alreadyDecided("a verification ladder run is already in flight for this room")
		}
		return err
	}
	// The click is the driver's: the run renews nothing, so the lease is
	// touched here (a second lock only for that; the run is out either way).
	if st, err := s.lockSetupRoom(ctx, user, roomID); err == nil {
		s.touchDriverLease(ctx, st, roomID)
		s.unlockRoom(st)
	}
	return nil
}

// startSetupLadder claims the room's run guard and starts the run on the
// service lifetime, so a cancelled request or a stopping service ends it
// instead of leaking it; a closed room ends a run's waits at their next
// roomStillOpen check. It returns errSetupLadderInFlight when this replica
// already runs the room's ladder.
func (s *Service) startSetupLadder(roomID string) error {
	if !s.beginSetupLadder(roomID) {
		return errSetupLadderInFlight
	}
	if !s.addWorker() {
		s.endSetupLadder(roomID)
		return fmt.Errorf("room: starting the verification ladder: the service is closing")
	}
	go func() {
		defer s.wg.Done()
		defer s.endSetupLadder(roomID)
		s.runSetupLadder(s.ctx, roomID)
	}()
	return nil
}

// runSetupLadder reads the room's state and runs the ladder if there is one to
// run. Every refusal is a note on the room, never an error: the caller (a
// click or the agent's request) has already been answered.
func (s *Service) runSetupLadder(ctx context.Context, roomID string) {
	snap, err := s.readRoom(ctx, roomID)
	if err != nil || snap == nil || !snap.IsSetupRoom() || snap.Status != "open" {
		return
	}
	if !s.setupCfg.Enabled {
		return
	}
	// The ladder starts from the room's setup.plan: without one there is no
	// run the verified event could belong to (U1-14 gates registration on a
	// verified event of the current plan).
	if snap.Setup.PlanID == "" {
		s.setupLadderNote(ctx, roomID, "verification: this room has no setup plan yet; the ladder runs once the agent records one (setup_plan)")
		return
	}
	prov, ok := snap.SetupProvisional()
	if !ok {
		s.setupLadderNote(ctx, roomID, "verification: the room's provisional pipeline could not be derived; nothing was verified")
		return
	}
	// No sink kind is carried by the setup request, so the ladder is the one
	// without a sink: rung 7 reports "no sink was set up" (U1-8b PR, known gap).
	rungs, err := plan.Ladder(snap.Setup.req.SourceKind, snap.Setup.req.Target, plan.SinkNone)
	if err != nil {
		s.setupLadderNote(ctx, roomID, "verification: "+setupLadderErrText(err)+"; nothing was verified")
		return
	}
	nonce, err := setupLadderNonce()
	if err != nil {
		s.setupLadderNote(ctx, roomID, "verification: a canary nonce could not be drawn; nothing was verified")
		return
	}
	change, err := setupLadderNonce()
	if err != nil {
		s.setupLadderNote(ctx, roomID, "verification: a change key could not be drawn; nothing was verified")
		return
	}
	l := &setupLadder{
		s:           s,
		roomID:      roomID,
		planID:      snap.Setup.PlanID,
		prov:        prov,
		rungs:       rungs,
		nonce:       nonce,
		change:      change,
		canaryTable: setupLadderCanaryTable(prov),
		timings:     s.ladderTimings(),
		started:     time.Now().UTC(),
	}
	l.canaryTopic = setupLadderTopic(prov.TopicPrefix, l.canaryTable)
	l.changeTopic = l.canaryTopic
	l.scope = setupLadderScope(prov, l.canaryTable)
	if len(prov.Connectors) > 0 {
		l.connector = prov.Connectors[0]
	}
	l.nonceHash = setupLadderHash(nonce)
	l.run(ctx)
}

// setupLadder is one run of the verification ladder.
type setupLadder struct {
	s      *Service
	roomID string
	planID string
	prov   owners.Pipeline
	rungs  []plan.Rung
	// scope is the pipeline every read runs under: prov plus the canary
	// table, so the canary topic is one of the pipeline's topics.
	scope owners.Pipeline

	nonce     string // this run's canary key; in the proposal params (the approver previews it), masked everywhere else in the log
	nonceHash string // SHA-256 of nonce, the only form setup.verified carries
	change    string // the key the person's change carries (rung 8)

	canaryTable string
	canaryTopic string
	changeTopic string
	connector   string

	timings setupLadderTimings
	started time.Time

	// The canary proposal this run created and the seq to follow it from;
	// canaryNote says why it could not run ("" when it did).
	canaryProposalID string
	canarySince      int64
	canaryNote       string
}

// run is the ladder in order: the canary insert, rungs 1 to 7 (verified as
// soon as they are decided, so the card does not wait on the person), then
// rung 8 until the change arrives or its window ends.
func (l *setupLadder) run(ctx context.Context) {
	canaryNote := l.insertCanary(ctx)
	var insertedAt time.Time
	if canaryNote == "" {
		// insertCanary left the run's proposal in l; await its execution.
		insertedAt = l.awaitCanary(ctx)
	}

	decided := make([]events.SetupRung, 0, len(l.rungs))
	var latency *int64
	var personRung *plan.Rung
	for i, rung := range l.rungs {
		if rung.Name == plan.RungPersonChange {
			// Found by name, not position: the ladder's order is plan's to
			// keep, and a missing rung 8 must not become an index panic.
			personRung = &l.rungs[i]
			continue
		}
		out := l.evaluate(ctx, rung)
		if rung.Name == plan.RungCanaryEvent {
			switch {
			case out.OK && !insertedAt.IsZero():
				ms := time.Since(insertedAt).Milliseconds()
				latency = &ms
			case !out.OK && l.canaryNote != "":
				// The rung could not be about the topic at all: the canary
				// insert never ran, and that is the note a person needs.
				out.Note = l.canaryNote
			}
		}
		decided = append(decided, out)
		if ctx.Err() != nil {
			return
		}
	}
	// Rungs 1 to 7 first: pipeline_register needs exactly these, and the
	// person's change (rung 8) must not hold them up (design §3.4, §3.9).
	if !l.emitVerified(ctx, decided, latency) || personRung == nil {
		return
	}

	person := l.evaluate(ctx, *personRung)
	if !person.OK {
		// A timeout is a note, not a failed step: nothing ran and nothing is
		// wrong with the pipeline's first seven rungs.
		l.note(ctx, "verification: the change you make did not arrive within "+l.timings.Change.String()+
			"; rungs 1 to 7 are unaffected. Make the change and press Verify again.")
	}
	l.emitVerified(ctx, append(decided, person), latency)
}

// insertCanary proposes and starts the canary insert with this run's nonce. It
// returns "" when the proposal exists (awaitCanary then waits for it) and the
// reason it could not be proposed otherwise.
func (l *setupLadder) insertCanary(ctx context.Context) string {
	acts := l.s.actionsFor(l.prov.ID)
	act, ok := acts.Get(setupLadderOpInsertCanary)
	if !ok {
		l.canaryNote = "the canary insert (" + setupLadderOpInsertCanary + ") is not registered on this server; rung 5 cannot pass"
		return l.canaryNote
	}
	// The nonce is the row's key and the ladder's own marker: 16 random bytes
	// the model never sees or chooses (design §3.4).
	params, err := json.Marshal(struct {
		Table string `json:"table"`
		Nonce string `json:"nonce"`
	}{Table: l.canaryTable, Nonce: l.nonce})
	if err != nil {
		l.canaryNote = "the canary insert's params could not be encoded"
		return l.canaryNote
	}
	p := events.ProposalCreatedPayload{
		Title:  "verification: insert the canary row",
		Origin: setupProposalOriginSystem,
		Risk: fmt.Sprintf("One row is inserted into the setup's own canary table %s so the ladder can follow it to the topic. "+
			"It carries no data: the key is a random nonce the server drew for this run.", l.canaryTable),
		Rollback: "Nothing to undo: the canary row is the ladder's own marker in the table this setup created, and it holds no data.",
		Steps: []events.ProposalStep{{
			Action:      act.Name(),
			Layer:       act.Layer(),
			Description: "Insert this run's canary row.",
			Params:      params,
		}},
	}
	id, ev, err := l.s.createProposal(ctx, l.roomID, p, systemActor, nil)
	if err != nil {
		l.canaryNote = "the canary insert could not be proposed: " + setupLadderErrText(err)
		return l.canaryNote
	}
	l.canaryProposalID = id
	l.canarySince = ev.Seq
	return ""
}

// awaitCanary waits for the run's own canary proposal to finish and returns
// the time it executed. A superseded or stale event (another proposal, a
// previous run's canary) is never this run's result: the proposal id must be
// this run's.
func (l *setupLadder) awaitCanary(ctx context.Context) time.Time {
	if l.timings.Approval <= 0 {
		return time.Time{}
	}
	wctx, cancel := context.WithTimeout(ctx, l.timings.Approval)
	defer cancel()
	ch, err := l.s.store.Subscribe(wctx, l.roomID, l.canarySince)
	if err != nil {
		l.canaryNote = "the canary insert's result could not be followed: " + setupLadderErrText(err)
		return time.Time{}
	}
	check := time.NewTicker(l.timings.OpenCheck)
	defer check.Stop()
	for {
		select {
		case <-check.C:
			// The wait runs on the service lifetime: a closed room ends it
			// early instead of holding the subscription out the window.
			if !l.roomStillOpen(wctx) {
				l.canaryNote = "the room closed before the canary insert ran"
				return time.Time{}
			}
		case <-wctx.Done():
			if ctx.Err() == nil {
				l.canaryNote = "the canary insert is still waiting for its approval; rung 5 cannot pass"
			}
			return time.Time{}
		case e, ok := <-ch:
			if !ok {
				l.canaryNote = "the room's event stream ended before the canary insert ran"
				return time.Time{}
			}
			switch e.Type {
			case events.ActionExecuted:
				p, dok := decode[events.ActionExecutedPayload](e)
				if !dok || p.ProposalID != l.canaryProposalID {
					continue // a stale run's or another proposal's step
				}
				if !p.OK {
					l.canaryNote = "the canary insert did not run: " + setupLadderErrText(errors.New(p.Output))
					return time.Time{}
				}
				return e.At
			case events.ApprovalRejected:
				p, dok := decode[events.ApprovalRejectedPayload](e)
				if !dok || p.ProposalID != l.canaryProposalID {
					continue
				}
				l.canaryNote = "the canary insert was rejected: " + setupLadderErrText(errors.New(p.Reason))
				return time.Time{}
			}
		}
	}
}

// evaluate decides one rung: it opens a step, issues the rung's reads in order
// (repeating them until the rung's window ends for a waiting rung), and
// records the step id and the deciding read's tool_use_id. Both are events of
// this room's log, written after the last executed step.
func (l *setupLadder) evaluate(ctx context.Context, rung plan.Rung) events.SetupRung {
	out := events.SetupRung{Name: rung.Name}
	out.StepID = l.startStep(ctx, rung.Name)
	if rung.NotApplicable != "" {
		// The setup has no such rung (rung 7 without a sink): green with the
		// reason, and no read is issued, so there is no receipt to record.
		out.OK = true
		out.Note = rung.NotApplicable
		return out
	}
	wait := time.Duration(0)
	switch rung.Name {
	case plan.RungCanaryEvent:
		wait = l.timings.Event
	case plan.RungPersonChange:
		wait = l.timings.Change
		l.emit(ctx, events.SetupAwaitingChange, events.SetupAwaitingChangePayload{
			Table: l.canaryTable, ExampleSQL: l.exampleSQL(),
		})
	}
	predicate := rung.Predicate
	switch rung.Name {
	case plan.RungCanaryEvent:
		// The rung is about a keyed change: only a read that answered for this
		// run's nonce may decide it, so a stale event from an earlier run (or
		// any other key) never satisfies it.
		predicate = setupLadderKeyed(l.nonce, predicate)
	case plan.RungPersonChange:
		predicate = setupLadderKeyed(l.change, predicate)
	}
	if wait <= 0 {
		out, _ = l.onePass(ctx, out, rung, predicate, true)
		return out
	}
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	lastOpenCheck := time.Now()
	for {
		// A waiting rung polls inside its window. Its polls are not written to
		// the room: only the read the verdict rests on is (the one that turned
		// the rung green, or the last one when the window ends). A 10-minute
		// wait would otherwise append hundreds of identical tool calls, and the
		// rung's receipt must be the read it was decided on.
		var last *ladderRead
		out, last = l.onePass(wctx, out, rung, predicate, false)
		if out.OK {
			out.ReceiptID = l.recordRead(ctx, out.StepID, last)
			return out
		}
		pause := time.NewTimer(l.timings.Poll)
		select {
		case <-wctx.Done():
			pause.Stop()
			if ctx.Err() != nil {
				return out // the room closed or the service stopped
			}
			out.OK = false
			out.Note = "timed out after " + wait.String() + ": " + out.Note
			// The rung names the read it ended on, as it does when green.
			out.ReceiptID = l.recordRead(ctx, out.StepID, last)
			return out
		case <-pause.C:
		}
		// The run lives on the service lifetime, so a closed room does not
		// cancel wctx: re-read the room now and then and end the wait instead
		// of polling the pipeline for the rest of the window. The verdict is
		// moot either way (emit and emitVerified refuse a closed room).
		if time.Since(lastOpenCheck) >= l.timings.OpenCheck {
			lastOpenCheck = time.Now()
			if !l.roomStillOpen(ctx) {
				out.OK = false
				out.Note = "the room closed before the rung was decided"
				return out
			}
		}
	}
}

// roomStillOpen says whether the run's room is still an open setup room; a
// read error keeps the run going (the next wait re-checks).
func (l *setupLadder) roomStillOpen(ctx context.Context) bool {
	snap, err := l.s.readRoom(ctx, l.roomID)
	if err != nil {
		return true
	}
	return snap != nil && snap.Status == "open"
}

// onePass issues the rung's reads once, in order, until a predicate decides.
// out carries the step id; when record is set each read is written to the room
// log and out.ReceiptID becomes that read's. Without it the reads are only
// made — a waiting rung records the one that decides it — and the last read
// issued is returned so the caller can.
func (l *setupLadder) onePass(ctx context.Context, out events.SetupRung, rung plan.Rung, predicate plan.Predicate, record bool) (events.SetupRung, *ladderRead) {
	var last *ladderRead
	// Each pass starts with no note: a waiting rung's earlier polls, or a
	// read before the deciding one, must not leave their failure on a rung
	// this pass decides.
	out.Note = ""
	for _, tool := range rung.Reads {
		input, err := l.readInput(rung.Name, tool)
		if err != nil {
			out.Note = setupLadderErrText(err)
			continue
		}
		r := l.issueRead(ctx, tool, input)
		last = r
		if record {
			if id := l.recordRead(ctx, out.StepID, r); id != "" {
				out.ReceiptID = id
			}
		}
		if r.Note != "" {
			out.Note = r.Note
		}
		if r.Data == nil {
			continue
		}
		ok, pnote := predicate(r.Data)
		if ok {
			out.OK, out.Note = true, pnote
			return out, last
		}
		if pnote != "" {
			out.Note = pnote
		}
	}
	return out, last
}

// readInput builds the tool input one rung's read needs. Every value comes
// from the room (the provisional pipeline, the registered target, this run's
// keys), never from model text.
func (l *setupLadder) readInput(rungName, tool string) (map[string]any, error) {
	switch tool {
	case "connect_status", "debezium_metrics":
		if l.connector == "" {
			return nil, fmt.Errorf("the provisional pipeline names no connector")
		}
		return map[string]any{"connector": l.connector}, nil
	case "kafka_topic_offsets":
		return map[string]any{"topic": l.canaryTopic}, nil
	case "kafka_find_by_key":
		key, topic := l.nonce, l.canaryTopic
		if rungName == plan.RungPersonChange {
			key, topic = l.change, l.changeTopic
		}
		// since is this run's start: an event from an earlier run is outside
		// the scan as well as keyed differently.
		return map[string]any{"topic": topic, "key": key, "since": l.started.Format(time.RFC3339)}, nil
	case "schema_registry_subjects":
		return map[string]any{"prefix": l.canaryTopic}, nil
	case "warehouse_row":
		return nil, fmt.Errorf("no sink was set up")
	}
	return nil, fmt.Errorf("the ladder has no input for %s", cleanPlanName(tool))
}

// ladderRead is one read the ladder issued: the tool, its input and the
// answer, whether or not the room log holds it yet (issueRead makes the read,
// recordRead writes the agent.tool_call and agent.tool_result pair). Data is
// the read's own Data, which the rung's predicate judges; LogData is its
// redacted form, which the event carries (truncated). Summary is redacted.
type ladderRead struct {
	Tool    string
	Input   json.RawMessage
	UseID   string
	Summary string
	Data    json.RawMessage
	LogData json.RawMessage
	IsError bool
	TookMS  int64
	// Note says why the read did not answer: "" when it did.
	Note string
}

// issueRead runs one read-only tool and returns its answer. It writes nothing
// to the room: recordRead does, once, for the read a rung is decided on.
func (l *setupLadder) issueRead(ctx context.Context, tool string, input map[string]any) *ladderRead {
	r := &ladderRead{Tool: tool}
	raw, err := json.Marshal(input)
	if err != nil {
		r.Note = "the read's input could not be encoded"
		return r
	}
	r.Input = raw
	t, ok := l.s.setupToolsFor(l.prov.ID).Get(tool)
	if !ok {
		r.Note = "the read " + cleanPlanName(tool) + " is not available in this room"
		return r
	}
	if t.Kind() != tools.KindRead {
		// The ladder reads; it never calls a room-writing tool.
		r.Note = "the tool " + cleanPlanName(tool) + " is not a read"
		return r
	}
	// The read tools refuse a call with no pipeline scope (tools.ErrNoScope);
	// the ladder runs on the service lifetime, so it adds the room's own.
	rctx, cancel := context.WithTimeout(tools.WithPipeline(ctx, l.scope), l.timings.Step)
	start := time.Now()
	res, rerr := t.Run(rctx, raw)
	cancel()
	r.TookMS = time.Since(start).Milliseconds()
	r.Summary = res.Summary
	r.IsError = rerr != nil
	if rerr != nil {
		r.Summary = setupLadderErrText(rerr)
		if r.Summary == "" {
			r.Summary = "the read failed"
		}
		r.Note = "the read " + cleanPlanName(tool) + " failed: " + r.Summary
		return r
	}
	r.Summary, r.Data, r.LogData = redactLadderRead(res.Summary, res.Data)
	return r
}

// redactLadderRead masks secrets in a successful read's summary and data the
// way the agent's scrubToolResult does: the room log only ever gets this form
// (and recordRead masks the run's nonce on top, which redact has no rule for).
// The rung's predicate keeps judging the raw Data, so a masked value never
// turns a rung red. Encoding the data can
// run a tool type's own MarshalJSON, which may panic; the panic drops the data
// instead of taking the run down.
func redactLadderRead(summary string, data any) (string, json.RawMessage, json.RawMessage) {
	masked, _ := redact.Text(summary)
	if data == nil {
		return masked, nil, nil
	}
	var raw, logged json.RawMessage
	func() {
		defer func() {
			if recover() != nil {
				raw, logged = nil, nil
			}
		}()
		if b, err := json.Marshal(data); err == nil {
			raw = b
		}
		red, _ := redact.JSONStats(data)
		if b, err := json.Marshal(red); err == nil {
			logged = b
		}
	}()
	return masked, raw, logged
}

// maskNonce hides this run's canary nonce behind its SHA-256 anywhere the room
// log would carry it. redact has no rule for a bare 32-hex value, so the
// replacement is explicit: only the nonce's hash may appear in an event
// (setup.verified carries NonceHash; the proposal's params are the one
// designed exception, which the approver previews). The change key is not
// masked: setup.awaiting_change hands it to the person in example_sql.
func (l *setupLadder) maskNonce(s string) string {
	if l.nonce == "" {
		return s
	}
	return strings.ReplaceAll(s, l.nonce, "sha256:"+l.nonceHash)
}

// recordRead writes one read as an agent.tool_call and agent.tool_result of
// the rung's step, so the room log holds the receipt of every read a rung's
// verdict rests on, and returns the read's tool_use_id ("" when the read never
// issued, or the room refused the append). The nonce is masked in everything
// the events carry: rung 5's input is key=nonce, and a tool's summary may echo
// its input.
func (l *setupLadder) recordRead(ctx context.Context, stepID string, r *ladderRead) string {
	if r == nil || r.Input == nil {
		return ""
	}
	useID := NewID("tu_")
	if !l.emit(ctx, events.AgentToolCall, events.AgentToolCallPayload{
		StepID: stepID, ToolUseID: useID, Tool: r.Tool, Input: json.RawMessage(l.maskNonce(string(r.Input))),
	}) {
		return ""
	}
	r.UseID = useID
	payload := events.AgentToolResultPayload{
		StepID: stepID, ToolUseID: useID, Tool: r.Tool,
		Summary: l.maskNonce(r.Summary), IsError: r.IsError, TookMS: r.TookMS,
	}
	if r.LogData != nil {
		masked := json.RawMessage(l.maskNonce(string(r.LogData)))
		payload.Data = masked
		if max := l.s.limits.EventMaxDataBytes; max > 0 && int64(len(masked)) > max {
			if cut, truncated := events.TruncateData(masked, int(max)); truncated {
				payload.Data, payload.Truncated = cut, true
			}
		}
	}
	l.emit(ctx, events.AgentToolResult, payload)
	return useID
}

// startStep opens the rung's step and returns its id. The step is the record
// whose id setup.verified's rung names.
func (l *setupLadder) startStep(ctx context.Context, rung string) string {
	id := NewID("st_")
	if !l.emit(ctx, events.AgentStepStart, events.AgentStepStartPayload{
		StepID: id, Title: "verification: " + rung,
	}) {
		return ""
	}
	return id
}

// emitVerified writes setup.verified with rungs. It refuses once the room's
// current plan is no longer the run's: a verified event for a superseded plan
// says nothing about what would be registered.
func (l *setupLadder) emitVerified(ctx context.Context, rungs []events.SetupRung, latency *int64) bool {
	snap, err := l.s.readRoom(ctx, l.roomID)
	if err != nil || snap == nil || !snap.IsSetupRoom() || snap.Status != "open" {
		return false
	}
	if snap.Setup.PlanID != l.planID {
		l.note(ctx, "verification: the setup plan changed during this run; this run's outcome is not recorded. Request verification again for the new plan.")
		return false
	}
	return l.emit(ctx, events.SetupVerified, events.SetupVerifiedPayload{
		PlanID:              l.planID,
		NonceHash:           l.nonceHash,
		Rungs:               append([]events.SetupRung{}, rungs...),
		FirstEventLatencyMS: latency,
	})
}

// emit appends one system event of the run. A closed room or a failed append
// ends the emit quietly (the run is a background job; its caller is gone).
func (l *setupLadder) emit(ctx context.Context, typ events.Type, payload any) bool {
	st, err := l.s.lockRoom(ctx, l.roomID)
	if err != nil {
		return false
	}
	defer l.s.unlockRoom(st)
	if !l.s.roomOpen(st) {
		return false
	}
	if _, err := l.s.emitLocked(ctx, st, l.roomID, typ, systemActor, payload); err != nil {
		l.s.log.Warn("room: verification ladder event not recorded", "room", l.roomID, "type", typ, "err", maskExecErr(err))
		return false
	}
	return true
}

// note writes one plain note for the room's feed (a system agent.message).
func (l *setupLadder) note(ctx context.Context, text string) {
	l.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: truncateDetail(maskExecOutput(text))})
}

// setupLadderNote is l.note for a run that never started.
func (s *Service) setupLadderNote(ctx context.Context, roomID, text string) {
	s.emitAs(ctx, roomID, systemActor, events.AgentMessage, events.AgentMessagePayload{Text: truncateDetail(maskExecOutput(text))})
}

// exampleSQL is the statement the UI shows for rung 8. It inserts one row into
// the canary table (whose shape the setup created: a nonce column) with the
// key this run waits for, so the answer is exact and the person runs one
// statement. The source driver of U1-9 will offer the real table's own example
// instead (design §3.4: "from the source driver, never from the model").
func (l *setupLadder) exampleSQL() string {
	return "INSERT INTO " + l.canaryTable + " (nonce) VALUES ('" + l.change + "');"
}

// setupLadderNonce draws the run's key: 16 crypto/rand bytes in hex, as
// docs/design/setup-agent.md §3.4 says.
func setupLadderNonce() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("room: drawing a ladder nonce: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// setupLadderHash is the only form of the nonce setup.verified carries.
func setupLadderHash(nonce string) string {
	sum := sha256.Sum256([]byte(nonce))
	return hex.EncodeToString(sum[:])
}

// setupLadderCanaryTable is the canary table in the schema of the pipeline's
// first source table: the setup creates it there, and the connector publishes
// it like any table of the schema.
// A source table name comes from the setup request, and the schema part ends
// up in the topic name and in exampleSQL's statement, which the person is
// asked to run: a part that is not a plain identifier (rule 6) is skipped.
func setupLadderCanaryTable(prov owners.Pipeline) string {
	for _, t := range prov.SourceTables {
		if i := strings.LastIndex(t, "."); i > 0 && i < len(t)-1 && setupLadderIdents(t[:i]) {
			return t[:i] + "." + setupLadderCanaryName
		}
	}
	return "public." + setupLadderCanaryName
}

// setupLadderScope is the pipeline the ladder's reads run under: the
// provisional pipeline plus the canary table. The setup creates that table
// (source_create_canary_table) and the connector publishes it, but it is not
// one of the tables the request named, so without it the canary topic is out
// of the pipeline's topic scope (tools.Scope.Topics) and rungs 3, 5, 6 and 8
// are refused. The copy leaves prov itself, and the actions' scope, as is.
func setupLadderScope(prov owners.Pipeline, canaryTable string) owners.Pipeline {
	scope := prov
	scope.SourceTables = append([]string{}, prov.SourceTables...)
	if !slices.Contains(scope.SourceTables, canaryTable) {
		scope.SourceTables = append(scope.SourceTables, canaryTable)
	}
	return scope
}

// setupLadderIdentRe is AGENTS.md rule 6's identifier shape.
var setupLadderIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// setupLadderIdents reports whether every dotted part of name is a plain
// identifier.
func setupLadderIdents(name string) bool {
	for _, part := range strings.Split(name, ".") {
		if !setupLadderIdentRe.MatchString(part) {
			return false
		}
	}
	return true
}

// setupLadderTopic is the topic a table of the pipeline reaches
// (owners.yaml: "<topic_prefix>.<schema>.<table>").
func setupLadderTopic(prefix, table string) string { return prefix + "." + table }

// setupLadderKeyed wraps a rung's predicate so it only accepts an answer about
// key: a read that answered for another key (a previous run's nonce, a stale
// event) leaves the rung red, whatever its found flag says.
func setupLadderKeyed(key string, predicate plan.Predicate) plan.Predicate {
	return func(data json.RawMessage) (bool, string) {
		var st struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal(data, &st); err != nil || st.Key != key {
			return false, "the read did not answer for this run's key"
		}
		return predicate(data)
	}
}

// setupLadderErrText makes an error safe for the room: masked and clipped. Tool
// and endpoint errors are untrusted text, never a claim about the pipeline.
func setupLadderErrText(err error) string {
	if err == nil {
		return ""
	}
	return truncateDetail(maskExecOutput(err.Error()))
}
