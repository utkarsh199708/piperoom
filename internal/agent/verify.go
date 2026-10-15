package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// verify-after-fix: once the last step of a proposal has succeeded, the agent
// re-checks the pipeline before it claims recovery. The room delivers each
// executed step as a HumanInput{Kind:"action_result"} whose ProposalID and
// StepIndex name the step and whose OK says whether it succeeded (see
// internal/room); Text is only for display. Because the agent created the
// proposal itself, it can match those fields back to the proposal and know
// whether the step is the last one.
//
// Recovery needs two things. The layer checks of the touched layers must pass,
// and the data must flow: every sink table of the alert's pipeline must receive
// rows after the fix. A sink table with no known timestamp column, or a pipeline
// with no or too many sink tables, is "data flow not verified" and never counts
// as recovered.
//
// The layers do not settle at once (Kafka Connect restarts asynchronously), so
// the layer checks are polled (verify_settle.go) before any verdict is reached.
// The verdict is then announced once, as agent.verification; a later repeat of
// the last step's result gets the Repeat form of the event and no new checks.

const (
	// freshnessAttempts is how many warehouse_freshness reads follow the check
	// baseline. Rows must show up in one of them for a sink to pass.
	freshnessAttempts = 3
	// maxSinkTables bounds the data-flow check so one alert never fans out into
	// an unbounded run of warehouse reads.
	maxSinkTables = 5
	// verifyEvidenceBytes caps one tool summary quoted in the verdict message.
	verifyEvidenceBytes = 500
	// verifyTag opens every verification line given to the model, so the model
	// can tell a verdict from ordinary input.
	verifyTag = "[verification] "
	// recoveredHead opens the verdict of a proposal whose checks all passed.
	// The wording says data is flowing again, not that the sink has caught up:
	// the check sees new rows after the fix, not the whole backlog.
	recoveredHead = "recovered: every layer check passed and data is flowing again (new rows arrived after the fix). This does not show that the sink has caught up.\n"
	// recoveredConfigHead opens the verdict of a proposal that only changed
	// schema configuration. No data path was touched, so it claims no flow.
	recoveredConfigHead = "recovered: every layer check passed. The proposal only changed schema configuration, so data flow was not part of this verdict.\n"
	// recoveredIdleHead opens the verdict of a proposal whose layer checks all
	// passed while the source had no new change: data flow cannot be judged.
	recoveredIdleHead = "recovered: every layer check passed; no new source changes to confirm data flow.\n"
	// idleDetail is the data-flow detail of that verdict.
	idleDetail = "inconclusive: the source was idle (the replication slots show no WAL the connector did not consume), so no new rows could be expected"
	// inconclusiveHead opens the verdict of a proposal whose layer checks all
	// passed while the sinks got no rows and the data topics did not grow, but
	// the source side could not prove it was idle: the connector may not be
	// capturing. Neither recovered nor NOT recovered (see the idle-source rule).
	inconclusiveHead = "inconclusive: no source changes observed; connector may not be capturing. Every layer check passed, but no new rows reached the sinks and the source could not be shown to be idle.\n"
	// inconclusiveDetail is the data-flow detail of that verdict.
	inconclusiveDetail = "inconclusive: no source changes observed; connector may not be capturing"
	// waitingDetail is the status detail of a recovered episode parked for input.
	waitingDetail = "pipeline recovered; waiting for input"
	// dataFlowInfoOnly is the data-flow detail of a verdict that did not need
	// data flow (a compatibility restore): the reading is shown, not judged.
	dataFlowInfoOnly = "information only, not part of this verdict"
)

// proposalRecord is what the agent remembers about a proposal it created, so a
// later action_result can be matched to its steps and layers.
type proposalRecord struct {
	id    string
	steps []events.ProposalStep
	// verified is set once the verdict for the last step is decided, and
	// recovered holds that verdict. A repeated last-step result reuses it.
	verified  bool
	recovered bool
	// baseline is each sink table's newest row time when the proposal was made.
	// Nil when none was taken (for example after an agent restart), and then the
	// check window alone decides the data flow.
	baseline map[string]time.Time
	// topicEnds is the summed end offset of each source data topic when the
	// proposal was made. Nil when not taken; then the check window decides.
	topicEnds map[string]int64
	// slots is the replication slots when the proposal was made, for the
	// source side of the idle-source rule. Nil when not taken.
	slots []slotPosition
	// tables is the change counters of the pipeline's source tables
	// (pg_table_activity) when the proposal was made. Nil when not taken.
	tables map[string]tableCount
	// verdict names the verdict once verified: "recovered", "NOT recovered" or
	// "inconclusive". A repeated last-step result repeats it.
	verdict string
}

// proposalTracker remembers the proposals the agent created during this run,
// and the proposals of earlier runs of the room that this run learned about (a
// room recap, or a lookup when their step results arrive; resume.go). It is
// per-runner state, never global. Proposals of earlier runs have no creation
// baselines: their checks rely on the check window alone.
type proposalTracker struct {
	mu   sync.Mutex
	byID map[string]proposalRecord
	// flows follows each proposal from approval to executed steps (approvals.go).
	flows map[string]*execFlow
	// unverifiable are proposals of earlier runs that could not be loaded; the
	// room has been told once that they are not verified.
	unverifiable map[string]bool
}

func newProposalTracker() *proposalTracker {
	return &proposalTracker{byID: map[string]proposalRecord{}, flows: map[string]*execFlow{}, unverifiable: map[string]bool{}}
}

// markUnverifiable records that a proposal could not be loaded and reports
// whether this is the first time, so the room hears it once.
func (t *proposalTracker) markUnverifiable(id string) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.unverifiable[id] {
		return false
	}
	t.unverifiable[id] = true
	return true
}

// record remembers a proposal the agent created. approvers is every layer the
// room requires for it: the step layers plus the impacted ones (the proposer's
// and each action's Impact layers), the same set the server stores as
// RequiredLayers. Nil falls back to the step layers alone.
func (t *proposalTracker) record(id string, steps []events.ProposalStep, approvers []string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.byID[id] = proposalRecord{id: id, steps: steps}
	t.flows[id] = newExecFlow(steps, approvers)
}

func (t *proposalTracker) get(id string) (proposalRecord, bool) {
	if t == nil {
		return proposalRecord{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.byID[id]
	return rec, ok
}

// markVerified stores the verdict for a proposal's last step; word names it
// (see verdictWord).
func (t *proposalTracker) markVerified(id string, recovered bool, word string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.byID[id]
	if !ok {
		return
	}
	rec.verified = true
	rec.recovered = recovered
	rec.verdict = word
	t.byID[id] = rec
}

// setTopicEnds stores the source-topic offsets of a proposal already recorded.
func (t *proposalTracker) setTopicEnds(id string, ends map[string]int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.byID[id]
	if !ok {
		return
	}
	rec.topicEnds = ends
	t.byID[id] = rec
}

// setTables stores the proposal-time table counters of a proposal already recorded.
func (t *proposalTracker) setTables(id string, tables map[string]tableCount) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.byID[id]
	if !ok {
		return
	}
	rec.tables = tables
	t.byID[id] = rec
}

// setSlots stores the proposal-time slot readings of a proposal already recorded.
func (t *proposalTracker) setSlots(id string, slots []slotPosition) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.byID[id]
	if !ok {
		return
	}
	rec.slots = slots
	t.byID[id] = rec
}

// setBaseline stores the creation baseline of a proposal already recorded.
func (t *proposalTracker) setBaseline(id string, baseline map[string]time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.byID[id]
	if !ok {
		return
	}
	rec.baseline = baseline
	t.byID[id] = rec
}

// actionResultFields reads which proposal step an action_result is about from
// the input's fields. ok is false when a field is missing (for example an event
// replayed from before the fields existed); the text is only for display.
func actionResultFields(in HumanInput) (id string, step int, ok bool) {
	if in.ProposalID == "" || in.StepIndex == nil {
		return "", 0, false
	}
	return in.ProposalID, *in.StepIndex, true
}

// actionOutcome is what a batch of inputs means for the loop.
type actionOutcome struct {
	// recovered is true when a last step is verified as recovered in this batch,
	// or was verified as recovered before (a repeat).
	recovered bool
	// broken is true when the batch says the pipeline is not known to be
	// recovered: a step failed or is unknown, or a last step was verified as not
	// recovered (including a repeat of that verdict).
	broken bool
	// needsModel is true when the model must read this batch: a person spoke,
	// or a step's outcome is failed or unknown.
	needsModel bool
	// statusSet is true when this batch already emitted a working status with a
	// detail (a compatibility restore is pending), which a plain working status
	// must not overwrite at once.
	statusSet bool
	// notes are the [verification] lines for the model, to be sent in the same
	// user message as the inputs they judge.
	notes []string
}

// handleActionResults runs verify-after-fix for every input in the batch and
// updates recoveredIdle: a recovery sets it, and anything that breaks the
// recovery clears it. It never ends the run. The done status is emitted by
// verifyActionResult when a recovery is first confirmed.
func (r *runner) handleActionResults(ctx context.Context, inputs []HumanInput) actionOutcome {
	var out actionOutcome
	out.needsModel = hasHumanInput(inputs)
	for _, in := range inputs {
		if in.Kind != "action_result" {
			continue
		}
		one := r.verifyActionResult(ctx, in)
		out.recovered = out.recovered || one.recovered
		out.broken = out.broken || one.broken
		out.needsModel = out.needsModel || one.needsModel
		out.statusSet = out.statusSet || one.statusSet
		out.notes = append(out.notes, one.notes...)
	}
	switch {
	case out.broken:
		r.recoveredIdle = false
	case out.recovered:
		r.recoveredIdle = true
	}
	// A verdict is going to the model: from here on the main model reads the
	// evidence (see pickModel).
	if len(out.notes) > 0 {
		r.verified = true
	}
	return out
}

// verifyActionResult handles one action_result input. Recovery is verified only
// from OK == true on the last step of a tracked proposal. A failed step is
// reported and the model runs; an unknown outcome is never turned into a
// recovery claim.
func (r *runner) verifyActionResult(ctx context.Context, in HumanInput) actionOutcome {
	id, step, parsed := actionResultFields(in)
	switch {
	case in.OK == nil:
		return actionOutcome{broken: true, needsModel: true, notes: []string{verifyTag + unknownOutcomeText(id, step, parsed)}}
	case !*in.OK:
		r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: failedStepText(id, step, parsed)})
		return actionOutcome{broken: true, needsModel: true, notes: []string{verifyTag + failedStepText(id, step, parsed)}}
	}
	if !parsed {
		return actionOutcome{}
	}
	rec, ok := r.proposals.get(id)
	if !ok {
		// An earlier run of the room made it (the restart case): load it, so its
		// last step is verified like this run's own. A proposal that cannot be
		// loaded is recorded as not verified, never as recovered.
		var why string
		if rec, why = r.loadEarlierProposal(ctx, id); why != "" {
			return r.notVerifiedEarlier(ctx, id, why)
		}
	}
	if step != len(rec.steps)-1 {
		// Not the last step yet: no verification, the normal loop continues.
		return actionOutcome{}
	}
	if rec.verified {
		// A repeated last-step result never runs the checks again, never emits
		// done again and never announces a second verdict: it gets the Repeat
		// form of agent.verification, which clients keep from overwriting the
		// full one. The model hears the verdict already reached.
		r.emit(ctx, events.AgentVerification, events.AgentVerificationPayload{
			ProposalID: id,
			Checks:     []events.VerificationCheck{},
			Recovered:  rec.recovered,
			Verdict:    "already verified: " + verdictWord(rec) + "; no checks were run again",
			Repeat:     true,
		})
		return actionOutcome{
			recovered: rec.recovered,
			broken:    !rec.recovered,
			notes:     []string{verifyTag + "already verified: " + verdictWord(rec) + "; no checks were run again"},
		}
	}

	v := r.verifyProposal(ctx, rec)
	if v.aborted {
		// The run is ending. A verdict reached half way would be a guess, and
		// marking the proposal verified would stop a later run from checking it.
		return actionOutcome{}
	}
	recovered, head, evidence := v.recovered, v.head, v.evidence
	var followUp string
	if recovered {
		followUp = compatFollowUp(rec.steps)
	}
	r.announceVerdict(ctx, id, v, compatFollowUpRoom(rec.steps), followUp)
	r.proposals.markVerified(id, recovered, v.word())
	if recovered {
		r.trackNoneSubjects(rec.steps)
	}
	out := actionOutcome{
		recovered: recovered,
		broken:    !recovered,
		notes:     []string{verifyTag + head + wrapToolOutput("verify", "", evidence) + followUp},
	}
	switch {
	case recovered && followUp != "":
		// The model must read the follow-up and get one turn before the episode
		// can park as done, so it can propose the restore. If it does not, the
		// episode parks as done at the end of that turn, like any recovery.
		out.needsModel = true
		out.statusSet = true
		r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working", Detail: "pipeline recovered; compatibility restore pending"})
	case recovered:
		r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "done", Detail: "pipeline recovered"})
	}
	return out
}

// announceVerdict tells the room the verdict on a proposal, once: as the typed
// agent.verification event, which replaces the plain-text verdict message.
// roomNote is the follow-up written for people (compatFollowUpRoom); followUp
// is the model's version with its guidance, which never reaches the room.
//
// A room service that does not know the type yet refuses it (an older build
// during a rolling upgrade). Then the same verdict goes out as one agent.message
// so the room is never left without it; the message is the fallback, never sent
// next to the event.
func (r *runner) announceVerdict(ctx context.Context, proposalID string, v verifyOutcome, roomNote, followUp string) {
	err := r.tryEmit(ctx, events.AgentVerification, events.AgentVerificationPayload{
		ProposalID: proposalID,
		Checks:     v.checks,
		DataFlow:   v.flow,
		Recovered:  v.recovered,
		Verdict:    strings.TrimSuffix(v.head, "\n"),
	})
	if err == nil {
		// The follow-up is a warning about what the fix left behind, not a second
		// announcement of the verdict: the room keeps seeing it.
		if note := strings.TrimSpace(roomNote); note != "" {
			r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: note})
		}
		return
	}
	r.log.Warn("agent: verification event not accepted; announcing the verdict as a message", "proposal", proposalID, "err", err)
	r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: v.head + v.evidence + roomNote})
}

// tryEmit is emit that returns the sink's error instead of logging it.
func (r *runner) tryEmit(ctx context.Context, typ events.Type, payload any) error {
	r.emitMu.Lock()
	defer r.emitMu.Unlock()
	return r.deps.Sink.Emit(ctx, typ, payload)
}

// compatFollowUp tells the model that a recovered fix left a schema subject at
// compatibility NONE, which lets data through that the old contract cannot read.
// It returns one line per such subject, each starting on a new line, or "" when
// the proposal left none. A later step of the same proposal that sets the subject
// to another level counts as the restore already done. The earlier level is
// never parsed here: the model reads it from the action result or the schema
// tools.
func compatFollowUp(steps []events.ProposalStep) string {
	order, final := compatFinalLevels(steps)
	var b strings.Builder
	for _, subject := range order {
		if final[subject] != "NONE" {
			continue
		}
		fmt.Fprintf(&b, "\nfollow-up: %s compatibility is still NONE. Propose a separate fix that restores the level shown before \"→\" in the schema_set_compatibility step result (or read it with the schema tools).", followUpSubject(subject))
	}
	return b.String()
}

// compatFollowUpRoom is compatFollowUp for people: the same subjects, each on a
// new line, with a sentence that says what is still off. How to restore it is
// guidance for the model (QA T11): it stays in the [verification] note.
func compatFollowUpRoom(steps []events.ProposalStep) string {
	order, final := compatFinalLevels(steps)
	var b strings.Builder
	for _, subject := range order {
		if final[subject] != "NONE" {
			continue
		}
		fmt.Fprintf(&b, "\nfollow-up: %s compatibility is still NONE, so data the old schema cannot read can get through. Its earlier level should be restored with a separate fix.", followUpSubject(subject))
	}
	return b.String()
}

// compatFinalLevels returns the subjects that schema_set_compatibility steps set,
// in first-seen order, with the upper-cased level each ends at: a later step on
// the same subject overrides an earlier one. Steps without a subject are skipped.
func compatFinalLevels(steps []events.ProposalStep) ([]string, map[string]string) {
	final := map[string]string{}
	var order []string
	for _, s := range steps {
		if s.Action != "schema_set_compatibility" {
			continue
		}
		subject, ok := stringParam(s.Params, "subject")
		if !ok {
			continue
		}
		level, _ := stringParam(s.Params, "level")
		if _, seen := final[subject]; !seen {
			order = append(order, subject)
		}
		final[subject] = strings.ToUpper(level)
	}
	return order, final
}

// followUpSubject renders a subject name for the follow-up line, which sits in a
// verification block outside any untrusted wrapper. The name comes from a step
// the model proposed, so a name that is not a plain token is marked, stripped of
// any verification tag and quoted, and cannot add lines or forge a verdict.
func followUpSubject(s string) string {
	if isPlainSubject(s) {
		return s
	}
	return strconv.Quote(verificationTag.ReplaceAllString(markInvisible(s), "(verification)"))
}

// isPlainSubject reports whether s uses only the characters real subjects use,
// such as pg.public.orders-value.
func isPlainSubject(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case strings.ContainsRune("._:/-", r):
		default:
			return false
		}
	}
	return true
}

// parked reports whether the run waits for human input instead of calling the
// model: the pipeline is recovered and nothing in the batch needs a turn.
func (r *runner) parked(o actionOutcome) bool {
	return r.recoveredIdle && !o.needsModel
}

// emitWaiting reports a recovered episode that is parked for input. It stays
// done, with the reason, rather than reporting idle.
func (r *runner) emitWaiting(ctx context.Context) {
	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "done", Detail: waitingDetail})
}

// verdictWord names a stored verdict for the model.
func verdictWord(rec proposalRecord) string {
	if rec.verdict != "" {
		return rec.verdict
	}
	if rec.recovered {
		return "recovered"
	}
	return "NOT recovered"
}

// word names the verdict: "recovered", "NOT recovered" or "inconclusive".
func (v verifyOutcome) word() string {
	switch {
	case v.recovered:
		return "recovered"
	case v.inconclusive:
		return "inconclusive"
	default:
		return "NOT recovered"
	}
}

// failedStepText tells the room a fix step did not succeed. Recovery is not
// checked in that case, and the agent keeps investigating.
func failedStepText(id string, step int, parsed bool) string {
	if !parsed {
		return "A fix step failed. I have not verified recovery and will keep investigating."
	}
	return fmt.Sprintf("Fix step failed: proposal %q step %d did not succeed. I have not verified recovery and will keep investigating.", id, step)
}

// unknownOutcomeText tells the model a step's outcome is unknown, so it does not
// assume the step worked.
func unknownOutcomeText(id string, step int, parsed bool) string {
	if !parsed {
		return "not verified: a step outcome is unknown; recovery is not claimed."
	}
	return fmt.Sprintf("not verified: the outcome of proposal %q step %d is unknown; recovery is not claimed.", id, step)
}

// verifyOutcome is the verdict on one proposal, before it is announced.
type verifyOutcome struct {
	recovered bool
	// head opens the model note and the legacy message; it ends in a newline.
	// The agent.verification verdict is the same text without it.
	head string
	// evidence is one line per check and per sink, for the model note.
	evidence string
	// checks and flow are the structured form of the evidence, for the event.
	checks []events.VerificationCheck
	flow   *events.DataFlowCheck
	// inconclusive is set, with recovered false, when the layers passed and the
	// sinks got no rows while the source could not be shown to be idle.
	inconclusive bool
	// aborted is true when ctx ended before a verdict: nothing is announced and
	// the proposal stays unverified, so a replayed result can verify it.
	aborted bool
}

// verifyProposal runs the layer checks, polling until they settle (see
// settleLayers), and then the data-flow check. The data-flow check runs only
// after the layer checks pass.
func (r *runner) verifyProposal(ctx context.Context, rec proposalRecord) verifyOutcome {
	settle := r.settleLayers(ctx, verifyChecks(rec.steps))
	if settle.aborted {
		return verifyOutcome{aborted: true}
	}
	lines := append([]string(nil), settle.lines...)
	out := verifyOutcome{checks: settle.checks}
	if out.checks == nil {
		out.checks = []events.VerificationCheck{} // JSON [], never null
	}
	if !settle.ok {
		const why = "not checked because a layer check did not pass"
		lines = append(lines, "data flow: "+why)
		out.evidence = strings.Join(lines, "\n")
		out.flow = &events.DataFlowCheck{Checked: false, Detail: why}
		out.head = "NOT recovered: a layer check did not pass.\n"
		if settle.timedOut {
			out.head = fmt.Sprintf("NOT recovered: a layer check had not passed after %s of waiting for the layers to settle.\n", r.timing.verifySettleDeadline())
		}
		return out
	}

	if r.recoveredIdle && schemaConfigOnly(rec.steps) && r.restoresTrackedSubjects(rec.steps) {
		// The episode was already verified recovered, and this proposal only
		// restores subjects that a verified fix left at NONE (the restore after a
		// fix). The layer checks decide. Data flow is read once for the evidence
		// and is not waited for: a compatibility change adds no rows by itself.
		// From a broken state nothing has been verified yet, and a compatibility
		// change of any other subject (for example FULL to BACKWARD after a
		// re-break) is not that restore, so the data-flow rule applies.
		flow := r.verifyDataFlow(ctx, rec, false)
		if ctx.Err() != nil {
			return verifyOutcome{aborted: true}
		}
		lines = append(lines, "data flow (information only, not part of this verdict):")
		lines = append(lines, flow.lines...)
		out.evidence = strings.Join(lines, "\n")
		out.flow = flow.event()
		if out.flow.Checked {
			// Checked is shown to people as a reading; Detail says it is not part
			// of the verdict (clients show Detail only for an unchecked flow).
			out.flow.Detail = dataFlowInfoOnly
		}
		out.recovered = true
		out.head = recoveredConfigHead
		return out
	}

	flow := r.verifyDataFlow(ctx, rec, true)
	if ctx.Err() != nil {
		return verifyOutcome{aborted: true}
	}
	lines = append(lines, flow.lines...)
	out.evidence = strings.Join(lines, "\n")
	out.flow = flow.event()
	switch {
	case !flow.verified:
		out.head = "NOT recovered: data flow not verified.\n"
	case !flow.fresh && flow.source == sourceQuiet:
		detail := idleDetail
		if flow.idleDetail != "" {
			detail = flow.idleDetail
		}
		out.flow = &events.DataFlowCheck{Checked: false, Detail: detail}
		out.recovered = true
		out.head = recoveredIdleHead
	case !flow.fresh && flow.source == sourceUnproven:
		out.flow = &events.DataFlowCheck{Checked: false, Detail: inconclusiveDetail}
		out.inconclusive = true
		out.head = inconclusiveHead
	case !flow.fresh:
		out.head = "NOT recovered: data flow did not pass; no new rows since the fix.\n"
	default:
		out.recovered = true
		out.head = recoveredHead
	}
	return out
}

// verifyCheck is one read-tool check of a layer in the verification pass.
type verifyCheck struct {
	name  string
	input json.RawMessage
	// layer is the layer the check reads (one of events.Layers), for the
	// agent.verification event.
	layer string
	judge func(tools.Result) bool
	// state, when set, tells a result that is not a pass apart: still settling
	// (checkPending) or really failed (checkFail). Nil means judge decides and
	// everything that is not a pass has failed: the layer answers at once.
	state func(tools.Result) checkState
	// settle makes a passing check count only if it still passes after
	// settleWait, so a transient healthy state does not count as recovery.
	settle bool
	// failConfirm is how many rounds in a row a failure must be read before it
	// counts as final (0 and 1 mean once). The layer may still be showing the
	// state from before the fix while it restarts.
	failConfirm int
}

// verifyChecks maps the layers a proposal touched to read-tool checks, in step
// order and de-duplicated by layer. A layer is marked seen only once a check was
// built for it, so a later step on the same layer with usable parameters still
// gets its check. Layers without a known check contribute none. The warehouse
// layer is not here: its freshness is judged by the data-flow check.
//
// A schema_set_compatibility step is the exception: it is checked per subject,
// against the level the proposal leaves the subject at, because two such steps
// on different subjects are two different facts.
func verifyChecks(steps []events.ProposalStep) []verifyCheck {
	seen := map[string]bool{}
	seenSubject := map[string]bool{}
	var checks []verifyCheck
	for _, s := range steps {
		if s.Action == "schema_set_compatibility" {
			if c, subject, ok := compatCheck(s, steps); ok && !seenSubject[subject] {
				seenSubject[subject] = true
				checks = append(checks, c)
			}
			continue
		}
		if seen[s.Layer] {
			continue
		}
		c, ok := checkForStep(s)
		if !ok {
			continue
		}
		seen[s.Layer] = true
		checks = append(checks, c)
	}
	return checks
}

// compatCheck builds the check of one schema_set_compatibility step: the
// subject's compatibility, read with schema_registry_versions, must equal the
// level the proposal leaves the subject at. It returns the subject too. ok is
// false when the step carries no usable subject or level.
func compatCheck(s events.ProposalStep, steps []events.ProposalStep) (verifyCheck, string, bool) {
	subject, ok := stringParam(s.Params, "subject")
	if !ok {
		return verifyCheck{}, "", false
	}
	_, final := compatFinalLevels(steps)
	want := final[subject]
	if want == "" {
		return verifyCheck{}, "", false
	}
	layer := s.Layer
	if layer == "" {
		layer = "schema-registry"
	}
	return verifyCheck{name: "schema_registry_versions", input: jsonObject("subject", subject), layer: layer, judge: compatibilityIs(want)}, subject, true
}

// compatibilityIs judges a schema_registry_versions result: the subject's
// compatibility must be the wanted level. A payload that cannot be decoded, or
// that names no level (the tool reports UNKNOWN when it cannot read it), is not
// a pass; the summary text is not judged.
func compatibilityIs(want string) func(tools.Result) bool {
	return func(res tools.Result) bool {
		var d struct {
			Compatibility string `json:"compatibility"`
		}
		if err := decodeData(res.Data, &d); err != nil {
			return false
		}
		return d.Compatibility != "" && strings.EqualFold(d.Compatibility, want)
	}
}

// schemaConfigOnly reports whether a proposal is a pure tightening of schema
// configuration: every step is a schema_set_compatibility step with a check, and
// no subject ends at NONE. Such a proposal changes no data path, so once the
// episode is already recovered its layer checks decide and data flow is only
// information: a quiet pipeline gets no new rows from a compatibility change.
// Any other step, even a schema_register, keeps the data-flow rule, and so does
// a proposal that loosens a subject to NONE: that one is a fix attempt, and only
// data flow shows whether it worked.
func schemaConfigOnly(steps []events.ProposalStep) bool {
	if len(steps) == 0 {
		return false
	}
	for _, s := range steps {
		if s.Action != "schema_set_compatibility" {
			return false
		}
		if _, _, ok := compatCheck(s, steps); !ok {
			return false
		}
	}
	_, final := compatFinalLevels(steps)
	for _, level := range final {
		if level == "NONE" {
			return false
		}
	}
	return true
}

// trackNoneSubjects records the effect of a proposal whose verdict is
// "recovered": every subject it leaves at NONE becomes tracked, and every subject
// it moves to another level stops being tracked. The restore of a tracked
// subject is the one change that may be judged without data flow.
func (r *runner) trackNoneSubjects(steps []events.ProposalStep) {
	if r.noneSubjects == nil {
		r.noneSubjects = map[string]bool{}
	}
	order, final := compatFinalLevels(steps)
	for _, subject := range order {
		if final[subject] == "NONE" {
			r.noneSubjects[subject] = true
		} else {
			delete(r.noneSubjects, subject)
		}
	}
}

// restoresTrackedSubjects reports whether every subject the steps set is one a
// verified proposal left at NONE. Together with schemaConfigOnly (which makes the
// steps compatibility steps that end away from NONE) it means the proposal moves
// only tracked subjects away from NONE. A proposal that also touches an untracked
// subject is not the restore and keeps the data-flow rule.
func (r *runner) restoresTrackedSubjects(steps []events.ProposalStep) bool {
	order, _ := compatFinalLevels(steps)
	if len(order) == 0 {
		return false
	}
	for _, subject := range order {
		if !r.noneSubjects[subject] {
			return false
		}
	}
	return true
}

// checkForStep returns the recovery check for one proposal step's layer, or ok
// false when the layer has no check or the step carries no usable parameters.
func checkForStep(s events.ProposalStep) (verifyCheck, bool) {
	switch s.Layer {
	case "kafka-connect":
		conn, ok := stringParam(s.Params, "connector")
		if !ok {
			return verifyCheck{}, false
		}
		return verifyCheck{name: "connect_status", input: jsonObject("connector", conn), layer: s.Layer, judge: connectRecovered, state: connectState, settle: true, failConfirm: connectFailConfirm}, true
	case "postgres":
		return verifyCheck{name: "pg_replication_slots", input: json.RawMessage(`{}`), layer: s.Layer, judge: pgSlotsRecovered, state: pgSlotsState}, true
	default:
		return verifyCheck{}, false
	}
}

// flowResult is the outcome of the data-flow check.
type flowResult struct {
	// verified is true when every sink table of the pipeline could be checked:
	// the sink tables are known, within the cap, and each has a timestamp column.
	verified bool
	// fresh is true when every sink table received rows after the fix.
	fresh bool
	// lines is the evidence, one line per sink table or per reason it was not
	// verified.
	lines []string
	// sinks is the same evidence for the agent.verification event; empty when
	// the check could not run.
	sinks []events.SinkCheck
	// source is the idle-source judgement, read only when no sink received
	// rows: quiet makes the data-flow check inconclusive with the layers
	// deciding, unproven makes the whole verdict inconclusive, active keeps the
	// failure. sourceLine is its evidence.
	source     sourceState
	sourceLine string
	// idleDetail replaces idleDetail as the data-flow detail of a quiet
	// source when the proof came from somewhere else than the slots.
	idleDetail string
}

// sinkTable is one warehouse table the alert's pipeline loads, with the column
// that says how fresh its rows are.
type sinkTable struct {
	name     string
	tsColumn string
}

// pipelineSinks returns the sink tables the data-flow check can judge. When it
// cannot, the reason is returned instead, and the data flow is not verified.
func (r *runner) pipelineSinks(ctx context.Context, verb string) ([]sinkTable, string) {
	pipeline := r.alert.PipelineID
	if pipeline == "" {
		return nil, "the alert names no pipeline"
	}
	sinks, err := r.sinkTables(ctx, verb, pipeline)
	if err != nil {
		return nil, err.Error()
	}
	if len(sinks) == 0 {
		return nil, "pipeline " + pipeline + " has no sink tables"
	}
	if len(sinks) > maxSinkTables {
		return nil, fmt.Sprintf("too many sink tables (%d, the limit is %d)", len(sinks), maxSinkTables)
	}
	for _, s := range sinks {
		if s.tsColumn == "" {
			return nil, "no timestamp column known for sink table " + s.name
		}
	}
	return sinks, ""
}

// sinkState is one sink table's progress through the data-flow check.
type sinkState struct {
	table       sinkTable
	input       json.RawMessage
	proposalAt  time.Time // creation baseline; meaningful only when hasProposal
	hasProposal bool
	checkAt     time.Time // check baseline: the newest row when the check began
	newest      time.Time // the newest row of the latest read
	passed      bool
	passedBy    string // "rows since proposal" or "rows during check"
	lastSummary string
}

// readings renders both baselines for the evidence, so a verdict shows what the
// newest row was at proposal time and at check time.
func (st sinkState) readings() string {
	check := "check baseline " + formatBaseline(st.checkAt)
	if st.hasProposal {
		return "proposal baseline " + formatBaseline(st.proposalAt) + "; " + check
	}
	return check
}

// verifyDataFlow checks that every sink table of the alert's pipeline received
// rows after the fix. A sink passes when its newest row is later than the
// creation baseline (rows since proposal), or when a later read inside the check
// window shows a newest row later than the check baseline (rows during check).
// Without a creation baseline (the agent restarted) only the window applies.
//
// The check baselines are read for every sink first. Each window round then
// sleeps once and re-reads only the sinks still pending, so the whole check
// sleeps at most freshnessAttempts × freshnessWait. With wait false the window
// is skipped and only the creation baseline can pass a sink: the caller wants a
// reading for the evidence, not a wait.
func (r *runner) verifyDataFlow(ctx context.Context, rec proposalRecord, wait bool) flowResult {
	sinks, why := r.pipelineSinks(ctx, "verify")
	if why != "" {
		return flowResult{lines: []string{"data flow not verified: " + why}}
	}

	states := make([]sinkState, len(sinks))
	for i, s := range sinks {
		st := sinkState{table: s, input: jsonObject("table", s.name, "ts_column", s.tsColumn)}
		st.proposalAt, st.hasProposal = rec.baseline[s.name]
		newest, summary, err := r.readNewest(ctx, "verify", st.input)
		if err != nil {
			return flowResult{lines: []string{"data flow not verified: reading " + s.name + ": " + err.Error()}}
		}
		st.checkAt = newest
		st.newest = newest
		st.lastSummary = summary
		if st.hasProposal && newest.After(st.proposalAt) {
			st.passed = true
			st.passedBy = "rows since proposal"
		}
		states[i] = st
	}

	for attempt := 0; wait && attempt < freshnessAttempts && anyPending(states); attempt++ {
		if err := r.clk.Sleep(ctx, r.timing.freshnessWait); err != nil {
			return flowResult{lines: []string{"data flow not verified: waiting for new rows: " + err.Error()}}
		}
		for i := range states {
			st := &states[i]
			if st.passed {
				continue
			}
			newest, summary, err := r.readNewest(ctx, "verify", st.input)
			if err != nil {
				return flowResult{lines: []string{"data flow not verified: reading " + st.table.name + ": " + err.Error()}}
			}
			st.lastSummary = summary
			st.newest = newest
			if newest.After(st.checkAt) {
				st.passed = true
				st.passedBy = "rows during check"
			}
		}
	}

	res := flowResult{verified: true, fresh: true}
	if wait && anyPending(states) && !anyPassed(states) {
		// Only a failing check pays for the source reads (the idle-source rule
		// above sourceIdle).
		res.source, res.sourceLine, res.idleDetail = r.judgeSource(ctx, rec)
	}
	for _, st := range states {
		sc := events.SinkCheck{Table: st.table.name, Newest: st.newest, Passed: st.passed, PassedBy: st.passedBy}
		if st.hasProposal {
			sc.Baseline = st.proposalAt
		}
		res.sinks = append(res.sinks, sc)
		if st.passed {
			res.lines = append(res.lines, fmt.Sprintf("data flow %s: %s (%s)", st.table.name, st.passedBy, st.readings()))
			continue
		}
		res.fresh = false
		res.lines = append(res.lines, fmt.Sprintf("data flow %s: no new rows since the fix (%s); last read %s", st.table.name, st.readings(), clipLine(st.lastSummary)))
	}
	if res.sourceLine != "" {
		res.lines = append(res.lines, res.sourceLine)
	}
	return res
}

// event renders the data-flow result for agent.verification. A check that
// could not run says why in Detail, without the "data flow not verified: "
// lead of the evidence line.
func (f flowResult) event() *events.DataFlowCheck {
	if !f.verified {
		detail := ""
		if len(f.lines) > 0 {
			detail = clipLine(strings.TrimPrefix(f.lines[0], "data flow not verified: "))
		}
		return &events.DataFlowCheck{Checked: false, Detail: detail}
	}
	return &events.DataFlowCheck{Checked: true, Fresh: f.fresh, Sinks: f.sinks}
}

// anyPassed reports whether some sink has passed.
func anyPassed(states []sinkState) bool {
	for _, st := range states {
		if st.passed {
			return true
		}
	}
	return false
}

// sourceState is what the source side says about a check whose sinks got no
// new rows.
type sourceState int

const (
	// sourceActive: the source changed, or nothing is known about it. Stale
	// sinks are then a data-flow failure: NOT recovered.
	sourceActive sourceState = iota
	// sourceQuiet: proven on the source side that there was no change to
	// replicate. Data flow cannot be judged; the verdict rests on the layers.
	sourceQuiet
	// sourceUnproven: the data topics did not grow, but the source side could
	// not prove it was quiet. Inconclusive: neither recovered nor NOT recovered.
	sourceUnproven
)

// The idle-source rule (QA review 2, R15 and R20).
//
// Flat source data topics are not proof that the source was idle: a connector
// that reads RUNNING but captures nothing (it loops on a decoding error, or
// never streams) also writes nothing to them, while the source keeps taking
// writes. So idleness must be proven on the source side as well, and the proof
// must survive Debezium heartbeats, which move the slot without any data change
// (they write only the heartbeat topic, which sourceTopics leaves out).
//
// When pg_table_activity is offered, the pipeline's own tables decide first
// (judgeSource, verify_tables.go): unchanged counters are quiet, changed ones
// are active. Without it, or when its readings cannot be compared, the
// source-side signal is the replication slots (pg_replication_slots), read at
// proposal time when that reading exists, else at both ends of one
// freshnessWait:
//
//   - data topics grew: the source changed. Active.
//   - data topics flat, and for every slot confirmed_flush_lsn moved: the
//     connector consumed and acknowledged WAL and emitted no data record from
//     it, so the WAL held no change of the pipeline's tables (a heartbeat).
//     Quiet.
//   - data topics flat, and every slot stood still with retained WAL unchanged:
//     the server wrote no WAL at all. Quiet.
//   - data topics flat, a slot's confirmed_flush_lsn stood still and its
//     retained WAL grew: WAL was written that the connector did not consume.
//     That is the stuck connector. Active. (A healthy connector without
//     heartbeats on a server whose other tables write looks the same; NOT
//     recovered keeps the agent looking, which is the safe side.)
//   - data topics flat, slots unreadable or not comparable: unproven. The
//     verdict says "inconclusive", never "recovered", and never "NOT
//     recovered" for a pure idle doubt.
//
// When the topics cannot be read at all, the old slot-only fallback applies: an
// unchanged slot fingerprint over one freshnessWait is quiet, anything else
// (including no reading) is active.

// sourceIdle judges the source for a check whose sinks got no new rows, and
// returns an evidence line saying why.
func (r *runner) sourceIdle(ctx context.Context, rec proposalRecord) (sourceState, string) {
	topics, ok := r.sourceTopics(ctx, "verify")
	if ok {
		slotsBefore, slotsOK := rec.slots, len(rec.slots) > 0
		if !slotsOK {
			slotsBefore, slotsOK = r.readSlots(ctx, "verify")
		}
		now, ok := r.topicEnds(ctx, "verify", topics)
		if ok {
			before := rec.topicEnds
			waited := false
			if len(before) == 0 {
				before = now
				if err := r.clk.Sleep(ctx, r.timing.freshnessWait); err != nil {
					return sourceActive, ""
				}
				waited = true
				if now, ok = r.topicEnds(ctx, "verify", topics); !ok {
					return sourceActive, ""
				}
			}
			for _, t := range topics {
				if b, have := before[t]; !have || now[t] != b {
					return sourceActive, "source: the data topics grew, so the source changed"
				}
			}
			if !slotsOK {
				return sourceUnproven, "source: the data topics did not grow; the replication slots could not be read to prove the source was idle"
			}
			if len(rec.slots) == 0 && !waited {
				// The slot reading must span a window, like the topic reading.
				if err := r.clk.Sleep(ctx, r.timing.freshnessWait); err != nil {
					return sourceActive, ""
				}
			}
			slotsAfter, ok := r.readSlots(ctx, "verify")
			if !ok {
				return sourceUnproven, "source: the data topics did not grow; the replication slots could not be read again to prove the source was idle"
			}
			return judgeSlots(slotsBefore, slotsAfter)
		}
	}
	before, ok := r.readSlots(ctx, "verify")
	if !ok {
		return sourceActive, ""
	}
	if err := r.clk.Sleep(ctx, r.timing.freshnessWait); err != nil {
		return sourceActive, ""
	}
	after, ok := r.readSlots(ctx, "verify")
	if ok && slotFingerprint(after) == slotFingerprint(before) {
		return sourceQuiet, "source: the replication slots did not move and no WAL was written"
	}
	return sourceActive, ""
}

// judgeSlots applies the slot part of the idle-source rule to two readings
// taken while the data topics stayed flat.
func judgeSlots(before, after []slotPosition) (sourceState, string) {
	if len(before) != len(after) {
		return sourceUnproven, "source: the data topics did not grow; the replication slots changed between readings, so idleness is not proven"
	}
	prev := make(map[string]slotPosition, len(before))
	for _, s := range before {
		prev[s.name] = s
	}
	consumed := false
	for _, a := range after {
		b, ok := prev[a.name]
		if !ok {
			return sourceUnproven, "source: the data topics did not grow; the replication slots changed between readings, so idleness is not proven"
		}
		switch {
		case a.confirmedFlush != b.confirmedFlush:
			consumed = true
		case a.retainedWAL > b.retainedWAL:
			return sourceActive, fmt.Sprintf("source: slot %s did not move while its retained WAL grew from %d to %d bytes: the source wrote changes the connector did not capture", a.name, b.retainedWAL, a.retainedWAL)
		}
	}
	if consumed {
		return sourceQuiet, "source: the data topics did not grow while the connector consumed WAL (heartbeats), so the source had no change to replicate"
	}
	return sourceQuiet, "source: the data topics did not grow and the replication slots show no new WAL"
}

// sourceTopics returns the pipeline's source data topics from pipeline_map,
// without heartbeat topics. ok is false when none can be named.
func (r *runner) sourceTopics(ctx context.Context, verb string) ([]string, bool) {
	if r.alert.PipelineID == "" {
		return nil, false
	}
	res, err := r.runStep(ctx, verb, "pipeline_map", jsonObject("pipeline_id", r.alert.PipelineID))
	if err != nil {
		return nil, false
	}
	var g struct {
		Nodes []struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"nodes"`
	}
	if err := decodeData(res.Data, &g); err != nil {
		return nil, false
	}
	var out []string
	for _, n := range g.Nodes {
		if n.Kind == "topic" && n.Name != "" && !strings.Contains(strings.ToLower(n.Name), "heartbeat") {
			out = append(out, n.Name)
		}
	}
	return out, len(out) > 0
}

// topicEnds reads the summed end offset of each topic with kafka_topic_offsets.
func (r *runner) topicEnds(ctx context.Context, verb string, topics []string) (map[string]int64, bool) {
	out := make(map[string]int64, len(topics))
	for _, t := range topics {
		res, err := r.runStep(ctx, verb, "kafka_topic_offsets", jsonObject("topic", t))
		if err != nil {
			return nil, false
		}
		var d struct {
			Partitions []struct {
				End int64 `json:"end"`
			} `json:"partitions"`
		}
		if err := decodeData(res.Data, &d); err != nil || len(d.Partitions) == 0 {
			return nil, false
		}
		var sum int64
		for _, p := range d.Partitions {
			sum += p.End
		}
		out[t] = sum
	}
	return out, true
}

// slotPosition is one replication slot as pg_replication_slots reports it.
type slotPosition struct {
	name           string
	confirmedFlush string
	restart        string
	retainedWAL    int64
}

// readSlots reads the pipeline's replication slots with pg_replication_slots.
// Retained WAL grows with every source write while the slot stands still, and
// confirmed_flush_lsn moves when the connector acknowledges WAL. ok is false
// when the tool is missing, fails, or names no slot: then the source side is
// unknown.
func (r *runner) readSlots(ctx context.Context, verb string) ([]slotPosition, bool) {
	res, err := r.runStep(ctx, verb, "pg_replication_slots", json.RawMessage(`{}`))
	if err != nil {
		return nil, false
	}
	var slots []struct {
		Name              string `json:"name"`
		ConfirmedFlushLSN string `json:"confirmed_flush_lsn"`
		RestartLSN        string `json:"restart_lsn"`
		RetainedWALBytes  int64  `json:"retained_wal_bytes"`
	}
	if err := decodeData(res.Data, &slots); err != nil || len(slots) == 0 {
		return nil, false
	}
	out := make([]slotPosition, 0, len(slots))
	for _, s := range slots {
		if s.ConfirmedFlushLSN == "" && s.RestartLSN == "" {
			return nil, false
		}
		out = append(out, slotPosition{name: s.Name, confirmedFlush: s.ConfirmedFlushLSN, restart: s.RestartLSN, retainedWAL: s.RetainedWALBytes})
	}
	return out, true
}

// slotFingerprint renders slot readings as one comparable string.
func slotFingerprint(slots []slotPosition) string {
	var sb strings.Builder
	for _, s := range slots {
		fmt.Fprintf(&sb, "%s|%s|%s|%d;", s.name, s.confirmedFlush, s.restart, s.retainedWAL)
	}
	return sb.String()
}

// anyPending reports whether some sink has not passed yet.
func anyPending(states []sinkState) bool {
	for _, st := range states {
		if !st.passed {
			return true
		}
	}
	return false
}

// snapshotBaseline records each sink table's newest row as the proposal is
// made, so the later check can count rows that arrived since the proposal. It is
// best effort: when a sink cannot be read or judged, no baseline is kept and the
// check falls back to its window alone.
//
// The whole snapshot shares one baselineTimeout deadline, on top of the per-tool
// timeouts, so a slow warehouse cannot hold up the proposal. A failure anywhere
// stores nothing: a partial baseline would judge some sinks against a time the
// others never had.
func (r *runner) snapshotBaseline(ctx context.Context, proposalID string) {
	ctx, cancel := context.WithTimeout(ctx, r.timing.baselineTimeout)
	defer cancel()
	sinks, why := r.pipelineSinks(ctx, "baseline")
	if why != "" {
		r.log.Info("agent: no creation baseline for proposal", "proposal", proposalID, "reason", why)
		return
	}
	base := make(map[string]time.Time, len(sinks))
	for _, s := range sinks {
		newest, _, err := r.readNewest(ctx, "baseline", jsonObject("table", s.name, "ts_column", s.tsColumn))
		if err != nil {
			r.log.Info("agent: no creation baseline for proposal", "proposal", proposalID, "table", s.name, "err", err)
			return
		}
		base[s.name] = newest
	}
	r.proposals.setBaseline(proposalID, base)
	// Best effort: the source tables' change counters, so the idle rule can see
	// a write to the pipeline's tables anywhere since the proposal.
	if tables, ok := r.readTables(ctx, "baseline"); ok {
		r.proposals.setTables(proposalID, tables)
	}
	// Best effort: source topic offsets at proposal time, so the idle rule can
	// tell "no change since the fix" from "no change during the check".
	if topics, ok := r.sourceTopics(ctx, "baseline"); ok {
		if ends, ok := r.topicEnds(ctx, "baseline", topics); ok {
			r.proposals.setTopicEnds(proposalID, ends)
			// The slots at the same moment: the idle-source rule then sees a
			// source write anywhere since the proposal, not only inside the
			// check window.
			if slots, ok := r.readSlots(ctx, "baseline"); ok {
				r.proposals.setSlots(proposalID, slots)
			}
		}
	}
}

// formatBaseline renders a newest-row time, or "none (empty table)" for a table
// with no rows.
func formatBaseline(t time.Time) string {
	if t.IsZero() {
		return "none (empty table)"
	}
	return t.Format(time.RFC3339Nano)
}

// readNewest reads warehouse_freshness once and returns the newest row time,
// which is the zero time for an empty table, plus the tool summary.
func (r *runner) readNewest(ctx context.Context, verb string, input json.RawMessage) (time.Time, string, error) {
	res, err := r.runStep(ctx, verb, "warehouse_freshness", input)
	if err != nil {
		return time.Time{}, "", err
	}
	var d struct {
		Newest *string `json:"newest"`
	}
	if err := decodeData(res.Data, &d); err != nil {
		return time.Time{}, res.Summary, fmt.Errorf("warehouse_freshness gave no readable newest: %w", err)
	}
	if d.Newest == nil || *d.Newest == "" {
		return time.Time{}, res.Summary, nil
	}
	ts, err := time.Parse(time.RFC3339Nano, *d.Newest)
	if err != nil {
		return time.Time{}, res.Summary, fmt.Errorf("warehouse_freshness newest is not a timestamp: %w", err)
	}
	return ts, res.Summary, nil
}

// sinkTables reads the sink tables of one pipeline from pipeline_map, with the
// timestamp column each one carries in its facts. It is a read tool like any
// other, so the call is traced in the room.
func (r *runner) sinkTables(ctx context.Context, verb, pipelineID string) ([]sinkTable, error) {
	res, err := r.runStep(ctx, verb, "pipeline_map", jsonObject("pipeline_id", pipelineID))
	if err != nil {
		return nil, err
	}
	var g struct {
		Nodes []struct {
			Kind  string            `json:"kind"`
			Name  string            `json:"name"`
			Facts map[string]string `json:"facts"`
		} `json:"nodes"`
	}
	if err := decodeData(res.Data, &g); err != nil {
		return nil, fmt.Errorf("pipeline_map gave no readable topology: %w", err)
	}
	var out []sinkTable
	for _, n := range g.Nodes {
		if n.Kind == "sink_table" && n.Name != "" {
			out = append(out, sinkTable{name: n.Name, tsColumn: n.Facts["ts_column"]})
		}
	}
	return out, nil
}

// decodeData round-trips a tool's Data through JSON so judges can read it
// without depending on the concrete client DTO types.
func decodeData(data any, v any) error {
	if data == nil {
		return errors.New("agent: verify: no tool data")
	}
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// connectRecovered reports whether a connect_status result shows a connector
// that is RUNNING with at least one task, and every task RUNNING. A payload that
// cannot be decoded is never a recovery: the summary text is not judged.
func connectRecovered(res tools.Result) bool { return connectState(res) == checkPass }

// connectState reads a connect_status result. Kafka Connect restarts a
// connector or task asynchronously, so right after a fix the status is often a
// transition, not an outcome:
//
//   - checkPass: the connector and every task are RUNNING, and there is at
//     least one task;
//   - checkPending: still settling. A connector or task is UNASSIGNED or
//     RESTARTING, or the connector is RUNNING with no task yet. This wins over
//     a failure elsewhere in the same status: while something restarts, a FAILED
//     task next to it may be the state from before the fix;
//   - checkFail: anything else (FAILED, PAUSED, STOPPED, a state this code does
//     not know), and a payload that cannot be decoded. Summary text is never
//     judged.
func connectState(res tools.Result) checkState {
	var d struct {
		State string `json:"state"`
		Tasks []struct {
			State string `json:"state"`
		} `json:"tasks"`
	}
	if err := decodeData(res.Data, &d); err != nil {
		return checkFail
	}
	pending, failed := false, false
	classify := func(state string) {
		switch state {
		case "RUNNING":
		case "UNASSIGNED", "RESTARTING":
			pending = true
		default:
			failed = true
		}
	}
	classify(d.State)
	if len(d.Tasks) == 0 && !failed {
		pending = true // the tasks are created after the connector starts
	}
	for _, t := range d.Tasks {
		classify(t.State)
	}
	switch {
	case pending:
		return checkPending
	case failed:
		return checkFail
	}
	return checkPass
}

// pgSlotsRecovered reports whether pg_replication_slots shows at least one
// active logical replication slot. A payload that cannot be decoded is never a
// recovery: the summary text carries slot names from the database, so it can
// hold "active=true" without any slot being active, and it is not judged.
func pgSlotsRecovered(res tools.Result) bool { return pgSlotsState(res) == checkPass }

// pgSlotsState reads a pg_replication_slots result. A slot turns active when
// the connector reconnects, which follows the fix by a few seconds: no active
// slot yet is checkPending. A payload that cannot be decoded is checkFail.
func pgSlotsState(res tools.Result) checkState {
	var slots []struct {
		Active bool `json:"active"`
	}
	if err := decodeData(res.Data, &slots); err != nil {
		return checkFail
	}
	for _, s := range slots {
		if s.Active {
			return checkPass
		}
	}
	return checkPending
}

// errVerifyUnrunnable marks a verification read that cannot run at all (the
// tool is missing or is not a read tool, or the input is invalid). Waiting does
// not change that, unlike a tool that failed while its layer restarts.
var errVerifyUnrunnable = errors.New("agent: verify: check cannot run")

// runStep executes one read tool for verification, emitting the same
// step/tool events a normal read-tool call produces, and returns the raw
// result so the caller can judge it. The room step is titled verb + " " +
// name, so the room tells a creation baseline read ("baseline ...") from a
// check read ("verify ...").
func (r *runner) runStep(ctx context.Context, verb, name string, input json.RawMessage) (tools.Result, error) {
	return r.runStepEvents(ctx, verb, name, input, true)
}

// runStepEvents is runStep with the room events optional. The settle loop
// reads the same check every few seconds; only its first read goes to the room,
// so a layer that takes a minute to restart does not fill the log with a dozen
// identical steps. The result is scrubbed either way, since its summary reaches
// the model and the verdict.
func (r *runner) runStepEvents(ctx context.Context, verb, name string, input json.RawMessage, withEvents bool) (tools.Result, error) {
	entry, ok := r.toolList.byName[name]
	if !ok {
		return tools.Result{}, fmt.Errorf("%w: unknown tool %q", errVerifyUnrunnable, name)
	}
	if entry.isRoom {
		return tools.Result{}, fmt.Errorf("%w: %q is not a read tool", errVerifyUnrunnable, name)
	}
	if err := validateInput(entry.schema, input); err != nil {
		return tools.Result{}, fmt.Errorf("%w: invalid input for %s: %w", errVerifyUnrunnable, name, err)
	}

	stepID := newStepID()
	toolUseID := "tu_" + newULID()
	// The room log must get every tool_call its tool_result pairs with, even when
	// the deadline below has passed: the real stores refuse events on a done
	// context. So events are emitted without the deadline, and only the tool
	// call itself is bounded by it.
	emitCtx := context.WithoutCancel(ctx)
	if withEvents {
		r.emit(emitCtx, events.AgentStepStart, events.AgentStepStartPayload{StepID: stepID, Title: verb + " " + name})
		r.emit(emitCtx, events.AgentToolCall, events.AgentToolCallPayload{
			StepID:    stepID,
			ToolUseID: toolUseID,
			Tool:      name,
			Input:     input,
		})
	}

	tctx, cancel := context.WithTimeout(r.toolScope(ctx), r.timing.toolTimeout)
	defer cancel()
	start := time.Now()
	res, err := entry.tool.Run(tctx, input)
	took := time.Since(start).Milliseconds()

	res = r.scrubToolResult(name, res)
	if err != nil {
		// The verdict lines built from this error reach the model and the log.
		err = r.scrubToolError(name, err)
		if withEvents {
			summary, truncated := tools.Truncate("error: " + err.Error())
			r.emit(emitCtx, events.AgentToolResult, events.AgentToolResultPayload{
				StepID: stepID, ToolUseID: toolUseID, Tool: name,
				Summary: summary, IsError: true, Truncated: truncated, TookMS: took,
			})
		}
		return tools.Result{}, err
	}
	if withEvents {
		summary, reTruncated := tools.Truncate(res.Summary)
		r.emit(emitCtx, events.AgentToolResult, events.AgentToolResultPayload{
			StepID: stepID, ToolUseID: toolUseID, Tool: name,
			Summary: summary, Data: r.toolDataJSON(name, res.Data), IsError: false,
			Truncated: res.Truncated || reTruncated, TookMS: took,
		})
	}
	return res, nil
}

// stringParam reads one string field from a proposal step's action params.
func stringParam(params json.RawMessage, key string) (string, bool) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(params, &m); err != nil {
		return "", false
	}
	raw, ok := m[key]
	if !ok {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil || s == "" {
		return "", false
	}
	return s, true
}

// jsonObject builds a flat JSON object from alternating key/value strings.
func jsonObject(kv ...string) json.RawMessage {
	m := make(map[string]string, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	b, _ := json.Marshal(m)
	return b
}

// verifyBlocks renders the verification notes of one batch as text blocks, for
// the same user message as the inputs they judge.
func verifyBlocks(notes []string) []anthropic.BetaContentBlockParamUnion {
	blocks := make([]anthropic.BetaContentBlockParamUnion, 0, len(notes))
	for _, n := range notes {
		blocks = append(blocks, anthropic.NewBetaTextBlock(n))
	}
	return blocks
}

// clipLine caps a tool summary quoted in the verdict message at
// verifyEvidenceBytes on a rune boundary.
func clipLine(s string) string {
	if len(s) <= verifyEvidenceBytes {
		return s
	}
	cut := verifyEvidenceBytes
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut] + "…"
}
