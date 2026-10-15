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
	// waitingDetail is the status detail of a recovered episode parked for input.
	waitingDetail = "pipeline recovered; waiting for input"
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
}

// proposalTracker remembers the proposals the agent created during this run. It
// is per-runner state, never global. It does not survive a restart: after a
// restart the agent has no record of earlier proposals, so their action results
// are not verified (the room waits for a steer after a restart), and their
// creation baselines are gone too.
type proposalTracker struct {
	mu   sync.Mutex
	byID map[string]proposalRecord
	// flows follows each proposal from approval to executed steps (approvals.go).
	flows map[string]*execFlow
}

func newProposalTracker() *proposalTracker {
	return &proposalTracker{byID: map[string]proposalRecord{}, flows: map[string]*execFlow{}}
}

func (t *proposalTracker) record(id string, steps []events.ProposalStep) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.byID[id] = proposalRecord{id: id, steps: steps}
	t.flows[id] = newExecFlow(steps)
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

// markVerified stores the verdict for a proposal's last step.
func (t *proposalTracker) markVerified(id string, recovered bool) {
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
		r.log.Warn("agent: action_result for a proposal the agent did not create", "proposal", id)
		return actionOutcome{}
	}
	if step != len(rec.steps)-1 {
		// Not the last step yet: no verification, the normal loop continues.
		return actionOutcome{}
	}
	if rec.verified {
		// A repeated last-step result never runs the checks again and never
		// emits done again; it reports the verdict already reached.
		repeat := "already verified: " + verdictWord(rec.recovered) + "; no checks were run again"
		r.emit(ctx, events.AgentVerification, events.AgentVerificationPayload{
			ProposalID: id,
			Checks:     []events.VerificationCheck{},
			Recovered:  rec.recovered,
			Verdict:    repeat,
			Repeat:     true,
		})
		return actionOutcome{
			recovered: rec.recovered,
			broken:    !rec.recovered,
			notes:     []string{verifyTag + repeat},
		}
	}

	vr := r.verifyProposal(ctx, rec)
	recovered, evidence, head := vr.recovered, vr.evidence, vr.head
	var followUp string
	if recovered {
		followUp = compatFollowUp(rec.steps)
	}
	// The room gets the typed verdict; the model note below keeps the text form.
	r.emit(ctx, events.AgentVerification, vr.payload(id))
	r.proposals.markVerified(id, recovered)
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
func verdictWord(recovered bool) string {
	if recovered {
		return "recovered"
	}
	return "NOT recovered"
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

// verifyResult is the outcome of verifyProposal: the verdict, the text form
// the model reads, and the structured form the room shows.
type verifyResult struct {
	recovered bool
	// evidence is one line per check, for the model note.
	evidence string
	// head is the verdict head that opens the model note, with its newline.
	head   string
	checks []events.VerificationCheck
	flow   *events.DataFlowCheck
}

// payload builds the agent.verification event of proposal id.
func (v verifyResult) payload(id string) events.AgentVerificationPayload {
	checks := v.checks
	if checks == nil {
		checks = []events.VerificationCheck{}
	}
	return events.AgentVerificationPayload{
		ProposalID: id,
		Checks:     checks,
		DataFlow:   v.flow,
		Recovered:  v.recovered,
		Verdict:    strings.TrimSuffix(v.head, "\n"),
	}
}

// dataFlowCheck converts a flowResult to its event form.
func (f flowResult) dataFlowCheck(detail string) *events.DataFlowCheck {
	d := &events.DataFlowCheck{Checked: f.verified, Fresh: f.fresh, Detail: detail}
	if !f.verified {
		d.Detail = f.detail
	}
	for _, st := range f.sinks {
		sc := events.SinkCheck{Table: st.table.name, Newest: st.newest, Passed: st.passed, PassedBy: st.passedBy}
		if st.hasProposal {
			sc.Baseline = st.proposalAt
		}
		d.Sinks = append(d.Sinks, sc)
	}
	return d
}

// verifyProposal runs the layer checks and then the data-flow check. It returns
// the verdict, one evidence line per check, the verdict head that opens the
// model note, and the structured checks. The data-flow check runs only after
// the layer checks pass.
func (r *runner) verifyProposal(ctx context.Context, rec proposalRecord) verifyResult {
	var lines []string
	var checks []events.VerificationCheck
	layersOK := true
	for _, c := range verifyChecks(rec.steps) {
		ok, line := r.runVerifyCheck(ctx, c)
		layersOK = layersOK && ok
		lines = append(lines, c.name+": "+line)
		checks = append(checks, events.VerificationCheck{Layer: c.layer, Name: c.name, OK: ok, Detail: clipLine(line)})
	}
	if !layersOK {
		const skipped = "not checked because a layer check did not pass"
		lines = append(lines, "data flow: "+skipped)
		return verifyResult{
			evidence: strings.Join(lines, "\n"),
			head:     "NOT recovered: a layer check did not pass.\n",
			checks:   checks,
			flow:     &events.DataFlowCheck{Detail: skipped},
		}
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
		lines = append(lines, "data flow (information only, not part of this verdict):")
		lines = append(lines, flow.lines...)
		return verifyResult{
			recovered: true,
			evidence:  strings.Join(lines, "\n"),
			head:      recoveredConfigHead,
			checks:    checks,
			flow:      flow.dataFlowCheck("information only, not part of this verdict"),
		}
	}

	flow := r.verifyDataFlow(ctx, rec, true)
	lines = append(lines, flow.lines...)
	res := verifyResult{evidence: strings.Join(lines, "\n"), checks: checks, flow: flow.dataFlowCheck("")}
	switch {
	case !flow.verified:
		res.head = "NOT recovered: data flow not verified.\n"
	case !flow.fresh:
		res.head = "NOT recovered: data flow did not pass; no new rows since the fix.\n"
	default:
		res.recovered = true
		res.head = recoveredHead
	}
	return res
}

// verifyCheck is one read-tool check of a layer in the verification pass.
type verifyCheck struct {
	// layer is the pipeline layer the check reads (one of events.Layers).
	layer string
	name  string
	input json.RawMessage
	judge func(tools.Result) bool
	// settle makes a passing check count only if it still passes after
	// settleWait, so a transient healthy state does not count as recovery.
	settle bool
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
	return verifyCheck{layer: "schema-registry", name: "schema_registry_versions", input: jsonObject("subject", subject), judge: compatibilityIs(want)}, subject, true
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
		return verifyCheck{layer: s.Layer, name: "connect_status", input: jsonObject("connector", conn), judge: connectRecovered, settle: true}, true
	case "postgres":
		return verifyCheck{layer: s.Layer, name: "pg_replication_slots", input: json.RawMessage(`{}`), judge: pgSlotsRecovered}, true
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
	// detail is the reason the flow was not verified, without the evidence prefix.
	detail string
	// sinks is the per-table state, for the verification event.
	sinks []sinkState
}

// flowNotVerified is the flowResult of a data-flow check that could not run.
func flowNotVerified(why string) flowResult {
	return flowResult{lines: []string{"data flow not verified: " + why}, detail: why}
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
	passed      bool
	passedBy    string    // "rows since proposal" or "rows during check"
	newest      time.Time // newest row of the latest read
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
		return flowNotVerified(why)
	}

	states := make([]sinkState, len(sinks))
	for i, s := range sinks {
		st := sinkState{table: s, input: jsonObject("table", s.name, "ts_column", s.tsColumn)}
		st.proposalAt, st.hasProposal = rec.baseline[s.name]
		newest, summary, err := r.readNewest(ctx, "verify", st.input)
		if err != nil {
			return flowNotVerified("reading " + s.name + ": " + err.Error())
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
			return flowNotVerified("waiting for new rows: " + err.Error())
		}
		for i := range states {
			st := &states[i]
			if st.passed {
				continue
			}
			newest, summary, err := r.readNewest(ctx, "verify", st.input)
			if err != nil {
				return flowNotVerified("reading " + st.table.name + ": " + err.Error())
			}
			st.lastSummary = summary
			st.newest = newest
			if newest.After(st.checkAt) {
				st.passed = true
				st.passedBy = "rows during check"
			}
		}
	}

	res := flowResult{verified: true, fresh: true, sinks: states}
	for _, st := range states {
		if st.passed {
			res.lines = append(res.lines, fmt.Sprintf("data flow %s: %s (%s)", st.table.name, st.passedBy, st.readings()))
			continue
		}
		res.fresh = false
		res.lines = append(res.lines, fmt.Sprintf("data flow %s: no new rows since the fix (%s); last read %s", st.table.name, st.readings(), clipLine(st.lastSummary)))
	}
	return res
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
func connectRecovered(res tools.Result) bool {
	var d struct {
		State string `json:"state"`
		Tasks []struct {
			State string `json:"state"`
		} `json:"tasks"`
	}
	if err := decodeData(res.Data, &d); err != nil {
		return false
	}
	if d.State != "RUNNING" || len(d.Tasks) == 0 {
		return false
	}
	for _, t := range d.Tasks {
		if t.State != "RUNNING" {
			return false
		}
	}
	return true
}

// pgSlotsRecovered reports whether pg_replication_slots shows at least one
// active logical replication slot. A payload that cannot be decoded is never a
// recovery: the summary text carries slot names from the database, so it can
// hold "active=true" without any slot being active, and it is not judged.
func pgSlotsRecovered(res tools.Result) bool {
	var slots []struct {
		Active bool `json:"active"`
	}
	if err := decodeData(res.Data, &slots); err != nil {
		return false
	}
	for _, s := range slots {
		if s.Active {
			return true
		}
	}
	return false
}

// runVerifyCheck runs one layer check. A settling check must also pass a second
// read after settleWait. It returns whether the check passed and an evidence
// line.
func (r *runner) runVerifyCheck(ctx context.Context, c verifyCheck) (bool, string) {
	ok, line := r.readCheck(ctx, c)
	if !ok || !c.settle {
		return ok, line
	}
	if err := r.clk.Sleep(ctx, r.timing.settleWait); err != nil {
		return false, "settling: " + err.Error()
	}
	ok, again := r.readCheck(ctx, c)
	return ok, line + "; read again after " + r.timing.settleWait.String() + ": " + again
}

// readCheck reads one layer check once and judges it. It returns whether the
// check passed and the evidence line (a tool summary, or an error message).
func (r *runner) readCheck(ctx context.Context, c verifyCheck) (bool, string) {
	res, err := r.runVerifyTool(ctx, c.name, c.input)
	if err != nil {
		return false, "error: " + err.Error()
	}
	return c.judge(res), clipLine(res.Summary)
}

// runVerifyTool executes one read tool for verification, emitting the same
// step/tool events a normal read-tool call produces, and returns the raw
// result so the caller can judge it.
func (r *runner) runVerifyTool(ctx context.Context, name string, input json.RawMessage) (tools.Result, error) {
	return r.runStep(ctx, "verify", name, input)
}

// runStep is runVerifyTool with the room step titled verb + " " + name, so the
// room tells a creation baseline read ("baseline ...") from a check read
// ("verify ...").
func (r *runner) runStep(ctx context.Context, verb, name string, input json.RawMessage) (tools.Result, error) {
	entry, ok := r.toolList.byName[name]
	if !ok {
		return tools.Result{}, fmt.Errorf("agent: verify: unknown tool %q", name)
	}
	if entry.isRoom {
		return tools.Result{}, fmt.Errorf("agent: verify: %q is not a read tool", name)
	}
	if err := validateInput(entry.schema, input); err != nil {
		return tools.Result{}, fmt.Errorf("agent: verify: invalid input for %s: %w", name, err)
	}

	stepID := newStepID()
	toolUseID := "tu_" + newULID()
	// The room log must get every tool_call its tool_result pairs with, even when
	// the deadline below has passed: the real stores refuse events on a done
	// context. So events are emitted without the deadline, and only the tool
	// call itself is bounded by it.
	emitCtx := context.WithoutCancel(ctx)
	r.emit(emitCtx, events.AgentStepStart, events.AgentStepStartPayload{StepID: stepID, Title: verb + " " + name})
	r.emit(emitCtx, events.AgentToolCall, events.AgentToolCallPayload{
		StepID:    stepID,
		ToolUseID: toolUseID,
		Tool:      name,
		Input:     input,
	})

	tctx, cancel := context.WithTimeout(r.toolScope(ctx), r.timing.toolTimeout)
	defer cancel()
	start := time.Now()
	res, err := entry.tool.Run(tctx, input)
	took := time.Since(start).Milliseconds()

	res, scrubErr := r.scrubToolResult(name, res)
	if err == nil {
		err = scrubErr
	}
	data, dataCut := r.toolDataJSON(name, res.Data)
	if err != nil {
		// The verdict lines built from this error reach the model and the log.
		err = r.scrubToolError(name, err)
		summary, truncated := tools.Truncate("error: " + err.Error())
		r.emit(emitCtx, events.AgentToolResult, events.AgentToolResultPayload{
			StepID: stepID, ToolUseID: toolUseID, Tool: name,
			Summary: summary, IsError: true, Truncated: truncated, TookMS: took,
		})
		return tools.Result{}, err
	}
	summary, reTruncated := tools.Truncate(res.Summary)
	r.emit(emitCtx, events.AgentToolResult, events.AgentToolResultPayload{
		StepID: stepID, ToolUseID: toolUseID, Tool: name,
		Summary: summary, Data: data, IsError: false,
		Truncated: res.Truncated || reTruncated || dataCut, TookMS: took,
	})
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
