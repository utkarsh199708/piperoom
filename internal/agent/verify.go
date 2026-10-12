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
// executed step as a HumanInput{Kind:"action_result"} whose Text is "proposal
// <id> step <n>: <output>" and whose OK says whether the step succeeded (see
// internal/room). Because the agent created the proposal itself, it can match
// that text back to the proposal and know whether n is the last step.
//
// Recovery needs two things. The layer checks of the touched layers must pass,
// and the data must flow: every sink table of the alert's pipeline must receive
// rows after the fix. A sink table with no known timestamp column, or a pipeline
// with no or too many sink tables, is "data flow not verified" and never counts
// as recovered.

const (
	// freshnessAttempts is how many warehouse_freshness reads follow the baseline
	// read. Rows must show up in one of them for the data flow to pass.
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
	recoveredHead = "recovered: every layer check passed and data flow is verified.\n"
)

var (
	// freshnessWait is the pause between warehouse_freshness reads. A var so
	// tests can shrink it.
	freshnessWait = 10 * time.Second
	// settleWait is the pause between the two connector reads that a recovery
	// needs, so a connector that only just came back is not counted as healthy.
	// A var so tests can shrink it.
	settleWait = 5 * time.Second
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
}

// proposalTracker remembers the proposals the agent created during this run. It
// is per-runner state, never global. It does not survive a restart: after a
// restart the agent has no record of earlier proposals, so their action results
// are not verified (the room waits for a steer after a restart).
type proposalTracker struct {
	mu   sync.Mutex
	byID map[string]proposalRecord
}

func newProposalTracker() *proposalTracker {
	return &proposalTracker{byID: map[string]proposalRecord{}}
}

func (t *proposalTracker) record(id string, steps []events.ProposalStep) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.byID[id] = proposalRecord{id: id, steps: steps}
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

// parseActionResult splits the room's action_result text back into its parts.
func parseActionResult(text string) (id string, step int, ok bool) {
	rest, found := strings.CutPrefix(text, "proposal ")
	if !found {
		return "", 0, false
	}
	id, rest, found = strings.Cut(rest, " step ")
	if !found {
		return "", 0, false
	}
	stepStr, _, found := strings.Cut(rest, ": ")
	if !found {
		return "", 0, false
	}
	n, err := strconv.Atoi(stepStr)
	if err != nil || id == "" {
		return "", 0, false
	}
	return id, n, true
}

// actionOutcome is what a batch of inputs means for the loop.
type actionOutcome struct {
	// recovered is true when a last step is verified as recovered in this batch,
	// or was verified as recovered before (a repeat).
	recovered bool
	// broken is true when the batch says the pipeline is not known to be
	// recovered: a step failed or is unknown, or a last step was verified as not
	// recovered.
	broken bool
	// needsModel is true when the model must read this batch: a person spoke,
	// or a step's outcome is failed or unknown.
	needsModel bool
	// notes are the [verification] lines for the model, to be sent in the same
	// user message as the inputs they judge.
	notes []string
}

// handleActionResults runs verify-after-fix for every input in the batch and
// updates recoveredIdle: set by a recovery, cleared by anything that breaks it.
// It never ends the run. The done status is emitted by verifyActionResult when
// a recovery is first confirmed.
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
		out.notes = append(out.notes, one.notes...)
	}
	switch {
	case out.broken:
		r.recoveredIdle = false
	case out.recovered:
		r.recoveredIdle = true
	}
	return out
}

// verifyActionResult handles one action_result input. Recovery is verified only
// from OK == true on the last step of a tracked proposal. A failed step is
// reported and the model runs; an unknown outcome is never turned into a
// recovery claim.
func (r *runner) verifyActionResult(ctx context.Context, in HumanInput) actionOutcome {
	id, step, parsed := parseActionResult(in.Text)
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
		return actionOutcome{
			recovered: rec.recovered,
			broken:    !rec.recovered,
			notes:     []string{verifyTag + "already verified: " + verdictWord(rec.recovered) + "; no checks were run again"},
		}
	}

	recovered, evidence, head := r.verifyProposal(ctx, rec)
	r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: head + evidence})
	r.proposals.markVerified(id, recovered)
	out := actionOutcome{
		recovered: recovered,
		broken:    !recovered,
		notes:     []string{verifyTag + head + wrapToolOutput("verify", "", evidence)},
	}
	if recovered {
		r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "done", Detail: "pipeline recovered"})
	}
	return out
}

// parked reports whether the run waits for human input instead of calling the
// model: the pipeline is recovered and nothing in the batch needs a turn.
func (r *runner) parked(o actionOutcome) bool {
	return r.recoveredIdle && !o.needsModel
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

// verifyProposal runs the layer checks and then the data-flow check. It returns
// whether recovery is confirmed, one evidence line per check, and the verdict
// head that opens the room message and the model note. The data-flow check runs
// only after the layer checks pass.
func (r *runner) verifyProposal(ctx context.Context, rec proposalRecord) (bool, string, string) {
	var lines []string
	layersOK := true
	for _, c := range verifyChecks(rec.steps) {
		ok, line := r.runVerifyCheck(ctx, c)
		layersOK = layersOK && ok
		lines = append(lines, c.name+": "+line)
	}
	if !layersOK {
		lines = append(lines, "data flow: not checked because a layer check did not pass")
		return false, strings.Join(lines, "\n"), "NOT recovered: a layer check did not pass.\n"
	}

	flow := r.verifyDataFlow(ctx)
	lines = append(lines, flow.lines...)
	evidence := strings.Join(lines, "\n")
	switch {
	case !flow.verified:
		return false, evidence, "NOT recovered: data flow not verified.\n"
	case !flow.fresh:
		return false, evidence, "NOT recovered: data flow did not pass; no new rows since the fix.\n"
	}
	return true, evidence, recoveredHead
}

// verifyCheck is one read-tool check of a layer in the verification pass.
type verifyCheck struct {
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
func verifyChecks(steps []events.ProposalStep) []verifyCheck {
	seen := map[string]bool{}
	var checks []verifyCheck
	for _, s := range steps {
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

// checkForStep returns the recovery check for one proposal step's layer, or ok
// false when the layer has no check or the step carries no usable parameters.
func checkForStep(s events.ProposalStep) (verifyCheck, bool) {
	switch s.Layer {
	case "kafka-connect":
		conn, ok := stringParam(s.Params, "connector")
		if !ok {
			return verifyCheck{}, false
		}
		return verifyCheck{name: "connect_status", input: jsonObject("connector", conn), judge: connectRecovered, settle: true}, true
	case "postgres":
		return verifyCheck{name: "pg_replication_slots", input: json.RawMessage(`{}`), judge: pgSlotsRecovered}, true
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
}

// sinkTable is one warehouse table the alert's pipeline loads, with the column
// that says how fresh its rows are.
type sinkTable struct {
	name     string
	tsColumn string
}

// verifyDataFlow checks that every sink table of the alert's pipeline received
// rows after the fix. The sink tables and their timestamp columns come from
// pipeline_map. Without them, or with more than maxSinkTables, the data flow is
// not verified.
func (r *runner) verifyDataFlow(ctx context.Context) flowResult {
	pipeline := r.alert.PipelineID
	if pipeline == "" {
		return flowResult{lines: []string{"data flow not verified: the alert names no pipeline"}}
	}
	sinks, err := r.sinkTables(ctx, pipeline)
	if err != nil {
		return flowResult{lines: []string{"data flow not verified: " + err.Error()}}
	}
	if len(sinks) == 0 {
		return flowResult{lines: []string{"data flow not verified: pipeline " + pipeline + " has no sink tables"}}
	}
	if len(sinks) > maxSinkTables {
		return flowResult{lines: []string{fmt.Sprintf("data flow not verified: too many sink tables (%d, the limit is %d)", len(sinks), maxSinkTables)}}
	}
	for _, s := range sinks {
		if s.tsColumn == "" {
			return flowResult{lines: []string{"data flow not verified: no timestamp column known for sink table " + s.name}}
		}
	}

	res := flowResult{verified: true, fresh: true}
	for _, s := range sinks {
		ok, line := r.checkNewRows(ctx, s)
		res.fresh = res.fresh && ok
		res.lines = append(res.lines, "data flow "+s.name+": "+line)
	}
	return res
}

// checkNewRows judges one sink table against the fix. The first read is the
// baseline. The check passes only when a later read shows a newest row strictly
// later than the baseline: rows that arrived after the fix. It makes at most
// freshnessAttempts later reads, freshnessWait apart.
func (r *runner) checkNewRows(ctx context.Context, s sinkTable) (bool, string) {
	input := jsonObject("table", s.name, "ts_column", s.tsColumn)
	base, baseSummary, err := r.readNewest(ctx, input)
	if err != nil {
		return false, "error: " + err.Error()
	}
	last := baseSummary
	for i := 0; i < freshnessAttempts; i++ {
		if err := r.clk.Sleep(ctx, freshnessWait); err != nil {
			return false, "waiting for new rows: " + err.Error()
		}
		newest, summary, err := r.readNewest(ctx, input)
		if err != nil {
			return false, "error: " + err.Error()
		}
		last = summary
		if newest.After(base) {
			return true, fmt.Sprintf("newest %s is later than the baseline %s", newest.Format(time.RFC3339Nano), formatBaseline(base))
		}
	}
	return false, "no new rows since the fix: " + clipLine(last)
}

// formatBaseline renders the baseline newest row, or "none" for an empty table.
func formatBaseline(t time.Time) string {
	if t.IsZero() {
		return "none (empty table)"
	}
	return t.Format(time.RFC3339Nano)
}

// readNewest reads warehouse_freshness once and returns the newest row time,
// which is the zero time for an empty table, plus the tool summary.
func (r *runner) readNewest(ctx context.Context, input json.RawMessage) (time.Time, string, error) {
	res, err := r.runVerifyTool(ctx, "warehouse_freshness", input)
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
func (r *runner) sinkTables(ctx context.Context, pipelineID string) ([]sinkTable, error) {
	res, err := r.runVerifyTool(ctx, "pipeline_map", jsonObject("pipeline_id", pipelineID))
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
// active logical replication slot.
func pgSlotsRecovered(res tools.Result) bool {
	var slots []struct {
		Active bool `json:"active"`
	}
	if err := decodeData(res.Data, &slots); err == nil {
		for _, s := range slots {
			if s.Active {
				return true
			}
		}
		return false
	}
	return strings.Contains(res.Summary, "active=true")
}

// runVerifyCheck runs one layer check. A settling check must also pass a second
// read after settleWait. It returns whether the check passed and an evidence
// line.
func (r *runner) runVerifyCheck(ctx context.Context, c verifyCheck) (bool, string) {
	ok, line := r.readCheck(ctx, c)
	if !ok || !c.settle {
		return ok, line
	}
	if err := r.clk.Sleep(ctx, settleWait); err != nil {
		return false, "settling: " + err.Error()
	}
	ok, again := r.readCheck(ctx, c)
	return ok, line + "; read again after " + settleWait.String() + ": " + again
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
	r.emit(ctx, events.AgentStepStart, events.AgentStepStartPayload{StepID: stepID, Title: "verify " + name})
	r.emit(ctx, events.AgentToolCall, events.AgentToolCallPayload{
		StepID:    stepID,
		ToolUseID: toolUseID,
		Tool:      name,
		Input:     input,
	})

	tctx, cancel := context.WithTimeout(ctx, toolTimeout)
	defer cancel()
	start := time.Now()
	res, err := entry.tool.Run(tctx, input)
	took := time.Since(start).Milliseconds()

	var data json.RawMessage
	if res.Data != nil {
		if b, mErr := json.Marshal(res.Data); mErr == nil {
			data = b
		} else {
			r.log.Warn("agent: verify tool data marshal failed", "tool", name, "err", mErr)
		}
	}
	if err != nil {
		summary, truncated := tools.Truncate("error: " + err.Error())
		r.emit(ctx, events.AgentToolResult, events.AgentToolResultPayload{
			StepID: stepID, ToolUseID: toolUseID, Tool: name,
			Summary: summary, IsError: true, Truncated: truncated, TookMS: took,
		})
		return tools.Result{}, err
	}
	summary, reTruncated := tools.Truncate(res.Summary)
	r.emit(ctx, events.AgentToolResult, events.AgentToolResultPayload{
		StepID: stepID, ToolUseID: toolUseID, Tool: name,
		Summary: summary, Data: data, IsError: false,
		Truncated: res.Truncated || reTruncated, TookMS: took,
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
