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
// and the data must flow end to end: every sink table of the alert's pipeline
// must be fresh. A pipeline with no known sink table, or a sink table whose
// timestamp column is not known, is "data flow not verified" and never counts
// as recovered.

const (
	// freshnessRetries is the total number of warehouse_freshness attempts
	// (including the first) before giving up on newer rows arriving.
	freshnessRetries = 3
	// verifyEvidenceBytes caps one tool summary quoted in the verdict message.
	verifyEvidenceBytes = 500
	// verifyTag opens every verification line given to the model, so the model
	// can tell a verdict from ordinary input.
	verifyTag = "[verification] "
	// recoveredHead opens the verdict of a proposal whose checks all passed.
	recoveredHead = "recovered: every layer check passed and data flow is verified.\n"
)

var (
	// freshnessWait is the pause between warehouse_freshness retries. A var so
	// tests can shrink it.
	freshnessWait = 2 * time.Second
	// warehouseFreshLimit is the maximum age a warehouse row may have for the
	// warehouse check to count as recovered. A var so tests can widen it.
	warehouseFreshLimit = 15 * time.Minute
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
	// needsModel is true when the model must read this batch: a person spoke,
	// or a step's outcome is failed or unknown.
	needsModel bool
	// notes are the [verification] lines for the model, to be sent in the same
	// user message as the inputs they judge.
	notes []string
}

// holds reports whether the run should wait for human input instead of calling
// the model: the pipeline is recovered and nothing in the batch needs a turn.
func (o actionOutcome) holds() bool {
	return o.recovered && !o.needsModel
}

// handleActionResults runs verify-after-fix for every input in the batch. The
// done status is emitted by verifyActionResult when a recovery is first
// confirmed. It never ends the run: callers use holds() to decide whether to
// wait for human input.
func (r *runner) handleActionResults(ctx context.Context, inputs []HumanInput) actionOutcome {
	var out actionOutcome
	out.needsModel = hasHumanInput(inputs)
	for _, in := range inputs {
		if in.Kind != "action_result" {
			continue
		}
		one := r.verifyActionResult(ctx, in)
		out.recovered = out.recovered || one.recovered
		out.needsModel = out.needsModel || one.needsModel
		out.notes = append(out.notes, one.notes...)
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
		return actionOutcome{needsModel: true, notes: []string{verifyTag + unknownOutcomeText(id, step, parsed)}}
	case !*in.OK:
		r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: failedStepText(id, step, parsed)})
		return actionOutcome{needsModel: true, notes: []string{verifyTag + failedStepText(id, step, parsed)}}
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
			notes:     []string{verifyTag + "already verified: " + verdictWord(rec.recovered) + "; no checks were run again"},
		}
	}

	recovered, evidence, head := r.verifyProposal(ctx, rec)
	r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: head + evidence})
	r.proposals.markVerified(id, recovered)
	out := actionOutcome{
		recovered: recovered,
		notes:     []string{verifyTag + head + wrapToolOutput("verify", "", evidence)},
	}
	if recovered {
		r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "done", Detail: "pipeline recovered"})
	}
	return out
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

	flow := r.verifyDataFlow(ctx, rec)
	lines = append(lines, flow.lines...)
	evidence := strings.Join(lines, "\n")
	switch {
	case !flow.verified:
		return false, evidence, "NOT recovered: data flow not verified.\n"
	case !flow.fresh:
		return false, evidence, "NOT recovered: data flow did not pass; a sink table is stale.\n"
	}
	return true, evidence, recoveredHead
}

// verifyCheck is one read-tool check in the verification pass.
type verifyCheck struct {
	name  string
	input json.RawMessage
	judge func(tools.Result) bool
	// retry repeats the read a bounded number of times before failing.
	retry bool
	// settle makes a passing check count only if it still passes after
	// settleWait, so a transient healthy state does not count as recovery.
	settle bool
}

// verifyChecks maps the layers a proposal touched to read-tool checks, in step
// order and de-duplicated by layer. A layer is marked seen only once a check was
// built for it, so a later step on the same layer with usable parameters still
// gets its check. Layers without a known check contribute none.
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
// The warehouse layer is not here: its freshness is part of the data-flow check.
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
	// the pipeline's sink tables are known and each has a timestamp column.
	verified bool
	// fresh is true when every sink table passed its freshness check.
	fresh bool
	// lines is the evidence, one line per sink table or per reason it was not
	// verified.
	lines []string
}

// verifyDataFlow checks that every sink table of the alert's pipeline is fresh,
// with the bounded freshness retry. It needs the sink tables from pipeline_map
// and each table's timestamp column from the proposal's warehouse step for that
// table. Without either it reports the data flow as not verified.
func (r *runner) verifyDataFlow(ctx context.Context, rec proposalRecord) flowResult {
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

	res := flowResult{verified: true, fresh: true}
	for _, table := range sinks {
		ts, ok := sinkTSColumn(rec.steps, table)
		if !ok {
			res.verified = false
			res.fresh = false
			res.lines = append(res.lines, "data flow not verified: no timestamp column known for sink table "+table)
			return res
		}
		c := verifyCheck{
			name:  "warehouse_freshness",
			input: jsonObject("table", table, "ts_column", ts),
			judge: warehouseRecovered,
			retry: true,
		}
		ok, line := r.runVerifyCheck(ctx, c)
		res.fresh = res.fresh && ok
		res.lines = append(res.lines, "data flow "+table+": "+line)
	}
	return res
}

// sinkTables reads the sink tables of one pipeline from pipeline_map. It is a
// read tool like any other, so the call is traced in the room.
func (r *runner) sinkTables(ctx context.Context, pipelineID string) ([]string, error) {
	res, err := r.runVerifyTool(ctx, "pipeline_map", jsonObject("pipeline_id", pipelineID))
	if err != nil {
		return nil, err
	}
	var g struct {
		Nodes []struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"nodes"`
	}
	if err := decodeData(res.Data, &g); err != nil {
		return nil, fmt.Errorf("pipeline_map gave no readable topology: %w", err)
	}
	var out []string
	for _, n := range g.Nodes {
		if n.Kind == "sink_table" && n.Name != "" {
			out = append(out, n.Name)
		}
	}
	return out, nil
}

// sinkTSColumn returns the timestamp column the proposal names for one warehouse
// table, if any step names that table.
func sinkTSColumn(steps []events.ProposalStep, table string) (string, bool) {
	for _, s := range steps {
		if s.Layer != "warehouse" {
			continue
		}
		t, ok := stringParam(s.Params, "table")
		if !ok || t != table {
			continue
		}
		if ts, ok := stringParam(s.Params, "ts_column"); ok {
			return ts, true
		}
	}
	return "", false
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

// warehouseRecovered reports whether warehouse_freshness shows rows no older
// than warehouseFreshLimit.
func warehouseRecovered(res tools.Result) bool {
	var d struct {
		Newest string `json:"newest"`
	}
	if err := decodeData(res.Data, &d); err != nil || d.Newest == "" {
		return false
	}
	ts, err := time.Parse(time.RFC3339Nano, d.Newest)
	if err != nil {
		return false
	}
	return time.Since(ts) <= warehouseFreshLimit
}

// runVerifyCheck runs one check. A settling check must also pass a second read
// after settleWait. It returns whether the check passed and an evidence line.
func (r *runner) runVerifyCheck(ctx context.Context, c verifyCheck) (bool, string) {
	ok, line := r.attemptCheck(ctx, c)
	if !ok || !c.settle {
		return ok, line
	}
	if err := r.clk.Sleep(ctx, settleWait); err != nil {
		return false, "settling: " + err.Error()
	}
	ok, again := r.attemptCheck(ctx, c)
	return ok, line + "; read again after " + settleWait.String() + ": " + again
}

// attemptCheck reads one check, retrying a bounded number of times when the
// check asks for it. It returns whether the check passed and the evidence line
// (a tool summary, or an error message).
func (r *runner) attemptCheck(ctx context.Context, c verifyCheck) (bool, string) {
	attempts := 1
	if c.retry {
		attempts = freshnessRetries
	}
	var last tools.Result
	for i := 0; i < attempts; i++ {
		res, err := r.runVerifyTool(ctx, c.name, c.input)
		if err != nil {
			return false, "error: " + err.Error()
		}
		last = res
		if c.judge(res) {
			return true, clipLine(res.Summary)
		}
		if i < attempts-1 {
			if err := r.clk.Sleep(ctx, freshnessWait); err != nil {
				return false, "waiting for freshness: " + err.Error()
			}
		}
	}
	return false, clipLine(last.Summary)
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
