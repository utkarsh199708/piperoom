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

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// verify-after-fix: once the last step of a proposal has run, the agent
// re-checks the layers the proposal touched with read tools before declaring
// recovery. The room delivers each executed step as a
// HumanInput{Kind:"action_result"} whose Text is "proposal <id> step <n>:
// <output>" (see internal/room on the A5 branch). Because the agent created the
// proposal itself, it can match that text back to the proposal it proposed and
// know whether n is the last step.

const (
	// maxVerifyChecks bounds the verification pass so one fix never triggers an
	// unbounded run of read tools.
	maxVerifyChecks = 3
	// freshnessRetries is the total number of warehouse_freshness attempts
	// (including the first) before giving up on newer rows arriving.
	freshnessRetries = 3
	// verifyEvidenceBytes caps one tool summary quoted in the verdict message.
	verifyEvidenceBytes = 500
)

var (
	// freshnessWait is the pause between warehouse_freshness retries. A var so
	// tests can shrink it.
	freshnessWait = 2 * time.Second
	// warehouseFreshLimit is the maximum age a warehouse row may have for the
	// warehouse check to count as recovered. A var so tests can widen it.
	warehouseFreshLimit = 15 * time.Minute
)

// followupMarker marks a proposal step as a follow-up that must run only after
// the pipeline is confirmed recovered (for example restoring BACKWARD
// compatibility). The model writes it into the step description.
const followupMarker = "run only after check"

// proposalRecord is what the agent remembers about a proposal it created, so a
// later action_result can be matched to its steps and layers.
type proposalRecord struct {
	id    string
	steps []events.ProposalStep
}

// proposalTracker remembers the proposals the agent created this run. It is
// per-runner state, never global.
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

// handleActionResults runs verify-after-fix for every action_result in inputs
// that marks the last step of a proposal the agent created. It returns true
// when the pipeline is confirmed recovered and Run should end (the done status
// has already been emitted).
func (r *runner) handleActionResults(ctx context.Context, inputs []HumanInput) bool {
	for _, in := range inputs {
		if in.Kind != "action_result" {
			continue
		}
		if r.verifyActionResult(ctx, in) {
			return true
		}
	}
	return false
}

// verifyActionResult handles one action_result input. It returns true only when
// the input was the last step of a tracked proposal and the pipeline was
// confirmed recovered, in which case the done status has already been emitted.
func (r *runner) verifyActionResult(ctx context.Context, in HumanInput) bool {
	id, step, ok := parseActionResult(in.Text)
	if !ok {
		return false
	}
	rec, ok := r.proposals.get(id)
	if !ok {
		r.log.Warn("agent: action_result for a proposal the agent did not create", "proposal", id)
		return false
	}
	if step != len(rec.steps)-1 {
		// Not the last step yet: no verification, the normal loop continues.
		return false
	}
	recovered, evidence := r.verifyProposal(ctx, rec)

	var b strings.Builder
	if recovered {
		b.WriteString("recovered: ")
	} else {
		b.WriteString("NOT recovered: ")
	}
	b.WriteString(evidence)
	for _, s := range rec.steps {
		if isFollowupStep(s) {
			b.WriteString("\n")
			b.WriteString(followupLine(s, recovered))
		}
	}
	r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: b.String()})

	if !recovered {
		return false
	}
	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "done", Detail: "pipeline recovered"})
	return true
}

// verifyProposal runs the layer checks for a proposal and returns whether every
// check passed, plus one evidence line per check joined by newlines.
func (r *runner) verifyProposal(ctx context.Context, rec proposalRecord) (bool, string) {
	checks := verifyChecks(rec.steps)
	if len(checks) == 0 {
		return false, "no verification checks available for the touched layers"
	}
	lines := make([]string, 0, len(checks))
	all := true
	for _, c := range checks {
		ok, line := r.runVerifyCheck(ctx, c)
		if !ok {
			all = false
		}
		lines = append(lines, c.name+": "+line)
	}
	return all, strings.Join(lines, "\n")
}

// verifyCheck is one read-tool check in the verification pass.
type verifyCheck struct {
	name  string
	input json.RawMessage
	judge func(tools.Result) bool
	retry bool
}

// verifyChecks maps the layers a proposal touched to read-tool checks, in step
// order and de-duplicated by layer, capped at maxVerifyChecks. Layers without a
// known recovery check contribute none.
func verifyChecks(steps []events.ProposalStep) []verifyCheck {
	seen := map[string]bool{}
	var checks []verifyCheck
	for _, s := range steps {
		if seen[s.Layer] {
			continue
		}
		seen[s.Layer] = true
		c, ok := checkForStep(s)
		if !ok {
			continue
		}
		checks = append(checks, c)
		if len(checks) >= maxVerifyChecks {
			break
		}
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
		return verifyCheck{name: "connect_status", input: jsonObject("connector", conn), judge: connectRecovered}, true
	case "postgres":
		return verifyCheck{name: "pg_replication_slots", input: json.RawMessage(`{}`), judge: pgSlotsRecovered}, true
	case "warehouse":
		table, ok1 := stringParam(s.Params, "table")
		ts, ok2 := stringParam(s.Params, "ts_column")
		if !ok1 || !ok2 {
			return verifyCheck{}, false
		}
		return verifyCheck{name: "warehouse_freshness", input: jsonObject("table", table, "ts_column", ts), judge: warehouseRecovered, retry: true}, true
	default:
		return verifyCheck{}, false
	}
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

// connectRecovered reports whether a connect_status result shows a running
// connector with no failed tasks.
func connectRecovered(res tools.Result) bool {
	var d struct {
		State string `json:"state"`
		Tasks []struct {
			State string `json:"state"`
		} `json:"tasks"`
	}
	if err := decodeData(res.Data, &d); err == nil && d.State != "" {
		if d.State != "RUNNING" {
			return false
		}
		for _, t := range d.Tasks {
			if t.State == "FAILED" {
				return false
			}
		}
		return true
	}
	return strings.Contains(res.Summary, "RUNNING") && !strings.Contains(res.Summary, "FAILED")
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

// runVerifyCheck runs one check, retrying warehouse freshness a bounded number
// of times. It returns whether the check passed and the evidence line (a tool
// summary, or an error message).
func (r *runner) runVerifyCheck(ctx context.Context, c verifyCheck) (bool, string) {
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

// isFollowupStep reports whether a proposal step is marked to run only after
// the pipeline is confirmed recovered.
func isFollowupStep(s events.ProposalStep) bool {
	return strings.Contains(strings.ToLower(s.Description), followupMarker)
}

// followupLine states whether a follow-up step is now safe.
func followupLine(s events.ProposalStep, safe bool) string {
	if safe {
		return fmt.Sprintf("follow-up %q: now safe", s.Description)
	}
	return fmt.Sprintf("follow-up %q: not safe", s.Description)
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
