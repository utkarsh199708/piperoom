package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"piperoom/internal/events"
)

// Verification of a batch fix (P4b; tasks/P4-pipeline-kinds-batch.md). A
// batch pipeline is healthy when its DAG's last run succeeded and its sink
// tables are fresh, so a fix is recovered only when both read so after it:
//
//  1. the last run of every DAG of the pipeline (pipeline_map's dag nodes)
//     reads success in dag_runs; a queued or running run is waited for, up to
//     freshnessAttempts × freshnessWait, and never counts as success;
//  2. then every sink table gets new rows after the fix: verifyDataFlow, the
//     same warehouse_freshness check as a CDC fix.
//
// Every read is a registered read tool run through runStepID, which refuses
// room tools and anything not in the room's read-only registry, so the check
// cannot write. No batch write action exists in this wave (a person runs the
// fix), so this runs only once an action declares batch (KindedAction).

const (
	// maxDAGs bounds the DAG checks, as maxSinkTables bounds the sink reads.
	maxDAGs = 5
	// batchRecoveredHead opens the verdict of a batch fix whose checks passed.
	// Like recoveredHead it claims new rows, not a caught-up sink.
	batchRecoveredHead = "recovered: the last DAG run succeeded and every sink table has new rows since the fix. This does not show that every missed interval is loaded.\n"
)

// verifyBatch runs the batch checks: the last DAG runs, then the data flow.
func (r *runner) verifyBatch(ctx context.Context, rec proposalRecord) verifyResult {
	const skipped = "not checked because the last DAG run did not pass"
	dags, why := r.pipelineDAGs(ctx, "verify")
	if why != "" {
		return verifyResult{
			evidence: "last DAG run: not verified: " + why + "\ndata flow: " + skipped,
			head:     "NOT recovered: the last DAG run could not be checked.\n",
			checks:   []events.VerificationCheck{{Layer: "orchestrator", Name: "dag_runs", Detail: clipLine(why)}},
			flow:     &events.DataFlowCheck{Detail: skipped},
		}
	}
	var lines []string
	var checks []events.VerificationCheck
	runsOK := true
	for _, dag := range dags {
		ok, line, ids := r.lastRunCheck(ctx, dag)
		runsOK = runsOK && ok
		label := "dag_runs " + dag
		if len(ids) > 0 {
			label += " (step " + strings.Join(ids, ", step ") + ")"
		}
		lines = append(lines, label+": "+line)
		checks = append(checks, events.VerificationCheck{Layer: "orchestrator", Name: "dag_runs", OK: ok, Detail: clipLine(dag + ": " + line)})
	}
	if !runsOK {
		lines = append(lines, "data flow: "+skipped)
		return verifyResult{
			evidence: strings.Join(lines, "\n"),
			head:     "NOT recovered: the last DAG run did not succeed.\n",
			checks:   checks,
			flow:     &events.DataFlowCheck{Detail: skipped},
		}
	}
	flow := r.verifyDataFlow(ctx, rec, true)
	lines = append(lines, flow.lines...)
	res := verifyResult{evidence: strings.Join(lines, "\n"), checks: checks, flow: flow.dataFlowCheck("")}
	switch {
	case !flow.verified:
		res.head = "NOT recovered: data flow not verified.\n"
	case !flow.fresh:
		res.head = "NOT recovered: the sink tables are not fresh; no new rows since the fix.\n"
	default:
		res.recovered = true
		res.head = batchRecoveredHead
	}
	return res
}

// pipelineDAGs returns the DAG ids of the alert's pipeline from pipeline_map,
// or the reason the DAG check cannot run.
func (r *runner) pipelineDAGs(ctx context.Context, verb string) ([]string, string) {
	pipeline := r.alert.PipelineID
	if pipeline == "" {
		return nil, "the alert names no pipeline"
	}
	nodes, err := r.pipelineNodes(ctx, verb, pipeline)
	if err != nil {
		return nil, err.Error()
	}
	var dags []string
	for _, n := range nodes {
		if n.Kind == "dag" && n.Name != "" {
			dags = append(dags, n.Name)
		}
	}
	switch {
	case len(dags) == 0:
		return nil, "pipeline " + pipeline + " has no DAGs"
	case len(dags) > maxDAGs:
		return nil, fmt.Sprintf("too many DAGs (%d, the limit is %d)", len(dags), maxDAGs)
	}
	return dags, ""
}

// lastRunCheck reads the last run of one DAG and passes when its state is
// success. A queued or running run is read again after freshnessWait, up to
// freshnessAttempts more times; it still fails if the run has not finished.
// It returns whether the check passed, the evidence line and the step ids.
func (r *runner) lastRunCheck(ctx context.Context, dag string) (bool, string, []string) {
	input := lastRunInput(dag)
	var ids []string
	for attempt := 0; ; attempt++ {
		state, line, id, err := r.readLastRun(ctx, input)
		ids = appendID(ids, id)
		if err != nil {
			return false, "error: " + err.Error(), ids
		}
		if state == "success" {
			return true, line, ids
		}
		if !runPending(state) || attempt >= freshnessAttempts {
			return false, line, ids
		}
		if err := r.clk.Sleep(ctx, r.timing.freshnessWait); err != nil {
			return false, "waiting for the run: " + err.Error(), ids
		}
	}
}

// lastRunInput is dag_runs' input for the newest run of dag.
func lastRunInput(dag string) json.RawMessage {
	b, _ := json.Marshal(struct {
		DAGID string `json:"dag_id"`
		Limit int    `json:"limit"`
	}{dag, 1})
	return b
}

// readLastRun reads dag_runs once and returns the newest run's state
// (lower-cased; "" when the DAG has no run), the evidence line and the step
// id. The state is judged from Data only: the summary carries run notes,
// which are untrusted text and could read "success" for a failed run.
func (r *runner) readLastRun(ctx context.Context, input json.RawMessage) (string, string, string, error) {
	res, id, err := r.runStepID(ctx, "verify", "dag_runs", input)
	if err != nil {
		return "", "", id, err
	}
	var d struct {
		Runs []struct {
			State string `json:"state"`
		} `json:"runs"`
	}
	if err := decodeData(res.Data, &d); err != nil {
		return "", "", id, fmt.Errorf("dag_runs gave no readable runs: %w", err)
	}
	if len(d.Runs) == 0 {
		return "", "no run (the DAG has not run since its runs were kept)", id, nil
	}
	state := strings.ToLower(d.Runs[0].State)
	return state, "last run " + oneWord(state), id, nil
}

// runPending reports the run states that may still end in success.
func runPending(state string) bool {
	return state == "queued" || state == "running" || state == "scheduled"
}

// oneWord renders a run state for the evidence line: a plain lower-case token
// as is, anything else quoted, so a state from the orchestrator cannot add a
// line or a verification tag to the verdict.
func oneWord(s string) string {
	for _, c := range s {
		if (c < 'a' || c > 'z') && c != '_' {
			return fmt.Sprintf("%q", clipLine(escapeToolText(s)))
		}
	}
	if s == "" {
		return "unknown"
	}
	return s
}
