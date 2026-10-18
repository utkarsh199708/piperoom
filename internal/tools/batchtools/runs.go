package batchtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/airflow"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// Limits of dag_runs.
const (
	defaultRuns = 10
	maxRuns     = 100
)

// RunRow is one run in dag_runs' Data.
type RunRow struct {
	clients.DAGRun
	// DurationSeconds is end - start; for a run still going, now - start; 0
	// before it started.
	DurationSeconds float64 `json:"duration_seconds"`
}

// RunsData is dag_runs' Data.
type RunsData struct {
	DAGID   string         `json:"dag_id"`
	Runs    []RunRow       `json:"runs"`
	ByState map[string]int `json:"by_state"`
}

type dagRuns struct{ d Deps }

func (t *dagRuns) Name() string { return NameDAGRuns }
func (t *dagRuns) Description() string {
	return "Lists the last runs of one Airflow DAG of the room's pipeline, newest first: run id, state (queued, running, success, failed), " +
		"run type (scheduled, manual, backfill), logical date, start, duration, and the run's note. " +
		"Use first in a batch incident to see whether the latest run failed, is stuck running, or never started; then task_instances on the failed run."
}
func (t *dagRuns) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"dag_id": map[string]any{"type": "string", "description": "Airflow dag_id; one of the room pipeline's dags."},
			"limit":  map[string]any{"type": "integer", "description": fmt.Sprintf("How many runs, 1 to %d (default %d).", maxRuns, defaultRuns)},
		},
		"required":             []string{"dag_id"},
		"additionalProperties": false,
	}
}
func (t *dagRuns) Kind() tools.Kind { return tools.KindRead }

func (t *dagRuns) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	var input struct {
		DAGID string `json:"dag_id"`
		Limit *int   `json:"limit"`
	}
	if err := decode(in, &input); err != nil {
		return tools.Result{}, err
	}
	if err := checkID("dag_id", input.DAGID); err != nil {
		return tools.Result{}, err
	}
	limit := defaultRuns
	if input.Limit != nil {
		if *input.Limit < 1 || *input.Limit > maxRuns {
			return tools.Result{}, fmt.Errorf("%w: limit %d: want 1 to %d", tools.ErrBadInput, *input.Limit, maxRuns)
		}
		limit = *input.Limit
	}
	if err := t.d.scopeDAG(ctx, input.DAGID); err != nil {
		return tools.Result{}, err
	}
	r, err := t.d.reader(NameDAGRuns)
	if err != nil {
		return tools.Result{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	runs, err := r.DAGRuns(ctx, input.DAGID, limit)
	if err != nil {
		return tools.Result{}, fmt.Errorf("batchtools: dag_runs %s: %w", input.DAGID, err)
	}
	if len(runs) > limit {
		runs = runs[:limit]
	}
	now := t.d.Now()
	data := RunsData{DAGID: input.DAGID, Runs: make([]RunRow, 0, len(runs)), ByState: map[string]int{}}
	var sb strings.Builder
	fmt.Fprintf(&sb, "DAG %s: %d run(s), newest first", input.DAGID, len(runs))
	for _, run := range runs {
		run.Note, _ = redact.Text(run.Note)
		row := RunRow{DAGRun: run, DurationSeconds: runDuration(run.StartedAt, run.EndedAt, now).Seconds()}
		data.Runs = append(data.Runs, row)
		data.ByState[run.State]++
	}
	if len(runs) > 0 {
		sb.WriteString(" (" + stateCounts(data.ByState) + ")")
	}
	sb.WriteString(":")
	if len(runs) == 0 {
		sb.WriteString(" none (the DAG has never run, or its runs were deleted)")
	}
	for _, row := range data.Runs {
		fmt.Fprintf(&sb, "\n- %s %s [%s] logical %s, started %s, %s",
			oneLine(row.ID, 120), row.State, row.RunType, stamp(row.LogicalDate), stamp(row.StartedAt), durationText(row.DAGRun, now))
		if row.Note != "" {
			// Run notes are written by people or automation: untrusted. The
			// quotes fence the text; clean folds it to one line.
			fmt.Fprintf(&sb, ", note (untrusted): %q", clean(row.Note, noteRunes))
		}
	}
	summary, cut := tools.Truncate(sb.String())
	return tools.Result{Summary: summary, Data: data, Truncated: cut}, nil
}

// runDuration is end - start; now - start for an unfinished run; 0 before it starts.
func runDuration(start, end, now time.Time) time.Duration {
	switch {
	case start.IsZero():
		return 0
	case end.IsZero():
		return max(now.Sub(start), 0)
	}
	return max(end.Sub(start), 0)
}

func durationText(r clients.DAGRun, now time.Time) string {
	switch {
	case r.StartedAt.IsZero():
		return "not started"
	case r.EndedAt.IsZero():
		return "running for " + human(runDuration(r.StartedAt, r.EndedAt, now))
	}
	return "took " + human(runDuration(r.StartedAt, r.EndedAt, now))
}

// stateCounts renders "2 failed, 1 success" in a stable order: most first,
// then by name.
func stateCounts(m map[string]int) string {
	type kv struct {
		k string
		n int
	}
	var list []kv
	for k, n := range m {
		if k == "" {
			k = "none"
		}
		list = append(list, kv{k, n})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].n != list[j].n {
			return list[i].n > list[j].n
		}
		return list[i].k < list[j].k
	})
	parts := make([]string, len(list))
	for i, e := range list {
		parts[i] = fmt.Sprintf("%d %s", e.n, e.k)
	}
	return strings.Join(parts, ", ")
}

// TaskData is task_instances' Data.
type TaskData struct {
	DAGID   string                 `json:"dag_id"`
	RunID   string                 `json:"run_id"`
	Tasks   []clients.TaskInstance `json:"tasks"`
	ByState map[string]int         `json:"by_state"`
	// Partial is true when the reader returned only part of the run's task
	// instances (a very wide mapped task).
	Partial bool `json:"partial,omitempty"`
}

type taskInstances struct{ d Deps }

func (t *taskInstances) Name() string { return NameTaskInstances }
func (t *taskInstances) Description() string {
	return "Lists the task instances of one DAG run: task id, state (success, failed, up_for_retry, upstream_failed, running, skipped), " +
		"try number, operator, start and duration; failed and retrying tasks first. " +
		"Use after dag_runs on a failed or stuck run to find the task that broke; then task_log_tail on that task and try."
}
func (t *taskInstances) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"dag_id": map[string]any{"type": "string", "description": "Airflow dag_id; one of the room pipeline's dags."},
			"run_id": map[string]any{"type": "string", "description": "The run id from dag_runs, e.g. scheduled__2026-10-17T00:00:00+00:00."},
		},
		"required":             []string{"dag_id", "run_id"},
		"additionalProperties": false,
	}
}
func (t *taskInstances) Kind() tools.Kind { return tools.KindRead }

func (t *taskInstances) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	var input struct {
		DAGID string `json:"dag_id"`
		RunID string `json:"run_id"`
	}
	if err := decode(in, &input); err != nil {
		return tools.Result{}, err
	}
	if err := checkID("dag_id", input.DAGID); err != nil {
		return tools.Result{}, err
	}
	if err := checkRunID(input.RunID); err != nil {
		return tools.Result{}, err
	}
	if err := t.d.scopeDAG(ctx, input.DAGID); err != nil {
		return tools.Result{}, err
	}
	r, err := t.d.reader(NameTaskInstances)
	if err != nil {
		return tools.Result{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	tis, err := r.TaskInstances(ctx, input.DAGID, input.RunID)
	partial := false
	if err != nil {
		// A reader may return part of a long list with an error saying so
		// (airflow.ErrTruncated); keep the part rather than lose it. Any
		// other error, or an error with no rows, fails the call.
		if len(tis) == 0 || !isPartial(err) {
			return tools.Result{}, fmt.Errorf("batchtools: task_instances %s/%s: %w", input.DAGID, input.RunID, err)
		}
		partial = true
	}
	now := t.d.Now()
	data := TaskData{DAGID: input.DAGID, RunID: input.RunID, Tasks: tis, ByState: map[string]int{}, Partial: partial}
	for _, ti := range tis {
		data.ByState[ti.State]++
	}
	shown := append([]clients.TaskInstance(nil), tis...)
	sort.SliceStable(shown, func(i, j int) bool { return stateRank(shown[i].State) < stateRank(shown[j].State) })

	var sb strings.Builder
	fmt.Fprintf(&sb, "DAG %s run %s: %d task instance(s)", input.DAGID, oneLine(input.RunID, 120), len(tis))
	if partial {
		sb.WriteString(" (partial: the run has more; the first ones are shown)")
	}
	if len(tis) > 0 {
		sb.WriteString(" (" + stateCounts(data.ByState) + ")")
	}
	sb.WriteString(":")
	for i, ti := range shown {
		if i == maxListLines {
			fmt.Fprintf(&sb, "\n… %d more in data", len(shown)-i)
			break
		}
		name := ti.TaskID
		if ti.MapIndex >= 0 {
			name = fmt.Sprintf("%s[%d]", ti.TaskID, ti.MapIndex)
		}
		state := ti.State
		if state == "" {
			state = "none"
		}
		fmt.Fprintf(&sb, "\n- %s %s try %d (%s), started %s", name, state, ti.Try, ti.Operator, stamp(ti.StartedAt))
		if !ti.StartedAt.IsZero() {
			fmt.Fprintf(&sb, ", %s", human(runDuration(ti.StartedAt, ti.EndedAt, now)))
		}
	}
	summary, cut := tools.Truncate(sb.String())
	return tools.Result{Summary: summary, Data: data, Truncated: cut || partial}, nil
}

// isPartial reports whether err says the list was cut (airflow.ErrTruncated,
// returned with the first airflow.MaxTaskInstances rows).
func isPartial(err error) bool { return errors.Is(err, airflow.ErrTruncated) }

// stateRank orders task states so the ones that explain an incident come first.
func stateRank(s string) int {
	switch s {
	case "failed":
		return 0
	case "up_for_retry", "up_for_reschedule":
		return 1
	case "upstream_failed":
		return 2
	case "running", "queued", "scheduled", "deferred", "restarting":
		return 3
	case "success", "skipped", "removed":
		return 5
	}
	return 4
}
