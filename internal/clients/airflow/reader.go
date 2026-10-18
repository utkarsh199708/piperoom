package airflow

import (
	"context"
	"time"
)

// Reader mirrors clients.OrchestratorReader (P0 item 8) until the P0
// contract PR merges. Then this becomes
// `type Reader = clients.OrchestratorReader` and the two DTOs below become
// aliases of clients.DAGRun and clients.TaskInstance, so callers keep
// compiling unchanged.
type Reader interface {
	DAGRuns(ctx context.Context, dagID string, limit int) ([]DAGRun, error)
	TaskInstances(ctx context.Context, dagID, runID string) ([]TaskInstance, error)
	TaskLogTail(ctx context.Context, dagID, runID, taskID string, try int, maxBytes int) (string, error)
}

// DAGRun mirrors clients.DAGRun (P0 item 8). A time is zero while unset: a
// queued run has no start, a running one no end.
type DAGRun struct {
	ID        string    `json:"id"`    // Airflow dag_run_id
	State     string    `json:"state"` // queued | running | success | failed
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Note      string    `json:"note,omitempty"` // set by a person in the UI (Airflow 2.5+); untrusted text
	// LogicalDate is the run's logical date (execution_date before Airflow
	// 2.2): the start of the data interval it processes, not when it ran.
	LogicalDate time.Time `json:"logical_date"`
	RunType     string    `json:"run_type"` // scheduled | manual | backfill | dataset_triggered
}

// TaskInstance mirrors clients.TaskInstance (P0 item 8). A mapped task has
// one entry per map index, all with the same TaskID.
type TaskInstance struct {
	TaskID    string    `json:"task_id"`
	Try       int       `json:"try"`   // Airflow try_number as the API reports it
	State     string    `json:"state"` // success | failed | upstream_failed | up_for_retry | running | ...; "" = none yet
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Operator  string    `json:"operator"`
	MapIndex  int       `json:"map_index"` // -1 = not mapped (also when Airflow < 2.3 omits it)
}

// ScheduleReader mirrors clients.DAGScheduleReader (the approved P0 item 8
// change request) until the P0 contract PR merges. Then this becomes
// `type ScheduleReader = clients.DAGScheduleReader` and DAG an alias of
// clients.DAG. It is optional: a nil one makes the tool answer
// clients.ErrNotSupported.
type ScheduleReader interface {
	DAG(ctx context.Context, dagID string) (DAG, error)
}

// DAG mirrors clients.DAG: the schedule state of one DAG.
type DAG struct {
	ID string `json:"id"`
	// Schedule is the cron expression or interval as text, with Airflow's
	// description in parentheses when it adds one, e.g.
	// "0 0 * * * (At 00:00)"; "" = no schedule. DAG-author text: untrusted.
	Schedule string `json:"schedule"`
	Paused   bool   `json:"paused"`
	// NextRunAt is when the scheduler creates the next run
	// (next_dagrun_create_after, else next_dagrun); zero = none planned.
	NextRunAt time.Time `json:"next_run_at"`
}
