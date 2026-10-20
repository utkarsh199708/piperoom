package airflow

import "piperoom/internal/clients"

// The client's types are the P0 contract types (internal/clients/clients.go,
// P0 item 8), named here so callers of this package can keep the short names.
type (
	// Reader is the read interface the batch tools take.
	Reader = clients.OrchestratorReader
	// ScheduleReader is optional next to Reader: a nil one makes the
	// dag_schedule_gap tool answer clients.ErrNotSupported.
	ScheduleReader = clients.DAGScheduleReader
	// DAGRun is one run of a DAG. The client fills LogicalDate from
	// logical_date, else execution_date (Airflow before 2.2).
	DAGRun = clients.DAGRun
	// TaskInstance is one task instance of a run. The client passes Airflow's
	// try_number through, so a task that never ran has Try 0, and MapIndex is
	// -1 also when Airflow before 2.3 omits map_index.
	TaskInstance = clients.TaskInstance
	// DAG is one DAG's schedule state. The client renders Schedule as the
	// cron expression or interval with Airflow's timetable description in
	// parentheses when it adds one, e.g. "0 0 * * * (At 00:00)"; DAG-author
	// text, untrusted. NextRunAt is next_dagrun_create_after, else
	// next_dagrun.
	DAG = clients.DAG
)
