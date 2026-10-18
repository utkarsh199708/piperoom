// Package airflow is a read-only client for the Apache Airflow 2.x stable
// REST API (v1, under /api/v1; Airflow 2.1 or later, which added order_by).
// It reads what the batch tools need (P4c): a DAG's schedule, its last runs,
// the task instances of one run, and the end of one task try's log. It sends
// GET requests only, so a Viewer-role Airflow user is enough.
//
// Paths used (each segment url.PathEscape'd and checked by netsec.CheckPath):
//
//	GET /api/v1/dags/{dag_id}
//	GET /api/v1/dags/{dag_id}/dagRuns?limit=N&order_by=-execution_date
//	GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances?limit=100&offset=N
//	GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}[?map_index=N]
//
// The log is requested with "Accept: text/plain", for which Airflow 2.x
// streams the whole log of that try, so the documented full_content
// parameter is not sent (to be confirmed against the P4g-1 dev profile). The
// client keeps only its tail while reading. Log text is untrusted: the
// client returns it as read (only the tail cut applies) and the tool wraps it.
//
// Airflow 3 moved the public API to /api/v2 with token login; supporting it
// is a follow-up.
package airflow
