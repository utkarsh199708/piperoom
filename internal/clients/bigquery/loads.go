package bigquery

import (
	"context"
	"fmt"
	"time"

	bq "cloud.google.com/go/bigquery"

	"piperoom/internal/clients"
)

const (
	// jobsRetention is how far back INFORMATION_SCHEMA.JOBS keeps jobs.
	jobsRetention    = 180 * 24 * time.Hour
	defaultLoadLimit = 20
	maxLoadLimit     = 100
)

// Load statuses, in the clients.LoadError vocabulary.
const (
	statusFailed  = "LOAD_FAILED"
	statusPartial = "PARTIALLY_LOADED"
)

// LoadErrors lists the jobs that wrote to table ("dataset.table") since a
// time and failed (error_result set: LOAD_FAILED) or finished with row errors
// (errors set but no error_result, e.g. a load under max_bad_records:
// PARTIALLY_LOADED), newest first. Both LOAD jobs and QUERY jobs (dbt's
// INSERT and MERGE into the sink) are listed. JOBS keeps 180 days, so an
// older since is raised to that. Rows is the DML inserted row count, 0 for a
// load job (JOBS does not report load output rows). FirstError is server
// text: redacted, one line, at most 300 runes. File is always empty: JOBS
// does not carry a load job's source URIs.
//
// The view is the project-wide `region-<location>`.INFORMATION_SCHEMA.JOBS,
// so the credential needs bigquery.jobs.listAll (roles/bigquery.resourceViewer);
// without it the call is ErrPermissionDenied.
func (d *DB) LoadErrors(ctx context.Context, table string, since time.Time, limit int) ([]clients.LoadError, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	if floor := time.Now().Add(-jobsRetention); since.Before(floor) {
		since = floor
	}
	q, err := loadErrorsQuery(d.cfg, table, since, limit)
	if err != nil {
		return nil, err
	}
	res, err := d.run(ctx, q)
	if err != nil {
		return nil, wrapQueryErr(ctx, "load history of", table, err)
	}
	out := make([]clients.LoadError, 0, len(res.Rows))
	for _, r := range res.Rows {
		le, err := loadErrorFromRow(table, r)
		if err != nil {
			return nil, err
		}
		out = append(out, le)
	}
	return out, nil
}

// Column order of loadErrorsQuery's SELECT, read by loadErrorFromRow.
const (
	colAt = iota
	colReason
	colMessage
	colErrorCount
	colInserted
	colFailed
	numLoadCols
)

func loadErrorsQuery(cfg Config, table string, since time.Time, limit int) (query, error) {
	t, err := parseTable(cfg.Project, table)
	if err != nil {
		return query{}, err
	}
	if limit <= 0 {
		limit = defaultLoadLimit
	}
	if limit > maxLoadLimit {
		limit = maxLoadLimit
	}
	// The location passed ParseDSN's pattern (letters, digits, hyphens).
	view := quote(cfg.Project) + "." + quote("region-"+cfg.Location) + ".INFORMATION_SCHEMA.JOBS"
	return query{
		SQL: `SELECT COALESCE(end_time, creation_time) AS at,
  COALESCE(error_result.reason, errors[SAFE_OFFSET(0)].reason) AS reason,
  COALESCE(error_result.message, errors[SAFE_OFFSET(0)].message) AS message,
  ARRAY_LENGTH(errors) AS error_count,
  dml_statistics.inserted_row_count AS inserted_rows,
  error_result IS NOT NULL AS failed
FROM ` + view + `
WHERE creation_time >= @since
  AND state = 'DONE'
  AND job_type IN ('LOAD', 'QUERY')
  AND destination_table.project_id = @project
  AND destination_table.dataset_id = @dataset
  AND destination_table.table_id = @table
  AND (error_result IS NOT NULL OR ARRAY_LENGTH(errors) > 0)
ORDER BY creation_time DESC
LIMIT @limit`,
		Params: []bq.QueryParameter{
			{Name: "since", Value: since.UTC()},
			{Name: "project", Value: t.project},
			{Name: "dataset", Value: t.dataset},
			{Name: "table", Value: t.table},
			{Name: "limit", Value: int64(limit)},
		},
		MaxRows: limit,
	}, nil
}

func loadErrorFromRow(table string, r []bq.Value) (clients.LoadError, error) {
	if len(r) != numLoadCols {
		return clients.LoadError{}, fmt.Errorf("bigquery: load history of %s: %d columns, want %d", table, len(r), numLoadCols)
	}
	le := clients.LoadError{
		Table:  table,
		Status: statusPartial,
		Rows:   asInt(r[colInserted]),
		Errors: asInt(r[colErrorCount]),
	}
	if failed, _ := r[colFailed].(bool); failed {
		le.Status = statusFailed
		if le.Errors == 0 {
			le.Errors = 1
		}
	}
	if at, ok := r[colAt].(time.Time); ok {
		le.At = at.UTC()
	}
	msg := asString(r[colMessage])
	if reason := asString(r[colReason]); reason != "" {
		if msg == "" {
			msg = reason
		} else {
			msg = reason + ": " + msg
		}
	}
	le.FirstError = cleanText(msg)
	return le, nil
}
