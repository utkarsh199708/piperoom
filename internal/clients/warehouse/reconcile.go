package warehouse

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// The sink side of the drift checks (X1): counts, keys and timestamps of the
// pipeline's sink_tables, on the read-only pool. pgconv.Reconcile holds the
// SQL, shared with the source client. The other warehouses (Snowflake,
// ClickHouse, BigQuery, Databricks) do not implement clients.Reconciler yet,
// so a caller's type assertion fails and the check is off for them.

var _ clients.Reconciler = (*DB)(nil)

func (d *DB) reconcile() pgconv.Reconcile {
	return pgconv.Reconcile{Name: "warehouse", Sink: true, Query: d.queryReceipt}
}

// CountBetween implements clients.Reconciler on a sink table.
func (d *DB) CountBetween(ctx context.Context, table, tsColumn string, from, to time.Time) (int64, error) {
	return d.reconcile().CountBetween(ctx, table, tsColumn, from, to)
}

// SampleKeys implements clients.Reconciler on a sink table.
func (d *DB) SampleKeys(ctx context.Context, table, pkColumn, tsColumn string, since time.Time, n int) ([]string, error) {
	return d.reconcile().SampleKeys(ctx, table, pkColumn, tsColumn, since, n)
}

// RowTimes implements clients.Reconciler on a sink table.
func (d *DB) RowTimes(ctx context.Context, table, pkColumn string, keys []string, tsColumns []string) (map[string]map[string]time.Time, error) {
	return d.reconcile().RowTimes(ctx, table, pkColumn, keys, tsColumns)
}

// RangeColumn is pgconv.RangeColumn: a column as the re-snapshot condition
// of pg_resnapshot_rows (X2a) needs it.
type RangeColumn = pgconv.RangeColumn

// RangeColumn reads one column of a sink table (sink_tables only): its catalog
// type, its kind and whether it alone is the primary key (X2a).
func (d *DB) RangeColumn(ctx context.Context, table, column string) (RangeColumn, error) {
	return d.reconcile().RangeColumn(ctx, table, column)
}

// CountRange counts the rows of a sink table (sink_tables only) whose column is
// in [from, to] (to "" = no upper bound): pg_resnapshot_rows' preview and
// verification count (X2a). A count only; the receipt never shows a bound.
func (d *DB) CountRange(ctx context.Context, table, column, from, to string) (int64, error) {
	return d.reconcile().CountRange(ctx, table, column, from, to)
}

// queryReceipt is query with the receipt's parameters given apart, so a
// read by key values records how many keys it read, never which.
func (d *DB) queryReceipt(ctx context.Context, receipt []any, sql string, args ...any) (pgx.Rows, error) {
	if receipt == nil {
		receipt = args
	}
	start := time.Now()
	rows, err := d.q.Query(ctx, sql, args...)
	d.record(ctx, start, sql, receipt)
	return rows, err
}
