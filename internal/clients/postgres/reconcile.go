package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// The source side of the drift checks (X1): counts, keys and timestamps of
// the pipeline's source_tables, on the read-only pool. pgconv.Reconcile holds
// the SQL, shared with the warehouse client.

var _ clients.Reconciler = (*DB)(nil)

func (d *DB) reconcile() pgconv.Reconcile {
	return pgconv.Reconcile{Name: "postgres", Query: d.queryReceipt}
}

// CountBetween implements clients.Reconciler on a source table.
func (d *DB) CountBetween(ctx context.Context, table, tsColumn string, from, to time.Time) (int64, error) {
	return d.reconcile().CountBetween(ctx, table, tsColumn, from, to)
}

// SampleKeys implements clients.Reconciler on a source table.
func (d *DB) SampleKeys(ctx context.Context, table, pkColumn, tsColumn string, since time.Time, n int) ([]string, error) {
	return d.reconcile().SampleKeys(ctx, table, pkColumn, tsColumn, since, n)
}

// RowTimes implements clients.Reconciler on a source table.
func (d *DB) RowTimes(ctx context.Context, table, pkColumn string, keys []string, tsColumns []string) (map[string]map[string]time.Time, error) {
	return d.reconcile().RowTimes(ctx, table, pkColumn, keys, tsColumns)
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
