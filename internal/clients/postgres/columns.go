package postgres

import (
	"context"

	"piperoom/internal/clients"
)

// The column catalog of the pipeline's source_tables (X7: column lineage and
// PII tags), on the read-only pool. pgconv.Reconcile.Columns holds the SQL,
// shared with the warehouse client.

var _ clients.ColumnReader = (*DB)(nil)

// Columns implements clients.ColumnReader on a source table.
func (d *DB) Columns(ctx context.Context, table string) ([]clients.ColumnInfo, error) {
	return d.reconcile().Columns(ctx, table)
}
