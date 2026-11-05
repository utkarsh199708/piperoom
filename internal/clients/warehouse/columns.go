package warehouse

import (
	"context"

	"piperoom/internal/clients"
)

// The column catalog of the pipeline's sink_tables (X7: column lineage and
// PII tags), on the read-only pool. pgconv.Reconcile.Columns holds the SQL,
// shared with the source client. The other warehouses do not implement
// clients.ColumnReader yet, so the map has no sink columns for them.

var _ clients.ColumnReader = (*DB)(nil)

// Columns implements clients.ColumnReader on a sink table.
func (d *DB) Columns(ctx context.Context, table string) ([]clients.ColumnInfo, error) {
	return d.reconcile().Columns(ctx, table)
}
