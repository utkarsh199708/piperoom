package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// estimatedRowsSQL reads the planner's row estimate of one ordinary or
// partitioned table. Catalog only: no row of the table is read. reltuples is
// -1 on PostgreSQL 14+ for a table never vacuumed or analysed.
const estimatedRowsSQL = `
		SELECT c.reltuples::bigint
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1::name AND c.relname = $2::name AND c.relkind IN ('r', 'p')`

// EstimatedRows returns the planner's row estimate of table ("schema.table"),
// pg_class.reltuples: -1 when the table was never analysed, an estimate
// otherwise (it lags inserts until the next ANALYZE). A missing table is
// clients.ErrNotFound. The identifier is checked before any I/O and travels
// as parameters. Not on clients.SourceDB: debezium_snapshot_progress (X6b) and
// connect_snapshot_table's Preview find it with a type assertion.
func (d *DB) EstimatedRows(ctx context.Context, table string) (int64, error) {
	schema, name, err := splitTable(table)
	if err != nil {
		return 0, err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()
	var n int64
	err = d.queryRow(ctx, estimatedRowsSQL, schema, name).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("postgres: row estimate of %s: %w", table, clients.ErrNotFound)
	}
	if err != nil {
		return 0, fmt.Errorf("postgres: row estimate of %s: %w", table, err)
	}
	if n < 0 {
		n = -1
	}
	return n, nil
}
