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

// TableSize is the on-disk size of one table, read from the catalog without
// scanning a row. TableBytes is pg_table_size (heap, TOAST and the free-space
// and visibility maps), IndexBytes is pg_indexes_size, TotalBytes their sum
// (pg_total_relation_size). A partitioned table's figures sum its leaves.
// pg_replident's FULL mode writes the whole old row to WAL, so TableBytes is
// the scale of the extra WAL a FULL identity costs per pass over every row
// (internal/actions/sourceactions).
type TableSize struct {
	TableBytes int64 `json:"table_bytes"`
	IndexBytes int64 `json:"index_bytes"`
	TotalBytes int64 `json:"total_bytes"`
}

// tableSizeSQL reads the sizes of table $1.$2. pg_partition_tree is empty for
// a table outside a partition tree, so the table's own oid is unioned in (the
// same rule as publishedCTEs); the internal nodes of a partition tree carry
// no storage and add 0. No row: no ordinary or partitioned table of that
// schema and name.
const tableSizeSQL = `
		SELECT COALESCE((SELECT sum(pg_table_size(x.relid))::bigint
		                 FROM (SELECT t.relid FROM pg_partition_tree(c.oid) t
		                       UNION
		                       SELECT c.oid) x), 0),
		       COALESCE((SELECT sum(pg_indexes_size(x.relid))::bigint
		                 FROM (SELECT t.relid FROM pg_partition_tree(c.oid) t
		                       UNION
		                       SELECT c.oid) x), 0)
		FROM pg_namespace n
		JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = $2::name
		WHERE n.nspname = $1::name AND c.relkind IN ('r', 'p')`

// TableSize returns the on-disk size of table ("schema.table"). A view,
// foreign table or missing table is clients.ErrNotFound. The identifiers are
// checked before any I/O and travel as parameters. Deadline 10 s (AGENTS.md
// rule 4). Not on clients.SourceDB: the Q2c source actions find it with a type
// assertion, like PublicationAddState.
func (d *DB) TableSize(ctx context.Context, table string) (TableSize, error) {
	schema, name, err := splitTable(table)
	if err != nil {
		return TableSize{}, err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	var ts TableSize
	err = d.queryRow(ctx, tableSizeSQL, schema, name).Scan(&ts.TableBytes, &ts.IndexBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return TableSize{}, fmt.Errorf("postgres: size of %s: %w", table, clients.ErrNotFound)
	}
	if err != nil {
		return TableSize{}, fmt.Errorf("postgres: size of %s: %w", table, err)
	}
	ts.TotalBytes = ts.TableBytes + ts.IndexBytes
	return ts, nil
}
