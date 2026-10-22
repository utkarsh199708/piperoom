package pgconv

import (
	"context"
	"fmt"

	"piperoom/internal/clients"
)

// Columns is the clients.ColumnReader (X7) of both Postgres-backed clients:
// the columns of one of the pipeline's tables, from pg_catalog, in ordinal
// order, at most clients.MaxTableColumns. Names, type names, positions and
// nullability only: no default expression, comment or row is read, and no
// query runs on the table itself. Identifier and scope checks come first, so a
// refused call does no I/O.
func (r Reconcile) Columns(ctx context.Context, table string) ([]clients.ColumnInfo, error) {
	rel, err := r.table(table)
	if err != nil {
		return nil, err
	}
	if err := r.scopeFor(ctx, table, "columns"); err != nil {
		return nil, err
	}
	ctx, cancel := WithDeadline(ctx, clients.CatalogTimeout)
	defer cancel()

	rows, err := r.Query(ctx, nil, columnsSQL, rel[0], rel[1], clients.MaxTableColumns)
	if err != nil {
		return nil, r.fail(ctx, "columns of "+table, err)
	}
	defer rows.Close()
	out := []clients.ColumnInfo{}
	found := false
	for rows.Next() {
		found = true
		var c clients.ColumnInfo
		var pos int64
		if err := rows.Scan(&c.Name, &c.Type, &pos, &c.Nullable); err != nil {
			return nil, r.fail(ctx, "columns of "+table, err)
		}
		// Position 0 is the LEFT JOIN's row for a table without columns.
		if pos > 0 {
			c.Position = int(pos)
			out = append(out, c)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, r.fail(ctx, "columns of "+table, err)
	}
	if !found {
		return nil, fmt.Errorf("%s: columns of %s: no such table or view: %w", r.Name, table, clients.ErrNotFound)
	}
	return out, nil
}

// columnsSQL reads the column list from pg_catalog, which a SELECT-only role
// can see (information_schema hides what the role does not own). The LEFT
// JOIN keeps one row for a table with no columns, so "no row" means "no such
// relation". format_type gives the type as information_schema spells it for
// built-in types ("timestamp with time zone", "character varying"); the
// modifier is left out (NULL), so no length or precision is read.
const columnsSQL = `SELECT coalesce(a.attname::text, ''), coalesce(format_type(a.atttypid, NULL), ''),
		       coalesce(a.attnum, 0)::int8, coalesce(NOT a.attnotnull, false)
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
		WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
		ORDER BY a.attnum
		LIMIT $3`
