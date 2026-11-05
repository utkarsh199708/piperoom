package pgconv

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// The column and count reads of the targeted re-snapshot (pg_resnapshot_rows,
// X2a): which kind of value a condition column takes, and how many rows a
// key or time range matches, on the source and on the dev Postgres sink.
// Same rules as the Reconciler above: identifiers checked and quoted, the
// pipeline scope checked before any I/O, 10 s, counts only, and a provenance
// receipt that never shows a bound.

// Kinds of a range column (RangeColumn.Kind): the value grammar the action
// parses a bound with before it builds a condition.
const (
	RangeKindInteger = "integer" // smallint, integer, bigint
	RangeKindText    = "text"    // text, varchar, char
	RangeKindUUID    = "uuid"
	RangeKindTime    = "time" // date, timestamp, timestamptz
)

// RangeColumn is one column as a re-snapshot condition needs it.
type RangeColumn struct {
	// Type is the catalog type name ("int8", "timestamptz", ...).
	Type string
	// Kind is one of RangeKind*, "" for a type no condition is built on.
	Kind string
	// PrimaryKey is true when the column is the table's whole primary key.
	PrimaryKey bool
}

// rangeColumnSQL reads one column's type and whether it alone is the
// primary key, from pg_catalog (visible to a SELECT-only role).
const rangeColumnSQL = `SELECT tn.nspname, t.typname, a.atttypid::int8,
		       EXISTS (SELECT 1 FROM pg_catalog.pg_index i
		               WHERE i.indrelid = c.oid AND i.indisprimary
		                 AND i.indnkeyatts = 1 AND i.indkey[0] = a.attnum)
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
		JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3
		  AND a.attnum > 0 AND NOT a.attisdropped`

func rangeKind(oid uint32) string {
	switch oid {
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		return RangeKindInteger
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return RangeKindText
	case pgtype.UUIDOID:
		return RangeKindUUID
	case pgtype.DateOID, pgtype.TimestampOID, pgtype.TimestamptzOID:
		return RangeKindTime
	}
	return ""
}

// RangeColumn reads column of table: its type, kind and whether it is the
// table's whole primary key. A missing table or column is
// clients.ErrNotFound.
func (r Reconcile) RangeColumn(ctx context.Context, table, column string) (RangeColumn, error) {
	rel, err := r.table(table)
	if err != nil {
		return RangeColumn{}, err
	}
	if err := r.ident("column", column); err != nil {
		return RangeColumn{}, err
	}
	if err := r.scope(ctx, table); err != nil {
		return RangeColumn{}, err
	}
	ctx, cancel := WithDeadline(ctx, clients.ReconcileTimeout)
	defer cancel()
	rc, _, err := r.rangeColumn(ctx, rel, table, column)
	return rc, err
}

// rangeColumn reads the column and returns its type as an identifier too,
// for the count's cast.
func (r Reconcile) rangeColumn(ctx context.Context, rel pgx.Identifier, table, column string) (RangeColumn, pgx.Identifier, error) {
	rows, err := r.Query(ctx, nil, rangeColumnSQL, rel[0], rel[1], column)
	if err != nil {
		return RangeColumn{}, nil, r.fail(ctx, "column "+column+" of "+table, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return RangeColumn{}, nil, r.fail(ctx, "column "+column+" of "+table, err)
		}
		return RangeColumn{}, nil, fmt.Errorf("%s: %s has no column %q: %w", r.Name, table, column, clients.ErrNotFound)
	}
	var schema, name string
	var oid int64
	var rc RangeColumn
	if err := rows.Scan(&schema, &name, &oid, &rc.PrimaryKey); err != nil {
		return RangeColumn{}, nil, r.fail(ctx, "column "+column+" of "+table, err)
	}
	if err := rows.Err(); err != nil {
		return RangeColumn{}, nil, r.fail(ctx, "column "+column+" of "+table, err)
	}
	rc.Type, rc.Kind = name, rangeKind(uint32(oid))
	return rc, pgx.Identifier{schema, name}, nil
}

// CountRange counts the rows of table whose column is >= from and, when to
// is not "", <= to: the rows a re-snapshot condition on that range matches.
// The bounds are text cast to the column's own type in the statement, as
// Postgres reads the quoted literals of the condition, so both count the
// same rows. A column of no RangeKind is tools.ErrBadInput; a bound that is
// not a value of the type is tools.ErrBadInput too, and never quoted back.
// It returns a count only; the receipt shows "<bound>" for each bound.
func (r Reconcile) CountRange(ctx context.Context, table, column, from, to string) (int64, error) {
	rel, err := r.table(table)
	if err != nil {
		return 0, err
	}
	if err := r.ident("column", column); err != nil {
		return 0, err
	}
	if from == "" {
		return 0, fmt.Errorf("%s: count %s: no lower bound: %w", r.Name, table, tools.ErrBadInput)
	}
	if err := r.scope(ctx, table); err != nil {
		return 0, err
	}
	ctx, cancel := WithDeadline(ctx, clients.ReconcileTimeout)
	defer cancel()

	rc, typ, err := r.rangeColumn(ctx, rel, table, column)
	if err != nil {
		return 0, err
	}
	if rc.Kind == "" || !reconcileIdentRe.MatchString(typ[0]) || !reconcileIdentRe.MatchString(typ[1]) {
		return 0, fmt.Errorf("%s: count %s: column %q is %s, not an integer, text, uuid, date or timestamp column: %w", r.Name, table, column, cut(rc.Type), tools.ErrBadInput)
	}
	args, receipt := []any{from}, []any{"<bound>"}
	if to != "" {
		args, receipt = append(args, to), append(receipt, "<bound>")
	}
	rows, err := r.Query(ctx, receipt, countRangeSQL(rel, column, typ, to != ""), args...)
	if err != nil {
		return 0, r.fail(ctx, "count "+table, err)
	}
	defer rows.Close()
	var n int64
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, r.fail(ctx, "count "+table, err)
		}
		return 0, fmt.Errorf("%s: count %s: no row returned", r.Name, table)
	}
	if err := rows.Scan(&n); err != nil {
		return 0, r.fail(ctx, "count "+table, err)
	}
	if err := rows.Err(); err != nil {
		return 0, r.fail(ctx, "count "+table, err)
	}
	return n, nil
}

// countRangeSQL builds the range count. Every identifier was validated and
// is quoted here; the bounds are parameters.
func countRangeSQL(rel pgx.Identifier, column string, typ pgx.Identifier, upper bool) string {
	c, t := pgx.Identifier{column}.Sanitize(), typ.Sanitize()
	q := fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s >= $1::text::%s`, rel.Sanitize(), c, t)
	if upper {
		q += fmt.Sprintf(` AND %s <= $2::text::%s`, c, t)
	}
	return q
}
