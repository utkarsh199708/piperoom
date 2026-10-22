package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"piperoom/internal/clients/pgconv"
	"piperoom/internal/tools"
)

// The catalog side of the TOAST truncation check (X1d, pg_toast_risk). This
// method is not on clients.SourceDB (contract change request in the X1d
// report); the dbtools tool finds it with a type assertion, the same pattern
// as PublicationSourceTables.

const (
	// MaxToastTables caps the tables one ToastColumns call reads; more is
	// tools.ErrBadInput before any I/O.
	MaxToastTables = 500
	// MaxToastColumns caps the column rows one call returns. A wider result is
	// cut (ToastColumns reports cut), never refused.
	MaxToastColumns = 5000
)

// ToastTable is one source table as the catalog describes it for TOAST:
// its replica identity, the size of its TOAST relation and its toastable
// columns. Catalog facts only: no row, no value, no statistic other than
// the average stored width.
type ToastTable struct {
	Table string `json:"table"` // "schema.table", as asked
	// Found is false when no ordinary or partitioned table of that name
	// exists (or the role cannot see it); the other fields are then empty.
	Found bool `json:"found"`
	// ReplicaIdentity is pg_class.relreplident spelled out: DEFAULT, FULL,
	// INDEX or NOTHING.
	ReplicaIdentity string `json:"replica_identity"`
	// ToastBytes is pg_relation_size of the table's TOAST relation: above
	// zero, some values of the table are (or were, until a VACUUM truncates)
	// stored out of line. 0 when the table has no TOAST relation (a
	// partitioned root has none; its partitions do).
	ToastBytes int64 `json:"toast_bytes"`
	// Columns are the table's toastable columns (attstorage x or e), in
	// column order.
	Columns []ToastColumn `json:"columns,omitempty"`
}

// ToastColumn is one toastable column.
type ToastColumn struct {
	Name string `json:"name"`
	// Type is format_type(atttypid, atttypmod), e.g. "text", "jsonb".
	Type string `json:"type"`
	// Storage is attstorage: "x" (extended: compressed, then out of line) or
	// "e" (external: out of line, uncompressed).
	Storage string `json:"storage"`
	// AvgWidth is pg_stats.avg_width, the average STORED width in bytes (a
	// value moved out of line counts as its 18-byte TOAST pointer); -1 when
	// the column has no statistics (never analysed, or not visible to the
	// role).
	AvgWidth int `json:"avg_width"`
}

// replicaIdentityWords spells pg_class.relreplident out.
var replicaIdentityWords = map[string]string{"d": "DEFAULT", "f": "FULL", "i": "INDEX", "n": "NOTHING"}

// toastSQL reads, for each requested table ($1 schemas, $2 names, matched
// pairwise, in order), the replica identity, the TOAST relation's size and
// every toastable column with its average stored width. Only pg_namespace,
// pg_class, pg_attribute and the avg_width column of pg_stats are read; the
// table itself is never scanned. pg_stats shows a row only for columns the
// role may read, and max() folds the inherited and the plain row of a
// partitioned table into one.
const toastSQL = `
		SELECT v.pos,
		       c.oid IS NOT NULL,
		       COALESCE(c.relreplident::text, ''),
		       COALESCE(CASE WHEN c.reltoastrelid <> 0 THEN pg_relation_size(c.reltoastrelid) END, 0)::bigint,
		       COALESCE(a.attname::text, ''),
		       COALESCE(format_type(a.atttypid, a.atttypmod), ''),
		       COALESCE(a.attstorage::text, ''),
		       COALESCE((SELECT max(s.avg_width) FROM pg_stats s
		                 WHERE s.schemaname = v.nsp AND s.tablename = v.relname AND s.attname = a.attname), -1)::int
		FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS v(nsp, relname, pos)
		LEFT JOIN pg_namespace n ON n.nspname = v.nsp
		LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = v.relname AND c.relkind IN ('r', 'p')
		LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
		                        AND a.attstorage IN ('x', 'e')
		ORDER BY v.pos, a.attnum
		LIMIT $3`

// ToastColumns reads the TOAST facts of tables (each "schema.table"), one
// entry per table in that order. It fails closed on scope before any I/O:
// every table must be one of the source_tables of the pipeline in ctx
// (tools.WithPipeline), else tools.ErrOutOfScope; a ctx with no pipeline is
// tools.ErrNoScope unless the caller opted out with tools.WithUnscoped. A bad
// identifier or more than MaxToastTables tables is tools.ErrBadInput, also
// before any I/O. cut is true when the column rows passed MaxToastColumns
// and the last tables' columns are incomplete. Deadline 10 s.
func (d *DB) ToastColumns(ctx context.Context, tables []string) (out []ToastTable, cut bool, err error) {
	if len(tables) > MaxToastTables {
		return nil, false, fmt.Errorf("postgres: toast columns: %d tables, at most %d: %w", len(tables), MaxToastTables, tools.ErrBadInput)
	}
	schemas := make([]string, 0, len(tables))
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		schema, tbl, err := splitTable(t)
		if err != nil {
			return nil, false, fmt.Errorf("%w: %w", tools.ErrBadInput, err)
		}
		schemas = append(schemas, schema)
		names = append(names, tbl)
	}
	if err := toastScope(ctx, tables); err != nil {
		return nil, false, err
	}
	if len(tables) == 0 {
		return nil, false, nil
	}
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	rows, err := d.query(ctx, toastSQL, schemas, names, MaxToastColumns+1)
	if err != nil {
		return nil, false, fmt.Errorf("postgres: toast columns: %w", err)
	}
	defer rows.Close()

	out = make([]ToastTable, len(tables))
	for i, t := range tables {
		out[i] = ToastTable{Table: t}
	}
	n := 0
	for rows.Next() {
		var (
			pos                      int64
			found                    bool
			ident, col, typ, storage string
			toastBytes               int64
			avgWidth                 int
		)
		if err := rows.Scan(&pos, &found, &ident, &toastBytes, &col, &typ, &storage, &avgWidth); err != nil {
			return nil, false, fmt.Errorf("postgres: toast columns: scan: %w", err)
		}
		if pos < 1 || int(pos) > len(out) {
			return nil, false, fmt.Errorf("postgres: toast columns: row for table %d of %d", pos, len(out))
		}
		n++
		if n > MaxToastColumns {
			cut = true
			break
		}
		t := &out[pos-1]
		t.Found = found
		if !found {
			continue
		}
		t.ReplicaIdentity = replicaIdentityWords[ident]
		if t.ReplicaIdentity == "" {
			t.ReplicaIdentity = ident
		}
		t.ToastBytes = toastBytes
		if col != "" {
			t.Columns = append(t.Columns, ToastColumn{Name: col, Type: typ, Storage: storage, AvgWidth: avgWidth})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("postgres: toast columns: %w", err)
	}
	return out, cut, nil
}

// toastScope is the fail-closed scope rule of the source writer applied to a
// catalog read: the pipeline in ctx must list every table as a source table.
// The client has no owners config, so a ctx holding only a pipeline id is
// tools.ErrNoScope; the tool passes the resolved pipeline.
func toastScope(ctx context.Context, tables []string) error {
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %w", tools.ErrNoScope, err)
		}
		return fmt.Errorf("postgres: toast columns refused: it needs the pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped opt-out: %w", err)
	}
	for _, t := range tables {
		if err := sc.CheckSourceTable(t); err != nil {
			return fmt.Errorf("postgres: toast columns: %w", err)
		}
	}
	return nil
}
