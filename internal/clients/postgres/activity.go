package postgres

import (
	"context"
	"fmt"
	"time"

	"piperoom/internal/clients/pgconv"
)

// maxActivityTables bounds one TableCounters call: a pipeline has a handful of
// source tables, so a long list is a caller bug, not a reason to scan the
// whole statistics view.
const maxActivityTables = 200

// TableCounters is one table's row-change counters from pg_stat_user_tables.
// The counters only grow (until the statistics are reset), so two readings tell
// whether the table took inserts, updates or deletes in between. No row value
// is read.
type TableCounters struct {
	Table      string `json:"table"` // "schema.table"
	Inserts    int64  `json:"n_tup_ins"`
	Updates    int64  `json:"n_tup_upd"`
	Deletes    int64  `json:"n_tup_del"`
	LiveTuples int64  `json:"n_live_tup"`
	// LastAutovacuum and LastAutoanalyze are nil when they never ran.
	LastAutovacuum  *time.Time `json:"last_autovacuum,omitempty"`
	LastAutoanalyze *time.Time `json:"last_autoanalyze,omitempty"`
}

// TableCounters reads the change counters of the given tables ("schema.table")
// from pg_stat_user_tables. Tables the view does not list (missing, or no
// statistics yet) are simply absent from the result. The names travel as one
// $1 array parameter, never as SQL text; they are validated first all the same,
// so a bad name fails before any statement runs.
func (d *DB) TableCounters(ctx context.Context, tables []string) ([]TableCounters, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	if len(tables) > maxActivityTables {
		return nil, fmt.Errorf("postgres: table activity: %d tables, the limit is %d", len(tables), maxActivityTables)
	}
	for _, t := range tables {
		if _, _, err := splitTable(t); err != nil {
			return nil, fmt.Errorf("postgres: table activity: %w", err)
		}
	}
	// The context deadline is the statement timeout: pgx cancels the query on
	// the server when it expires.
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	rows, err := d.query(ctx, `
		SELECT schemaname || '.' || relname,
		       n_tup_ins, n_tup_upd, n_tup_del, n_live_tup,
		       last_autovacuum, last_autoanalyze
		FROM pg_stat_user_tables
		WHERE schemaname || '.' || relname = ANY($1::text[])
		ORDER BY 1`, tables)
	if err != nil {
		return nil, fmt.Errorf("postgres: table activity: %w", err)
	}
	defer rows.Close()

	var out []TableCounters
	for rows.Next() {
		var a TableCounters
		if err := rows.Scan(&a.Table, &a.Inserts, &a.Updates, &a.Deletes, &a.LiveTuples, &a.LastAutovacuum, &a.LastAutoanalyze); err != nil {
			return nil, fmt.Errorf("postgres: table activity: scan: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: table activity: %w", err)
	}
	return out, nil
}
