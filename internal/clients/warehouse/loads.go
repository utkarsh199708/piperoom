package warehouse

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// LoadErrors is not supported by the Postgres warehouse: it keeps no load
// history. Callers fall back to TableActivity.
func (d *DB) LoadErrors(context.Context, string, time.Time, int) ([]clients.LoadError, error) {
	return nil, clients.ErrNotSupported
}

// TableActivity is what Postgres knows about a table's writes: the stand-in
// for a load history. Counters only, never a row.
type TableActivity struct {
	Table          string     `json:"table"`
	Inserts        int64      `json:"n_tup_ins"` // since the statistics were last reset
	DeadTuples     int64      `json:"n_dead_tup"`
	LastAutovacuum *time.Time `json:"last_autovacuum,omitempty"`
	LockWaiters    int        `json:"lock_waiters"` // sessions waiting on a lock on the table
}

// TableActivity reads pg_stat_user_tables and the waiting lock requests of
// table ("schema.table"). A table that is not in the statistics view is
// clients.ErrNotFound.
func (d *DB) TableActivity(ctx context.Context, table string) (TableActivity, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	parts, err := splitTableName(table)
	if err != nil {
		return TableActivity{}, err
	}
	a := TableActivity{Table: table}
	err = d.queryRow(ctx, `
		SELECT n_tup_ins, n_dead_tup, last_autovacuum
		FROM pg_stat_user_tables
		WHERE schemaname = $1 AND relname = $2`, parts[0], parts[1]).
		Scan(&a.Inserts, &a.DeadTuples, &a.LastAutovacuum)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TableActivity{}, fmt.Errorf("warehouse: table activity of %s: %w", table, clients.ErrNotFound)
		}
		return TableActivity{}, fmt.Errorf("warehouse: table activity of %s: %w", table, err)
	}
	err = d.queryRow(ctx, `
		SELECT count(*)::int
		FROM pg_locks l
		JOIN pg_class c ON c.oid = l.relation
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE NOT l.granted AND n.nspname = $1 AND c.relname = $2`, parts[0], parts[1]).
		Scan(&a.LockWaiters)
	if err != nil {
		return TableActivity{}, fmt.Errorf("warehouse: lock waits on %s: %w", table, err)
	}
	return a, nil
}

// splitTableName validates "schema.table" and returns the two parts.
func splitTableName(table string) ([]string, error) {
	schema, tbl, _, err := validateTableColumn(table, "id")
	if err != nil {
		return nil, err
	}
	return []string{schema, tbl}, nil
}

var _ clients.LoadErrorReader = (*DB)(nil)
