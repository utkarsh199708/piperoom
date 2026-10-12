// Package postgres implements clients.SourceDB against the CDC source
// Postgres database. Every connection is opened read-only.
package postgres

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// DB is a read-only connection pool to the source Postgres.
type DB struct {
	pool *pgxpool.Pool
}

// New opens a read-only connection pool and verifies it with a ping.
func New(ctx context.Context, url string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse config: %w", err)
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET default_transaction_read_only = on")
		if err != nil {
			return fmt.Errorf("postgres: set read-only: %w", err)
		}
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	pctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &DB{pool: pool}, nil
}

// Close releases the pool.
func (d *DB) Close() {
	if d != nil && d.pool != nil {
		d.pool.Close()
	}
}

// ReplicationSlots lists logical replication slots with the retained WAL size
// and the safe_wal_size left before the slot is invalidated (PG13+, like wal_status).
func (d *DB) ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	rows, err := d.pool.Query(ctx, `
		SELECT slot_name, plugin, active,
		       COALESCE(restart_lsn::text, ''),
		       COALESCE(confirmed_flush_lsn::text, ''),
		       COALESCE(wal_status, ''),
		       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, 0),
		       safe_wal_size
		FROM pg_replication_slots
		ORDER BY slot_name`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list replication slots: %w", err)
	}
	defer rows.Close()

	var out []clients.ReplicationSlot
	for rows.Next() {
		var s clients.ReplicationSlot
		if err := rows.Scan(&s.Name, &s.Plugin, &s.Active, &s.RestartLSN, &s.ConfirmedFlushLSN, &s.WALStatus, &s.RetainedWALBytes, &s.SafeWALBytes); err != nil {
			return nil, fmt.Errorf("postgres: scan replication slot: %w", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read replication slots: %w", err)
	}
	return out, nil
}

// RecentDDL returns DDL audit rows newer than since, newest first. If the
// audit table is missing it reports that the trigger is not installed.
func (d *DB) RecentDDL(ctx context.Context, since time.Time, limit int) ([]clients.DDLChange, error) {
	return d.recentDDL(ctx, since, "", limit)
}

// RecentDDLTable returns DDL audit rows for one table ("schema.table") newer
// than since, newest first. The table filter is applied in SQL as an exact
// object_identity match so rows older than limit are not missed.
func (d *DB) RecentDDLTable(ctx context.Context, since time.Time, table string, limit int) ([]clients.DDLChange, error) {
	if _, _, err := splitTable(table); err != nil {
		return nil, err
	}
	return d.recentDDL(ctx, since, table, limit)
}

func (d *DB) recentDDL(ctx context.Context, since time.Time, table string, limit int) ([]clients.DDLChange, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	var installed bool
	if err := d.pool.QueryRow(ctx, `SELECT to_regclass('piperoom_audit.ddl_log') IS NOT NULL`).Scan(&installed); err != nil {
		return nil, fmt.Errorf("postgres: check ddl audit table: %w", err)
	}
	if !installed {
		return nil, fmt.Errorf("postgres: DDL audit trigger is not installed: table piperoom_audit.ddl_log does not exist")
	}

	const cols = `
		SELECT at,
		       COALESCE(command_tag, ''),
		       COALESCE(object_identity, ''),
		       COALESCE(query, ''),
		       COALESCE(username, '')
		FROM piperoom_audit.ddl_log
		WHERE at >= $1`
	var rows pgx.Rows
	var err error
	if table == "" {
		rows, err = d.pool.Query(ctx, cols+` ORDER BY at DESC LIMIT $2`, since, limit)
	} else {
		rows, err = d.pool.Query(ctx, cols+` AND object_identity = $2 ORDER BY at DESC LIMIT $3`, since, table, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read ddl log: %w", err)
	}
	defer rows.Close()

	var out []clients.DDLChange
	for rows.Next() {
		var c clients.DDLChange
		if err := rows.Scan(&c.At, &c.CommandTag, &c.ObjectIdentity, &c.Query, &c.Username); err != nil {
			return nil, fmt.Errorf("postgres: scan ddl change: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read ddl log: %w", err)
	}
	return out, nil
}

// LongTransactions lists transactions open longer than olderThan, excluding
// this backend. It never returns query text: pg_stat_activity.query often holds
// literal row values, so only the statement verb (first keyword) is selected.
// Only transactions that hold a transaction id (backend_xid IS NOT NULL) are
// returned: a read-only transaction has no xid and pins no WAL for a
// replication slot, so listing BI reads would only add noise.
func (d *DB) LongTransactions(ctx context.Context, olderThan time.Duration) ([]clients.Transaction, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	rows, err := d.pool.Query(ctx, `
		SELECT pid,
		       COALESCE(usename, ''),
		       COALESCE(application_name, ''),
		       COALESCE(state, ''),
		       EXTRACT(EPOCH FROM (now() - xact_start))::bigint,
		       COALESCE(wait_event_type || ':' || wait_event, ''),
		       COALESCE(upper(substring(ltrim(query, E' \t\r\n') from '^[A-Za-z]+')), ''),
		       backend_xid IS NOT NULL
		FROM pg_stat_activity
		WHERE xact_start IS NOT NULL
		  AND backend_xid IS NOT NULL
		  AND xact_start < now() - make_interval(secs => $1)
		  AND pid <> pg_backend_pid()
		ORDER BY xact_start ASC`, olderThan.Seconds())
	if err != nil {
		return nil, fmt.Errorf("postgres: list long transactions: %w", err)
	}
	defer rows.Close()

	var out []clients.Transaction
	for rows.Next() {
		var t clients.Transaction
		var ageSec int64
		if err := rows.Scan(&t.PID, &t.Username, &t.ApplicationName, &t.State, &ageSec, &t.WaitEvent, &t.Query, &t.HasXID); err != nil {
			return nil, fmt.Errorf("postgres: scan transaction: %w", err)
		}
		t.Age = time.Duration(ageSec) * time.Second
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read long transactions: %w", err)
	}
	return out, nil
}

// Row reads one row by primary key. table is "schema.table". Values are
// converted to JSON-friendly types: numbers become strings, times become RFC
// 3339 strings.
func (d *DB) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	q, err := rowQuery(table, pkColumn)
	if err != nil {
		return nil, false, err
	}
	// DescribeExec re-describes the statement on every call, so a concurrent
	// schema change (ALTER COLUMN ... TYPE) cannot break a cached plan.
	rows, err := d.pool.Query(ctx, q, pgx.QueryExecModeDescribeExec, pk)
	if err != nil {
		return nil, false, fmt.Errorf("postgres: query row: %w", err)
	}
	defer rows.Close()

	cols := rows.FieldDescriptions()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, false, fmt.Errorf("postgres: read row: %w", err)
		}
		return nil, false, nil
	}
	vals, err := rows.Values()
	if err != nil {
		return nil, false, fmt.Errorf("postgres: decode row: %w", err)
	}
	m := make(map[string]any, len(cols))
	for i, fd := range cols {
		if i < len(vals) {
			m[fd.Name] = pgconv.ToJSON(vals[i])
		}
	}
	return m, true, nil
}

// PrimaryKeyColumn returns the first primary key column of a table. It is an
// optional capability used by the record trace to look up rows without the
// caller knowing the key column name ahead of time.
func (d *DB) PrimaryKeyColumn(ctx context.Context, table string) (string, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	schema, tbl, err := splitTable(table)
	if err != nil {
		return "", err
	}
	var col string
	err = d.pool.QueryRow(ctx, `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		 AND tc.constraint_schema = kcu.constraint_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
		  AND tc.table_schema = $1
		  AND tc.table_name = $2
		ORDER BY kcu.ordinal_position
		LIMIT 1`, schema, tbl).Scan(&col)
	if err != nil {
		return "", fmt.Errorf("postgres: primary key of %s: %w", table, err)
	}
	return col, nil
}

var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// splitTable validates a "schema.table" identifier and returns the raw parts.
func splitTable(table string) (string, string, error) {
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("postgres: table must be \"schema.table\", got %q", table)
	}
	schema, err := validateIdent(parts[0])
	if err != nil {
		return "", "", err
	}
	tbl, err := validateIdent(parts[1])
	if err != nil {
		return "", "", err
	}
	return schema, tbl, nil
}

// rowQuery builds a parameterised SELECT for one row by primary key.
func rowQuery(table, pkColumn string) (string, error) {
	schema, tbl, err := splitTable(table)
	if err != nil {
		return "", err
	}
	col, err := validateIdent(pkColumn)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s = $1 LIMIT 1",
		pgx.Identifier{schema, tbl}.Sanitize(), pgx.Identifier{col}.Sanitize()), nil
}

func validateIdent(name string) (string, error) {
	if !identRe.MatchString(name) {
		return "", fmt.Errorf("postgres: invalid identifier %q", name)
	}
	return name, nil
}

var _ = clients.SourceDB((*DB)(nil))
