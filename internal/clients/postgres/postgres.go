// Package postgres implements clients.SourceDB against the CDC source
// Postgres database. Every read runs in its own READ ONLY transaction that is
// rolled back (pgconv.ReadOnlyPool).
package postgres

import (
	"context"
	"errors"
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
	pool   *pgxpool.Pool
	q      querier // the pool; a fake in tests
	target string  // receipt target: database@host:port, never credentials
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
	return &DB{pool: pool, q: pgconv.ReadOnlyPool{Pool: pool}, target: targetOf(pool)}, nil
}

// Close releases the pool.
func (d *DB) Close() {
	if d != nil && d.pool != nil {
		d.pool.Close()
	}
}

// ReplicationSlots lists replication slots with the retained WAL size and the
// safe_wal_size left before the slot is invalidated (PG13+, like wal_status).
// A physical slot (a standby's) is listed with an empty Plugin: it holds WAL
// too, and its NULL plugin must not fail the whole read. On a hot standby the
// retained WAL is measured from the received or replayed position (walHead).
func (d *DB) ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	rows, err := d.query(ctx, slotColumns+`
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

// RecentDDLTable returns DDL audit rows for one table ("schema.table", exact
// catalog names) and its columns newer than since, newest first. The table
// filter is applied in SQL (ddlTableFilter) so rows older than limit are not
// missed.
func (d *DB) RecentDDLTable(ctx context.Context, since time.Time, table string, limit int) ([]clients.DDLChange, error) {
	if _, _, err := splitTable(table); err != nil {
		return nil, err
	}
	return d.recentDDL(ctx, since, table, limit)
}

// ddlTableFilter matches the audit rows of one table ($2 schema, $3 name):
// the table itself, quoted the way the event trigger's object_identity is
// (public."MixedCase"), and its columns (schema.table.column), under which a
// column rename or comment is logged.
const ddlTableFilter = `
		  AND (object_identity = quote_ident($2::text) || '.' || quote_ident($3::text)
		       OR starts_with(object_identity, quote_ident($2::text) || '.' || quote_ident($3::text) || '.'))`

func (d *DB) recentDDL(ctx context.Context, since time.Time, table string, limit int) ([]clients.DDLChange, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	var installed bool
	if err := d.queryRow(ctx, `SELECT to_regclass('piperoom_audit.ddl_log') IS NOT NULL`).Scan(&installed); err != nil {
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
		rows, err = d.query(ctx, cols+` ORDER BY at DESC LIMIT $2`, since, limit)
	} else {
		schema, tbl, _ := splitTable(table) // validated by RecentDDLTable
		rows, err = d.query(ctx, cols+ddlTableFilter+` ORDER BY at DESC LIMIT $4`, since, schema, tbl, limit)
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
// Read-only transactions are listed too (they block DDL and VACUUM); HasXID
// (backend_xid IS NOT NULL) tells writers apart, because only a writer holds
// the WAL horizon for a replication slot. Callers that care filter on HasXID.
func (d *DB) LongTransactions(ctx context.Context, olderThan time.Duration) ([]clients.Transaction, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	rows, err := d.query(ctx, `
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
	rows, err := d.query(ctx, q, pgx.QueryExecModeDescribeExec, pk)
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
// caller knowing the key column name ahead of time. A table without a primary
// key is clients.ErrNotFound.
//
// It reads pg_catalog, not information_schema: information_schema shows a
// constraint only to the table's owner or a role with a privilege other than
// SELECT, so the documented SELECT-only role saw no key on any table (#385).
func (d *DB) PrimaryKeyColumn(ctx context.Context, table string) (string, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	schema, tbl, err := splitTable(table)
	if err != nil {
		return "", err
	}
	var col string
	err = d.queryRow(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[0]
		WHERE i.indisprimary AND n.nspname = $1 AND c.relname = $2`, schema, tbl).Scan(&col)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("postgres: primary key of %s: %w", table, clients.ErrNotFound)
	}
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
