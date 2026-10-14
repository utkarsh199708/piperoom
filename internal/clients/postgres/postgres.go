// Package postgres implements clients.SourceDB against the CDC source
// Postgres database. Every query runs inside a READ ONLY transaction that is
// always rolled back.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
		// Defence in depth only: a session GUC can be changed by any function
		// a query calls (set_config). The guarantee is that every read runs
		// in its own READ ONLY transaction (see begin).
		_, err := conn.Exec(ctx, "SET default_transaction_read_only = on")
		if err != nil {
			return fmt.Errorf("postgres: set read-only: %w", err)
		}
		pgconv.RegisterJSONNumber(conn.TypeMap())
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

// begin starts a READ ONLY transaction for one client call. The caller defers
// d.end(ctx, tx).
//
// Why a transaction and not only default_transaction_read_only: a view or
// function the query touches can run set_config('default_transaction_read_only',
// 'off', false) and the pooled session would stay writable for the next user
// of the connection. Inside BEGIN READ ONLY no write can run (the mode cannot
// be turned off after the first query), and the rollback in end undoes every
// session setting changed during the transaction, so the connection goes back
// to the pool as it came out.
func (d *DB) begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("postgres: begin read-only transaction: %w", err)
	}
	return tx, nil
}

// end rolls tx back. It runs even when ctx is already cancelled, with its own
// short deadline; if the rollback fails pgx closes the connection, so a
// session that may hold a changed setting is never reused.
func (d *DB) end(ctx context.Context, tx pgx.Tx) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := tx.Rollback(rctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		slog.Warn("postgres: rollback of read-only transaction failed; connection discarded", "err", err)
	}
}

// slotColumns selects the columns of clients.ReplicationSlot.
//
//   - plugin is NULL for a physical slot; it reads "" (the DTO has no slot
//     type, so an empty plugin is how a physical slot shows).
//   - A slot that can no longer be used reads wal_status "lost" even where
//     Postgres leaves wal_status as something else or NULL: an invalidated
//     slot (invalidation_reason, PG17+; conflicting, PG16+) or a logical slot
//     with no restart_lsn. Such a slot holds no WAL, so its retained bytes are
//     0, and 0 must not read as healthy. The optional columns are read through
//     to_jsonb so the query also runs on PG13-15.
//   - A physical slot that never reserved WAL has no restart_lsn either; it
//     holds nothing and keeps wal_status "" and RestartLSN "".
const slotColumns = `
		SELECT slot_name, COALESCE(plugin, ''), active,
		       COALESCE(restart_lsn::text, ''),
		       COALESCE(confirmed_flush_lsn::text, ''),
		       CASE WHEN wal_status = 'lost'
		              OR (to_jsonb(s) ->> 'invalidation_reason') IS NOT NULL
		              OR (to_jsonb(s) ->> 'conflicting') = 'true'
		              OR (restart_lsn IS NULL AND slot_type = 'logical')
		            THEN 'lost'
		            ELSE COALESCE(wal_status, '') END,
		       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, 0),
		       safe_wal_size
		FROM pg_replication_slots s`

// scanSlots reads rows of slotColumns.
func scanSlots(rows pgx.Rows) ([]clients.ReplicationSlot, error) {
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

// ReplicationSlots lists the replication slots (logical and physical; a
// physical slot has Plugin "") with the retained WAL size and the
// safe_wal_size left before the slot is invalidated (PG13+, like wal_status).
// An invalidated slot reads WALStatus "lost".
func (d *DB) ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()
	tx, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer d.end(ctx, tx)

	rows, err := tx.Query(ctx, slotColumns+` ORDER BY slot_name`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list replication slots: %w", err)
	}
	return scanSlots(rows)
}

// RecentDDL returns DDL audit rows newer than since, newest first. If the
// audit table is missing it reports that the trigger is not installed.
func (d *DB) RecentDDL(ctx context.Context, since time.Time, limit int) ([]clients.DDLChange, error) {
	return d.recentDDL(ctx, since, "", limit)
}

// RecentDDLTable returns DDL audit rows for one table ("schema.table", exact
// catalog names, so "public.MixedCase" is the table created as
// "MixedCase") newer than since, newest first. A row matches when its
// object_identity is the table itself, one of its columns
// (schema.table.column, e.g. a column rename), an object attached to it
// ("name on schema.table": constraints, triggers, rules, policies) or one of
// its current indexes. The filter runs in SQL so rows older than limit are not
// missed.
func (d *DB) RecentDDLTable(ctx context.Context, since time.Time, table string, limit int) ([]clients.DDLChange, error) {
	if _, _, err := splitTable(table); err != nil {
		return nil, err
	}
	return d.recentDDL(ctx, since, table, limit)
}

// ddlTableFilter matches object_identity against the table quoted the way
// Postgres quotes identities (quote_ident: public."MixedCase"). $3 and $4 are
// the schema and table names.
const ddlTableFilter = `
		  AND (object_identity = t.ident
		       OR starts_with(object_identity, t.ident || '.')
		       OR right(object_identity, length(t.ident) + 4) = ' on ' || t.ident
		       OR object_identity IN (
		           SELECT quote_ident(n.nspname) || '.' || quote_ident(ic.relname)
		           FROM pg_index i
		           JOIN pg_class ic ON ic.oid = i.indexrelid
		           JOIN pg_namespace n ON n.oid = ic.relnamespace
		           WHERE i.indrelid = to_regclass(t.ident)))`

func (d *DB) recentDDL(ctx context.Context, since time.Time, table string, limit int) ([]clients.DDLChange, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	tx, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer d.end(ctx, tx)

	var installed bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('piperoom_audit.ddl_log') IS NOT NULL`).Scan(&installed); err != nil {
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
		FROM piperoom_audit.ddl_log`
	var rows pgx.Rows
	if table == "" {
		rows, err = tx.Query(ctx, cols+` WHERE at >= $1 ORDER BY at DESC LIMIT $2`, since, limit)
	} else {
		schema, tbl, serr := splitTable(table)
		if serr != nil {
			return nil, serr
		}
		rows, err = tx.Query(ctx, `
		WITH t AS (SELECT quote_ident($3::text) || '.' || quote_ident($4::text) AS ident)`+
			cols+`, t
		WHERE at >= $1`+ddlTableFilter+`
		ORDER BY at DESC LIMIT $2`, since, limit, schema, tbl)
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
	tx, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer d.end(ctx, tx)

	rows, err := tx.Query(ctx, `
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

// MaxRowValueBytes caps one variable-length column value that Row fetches
// (its text form). A larger value is not transferred: Row reports
// OmittedValue for it instead, so a wide TOASTed column (a document, an
// image) never reaches the process.
const MaxRowValueBytes = 8192

// OmittedValue is the value Row reports for a column over MaxRowValueBytes.
var OmittedValue = fmt.Sprintf("(omitted: larger than %d bytes)", MaxRowValueBytes)

// Row reads one row by primary key. table is "schema.table". Values are
// converted to JSON-friendly types (pgconv.ValueToJSON): numbers become
// strings, times become RFC 3339 strings, other types their Postgres text
// form. A variable-length value over MaxRowValueBytes reads OmittedValue.
func (d *DB) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	row, _, found, err := d.RowTyped(ctx, table, pkColumn, pk)
	return row, found, err
}

// RowTyped is Row plus the Postgres type OID of each column (keyed like the
// row), so a caller can tell a timestamptz column from a text column that
// only has a timestamp-like name. It is not on clients.SourceDB; the db tools
// find it with a type assertion.
func (d *DB) RowTyped(ctx context.Context, table, pkColumn, pk string) (map[string]any, map[string]uint32, bool, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	schema, tbl, err := splitTable(table)
	if err != nil {
		return nil, nil, false, err
	}
	if _, err := validateIdent(pkColumn); err != nil {
		return nil, nil, false, err
	}
	tx, err := d.begin(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	defer d.end(ctx, tx)

	cols, err := tableColumns(ctx, tx, schema, tbl)
	if err != nil {
		return nil, nil, false, err
	}
	q, err := rowQuery(table, pkColumn, cols)
	if err != nil {
		return nil, nil, false, err
	}
	// DescribeExec re-describes the statement on every call, so a concurrent
	// schema change (ALTER COLUMN ... TYPE) cannot break a cached plan.
	rows, err := tx.Query(ctx, q, pgx.QueryExecModeDescribeExec, pk)
	if err != nil {
		return nil, nil, false, fmt.Errorf("postgres: query row: %w", err)
	}
	defer rows.Close()

	fds := rows.FieldDescriptions()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, nil, false, fmt.Errorf("postgres: read row: %w", err)
		}
		return nil, nil, false, nil
	}
	vals, err := rows.Values()
	if err != nil {
		return nil, nil, false, fmt.Errorf("postgres: decode row: %w", err)
	}
	m, types := rowValues(cols, fds, vals)
	return m, types, true, nil
}

// column is one attribute of a table as Row selects it.
type column struct {
	name string
	// varlena is attlen = -1: a variable-length (possibly TOASTed) type whose
	// size Row caps.
	varlena bool
}

// tableColumns lists the live columns of schema.tbl in attnum order. A
// missing relation is clients.ErrNotFound.
func tableColumns(ctx context.Context, tx pgx.Tx, schema, tbl string) ([]column, error) {
	rows, err := tx.Query(ctx, `
		SELECT a.attname, a.attlen = -1
		FROM pg_attribute a
		WHERE a.attrelid = to_regclass($1::text)
		  AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, pgx.Identifier{schema, tbl}.Sanitize())
	if err != nil {
		return nil, fmt.Errorf("postgres: list columns of %s.%s: %w", schema, tbl, err)
	}
	defer rows.Close()
	var cols []column
	for rows.Next() {
		var c column
		if err := rows.Scan(&c.name, &c.varlena); err != nil {
			return nil, fmt.Errorf("postgres: scan column: %w", err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list columns of %s.%s: %w", schema, tbl, err)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("postgres: table %s.%s: %w", schema, tbl, clients.ErrNotFound)
	}
	return cols, nil
}

// rowValues maps the selected values back to column names. The last value is
// the oversize flag array when any column is varlena.
func rowValues(cols []column, fds []pgconn.FieldDescription, vals []any) (map[string]any, map[string]uint32) {
	var over []any
	if hasVarlena(cols) && len(vals) == len(cols)+1 {
		over, _ = vals[len(cols)].([]any)
	}
	m := make(map[string]any, len(cols))
	types := make(map[string]uint32, len(cols))
	vi := 0
	for i, c := range cols {
		if i >= len(vals) || i >= len(fds) {
			break
		}
		oid := fds[i].DataTypeOID
		types[c.name] = oid
		if c.varlena {
			if vi < len(over) {
				if b, ok := over[vi].(bool); ok && b {
					m[c.name] = OmittedValue
					vi++
					continue
				}
			}
			vi++
		}
		m[c.name] = pgconv.ValueToJSON(oid, vals[i])
	}
	return m, types
}

func hasVarlena(cols []column) bool {
	for _, c := range cols {
		if c.varlena {
			return true
		}
	}
	return false
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
	tx, err := d.begin(ctx)
	if err != nil {
		return "", err
	}
	defer d.end(ctx, tx)

	var col string
	err = tx.QueryRow(ctx, `
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

// rowQuery builds a parameterised SELECT for one row by primary key. It
// selects cols by name (never SELECT *): each variable-length column only when
// its text form is at most MaxRowValueBytes (else NULL), plus one bool[] of
// "too large" flags for the variable-length columns, in order. Column names
// come from the catalog and are quoted with pgx.Identifier.
func rowQuery(table, pkColumn string, cols []column) (string, error) {
	schema, tbl, err := splitTable(table)
	if err != nil {
		return "", err
	}
	col, err := validateIdent(pkColumn)
	if err != nil {
		return "", err
	}
	if len(cols) == 0 {
		return "", fmt.Errorf("postgres: table %s has no columns", table)
	}
	list := make([]string, 0, len(cols)+1)
	var flags []string
	for _, c := range cols {
		qc := pgx.Identifier{c.name}.Sanitize()
		if !c.varlena {
			list = append(list, qc)
			continue
		}
		list = append(list, fmt.Sprintf("CASE WHEN octet_length(%s::text) <= %d THEN %s END AS %s", qc, MaxRowValueBytes, qc, qc))
		flags = append(flags, fmt.Sprintf("COALESCE(octet_length(%s::text) > %d, false)", qc, MaxRowValueBytes))
	}
	if len(flags) > 0 {
		list = append(list, "ARRAY["+strings.Join(flags, ", ")+"]")
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1 LIMIT 1",
		strings.Join(list, ", "), pgx.Identifier{schema, tbl}.Sanitize(), pgx.Identifier{col}.Sanitize()), nil
}

func validateIdent(name string) (string, error) {
	if !identRe.MatchString(name) {
		return "", fmt.Errorf("postgres: invalid identifier %q", name)
	}
	return name, nil
}

var _ = clients.SourceDB((*DB)(nil))
