// Package postgres implements clients.SourceDB against the CDC source
// Postgres database. Every connection is opened read-only.
package postgres

import (
	"context"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/clients"
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
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	pctx, cancel := withDeadline(ctx, 10*time.Second)
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

// ReplicationSlots lists logical replication slots with the retained WAL size.
func (d *DB) ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error) {
	ctx, cancel := withDeadline(ctx, 10*time.Second)
	defer cancel()

	rows, err := d.pool.Query(ctx, `
		SELECT slot_name, plugin, active,
		       COALESCE(restart_lsn::text, ''),
		       COALESCE(confirmed_flush_lsn::text, ''),
		       COALESCE(wal_status, ''),
		       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, 0)
		FROM pg_replication_slots
		ORDER BY slot_name`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list replication slots: %w", err)
	}
	defer rows.Close()

	var out []clients.ReplicationSlot
	for rows.Next() {
		var s clients.ReplicationSlot
		if err := rows.Scan(&s.Name, &s.Plugin, &s.Active, &s.RestartLSN, &s.ConfirmedFlushLSN, &s.WALStatus, &s.RetainedWALBytes); err != nil {
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
	ctx, cancel := withDeadline(ctx, 10*time.Second)
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

	rows, err := d.pool.Query(ctx, `
		SELECT at,
		       COALESCE(command_tag, ''),
		       COALESCE(object_identity, ''),
		       COALESCE(query, ''),
		       COALESCE(username, '')
		FROM piperoom_audit.ddl_log
		WHERE at >= $1
		ORDER BY at DESC
		LIMIT $2`, since, limit)
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
// this backend.
func (d *DB) LongTransactions(ctx context.Context, olderThan time.Duration) ([]clients.Transaction, error) {
	ctx, cancel := withDeadline(ctx, 10*time.Second)
	defer cancel()

	rows, err := d.pool.Query(ctx, `
		SELECT pid,
		       COALESCE(usename, ''),
		       COALESCE(state, ''),
		       EXTRACT(EPOCH FROM (now() - xact_start))::bigint,
		       COALESCE(left(query, 500), '')
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
		if err := rows.Scan(&t.PID, &t.Username, &t.State, &ageSec, &t.Query); err != nil {
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
	ctx, cancel := withDeadline(ctx, 10*time.Second)
	defer cancel()

	q, err := rowQuery(table, pkColumn)
	if err != nil {
		return nil, false, err
	}
	rows, err := d.pool.Query(ctx, q, pk)
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
			m[fd.Name] = toJSON(vals[i])
		}
	}
	return m, true, nil
}

// PrimaryKeyColumn returns the first primary key column of a table. It is an
// optional capability used by the record trace to look up rows without the
// caller knowing the key column name ahead of time.
func (d *DB) PrimaryKeyColumn(ctx context.Context, table string) (string, error) {
	ctx, cancel := withDeadline(ctx, 10*time.Second)
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

// toJSON converts a decoded pgx value into a JSON-friendly one.
func toJSON(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case int64:
		return strconv.FormatInt(t, 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int16:
		return strconv.FormatInt(int64(t), 10)
	case int8:
		return strconv.FormatInt(int64(t), 10)
	case int:
		return strconv.Itoa(t)
	case uint64:
		return strconv.FormatUint(t, 10)
	case uint32:
		return strconv.FormatUint(uint64(t), 10)
	case uint16:
		return strconv.FormatUint(uint64(t), 10)
	case uint8:
		return strconv.FormatUint(uint64(t), 10)
	case uint:
		return strconv.FormatUint(uint64(t), 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case pgtype.Numeric:
		return numericString(t)
	case []byte:
		return string(t)
	case string:
		return t
	case bool:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}

// numericString renders a PostgreSQL numeric without float rounding.
func numericString(n pgtype.Numeric) string {
	if !n.Valid {
		return ""
	}
	if n.NaN {
		return "NaN"
	}
	switch n.InfinityModifier {
	case pgtype.Infinity:
		return "Infinity"
	case pgtype.NegativeInfinity:
		return "-Infinity"
	}
	if n.Int == nil {
		return "0"
	}
	neg := n.Int.Sign() < 0
	abs := new(big.Int).Abs(n.Int)
	s := abs.String()
	if n.Exp > 0 {
		e := int(n.Exp)
		if e > 100000 {
			return s + "e+" + strconv.Itoa(e)
		}
		s += strings.Repeat("0", e)
	} else if n.Exp < 0 {
		e := -int(n.Exp)
		if e > 100000 {
			return s + "e" + strconv.Itoa(int(n.Exp))
		}
		point := len(s) - e
		if point <= 0 {
			s = "0." + strings.Repeat("0", -point) + s
		} else {
			s = s[:point] + "." + s[point:]
		}
	}
	if neg {
		s = "-" + s
	}
	return s
}

// withDeadline returns ctx unchanged when it already has a deadline, and
// otherwise applies a default timeout.
func withDeadline(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

var _ = clients.SourceDB((*DB)(nil))
