package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
)

// ReplicationSlots: MySQL has no replication slots.
func (d *DB) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	return nil, fmt.Errorf("mysql: replication slots: MySQL has none: %w", clients.ErrNotSupported)
}

// RecentDDL: MySQL has no DDL event trigger. A MySQL pipeline's DDL history is
// the connector's schema history topic (tool kafka_schema_history).
func (d *DB) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	return nil, fmt.Errorf("mysql: recent DDL: no DDL trigger on MySQL; read the connector's schema history topic: %w", clients.ErrNotSupported)
}

// longTransactionsSQL lists InnoDB transactions open for at least ? seconds
// (server clock), oldest first, without this session. Only the statement's
// first keyword is selected: PROCESSLIST_INFO often holds literal row values
// and belongs to any user, so its text never leaves the server.
const longTransactionsSQL = `
		SELECT t.PROCESSLIST_ID,
		       COALESCE(t.PROCESSLIST_USER, ''),
		       COALESCE(a.ATTR_VALUE, ''),
		       COALESCE(t.PROCESSLIST_COMMAND, ''),
		       TIMESTAMPDIFF(SECOND, x.trx_started, NOW()),
		       COALESCE(x.trx_state, ''),
		       UPPER(COALESCE(REGEXP_SUBSTR(t.PROCESSLIST_INFO, '^[[:space:]]*[A-Za-z]+'), '')),
		       x.trx_rows_modified > 0
		FROM information_schema.innodb_trx x
		JOIN performance_schema.threads t ON t.PROCESSLIST_ID = x.trx_mysql_thread_id
		LEFT JOIN performance_schema.session_connect_attrs a
		       ON a.PROCESSLIST_ID = t.PROCESSLIST_ID AND a.ATTR_NAME = 'program_name'
		WHERE TIMESTAMPDIFF(SECOND, x.trx_started, NOW()) >= ?
		  AND t.PROCESSLIST_ID <> CONNECTION_ID()
		ORDER BY x.trx_started, t.PROCESSLIST_ID`

// needLongTrx is what the long-transaction and blocker reads need.
const needLongTrx = "PROCESS and SELECT on performance_schema"

// LongTransactions lists open InnoDB transactions older than olderThan.
// Read-only ones are included; HasXID is true for a transaction that has
// modified rows. PID is the processlist id. WaitEvent is "lock:LOCK WAIT"
// while the transaction waits for a row lock.
func (d *DB) LongTransactions(ctx context.Context, olderThan time.Duration) ([]clients.Transaction, error) {
	if olderThan < 0 {
		return nil, fmt.Errorf("mysql: long transactions: negative age %s", olderThan)
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	rows, err := d.query(ctx, longTransactionsSQL, int64(olderThan/time.Second))
	if err != nil {
		return nil, fail("list long transactions", needLongTrx, err)
	}
	defer rows.Close()
	var out []clients.Transaction
	for rows.Next() {
		var (
			t        clients.Transaction
			pid, age int64
			state    string
		)
		if err := rows.Scan(&pid, &t.Username, &t.ApplicationName, &t.State, &age, &state, &t.Query, &t.HasXID); err != nil {
			return nil, fmt.Errorf("mysql: scan long transaction: %w", err)
		}
		t.PID = int(pid)
		t.Age = time.Duration(age) * time.Second
		t.Query = strings.TrimSpace(t.Query)
		if state == "LOCK WAIT" {
			t.WaitEvent = "lock:" + state
		}
		t.ApplicationName = cutText(t.ApplicationName, 64)
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fail("list long transactions", needLongTrx, err)
	}
	return out, nil
}

// Row reads one row by primary key. table is "db.table". pk is bound as a
// string, and the server converts it to the column's type. Values become
// JSON-friendly: numbers and decimals as strings, times as RFC 3339, bytes
// as a string when they are valid UTF-8 and "<binary N bytes>" otherwise.
// The values are customer data and untrusted; the tool redacts and wraps
// them. The table and pkColumn are checked against the identifier rule, then
// resolved to the server's own names (resolveColumn): a missing table or
// column is clients.ErrNotFound and no row is read.
func (d *DB) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	db, name, err := splitTable(table)
	if err != nil {
		return nil, false, err
	}
	if !validIdent(pkColumn) {
		return nil, false, fmt.Errorf("mysql: invalid identifier %q (want %s)", cutText(pkColumn, 70), identRe)
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	db, name, pkColumn, err = d.resolveColumn(ctx, table, db, name, pkColumn)
	if err != nil {
		return nil, false, err
	}
	q := "SELECT * FROM " + quoteIdent(db) + "." + quoteIdent(name) + " WHERE " + quoteIdent(pkColumn) + " = ? LIMIT 1"
	rows, err := d.query(ctx, q, pk)
	if err != nil {
		return nil, false, fail("read row of "+table, "SELECT on "+table, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, false, fmt.Errorf("mysql: read row of %s: %w", table, err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, false, fail("read row of "+table, "SELECT on "+table, err)
		}
		return nil, false, nil
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, false, fmt.Errorf("mysql: decode row of %s: %w", table, err)
	}
	m := make(map[string]any, len(cols))
	for i, c := range cols {
		m[c] = toJSON(vals[i])
	}
	return m, true, nil
}

// resolveColumnSQL looks up a table and one of its columns in the data
// dictionary. Every name is a bound parameter. The = comparisons follow the
// server's own name rules (the dictionary collation that
// lower_case_table_names sets for schema and table, case-insensitive for
// columns), so a name resolves here exactly when the SELECT that follows
// would resolve it. The last column says whether the requested column
// matched; ordering by it lets one statement tell a missing table (no row)
// from a missing column (a row with 0).
const resolveColumnSQL = `
		SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.COLUMN_NAME = ?
		FROM information_schema.COLUMNS c
		WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
		ORDER BY 4 DESC, c.ORDINAL_POSITION
		LIMIT 1`

// resolveColumn returns the server's names for db.name and column, read from
// information_schema with bound parameters. Row builds its statement only
// from these names, never from the caller's text. They are validated again:
// a dictionary name outside the identifier rule is refused, not quoted.
// information_schema shows only what the read user holds a privilege on, so
// a missing table or column is reported as "missing, or not visible".
func (d *DB) resolveColumn(ctx context.Context, table, db, name, column string) (string, string, string, error) {
	var (
		sdb, sname, scol string
		matched          bool
	)
	err := d.queryRow(ctx, resolveColumnSQL, column, db, name).Scan(&sdb, &sname, &scol, &matched)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", "", "", fmt.Errorf("mysql: read row of %s: table missing, or not visible to the read user: %w", table, clients.ErrNotFound)
	case err != nil:
		return "", "", "", fail("look up columns of "+table, "SELECT on "+table, err)
	case !matched:
		return "", "", "", fmt.Errorf("mysql: read row of %s: column %q missing, or not visible to the read user: %w", table, column, clients.ErrNotFound)
	}
	if !validIdent(sdb) || !validIdent(sname) || !validIdent(scol) {
		return "", "", "", fmt.Errorf("mysql: read row of %s: the server names the table or column outside %s; refusing to query it", table, identRe)
	}
	return sdb, sname, scol, nil
}

// primaryKeySQL lists the primary key columns of one table, in key order.
// Every name is a bound parameter. LIMIT 2 is enough to tell a one-column key
// from a composite one.
const primaryKeySQL = `
		SELECT k.COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE k
		WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ? AND k.CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY k.ORDINAL_POSITION
		LIMIT 2`

// tableExistsSQL tells a table without a primary key from a missing one.
const tableExistsSQL = `
		SELECT 1 FROM information_schema.TABLES t
		WHERE t.TABLE_SCHEMA = ? AND t.TABLE_NAME = ?
		LIMIT 1`

// PrimaryKeyColumn returns the one primary key column of table ("db.table"),
// by the server's own name, so a trace reads the row by its real key and
// never by a guessed "id". A composite key, a table without a primary key, or
// a key column named outside the identifier rule is clients.ErrNotSupported:
// a lookup by one value would not be one row (#483). A missing or invisible
// table is clients.ErrNotFound. Read only; 10 s per call.
func (d *DB) PrimaryKeyColumn(ctx context.Context, table string) (string, error) {
	db, name, err := splitTable(table)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	rows, err := d.query(ctx, primaryKeySQL, db, name)
	if err != nil {
		return "", fail("look up the primary key of "+table, "SELECT on "+table, err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return "", fmt.Errorf("mysql: primary key of %s: %w", table, err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return "", fail("look up the primary key of "+table, "SELECT on "+table, err)
	}
	switch len(cols) {
	case 0:
		var one int
		err := d.queryRow(ctx, tableExistsSQL, db, name).Scan(&one)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return "", fmt.Errorf("mysql: primary key of %s: table missing, or not visible to the read user: %w", table, clients.ErrNotFound)
		case err != nil:
			return "", fail("look up the primary key of "+table, "SELECT on "+table, err)
		}
		return "", fmt.Errorf("mysql: %s has no primary key: a lookup by one column is not one row: %w", table, clients.ErrNotSupported)
	case 1:
	default:
		return "", fmt.Errorf("mysql: primary key of %s is composite: a lookup by %q alone is not one row: %w", table, cutText(cols[0], 70), clients.ErrNotSupported)
	}
	if !validIdent(cols[0]) {
		return "", fmt.Errorf("mysql: primary key of %s: the server names the key column outside %s; refusing to use it: %w", table, identRe, clients.ErrNotSupported)
	}
	return cols[0], nil
}

// toJSON converts a value database/sql decoded from the driver into a
// JSON-friendly one (the pgconv.ToJSON rules for MySQL's types).
func toJSON(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case []byte:
		if utf8.Valid(t) {
			return string(t)
		}
		return fmt.Sprintf("<binary %d bytes>", len(t))
	case sql.RawBytes:
		return toJSON([]byte(t))
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case uint32:
		return strconv.FormatUint(uint64(t), 10)
	case int16:
		return strconv.FormatInt(int64(t), 10)
	case uint16:
		return strconv.FormatUint(uint64(t), 10)
	case int8:
		return strconv.FormatInt(int64(t), 10)
	case uint8:
		return strconv.FormatUint(uint64(t), 10)
	case int:
		return strconv.Itoa(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case bool:
		return t
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", t)
	}
}
