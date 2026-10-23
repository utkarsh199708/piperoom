package mysql

import (
	"context"
	"database/sql"
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
// them.
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
