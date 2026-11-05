package mysql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/tools"
)

// blockersSQL lists the sessions that hold a MySQL connector back (design
// §6.5): an InnoDB transaction open for at least ? seconds, or a session
// holding a granted metadata lock that another session waits for (the
// snapshot's FLUSH TABLES WITH READ LOCK or LOCK TABLES behind an open
// transaction or a LOCK TABLES). Background threads, binlog dump threads (a
// connector's or a replica's stream) and this session are never listed.
//
// The first two ? are the connector's user (design §6.5 part 2): when it is
// not "", a lock holder counts (blocks_slot) only when a waiter's
// PROCESSLIST_USER is that user. When it is "" (the caller does not know the
// connector), any waiter counts, but only for a session in scope: a session
// whose identity the scoped read withholds is never flagged on the say-so of
// some other application's waiting DDL. The last ? repeats the user for
// that rule.
//
// The fourth ? scopes the read: when true, a session is in scope only if it
// holds or waits for a metadata lock on one of the (schema, table) pairs
// that follow. The user, program_name and statement of an out-of-scope
// session are blanked in SQL and never leave the server (the Postgres
// rule); pid, times, state and blocks_slot remain. %s is the pairs list,
// rendered from the count only.
const blockersSQL = `
		WITH waited AS (
			SELECT DISTINCT h.OWNER_THREAD_ID AS tid
			FROM performance_schema.metadata_locks h
			JOIN performance_schema.metadata_locks w
			  ON w.OBJECT_TYPE = h.OBJECT_TYPE
			 AND w.OBJECT_SCHEMA <=> h.OBJECT_SCHEMA
			 AND w.OBJECT_NAME <=> h.OBJECT_NAME
			 AND w.LOCK_STATUS = 'PENDING'
			 AND w.OWNER_THREAD_ID <> h.OWNER_THREAD_ID
			JOIN performance_schema.threads wt ON wt.THREAD_ID = w.OWNER_THREAD_ID
			WHERE h.LOCK_STATUS = 'GRANTED'
			  AND (? = '' OR wt.PROCESSLIST_USER = ?)
		), s AS (
			SELECT t.PROCESSLIST_ID AS pid,
			       x.trx_started,
			       t.PROCESSLIST_USER AS usr,
			       a.ATTR_VALUE AS app,
			       TRIM(CONCAT(COALESCE(t.PROCESSLIST_COMMAND, ''), ' ', COALESCE(t.PROCESSLIST_STATE, ''))) AS st,
			       t.PROCESSLIST_INFO AS info,
			       (x.trx_started IS NOT NULL AND TIMESTAMPDIFF(SECOND, x.trx_started, NOW()) >= ?) AS old,
			       t.THREAD_ID IN (SELECT tid FROM waited) AS waited_on,
			       (NOT ? OR EXISTS (
			           SELECT 1 FROM performance_schema.metadata_locks m
			           WHERE m.OWNER_THREAD_ID = t.THREAD_ID
			             AND m.OBJECT_TYPE = 'TABLE'
			             AND %s)) AS in_scope,
			       COALESCE(t.PROCESSLIST_DB = DATABASE(), FALSE) AS same_db,
			       COALESCE(t.PROCESSLIST_USER NOT IN ('system user', 'event_scheduler'), FALSE) AS client
			FROM performance_schema.threads t
			LEFT JOIN information_schema.innodb_trx x ON x.trx_mysql_thread_id = t.PROCESSLIST_ID
			LEFT JOIN performance_schema.session_connect_attrs a
			       ON a.PROCESSLIST_ID = t.PROCESSLIST_ID AND a.ATTR_NAME = 'program_name'
			WHERE t.TYPE = 'FOREGROUND'
			  AND t.PROCESSLIST_ID IS NOT NULL
			  AND t.PROCESSLIST_ID <> CONNECTION_ID()
			  AND COALESCE(t.PROCESSLIST_COMMAND, '') NOT IN ('Binlog Dump', 'Binlog Dump GTID')
		), f AS (
			SELECT s.*, (waited_on AND (in_scope OR ? <> '')) AS blocks FROM s
		)
		SELECT pid,
		       COALESCE(CAST(UNIX_TIMESTAMP(trx_started) AS SIGNED), 0),
		       CASE WHEN in_scope THEN COALESCE(usr, '') ELSE '' END,
		       CASE WHEN in_scope THEN COALESCE(app, '') ELSE '' END,
		       st,
		       CASE WHEN in_scope THEN COALESCE(info, '') ELSE '' END,
		       blocks, in_scope, old, same_db, client
		FROM f
		WHERE old OR blocks
		ORDER BY trx_started IS NULL, trx_started, pid`

// Blockers lists the sessions that can hold the connector back, with full
// identity on every row (InScope is always true). The connector's user is
// not known here, so BlocksSlot means "holds a metadata lock another session
// waits for". Query is redacted (N5) and cut to MaxSessionQueryBytes.
func (d *DB) Blockers(ctx context.Context, olderThan time.Duration) ([]DBSession, error) {
	return d.blockers(ctx, olderThan, nil, nil, false, "")
}

// BlockersOnTables is Blockers limited to a pipeline's view: every blocker is
// still listed, but a session that holds or waits for no metadata lock on one
// of tables (each "db.table") has its user, program and query blanked. The
// connector's user is not known here: BlocksSlot means "another session waits
// for a metadata lock it holds" and is never set on such a blanked session
// (BlockersForConnector applies design §6.5's rule when the user is known).
func (d *DB) BlockersOnTables(ctx context.Context, olderThan time.Duration, tables []string) ([]DBSession, error) {
	dbs, names, err := splitTables(tables)
	if err != nil {
		return nil, err
	}
	return d.blockers(ctx, olderThan, dbs, names, true, "")
}

// BlockersForConnector is BlockersOnTables for a caller that knows the
// connector's database.user: BlocksSlot is then design §6.5's rule exactly,
// "holds a metadata lock that a session of connectorUser waits for", on
// every row, blanked or not. MySQL-only (not in the clients contract);
// source_end_session asserts it. connectorUser must not be "".
func (d *DB) BlockersForConnector(ctx context.Context, olderThan time.Duration, tables []string, connectorUser string) ([]DBSession, error) {
	if connectorUser == "" {
		return nil, fmt.Errorf("mysql: blockers: the connector's user is required: %w", tools.ErrBadInput)
	}
	dbs, names, err := splitTables(tables)
	if err != nil {
		return nil, err
	}
	return d.blockers(ctx, olderThan, dbs, names, true, connectorUser)
}

func (d *DB) blockers(ctx context.Context, olderThan time.Duration, dbs, names []string, scoped bool, connUser string) ([]DBSession, error) {
	if olderThan < 0 {
		return nil, fmt.Errorf("mysql: blockers: negative age %s", olderThan)
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	pred := "FALSE"
	if len(dbs) > 0 {
		pred = "(m.OBJECT_SCHEMA, m.OBJECT_NAME) IN (" + pairsIn(len(dbs)) + ")"
	}
	args := append([]any{connUser, connUser, int64(olderThan / time.Second), scoped}, pairArgs(dbs, names)...)
	args = append(args, connUser)
	rows, err := d.query(ctx, fmt.Sprintf(blockersSQL, pred), args...)
	if err != nil {
		return nil, fail("list blocking sessions", needLongTrx, err)
	}
	defer rows.Close()
	var out []DBSession
	for rows.Next() {
		var (
			s       DBSession
			started int64
		)
		if err := rows.Scan(&s.PID, &started, &s.User, &s.App, &s.State, &s.Query, &s.BlocksSlot, &s.InScope, &s.OldXact, &s.SameDB, &s.ClientBackend); err != nil {
			return nil, fmt.Errorf("mysql: scan blocking session: %w", err)
		}
		if started > 0 {
			s.StartedAt = time.Unix(started, 0).UTC()
			s.XactStart = s.StartedAt
		}
		s.Query = RedactSessionQuery(s.Query)
		s.App = cutText(s.App, 64)
		s.State = cutText(strings.TrimSpace(s.State), 128)
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fail("list blocking sessions", needLongTrx, err)
	}
	return out, nil
}
