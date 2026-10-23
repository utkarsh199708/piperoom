package mysql

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// blockersSQL lists the sessions that hold a MySQL connector back (design
// §6.5): an InnoDB transaction open for at least ? seconds, or a session
// holding a granted metadata lock that another session waits for (the
// snapshot's FLUSH TABLES WITH READ LOCK or LOCK TABLES behind an open
// transaction or a LOCK TABLES). Background threads, binlog dump threads (a
// connector's or a replica's stream) and this session are never listed.
//
// The second ? scopes the read: when true, a session is in scope only if it
// holds or waits for a metadata lock on one of the (schema, table) pairs
// that follow. The user, program_name and statement of an out-of-scope
// session are blanked in SQL and never leave the server (the Postgres
// rule); pid, times, state and blocks_slot remain. %s is the pairs list,
// rendered from the count only.
//
// Note: the interface gives no connector user, so blocks_slot means "another
// session waits for a lock this one holds", not "the connector's session
// waits": a superset of the design's rule (see the PR).
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
			WHERE h.LOCK_STATUS = 'GRANTED'
		), s AS (
			SELECT t.PROCESSLIST_ID AS pid,
			       x.trx_started,
			       t.PROCESSLIST_USER AS usr,
			       a.ATTR_VALUE AS app,
			       TRIM(CONCAT(COALESCE(t.PROCESSLIST_COMMAND, ''), ' ', COALESCE(t.PROCESSLIST_STATE, ''))) AS st,
			       t.PROCESSLIST_INFO AS info,
			       (x.trx_started IS NOT NULL AND TIMESTAMPDIFF(SECOND, x.trx_started, NOW()) >= ?) AS old,
			       t.THREAD_ID IN (SELECT tid FROM waited) AS blocks,
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
		)
		SELECT pid,
		       COALESCE(CAST(UNIX_TIMESTAMP(trx_started) AS SIGNED), 0),
		       CASE WHEN in_scope THEN COALESCE(usr, '') ELSE '' END,
		       CASE WHEN in_scope THEN COALESCE(app, '') ELSE '' END,
		       st,
		       CASE WHEN in_scope THEN COALESCE(info, '') ELSE '' END,
		       blocks, in_scope, old, same_db, client
		FROM s
		WHERE old OR blocks
		ORDER BY trx_started IS NULL, trx_started, pid`

// Blockers lists the sessions that can hold the connector back, with full
// identity on every row (InScope is always true). Query is redacted (N5) and
// cut to MaxSessionQueryBytes.
func (d *DB) Blockers(ctx context.Context, olderThan time.Duration) ([]DBSession, error) {
	return d.blockers(ctx, olderThan, nil, nil, false)
}

// BlockersOnTables is Blockers limited to a pipeline's view: every blocker is
// still listed, but a session that holds or waits for no metadata lock on one
// of tables (each "db.table") has its user, program and query blanked.
func (d *DB) BlockersOnTables(ctx context.Context, olderThan time.Duration, tables []string) ([]DBSession, error) {
	dbs, names, err := splitTables(tables)
	if err != nil {
		return nil, err
	}
	return d.blockers(ctx, olderThan, dbs, names, true)
}

func (d *DB) blockers(ctx context.Context, olderThan time.Duration, dbs, names []string, scoped bool) ([]DBSession, error) {
	if olderThan < 0 {
		return nil, fmt.Errorf("mysql: blockers: negative age %s", olderThan)
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	pred := "FALSE"
	if len(dbs) > 0 {
		pred = "(m.OBJECT_SCHEMA, m.OBJECT_NAME) IN (" + pairsIn(len(dbs)) + ")"
	}
	args := append([]any{int64(olderThan / time.Second), scoped}, pairArgs(dbs, names)...)
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
