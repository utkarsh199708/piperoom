package postgres

import (
	"context"
	"fmt"
	"time"

	"piperoom/internal/clients/pgconv"
)

// OpenTransactions is the result of DB.OpenTransactions: every open
// transaction of the server, of any age. LongTransactions and
// LongTransactionsOnTables filter on age before they count, so a writer younger
// than their threshold was invisible there although it already holds back the
// restart_lsn of every logical slot (live test of 2026-10-25: a 3-minute writer
// against the 5-minute default read as "no long transactions").
type OpenTransactions struct {
	// Open counts every open transaction (xact_start set) in every database,
	// except this backend, walsenders and the reader pool's own read-only
	// sessions; Writers counts those that hold a transaction id (backend_xid).
	Open    int
	Writers int
	// Oldest is the oldest open transaction (nil when none is open);
	// OldestWriter the oldest one that holds an xid (nil when none does). Only
	// a writer holds the WAL horizon of a logical slot; both may be the same
	// session.
	Oldest       *OpenTransaction
	OldestWriter *OpenTransaction
	// Prepared counts two-phase transactions (pg_prepared_xacts) and
	// OldestPreparedAgeSeconds is the age of the oldest. A prepared transaction
	// holds the WAL horizon with no session in pg_stat_activity at all, so it
	// would otherwise be invisible. Their gid, owner and database are not read.
	Prepared                 int
	OldestPreparedAgeSeconds int64
}

// OpenTransaction is one open transaction with the allow-listed fields of
// OtherWriter plus the application name, which is selected only for a session
// that locks one of the asked tables (or for every session when the caller
// asked for identity, an unscoped call). User, client address, database name
// and query text are never selected.
type OpenTransaction struct {
	PID int
	// OnTables is true for a session of this database that holds or waits
	// for a relation lock on one of the asked tables.
	OnTables bool
	// SameDatabase is datname = current_database(); the name is not read.
	SameDatabase    bool
	ApplicationName string
	State           string
	AgeSeconds      int64
	HasXID          bool
	// XIDAge is age(backend_xid), 0 without an xid.
	XIDAge int64
	// WaitEventType is wait_event_type only, e.g. "Client".
	WaitEventType string
}

// openTxnScope lists the open transactions ($1 schemas, $2 names of the asked
// tables, matched pairwise; $3 the reader pool's application_name) with
// on_tables as in longTxnScope, but with no age filter.
//
// Excluded: this backend; walsenders (backend_type 'walsender'), which are
// the replication connections themselves; and sessions of the read pool
// (application_name $3) without an xid, which are this process's other tool
// reads, read-only and milliseconds old. A session that only names itself
// piperoom-reader cannot hide a write that way: with an xid it is counted.
const openTxnScope = `
	WITH rel AS (
		SELECT c.oid
		FROM unnest($1::text[], $2::text[]) AS v(nsp, relname)
		JOIN pg_namespace n ON n.nspname = v.nsp
		JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = v.relname
	), ot AS (
		SELECT a.pid, a.datname, a.application_name, a.state, a.xact_start,
		       a.backend_xid, a.wait_event_type,
		       COALESCE(a.datname = current_database() AND EXISTS (
		           SELECT 1 FROM pg_locks l
		           WHERE l.pid = a.pid
		             AND l.locktype = 'relation'
		             AND l.database = a.datid
		             AND l.relation IN (SELECT oid FROM rel)), false) AS on_tables
		FROM pg_stat_activity a
		WHERE a.xact_start IS NOT NULL
		  AND a.pid <> pg_backend_pid()
		  AND a.backend_type IS DISTINCT FROM 'walsender'
		  AND NOT (a.backend_xid IS NULL AND COALESCE(a.application_name, '') = $3)
	)`

// OpenTransactions counts every open transaction of the server regardless of
// age, and returns the oldest one and the oldest writer. tables ("schema.table")
// only decide OnTables; identifyAll selects the application name of every
// session (unscoped callers), otherwise only of sessions on tables.
func (d *DB) OpenTransactions(ctx context.Context, tables []string, identifyAll bool) (OpenTransactions, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	schemas := make([]string, 0, len(tables))
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		schema, tbl, err := splitTable(t)
		if err != nil {
			return OpenTransactions{}, err
		}
		schemas = append(schemas, schema)
		names = append(names, tbl)
	}

	var out OpenTransactions
	if err := d.queryRow(ctx, openTxnScope+`
	SELECT count(*),
	       count(*) FILTER (WHERE backend_xid IS NOT NULL),
	       (SELECT count(*) FROM pg_prepared_xacts),
	       COALESCE((SELECT EXTRACT(EPOCH FROM (now() - min(prepared)))::bigint FROM pg_prepared_xacts), 0)
	FROM ot`, schemas, names, readerAppName).Scan(&out.Open, &out.Writers, &out.Prepared, &out.OldestPreparedAgeSeconds); err != nil {
		return OpenTransactions{}, fmt.Errorf("postgres: count open transactions: %w", err)
	}

	rows, err := d.query(ctx, openTxnScope+`
	SELECT kind, pid, on_tables,
	       COALESCE(datname = current_database(), false),
	       CASE WHEN on_tables OR $4::bool THEN COALESCE(application_name, '') ELSE '' END,
	       COALESCE(state, ''),
	       EXTRACT(EPOCH FROM (now() - xact_start))::bigint,
	       backend_xid IS NOT NULL,
	       COALESCE(age(backend_xid), 0)::bigint,
	       COALESCE(wait_event_type, '')
	FROM (
		(SELECT 'oldest'::text AS kind, * FROM ot ORDER BY xact_start ASC, pid LIMIT 1)
		UNION ALL
		(SELECT 'writer'::text AS kind, * FROM ot WHERE backend_xid IS NOT NULL ORDER BY xact_start ASC, pid LIMIT 1)
	) s`, schemas, names, readerAppName, identifyAll)
	if err != nil {
		return OpenTransactions{}, fmt.Errorf("postgres: read oldest open transaction: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		var t OpenTransaction
		if err := rows.Scan(&kind, &t.PID, &t.OnTables, &t.SameDatabase, &t.ApplicationName, &t.State,
			&t.AgeSeconds, &t.HasXID, &t.XIDAge, &t.WaitEventType); err != nil {
			return OpenTransactions{}, fmt.Errorf("postgres: scan open transaction: %w", err)
		}
		switch kind {
		case "oldest":
			out.Oldest = &t
		case "writer":
			out.OldestWriter = &t
		}
	}
	if err := rows.Err(); err != nil {
		return OpenTransactions{}, fmt.Errorf("postgres: read oldest open transaction: %w", err)
	}
	return out, nil
}
