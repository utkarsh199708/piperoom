package postgres

import (
	"context"
	"fmt"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// Pipeline-scoped reads (task T2). Each one filters in SQL, with parameters, so
// the rows of objects outside the room's pipeline never reach the process:
// they are only counted. These methods are not on clients.SourceDB; the db
// tools find them with a type assertion, the same pattern as
// PublicationTables.

const slotColumns = `
		SELECT slot_name, COALESCE(plugin, ''), active,
		       COALESCE(restart_lsn::text, ''),
		       COALESCE(confirmed_flush_lsn::text, ''),
		       COALESCE(wal_status, ''),
		       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, 0),
		       safe_wal_size
		FROM pg_replication_slots`

// ReplicationSlotsNamed returns the replication slots whose name is in names
// (same columns as ReplicationSlots) and the number of other slots, which are
// counted in SQL and never read. No names returns no slots and counts them all.
func (d *DB) ReplicationSlotsNamed(ctx context.Context, names []string) ([]clients.ReplicationSlot, int, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()
	if names == nil {
		names = []string{} // a NULL array would make "<> ALL" NULL, and count nothing
	}

	rows, err := d.query(ctx, slotColumns+`
		WHERE slot_name = ANY($1::text[])
		ORDER BY slot_name`, names)
	if err != nil {
		return nil, 0, fmt.Errorf("postgres: list named replication slots: %w", err)
	}
	defer rows.Close()
	var out []clients.ReplicationSlot
	for rows.Next() {
		var s clients.ReplicationSlot
		if err := rows.Scan(&s.Name, &s.Plugin, &s.Active, &s.RestartLSN, &s.ConfirmedFlushLSN, &s.WALStatus, &s.RetainedWALBytes, &s.SafeWALBytes); err != nil {
			return nil, 0, fmt.Errorf("postgres: scan replication slot: %w", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("postgres: read named replication slots: %w", err)
	}

	var other int
	if err := d.queryRow(ctx, `
		SELECT count(*) FROM pg_replication_slots
		WHERE slot_name <> ALL($1::text[])`, names).Scan(&other); err != nil {
		return nil, 0, fmt.Errorf("postgres: count other replication slots: %w", err)
	}
	return out, other, nil
}

// TableTransactions is the result of LongTransactionsOnTables.
type TableTransactions struct {
	// Transactions are the long transactions of this database that hold or
	// wait for a lock on one of the tables, oldest first.
	Transactions []clients.Transaction
	// Other counts every other long transaction, in any database of the
	// server; OtherWriters counts those of them that hold a transaction id
	// (backend_xid).
	Other        int
	OtherWriters int
	// OldestOtherWriters are the oldest of those writers, at most
	// MaxOtherWriters, oldest first, anonymised (OtherWriter). A writer on any
	// table, in any database, holds back the restart_lsn of every logical
	// slot, so the room must see that one exists and how old it is.
	OldestOtherWriters []OtherWriter
}

// MaxOtherWriters caps TableTransactions.OldestOtherWriters.
const MaxOtherWriters = 5

// OtherWriter is a long writer outside the pipeline's tables, with only the
// fields that tell whether it pins a slot and for how long. Its user,
// application, client address, database name and query (even the verb) are
// never selected: they belong to another pipeline or team.
type OtherWriter struct {
	PID int `json:"pid"`
	// SameDatabase is datname = current_database(); the name itself is not read.
	SameDatabase bool   `json:"same_database"`
	State        string `json:"state"`
	// AgeSeconds is the transaction's age (now() - xact_start).
	AgeSeconds int64 `json:"age_seconds"`
	// XIDAge is age(backend_xid): how many transaction ids ago it got its xid.
	XIDAge int64 `json:"xid_age"`
	// WaitEventType is wait_event_type only, e.g. "Client"; not the wait event.
	WaitEventType string `json:"wait_event_type"`
}

// longTxnScope is the shared part of the LongTransactionsOnTables queries: the
// long transactions ($1 seconds) of every database but this backend, with
// on_tables true for a session of this database that holds or waits for a
// relation lock on one of the tables ($2 schemas, $3 names, matched pairwise).
// pg_locks is used because a transaction's open locks are what ties it to a
// table: an UPDATE holds RowExclusiveLock and a read AccessShareLock until the
// transaction ends.
//
// The datname check matters for a database cloned with CREATE DATABASE ...
// TEMPLATE: its tables keep the template's oids, so a lock on the clone's copy
// of a pipeline table matches the oid. on_tables is never NULL (a session with
// no datname, such as a background worker, is false), so NOT on_tables counts
// every other session.
const longTxnScope = `
	WITH rel AS (
		SELECT c.oid
		FROM unnest($2::text[], $3::text[]) AS v(nsp, relname)
		JOIN pg_namespace n ON n.nspname = v.nsp
		JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = v.relname
	), lt AS (
		SELECT a.*,
		       COALESCE(a.datname = current_database() AND EXISTS (
		           SELECT 1 FROM pg_locks l
		           WHERE l.pid = a.pid
		             AND l.locktype = 'relation'
		             AND l.database = a.datid
		             AND l.relation IN (SELECT oid FROM rel)), false) AS on_tables
		FROM pg_stat_activity a
		WHERE a.xact_start IS NOT NULL
		  AND a.xact_start < now() - make_interval(secs => $1)
		  AND a.pid <> pg_backend_pid()
	)`

// LongTransactionsOnTables is LongTransactions limited to the sessions of this
// database that hold or wait for a lock on one of tables (each
// "schema.table"). Every other long transaction is counted (Other,
// OtherWriters), and the oldest other writers are returned anonymised
// (OldestOtherWriters). Like LongTransactions it never selects query text,
// only the statement verb of the pipeline's own sessions.
func (d *DB) LongTransactionsOnTables(ctx context.Context, olderThan time.Duration, tables []string) (TableTransactions, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	schemas := make([]string, 0, len(tables))
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		schema, tbl, err := splitTable(t)
		if err != nil {
			return TableTransactions{}, err
		}
		schemas = append(schemas, schema)
		names = append(names, tbl)
	}
	secs := olderThan.Seconds()

	rows, err := d.query(ctx, longTxnScope+`
	SELECT pid,
	       COALESCE(usename, ''),
	       COALESCE(application_name, ''),
	       COALESCE(state, ''),
	       EXTRACT(EPOCH FROM (now() - xact_start))::bigint,
	       COALESCE(wait_event_type || ':' || wait_event, ''),
	       COALESCE(upper(substring(ltrim(query, E' \t\r\n') from '^[A-Za-z]+')), ''),
	       backend_xid IS NOT NULL
	FROM lt
	WHERE on_tables
	ORDER BY xact_start ASC`, secs, schemas, names)
	if err != nil {
		return TableTransactions{}, fmt.Errorf("postgres: list long transactions on tables: %w", err)
	}
	defer rows.Close()
	var out TableTransactions
	for rows.Next() {
		var t clients.Transaction
		var ageSec int64
		if err := rows.Scan(&t.PID, &t.Username, &t.ApplicationName, &t.State, &ageSec, &t.WaitEvent, &t.Query, &t.HasXID); err != nil {
			return TableTransactions{}, fmt.Errorf("postgres: scan transaction: %w", err)
		}
		t.Age = time.Duration(ageSec) * time.Second
		out.Transactions = append(out.Transactions, t)
	}
	if err := rows.Err(); err != nil {
		return TableTransactions{}, fmt.Errorf("postgres: read long transactions on tables: %w", err)
	}

	if err := d.queryRow(ctx, longTxnScope+`
	SELECT count(*), count(*) FILTER (WHERE backend_xid IS NOT NULL)
	FROM lt
	WHERE NOT on_tables`, secs, schemas, names).Scan(&out.Other, &out.OtherWriters); err != nil {
		return TableTransactions{}, fmt.Errorf("postgres: count other long transactions: %w", err)
	}

	// Only the allow-listed columns of the oldest other writers. Readers
	// (no xid) cannot hold a slot's restart_lsn back and stay a count.
	wrows, err := d.query(ctx, longTxnScope+`
	SELECT pid,
	       COALESCE(datname = current_database(), false),
	       COALESCE(state, ''),
	       EXTRACT(EPOCH FROM (now() - xact_start))::bigint,
	       age(backend_xid)::bigint,
	       COALESCE(wait_event_type, '')
	FROM lt
	WHERE NOT on_tables AND backend_xid IS NOT NULL
	ORDER BY xact_start ASC
	LIMIT $4`, secs, schemas, names, MaxOtherWriters)
	if err != nil {
		return TableTransactions{}, fmt.Errorf("postgres: list other long writers: %w", err)
	}
	defer wrows.Close()
	for wrows.Next() {
		var w OtherWriter
		if err := wrows.Scan(&w.PID, &w.SameDatabase, &w.State, &w.AgeSeconds, &w.XIDAge, &w.WaitEventType); err != nil {
			return TableTransactions{}, fmt.Errorf("postgres: scan other long writer: %w", err)
		}
		out.OldestOtherWriters = append(out.OldestOtherWriters, w)
	}
	if err := wrows.Err(); err != nil {
		return TableTransactions{}, fmt.Errorf("postgres: read other long writers: %w", err)
	}
	return out, nil
}

// PublicationSourceTables is PublicationTables limited to tables: one entry per
// table (in that order) with its membership in publication and role's SELECT
// privilege, and the number of the publication's other members, counted in
// SQL without reading their names. A missing publication is
// clients.ErrNotFound.
func (d *DB) PublicationSourceTables(ctx context.Context, publication, role string, tables []string) ([]PublicationTable, int, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	var exists bool
	if err := d.queryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, publication).Scan(&exists); err != nil {
		return nil, 0, fmt.Errorf("postgres: check publication %q: %w", publication, err)
	}
	if !exists {
		return nil, 0, fmt.Errorf("postgres: publication %q: %w", publication, clients.ErrNotFound)
	}
	sources, err := d.sourcePrivileges(ctx, publication, role, tables)
	if err != nil {
		return nil, 0, err
	}

	schemas := make([]string, 0, len(tables))
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		schema, tbl, err := splitTable(t)
		if err != nil {
			return nil, 0, err
		}
		schemas = append(schemas, schema)
		names = append(names, tbl)
	}
	// A partition (or partition root) of a pipeline table is that table, not
	// another member (#400).
	var other int
	if err := d.queryRow(ctx, `
		SELECT count(*)
		FROM pg_publication_tables pt
		JOIN pg_namespace pn ON pn.nspname = pt.schemaname
		JOIN pg_class pc ON pc.relnamespace = pn.oid AND pc.relname = pt.tablename
		WHERE pt.pubname = $1
		  AND NOT EXISTS (
		      SELECT 1 FROM unnest($2::text[], $3::text[]) AS v(nsp, relname)
		      JOIN pg_namespace vn ON vn.nspname = v.nsp
		      JOIN pg_class vc ON vc.relnamespace = vn.oid AND vc.relname = v.relname
		      WHERE vc.oid = pc.oid
		         OR vc.oid IN (SELECT relid FROM pg_partition_ancestors(pc.oid))
		         OR pc.oid IN (SELECT relid FROM pg_partition_ancestors(vc.oid)))`, publication, schemas, names).Scan(&other); err != nil {
		return nil, 0, fmt.Errorf("postgres: count other publication %q tables: %w", publication, err)
	}
	return sources, other, nil
}
