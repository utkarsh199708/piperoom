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
		SELECT slot_name, plugin, active,
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

	rows, err := d.pool.Query(ctx, slotColumns+`
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
	if err := d.pool.QueryRow(ctx, `
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
	// (backend_xid), so they hold the WAL horizon. They are counted in SQL and
	// none of their columns is read.
	Other        int
	OtherWriters int
}

// longTxnScope is the shared part of both LongTransactionsOnTables queries: the
// long transactions ($1 seconds) of every database but this backend, with
// on_tables true for a session of this database that holds or waits for a
// relation lock on one of the tables ($2 schemas, $3 names, matched pairwise).
// pg_locks is used because a transaction's open locks are what ties it to a
// table: an UPDATE holds RowExclusiveLock and a read AccessShareLock until the
// transaction ends.
const longTxnScope = `
	WITH rel AS (
		SELECT c.oid
		FROM unnest($2::text[], $3::text[]) AS v(nsp, relname)
		JOIN pg_namespace n ON n.nspname = v.nsp
		JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = v.relname
	), lt AS (
		SELECT a.*,
		       (a.datname = current_database() AND EXISTS (
		           SELECT 1 FROM pg_locks l
		           WHERE l.pid = a.pid
		             AND l.locktype = 'relation'
		             AND l.database = a.datid
		             AND l.relation IN (SELECT oid FROM rel))) AS on_tables
		FROM pg_stat_activity a
		WHERE a.xact_start IS NOT NULL
		  AND a.xact_start < now() - make_interval(secs => $1)
		  AND a.pid <> pg_backend_pid()
	)`

// LongTransactionsOnTables is LongTransactions limited to the sessions of this
// database that hold or wait for a lock on one of tables (each
// "schema.table"). Every other long transaction is only counted (Other,
// OtherWriters). Like LongTransactions it never selects query text, only the
// statement verb.
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

	rows, err := d.pool.Query(ctx, longTxnScope+`
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

	if err := d.pool.QueryRow(ctx, longTxnScope+`
	SELECT count(*), count(*) FILTER (WHERE backend_xid IS NOT NULL)
	FROM lt
	WHERE NOT on_tables`, secs, schemas, names).Scan(&out.Other, &out.OtherWriters); err != nil {
		return TableTransactions{}, fmt.Errorf("postgres: count other long transactions: %w", err)
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
	if err := d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, publication).Scan(&exists); err != nil {
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
	var other int
	if err := d.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_publication_tables pt
		WHERE pt.pubname = $1
		  AND NOT EXISTS (
		      SELECT 1 FROM unnest($2::text[], $3::text[]) AS v(nsp, relname)
		      WHERE v.nsp = pt.schemaname AND v.relname = pt.tablename)`, publication, schemas, names).Scan(&other); err != nil {
		return nil, 0, fmt.Errorf("postgres: count other publication %q tables: %w", publication, err)
	}
	return sources, other, nil
}
