//go:build integration

package postgres

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/testenv"
)

// hostPort returns only the host:port of a DSN so a failing test never prints
// the embedded password (finding F5).
func hostPort(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return "(invalid dsn)"
	}
	return u.Host
}

func sourceDB(t *testing.T) *DB {
	t.Helper()
	cfg := testenv.Require(t)
	db, err := New(context.Background(), cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("source postgres unreachable (%s): %v", hostPort(cfg.SourceDBURL), err)
	}
	t.Cleanup(db.Close)
	return db
}

func writeConn(t *testing.T, url string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("cannot open write connection to %s: %v", hostPort(url), err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func TestReplicationSlotsIntegration(t *testing.T) {
	db := sourceDB(t)
	slots, err := db.ReplicationSlots(context.Background())
	if err != nil {
		t.Fatalf("ReplicationSlots: %v", err)
	}
	if len(slots) == 0 {
		t.Fatal("expected at least one replication slot")
	}
	// The orders connector's slot.name is "debezium_orders"
	// (dev/connectors/pg-orders-connector.json).
	found := false
	for _, s := range slots {
		if s.Name == "debezium_orders" {
			found = true
			if s.RetainedWALBytes < 0 {
				t.Fatalf("bad retained WAL bytes: %d", s.RetainedWALBytes)
			}
		}
	}
	if !found {
		t.Fatalf("slot debezium_orders not found; got %+v", slots)
	}
}

func TestRecentDDLSeesTempTable(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)

	table := testenv.Name("ddl")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE public."+table+" (id int primary key)"); err != nil {
		t.Fatalf("create temp table: %v", err)
	}
	if _, err := conn.Exec(ctx, "ALTER TABLE public."+table+" ADD COLUMN note text"); err != nil {
		t.Fatalf("alter temp table: %v", err)
	}

	since := time.Now().Add(-time.Minute)
	changes, err := db.RecentDDL(context.Background(), since, 100)
	if err != nil {
		t.Fatalf("RecentDDL: %v", err)
	}
	for _, c := range changes {
		if c.CommandTag == "ALTER TABLE" && strings.Contains(c.ObjectIdentity, table) {
			return // found the ALTER
		}
	}
	t.Fatalf("ALTER TABLE on %s not found in %+v", table, changes)
}

func TestRowIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)

	table := testenv.Name("row")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE public."+table+" (id bigint primary key, n numeric(12,2), ts timestamptz)"); err != nil {
		t.Fatalf("create temp table: %v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO public."+table+" (id, n, ts) VALUES ($1, $2, now())", 42, 12.34); err != nil {
		t.Fatalf("insert: %v", err)
	}

	row, found, err := db.Row(context.Background(), "public."+table, "id", "42")
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	if !found {
		t.Fatal("expected row to be found")
	}
	if row["id"] != "42" {
		t.Fatalf("id = %v (%T), want string \"42\"", row["id"], row["id"])
	}
	if row["n"] != "12.34" {
		t.Fatalf("n = %v (%T), want \"12.34\"", row["n"], row["n"])
	}
	ts, ok := row["ts"].(string)
	if !ok {
		t.Fatalf("ts = %v (%T), want RFC 3339 string", row["ts"], row["ts"])
	}
	if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
		t.Fatalf("ts %q not RFC 3339: %v", ts, err)
	}
}

func TestRowAfterAlterIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)

	table := testenv.Name("alter")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE public."+table+" (id bigint primary key, amount numeric(10,2))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO public."+table+" (id, amount) VALUES ($1, $2)", 1, 50); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Prime the SELECT * statement on the pooled connection.
	row, found, err := db.Row(context.Background(), "public."+table, "id", "1")
	if err != nil {
		t.Fatalf("first Row: %v", err)
	}
	if !found || row["amount"] != "50.00" {
		t.Fatalf("first Row = %v (found=%v)", row, found)
	}

	// A schema change must not break the next Row on the cached statement.
	if _, err := conn.Exec(ctx, "ALTER TABLE public."+table+" ALTER COLUMN amount TYPE numeric(12,2)"); err != nil {
		t.Fatalf("alter: %v", err)
	}
	if _, found, err = db.Row(context.Background(), "public."+table, "id", "1"); err != nil {
		t.Fatalf("Row after ALTER failed: %v", err)
	}
	if !found {
		t.Fatal("row not found after ALTER")
	}
}

func TestReadOnlyPoolIntegration(t *testing.T) {
	db := sourceDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := db.pool.Exec(ctx, "CREATE TABLE public.should_not_exist (id int)")
	if err == nil {
		t.Fatal("write on the read-only pool should fail")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("error is not a PgError: %v", err)
	}
	if pgErr.Code != "25006" {
		t.Fatalf("SQLSTATE = %s, want 25006 (read_only_sql_transaction)", pgErr.Code)
	}
}

func TestRowNonNumericKeyIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)

	table := testenv.Name("key")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE public."+table+" (id bigint primary key, n int)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO public."+table+" (id, n) VALUES ($1, $2)", 1, 5); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, _, err := db.Row(context.Background(), "public."+table, "id", "not-a-number")
	if err == nil {
		t.Fatal("expected an error for a non-numeric key on a bigint column")
	}
}

func TestPrimaryKeyColumnIntegration(t *testing.T) {
	db := sourceDB(t)
	col, err := db.PrimaryKeyColumn(context.Background(), "public.orders")
	if err != nil {
		t.Fatalf("PrimaryKeyColumn: %v", err)
	}
	if col != "id" {
		t.Fatalf("primary key = %q, want id", col)
	}
}

func TestLongTransactionsIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), "ROLLBACK") }()
	// txid_current() assigns a transaction id, so this backend now holds a
	// backend_xid like any writer; a bare SELECT would not.
	if _, err := conn.Exec(ctx, "SELECT txid_current()"); err != nil {
		t.Fatalf("SELECT txid_current: %v", err)
	}

	// olderThan=0 selects every open transaction that holds an xid.
	txns, err := db.LongTransactions(context.Background(), 0)
	if err != nil {
		t.Fatalf("LongTransactions: %v", err)
	}
	for _, tx := range txns {
		if tx.Username == "piperoom" && strings.Contains(tx.State, "transaction") {
			// The client returns the statement verb only, never the text.
			if tx.Query != "SELECT" {
				t.Fatalf("Query = %q, want only the statement verb SELECT", tx.Query)
			}
			if !tx.HasXID {
				t.Fatalf("returned transaction has HasXID=false: %+v", tx)
			}
			return
		}
	}
	t.Fatalf("did not find the open transaction in %+v", txns)
}

// A read-only transaction is listed with HasXID=false (it blocks DDL and
// VACUUM); a writer is listed with HasXID=true (it holds the WAL horizon).
func TestLongTransactionsHasXIDIntegration(t *testing.T) {
	db := sourceDB(t)
	readConn := writeConn(t, testenv.Require(t).SourceDBURL)
	writeC := writeConn(t, testenv.Require(t).SourceDBURL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pid := func(c *pgx.Conn) int {
		var p int
		if err := c.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&p); err != nil {
			t.Fatalf("pg_backend_pid: %v", err)
		}
		return p
	}
	for _, c := range []*pgx.Conn{readConn, writeC} {
		if _, err := c.Exec(ctx, "BEGIN"); err != nil {
			t.Fatalf("BEGIN: %v", err)
		}
		defer func(c *pgx.Conn) { _, _ = c.Exec(context.Background(), "ROLLBACK") }(c)
	}
	if _, err := readConn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if _, err := writeC.Exec(ctx, "SELECT txid_current()"); err != nil {
		t.Fatalf("SELECT txid_current: %v", err)
	}
	readPID, writePID := pid(readConn), pid(writeC)

	txns, err := db.LongTransactions(context.Background(), 0)
	if err != nil {
		t.Fatalf("LongTransactions: %v", err)
	}
	foundReader, foundWriter := false, false
	for _, tx := range txns {
		switch tx.PID {
		case readPID:
			foundReader = true
			if tx.HasXID {
				t.Fatalf("read-only transaction has HasXID=true: %+v", tx)
			}
		case writePID:
			foundWriter = true
			if !tx.HasXID {
				t.Fatalf("writer has HasXID=false: %+v", tx)
			}
		}
	}
	if !foundReader {
		t.Fatalf("read-only transaction (pid %d) not listed in %+v", readPID, txns)
	}
	if !foundWriter {
		t.Fatalf("writer transaction (pid %d) not listed in %+v", writePID, txns)
	}
}

// SafeWALBytes mirrors pg_replication_slots.safe_wal_size: nil when Postgres
// reports NULL (max_slot_wal_keep_size = -1), a number otherwise (it goes
// negative only after the slot is past its limit; a fresh slot is not).
func TestReplicationSlotsSafeWALIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)

	slot := testenv.Name("slot")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot); err != nil {
		t.Fatalf("create slot (needs wal_level=logical): %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(c, "SELECT pg_drop_replication_slot($1)", slot)
	})

	var wantNull bool
	if err := conn.QueryRow(ctx, "SELECT safe_wal_size IS NULL FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&wantNull); err != nil {
		t.Fatalf("read safe_wal_size: %v", err)
	}

	slots, err := db.ReplicationSlots(context.Background())
	if err != nil {
		t.Fatalf("ReplicationSlots: %v", err)
	}
	for _, s := range slots {
		if s.Name != slot {
			continue
		}
		if wantNull != (s.SafeWALBytes == nil) {
			t.Fatalf("SafeWALBytes nil = %v, but safe_wal_size IS NULL = %v", s.SafeWALBytes == nil, wantNull)
		}
		if s.SafeWALBytes != nil && *s.SafeWALBytes < 0 {
			t.Fatalf("SafeWALBytes = %d, want >= 0", *s.SafeWALBytes)
		}
		if s.SafeWALBytes == nil {
			t.Logf("slot %s: wal_status=%s safe_wal_bytes=NULL", slot, s.WALStatus)
		} else {
			t.Logf("slot %s: wal_status=%s safe_wal_bytes=%d", slot, s.WALStatus, *s.SafeWALBytes)
		}
		return
	}
	t.Fatalf("slot %s not in %+v", slot, slots)
}

// A physical slot (a streaming standby's) has plugin NULL. It must be listed
// with an empty plugin, not fail the whole read (QA sweep H2): a primary with
// a standby would otherwise break every slot tool, topology health and the
// watcher.
func TestReplicationSlotsPhysicalIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)

	slot := testenv.Name("phys")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "SELECT pg_create_physical_replication_slot($1, true)", slot); err != nil {
		t.Fatalf("create physical slot: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(c, "SELECT pg_drop_replication_slot($1)", slot)
	})

	slots, err := db.ReplicationSlots(ctx)
	if err != nil {
		t.Fatalf("ReplicationSlots: %v", err)
	}
	named, _, err := db.ReplicationSlotsNamed(ctx, []string{slot})
	if err != nil {
		t.Fatalf("ReplicationSlotsNamed: %v", err)
	}
	if len(named) != 1 || named[0].Name != slot || named[0].Plugin != "" {
		t.Fatalf("named = %+v, want the physical slot %s with no plugin", named, slot)
	}
	for _, s := range slots {
		if s.Name == slot {
			if s.Plugin != "" {
				t.Fatalf("physical slot plugin = %q, want empty", s.Plugin)
			}
			return
		}
	}
	t.Fatalf("physical slot %s not in %+v", slot, slots)
}
