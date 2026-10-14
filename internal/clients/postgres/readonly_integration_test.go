//go:build integration

package postgres

import (
	"context"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/testenv"
)

// singleConnDB opens the client with a one-connection pool, so the next query
// is guaranteed to run on the session an earlier query may have tampered with.
func singleConnDB(t *testing.T) *DB {
	t.Helper()
	cfg := testenv.Require(t)
	u, err := url.Parse(cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("bad source db url: %v", err)
	}
	q := u.Query()
	q.Set("pool_max_conns", "1")
	u.RawQuery = q.Encode()
	db, err := New(context.Background(), u.String())
	if err != nil {
		t.Fatalf("source postgres unreachable (%s): %v", hostPort(cfg.SourceDBURL), err)
	}
	t.Cleanup(db.Close)
	return db
}

func execAll(t *testing.T, stmts ...string) {
	t.Helper()
	conn := writeConn(t, testenv.Require(t).SourceDBURL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

func cleanupAll(t *testing.T, stmts ...string) {
	t.Helper()
	url := testenv.Require(t).SourceDBURL
	t.Cleanup(func() {
		conn := writeConn(t, url)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, s := range stmts {
			_, _ = conn.Exec(ctx, s)
		}
	})
}

// QA3: a view whose function calls set_config('default_transaction_read_only',
// 'off', false) used to leave the pooled session writable for every later
// query. Every read now runs in BEGIN READ ONLY ... ROLLBACK, which undoes the
// setting and blocks writes inside the call.
func TestReadOnlySurvivesSetConfigViewIntegration(t *testing.T) {
	db := singleConnDB(t)
	n := testenv.Name("fixpg_ro")
	cleanupAll(t,
		"DROP VIEW IF EXISTS public."+n+"_flip",
		"DROP VIEW IF EXISTS public."+n+"_write",
		"DROP VIEW IF EXISTS public."+n+"_local",
		"DROP FUNCTION IF EXISTS public."+n+"_flip()",
		"DROP FUNCTION IF EXISTS public."+n+"_write()",
		"DROP FUNCTION IF EXISTS public."+n+"_local()",
		"DROP TABLE IF EXISTS public."+n+"_t",
	)
	execAll(t,
		"CREATE TABLE public."+n+"_t (id int primary key)",
		`CREATE FUNCTION public.`+n+`_flip() RETURNS int LANGUAGE plpgsql AS $$
		 BEGIN PERFORM set_config('default_transaction_read_only', 'off', false); RETURN 1; END $$`,
		`CREATE FUNCTION public.`+n+`_write() RETURNS int LANGUAGE plpgsql AS $$
		 BEGIN INSERT INTO public.`+n+`_t VALUES (1); RETURN 1; END $$`,
		`CREATE FUNCTION public.`+n+`_local() RETURNS int LANGUAGE plpgsql AS $$
		 BEGIN PERFORM set_config('transaction_read_only', 'off', true); INSERT INTO public.`+n+`_t VALUES (2); RETURN 1; END $$`,
		"CREATE VIEW public."+n+"_flip AS SELECT 1 AS id, public."+n+"_flip() AS f",
		"CREATE VIEW public."+n+"_write AS SELECT 1 AS id, public."+n+"_write() AS f",
		"CREATE VIEW public."+n+"_local AS SELECT 1 AS id, public."+n+"_local() AS f",
	)

	ctx := context.Background()
	if _, _, err := db.Row(ctx, "public."+n+"_flip", "id", "1"); err != nil {
		t.Fatalf("Row on flip view: %v", err)
	}
	// Same (only) pooled connection: the session must still be read-only.
	var ro string
	if err := db.pool.QueryRow(ctx, "SHOW default_transaction_read_only").Scan(&ro); err != nil {
		t.Fatalf("show: %v", err)
	}
	if ro != "on" {
		t.Fatalf("default_transaction_read_only = %q after the set_config view, want on", ro)
	}

	// A function that writes fails inside the call, with or without trying to
	// flip the transaction's own mode first.
	for _, v := range []string{"_write", "_local"} {
		if _, _, err := db.Row(ctx, "public."+n+v, "id", "1"); err == nil {
			t.Fatalf("Row on %s view: want a read-only error, got nil", v)
		}
	}
	// Run the write view twice more after a flip: still refused.
	if _, _, err := db.Row(ctx, "public."+n+"_flip", "id", "1"); err != nil {
		t.Fatalf("Row on flip view: %v", err)
	}
	if _, _, err := db.Row(ctx, "public."+n+"_write", "id", "1"); err == nil {
		t.Fatal("write view after a flip: want a read-only error, got nil")
	}

	var count int
	conn := writeConn(t, testenv.Require(t).SourceDBURL)
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM public."+n+"_t").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("%d rows were written through the read-only client", count)
	}
}

// QA3 HIGH: a physical slot has plugin NULL, which made ReplicationSlots and
// ReplicationSlotsNamed fail to scan. A physical slot that never reserved WAL
// also has no restart_lsn.
func TestPhysicalSlotsIntegration(t *testing.T) {
	db := sourceDB(t)
	n := testenv.Name("fixpg_phys")
	unreserved, reserved := n+"_u", n+"_r"
	cleanupAll(t,
		"SELECT pg_drop_replication_slot('"+unreserved+"')",
		"SELECT pg_drop_replication_slot('"+reserved+"')",
	)
	execAll(t,
		"SELECT pg_create_physical_replication_slot('"+unreserved+"')",
		"SELECT pg_create_physical_replication_slot('"+reserved+"', true)",
	)

	ctx := context.Background()
	all, err := db.ReplicationSlots(ctx)
	if err != nil {
		t.Fatalf("ReplicationSlots with physical slots: %v", err)
	}
	named, other, err := db.ReplicationSlotsNamed(ctx, []string{unreserved, reserved})
	if err != nil {
		t.Fatalf("ReplicationSlotsNamed with physical slots: %v", err)
	}
	if len(named) != 2 || other != len(all)-2 {
		t.Fatalf("named = %+v other = %d, all = %d", named, other, len(all))
	}
	for _, s := range named {
		if s.Plugin != "" {
			t.Fatalf("physical slot %s plugin = %q, want \"\"", s.Name, s.Plugin)
		}
		switch s.Name {
		case unreserved:
			// Holds no WAL: no restart_lsn, no wal_status, 0 retained.
			if s.RestartLSN != "" || s.WALStatus != "" || s.RetainedWALBytes != 0 {
				t.Fatalf("unreserved physical slot = %+v", s)
			}
		case reserved:
			if s.RestartLSN == "" || s.WALStatus == "" || s.WALStatus == "lost" {
				t.Fatalf("reserved physical slot = %+v", s)
			}
		}
	}
}

// QA3: pg_recent_ddl missed DDL on a table's columns and indexes, and on a
// quoted mixed-case table.
func TestRecentDDLTableSubObjectsIntegration(t *testing.T) {
	db := sourceDB(t)
	lower := testenv.Name("fixpg_ddl")
	mixed := "It_" + strings.TrimPrefix(lower, "it_") + "_Mixed"
	other := lower + "_other"
	cleanupAll(t,
		"DROP TABLE IF EXISTS public."+lower,
		`DROP TABLE IF EXISTS public."`+mixed+`"`,
		"DROP TABLE IF EXISTS public."+other,
	)
	since := time.Now().Add(-time.Minute)
	execAll(t,
		"CREATE TABLE public."+lower+" (id int primary key, a int)",
		"CREATE TABLE public."+other+" (id int primary key, a int)",
		"ALTER TABLE public."+lower+" RENAME COLUMN a TO b",
		"CREATE INDEX "+lower+"_bidx ON public."+lower+" (b)",
		"CREATE TRIGGER "+lower+"_trg BEFORE UPDATE ON public."+lower+" FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger()",
		"ALTER TABLE public."+other+" RENAME COLUMN a TO c",
		`CREATE TABLE public."`+mixed+`" (id int)`,
		`ALTER TABLE public."`+mixed+`" ADD COLUMN "Note" text`,
	)

	ctx := context.Background()
	got, err := db.RecentDDLTable(ctx, since, "public."+lower, 100)
	if err != nil {
		t.Fatalf("RecentDDLTable: %v", err)
	}
	var ids []string
	for _, c := range got {
		ids = append(ids, c.ObjectIdentity)
	}
	for _, want := range []string{
		"public." + lower,                 // CREATE TABLE
		"public." + lower + ".b",          // RENAME COLUMN
		"public." + lower + "_bidx",       // CREATE INDEX
		"public." + lower + "_pkey",       // implicit PK index
		lower + "_trg on public." + lower, // CREATE TRIGGER
	} {
		if !slices.Contains(ids, want) {
			t.Fatalf("RecentDDLTable(%s) = %v, missing %q", lower, ids, want)
		}
	}
	for _, id := range ids {
		if strings.Contains(id, other) {
			t.Fatalf("RecentDDLTable(%s) returned DDL of another table: %v", lower, ids)
		}
	}

	got, err = db.RecentDDLTable(ctx, since, "public."+mixed, 100)
	if err != nil {
		t.Fatalf("RecentDDLTable mixed case: %v", err)
	}
	if len(got) < 2 {
		t.Fatalf("RecentDDLTable(%s) = %+v, want CREATE and ALTER", mixed, got)
	}
	for _, c := range got {
		if c.ObjectIdentity != `public."`+mixed+`"` {
			t.Fatalf("unexpected identity %q", c.ObjectIdentity)
		}
	}
}

// QA3 LOW: Row used SELECT * (whole TOASTed values) and rendered several types
// wrongly. Values must read like psql, and a huge value must not be fetched.
func TestRowTypesAndSizeCapIntegration(t *testing.T) {
	db := sourceDB(t)
	n := testenv.Name("fixpg_row")
	cleanupAll(t, "DROP TABLE IF EXISTS public."+n)
	execAll(t,
		`CREATE TABLE public.`+n+` (
			id bigint primary key,
			tstz timestamptz, ts timestamp, d date,
			j jsonb, b bytea, ip inet, net cidr, p point, tm time, iv interval,
			r int4range, bits bit(4), n numeric, big text, small text)`,
		`INSERT INTO public.`+n+` VALUES (1,
			'infinity', '2026-10-11 10:59:03.5', '-infinity',
			'{"v": 12345678901234567890.123}', '\x0102ff', '10.0.0.1', '10.0.0.0/8', '(1.5,2)',
			'12:34:56.5', '1 year 2 mons -3 days 04:05:06', '[1,5)', B'0101', 0e2,
			repeat('x', 100000), 'hello')`,
	)
	row, types, found, err := db.RowTyped(context.Background(), "public."+n, "id", "1")
	if err != nil || !found {
		t.Fatalf("RowTyped: found=%v err=%v", found, err)
	}
	want := map[string]any{
		"id":    "1",
		"tstz":  "infinity",
		"ts":    "2026-10-11T10:59:03.5",
		"d":     "-infinity",
		"j":     `{"v":12345678901234567890.123}`,
		"b":     `\x0102ff`,
		"ip":    "10.0.0.1",
		"net":   "10.0.0.0/8",
		"p":     "(1.5,2)",
		"tm":    "12:34:56.5",
		"iv":    "1 year 2 mons -3 days +04:05:06",
		"r":     "[1,5)",
		"bits":  "0101",
		"n":     "0",
		"big":   OmittedValue,
		"small": "hello",
	}
	for k, v := range want {
		if row[k] != v {
			t.Errorf("%s = %#v, want %#v", k, row[k], v)
		}
	}
	if len(row) != len(want) {
		t.Errorf("row has %d columns, want %d: %v", len(row), len(want), row)
	}
	if types["tstz"] != 1184 || types["small"] != 25 {
		t.Errorf("types = %v", types)
	}

	if _, _, err := db.Row(context.Background(), "public."+n+"_missing", "id", "1"); err == nil {
		t.Fatal("Row on a missing table: want an error")
	}
}
