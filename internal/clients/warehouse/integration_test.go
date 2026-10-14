//go:build integration

package warehouse

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients"
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

func warehouseDB(t *testing.T) *DB {
	t.Helper()
	cfg := testenv.Require(t)
	db, err := New(context.Background(), cfg.WarehouseDBURL)
	if err != nil {
		t.Fatalf("warehouse unreachable (%s): %v", hostPort(cfg.WarehouseDBURL), err)
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

func TestFreshnessIntegration(t *testing.T) {
	db := warehouseDB(t)
	conn := writeConn(t, testenv.Require(t).WarehouseDBURL)

	table := testenv.Name("wh")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS analytics."+table)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE analytics."+table+" (id bigint primary key, ts timestamptz)"); err != nil {
		t.Fatalf("create temp table: %v", err)
	}
	inserted := time.Now().Add(-5 * time.Minute)
	if _, err := conn.Exec(ctx, "INSERT INTO analytics."+table+" (id, ts) VALUES ($1, $2)", 1, inserted); err != nil {
		t.Fatalf("insert: %v", err)
	}

	ts, err := db.Freshness(context.Background(), "analytics."+table, "ts")
	if err != nil {
		t.Fatalf("Freshness: %v", err)
	}
	if ts.IsZero() {
		t.Fatal("expected a non-zero freshness timestamp")
	}
	if ts.Before(inserted.Add(-time.Second)) || ts.After(inserted.Add(time.Second)) {
		t.Fatalf("freshness = %v, want ~%v", ts, inserted)
	}
}

func TestRowIntegration(t *testing.T) {
	db := warehouseDB(t)
	conn := writeConn(t, testenv.Require(t).WarehouseDBURL)

	table := testenv.Name("whrow")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS analytics."+table)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE analytics."+table+" (id bigint primary key, n numeric(12,2), ts timestamptz)"); err != nil {
		t.Fatalf("create temp table: %v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO analytics."+table+" (id, n, ts) VALUES ($1, $2, now())", 7, 12.34); err != nil {
		t.Fatalf("insert: %v", err)
	}

	row, found, err := db.Row(context.Background(), "analytics."+table, "id", "7")
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	if !found {
		t.Fatal("expected row to be found")
	}
	if row["id"] != "7" {
		t.Fatalf("id = %v (%T), want \"7\"", row["id"], row["id"])
	}
	if row["n"] != "12.34" {
		t.Fatalf("n = %v (%T), want \"12.34\"", row["n"], row["n"])
	}
}

func TestRowAfterAlterIntegration(t *testing.T) {
	db := warehouseDB(t)
	conn := writeConn(t, testenv.Require(t).WarehouseDBURL)

	table := testenv.Name("whalter")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS analytics."+table)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE analytics."+table+" (id bigint primary key, amount numeric(10,2))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO analytics."+table+" (id, amount) VALUES ($1, $2)", 1, 50); err != nil {
		t.Fatalf("insert: %v", err)
	}

	row, found, err := db.Row(context.Background(), "analytics."+table, "id", "1")
	if err != nil {
		t.Fatalf("first Row: %v", err)
	}
	if !found || row["amount"] != "50.00" {
		t.Fatalf("first Row = %v (found=%v)", row, found)
	}

	if _, err := conn.Exec(ctx, "ALTER TABLE analytics."+table+" ALTER COLUMN amount TYPE numeric(12,2)"); err != nil {
		t.Fatalf("alter: %v", err)
	}
	if _, found, err = db.Row(context.Background(), "analytics."+table, "id", "1"); err != nil {
		t.Fatalf("Row after ALTER failed: %v", err)
	}
	if !found {
		t.Fatal("row not found after ALTER")
	}
}

func TestReadOnlyPoolIntegration(t *testing.T) {
	db := warehouseDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := db.pool.Exec(ctx, "CREATE TABLE analytics.should_not_exist (id int)")
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

// TestTableActivityIntegration is the Postgres warehouse's answer to
// warehouse_load_errors: no load history, but write counters of the table.
func TestTableActivityIntegration(t *testing.T) {
	db := warehouseDB(t)
	conn := writeConn(t, testenv.Require(t).WarehouseDBURL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	table := testenv.Name("act")
	if _, err := conn.Exec(ctx, "CREATE TABLE analytics."+table+" (id int)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(c, "DROP TABLE IF EXISTS analytics."+table)
	})
	if _, err := conn.Exec(ctx, "INSERT INTO analytics."+table+" SELECT g FROM generate_series(1, 5) g"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if _, err := db.LoadErrors(ctx, "analytics."+table, time.Now().Add(-time.Hour), 5); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("LoadErrors err = %v, want ErrNotSupported", err)
	}
	// Statistics are reported asynchronously: poll briefly for the inserts.
	var a TableActivity
	for i := 0; i < 20; i++ {
		var err error
		if a, err = db.TableActivity(ctx, "analytics."+table); err != nil {
			t.Fatalf("TableActivity: %v", err)
		}
		if a.Inserts >= 5 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if a.Inserts < 5 || a.LockWaiters != 0 {
		t.Errorf("activity = %+v, want >= 5 inserts and no lock waiters", a)
	}
	if _, err := db.TableActivity(ctx, "analytics.no_such_"+table); !errors.Is(err, clients.ErrNotFound) {
		t.Errorf("missing table err = %v, want ErrNotFound", err)
	}
}

// TestCountByBucketIntegration checks the date_bin query end to end: bucket
// anchor at since, counts only, empty buckets absent.
func TestCountByBucketIntegration(t *testing.T) {
	db := warehouseDB(t)
	conn := writeConn(t, testenv.Require(t).WarehouseDBURL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	table := testenv.Name("cnt")
	if _, err := conn.Exec(ctx, "CREATE TABLE analytics."+table+" (id int, updated_at timestamptz)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(c, "DROP TABLE IF EXISTS analytics."+table)
	})
	since := time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	// 3 rows in bucket 0, none in bucket 1, 2 in bucket 2 (5-minute buckets).
	for _, off := range []time.Duration{0, time.Minute, 4 * time.Minute, 11 * time.Minute, 14 * time.Minute} {
		if _, err := conn.Exec(ctx, "INSERT INTO analytics."+table+" VALUES (1, $1)", since.Add(off)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	got, err := db.CountByBucket(ctx, "analytics."+table, "updated_at", since, since.Add(15*time.Minute), 5*time.Minute)
	if err != nil {
		t.Fatalf("CountByBucket: %v", err)
	}
	if len(got) != 2 || got[0].Count != 3 || !got[0].Bucket.Equal(since) || got[1].Count != 2 || !got[1].Bucket.Equal(since.Add(10*time.Minute)) {
		t.Errorf("got %+v", got)
	}
}
