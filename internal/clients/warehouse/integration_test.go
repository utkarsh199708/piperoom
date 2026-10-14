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

// QA4: a missing sink table or column is clients.ErrNotFound, not an outage.
func TestNotFoundIntegration(t *testing.T) {
	db := warehouseDB(t)
	conn := writeConn(t, testenv.Require(t).WarehouseDBURL)
	table := testenv.Name("whnf")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS analytics."+table)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE analytics."+table+" (id bigint primary key, ts timestamptz)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if _, err := db.Freshness(ctx, "analytics.it_no_such_table_x", "ts"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Freshness missing table err = %v, want ErrNotFound", err)
	}
	if _, err := db.Freshness(ctx, "analytics."+table, "no_such_col"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Freshness missing column err = %v, want ErrNotFound", err)
	}
	if _, _, err := db.Row(ctx, "analytics.it_no_such_table_x", "id", "1"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Row missing table err = %v, want ErrNotFound", err)
	}
}

// A date column is a day, not midnight: a row dated today is fresh, and the
// returned time never lies ahead of now.
func TestFreshnessDateColumnIntegration(t *testing.T) {
	db := warehouseDB(t)
	conn := writeConn(t, testenv.Require(t).WarehouseDBURL)
	table := testenv.Name("whdate")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS analytics."+table)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE analytics."+table+" (id bigint primary key, d date)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	today := time.Now().UTC().Truncate(24 * time.Hour)
	if _, err := conn.Exec(ctx, "INSERT INTO analytics."+table+" VALUES (1, $1), (2, $2)", today.AddDate(0, 0, -3), today); err != nil {
		t.Fatalf("insert: %v", err)
	}
	ts, err := db.Freshness(ctx, "analytics."+table, "d")
	if err != nil {
		t.Fatalf("Freshness: %v", err)
	}
	if ts.After(time.Now()) || time.Since(ts) > time.Minute {
		t.Fatalf("freshness of a row dated today = %v, want ~now", ts)
	}
	if _, err := conn.Exec(ctx, "DELETE FROM analytics."+table+" WHERE id = 2"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	ts, err = db.Freshness(ctx, "analytics."+table, "d")
	if err != nil {
		t.Fatalf("Freshness: %v", err)
	}
	if want := today.AddDate(0, 0, -2); !ts.Equal(want) {
		t.Fatalf("freshness of a row dated 3 days ago = %v, want end of that day %v", ts, want)
	}
}

// A row larger than maxRowBytes is refused, not loaded into memory whole.
func TestRowSizeCapIntegration(t *testing.T) {
	db := warehouseDB(t)
	conn := writeConn(t, testenv.Require(t).WarehouseDBURL)
	table := testenv.Name("whbig")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS analytics."+table)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE analytics."+table+" (id bigint primary key, blob text)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO analytics."+table+" VALUES (1, repeat('x', $1::int)), (2, 'small')", maxRowBytes+1); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, _, err := db.Row(ctx, "analytics."+table, "id", "1"); !errors.Is(err, ErrRowTooLarge) {
		t.Fatalf("Row of a huge row err = %v, want ErrRowTooLarge", err)
	}
	row, found, err := db.Row(ctx, "analytics."+table, "id", "2")
	if err != nil || !found || row["blob"] != "small" {
		t.Fatalf("small row = %v, %v, %v", row, found, err)
	}
	if _, found, err := db.Row(ctx, "analytics."+table, "id", "3"); err != nil || found {
		t.Fatalf("absent row found=%v err=%v", found, err)
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
