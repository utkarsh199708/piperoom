//go:build integration

package warehouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/testenv"
)

func warehouseDB(t *testing.T) *DB {
	t.Helper()
	cfg := testenv.Require(t)
	db, err := New(context.Background(), cfg.WarehouseDBURL)
	if err != nil {
		t.Fatalf("warehouse unreachable (%s): %v", cfg.WarehouseDBURL, err)
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
		t.Fatalf("cannot open write connection to %s: %v", url, err)
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
