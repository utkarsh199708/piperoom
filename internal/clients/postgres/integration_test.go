//go:build integration

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/testenv"
)

func sourceDB(t *testing.T) *DB {
	t.Helper()
	cfg := testenv.Require(t)
	db, err := New(context.Background(), cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("source postgres unreachable (%s): %v", cfg.SourceDBURL, err)
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
	if _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	// olderThan=0 selects every open transaction.
	txns, err := db.LongTransactions(context.Background(), 0)
	if err != nil {
		t.Fatalf("LongTransactions: %v", err)
	}
	for _, tx := range txns {
		if tx.Username == "piperoom" && strings.Contains(tx.State, "transaction") {
			return
		}
	}
	t.Fatalf("did not find the open transaction in %+v", txns)
}
