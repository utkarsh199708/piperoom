//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/testenv"
)

// The writer against the dev stack. It connects as the dev SOURCE_DB_URL
// role, which may create tables, publications and slots there (production
// gives the writer its own role, SOURCE_DB_WRITE_URL). Every object is named
// it_w_<random> and dropped at the end. Slots are server-wide and the T2 tests
// count them, so these tests hold the same advisory lock (scopeTestLock).

type writerFixture struct {
	w        *Writer
	admin    *pgx.Conn
	url      string
	pub      string
	tableIn  string // "public.<name>", in pub
	tableOut string // "public.<name>", not in pub
	signal   string // "public.<name>", a Debezium signal table
	slot     string // an inactive logical slot
}

func newWriterFixture(t *testing.T) writerFixture {
	t.Helper()
	dsn := testenv.Require(t).SourceDBURL
	lockScopeTests(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, err := NewWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("writer on the dev stack (%s): %v", hostPort(dsn), err)
	}
	t.Cleanup(w.Close)
	admin := writeConn(t, dsn)

	p := testenv.Name("w")
	f := writerFixture{
		w: w, admin: admin, url: dsn,
		pub:     p + "_pub",
		tableIn: "public." + p + "_in", tableOut: "public." + p + "_out",
		signal: "public." + p + "_signal",
		slot:   p + "_slot",
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", f.slot)
		_, _ = admin.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{f.pub}.Sanitize())
		for _, tbl := range []string{p + "_in", p + "_out", p + "_signal"} {
			_, _ = admin.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{"public", tbl}.Sanitize())
		}
	})
	in := pgx.Identifier{"public", p + "_in"}.Sanitize()
	for _, q := range []string{
		"CREATE TABLE " + in + " (id int PRIMARY KEY, v text)",
		"CREATE TABLE " + pgx.Identifier{"public", p + "_out"}.Sanitize() + " (id int PRIMARY KEY, v text)",
		// The signal table as the Debezium docs define it.
		"CREATE TABLE " + pgx.Identifier{"public", p + "_signal"}.Sanitize() + " (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048) NULL)",
		"CREATE PUBLICATION " + pgx.Identifier{f.pub}.Sanitize() + " FOR TABLE " + in,
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	if _, err := admin.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", f.slot); err != nil {
		t.Fatalf("create slot %s (needs wal_level=logical): %v", f.slot, err)
	}
	return f
}

func TestWriterAddPublicationTablesIntegration(t *testing.T) {
	f := newWriterFixture(t)
	ctx := context.Background()

	if err := f.w.AddPublicationTables(ctx, f.pub, []string{f.tableOut}); err != nil {
		t.Fatalf("AddPublicationTables: %v", err)
	}
	var n int
	_, out, _ := strings.Cut(f.tableOut, ".")
	if err := f.admin.QueryRow(ctx, `SELECT count(*) FROM pg_publication_tables WHERE pubname = $1 AND schemaname = 'public' AND tablename = $2`, f.pub, out).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("%s is not in %s after the ALTER", f.tableOut, f.pub)
	}

	for _, tbl := range []string{f.tableOut, f.tableIn} {
		if err := f.w.AddPublicationTables(ctx, f.pub, []string{tbl}); !errors.Is(err, ErrAlreadyPublished) {
			t.Fatalf("%s again: %v, want ErrAlreadyPublished", tbl, err)
		}
	}
	if err := f.w.AddPublicationTables(ctx, f.pub+"_missing", []string{f.tableOut}); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing publication: %v, want ErrNotFound", err)
	}
	if err := f.w.AddPublicationTables(ctx, f.pub, []string{f.tableOut + "_missing"}); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing table: %v, want ErrNotFound", err)
	}
}

func TestWriterDropReplicationSlotIntegration(t *testing.T) {
	f := newWriterFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A temporary slot is active for as long as the session that made it
	// lives: the shape of a slot a running connector holds.
	holder := writeConn(t, f.url)
	tmp := f.slot + "_tmp"
	if _, err := holder.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput', true)", tmp); err != nil {
		t.Fatalf("create temporary slot: %v", err)
	}
	var active bool
	if err := f.admin.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name = $1", tmp).Scan(&active); err != nil || !active {
		t.Fatalf("precondition: temporary slot active = %v, %v", active, err)
	}
	if err := f.w.DropReplicationSlot(ctx, tmp); !errors.Is(err, clients.ErrSlotActive) {
		t.Fatalf("active slot: %v, want ErrSlotActive", err)
	}

	if err := f.w.DropReplicationSlot(ctx, f.slot); err != nil {
		t.Fatalf("DropReplicationSlot: %v", err)
	}
	var n int
	if err := f.admin.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", f.slot).Scan(&n); err != nil || n != 0 {
		t.Fatalf("slot still there: count %d, %v", n, err)
	}
	if err := f.w.DropReplicationSlot(ctx, f.slot); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("dropped slot again: %v, want ErrNotFound", err)
	}
}

func TestWriterInsertSnapshotSignalIntegration(t *testing.T) {
	f := newWriterFixture(t)
	ctx := context.Background()

	id, err := f.w.InsertSnapshotSignal(ctx, f.signal, "pg-orders-connector", []string{f.tableIn})
	if err != nil {
		t.Fatalf("InsertSnapshotSignal: %v", err)
	}
	schema, table, _ := strings.Cut(f.signal, ".")
	var typ, data string
	if err := f.admin.QueryRow(ctx, "SELECT type, data FROM "+pgx.Identifier{schema, table}.Sanitize()+" WHERE id = $1", id).Scan(&typ, &data); err != nil {
		t.Fatalf("signal row %s: %v", id, err)
	}
	var got snapshotSignal
	if err := json.Unmarshal([]byte(data), &got); err != nil {
		t.Fatalf("data %q: %v", data, err)
	}
	want := snapshotSignal{DataCollections: []string{f.tableIn}, Type: "incremental"}
	if typ != "execute-snapshot" || !reflect.DeepEqual(got, want) {
		t.Fatalf("row = %q %+v, want execute-snapshot %+v", typ, got, want)
	}

	if _, err := f.w.InsertSnapshotSignal(ctx, f.signal+"_missing", "pg-orders-connector", []string{f.tableIn}); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing signal table: %v, want ErrNotFound", err)
	}
}

// A write URL whose role starts its sessions read-only (the documented
// read-only role) is refused when the writer starts, without the password in
// the error.
func TestNewWriterRefusesReadOnlyRoleIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	admin := writeConn(t, cfg.SourceDBURL)
	role := testenv.Name("ro_writer")
	const pw = "it-ro-writer-pw" // a throwaway role on the local dev stack
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, q := range []string{
		"CREATE ROLE " + pgx.Identifier{role}.Sanitize() + " LOGIN PASSWORD '" + pw + "'",
		"ALTER ROLE " + pgx.Identifier{role}.Sanitize() + " SET default_transaction_read_only = on",
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.Exec(ctx, "DROP ROLE IF EXISTS "+pgx.Identifier{role}.Sanitize())
	})

	u, err := url.Parse(cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("bad SOURCE_DB_URL: %v", err)
	}
	u.User = url.UserPassword(role, pw)
	w, err := NewWriter(ctx, u.String())
	if err == nil {
		w.Close()
		t.Fatal("a read-only role was accepted as the writer")
	}
	if !errors.Is(err, errReadOnlyRole) || strings.Contains(err.Error(), pw) {
		t.Fatalf("error = %v; want errReadOnlyRole without the password", err)
	}
}
