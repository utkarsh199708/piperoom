//go:build integration

package postgres

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/config"
	"piperoom/internal/testenv"
)

// The pipeline-scoped reads (task T2) against a real Postgres with logical
// replication: PIPEROOM_SCOPE_PG_URL (a throwaway postgres:16 started with
// -c wal_level=logical), else the dev stack's SOURCE_DB_URL. The role must be
// allowed to create tables, publications and logical slots. Every object is
// named it_scope_<random> and dropped at the end.

func scopeURL() string {
	if u := os.Getenv("PIPEROOM_SCOPE_PG_URL"); u != "" {
		return u
	}
	return config.Load().SourceDBURL
}

// scopeFixture is two pipelines' objects: a table, a publication and a
// logical slot each. pubMine publishes both tables, so it has one member that
// is not the pipeline's.
type scopeFixture struct {
	tableMine, tableOther string // "public.<name>"
	pubMine, pubOther     string
	slotMine, slotOther   string
	role                  string
	admin                 *pgx.Conn
}

func newScopeFixture(t *testing.T) (*DB, scopeFixture) {
	t.Helper()
	url := scopeURL()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := New(ctx, url)
	if err != nil {
		t.Skipf("postgres not reachable at %s (set PIPEROOM_SCOPE_PG_URL): %v", hostPort(url), err)
	}
	t.Cleanup(db.Close)
	admin := writeConn(t, url)

	p := testenv.Name("scope")
	f := scopeFixture{
		tableMine: "public." + p + "_mine", tableOther: "public." + p + "_other",
		pubMine: p + "_pub_mine", pubOther: p + "_pub_other",
		slotMine: p + "_slot_mine", slotOther: p + "_slot_other",
		admin: admin,
	}
	if err := admin.QueryRow(ctx, "SELECT current_user").Scan(&f.role); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, s := range []string{f.slotMine, f.slotOther} {
			_, _ = admin.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", s)
		}
		for _, pub := range []string{f.pubMine, f.pubOther} {
			_, _ = admin.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{pub}.Sanitize())
		}
		for _, tbl := range []string{p + "_mine", p + "_other"} {
			_, _ = admin.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{"public", tbl}.Sanitize())
		}
	})
	mine, other := pgx.Identifier{"public", p + "_mine"}.Sanitize(), pgx.Identifier{"public", p + "_other"}.Sanitize()
	for _, q := range []string{
		"CREATE TABLE " + mine + " (id int PRIMARY KEY, v text)",
		"CREATE TABLE " + other + " (id int PRIMARY KEY, v text)",
		"CREATE PUBLICATION " + pgx.Identifier{f.pubMine}.Sanitize() + " FOR TABLE " + mine + ", " + other,
		"CREATE PUBLICATION " + pgx.Identifier{f.pubOther}.Sanitize() + " FOR TABLE " + other,
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	for _, s := range []string{f.slotMine, f.slotOther} {
		if _, err := admin.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", s); err != nil {
			t.Fatalf("create slot %s (needs wal_level=logical): %v", s, err)
		}
	}
	return db, f
}

func TestReplicationSlotsNamedIntegration(t *testing.T) {
	db, f := newScopeFixture(t)
	ctx := context.Background()
	all, err := db.ReplicationSlots(ctx)
	if err != nil {
		t.Fatal(err)
	}

	got, other, err := db.ReplicationSlotsNamed(ctx, []string{f.slotMine})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != f.slotMine || got[0].Plugin != "pgoutput" {
		t.Fatalf("named slots = %+v, want only %s", got, f.slotMine)
	}
	if other != len(all)-1 || other < 1 {
		t.Fatalf("other = %d, want %d (every slot but %s)", other, len(all)-1, f.slotMine)
	}

	// No names: no rows, every slot counted.
	got, other, err = db.ReplicationSlotsNamed(ctx, nil)
	if err != nil || len(got) != 0 || other != len(all) {
		t.Fatalf("nil names: %v slots, other %d, err %v; want none and %d", got, other, err, len(all))
	}
}

func TestPublicationSourceTablesIntegration(t *testing.T) {
	db, f := newScopeFixture(t)
	got, other, err := db.PublicationSourceTables(context.Background(), f.pubMine, f.role, []string{f.tableMine})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || "public."+got[0].Table != f.tableMine || !got[0].InPublication || !got[0].Selectable {
		t.Fatalf("entries = %+v, want only %s, in the publication and selectable", got, f.tableMine)
	}
	if other != 1 {
		t.Fatalf("other members = %d, want 1 (the other pipeline's table)", other)
	}
	got, other, err = db.PublicationSourceTables(context.Background(), f.pubOther, f.role, []string{f.tableMine})
	if err != nil || len(got) != 1 || got[0].InPublication || other != 1 {
		t.Fatalf("other publication: %+v, other %d, err %v; want mine not a member and 1 other", got, other, err)
	}
}

// Two sessions keep a transaction open: one wrote to the pipeline's table, the
// other to the other pipeline's table. Only the first is listed; the second is
// counted as a writer.
func TestLongTransactionsOnTablesIntegration(t *testing.T) {
	db, f := newScopeFixture(t)
	url := scopeURL()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	open := func(table string) int {
		c := writeConn(t, url)
		var pid int
		if err := c.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
			t.Fatal(err)
		}
		tx, err := c.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
		schema, name, _ := splitTable(table)
		if _, err := tx.Exec(ctx, "INSERT INTO "+pgx.Identifier{schema, name}.Sanitize()+" VALUES (1, 'x')"); err != nil {
			t.Fatal(err)
		}
		return pid
	}
	pidMine := open(f.tableMine)
	pidOther := open(f.tableOther)
	time.Sleep(200 * time.Millisecond) // both transactions are now older than the threshold below

	got, err := db.LongTransactionsOnTables(ctx, 50*time.Millisecond, []string{f.tableMine})
	if err != nil {
		t.Fatal(err)
	}
	var pids []int
	for _, tx := range got.Transactions {
		pids = append(pids, tx.PID)
		if tx.Query != "" && tx.Query != "INSERT" && tx.Query != "SELECT" {
			t.Fatalf("query text must be a verb at most: %q", tx.Query)
		}
	}
	if !slices.Contains(pids, pidMine) || slices.Contains(pids, pidOther) {
		t.Fatalf("listed pids = %v, want %d and not %d", pids, pidMine, pidOther)
	}
	if got.Other < 1 || got.OtherWriters < 1 {
		t.Fatalf("other = %d, writers = %d; want the other session counted as a writer", got.Other, got.OtherWriters)
	}

	all, err := db.LongTransactions(ctx, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < len(got.Transactions)+1 {
		t.Fatalf("unscoped list has %d, scoped %d + %d other", len(all), len(got.Transactions), got.Other)
	}
}
