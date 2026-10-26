//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The live failure of 2026-10-25: a writer younger than the long-transaction
// threshold holds every logical slot's restart_lsn. LongTransactionsOnTables
// with a 5-minute threshold cannot see it; OpenTransactions counts it and,
// when it is the server's oldest writer, names it.
func TestOpenTransactionsSeesYoungWriterIntegration(t *testing.T) {
	db, f := newScopeFixture(t)
	url := scopeURL()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	ident := func(table string) string {
		schema, name, _ := splitTable(table)
		return pgx.Identifier{schema, name}.Sanitize()
	}
	// open starts a session named app that keeps a write transaction open.
	open := func(app, table string) int {
		c := writeConn(t, url)
		var pid int
		if err := c.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, "SELECT set_config('application_name', $1, false)", app); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, "BEGIN"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = c.Exec(context.Background(), "ROLLBACK") })
		if _, err := c.Exec(ctx, "INSERT INTO "+ident(table)+" VALUES (1, 'x')"); err != nil {
			t.Fatal(err)
		}
		return pid
	}
	const appOther, appMine = "it-open-other", "it-open-mine"
	pidOther := open(appOther, f.tableOther) // older
	time.Sleep(50 * time.Millisecond)
	pidMine := open(appMine, f.tableMine)

	long, err := db.LongTransactionsOnTables(ctx, 5*time.Minute, []string{f.tableMine})
	if err != nil {
		t.Fatal(err)
	}
	if len(long.Transactions) != 0 || long.OtherWriters != 0 {
		t.Logf("long transactions over 5m exist on this server: %+v", long)
	}

	got, err := db.OpenTransactions(ctx, []string{f.tableMine}, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Writers < 2 || got.Open < got.Writers {
		t.Fatalf("open = %d writers = %d, want both young writers counted", got.Open, got.Writers)
	}
	w := got.OldestWriter
	if w == nil {
		t.Fatalf("no oldest writer: %+v", got)
	}
	if !w.HasXID || w.AgeSeconds < 0 || w.XIDAge < 1 {
		t.Fatalf("oldest writer = %+v, want an xid and an age", w)
	}
	switch w.PID {
	case pidOther:
		// Outside the asked tables: no identity unless asked for.
		if w.OnTables || w.ApplicationName != "" || !w.SameDatabase {
			t.Fatalf("other-table writer = %+v, want on_tables false, no application name, same database", w)
		}
		all, err := db.OpenTransactions(ctx, []string{f.tableMine}, true)
		if err != nil {
			t.Fatal(err)
		}
		if all.OldestWriter == nil || all.OldestWriter.ApplicationName != appOther {
			t.Fatalf("identifyAll oldest writer = %+v, want application %q", all.OldestWriter, appOther)
		}
	case pidMine:
		t.Fatalf("pid %d (opened second) is reported older than pid %d", pidMine, pidOther)
	default:
		t.Logf("an older writer (pid %d) exists on this server; the identity checks need ours to be the oldest", w.PID)
	}

	// Asked for the other table, the same session is on the tables and named.
	mine, err := db.OpenTransactions(ctx, []string{f.tableOther}, false)
	if err != nil {
		t.Fatal(err)
	}
	if ow := mine.OldestWriter; ow != nil && ow.PID == pidOther && (!ow.OnTables || ow.ApplicationName != appOther) {
		t.Fatalf("writer on the asked table = %+v, want on_tables and application %q", ow, appOther)
	}
}
