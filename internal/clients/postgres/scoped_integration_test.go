//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/config"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
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

// scopeTestLock is the advisory lock key that serialises the T2 integration
// tests on one server. go test runs packages in parallel, and these tests
// count server-wide sessions and slots: another package's fixture must not
// appear in the middle of one. internal/tools/dbtools uses the same key.
const scopeTestLock = 74020001

// lockScopeTests holds scopeTestLock until the test ends. The holding session
// is idle outside a transaction, so it is no long transaction itself.
func lockScopeTests(t *testing.T, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Skipf("postgres not reachable at %s (set PIPEROOM_SCOPE_PG_URL): %v", hostPort(dsn), err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	if _, err := c.Exec(ctx, "SELECT pg_advisory_lock($1)", int64(scopeTestLock)); err != nil {
		t.Fatalf("scope test lock: %v", err)
	}
}

func newScopeFixture(t *testing.T) (*DB, scopeFixture) {
	t.Helper()
	url := scopeURL()
	lockScopeTests(t, url)
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

	// open starts a session that keeps a transaction open after stmt.
	open := func(begin, stmt string) int {
		c := writeConn(t, url)
		var pid int
		if err := c.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, begin); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = c.Exec(context.Background(), "ROLLBACK") })
		if _, err := c.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
		return pid
	}
	ident := func(table string) string {
		schema, name, _ := splitTable(table)
		return pgx.Identifier{schema, name}.Sanitize()
	}
	pidMine := open("BEGIN", "INSERT INTO "+ident(f.tableMine)+" VALUES (1, 'x')")
	pidOther := open("BEGIN", "INSERT INTO "+ident(f.tableOther)+" VALUES (1, 'x')")
	// A reader on the other table: a snapshot, but no xid.
	pidReader := open("BEGIN ISOLATION LEVEL REPEATABLE READ", "SELECT count(*) FROM "+ident(f.tableOther))
	time.Sleep(200 * time.Millisecond) // all three are now older than the threshold below

	got, err := db.LongTransactionsOnTables(ctx, 50*time.Millisecond, []string{f.tableMine})
	if err != nil {
		t.Fatal(err)
	}
	all, err := db.LongTransactions(ctx, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	var pids []int
	listedWriters := 0
	for _, tx := range got.Transactions {
		pids = append(pids, tx.PID)
		if tx.HasXID {
			listedWriters++
		}
		if tx.Query != "" && tx.Query != "INSERT" && tx.Query != "SELECT" {
			t.Fatalf("query text must be a verb at most: %q", tx.Query)
		}
	}
	if !slices.Contains(pids, pidMine) || slices.Contains(pids, pidOther) || slices.Contains(pids, pidReader) {
		t.Fatalf("listed pids = %v, want %d and neither %d nor %d", pids, pidMine, pidOther, pidReader)
	}
	allWriters := 0
	for _, tx := range all {
		if tx.HasXID {
			allWriters++
		}
	}
	// Exact counts: every long transaction is either listed or counted.
	if got.Other != len(all)-len(got.Transactions) {
		t.Fatalf("other = %d, want %d (all %d - listed %d)", got.Other, len(all)-len(got.Transactions), len(all), len(got.Transactions))
	}
	if got.OtherWriters != allWriters-listedWriters {
		t.Fatalf("other writers = %d, want %d (the reader has no xid)", got.OtherWriters, allWriters-listedWriters)
	}
	var writerPIDs []int
	for _, w := range got.OldestOtherWriters {
		writerPIDs = append(writerPIDs, w.PID)
		if w.PID == pidOther && (!w.SameDatabase || w.State != "idle in transaction" || w.XIDAge < 1 || w.WaitEventType != "Client") {
			t.Fatalf("other writer = %+v, want same database, idle in transaction, an xid age, wait type Client", w)
		}
	}
	if !slices.Contains(writerPIDs, pidOther) || slices.Contains(writerPIDs, pidReader) || slices.Contains(writerPIDs, pidMine) {
		t.Fatalf("other writer pids = %v, want %d and neither the reader %d nor the listed %d", writerPIDs, pidOther, pidReader, pidMine)
	}
}

// Six writers on the other pipeline's table, opened one after another: the
// other-writer list holds at most MaxOtherWriters of them, oldest first, so the
// newest of the six is never in it.
func TestLongTransactionsOnTablesOldestWritersIntegration(t *testing.T) {
	db, f := newScopeFixture(t)
	url := scopeURL()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema, name, _ := splitTable(f.tableOther)
	other := pgx.Identifier{schema, name}.Sanitize()

	var opened []int // oldest first
	for i := 1; i <= MaxOtherWriters+1; i++ {
		c := writeConn(t, url)
		var pid int
		if err := c.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, "BEGIN"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = c.Exec(context.Background(), "ROLLBACK") })
		// A distinct key per session, so no insert waits on another.
		if _, err := c.Exec(ctx, "INSERT INTO "+other+" VALUES ($1, 'x')", i); err != nil {
			t.Fatal(err)
		}
		opened = append(opened, pid)
		time.Sleep(30 * time.Millisecond) // distinct xact_start
	}
	time.Sleep(200 * time.Millisecond)

	got, err := db.LongTransactionsOnTables(ctx, 50*time.Millisecond, []string{f.tableMine})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.OldestOtherWriters) != MaxOtherWriters {
		t.Fatalf("other writers = %d, want %d (LIMIT): %+v", len(got.OldestOtherWriters), MaxOtherWriters, got.OldestOtherWriters)
	}
	if got.OtherWriters < MaxOtherWriters+1 {
		t.Fatalf("other writer count = %d, want at least %d", got.OtherWriters, MaxOtherWriters+1)
	}
	// Ours, in list order. Older writers of other sessions can only come
	// before them; ours must be a prefix of the order they were opened in.
	var ours []int
	for _, w := range got.OldestOtherWriters {
		if slices.Contains(opened, w.PID) {
			ours = append(ours, w.PID)
		}
	}
	if len(ours) == 0 || !slices.Equal(ours, opened[:len(ours)]) {
		t.Fatalf("listed writers %v are not the oldest of %v in order", ours, opened)
	}
	if newest := opened[len(opened)-1]; slices.Contains(ours, newest) {
		t.Fatalf("the newest writer %d is listed; want the %d oldest: %v", newest, MaxOtherWriters, ours)
	}
}

// dsnFor returns base with another database and, when user is set, another
// login and application_name.
func dsnFor(t *testing.T, base, db, user, password, app string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + db
	if user != "" {
		u.User = url.UserPassword(user, password)
	}
	if app != "" {
		q := u.Query()
		q.Set("application_name", app)
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// A database cloned with CREATE DATABASE ... TEMPLATE keeps the template's
// table oids. A writer in the clone, on the clone's copy of the pipeline
// table, matches the oid in pg_locks; the datname check keeps it out of the
// listed sessions. It is an other writer, anonymised: its login and
// application_name (markers) appear nowhere in the result.
func TestLongTransactionsOnTablesClonedDatabaseIntegration(t *testing.T) {
	base := scopeURL()
	lockScopeTests(t, base)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	probe, err := New(ctx, base)
	if err != nil {
		t.Skipf("postgres not reachable at %s (set PIPEROOM_SCOPE_PG_URL): %v", hostPort(base), err)
	}
	probe.Close()
	admin := writeConn(t, base)

	p := testenv.Name("clone")
	dbA, dbB, role := p+"_a", p+"_b", p+"_mkuser"
	const rolePW = "it-only-password"
	app := "FOREIGNMARK-" + p
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for _, d := range []string{dbB, dbA} {
			_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{d}.Sanitize()+" WITH (FORCE)")
		}
		_, _ = admin.Exec(ctx, "DROP ROLE IF EXISTS "+pgx.Identifier{role}.Sanitize())
	})
	for _, q := range []string{
		"CREATE DATABASE " + pgx.Identifier{dbA}.Sanitize(),
		"CREATE ROLE " + pgx.Identifier{role}.Sanitize() + " LOGIN PASSWORD '" + rolePW + "'",
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	setupA, err := pgx.Connect(ctx, dsnFor(t, base, dbA, "", "", ""))
	if err != nil {
		t.Fatalf("connect %s: %v", dbA, err)
	}
	for _, q := range []string{
		"CREATE TABLE public.t_mine (id int PRIMARY KEY, v text)",
		"GRANT SELECT, INSERT ON public.t_mine TO " + pgx.Identifier{role}.Sanitize(),
	} {
		if _, err := setupA.Exec(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	_ = setupA.Close(ctx) // a template must have no other session
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbB}.Sanitize()+" TEMPLATE "+pgx.Identifier{dbA}.Sanitize()); err != nil {
		t.Fatalf("clone: %v", err)
	}

	db, err := New(ctx, dsnFor(t, base, dbA, "", "", ""))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)

	open := func(dsn string) (*pgx.Conn, int) {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		var pid int
		if err := c.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, "BEGIN"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, "INSERT INTO public.t_mine VALUES (1, 'x')"); err != nil {
			t.Fatal(err)
		}
		return c, pid
	}
	connA, pidA := open(dsnFor(t, base, dbA, "", "", ""))
	connB, pidB := open(dsnFor(t, base, dbB, role, rolePW, app))

	// The premise: the clone's table has the template's oid.
	var oidA, oidB uint32
	if err := connA.QueryRow(ctx, "SELECT 'public.t_mine'::regclass::oid").Scan(&oidA); err != nil {
		t.Fatal(err)
	}
	if err := connB.QueryRow(ctx, "SELECT 'public.t_mine'::regclass::oid").Scan(&oidB); err != nil {
		t.Fatal(err)
	}
	if oidA != oidB {
		t.Fatalf("clone table oid %d != template oid %d: the test no longer exercises the datname check", oidB, oidA)
	}
	time.Sleep(200 * time.Millisecond)

	got, err := db.LongTransactionsOnTables(ctx, 50*time.Millisecond, []string{"public.t_mine"})
	if err != nil {
		t.Fatal(err)
	}
	var pids []int
	for _, tx := range got.Transactions {
		pids = append(pids, tx.PID)
	}
	if !slices.Contains(pids, pidA) || slices.Contains(pids, pidB) {
		t.Fatalf("listed pids = %v, want %d (this database) and not %d (the clone)", pids, pidA, pidB)
	}
	var found bool
	for _, w := range got.OldestOtherWriters {
		if w.PID == pidB {
			found = true
			if w.SameDatabase {
				t.Fatalf("clone writer = %+v, want same_database false", w)
			}
		}
	}
	if !found {
		t.Fatalf("the clone's writer %d is not among the other writers: %+v", pidB, got.OldestOtherWriters)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, mark := range []string{role, app, dbB} {
		if strings.Contains(string(raw), mark) {
			t.Fatalf("result names %q of the other session: %s", mark, raw)
		}
	}
}

func TestReplicationSlotsNamedRecordsProvenanceIntegration(t *testing.T) {
	db, f := newScopeFixture(t)
	rec := tools.NewRecorder()
	ctx := tools.WithRecorder(context.Background(), rec)
	if _, _, err := db.ReplicationSlotsNamed(ctx, []string{f.slotMine}); err != nil {
		t.Fatal(err)
	}
	calls := rec.Calls()
	if len(calls) == 0 {
		t.Fatal("no call recorded")
	}
	c := calls[0]
	if !strings.HasPrefix(c.Text, "SELECT slot_name, plugin, active") || len(c.ParamsRedacted) != 1 || c.ParamsRedacted[0] != "$1=["+f.slotMine+"]" {
		t.Fatalf("call = %+v", c)
	}
	if strings.Contains(c.Target, "@") && strings.Contains(c.Target, ":") && !strings.HasPrefix(c.Target, "postgres ") {
		t.Fatalf("target = %q", c.Target)
	}
}
