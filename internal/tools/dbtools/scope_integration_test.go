//go:build integration

package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients/postgres"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// Task T2 end to end: the slot and publication tools, on a real Postgres that
// holds two pipelines' publications and slots, show only the room pipeline's.
// It runs on PIPEROOM_SCOPE_PG_URL (a throwaway postgres:16 started with
// -c wal_level=logical), else on the dev stack's SOURCE_DB_URL; the role must
// be allowed to create tables, publications and logical slots. Every object
// is named it_scope_<random> and dropped at the end.

func scopePGURL() string {
	if u := os.Getenv("PIPEROOM_SCOPE_PG_URL"); u != "" {
		return u
	}
	return config.Load().SourceDBURL
}

// pgHost is the host:port of a DSN, so a failure never prints the password.
func pgHost(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return "(invalid dsn)"
	}
	return u.Host
}

func TestScopedSlotAndPublicationToolsIntegration(t *testing.T) {
	dsn := scopePGURL()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres not reachable at %s (set PIPEROOM_SCOPE_PG_URL): %v", pgHost(dsn), err)
	}
	t.Cleanup(db.Close)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("admin connection to %s: %v", pgHost(dsn), err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })

	p := testenv.Name("scope")
	tMine, tOther := p+"_mine", p+"_other"
	pubMine, pubOther := p+"_pub_mine", p+"_pub_other"
	slotMine, slotOther := p+"_slot_mine", p+"_slot_other"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, s := range []string{slotMine, slotOther} {
			_, _ = admin.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", s)
		}
		for _, pub := range []string{pubMine, pubOther} {
			_, _ = admin.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{pub}.Sanitize())
		}
		for _, tbl := range []string{tMine, tOther} {
			_, _ = admin.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{"public", tbl}.Sanitize())
		}
	})
	var role string
	if err := admin.QueryRow(ctx, "SELECT current_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	mine, other := pgx.Identifier{"public", tMine}.Sanitize(), pgx.Identifier{"public", tOther}.Sanitize()
	for _, q := range []string{
		"CREATE TABLE " + mine + " (id int PRIMARY KEY)",
		"CREATE TABLE " + other + " (id int PRIMARY KEY)",
		// The room's publication also carries the other pipeline's table.
		"CREATE PUBLICATION " + pgx.Identifier{pubMine}.Sanitize() + " FOR TABLE " + mine + ", " + other,
		"CREATE PUBLICATION " + pgx.Identifier{pubOther}.Sanitize() + " FOR TABLE " + other,
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	for _, s := range []string{slotMine, slotOther} {
		if _, err := admin.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", s); err != nil {
			t.Fatalf("create slot %s (needs wal_level=logical): %v", s, err)
		}
	}
	// Every slot name the server has that is not the room's: none may show.
	var foreign []string
	rows, err := admin.Query(ctx, "SELECT slot_name::text FROM pg_replication_slots WHERE slot_name <> $1", slotMine)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		foreign = append(foreign, n)
	}
	rows.Close()
	foreign = append(foreign, pubOther, tOther)

	o := &owners.Owners{Pipelines: []owners.Pipeline{
		{ID: "mine", TopicPrefix: "pg", SourceTables: []string{"public." + tMine}, Connectors: []string{"conn-mine"}},
		{ID: "other", TopicPrefix: "pg", SourceTables: []string{"public." + tOther}, Connectors: []string{"conn-other"}},
	}}
	pgSource := "io.debezium.connector.postgresql.PostgresConnector"
	connect := &fakeConnect{configs: map[string]map[string]string{
		"conn-mine":  {"connector.class": pgSource, "database.user": role, "slot.name": slotMine, "publication.name": pubMine},
		"conn-other": {"connector.class": pgSource, "database.user": role, "slot.name": slotOther, "publication.name": pubOther},
	}}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Src: db, Connect: connect, Owners: o}); err != nil {
		t.Fatal(err)
	}
	room := tools.WithPipelineID(context.Background(), "mine")
	run := func(name, in string, ctx context.Context) (tools.Result, error) {
		tl, ok := reg.Get(name)
		if !ok {
			t.Fatalf("%s not registered", name)
		}
		return tl.Run(ctx, json.RawMessage(in))
	}
	noForeign := func(t *testing.T, r tools.Result) {
		t.Helper()
		data, _ := json.Marshal(r.Data)
		for _, f := range foreign {
			if strings.Contains(r.Summary, f) || strings.Contains(string(data), f) {
				t.Fatalf("output names %q, which is not the room's:\nsummary: %s\ndata: %s", f, r.Summary, data)
			}
		}
	}

	t.Run("pg_replication_slots", func(t *testing.T) {
		r, err := run("pg_replication_slots", `{}`, room)
		if err != nil {
			t.Fatal(err)
		}
		noForeign(t, r)
		if !strings.Contains(r.Summary, "slot "+slotMine+": active=false") {
			t.Fatalf("summary misses the room's slot: %q", r.Summary)
		}
		if want := tools.NotShown(len(foreign)-2, "replication slot", "replication slots"); !strings.Contains(r.Summary, want) {
			t.Fatalf("summary misses %q: %q", want, r.Summary)
		}
		// The opt-out still lists every slot.
		r, err = run("pg_replication_slots", `{}`, tools.WithUnscoped(context.Background()))
		if err != nil || !strings.Contains(r.Summary, slotOther) {
			t.Fatalf("unscoped: %q, %v", r.Summary, err)
		}
	})

	t.Run("pg_publication_tables", func(t *testing.T) {
		r, err := run("pg_publication_tables", `{"pipeline_id":"mine"}`, room)
		if err != nil {
			t.Fatal(err)
		}
		noForeign(t, r)
		for _, want := range []string{
			"publication " + pubMine,
			"public." + tMine + ": in_publication=yes, selectable=yes",
			"1 other table in publication " + pubMine + " not shown",
		} {
			if !strings.Contains(r.Summary, want) {
				t.Fatalf("summary misses %q: %q", want, r.Summary)
			}
		}
	})

	t.Run("connector config unreadable fails closed", func(t *testing.T) {
		down := errors.New("connect: connection refused")
		reg := tools.NewRegistry()
		if err := Register(reg, Deps{Src: db, Connect: &fakeConnect{configErr: down}, Owners: o}); err != nil {
			t.Fatal(err)
		}
		for name, in := range map[string]string{"pg_replication_slots": `{}`, "pg_publication_tables": `{"pipeline_id":"mine"}`} {
			tl, _ := reg.Get(name)
			r, err := tl.Run(room, json.RawMessage(in))
			if !errors.Is(err, down) || r.Summary != "" || r.Data != nil {
				t.Fatalf("%s: r = %+v, err = %v; want the Connect error and nothing else", name, r, err)
			}
		}
	})
}

// shortTxnSource is the real client with the long-transaction threshold cut
// to 50 ms: the tool's own minimum is one minute, too slow for a test. The SQL
// and the tool's rendering are the real ones.
type shortTxnSource struct{ *postgres.DB }

func (s shortTxnSource) LongTransactionsOnTables(ctx context.Context, _ time.Duration, tables []string) (postgres.TableTransactions, error) {
	return s.DB.LongTransactionsOnTables(ctx, 50*time.Millisecond, tables)
}

// withDB returns dsn with another database and, when user is set, another
// login and application_name.
func withDB(t *testing.T, dsn, db, user, password, app string) string {
	t.Helper()
	u, err := url.Parse(dsn)
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

// An idle-in-transaction writer in another database holds back every logical
// slot's restart_lsn. The scoped pg_long_transactions lists it anonymised
// (pid, same_database=no, state, ages, wait type); its login, application_name
// and database (markers) appear nowhere in Summary or Data.
func TestScopedLongTransactionsOtherDatabaseIntegration(t *testing.T) {
	dsn := scopePGURL()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres not reachable at %s (set PIPEROOM_SCOPE_PG_URL): %v", pgHost(dsn), err)
	}
	t.Cleanup(db.Close)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("admin connection to %s: %v", pgHost(dsn), err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })

	p := testenv.Name("txdb")
	mine := p + "_mine"
	dbB, role := p+"_b", p+"_mkuser"
	const rolePW = "it-only-password"
	app := foreignMark + "-" + p
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbB}.Sanitize()+" WITH (FORCE)")
		_, _ = admin.Exec(ctx, "DROP ROLE IF EXISTS "+pgx.Identifier{role}.Sanitize())
		_, _ = admin.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{"public", mine}.Sanitize())
	})
	for _, q := range []string{
		"CREATE TABLE " + pgx.Identifier{"public", mine}.Sanitize() + " (id int PRIMARY KEY)",
		"CREATE DATABASE " + pgx.Identifier{dbB}.Sanitize(),
		"CREATE ROLE " + pgx.Identifier{role}.Sanitize() + " LOGIN PASSWORD '" + rolePW + "'",
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	setupB, err := pgx.Connect(ctx, withDB(t, dsn, dbB, "", "", ""))
	if err != nil {
		t.Fatalf("connect %s: %v", dbB, err)
	}
	for _, q := range []string{
		"CREATE TABLE public.billing (id int PRIMARY KEY)",
		"GRANT INSERT ON public.billing TO " + pgx.Identifier{role}.Sanitize(),
	} {
		if _, err := setupB.Exec(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	_ = setupB.Close(ctx)

	open := func(dsn, stmt string) int {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		var pid int
		if err := c.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{"BEGIN", stmt} {
			if _, err := c.Exec(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		return pid
	}
	pidMine := open(dsn, "INSERT INTO "+pgx.Identifier{"public", mine}.Sanitize()+" VALUES (1)")
	pidB := open(withDB(t, dsn, dbB, role, rolePW, app), "INSERT INTO public.billing VALUES (1)")
	time.Sleep(200 * time.Millisecond)

	o := &owners.Owners{Pipelines: []owners.Pipeline{
		{ID: "mine", TopicPrefix: "pg", SourceTables: []string{"public." + mine}, Connectors: []string{"conn-mine"}},
	}}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Src: shortTxnSource{db}, Owners: o}); err != nil {
		t.Fatal(err)
	}
	tl, _ := reg.Get("pg_long_transactions")
	r, err := tl.Run(tools.WithPipelineID(context.Background(), "mine"), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("scoped summary:\n%s", r.Summary)
	data, _ := json.Marshal(r.Data)
	for _, mark := range []string{role, app, dbB, "billing", foreignMark} {
		if strings.Contains(r.Summary, mark) || strings.Contains(string(data), mark) {
			t.Fatalf("output names %q of the other database's session:\nsummary: %s\ndata: %s", mark, r.Summary, data)
		}
	}
	if !strings.Contains(r.Summary, "pid="+strconv.Itoa(pidMine)+" user=") {
		t.Fatalf("the pipeline's own session %d is not listed: %s", pidMine, r.Summary)
	}
	if !strings.Contains(r.Summary, "  pid="+strconv.Itoa(pidB)+" same_database=no state=idle in transaction ") {
		t.Fatalf("the other database's writer %d is not listed anonymised: %s", pidB, r.Summary)
	}
}
