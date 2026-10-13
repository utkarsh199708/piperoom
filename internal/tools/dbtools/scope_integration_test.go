//go:build integration

package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
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
		if want := tools.NotShown(len(foreign)-2, "replication slots"); !strings.Contains(r.Summary, want) {
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
			"1 other tables in publication " + pubMine + " not shown",
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
