//go:build integration

package pgactions

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// pg_add_publication_table against the dev stack's source Postgres: the
// read-only client for Validate and Preview, the writer (the dev
// SOURCE_DB_URL role, which may ALTER its own publications) for Execute.
// Connect is a fake: the connector config only names the publication. Every
// object is named it_pa_<random> and dropped at the end.
func TestAddPublicationTableIntegration(t *testing.T) {
	dsn := testenv.Require(t).SourceDBURL
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("source postgres unreachable: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	src, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatalf("read-only client: %v", err)
	}
	t.Cleanup(src.Close)
	w, err := postgres.NewWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	t.Cleanup(w.Close)

	p := testenv.Name("pa")
	pub, in, add, nopk := p+"_pub", p+"_in", p+"_add", p+"_nopk"
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		_, _ = admin.Exec(cctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{pub}.Sanitize())
		for _, tbl := range []string{in, add, nopk} {
			_, _ = admin.Exec(cctx, "DROP TABLE IF EXISTS "+pgx.Identifier{"public", tbl}.Sanitize())
		}
	})
	for _, q := range []string{
		"CREATE TABLE " + pgx.Identifier{"public", in}.Sanitize() + " (id int PRIMARY KEY)",
		"CREATE TABLE " + pgx.Identifier{"public", add}.Sanitize() + " (id int PRIMARY KEY)",
		"CREATE TABLE " + pgx.Identifier{"public", nopk}.Sanitize() + " (id int)",
		"CREATE PUBLICATION " + pgx.Identifier{pub}.Sanitize() + " FOR TABLE " + pgx.Identifier{"public", in}.Sanitize(),
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}

	conn := &addPubConnect{cfg: map[string]map[string]string{"it-pg": {
		"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "publication.name": pub,
	}}}
	pl := owners.Pipeline{ID: p, TopicPrefix: "it", Connectors: []string{"it-pg"},
		SourceTables: []string{"public." + in, "public." + add, "public." + nopk, "public." + p + "_missing"}}
	a := NewAddPublicationTable(AddPublicationTableDeps{Connect: conn, Source: src, Writer: w})
	sctx := tools.WithPipeline(ctx, pl)

	// Refusals, read-only.
	if err := a.Validate(ctx, addPubParams("it-pg", pub, "public."+in)); !errors.Is(err, clients.ErrAlreadyPublished) {
		t.Fatalf("published table: %v, want ErrAlreadyPublished", err)
	}
	if err := a.Validate(ctx, addPubParams("it-pg", pub, "public."+nopk)); err == nil || !strings.Contains(err.Error(), "no primary key") {
		t.Fatalf("no primary key: %v", err)
	}
	if err := a.Validate(ctx, addPubParams("it-pg", pub, "public."+p+"_missing")); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing table: %v, want ErrNotFound", err)
	}
	if err := a.Validate(ctx, addPubParams("it-pg", pub+"_x", "public."+add)); err == nil || !strings.Contains(err.Error(), "wrong publication") {
		t.Fatalf("wrong publication: %v", err)
	}

	raw := addPubParams("it-pg", pub, "public."+add)
	if err := a.Validate(ctx, raw); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := a.(actions.Scoped).CheckScope(pl, raw); err != nil {
		t.Fatalf("CheckScope: %v", err)
	}
	res, err := a.(actions.Preview).Preview(sctx, raw)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(res.Current, "has 1 tables: public."+in) || !strings.Contains(res.Expected, "2 tables after") {
		t.Fatalf("preview = %+v", res)
	}
	if _, err := a.Execute(ctx, raw); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("Execute without scope: %v, want ErrNoScope", err)
	}

	out, err := a.Execute(sctx, raw)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(out, "added public."+add) {
		t.Fatalf("result %q", out)
	}
	var n int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_publication_tables WHERE pubname = $1 AND schemaname = 'public' AND tablename = $2`, pub, add).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("%s is not in %s after Execute", add, pub)
	}
	// Idempotent: a second run is a no-op, and Validate now refuses.
	if out, err := a.Execute(sctx, raw); err != nil || !strings.Contains(out, "no change made") {
		t.Fatalf("second Execute: %q %v", out, err)
	}
	if err := a.Validate(ctx, raw); !errors.Is(err, clients.ErrAlreadyPublished) {
		t.Fatalf("Validate after: %v, want ErrAlreadyPublished", err)
	}
}
