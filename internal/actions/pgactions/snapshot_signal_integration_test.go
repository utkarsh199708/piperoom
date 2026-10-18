//go:build integration

package pgactions

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
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

// pg_snapshot_signal against the dev stack's source Postgres: the read-only
// client for Validate and Preview, the writer (the dev SOURCE_DB_URL role,
// which owns the tables it creates) for Execute. Connect is a fake: the
// connector config only names the signal table and the publication. Every
// object is named it_ss_<random> and dropped at the end; the dev connector
// never sees the it_ publication, so no snapshot actually starts.
func TestSnapshotSignalIntegration(t *testing.T) {
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

	p := testenv.Name("ss")
	pub, sig, unpub, tbl := p+"_pub", p+"_signal", p+"_signal_unpub", p+"_tbl"
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		_, _ = admin.Exec(cctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{pub}.Sanitize())
		for _, n := range []string{sig, unpub, tbl} {
			_, _ = admin.Exec(cctx, "DROP TABLE IF EXISTS "+pgx.Identifier{"public", n}.Sanitize())
		}
	})
	sigDDL := " (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048) NULL)"
	for _, q := range []string{
		"CREATE TABLE " + pgx.Identifier{"public", sig}.Sanitize() + sigDDL,
		"CREATE TABLE " + pgx.Identifier{"public", unpub}.Sanitize() + sigDDL,
		"CREATE TABLE " + pgx.Identifier{"public", tbl}.Sanitize() + " (id int PRIMARY KEY)",
		"INSERT INTO " + pgx.Identifier{"public", tbl}.Sanitize() + " SELECT generate_series(1, 500)",
		"ANALYZE " + pgx.Identifier{"public", tbl}.Sanitize(),
		"CREATE PUBLICATION " + pgx.Identifier{pub}.Sanitize() + " FOR TABLE " +
			pgx.Identifier{"public", sig}.Sanitize() + ", " + pgx.Identifier{"public", tbl}.Sanitize(),
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}

	cfg := func(signal string) map[string]string {
		return map[string]string{
			"connector.class":         "io.debezium.connector.postgresql.PostgresConnector",
			"publication.name":        pub,
			"signal.enabled.channels": "source",
			"signal.data.collection":  "public." + signal,
		}
	}
	conn := &addPubConnect{cfg: map[string]map[string]string{"it-pg": cfg(sig), "it-pg-unpub": cfg(unpub)}}
	pl := owners.Pipeline{ID: p, TopicPrefix: "it", Connectors: []string{"it-pg", "it-pg-unpub"},
		SourceTables: []string{"public." + tbl, "public." + p + "_missing"}}
	a := NewSnapshotSignal(SnapshotSignalDeps{Connect: conn, Source: src, Writer: w})
	sctx := tools.WithPipeline(ctx, pl)

	// Refusals, read-only.
	if err := a.Validate(ctx, snapSigParams("it-pg-unpub", "public."+tbl)); !errors.Is(err, ErrSignalTableNotConfigured) || !strings.Contains(err.Error(), "not in publication") {
		t.Fatalf("unpublished signal table: %v", err)
	}
	if err := a.Validate(ctx, snapSigParams("it-pg", "public."+p+"_missing")); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing table: %v, want ErrNotFound", err)
	}

	raw := snapSigParams("it-pg", "public."+tbl)
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
	if !strings.Contains(res.Current, "is in publication "+pub) || !strings.Contains(res.Current, "public."+tbl+" ~500 rows") ||
		!strings.Contains(res.Expected, `"type":"INCREMENTAL"`) {
		t.Fatalf("preview = %+v", res)
	}
	if _, err := a.Execute(ctx, raw); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("Execute without scope: %v, want ErrNoScope", err)
	}

	out, err := a.Execute(sctx, raw)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	var id, typ, data string
	if err := admin.QueryRow(ctx, "SELECT id, type, data FROM "+pgx.Identifier{"public", sig}.Sanitize()).Scan(&id, &typ, &data); err != nil {
		t.Fatalf("signal row: %v", err)
	}
	if !strings.Contains(out, id) {
		t.Fatalf("result %q does not name signal id %s", out, id)
	}
	var got struct {
		DataCollections []string `json:"data-collections"`
		Type            string   `json:"type"`
	}
	if err := json.Unmarshal([]byte(data), &got); err != nil {
		t.Fatalf("data %q: %v", data, err)
	}
	if typ != "execute-snapshot" || got.Type != "INCREMENTAL" || !reflect.DeepEqual(got.DataCollections, []string{"public." + tbl}) {
		t.Fatalf("row = %q %q, want execute-snapshot of public.%s, INCREMENTAL", typ, data, tbl)
	}
}
