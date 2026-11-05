//go:build integration

package pgactions

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/actions"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/clients/warehouse"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// pg_resnapshot_rows against the dev stack's source Postgres and Postgres
// warehouse: the read-only clients for Validate, Preview and Verify, the
// writer for Execute. Connect is a fake naming an it_ signal table and
// publication, so the dev connector never sees the signal. The sink lacks
// 20 rows of a key range; the test fills them in by hand where the
// connector would, and Verify goes from 20 missing to 0. The full loop
// (the dev connector re-emitting the rows into the sink within 60 s) needs
// a dev connector with signalling on (an A1 dev-stack change) and is not
// covered here.
func TestResnapshotRowsIntegration(t *testing.T) {
	env := testenv.Require(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, env.SourceDBURL)
	if err != nil {
		t.Fatalf("source postgres unreachable: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	whAdmin, err := pgx.Connect(ctx, env.WarehouseDBURL)
	if err != nil {
		t.Fatalf("warehouse unreachable: %v", err)
	}
	t.Cleanup(func() { _ = whAdmin.Close(context.Background()) })
	src, err := postgres.New(ctx, env.SourceDBURL)
	if err != nil {
		t.Fatalf("read-only client: %v", err)
	}
	t.Cleanup(src.Close)
	wh, err := warehouse.New(ctx, env.WarehouseDBURL)
	if err != nil {
		t.Fatalf("warehouse client: %v", err)
	}
	t.Cleanup(wh.Close)
	w, err := postgres.NewWriter(ctx, env.SourceDBURL)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	t.Cleanup(w.Close)

	p := testenv.Name("rr")
	pub, sig, tbl := p+"_pub", p+"_signal", p+"_tbl"
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		_, _ = admin.Exec(cctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{pub}.Sanitize())
		for _, n := range []string{sig, tbl} {
			_, _ = admin.Exec(cctx, "DROP TABLE IF EXISTS "+pgx.Identifier{"public", n}.Sanitize())
		}
		_, _ = whAdmin.Exec(cctx, "DROP TABLE IF EXISTS "+pgx.Identifier{"analytics", tbl}.Sanitize())
	})
	src0 := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	srcT, sinkT := pgx.Identifier{"public", tbl}.Sanitize(), pgx.Identifier{"analytics", tbl}.Sanitize()
	for _, q := range []string{
		"CREATE TABLE " + pgx.Identifier{"public", sig}.Sanitize() + " (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048) NULL)",
		"CREATE TABLE " + srcT + " (id bigint PRIMARY KEY, updated_at timestamptz NOT NULL, note text)",
		"INSERT INTO " + srcT + " SELECT g, $1::timestamptz + g * interval '1 minute', 'n' FROM generate_series(1, 500) g",
		"CREATE PUBLICATION " + pgx.Identifier{pub}.Sanitize() + " FOR TABLE " + pgx.Identifier{"public", sig}.Sanitize() + ", " + srcT,
	} {
		var args []any
		if strings.Contains(q, "$1") {
			args = []any{src0}
		}
		if _, err := admin.Exec(ctx, q, args...); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	for _, q := range []string{
		"CREATE TABLE " + sinkT + " (id bigint PRIMARY KEY, updated_at timestamptz NOT NULL)",
		// The sink lost ids 101..120.
		"INSERT INTO " + sinkT + " SELECT g, $1::timestamptz + g * interval '1 minute' FROM generate_series(1, 500) g WHERE g NOT BETWEEN 101 AND 120",
	} {
		var args []any
		if strings.Contains(q, "$1") {
			args = []any{src0}
		}
		if _, err := whAdmin.Exec(ctx, q, args...); err != nil {
			t.Fatalf("sink setup %q: %v", q, err)
		}
	}

	conn := &addPubConnect{cfg: map[string]map[string]string{"it-pg": {
		"connector.class":         "io.debezium.connector.postgresql.PostgresConnector",
		"publication.name":        pub,
		"signal.enabled.channels": "source",
		"signal.data.collection":  "public." + sig,
	}}}
	pl := owners.Pipeline{ID: p, TopicPrefix: "it", Connectors: []string{"it-pg"},
		SourceTables: []string{"public." + tbl}, SinkTables: []string{"analytics." + tbl}}
	a := NewResnapshotRows(ResnapshotRowsDeps{Connect: conn, Source: src, Sink: wh, Writer: w})
	sctx := tools.WithPipeline(ctx, pl)
	keys := json.RawMessage(`{"connector":"it-pg","table":"public.` + tbl + `","key_range":{"column":"id","from":"101","to":"120"}}`)

	res, err := a.(actions.Preview).Preview(sctx, keys)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if res.Count == nil || *res.Count != 20 || !strings.Contains(res.Expected, "will re-emit 20 rows") {
		t.Fatalf("preview = %+v", res)
	}
	v := a.(actions.Verifier)
	if got, err := v.Verify(sctx, keys, time.Now()); err != nil || got.Before != 20 || got.After != 20 {
		t.Fatalf("Verify before = %+v, %v; want 20 / 20", got, err)
	}

	out, err := a.Execute(sctx, keys)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	var id, data string
	if err := admin.QueryRow(ctx, "SELECT id, data FROM "+pgx.Identifier{"public", sig}.Sanitize()).Scan(&id, &data); err != nil {
		t.Fatalf("signal row: %v", err)
	}
	if !strings.Contains(out, id) {
		t.Fatalf("result %q does not name signal id %s", out, id)
	}
	var got struct {
		Collections []string `json:"data-collections"`
		Conditions  []struct {
			Collection string `json:"data-collection"`
			Filter     string `json:"filter"`
		} `json:"additional-conditions"`
	}
	if err := json.Unmarshal([]byte(data), &got); err != nil {
		t.Fatalf("data %q: %v", data, err)
	}
	if len(got.Conditions) != 1 || got.Conditions[0].Collection != "public."+tbl || got.Conditions[0].Filter != `"id" >= '101' AND "id" <= '120'` {
		t.Fatalf("signal data = %s", data)
	}
	// The condition Debezium will append selects exactly the counted rows.
	var n int64
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+srcT+" WHERE "+got.Conditions[0].Filter).Scan(&n); err != nil || n != 20 {
		t.Fatalf("rows matching the filter = %d, %v", n, err)
	}

	// Where the connector would: the rows reach the sink.
	if _, err := whAdmin.Exec(ctx, "INSERT INTO "+sinkT+" SELECT g, $1::timestamptz + g * interval '1 minute' FROM generate_series(101, 120) g", src0); err != nil {
		t.Fatalf("fill sink: %v", err)
	}
	if got, err := v.Verify(sctx, keys, time.Now()); err != nil || got.Before != 20 || got.After != 0 {
		t.Fatalf("Verify after = %+v, %v; want 20 / 0", got, err)
	}

	// A time window: since 09:01 counts rows 1..500 updated in [at, executed).
	since := json.RawMessage(`{"connector":"it-pg","table":"public.` + tbl + `","since":{"column":"updated_at","at":"2026-10-22T09:01:00Z"}}`)
	if got, err := v.Verify(sctx, since, src0.Add(101*time.Minute)); err != nil || got.Before != 100 || got.After != 0 {
		t.Fatalf("Verify since = %+v, %v; want 100 / 0", got, err)
	}
	if err := a.Validate(sctx, json.RawMessage(`{"connector":"it-pg","table":"public.`+tbl+`","since":{"column":"note","at":"x"}}`)); err == nil {
		t.Fatal("a condition on a column that is neither the key nor a timestamp must be refused")
	}
}
