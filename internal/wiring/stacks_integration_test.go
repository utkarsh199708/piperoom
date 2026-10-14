//go:build integration

package wiring

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/config"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// TestStacksRealPostgresPerPipelineDSN opens two client sets with the real
// opener, each from its own environment variables naming its own database,
// and shows that each pipeline's pg_row reads its own database only.
//
// It needs only the source Postgres of the dev stack (SOURCE_DB_URL, with a
// role that may CREATE DATABASE); Kafka, Connect and the Schema Registry are
// not contacted when a set opens. It creates and drops its own databases
// (it_tenancy_<random>_a and _b).
func TestStacksRealPostgresPerPipelineDSN(t *testing.T) {
	admin := config.Load().SourceDBURL
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Skipf("source postgres not reachable at %s: %v", hostOf(admin), err)
	}
	// Registered first, so it runs after the DROP DATABASE clean-ups (LIFO).
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	base := testenv.Name("tenancy")
	dsn := map[string]string{}
	for i, side := range []string{"a", "b"} {
		db := base + "_" + side
		if _, err := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{db}.Sanitize()); err != nil {
			t.Fatalf("create database: %v", err)
		}
		t.Cleanup(func() {
			cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer ccancel()
			if _, err := conn.Exec(cctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{db}.Sanitize()+" WITH (FORCE)"); err != nil {
				t.Logf("drop database %s: %v", db, err)
			}
		})
		u, err := url.Parse(admin)
		if err != nil {
			t.Fatal(err)
		}
		u.Path = "/" + db
		dsn[side] = u.String()
		c, err := pgx.Connect(ctx, dsn[side])
		if err != nil {
			t.Fatalf("connect %s: %v", db, err)
		}
		// Database a holds row 1, database b holds row 2.
		if _, err := c.Exec(ctx, "CREATE TABLE public.shared (id int PRIMARY KEY, updated_at timestamptz NOT NULL DEFAULT now())"); err != nil {
			t.Fatalf("create table: %v", err)
		}
		if _, err := c.Exec(ctx, "INSERT INTO public.shared (id) VALUES ($1)", i+1); err != nil {
			t.Fatalf("insert: %v", err)
		}
		_ = c.Close(ctx)
	}

	for _, p := range []string{"A", "B"} {
		side := strings.ToLower(p)
		t.Setenv("IT_"+p+"_SRC", dsn[side])
		t.Setenv("IT_"+p+"_WH", dsn[side])
		t.Setenv("IT_"+p+"_K", "127.0.0.1:1")
		t.Setenv("IT_"+p+"_C", "http://127.0.0.1:1")
		t.Setenv("IT_"+p+"_SR", "http://127.0.0.1:1")
	}
	y := "tenants:\n  a: {}\n  b: {}\npipelines:\n"
	for _, p := range []string{"a", "b"} {
		P := strings.ToUpper(p)
		y += "  - {id: p" + p + ", tenant: " + p + ", topic_prefix: p" + p + ", source_tables: [public.shared], connectors: [c" + p + "],\n" +
			"     connections: {source_db_url: {env: IT_" + P + "_SRC}, warehouse_db_url: {env: IT_" + P + "_WH}, kafka_brokers: {env: IT_" + P +
			"_K}, connect_url: {env: IT_" + P + "_C}, schema_registry_url: {env: IT_" + P + "_SR}}}\n"
	}
	own := loadOwners(t, y)

	// The process-wide settings point nowhere: no pipeline may use them.
	st, err := OpenStacks(ctx, config.Config{SourceDBURL: "postgres://%zz"}, nil, own, StackOptions{})
	if err != nil {
		t.Fatalf("OpenStacks: %v", err)
	}
	defer st.Close()

	row := func(set, pipeline, key string) (string, error) {
		tl, ok := st.For(set).Tools.Get("pg_row")
		if !ok {
			t.Fatal("no pg_row")
		}
		in, _ := json.Marshal(map[string]string{"pipeline_id": pipeline, "table": "public.shared", "pk_column": "id", "key": key})
		rctx, rcancel := context.WithTimeout(tools.WithPipelineID(context.Background(), pipeline), 10*time.Second)
		defer rcancel()
		res, err := tl.Run(rctx, in)
		return res.Summary, err
	}
	for _, tc := range []struct {
		set, key string
		found    bool
	}{
		{"pa", "1", true}, {"pa", "2", false},
		{"pb", "2", true}, {"pb", "1", false},
	} {
		got, err := row(tc.set, tc.set, tc.key)
		if err != nil {
			t.Fatalf("%s pg_row %s: %v", tc.set, tc.key, err)
		}
		if strings.HasPrefix(got, "found: yes") != tc.found {
			t.Fatalf("%s pg_row %s = %q, want found=%v (each set reads its own database)", tc.set, tc.key, got, tc.found)
		}
	}
	// pa's tool cannot even name pb.
	if _, err := row("pa", "pb", "2"); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("pa's pg_row with pb: err = %v, want ErrOutOfScope", err)
	}
}

// hostOf returns host:port of a DSN, never the password (finding F5).
func hostOf(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return "(invalid dsn)"
	}
	return u.Host
}
