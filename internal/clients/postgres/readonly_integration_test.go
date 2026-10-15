//go:build integration

package postgres

import (
	"context"
	"net/url"
	"testing"
	"time"

	"piperoom/internal/testenv"
)

// selectOnlyDB connects as a new role that has only SELECT on table, the
// pilot role of docs/pilot/permissions.md. The role is dropped at cleanup.
func selectOnlyDB(t *testing.T, table string) *DB {
	t.Helper()
	cfg := testenv.Require(t)
	conn := writeConn(t, cfg.SourceDBURL)
	role := testenv.Name("ro")
	const pw = "it-select-only" // a throwaway role on the local dev stack

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE ROLE "+role+" LOGIN PASSWORD '"+pw+"'"); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP OWNED BY "+role)
		_, _ = conn.Exec(ctx, "DROP ROLE IF EXISTS "+role)
	})
	if _, err := conn.Exec(ctx, "GRANT SELECT ON "+table+" TO "+role); err != nil {
		t.Fatalf("grant select: %v", err)
	}

	u, err := url.Parse(cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("bad SOURCE_DB_URL: %v", err)
	}
	u.User = url.UserPassword(role, pw)
	db, err := New(context.Background(), u.String())
	if err != nil {
		t.Fatalf("connect as the select-only role (%s): %v", hostPort(cfg.SourceDBURL), err)
	}
	t.Cleanup(db.Close)
	return db
}

// A SELECT-only role sees no constraints in information_schema, so pg_row
// failed on every table for the documented pilot role (#385).
func TestPrimaryKeyColumnSelectOnlyRoleIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	conn := writeConn(t, cfg.SourceDBURL)
	table := testenv.Name("pk")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "CREATE TABLE public."+table+" (code text, id int PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	})

	db := selectOnlyDB(t, "public."+table)
	col, err := db.PrimaryKeyColumn(context.Background(), "public."+table)
	if err != nil || col != "id" {
		t.Fatalf("PrimaryKeyColumn as a SELECT-only role = %q, %v; want \"id\"", col, err)
	}
}

// A partitioned table's publication lists its leaf partitions only
// (publish_via_partition_root = false), and the table read as not in the
// publication, with its own partition counted as another member (#400).
func TestPublicationPartitionedTableIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	db := sourceDB(t)
	conn := writeConn(t, cfg.SourceDBURL)
	table := testenv.Name("part")
	for _, viaRoot := range []string{"false", "true"} {
		t.Run("publish_via_partition_root="+viaRoot, func(t *testing.T) {
			pub := table + "_pub_" + viaRoot
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for _, stmt := range []string{
				"CREATE TABLE IF NOT EXISTS public." + table + " (id int, created date, PRIMARY KEY (id, created)) PARTITION BY RANGE (created)",
				"CREATE TABLE IF NOT EXISTS public." + table + "_2026 PARTITION OF public." + table + " FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
				"CREATE PUBLICATION " + pub + " FOR TABLE public." + table + " WITH (publish_via_partition_root = " + viaRoot + ")",
			} {
				if _, err := conn.Exec(ctx, stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, _ = conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pub)
				_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
			})

			got, other, err := db.PublicationSourceTables(context.Background(), pub, "piperoom", []string{"public." + table})
			if err != nil {
				t.Fatalf("PublicationSourceTables: %v", err)
			}
			if len(got) != 1 || !got[0].InPublication || other != 0 {
				t.Fatalf("got %+v other=%d; want the table in the publication and no other member", got, other)
			}
		})
	}
}
