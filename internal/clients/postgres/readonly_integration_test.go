//go:build integration

package postgres

import (
	"context"
	"net/url"
	"strconv"
	"strings"
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
// publication, with its own partition counted as another member (#400). A
// publication of only some leaves leaves the other leaves' rows unpublished:
// the table is then not a member, and the listed leaf still counts as another
// member (#400 review).
func TestPublicationPartitionedTableIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	db := sourceDB(t)
	conn := writeConn(t, cfg.SourceDBURL)
	cases := []struct {
		name      string
		publish   string // FOR TABLE list; %s is the partitioned table
		with      string
		member    bool
		wantOther int
	}{
		{"root, via leaves", "public.%s", "publish_via_partition_root = false", true, 0},
		{"root, via root", "public.%s", "publish_via_partition_root = true", true, 0},
		{"every leaf", "public.%s_a, public.%s_b", "publish_via_partition_root = false", true, 0},
		{"one leaf of two", "public.%s_a", "publish_via_partition_root = false", false, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := testenv.Name("part")
			pub := table + "_pub"
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, _ = conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pub)
				_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
			})
			for _, stmt := range []string{
				"CREATE TABLE public." + table + " (id int, k int, PRIMARY KEY (id, k)) PARTITION BY LIST (k)",
				"CREATE TABLE public." + table + "_a PARTITION OF public." + table + " FOR VALUES IN (1)",
				"CREATE TABLE public." + table + "_b PARTITION OF public." + table + " FOR VALUES IN (2)",
				"CREATE PUBLICATION " + pub + " FOR TABLE " + strings.ReplaceAll(tc.publish, "%s", table) + " WITH (" + tc.with + ")",
			} {
				if _, err := conn.Exec(ctx, stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}

			got, other, err := db.PublicationSourceTables(context.Background(), pub, "piperoom", []string{"public." + table})
			if err != nil {
				t.Fatalf("PublicationSourceTables: %v", err)
			}
			if len(got) != 1 || got[0].InPublication != tc.member || other != tc.wantOther {
				t.Fatalf("got %+v other=%d; want in_publication=%v other=%d", got, other, tc.member, tc.wantOther)
			}
		})
	}
}

// A year of daily partitions must not time the tool out: the membership and
// other-member queries read the publication once (#416 review round 2, where
// a per-row membership check passed the 10 s deadline at about 150
// partitions). Both publish_via_partition_root settings are checked, since
// false lists every leaf.
func TestPublicationManyPartitionsIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	db := sourceDB(t)
	conn := writeConn(t, cfg.SourceDBURL)
	const partitions = 365
	for _, viaRoot := range []string{"false", "true"} {
		t.Run("publish_via_partition_root="+viaRoot, func(t *testing.T) {
			table := testenv.Name("many")
			pub := table + "_pub"
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_, _ = conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pub)
				_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
			})
			if _, err := conn.Exec(ctx, "CREATE TABLE public."+table+" (id int, k int, PRIMARY KEY (id, k)) PARTITION BY LIST (k)"); err != nil {
				t.Fatalf("create table: %v", err)
			}
			// table is testenv.Name output (it_many_<hex>): a safe identifier.
			if _, err := conn.Exec(ctx, `DO $$ BEGIN FOR i IN 1..`+strconv.Itoa(partitions)+` LOOP
				EXECUTE format('CREATE TABLE public.%I PARTITION OF public.%I FOR VALUES IN (%s)', '`+table+`_' || i, '`+table+`', i);
			END LOOP; END $$`); err != nil {
				t.Fatalf("create partitions: %v", err)
			}
			if _, err := conn.Exec(ctx, "CREATE PUBLICATION "+pub+" FOR TABLE public."+table+" WITH (publish_via_partition_root = "+viaRoot+")"); err != nil {
				t.Fatalf("create publication: %v", err)
			}

			start := time.Now()
			got, other, err := db.PublicationSourceTables(context.Background(), pub, "piperoom", []string{"public." + table})
			took := time.Since(start)
			if err != nil {
				t.Fatalf("PublicationSourceTables after %s: %v", took, err)
			}
			t.Logf("%d partitions: PublicationSourceTables took %s", partitions, took)
			if len(got) != 1 || !got[0].InPublication || other != 0 {
				t.Fatalf("got %+v other=%d; want the table in the publication and no other member", got, other)
			}
			if took > 2*time.Second {
				t.Fatalf("PublicationSourceTables took %s for %d partitions; want well under the 10 s deadline", took, partitions)
			}
		})
	}
}

// FOR ALL TABLES never publishes an unlogged partition, so a root whose
// other leaves are all listed is a member: it must not read as missing (and
// lead to ALTER PUBLICATION ... ADD TABLE, which fails on such a
// publication). #416 review round 2.
func TestPublicationAllTablesUnloggedLeafIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	db := sourceDB(t)
	conn := writeConn(t, cfg.SourceDBURL)
	table := testenv.Name("unlogged")
	pub := table + "_pub"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	})
	for _, stmt := range []string{
		"CREATE TABLE public." + table + " (id int, k int, PRIMARY KEY (id, k)) PARTITION BY LIST (k)",
		"CREATE TABLE public." + table + "_a PARTITION OF public." + table + " FOR VALUES IN (1)",
		"CREATE UNLOGGED TABLE public." + table + "_u PARTITION OF public." + table + " FOR VALUES IN (2)",
		"CREATE PUBLICATION " + pub + " FOR ALL TABLES",
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	got, _, err := db.PublicationSourceTables(context.Background(), pub, "piperoom", []string{"public." + table})
	if err != nil {
		t.Fatalf("PublicationSourceTables: %v", err)
	}
	if len(got) != 1 || !got[0].InPublication {
		t.Fatalf("got %+v; want the table in the publication", got)
	}
}
