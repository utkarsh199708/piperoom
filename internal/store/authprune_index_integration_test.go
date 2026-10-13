//go:build integration

package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/testenv"
	"piperoom/migrations"
)

// The prune DELETEs must use an index, or every run scans the whole table.
// The test seeds enough rows for the planner to prefer an index on its own
// (ANALYZE first), then checks the plan the code's SQL actually gets. Each
// test has its own schema, so the plan does not depend on other tests' data.
//
// EXPLAIN without ANALYZE is used on purpose: it plans the DELETE and does not
// run it.
//
// The second check sets enable_seqscan = off for the transaction only. That
// does not test the planner's choice; it proves each OR arm of the sessions
// prune can use an index by itself, including the partial revoked index (the
// query's "revoked_at IS NOT NULL" arm must imply its predicate).
func TestAuthPruneDeletesUseIndexesOnPostgres(t *testing.T) {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		t.Skipf("store: room Postgres unreachable at %s: %v", cfg.RoomDBURL, err)
	}
	defer admin.Close(ctx)
	schema := testenv.Name("prune_idx")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Skipf("store: cannot create schema: %v", err)
	}
	t.Cleanup(func() { dropPruneSchema(cfg.RoomDBURL, schema) })

	sqlCfg, err := pgx.ParseConfig(cfg.RoomDBURL)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	sqlCfg.RuntimeParams["search_path"] = schema
	sqlDB := stdlib.OpenDB(*sqlCfg)
	defer sqlDB.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, migrations.FS)
	if err != nil {
		t.Fatalf("goose provider: %v", err)
	}

	// Connection for EXPLAIN and seeding, pinned to the same schema.
	conn, err := pgx.ConnectConfig(ctx, sqlCfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	upAll := func(label string) {
		t.Helper()
		if _, err := provider.Up(ctx); err != nil {
			t.Fatalf("%s: migrate up: %v", label, err)
		}
		if v, err := provider.GetDBVersion(ctx); err != nil || v != 10 {
			t.Fatalf("%s: db version = %d, %v; want 10", label, v, err)
		}
	}
	upAll("first up")

	seed(t, ctx, conn)

	cutoff := time.Now().UTC().Add(-auth.SessionGrace)
	at := time.Now().UTC()

	// The plans for the exact statements authprune.go issues.
	t.Run("planner choice", func(t *testing.T) {
		sessPlan := explain(t, ctx, conn, pruneSessionsSQL, cutoff)
		lsPlan := explain(t, ctx, conn, pruneLoginStatesSQL, at)
		t.Logf("sessions prune plan:\n%s", sessPlan)
		t.Logf("login_states prune plan:\n%s", lsPlan)
		if !strings.Contains(sessPlan, "sessions_expires_at_idx") || !strings.Contains(sessPlan, "sessions_revoked_at_idx") {
			t.Errorf("sessions prune does not use both indexes:\n%s", sessPlan)
		}
		if strings.Contains(sessPlan, "Seq Scan on sessions") {
			t.Errorf("sessions prune seq-scans sessions:\n%s", sessPlan)
		}
		if !strings.Contains(lsPlan, "login_states_expires_idx") || strings.Contains(lsPlan, "Seq Scan on login_states") {
			t.Errorf("login_states prune does not use login_states_expires_idx:\n%s", lsPlan)
		}
	})

	t.Run("each arm indexable", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
			t.Fatalf("set: %v", err)
		}
		plan := explainTx(t, ctx, tx, pruneSessionsSQL, cutoff)
		t.Logf("sessions prune plan (seqscan off):\n%s", plan)
		if !strings.Contains(plan, "sessions_expires_at_idx") || !strings.Contains(plan, "sessions_revoked_at_idx") {
			t.Errorf("an OR arm of the sessions prune cannot use its index:\n%s", plan)
		}
	})

	// Down to 00008 removes exactly the two session indexes; up restores them.
	if _, err := provider.DownTo(ctx, 8); err != nil {
		t.Fatalf("migrate down to 8: %v", err)
	}
	if v, err := provider.GetDBVersion(ctx); err != nil || v != 8 {
		t.Fatalf("after down: version = %d, %v; want 8", v, err)
	}
	if got := indexNames(t, ctx, conn, "sessions_expires_at_idx", "sessions_revoked_at_idx"); len(got) != 0 {
		t.Fatalf("after down: indexes still present: %v", got)
	}
	upAll("second up")
	if got := indexNames(t, ctx, conn, "sessions_expires_at_idx", "sessions_revoked_at_idx"); len(got) != 2 {
		t.Fatalf("after second up: indexes = %v; want both", got)
	}
}

// dropPruneSchema is best effort, like dropSchema in the store_test package.
func dropPruneSchema(url, schema string) {
	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(dctx, url)
	if err != nil {
		return
	}
	defer c.Close(dctx)
	_, _ = c.Exec(dctx, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
}

// seed fills sessions and login_states so an index is the cheap plan: about 1%
// of rows match each prune condition, and ANALYZE gives the planner the counts.
func seed(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	const n = 50000
	if _, err := conn.Exec(ctx, `
INSERT INTO sessions (id, token_hash, user_id, method, created_at, last_seen_at, expires_at, revoked_at)
SELECT 's' || g, decode(md5(g::text), 'hex'), 'u' || g, 'dev', now() - interval '30 days', now(),
       CASE WHEN g % 100 = 0 THEN now() - interval '10 days' ELSE now() + interval '1 day' END,
       CASE WHEN g % 100 = 1 THEN now() - interval '10 days' ELSE NULL END
FROM generate_series(1, $1::int) g`, n); err != nil {
		t.Fatalf("seed sessions: %v", err)
	}
	if _, err := conn.Exec(ctx, `
INSERT INTO login_states (id, state, nonce, pkce_verifier, return_path, created_at, expires_at)
SELECT 'l' || g, 's', 'n', 'v', '/', now() - interval '1 hour',
       CASE WHEN g % 100 = 0 THEN now() - interval '1 minute' ELSE now() + interval '5 minutes' END
FROM generate_series(1, $1::int) g`, n); err != nil {
		t.Fatalf("seed login_states: %v", err)
	}
	if _, err := conn.Exec(ctx, `ANALYZE sessions; ANALYZE login_states`); err != nil {
		t.Fatalf("analyze: %v", err)
	}
}

func explain(t *testing.T, ctx context.Context, conn *pgx.Conn, query string, arg any) string {
	t.Helper()
	rows, err := conn.Query(ctx, "EXPLAIN "+query, arg)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	return collectPlan(t, rows)
}

func explainTx(t *testing.T, ctx context.Context, tx pgx.Tx, query string, arg any) string {
	t.Helper()
	rows, err := tx.Query(ctx, "EXPLAIN "+query, arg)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	return collectPlan(t, rows)
}

func collectPlan(t *testing.T, rows pgx.Rows) string {
	t.Helper()
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("plan rows: %v", err)
	}
	return b.String()
}

// indexNames returns which of want exist in the test schema (search_path).
func indexNames(t *testing.T, ctx context.Context, conn *pgx.Conn, want ...string) []string {
	t.Helper()
	var out []string
	for _, name := range want {
		var n int
		err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1`, name).Scan(&n)
		if err != nil {
			t.Fatalf("pg_indexes %s: %v", name, err)
		}
		if n > 0 {
			out = append(out, name)
		}
	}
	return out
}
