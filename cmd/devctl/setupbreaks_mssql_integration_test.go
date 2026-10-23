//go:build integration

package main

// The SQL Server setup breaks on the real mssql profile (U4c): each break
// makes the SQL Server client's own check fail — AgentState for
// agent-stopped, TableCDC (with DatabaseCDC true) for cdc-not-enabled-table —
// and its fix makes it pass again. Start the profile first:
//
//	docker compose -f dev/docker-compose.yml --profile mssql up -d --wait mssql
//
// The test skips when the profile is not reachable, like the other
// integration tests skip without the dev stack.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"piperoom/internal/clients/sqlserver"
)

// mssqlDevDSN is the profile's sa login (dev-only credentials, AGENTS.md §3;
// dev/mssql/README.md). PIPEROOM_MSSQL_DSN overrides it.
const mssqlDevDSN = "sqlserver://sa:Piperoom-dev-1@localhost:11433?database=orders&encrypt=disable"

func mssqlIntegration(t *testing.T) (*stack, *sqlserver.Client) {
	t.Helper()
	dsn := os.Getenv("PIPEROOM_MSSQL_DSN")
	if dsn == "" {
		dsn = mssqlDevDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := sqlserver.New(ctx, dsn)
	if err != nil {
		t.Skipf("mssql profile not reachable (docker compose -f dev/docker-compose.yml --profile mssql up -d --wait mssql): %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	s := &stack{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return s, c
}

func TestMSSQLSetupBreaksIntegration(t *testing.T) {
	s, c := mssqlIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Run("agent-stopped", func(t *testing.T) {
		t.Cleanup(func() { _ = setupFixCmd(context.Background(), s, "agent-stopped") })
		start := time.Now()
		if err := setupBreakCmd(ctx, s, "agent-stopped"); err != nil {
			t.Fatal(err)
		}
		st, err := c.AgentState(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if st.Running {
			t.Fatalf("client AgentState after the break = %+v, want not running", st)
		}
		t.Logf("break: %s, AgentState %+v", time.Since(start).Round(time.Millisecond), st)

		start = time.Now()
		if err := setupFixCmd(ctx, s, "agent-stopped"); err != nil {
			t.Fatal(err)
		}
		if st, err = c.AgentState(ctx); err != nil || !st.Running {
			t.Fatalf("client AgentState after the fix = %+v, %v; want running", st, err)
		}
		t.Logf("fix: %s, AgentState %+v", time.Since(start).Round(time.Millisecond), st)
	})

	t.Run("cdc-not-enabled-table", func(t *testing.T) {
		start := time.Now()
		if err := setupBreakCmd(ctx, s, "cdc-not-enabled-table"); err != nil {
			t.Fatal(err)
		}
		dbOn, err := c.DatabaseCDC(ctx, "orders")
		if err != nil || !dbOn {
			t.Fatalf("client DatabaseCDC(orders) after the break = %v, %v; want true", dbOn, err)
		}
		tables, err := c.TableCDC(ctx, []string{"dbo.orders"})
		if err != nil {
			t.Fatal(err)
		}
		if len(tables) != 1 || !tables[0].Exists || tables[0].CDCEnabled {
			t.Fatalf("client TableCDC after the break = %+v, want dbo.orders existing and not enabled", tables)
		}
		t.Logf("break: %s, TableCDC %+v", time.Since(start).Round(time.Millisecond), tables[0])

		start = time.Now()
		if err := setupFixCmd(ctx, s, "cdc-not-enabled-table"); err != nil {
			t.Fatal(err)
		}
		if tables, err = c.TableCDC(ctx, []string{"dbo.orders"}); err != nil || len(tables) != 1 || !tables[0].CDCEnabled {
			t.Fatalf("client TableCDC after the fix = %+v, %v; want enabled", tables, err)
		}
		t.Logf("fix: %s, TableCDC %+v", time.Since(start).Round(time.Millisecond), tables[0])

		// Idempotent on the real server: a second fix and a second break only read.
		if err := setupFixCmd(ctx, s, "cdc-not-enabled-table"); err != nil {
			t.Fatalf("second fix: %v", err)
		}
		if err := setupBreakCmd(ctx, s, "cdc-not-enabled-table"); err != nil {
			t.Fatal(err)
		}
		if err := setupBreakCmd(ctx, s, "cdc-not-enabled-table"); err != nil {
			t.Fatalf("second break: %v", err)
		}
		if err := setupFixCmd(ctx, s, "cdc-not-enabled-table"); err != nil {
			t.Fatal(err)
		}
	})
}
