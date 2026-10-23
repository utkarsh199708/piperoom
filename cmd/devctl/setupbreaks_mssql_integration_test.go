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
// integration tests skip without the dev stack. It leaves the profile as it
// found it: Agent running, dbo.orders tracked again when it was, and CDC off
// for the database when it was off at the start.

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"piperoom/internal/clients/sqlserver"
)

// mssqlDevDSN is the profile's sa login (dev-only credentials, AGENTS.md §3;
// dev/mssql/README.md). It is fixed, not read from PIPEROOM_MSSQL_DSN: the
// breaks run sqlcmd inside the profile's container (mssqlContainer), so the
// client must read that same server.
const mssqlDevDSN = "sqlserver://sa:Piperoom-dev-1@localhost:11433?database=orders&encrypt=disable"

// sqlMSSQLDisableDatabaseCDC turns CDC off for database orders (a no-op when
// it is off): the cleanup's way back to the fresh profile.
const sqlMSSQLDisableDatabaseCDC = `SET NOCOUNT ON; USE orders; ` +
	`IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = N'orders') = 1 EXEC sys.sp_cdc_disable_db`

func mssqlIntegration(t *testing.T) (*stack, *sqlserver.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := sqlserver.New(ctx, mssqlDevDSN)
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

	// The cdc-not-enabled-table break turns CDC on for the database; put it
	// back the way it was so the next run (and anyone sharing the profile)
	// starts from the same state.
	dbCDCBefore, err := c.DatabaseCDC(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer ccancel()
		if err := setupFixCmd(cctx, s, "cdc-not-enabled-table"); err != nil {
			t.Errorf("cleanup: setup-fix cdc-not-enabled-table: %v", err)
		}
		if dbCDCBefore {
			return
		}
		if _, err := s.mssqlQuery(cctx, sqlMSSQLDisableDatabaseCDC); err != nil {
			t.Errorf("cleanup: disabling CDC on database orders: %v", err)
		}
	})

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
