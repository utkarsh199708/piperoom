package main

// SQL Server setup breaks (U4c): the `mssql` profile of dev/docker-compose.yml
// made to fail one prerequisite check of the SQL Server source, the way
// setupbreaks.go does it for Postgres. Each check runs the same read as the
// SQL Server client (internal/clients/sqlserver, U4a) that a setup run uses:
//
//   - agent-stopped: SQL Server Agent is stopped, so the client's AgentState
//     reports Running false (check sqlserver-agent-running). CDC's capture and
//     cleanup jobs are Agent jobs, so nothing reaches the change tables.
//   - cdc-not-enabled-table: CDC is on for database orders but dbo.orders is
//     not tracked, so the client's TableCDC reports CDCEnabled false for it
//     (check sqlserver-table-cdc) while DatabaseCDC reports true.
//
// The profile starts with CDC off (a setup run enables it), so the table check
// is defined for a CDC-enabled database: with CDC off on the database the
// database check is the one that fails, and the table check has nothing to
// say yet. The break therefore turns CDC on for the database before it takes
// the table out, and the fix turns the table back on under the capture
// instance names the break dropped (recorded on dbo.orders in between).
//
// Every statement is a fixed constant below; nothing from a request reaches
// sqlcmd. sqlcmd runs inside the container with the container's own sa
// password (MSSQL_SA_PASSWORD), so devctl never holds or prints it.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	mssqlContainer = "piperoom-dev-mssql-1"
	// mssqlSqlcmd is the tools18 sqlcmd of the 2022 images (the older
	// /opt/mssql-tools path is gone from them).
	mssqlSqlcmd = "/opt/mssql-tools18/bin/sqlcmd"
	// mssqlStateTimeout bounds the wait for Agent to stop or start after
	// xp_servicecontrol, which returns before the state changes.
	mssqlStateTimeout = 60 * time.Second
)

// The SQL Server named checks; unique among every setup check (a test in
// setupbreaks_test.go asserts it).
const (
	checkMSSQLAgentRunning = "sqlserver-agent-running"
	checkMSSQLTableCDC     = "sqlserver-table-cdc"
)

// scriptMSSQLQuery runs one batch ($1) as sa. -b makes a SQL error exit 1,
// -h -1 drops headers, -W trims padding, -s "|" separates columns, -C trusts
// the image's self-signed certificate (tools18 encrypts by default).
const scriptMSSQLQuery = `SQLCMDPASSWORD="$MSSQL_SA_PASSWORD" exec ` + mssqlSqlcmd + ` -S localhost -U sa -C -b -h -1 -W -s "|" -Q "$1"`

// The statements. The two reads mirror the client's queries (readers.go):
// AgentState reads sys.dm_server_services and falls back to Agent sessions
// when the DMV has no Agent row; TableCDC reads sys.tables.is_tracked_by_cdc.
const (
	sqlMSSQLAgentState = `SET NOCOUNT ON; ` +
		`SELECT COALESCE((SELECT TOP (1) status_desc FROM sys.dm_server_services WHERE servicename LIKE N'SQL Server Agent%'), ` +
		`CASE WHEN EXISTS (SELECT 1 FROM sys.dm_exec_sessions WHERE program_name LIKE N'SQLAgent - %') THEN N'Running' ELSE N'Unknown' END)`
	sqlMSSQLStopAgent  = `EXEC master.dbo.xp_servicecontrol N'STOP', N'SQLServerAGENT'`
	sqlMSSQLStartAgent = `EXEC master.dbo.xp_servicecontrol N'START', N'SQLServerAGENT'`

	// sqlMSSQLTableCDCState returns "<db cdc 0|1>|<table tracked 0|1, -1 = no table>",
	// or no row when database orders does not exist.
	sqlMSSQLTableCDCState = `SET NOCOUNT ON; ` +
		`SELECT CAST(d.is_cdc_enabled AS int), COALESCE((SELECT CAST(t.is_tracked_by_cdc AS int) FROM orders.sys.tables t ` +
		`JOIN orders.sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = N'dbo' AND t.name = N'orders'), -1) ` +
		`FROM sys.databases d WHERE d.name = N'orders'`
	// sqlMSSQLBreakTableCDC enables CDC on the database (a no-op when it is
	// on) and disables it on dbo.orders (a no-op when it is off). Before it
	// drops the table's capture instances it records their names as extended
	// properties on dbo.orders (piperoom_devctl_capture_instance_<n>), so the
	// fix recreates the same instances: the setup writer names them after the
	// publication (internal/clients/sqlserver CreatePublication), not with
	// SQL Server's default schema_table. A record from an earlier break is
	// dropped first, so the record is always this break's.
	sqlMSSQLBreakTableCDC = `SET NOCOUNT ON; USE orders; ` +
		`IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = N'orders') = 0 EXEC sys.sp_cdc_enable_db; ` +
		sqlMSSQLDropInstanceRecord +
		`IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = N'dbo' AND t.name = N'orders' AND t.is_tracked_by_cdc = 1) BEGIN ` +
		`DECLARE @inst sysname, @prop sysname, @n int = 0; ` +
		`DECLARE insts CURSOR LOCAL FAST_FORWARD FOR SELECT capture_instance FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(N'dbo.orders') ORDER BY create_date, capture_instance; ` +
		`OPEN insts; FETCH NEXT FROM insts INTO @inst; ` +
		`WHILE @@FETCH_STATUS = 0 BEGIN SET @n += 1; SET @prop = N'piperoom_devctl_capture_instance_' + CAST(@n AS nvarchar(10)); ` +
		`EXEC sys.sp_addextendedproperty @name = @prop, @value = @inst, @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'orders'; ` +
		`FETCH NEXT FROM insts INTO @inst; END; CLOSE insts; DEALLOCATE insts; ` +
		`EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'orders', @capture_instance = N'all'; END`
	// sqlMSSQLFixTableCDC enables CDC on dbo.orders when it is off, with the
	// arguments the setup writer's sp_cdc_enable_table uses (no gating role,
	// net changes on: the table has a primary key), once for each capture
	// instance the break recorded and under the recorded name. Without a
	// record (the table was turned off some other way) it creates one instance
	// with SQL Server's default name, dbo_orders. The record is dropped
	// afterwards.
	sqlMSSQLFixTableCDC = `SET NOCOUNT ON; USE orders; ` +
		`IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = N'dbo' AND t.name = N'orders' AND t.is_tracked_by_cdc = 1) BEGIN ` +
		`DECLARE @inst sysname, @restored int = 0; ` +
		`DECLARE insts CURSOR LOCAL FAST_FORWARD FOR SELECT CAST(value AS sysname) FROM sys.extended_properties ` + sqlMSSQLInstanceRecordWhere + ` ORDER BY name; ` +
		`OPEN insts; FETCH NEXT FROM insts INTO @inst; ` +
		`WHILE @@FETCH_STATUS = 0 BEGIN ` +
		`IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = @inst) ` +
		`EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders', @role_name = NULL, @supports_net_changes = 1, @capture_instance = @inst; ` +
		`SET @restored += 1; FETCH NEXT FROM insts INTO @inst; END; CLOSE insts; DEALLOCATE insts; ` +
		`IF @restored = 0 EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders', @role_name = NULL, @supports_net_changes = 1; END; ` +
		sqlMSSQLDropInstanceRecord

	// sqlMSSQLInstanceRecordWhere selects the break's record among the
	// extended properties of dbo.orders (the current database is orders).
	sqlMSSQLInstanceRecordWhere = `WHERE class = 1 AND major_id = OBJECT_ID(N'dbo.orders') AND minor_id = 0 AND name LIKE N'piperoom[_]devctl[_]capture[_]instance[_]%'`
	// sqlMSSQLDropInstanceRecord drops every recorded capture instance name.
	sqlMSSQLDropInstanceRecord = `DECLARE @rec sysname; ` +
		`WHILE 1 = 1 BEGIN SET @rec = (SELECT TOP (1) name FROM sys.extended_properties ` + sqlMSSQLInstanceRecordWhere + `); ` +
		`IF @rec IS NULL BREAK; ` +
		`EXEC sys.sp_dropextendedproperty @name = @rec, @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'orders'; END; `
)

// mssqlAgentStopped and mssqlCDCNotEnabledTable are registered in
// setupScenarios (setupbreaks.go).
var (
	mssqlAgentStopped = setupScenario{
		name: "agent-stopped", check: checkMSSQLAgentRunning,
		breakIt: breakMSSQLAgentStopped, fix: fixMSSQLAgentStopped, verify: verifyMSSQLAgentRunning,
	}
	mssqlCDCNotEnabledTable = setupScenario{
		name: "cdc-not-enabled-table", check: checkMSSQLTableCDC,
		breakIt: breakMSSQLTableCDC, fix: fixMSSQLTableCDC, verify: verifyMSSQLTableCDC,
	}
)

// mssqlQuery runs one fixed batch in the mssql container and returns its
// trimmed output. A non-zero exit (SQL error, or no such container) is an
// error naming how to start the profile.
func (s *stack) mssqlQuery(ctx context.Context, query string) (string, error) {
	out, code, err := s.execIn(ctx, mssqlContainer, "bash", "-c", scriptMSSQLQuery, "_", query)
	if err != nil {
		return "", fmt.Errorf("devctl: sqlcmd in %s: %w", mssqlContainer, err)
	}
	if code != 0 {
		return "", fmt.Errorf("devctl: sqlcmd in %s: exit %d: %s (is the profile up? docker compose -f dev/docker-compose.yml --profile mssql up -d --wait mssql)",
			mssqlContainer, code, strings.TrimSpace(out))
	}
	return strings.TrimSpace(out), nil
}

// ------------------------------------------------------------ agent-stopped

// mssqlAgentStatus is the client's AgentState status: "Running", "Stopped",
// ... or "Unknown" when neither the DMV nor a session shows the Agent.
func (s *stack) mssqlAgentStatus(ctx context.Context) (string, error) {
	out, err := s.mssqlQuery(ctx, sqlMSSQLAgentState)
	if err != nil {
		return "", err
	}
	return lastLine(out), nil
}

// verifyMSSQLAgentRunning is the sqlserver-agent-running check.
func verifyMSSQLAgentRunning(ctx context.Context, s *stack) (bool, string, error) {
	status, err := s.mssqlAgentStatus(ctx)
	if err != nil {
		return false, "", err
	}
	if status != "Running" {
		return false, "SQL Server Agent is " + status + "; CDC capture and cleanup jobs do not run", nil
	}
	return true, "SQL Server Agent is Running", nil
}

func breakMSSQLAgentStopped(ctx context.Context, s *stack) error {
	if _, err := s.mssqlQuery(ctx, sqlMSSQLStopAgent); err != nil {
		return fmt.Errorf("stopping SQL Server Agent: %w", err)
	}
	if err := s.waitMSSQLAgent(ctx, false); err != nil {
		return err
	}
	s.log.Info("broken: SQL Server Agent stopped (the setup check " + checkMSSQLAgentRunning + " fails; a container restart starts it again)")
	return nil
}

func fixMSSQLAgentStopped(ctx context.Context, s *stack) error {
	if _, err := s.mssqlQuery(ctx, sqlMSSQLStartAgent); err != nil {
		return fmt.Errorf("starting SQL Server Agent: %w", err)
	}
	if err := s.waitMSSQLAgent(ctx, true); err != nil {
		return err
	}
	s.log.Info("fixed: SQL Server Agent is Running")
	return nil
}

// waitMSSQLAgent waits until the Agent is (running) or is not (!running)
// Running: xp_servicecontrol returns before the state changes.
func (s *stack) waitMSSQLAgent(ctx context.Context, running bool) error {
	err := s.waitFor(ctx, mssqlStateTimeout, func() (bool, error) {
		status, err := s.mssqlAgentStatus(ctx)
		if err != nil {
			return false, err
		}
		return (status == "Running") == running, nil
	})
	if err != nil {
		return fmt.Errorf("waiting for SQL Server Agent (running=%t): %w", running, err)
	}
	return nil
}

// ---------------------------------------------------- cdc-not-enabled-table

// mssqlCDCState is what sqlMSSQLTableCDCState reads.
type mssqlCDCState struct {
	DatabaseCDC  bool
	TableExists  bool
	TableTracked bool
}

// parseMSSQLCDCState parses the "<db>|<table>" row. No row means database
// orders does not exist.
func parseMSSQLCDCState(out string) (mssqlCDCState, error) {
	line := lastLine(out)
	if line == "" {
		return mssqlCDCState{}, fmt.Errorf("database orders does not exist on %s (dev/mssql/init did not run; recreate the container)", mssqlContainer)
	}
	parts := strings.Split(line, "|")
	if len(parts) != 2 {
		return mssqlCDCState{}, fmt.Errorf("unexpected CDC state row %q", line)
	}
	db, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
	tbl, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err1 != nil || err2 != nil || db < 0 || db > 1 || tbl < -1 || tbl > 1 {
		return mssqlCDCState{}, fmt.Errorf("unexpected CDC state row %q", line)
	}
	return mssqlCDCState{DatabaseCDC: db == 1, TableExists: tbl >= 0, TableTracked: tbl == 1}, nil
}

func (s *stack) mssqlCDCState(ctx context.Context) (mssqlCDCState, error) {
	out, err := s.mssqlQuery(ctx, sqlMSSQLTableCDCState)
	if err != nil {
		return mssqlCDCState{}, err
	}
	st, err := parseMSSQLCDCState(out)
	if err != nil {
		return mssqlCDCState{}, fmt.Errorf("devctl: %w", err)
	}
	if !st.TableExists {
		return mssqlCDCState{}, fmt.Errorf("devctl: table orders.dbo.orders does not exist on %s (recreate the container)", mssqlContainer)
	}
	return st, nil
}

// verifyMSSQLTableCDC is the sqlserver-table-cdc check: in a CDC-enabled
// database, the captured table is tracked. A database with CDC off passes it
// (the database check fails there instead; see the file comment).
func verifyMSSQLTableCDC(ctx context.Context, s *stack) (bool, string, error) {
	st, err := s.mssqlCDCState(ctx)
	if err != nil {
		return false, "", err
	}
	switch {
	case !st.DatabaseCDC:
		return true, "CDC is off for database orders (the database check applies, not the table check)", nil
	case !st.TableTracked:
		return false, "CDC is on for database orders but dbo.orders is not tracked (sys.tables.is_tracked_by_cdc = 0)", nil
	}
	return true, "dbo.orders is tracked by CDC", nil
}

func breakMSSQLTableCDC(ctx context.Context, s *stack) error {
	if _, err := s.mssqlQuery(ctx, sqlMSSQLBreakTableCDC); err != nil {
		return fmt.Errorf("enabling CDC on orders without dbo.orders: %w", err)
	}
	s.log.Info("broken: CDC on for database orders, off for dbo.orders (the setup check " + checkMSSQLTableCDC + " fails)")
	return nil
}

func fixMSSQLTableCDC(ctx context.Context, s *stack) error {
	if _, err := s.mssqlQuery(ctx, sqlMSSQLFixTableCDC); err != nil {
		return fmt.Errorf("enabling CDC on dbo.orders: %w", err)
	}
	s.log.Info("fixed: dbo.orders is tracked by CDC")
	return nil
}

// lastLine is the last non-empty line of sqlcmd output: sp_cdc_* and Agent
// procedures print informational lines before a result.
func lastLine(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if l := strings.TrimSpace(lines[i]); l != "" {
			return l
		}
	}
	return ""
}
