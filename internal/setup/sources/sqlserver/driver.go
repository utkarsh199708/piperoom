package sqlserver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"piperoom/internal/clients"
	sqlclient "piperoom/internal/clients/sqlserver"
	"piperoom/internal/setup/plan"
)

// Constants of the driver.
const (
	// SourceKind is the SetupRequest.SourceKind this driver serves.
	SourceKind = "sqlserver"
	// ConnectorClass is the Debezium SQL Server connector plugin.
	ConnectorClass = "io.debezium.connector.sqlserver.SqlServerConnector"
	// DefaultCanaryTable is the ladder's canary table on SQL Server.
	DefaultCanaryTable = "dbo.piperoom_canary"
	// DefaultCaptureInstance is the CreatePublication name (the capture
	// instance, or its prefix for several tables) when the driver sets none.
	DefaultCaptureInstance = "piperoom"
	// MinMajorVersion is SQL Server 2017, the oldest release Debezium 3.x
	// documents for its SQL Server connector.
	MinMajorVersion = 14
)

// Platforms of Driver.Platform: where the SQL Server runs. Only the person or
// the provisional pipeline sets it, never model text.
const (
	PlatformSelfManaged = ""             // a server the person runs (host, VM, container)
	PlatformAzureSQL    = "azure_sql"    // Azure SQL Database: CDC runs on its own scheduler, no Agent
	PlatformAzureMI     = "azure_sql_mi" // Azure SQL Managed Instance: the Agent is the provider's
	PlatformRDS         = "rds"          // Amazon RDS for SQL Server: msdb.dbo.rds_cdc_enable_db
)

// TargetHost is the SetupRequest.Target whose runner holds the host's service
// manager: the only target where an Agent fix can be an op.
const TargetHost = "host"

// The operations that fix a failing check. TestCheckFixOpsAreRegistered pins
// each to remoteclients' op table or the host target's catalogue.
const (
	opCreatePublication = "source_create_publication" // sp_cdc_enable_db + sp_cdc_enable_table (T2)
	opCreateReadRole    = "source_create_read_role"   // the capture login (T2)
	opServiceRestart    = "service_restart"           // host target (T2)
)

// The read ops each check names. The runner has none of them for SQL Server
// yet (the PR's asks); *sqlserver.Client answers them locally.
const (
	readServerInfo  = "source_server_info"
	readAgentState  = "source_agent_state"
	readDatabaseCDC = "source_database_cdc"
	readTableCDC    = "source_table_cdc"
	readRoleInfo    = "source_role_info"
)

// The check ids, stable in logs, devctl breaks (U4c) and eval labels (U4d).
const (
	CheckVersionEdition    = "sqlserver-version-edition"
	CheckAgentRunning      = "sqlserver-agent-running"
	CheckDatabaseCDC       = "sqlserver-db-cdc"
	CheckTableCDC          = "sqlserver-table-cdc"
	CheckCaptureLogin      = "sqlserver-capture-login"
	CheckCaptureLoginGrant = "sqlserver-capture-login-grants"
)

// Reader is the read-only source surface the checks need: the U4a client.
type Reader = sqlclient.SettingsReader

var _ Reader = (*sqlclient.Client)(nil)

// Evidence is what one check's read returned.
type Evidence struct {
	Server sqlclient.ServerInfo
	Agent  sqlclient.AgentState
	Tables []sqlclient.TableCDC
	Login  sqlclient.RoleInfo
	// AgentNotNeeded: Azure SQL Database runs CDC without an Agent, so the
	// Agent read is not issued there.
	AgentNotNeeded bool
	// DatabaseExists is false when DatabaseCDC answered clients.ErrNotFound,
	// a check failure rather than a read error.
	DatabaseExists bool
	DatabaseCDC    bool
}

// ReadFunc is one check's read.
type ReadFunc func(ctx context.Context, r Reader, d Driver) (Evidence, error)

// Check is one prerequisite, shaped as the Postgres driver's: ReadOp is the
// source read's name, FixOp the operation the room proposes, FixAsk the
// documented ask when no op can fix it, FixFor the override when the fix
// depends on the evidence or the platform, TierFloor the fix's floor.
type Check struct {
	ID          string
	Description string
	ReadOp      string
	Read        ReadFunc
	Pass        func(d Driver, ev Evidence) bool
	FixOp       string
	FixFor      func(d Driver, ev Evidence) (fixOp, ask string)
	TierFloor   string
	FixAsk      string
}

// Fix is the operation or the ask a failure of this check needs for one
// reading. Only one of the two is set.
func (c Check) Fix(d Driver, ev Evidence) (fixOp, ask string) {
	if c.FixFor != nil {
		return c.FixFor(d, ev)
	}
	return c.FixOp, c.FixAsk
}

// Tier is the tier the check's fix runs at on class (plan.TierOf), or
// plan.ErrGuard when the guard table forbids the fix there (plan §3.6).
func (c Check) Tier(class string) (string, error) {
	tier, err := plan.TierOf(c.TierFloor, class)
	if err != nil {
		return "", err
	}
	if c.FixOp == "" {
		return tier, nil
	}
	if row, forbidden := plan.Forbidden(plan.GuardRequest{Op: c.FixOp, Class: class}); forbidden {
		return "", fmt.Errorf("%w: %s's fix %q on a %s target: %s", plan.ErrGuard, c.ID, c.FixOp, class, row.Reason)
	}
	return tier, nil
}

// Result is one check's outcome. Err is a read failure (OK is false then too);
// Note qualifies a failure another check's fix also resolves.
type Result struct {
	Check    Check
	OK       bool
	Err      error
	Evidence Evidence
	Note     string
}

// Driver is the SQL Server prerequisite driver for one pipeline.
type Driver struct {
	Database        string   // the source database (database.names)
	Login           string   // the connector's login (database.user)
	CaptureInstance string   // the CreatePublication name; DefaultCaptureInstance when ""
	Tables          []string // "schema.table"
	Target          string   // SetupRequest.Target ("host", "compose", ...)
	Platform        string   // one of the Platform* constants
}

func (d Driver) captureInstance() string {
	if d.CaptureInstance == "" {
		return DefaultCaptureInstance
	}
	return d.CaptureInstance
}

// Validate checks the driver's configuration before any read. No I/O.
func (d Driver) Validate() error {
	if err := checkTables(d.Tables); err != nil {
		return err
	}
	if err := checkDatabase(d.Database); err != nil {
		return fmt.Errorf("database: %w", err)
	}
	if d.Login == "" {
		return fmt.Errorf("%w: the connector's database.user is required", ErrParams)
	}
	if err := checkLogin(d.Login); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	if err := checkCaptureInstance(d.captureInstance(), d.Tables); err != nil {
		return fmt.Errorf("capture instance: %w", err)
	}
	switch d.Platform {
	case PlatformSelfManaged, PlatformAzureSQL, PlatformAzureMI, PlatformRDS:
	default:
		return fmt.Errorf("%w: platform %q", ErrParams, clipName(d.Platform))
	}
	return nil
}

// Run validates the driver, then runs every check in order. A configuration
// error is returned before any read; a read's own error is stored on its
// Result so the other checks still run.
func (d Driver) Run(ctx context.Context, r Reader) ([]Result, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	if r == nil {
		return nil, fmt.Errorf("%w: no source reader", ErrParams)
	}
	checks := d.Checks()
	out := make([]Result, 0, len(checks))
	dbCDCOff := false
	for _, c := range checks {
		ev, err := c.Read(ctx, r, d)
		res := Result{Check: c, OK: err == nil && c.Pass(d, ev), Err: err, Evidence: ev}
		if c.ID == CheckDatabaseCDC && err == nil && ev.DatabaseExists && !ev.DatabaseCDC {
			dbCDCOff = true
		}
		if c.ID == CheckTableCDC && dbCDCOff && !res.OK {
			res.Note = "CDC is off for the database: the " + CheckDatabaseCDC + " fix enables the database and these tables in one step"
		}
		out = append(out, res)
	}
	return out, nil
}

// Checks returns the ordered checklist.
func (d Driver) Checks() []Check {
	return []Check{
		{
			ID:          CheckVersionEdition,
			Description: "SQL Server 2017 (14.x) or later in an edition with CDC (Enterprise, Developer, Standard, Evaluation), or Azure SQL.",
			ReadOp:      readServerInfo,
			Read:        readServer,
			Pass:        func(d Driver, ev Evidence) bool { return versionEditionOK(ev.Server) },
			TierFloor:   plan.TierT3,
			FixAsk:      "No setup op upgrades SQL Server or changes its edition: Express and Web have no CDC, and Debezium needs 2017 or later. Upgrade or move the database, then re-read.",
		},
		{
			ID:          CheckAgentRunning,
			Description: "SQL Server Agent must be running: CDC's capture and cleanup jobs are Agent jobs, so nothing reaches the change tables without it.",
			ReadOp:      readAgentState,
			Read:        readAgent,
			Pass:        func(d Driver, ev Evidence) bool { return ev.AgentNotNeeded || ev.Agent.Running },
			FixOp:       opServiceRestart,
			FixFor:      agentFix,
			TierFloor:   plan.TierT2,
		},
		{
			ID:          CheckDatabaseCDC,
			Description: "CDC must be enabled on the source database (sys.databases.is_cdc_enabled).",
			ReadOp:      readDatabaseCDC,
			Read:        readDatabase,
			Pass:        func(d Driver, ev Evidence) bool { return ev.DatabaseExists && ev.DatabaseCDC },
			FixOp:       opCreatePublication,
			FixFor:      databaseFix,
			TierFloor:   plan.TierT2,
		},
		{
			ID:          CheckTableCDC,
			Description: "Every source table must exist and be tracked by CDC (sys.tables.is_tracked_by_cdc); the fix enables only UntrackedTables.",
			ReadOp:      readTableCDC,
			Read:        readTables,
			Pass:        tablesTracked,
			FixOp:       opCreatePublication,
			FixFor:      tablesFix,
			TierFloor:   plan.TierT2,
		},
		{
			ID:          CheckCaptureLogin,
			Description: "The connector's login must exist and be enabled.",
			ReadOp:      readRoleInfo,
			Read:        readLogin,
			Pass:        func(d Driver, ev Evidence) bool { return ev.Login.Exists && !ev.Login.Disabled },
			FixOp:       opCreateReadRole,
			FixFor:      loginFix,
			TierFloor:   plan.TierT2,
		},
		{
			ID:          CheckCaptureLoginGrant,
			Description: "The connector's login must have SELECT on every source table.",
			ReadOp:      readRoleInfo,
			Read:        readLogin,
			Pass:        loginSelectsAll,
			FixOp:       opCreateReadRole,
			FixFor:      grantsFix,
			TierFloor:   plan.TierT2,
		},
	}
}

// ---- reads ----

func readServer(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	info, err := r.ServerInfo(ctx)
	if err != nil {
		return Evidence{}, fmt.Errorf("sqlserver driver: read server info: %w", err)
	}
	return Evidence{Server: info}, nil
}

func readAgent(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	if d.Platform == PlatformAzureSQL {
		return Evidence{AgentNotNeeded: true}, nil
	}
	st, err := r.AgentState(ctx)
	if err != nil {
		return Evidence{}, fmt.Errorf("sqlserver driver: read SQL Server Agent state: %w", err)
	}
	return Evidence{Agent: st}, nil
}

func readDatabase(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	on, err := r.DatabaseCDC(ctx, d.Database)
	if errors.Is(err, clients.ErrNotFound) {
		return Evidence{DatabaseExists: false}, nil
	}
	if err != nil {
		return Evidence{}, fmt.Errorf("sqlserver driver: read CDC state of database %s: %w", d.Database, err)
	}
	return Evidence{DatabaseExists: true, DatabaseCDC: on}, nil
}

func readTables(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	tables, err := r.TableCDC(ctx, d.Tables)
	if err != nil {
		return Evidence{}, fmt.Errorf("sqlserver driver: read table CDC state: %w", err)
	}
	return Evidence{Tables: tables}, nil
}

func readLogin(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	info, err := r.RoleInfo(ctx, d.Login, d.Tables)
	if err != nil {
		return Evidence{}, fmt.Errorf("sqlserver driver: read login %s: %w", d.Login, err)
	}
	return Evidence{Login: info}, nil
}

// ---- pass predicates and fixes ----

// versionEditionOK: Azure SQL reports a fixed 12.x ProductVersion whatever its
// engine, so its edition alone passes; elsewhere the major version and an
// edition with CDC.
func versionEditionOK(s sqlclient.ServerInfo) bool {
	if s.Edition == "SQL Azure" {
		return true
	}
	major, err := strconv.Atoi(strings.SplitN(s.Version, ".", 2)[0])
	if err != nil || major < MinMajorVersion {
		return false
	}
	for _, e := range []string{"Enterprise", "Developer", "Standard"} {
		if strings.HasPrefix(s.Edition, e) {
			return true
		}
	}
	return false
}

// agentFix: on a host target the runner's service manager restarts SQL Server
// (T2); a managed Agent is the provider's; anywhere else the server is the
// person's own, so the fix is an ask.
func agentFix(d Driver, ev Evidence) (string, string) {
	switch {
	case d.Platform == PlatformRDS || d.Platform == PlatformAzureMI:
		return "", "The SQL Server Agent of a managed instance is the provider's: it is normally always on; open a support case or restart the instance from the provider's console, then re-read."
	case d.Target == TargetHost:
		return opServiceRestart, ""
	}
	return "", "Start SQL Server Agent as a sysadmin: EXEC xp_servicecontrol N'START', N'SQLServerAGENT'. On Linux enable it first (mssql-conf set sqlagent.enabled true, then restart mssql-server); in a container restart it with MSSQL_AGENT_ENABLED=true. Then re-read."
}

func databaseFix(d Driver, ev Evidence) (string, string) {
	if !ev.DatabaseExists {
		return "", "Database " + bracket(d.Database) + " does not exist or the read login cannot see it: correct database.names or grant the login access, then re-read."
	}
	if d.Platform == PlatformRDS {
		return "", "On Amazon RDS sys.sp_cdc_enable_db needs sysadmin, which RDS does not grant: as the master user run EXEC msdb.dbo.rds_cdc_enable_db '" + d.Database + "', then re-read (the table fix then runs as an op)."
	}
	return opCreatePublication, ""
}

func tablesTracked(d Driver, ev Evidence) bool {
	return len(missingTables(d, ev)) == 0 && len(UntrackedTables(d, ev)) == 0
}

// missingTables are the source tables the read did not find.
func missingTables(d Driver, ev Evidence) []string {
	var out []string
	for _, t := range d.Tables {
		i := slices.IndexFunc(ev.Tables, func(tc sqlclient.TableCDC) bool { return tc.Table == t })
		if i < 0 || !ev.Tables[i].Exists {
			out = append(out, t)
		}
	}
	return out
}

// UntrackedTables are the existing source tables CDC does not track: the
// tables the source_create_publication fix names, so a capture instance that
// already exists is never asked for again (the writer refuses that).
func UntrackedTables(d Driver, ev Evidence) []string {
	var out []string
	for _, tc := range ev.Tables {
		if tc.Exists && !tc.CDCEnabled && slices.Contains(d.Tables, tc.Table) {
			out = append(out, tc.Table)
		}
	}
	return out
}

func tablesFix(d Driver, ev Evidence) (string, string) {
	if missing := missingTables(d, ev); len(missing) > 0 {
		return "", "Table(s) " + strings.Join(missing, ", ") + " do not exist or the read login cannot see them: correct table.include.list or create the tables, then re-read."
	}
	return opCreatePublication, ""
}

func loginFix(d Driver, ev Evidence) (string, string) {
	if ev.Login.Exists {
		return "", "The login exists but is disabled; no setup op changes an existing login (it is never re-passworded). Run ALTER LOGIN " + bracket(d.Login) + " ENABLE, then re-read."
	}
	if d.Platform == PlatformAzureSQL {
		return "", "Azure SQL Database creates logins in master only: create a contained user (CREATE USER " + bracket(d.Login) + " WITH PASSWORD = ...) in the database with SELECT on the tables and on SCHEMA::cdc, store the password in the runner's secret store, then re-read."
	}
	return opCreateReadRole, ""
}

func loginSelectsAll(d Driver, ev Evidence) bool {
	if !ev.Login.Exists {
		return false
	}
	for _, t := range d.Tables {
		if !ev.Login.Selectable[t] {
			return false
		}
	}
	return true
}

// grantsFix: a missing login is created with its grants; an existing one is
// never altered by a setup op, so the ask lists the exact grants.
func grantsFix(d Driver, ev Evidence) (string, string) {
	if !ev.Login.Exists {
		return loginFix(d, ev)
	}
	var stmts []string
	for _, t := range d.Tables {
		if !ev.Login.Selectable[t] {
			stmts = append(stmts, "GRANT SELECT ON "+bracketTable(t)+" TO "+bracket(d.Login))
		}
	}
	stmts = append(stmts, "GRANT SELECT ON SCHEMA::cdc TO "+bracket(d.Login))
	return "", "No setup op grants to an existing login. In database " + bracket(d.Database) + " run: " + strings.Join(stmts, "; ") + ". Then re-read."
}
