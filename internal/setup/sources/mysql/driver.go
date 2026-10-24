package mysql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"piperoom/internal/clients"
	mysqlclient "piperoom/internal/clients/mysql"
	"piperoom/internal/setup/plan"
)

// Constants of the driver: the source kind, the connector class and the
// retention floors.
const (
	// SourceKind is the SetupRequest.SourceKind this driver serves.
	SourceKind = "mysql"
	// ConnectorClass is the Debezium MySQL connector plugin.
	ConnectorClass = "io.debezium.connector.mysql.MySqlConnector"
	// MinBinlogRetentionSeconds is the floor of binlog_expire_logs_seconds: a
	// stopped connector must be able to stay stopped for a day, or its first
	// restart finds its position purged.
	MinBinlogRetentionSeconds = 86400
	// MinRDSRetentionHours is the same floor for RDS/Aurora, whose "binlog
	// retention hours" default (NULL) purges a file as soon as it is closed.
	MinRDSRetentionHours = 24
	// maxServerID is the largest MySQL server id (an unsigned 32-bit value).
	maxServerID = 4294967295
)

// The real names of the operations that fix a failing check. A check names one
// of these; a test pins every one to the runner's op table, so a drift is a test
// failure, not an agent proposing an op that does not exist. A check with no op
// has a documented ask instead: there is deliberately no AlterSystem on MySQL.
const (
	opCreateReadRole = "source_create_read_role"
	opGrantSelect    = "source_grant_select"
)

// The check ids, stable in logs and tests.
const (
	CheckLogBin             = "log_bin"
	CheckBinlogFormat       = "binlog_format"
	CheckBinlogRowImage     = "binlog_row_image"
	CheckGTID               = "gtid"
	CheckServerID           = "server_id"
	CheckBinlogRetention    = "binlog_expire_logs_seconds"
	CheckRDSBinlogRetention = "rds_binlog_retention"
	CheckTables             = "tables"
	CheckTablePrimaryKey    = "table_primary_key"
	CheckReadRole           = "read_role"
	CheckReadRoleGrants     = "read_role_grants"
	CheckReadRolePrivileges = "read_role_privileges"
)

// Setting names the checks read (the Q8 client's settings).
const (
	settingLogBin          = "log_bin"
	settingBinlogFormat    = "binlog_format"
	settingBinlogRowImage  = "binlog_row_image"
	settingGTIDMode        = "gtid_mode"
	settingEnforceGTID     = "enforce_gtid_consistency"
	settingServerID        = "server_id"
	settingExpireSeconds   = "binlog_expire_logs_seconds"
	settingAutoPurge       = "binlog_expire_logs_auto_purge"
	settingRDSRetentionHrs = "rds.binlog_retention_hours"
)

// Reader is the read-only source surface the checks need. It is what
// *mysql.DB (the Q8 client) provides on the read pool and what a setup room's
// remote client (remoteclients.Source) provides over the runner's
// source_settings, source_table_replication and source_role_info ops.
type Reader interface {
	Settings(ctx context.Context) ([]clients.DBSetting, error)
	TableReplication(ctx context.Context, tables []string) ([]clients.TableReplication, error)
	RoleInfo(ctx context.Context, role string, tables []string) (clients.RoleInfo, error)
}

var _ Reader = (*mysqlclient.DB)(nil)

// Evidence is what one check's read returned.
type Evidence struct {
	Settings []clients.DBSetting
	Tables   []clients.TableReplication
	Role     clients.RoleInfo

	// TablesMissing is true when the read reported a table as absent or not
	// visible to the read user (clients.ErrNotFound): a check failure, not an
	// error.
	TablesMissing bool
}

// ReadFunc is one check's read.
type ReadFunc func(ctx context.Context, r Reader, d Driver) (Evidence, error)

// Check is one prerequisite: the read that answers it, the condition that
// passes, and the fix a failure needs. ReadOp is the source read's name
// (source_settings, source_table_replication, source_role_info), FixOp the
// operation the room proposes (see the op* constants), FixAsk the documented
// ask for a failure no runner op can fix (every server setting, the RDS
// parameter group, a privilege of an existing user).
//
// FixFor overrides FixOp/FixAsk for a check whose fix depends on what the read
// returned (RDS wants the parameter group; the user exists but lacks a
// privilege, so creating it cannot help). Advisory marks a check whose failure
// is worth fixing but does not stop the connector from working (GTIDs, a table
// without a primary key): the room offers the fix and does not block on it.
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
	Advisory    bool
}

// Fix is the operation and the ask a failure of this check needs for one
// reading: FixFor's answer when it has one, the check's static fields
// otherwise.
func (c Check) Fix(d Driver, ev Evidence) (fixOp, ask string) {
	if c.FixFor != nil {
		return c.FixFor(d, ev)
	}
	return c.FixOp, c.FixAsk
}

// Tier is the tier the check's fix runs at on class: the higher of the op's
// floor and the class (plan.TierOf), so a production target routes a T2 fix to
// T3. It returns plan.ErrGuard when the guard table forbids the fix on class.
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

// Result is one check's outcome. Err is a read failure (the database was
// unreachable, the user was refused); OK is false then too.
type Result struct {
	Check    Check
	OK       bool
	Err      error
	Evidence Evidence
}

// Driver is the MySQL prerequisite driver for one pipeline: the user the
// connector logs in as, the source tables ("db.table") and, once chosen, the
// connector's database.server.id.
type Driver struct {
	Role   string
	Tables []string
	// ConnectorServerID is the database.server.id the connector will use; 0
	// when none is chosen yet. It must differ from the server's own server_id.
	ConnectorServerID int64
}

// Validate checks the driver's configuration before any read: a user and a
// non-empty list of identifier-shaped tables. It performs no I/O.
func (d Driver) Validate() error {
	if err := checkTables(d.Tables); err != nil {
		return err
	}
	if d.Role == "" {
		return fmt.Errorf("%w: the connector's database.user is required", ErrParams)
	}
	if err := checkUser(d.Role); err != nil {
		return fmt.Errorf("role: %w", err)
	}
	if d.ConnectorServerID < 0 || d.ConnectorServerID > maxServerID {
		return fmt.Errorf("%w: database.server.id %d is outside 1..%d", ErrParams, d.ConnectorServerID, uint64(maxServerID))
	}
	return nil
}

// Run validates the driver, then runs every check in order. A configuration
// error is returned before any read; a read's own error is stored on its Result
// so the other checks still run.
func (d Driver) Run(ctx context.Context, r Reader) ([]Result, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	if r == nil {
		return nil, fmt.Errorf("%w: no source reader", ErrParams)
	}
	checks := d.Checks()
	out := make([]Result, 0, len(checks))
	for _, c := range checks {
		ev, err := c.Read(ctx, r, d)
		out = append(out, Result{Check: c, OK: err == nil && c.Pass(d, ev), Err: err, Evidence: ev})
	}
	return out, nil
}

// Checks returns the driver's ordered checklist: the server settings that make
// the binlog readable as row events, the server id and retention, the tables,
// and the connector's user with its grants.
func (d Driver) Checks() []Check {
	return []Check{
		{
			ID:          CheckLogBin,
			Description: "log_bin must be ON: with the binary log off the connector has nothing to read.",
			ReadOp:      "source_settings",
			Pass:        func(d Driver, ev Evidence) bool { return settingIs(ev.Settings, settingLogBin, "on", "1") },
			FixFor: settingFix(
				"Remove --skip-log-bin / --disable-log-bin from the server's options (MySQL 8.0 and later has the binary log on by default) and restart in a maintenance window.",
				"On RDS/Aurora the binary log is on while automated backups are enabled: set the backup retention period above 0 (Aurora: set binlog_format in the cluster parameter group) and reboot in a maintenance window.",
			),
			TierFloor: plan.TierT3,
			Read:      readSettings,
		},
		{
			ID:          CheckBinlogFormat,
			Description: "binlog_format must be ROW: STATEMENT and MIXED carry no row images, and Debezium refuses them.",
			ReadOp:      "source_settings",
			Pass:        func(d Driver, ev Evidence) bool { return settingIs(ev.Settings, settingBinlogFormat, "row") },
			FixFor: settingFix(
				"Set binlog_format=ROW in the option file and restart in a maintenance window (the setup offers no SET PERSIST; a SET GLOBAL does not reach running sessions).",
				"On RDS/Aurora change binlog_format to ROW in the DB (cluster) parameter group and apply it in a maintenance window.",
			),
			TierFloor: plan.TierT3,
			Read:      readSettings,
		},
		{
			ID:          CheckBinlogRowImage,
			Description: "binlog_row_image must be FULL: MINIMAL and NOBLOB leave out the columns Debezium puts into before/after.",
			ReadOp:      "source_settings",
			Pass:        func(d Driver, ev Evidence) bool { return settingIs(ev.Settings, settingBinlogRowImage, "full") },
			FixFor: settingFix(
				"Set binlog_row_image=FULL in the option file and restart in a maintenance window.",
				"On RDS/Aurora change binlog_row_image to FULL in the DB (cluster) parameter group and apply it in a maintenance window.",
			),
			TierFloor: plan.TierT3,
			Read:      readSettings,
		},
		{
			ID:          CheckGTID,
			Description: "gtid_mode and enforce_gtid_consistency should be ON: with GTIDs the connector's position survives a failover to a replica, without them a failover forces a new snapshot.",
			ReadOp:      "source_settings",
			Pass: func(d Driver, ev Evidence) bool {
				return settingIs(ev.Settings, settingGTIDMode, "on") && settingIs(ev.Settings, settingEnforceGTID, "on", "1", "true")
			},
			FixFor: settingFix(
				"Enable GTIDs in the option file (gtid_mode=ON, enforce_gtid_consistency=ON), online through MySQL's OFF_PERMISSIVE, ON_PERMISSIVE, ON sequence or at a restart; nothing here changes it.",
				"On RDS/Aurora set gtid-mode=ON and enforce_gtid_consistency=ON in the DB parameter group.",
			),
			TierFloor: plan.TierT3,
			Advisory:  true,
			Read:      readSettings,
		},
		{
			ID:          CheckServerID,
			Description: "server_id must be non-zero (a server with server_id 0 refuses every replication connection) and different from the connector's database.server.id.",
			ReadOp:      "source_settings",
			Pass:        serverIDReady,
			FixFor:      serverIDFix,
			TierFloor:   plan.TierT3,
			Read:        readSettings,
		},
		{
			ID:          CheckBinlogRetention,
			Description: fmt.Sprintf("binlog_expire_logs_seconds must be at least %d (one day) or 0 (never purge by age), so a stopped connector finds its position again. Passes on RDS/Aurora, which purges by its own retention hours.", MinBinlogRetentionSeconds),
			ReadOp:      "source_settings",
			Pass:        binlogRetentionReady,
			FixAsk:      fmt.Sprintf("Set binlog_expire_logs_seconds to at least %d (and keep binlog_expire_logs_auto_purge ON) in the option file and restart in a maintenance window; the setup offers no SET PERSIST.", MinBinlogRetentionSeconds),
			TierFloor:   plan.TierT3,
			Read:        readSettings,
		},
		{
			ID:          CheckRDSBinlogRetention,
			Description: fmt.Sprintf("On RDS/Aurora, 'binlog retention hours' must be set to at least %d: unset (NULL) purges a binlog file as soon as it is closed.", MinRDSRetentionHours),
			ReadOp:      "source_settings",
			Pass:        rdsRetentionReady,
			FixAsk:      fmt.Sprintf("No setup op calls RDS procedures: run CALL mysql.rds_set_configuration('binlog retention hours', %d) as the master user, then re-read.", MinRDSRetentionHours),
			TierFloor:   plan.TierT3,
			Read:        readSettings,
		},
		{
			ID:          CheckTables,
			Description: "Every source table must exist and be visible to the read user.",
			ReadOp:      "source_table_replication",
			Pass:        func(d Driver, ev Evidence) bool { return !ev.TablesMissing && tablesSeen(d, ev) },
			FixAsk:      "A source table is missing or the read user cannot see it: create the table, or grant the read user SELECT on it, then re-read.",
			TierFloor:   plan.TierT2,
			Read:        readTables,
		},
		{
			ID:          CheckTablePrimaryKey,
			Description: "Every table should have a primary key: without one the change events have no key, so Kafka cannot order or compact them per row.",
			ReadOp:      "source_table_replication",
			Pass:        tablesHavePrimaryKey,
			FixAsk:      "Add a primary key to the table (a schema change for the table's owner); the connector works without one, but its events are keyless.",
			TierFloor:   plan.TierT3,
			Advisory:    true,
			Read:        readTables,
		},
		{
			ID:          CheckReadRole,
			Description: "The connector's database.user must exist, be unlocked with an unexpired password, and hold REPLICATION SLAVE and REPLICATION CLIENT.",
			ReadOp:      "source_role_info",
			Pass:        func(d Driver, ev Evidence) bool { return ev.Role.Exists && ev.Role.CanLogin && ev.Role.Replication },
			FixOp:       opCreateReadRole,
			FixFor:      readRoleFix,
			TierFloor:   plan.TierT2,
			Read:        readRole,
		},
		{
			ID:          CheckReadRoleGrants,
			Description: "The connector's user must have SELECT on every source table.",
			ReadOp:      "source_role_info",
			Pass:        roleSelectsAll,
			FixOp:       opGrantSelect,
			FixFor:      grantsFix,
			TierFloor:   plan.TierT2,
			Read:        readRole,
		},
		{
			ID:          CheckReadRolePrivileges,
			Description: "The connector's user must hold RELOAD and SHOW DATABASES (the initial snapshot), and on RDS/Aurora LOCK TABLES as well (RDS refuses the global read lock, so Debezium locks tables instead).",
			ReadOp:      "source_role_info",
			Pass:        privilegesReady,
			FixFor:      privilegesFix,
			TierFloor:   plan.TierT2,
			Read:        readRoleWithSettings,
		},
	}
}

// ---- reads ----

func readSettings(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	settings, err := r.Settings(ctx)
	if err != nil {
		return Evidence{}, fmt.Errorf("mysql driver: read settings: %w", err)
	}
	return Evidence{Settings: settings}, nil
}

// readTables reads the tables; a table the read user cannot see (or that does
// not exist) is the check's failure, not a read error.
func readTables(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	tables, err := r.TableReplication(ctx, d.Tables)
	if errors.Is(err, clients.ErrNotFound) {
		return Evidence{TablesMissing: true}, nil
	}
	if err != nil {
		return Evidence{}, fmt.Errorf("mysql driver: read table replication: %w", err)
	}
	return Evidence{Tables: tables}, nil
}

func readRole(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	role, err := r.RoleInfo(ctx, d.Role, d.Tables)
	if err != nil {
		return Evidence{}, fmt.Errorf("mysql driver: read user %s: %w", clipName(d.Role), err)
	}
	return Evidence{Role: role}, nil
}

// readRoleWithSettings reads the user and the settings: whether LOCK TABLES is
// needed depends on the server being RDS/Aurora, a settings fact.
func readRoleWithSettings(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	ev, err := readRole(ctx, r, d)
	if err != nil {
		return Evidence{}, err
	}
	settings, err := r.Settings(ctx)
	if err != nil {
		return Evidence{}, fmt.Errorf("mysql driver: read settings: %w", err)
	}
	ev.Settings = settings
	return ev, nil
}

// ---- pass predicates ----

// settingValue returns a setting's value, and false when the server did not
// report it.
func settingValue(settings []clients.DBSetting, name string) (string, bool) {
	for _, s := range settings {
		if s.Name == name {
			return s.Value, true
		}
	}
	return "", false
}

// settingIs reports whether the setting is reported and equals (ignoring case)
// one of want.
func settingIs(settings []clients.DBSetting, name string, want ...string) bool {
	v, ok := settingValue(settings, name)
	if !ok {
		return false
	}
	v = strings.TrimSpace(v)
	for _, w := range want {
		if strings.EqualFold(v, w) {
			return true
		}
	}
	return false
}

// settingInt parses a numeric setting.
func settingInt(settings []clients.DBSetting, name string) (int64, bool) {
	v, ok := settingValue(settings, name)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	return n, err == nil
}

// isRDS reports whether the server is RDS/Aurora: the Q8 client adds
// rds.binlog_retention_hours only when mysql.rds_show_configuration answered.
// A server where that call is refused looks like a plain server; its asks then
// name the option file, and the RDS check cannot run (a documented gap).
func isRDS(settings []clients.DBSetting) bool {
	_, ok := settingValue(settings, settingRDSRetentionHrs)
	return ok
}

func serverIDReady(d Driver, ev Evidence) bool {
	n, ok := settingInt(ev.Settings, settingServerID)
	if !ok || n <= 0 {
		return false
	}
	return d.ConnectorServerID == 0 || d.ConnectorServerID != n
}

// binlogRetentionReady passes on RDS/Aurora (its own check judges), with
// auto-purge off, with 0 (never purged by age) and from the floor up.
func binlogRetentionReady(d Driver, ev Evidence) bool {
	if isRDS(ev.Settings) {
		return true
	}
	if v, ok := settingValue(ev.Settings, settingAutoPurge); ok && strings.EqualFold(strings.TrimSpace(v), "off") {
		return true
	}
	n, ok := settingInt(ev.Settings, settingExpireSeconds)
	return ok && (n == 0 || n >= MinBinlogRetentionSeconds)
}

// rdsRetentionReady passes on a server that is not RDS/Aurora and on one whose
// retention hours reach the floor.
func rdsRetentionReady(d Driver, ev Evidence) bool {
	v, ok := settingValue(ev.Settings, settingRDSRetentionHrs)
	if !ok {
		return true
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	return err == nil && n >= MinRDSRetentionHours
}

func tablesSeen(d Driver, ev Evidence) bool {
	seen := make(map[string]bool, len(ev.Tables))
	for _, t := range ev.Tables {
		seen[t.Table] = true
	}
	for _, t := range d.Tables {
		if !seen[t] {
			return false
		}
	}
	return true
}

func tablesHavePrimaryKey(d Driver, ev Evidence) bool {
	if !tablesSeen(d, ev) {
		return false
	}
	for _, t := range ev.Tables {
		if !t.HasPrimaryKey {
			return false
		}
	}
	return true
}

// roleSelectsAll reports whether the user has SELECT on every source table.
func roleSelectsAll(d Driver, ev Evidence) bool {
	if !ev.Role.Exists {
		return false
	}
	for _, t := range d.Tables {
		if !ev.Role.Selectable[t] {
			return false
		}
	}
	return true
}

func privilegesReady(d Driver, ev Evidence) bool {
	if !ev.Role.Exists {
		return false
	}
	need := []string{mysqlclient.PrivReload, mysqlclient.PrivShowDatabases}
	if isRDS(ev.Settings) {
		need = append(need, mysqlclient.PrivLockTables)
	}
	for _, p := range need {
		if !ev.Role.Privileges[p] {
			return false
		}
	}
	return true
}

// ---- fixes ----

// settingFix is a FixFor for a server setting: no op (there is no AlterSystem
// on MySQL), the option-file ask on a plain server and the parameter-group ask
// on RDS/Aurora.
func settingFix(plain, rds string) func(Driver, Evidence) (string, string) {
	return func(d Driver, ev Evidence) (string, string) {
		if isRDS(ev.Settings) {
			return "", rds
		}
		return "", plain
	}
}

func serverIDFix(d Driver, ev Evidence) (string, string) {
	if n, ok := settingInt(ev.Settings, settingServerID); ok && n > 0 && d.ConnectorServerID == n {
		return "", fmt.Sprintf("The connector's database.server.id (%d) equals the server's server_id: choose another id for the connector (any whole number 1..%d not used by the server or a replica); the server's own id is not changed.", n, uint64(maxServerID))
	}
	if isRDS(ev.Settings) {
		return "", "server_id is 0 or not readable: set server_id in the DB parameter group and reboot in a maintenance window."
	}
	return "", "server_id is 0 or not readable: set server_id to a unique non-zero number in the option file and restart in a maintenance window."
}

// readRoleFix names what a failing read_role check needs. A user that does not
// exist is created (source_create_read_role). An existing user cannot be created
// again and must never be re-passworded (clients.SetupSourceWriter), so it gets
// no op and an honest ask.
func readRoleFix(d Driver, ev Evidence) (string, string) {
	if !ev.Role.Exists {
		return opCreateReadRole, ""
	}
	accts := "'" + clipName(d.Role) + "'@'<host>'"
	if len(ev.Role.Accounts) > 0 {
		accts = strings.Join(ev.Role.Accounts, ", ")
	}
	return "", "The user exists (" + accts + ") but is locked, has an expired password or lacks REPLICATION SLAVE or REPLICATION CLIENT: no setup op changes an existing user (it is never re-passworded). Run ALTER USER ... ACCOUNT UNLOCK or GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO the account, then re-read."
}

func grantsFix(d Driver, ev Evidence) (string, string) {
	if !ev.Role.Exists {
		return "", "The user does not exist yet: the read_role check creates it with SELECT on these tables."
	}
	return opGrantSelect, ""
}

func privilegesFix(d Driver, ev Evidence) (string, string) {
	if !ev.Role.Exists {
		return "", "The user does not exist yet: the read_role check creates it with RELOAD, SHOW DATABASES and LOCK TABLES."
	}
	return "", "No setup op grants a global privilege to an existing user: run GRANT RELOAD, SHOW DATABASES, LOCK TABLES ON *.* TO the account (LOCK TABLES is needed on RDS/Aurora only), then re-read."
}
