package oracle

import (
	"context"
	"fmt"
	oracleclient "piperoom/internal/clients/oracle"
	"piperoom/internal/setup/plan"
	"slices"
	"strings"
)

// Constants of the Oracle source and Debezium connector.
const (
	SourceKind         = "oracle"
	ConnectorClass     = "io.debezium.connector.oracle.OracleConnector"
	DefaultCanaryTable = "PIPEROOM.PIPEROOM_CANARY"
)

const (
	opCreatePublication = "source_create_publication"
	opCreateCanaryTable = "source_create_canary_table"
)

const (
	CheckEdition              = "oracle-edition"
	CheckDatabaseIdentity     = "oracle-database-identity"
	CheckArchiveLog           = "oracle-archive-log"
	CheckSupplementalLogging  = "oracle-supplemental-logging"
	CheckTableSupplementalLog = "oracle-table-supplemental-logging"
	CheckLogMinerGrants       = "oracle-logminer-grants"
)

const (
	readDatabaseInfo = "source_database_info"
	readTableLogging = "source_table_supplemental_logging"
	readLogMiner     = "source_logminer_grants"
)

// Reader is the read-only Oracle client surface used by the checks.
type Reader = oracleclient.Reader

var _ Reader = (*oracleclient.Client)(nil)

// Evidence is what one check's read returned.
type Evidence struct {
	Database oracleclient.DatabaseInfo
	Tables   []oracleclient.TableSupplementalLogging
	Grants   oracleclient.LogMinerGrants
}

// ReadFunc is one check's read.
type ReadFunc func(ctx context.Context, r Reader, d Driver) (Evidence, error)

// Check is one prerequisite and the operation or human action its failure
// needs. FixFor supplies a fix that depends on the evidence; TierFloor is the
// minimum approval tier for that fix.
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

// Fix is the operation or DBA ask for one reading. Only one is returned.
func (c Check) Fix(d Driver, ev Evidence) (fixOp, ask string) {
	if c.FixFor != nil {
		return c.FixFor(d, ev)
	}
	return c.FixOp, c.FixAsk
}

// Tier returns the tier the fix needs on class, or plan.ErrGuard if the plan
// forbids that operation on the target class.
func (c Check) Tier(class string) (string, error) {
	tier, err := plan.TierOf(c.TierFloor, class)
	if err != nil {
		return "", err
	}
	if c.FixOp != "" {
		if row, forbidden := plan.Forbidden(plan.GuardRequest{Op: c.FixOp, Class: class}); forbidden {
			return "", fmt.Errorf("%w: %s's fix %q on a %s target: %s", plan.ErrGuard, c.ID, c.FixOp, class, row.Reason)
		}
	}
	return tier, nil
}

// Result is one check outcome. Err is a read failure; OK is false then too.
type Result struct {
	Check    Check
	OK       bool
	Err      error
	Evidence Evidence
}

// Driver describes one Oracle source: the database identity and PDB selected
// for Debezium, the LogMiner account, source tables and the canary table.
// Tables are the pipeline tables; the canary is added to the checks and
// connector config automatically.
type Driver struct {
	Database     string
	PDB          string
	LogMinerUser string
	Tables       []string
	CanaryTable  string
}

// canaryTable returns the configured canary name or the dev-profile default.
func (d Driver) canaryTable() string {
	if strings.TrimSpace(d.CanaryTable) != "" {
		return strings.TrimSpace(d.CanaryTable)
	}
	return DefaultCanaryTable
}

func (d Driver) allTables() []string {
	return withCanary(d.Tables, d.canaryTable())
}

// Validate checks inputs before any reads.
func (d Driver) Validate() error {
	if err := checkIdent(d.Database); err != nil {
		return fmt.Errorf("database: %w", err)
	}
	if d.PDB != "" {
		if err := checkIdent(d.PDB); err != nil {
			return fmt.Errorf("PDB: %w", err)
		}
	}
	if err := checkIdent(d.LogMinerUser); err != nil {
		return fmt.Errorf("LogMiner user: %w", err)
	}
	if len(d.Tables) == 0 {
		return ErrNoTables
	}
	if err := checkTables(d.allTables()); err != nil {
		return err
	}
	return nil
}

// Run validates configuration before I/O, then reads each prerequisite in
// order. A read error stays on its result so the other independent checks run.
func (d Driver) Run(ctx context.Context, r Reader) ([]Result, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	if r == nil {
		return nil, fmt.Errorf("%w: no Oracle reader", ErrParams)
	}
	checks := d.Checks()
	results := make([]Result, 0, len(checks))
	for _, c := range checks {
		ev, err := c.Read(ctx, r, d)
		results = append(results, Result{Check: c, OK: err == nil && c.Pass(d, ev), Err: err, Evidence: ev})
	}
	return results, nil
}

// Checks returns the ordered Oracle prerequisites. Oracle Free is the only
// supported edition in this driver; other editions need edition-specific
// validation before the agent can claim they are ready.
func (d Driver) Checks() []Check {
	return []Check{
		{
			ID:          CheckEdition,
			Description: "This driver supports Oracle Database Free; edition-specific behavior for other Oracle editions is not verified.",
			ReadOp:      readDatabaseInfo,
			Read:        readDatabase,
			Pass:        func(_ Driver, ev Evidence) bool { return strings.EqualFold(ev.Database.Edition, "Free") },
			TierFloor:   plan.TierT3,
			FixAsk:      "Use a supported Oracle Database Free target, or have the database owner validate the edition-specific LogMiner and multitenant requirements before setup is treated as ready.",
		},
		{
			ID:          CheckDatabaseIdentity,
			Description: "The connected database and PDB must match the database.dbname and database.pdb.name that the connector will use.",
			ReadOp:      readDatabaseInfo,
			Read:        readDatabase,
			Pass:        databaseIdentityReady,
			TierFloor:   plan.TierT1,
			FixAsk:      "Connect to the intended service. For a CDB, set database.dbname to the CDB name and database.pdb.name to the PDB service, then re-read.",
		},
		{
			ID:          CheckArchiveLog,
			Description: "The Oracle database must be in ARCHIVELOG mode so LogMiner can read committed changes.",
			ReadOp:      readDatabaseInfo,
			Read:        readDatabase,
			Pass:        func(_ Driver, ev Evidence) bool { return strings.EqualFold(ev.Database.LogMode, "ARCHIVELOG") },
			TierFloor:   plan.TierT3,
			FixAsk:      "ARCHIVELOG mode requires the instance to be mounted and restarted. On any non-trivial target, schedule a maintenance window; a DBA must run SHUTDOWN IMMEDIATE, STARTUP MOUNT, ALTER DATABASE ARCHIVELOG, ALTER DATABASE OPEN, then reopen the PDBs.",
		},
		{
			ID:          CheckSupplementalLogging,
			Description: "Database-level minimal supplemental logging must be enabled for LogMiner.",
			ReadOp:      readDatabaseInfo,
			Read:        readDatabase,
			Pass:        func(_ Driver, ev Evidence) bool { return ev.Database.Supplemental.Minimal },
			TierFloor:   plan.TierT2,
			FixAsk:      "A DBA must enable minimal database-level supplemental logging with ALTER DATABASE ADD SUPPLEMENTAL LOG DATA, then re-read. The current Oracle setup writer has no operation for this database-level change.",
		},
		{
			ID:          CheckTableSupplementalLog,
			Description: "Every source and canary table needs an unconditional all-column supplemental log group, unless database-level ALL supplemental logging is enabled.",
			ReadOp:      readTableLogging,
			Read:        readSupplementalLogging,
			Pass:        tableLoggingReady,
			FixOp:       opCreatePublication,
			FixFor:      tableLoggingFix,
			TierFloor:   plan.TierT2,
			FixAsk:      "Add an unconditional all-column supplemental log group to each listed source table with ALTER TABLE ... ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS, then re-read.",
		},
		{
			ID:          CheckLogMinerGrants,
			Description: "The connector account needs the LogMiner system privileges, SELECT_CATALOG_ROLE and SELECT on every source and canary table.",
			ReadOp:      readLogMiner,
			Read:        readGrants,
			Pass:        logMinerGrantsReady,
			FixFor:      logMinerGrantFix,
			TierFloor:   plan.TierT2,
			FixAsk:      "The Oracle reader does not expose whether the LogMiner user exists. A DBA must inspect the account and either create it with the required grants or grant the missing LogMiner privileges, SELECT_CATALOG_ROLE and per-table SELECT, then re-read.",
		},
	}
}

func readDatabase(ctx context.Context, r Reader, _ Driver) (Evidence, error) {
	info, err := r.Database(ctx)
	if err != nil {
		return Evidence{}, fmt.Errorf("oracle driver: read database prerequisites: %w", err)
	}
	return Evidence{Database: info}, nil
}

func readSupplementalLogging(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	info, err := r.Database(ctx)
	if err != nil {
		return Evidence{}, fmt.Errorf("oracle driver: read database prerequisites: %w", err)
	}
	tables, err := r.TableSupplementalLogging(ctx, d.allTables())
	if err != nil {
		return Evidence{}, fmt.Errorf("oracle driver: read table supplemental logging: %w", err)
	}
	return Evidence{Database: info, Tables: tables}, nil
}

func readGrants(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	grants, err := r.LogMinerGrants(ctx, d.LogMinerUser, d.allTables())
	if err != nil {
		return Evidence{}, fmt.Errorf("oracle driver: read LogMiner grants: %w", err)
	}
	return Evidence{Grants: grants}, nil
}

func databaseIdentityReady(d Driver, ev Evidence) bool {
	db := ev.Database
	if !strings.EqualFold(db.Name, d.Database) {
		return false
	}
	if db.IsCDB {
		if d.PDB == "" || !strings.EqualFold(db.Container, d.PDB) {
			return false
		}
		return slices.ContainsFunc(db.PDBs, func(pdb string) bool { return strings.EqualFold(pdb, d.PDB) })
	}
	return d.PDB == ""
}

func tableLoggingReady(d Driver, ev Evidence) bool {
	if ev.Database.Supplemental.All {
		return true
	}
	logged := make(map[string]bool, len(ev.Tables))
	for _, table := range ev.Tables {
		logged[strings.ToUpper(table.Table)] = table.AllColumns && table.Always
	}
	for _, table := range d.allTables() {
		if !logged[strings.ToUpper(table)] {
			return false
		}
	}
	return true
}

func tableLoggingFix(d Driver, ev Evidence) (fixOp, ask string) {
	if ev.Database.Supplemental.All {
		return "", ""
	}
	seen := make(map[string]oracleclient.TableSupplementalLogging, len(ev.Tables))
	for _, table := range ev.Tables {
		seen[strings.ToUpper(table.Table)] = table
	}
	canary := strings.ToUpper(d.canaryTable())
	if c, ok := seen[canary]; !ok || !c.AllColumns || !c.Always {
		return opCreateCanaryTable, ""
	}
	for _, table := range d.Tables {
		if t, ok := seen[strings.ToUpper(table)]; !ok || !t.AllColumns || !t.Always {
			return opCreatePublication, ""
		}
	}
	return "", "The table logging read was incomplete; re-read the source before proposing a write."
}

var requiredSystemPrivileges = []string{
	"CREATE SESSION",
	"SET CONTAINER",
	"FLASHBACK ANY TABLE",
	"SELECT ANY TRANSACTION",
	"LOGMINING",
}

var requiredRoles = []string{"SELECT_CATALOG_ROLE"}

func missingLogMinerGrants(d Driver, grants oracleclient.LogMinerGrants) []string {
	var missing []string
	for _, want := range requiredSystemPrivileges {
		if !slices.ContainsFunc(grants.SysPrivs, func(got string) bool { return strings.EqualFold(got, want) }) {
			missing = append(missing, want)
		}
	}
	for _, want := range requiredRoles {
		if !slices.ContainsFunc(grants.Roles, func(got string) bool { return strings.EqualFold(got, want) }) {
			missing = append(missing, want)
		}
	}
	for _, table := range d.allTables() {
		ok := slices.ContainsFunc(grants.TablePrivs, func(got oracleclient.TablePrivilege) bool {
			return strings.EqualFold(got.Table, table) && strings.EqualFold(got.Privilege, "SELECT")
		})
		if !ok {
			missing = append(missing, "SELECT ON "+table)
		}
	}
	return missing
}

func logMinerGrantsReady(d Driver, ev Evidence) bool {
	return len(missingLogMinerGrants(d, ev.Grants)) == 0
}

func logMinerGrantFix(d Driver, ev Evidence) (fixOp, ask string) {
	missing := missingLogMinerGrants(d, ev.Grants)
	if len(missing) == 0 {
		return "", ""
	}
	if len(ev.Grants.SysPrivs)+len(ev.Grants.Roles)+len(ev.Grants.TablePrivs) == 0 {
		return "", "No grants were observed, but the Oracle reader does not expose whether the user exists. A DBA must inspect the account, create it if absent or grant its missing privileges if it exists, then re-read."
	}
	return "", "A DBA must grant the missing LogMiner privileges, SELECT_CATALOG_ROLE and per-table SELECT: " + strings.Join(missing, ", ") + "."
}

func (d Driver) TablesForConnector() []string {
	return withCanary(d.Tables, d.canaryTable())
}

func withCanary(tables []string, canary string) []string {
	out := slices.Clone(tables)
	if !slices.Contains(out, canary) {
		out = append(out, canary)
	}
	return out
}
