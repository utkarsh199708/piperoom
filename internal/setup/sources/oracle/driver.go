package oracle

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	oraclient "piperoom/internal/clients/oracle"
	"piperoom/internal/setup/plan"
)

// Constants of the driver: the source kind, the connector class and the
// LogMiner adapter Debezium's Oracle connector uses.
const (
	// SourceKind is the SetupRequest.SourceKind this driver serves.
	SourceKind = "oracle"
	// ConnectorClass is the Debezium Oracle connector plugin.
	ConnectorClass = "io.debezium.connector.oracle.OracleConnector"
	// ConnectionAdapter is the Debezium Oracle adapter the setup uses:
	// LogMiner ships with every edition; XStream needs a GoldenGate licence.
	ConnectionAdapter = "logminer"
	// MiningStrategy reads the data dictionary from the online catalog, which
	// needs no DBMS_LOGMNR_D.BUILD into the redo logs (fewer grants, less redo).
	MiningStrategy = "online_catalog"
	// DefaultPort is the Oracle listener's default port.
	DefaultPort = 1521
	// CanaryTableName is the ladder's canary table name, created in the
	// schema of the pipeline's first table unless the setup names another.
	CanaryTableName = "PIPEROOM_CANARY"
)

// The real names of the operations that fix a failing check: the runner's
// setup-writer ops (remoteclients.OpSourceCreatePublication and
// OpSourceCreateReadRole). The U9a writer implements CreatePublication for
// Oracle as the per-table ALL COLUMNS supplemental log group, and
// CreateReadRole as the LogMiner user with the Debezium docs' grants. A test
// pins both to the runner's op table.
const (
	opCreatePublication = "source_create_publication"
	opCreateReadRole    = "source_create_read_role"
)

// The source reads the checks name. The runner has no Oracle reads yet
// (remoteclients.Source speaks Postgres): *oracle.Client answers them, and a
// setup room reaches them over the runner when it gains these ops (the PR's
// gaps).
const (
	readDatabase     = "oracle_database"
	readTableLogging = "oracle_table_supplemental_logging"
	readGrants       = "oracle_logminer_grants"
)

// The check ids, stable in logs and tests.
const (
	CheckEdition              = "edition"
	CheckArchiveLog           = "archive_log_mode"
	CheckSupplementalDatabase = "supplemental_logging_database"
	CheckContainer            = "container"
	CheckCommonUser           = "logminer_common_user"
	CheckLogMinerUser         = "logminer_user"
	CheckLogMinerGrants       = "logminer_grants"
	CheckTableGrants          = "logminer_table_grants"
	CheckSupplementalTables   = "supplemental_logging_tables"
)

// Reader is the read-only source surface the checks need: exactly the U9a
// client's oracle.Reader.
type Reader interface {
	Database(ctx context.Context) (oraclient.DatabaseInfo, error)
	TableSupplementalLogging(ctx context.Context, tables []string) ([]oraclient.TableSupplementalLogging, error)
	LogMinerGrants(ctx context.Context, user string, tables []string) (oraclient.LogMinerGrants, error)
}

var (
	_ Reader = (*oraclient.Client)(nil)
	_ Reader = (oraclient.Reader)(nil)
)

// Evidence is what one check's read returned.
type Evidence struct {
	DB     oraclient.DatabaseInfo
	Tables []oraclient.TableSupplementalLogging
	Grants oraclient.LogMinerGrants
}

// ReadFunc is one check's read.
type ReadFunc func(ctx context.Context, r Reader, d Driver) (Evidence, error)

// Check is one prerequisite: the read that answers it, the condition that
// passes, and the fix a failure needs. FixOp is the runner op the room
// proposes; FixAsk the documented ask for a failure no op can fix. FixFor
// overrides both when the fix depends on what the read returned (a CDB's
// LogMiner user cannot be created by the setup writer, a non-CDB's can).
//
// MaintenanceWindow is true when the fix restarts the database: the room
// schedules it only inside a recorded maintenance window.
type Check struct {
	ID                string
	Description       string
	ReadOp            string
	Read              ReadFunc
	Pass              func(d Driver, ev Evidence) bool
	FixOp             string
	FixFor            func(d Driver, ev Evidence) (fixOp, ask string)
	TierFloor         string
	FixAsk            string
	MaintenanceWindow bool
}

// Fix is the operation and the ask a failure of this check needs for one
// reading: FixFor's answer when it has one, the static fields otherwise.
func (c Check) Fix(d Driver, ev Evidence) (fixOp, ask string) {
	if c.FixFor != nil {
		return c.FixFor(d, ev)
	}
	return c.FixOp, c.FixAsk
}

// Tier is the tier the check's fix runs at on class: the higher of the floor
// and the class (plan.TierOf), so a production target routes a T2 fix to T3.
// It returns plan.ErrGuard when the guard table forbids the fix op on class.
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

// Result is one check's outcome. Err is a read failure; OK is false then too.
type Result struct {
	Check    Check
	OK       bool
	Err      error
	Evidence Evidence
}

// Driver is the Oracle prerequisite driver for one pipeline: the LogMiner
// user the connector logs in as, the source tables ("OWNER.TABLE", upper
// case), the PDB that holds them (empty on a non-CDB), and whether this room
// created the database.
//
// CreatedByRoom comes from the server's derivation (plan.CreatedByRoom over
// the room log), never from model text: it lowers the archive log fix from a
// T3 maintenance-window restart to a T2 restart of a database nobody else
// uses.
type Driver struct {
	User          string
	Tables        []string
	PDB           string
	CreatedByRoom bool
}

// Validate checks the driver's configuration before any read. It performs no
// I/O.
func (d Driver) Validate() error {
	if err := checkTables(d.Tables); err != nil {
		return err
	}
	if err := checkUser(d.User); err != nil {
		return err
	}
	if d.PDB != "" {
		if err := upperIdent(d.PDB); err != nil {
			return fmt.Errorf("pdb: %w", err)
		}
	}
	return nil
}

// CanaryTable is the ladder's canary table for this pipeline: CanaryTableName
// in the schema of the first source table. The U9a writer creates it only
// when it is one of the pipeline's source tables, so the setup adds it to the
// pipeline and to the connector's table list.
func (d Driver) CanaryTable() (string, error) {
	if err := checkTables(d.Tables); err != nil {
		return "", err
	}
	owner, _, _ := tableParts(d.Tables[0])
	return owner + "." + CanaryTableName, nil
}

// Run validates the driver, then runs every check in order. A configuration
// error is returned before any read; a read's own error is stored on its
// Result so the other checks still run. Each read runs at most once per Run:
// several checks judge the same V$DATABASE row.
func (d Driver) Run(ctx context.Context, r Reader) ([]Result, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	if r == nil {
		return nil, fmt.Errorf("%w: no source reader", ErrParams)
	}
	m := &memoReader{r: r}
	checks := d.Checks()
	out := make([]Result, 0, len(checks))
	for _, c := range checks {
		ev, err := c.Read(ctx, m, d)
		out = append(out, Result{Check: c, OK: err == nil && c.Pass(d, ev), Err: err, Evidence: ev})
	}
	return out, nil
}

// memoReader answers each read once per Run.
type memoReader struct {
	r Reader

	dbDone bool
	db     oraclient.DatabaseInfo
	dbErr  error

	tablesDone bool
	tables     []oraclient.TableSupplementalLogging
	tablesErr  error

	grantsDone bool
	grants     oraclient.LogMinerGrants
	grantsErr  error
}

func (m *memoReader) Database(ctx context.Context) (oraclient.DatabaseInfo, error) {
	if !m.dbDone {
		m.db, m.dbErr = m.r.Database(ctx)
		m.dbDone = true
	}
	return m.db, m.dbErr
}

// TableSupplementalLogging memoises the one table list the driver reads (its
// own); the argument is always d.Tables.
func (m *memoReader) TableSupplementalLogging(ctx context.Context, tables []string) ([]oraclient.TableSupplementalLogging, error) {
	if !m.tablesDone {
		m.tables, m.tablesErr = m.r.TableSupplementalLogging(ctx, tables)
		m.tablesDone = true
	}
	return m.tables, m.tablesErr
}

// LogMinerGrants memoises the one user's grants the driver reads.
func (m *memoReader) LogMinerGrants(ctx context.Context, user string, tables []string) (oraclient.LogMinerGrants, error) {
	if !m.grantsDone {
		m.grants, m.grantsErr = m.r.LogMinerGrants(ctx, user, tables)
		m.grantsDone = true
	}
	return m.grants, m.grantsErr
}

// archiveLogSteps are the statements a DBA runs as SYSDBA to switch archive
// log mode on. Fixed text: nothing from the room or the database reaches it.
const archiveLogSteps = "SHUTDOWN IMMEDIATE; STARTUP MOUNT; ALTER DATABASE ARCHIVELOG; ALTER DATABASE OPEN; " +
	"(on a CDB also ALTER PLUGGABLE DATABASE ALL OPEN), with db_recovery_file_dest and db_recovery_file_dest_size set first so archives land in a sized recovery area"

// Checks returns the driver's ordered checklist: the edition the driver
// covers, archive log mode, database-level supplemental logging, the
// CDB/PDB layout, the LogMiner user and its grants, and the tables'
// supplemental log groups.
func (d Driver) Checks() []Check {
	archiveFloor, archiveWindow := plan.TierT3, true
	archiveAsk := "Archive log mode needs a restart, which no runner op performs (the setup writer refuses ALTER DATABASE ARCHIVELOG on an open instance). " +
		"This room did not create the database, so a DBA runs it inside a recorded maintenance window: " + archiveLogSteps + ". Then re-read."
	if d.CreatedByRoom {
		archiveFloor, archiveWindow = plan.TierT2, false
		archiveAsk = "Archive log mode needs a restart, which no runner op performs (the setup writer refuses ALTER DATABASE ARCHIVELOG on an open instance). " +
			"This room created the database, so no maintenance window is needed: run as SYSDBA in its container " + archiveLogSteps + ". Then re-read."
	}
	return []Check{
		{
			ID:          CheckEdition,
			Description: "The driver covers Oracle Database Free (23ai, 26ai), Express Edition, and the 19c and 21c releases; on another edition or release it says so.",
			ReadOp:      readDatabase,
			Read:        readDB,
			Pass:        func(d Driver, ev Evidence) bool { return editionCovered(ev.DB) },
			TierFloor:   plan.TierT0,
			FixFor:      editionAsk,
		},
		{
			ID:                CheckArchiveLog,
			Description:       "The database must be in ARCHIVELOG mode: LogMiner reads archived redo, and a NOARCHIVELOG database loses changes at every log switch.",
			ReadOp:            readDatabase,
			Read:              readDB,
			Pass:              func(d Driver, ev Evidence) bool { return ev.DB.LogMode == "ARCHIVELOG" },
			TierFloor:         archiveFloor,
			FixAsk:            archiveAsk,
			MaintenanceWindow: archiveWindow,
		},
		{
			ID:          CheckSupplementalDatabase,
			Description: "Minimal supplemental logging must be on for the database (V$DATABASE.SUPPLEMENTAL_LOG_DATA_MIN), or ALL columns database-wide.",
			ReadOp:      readDatabase,
			Read:        readDB,
			Pass:        func(d Driver, ev Evidence) bool { return ev.DB.Supplemental.Minimal || ev.DB.Supplemental.All },
			TierFloor:   plan.TierT2,
			FixAsk: "No runner op changes database-wide logging: a DBA runs ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (online, no restart; in a CDB from CDB$ROOT), " +
				"which adds a little redo for every change in the database, then re-reads.",
		},
		{
			ID:          CheckContainer,
			Description: "In a CDB the connector names the PDB that holds the tables (database.pdb.name), and it must be one the database lists; a non-CDB names none.",
			ReadOp:      readDatabase,
			Read:        readDB,
			Pass:        containerReady,
			TierFloor:   plan.TierT0,
			FixFor:      containerAsk,
		},
		{
			ID:          CheckCommonUser,
			Description: "In a CDB the LogMiner user must be a common user (C##...): the connector mines from CDB$ROOT and switches into the PDB.",
			ReadOp:      readDatabase,
			Read:        readDB,
			Pass:        func(d Driver, ev Evidence) bool { return !ev.DB.IsCDB || isCommonUser(d.User) },
			TierFloor:   plan.TierT0,
			FixAsk: "Set database.user to a common user, such as C##DBZUSER, created in CDB$ROOT with CONTAINER=ALL; a local PDB user cannot open a LogMiner session. " +
				"Change the pipeline's user, then re-read.",
		},
		{
			ID:          CheckLogMinerUser,
			Description: "The LogMiner user must exist and be able to log in (CREATE SESSION).",
			ReadOp:      readGrants,
			Read:        readUserGrantsWithDB,
			Pass:        func(d Driver, ev Evidence) bool { return slices.Contains(ev.Grants.SysPrivs, "CREATE SESSION") },
			FixOp:       opCreateReadRole,
			TierFloor:   plan.TierT2,
			FixFor:      userFix,
		},
		{
			ID:          CheckLogMinerGrants,
			Description: "The LogMiner user needs the Debezium docs' system privileges (CREATE SESSION, SET CONTAINER in a CDB, FLASHBACK ANY TABLE, SELECT ANY TRANSACTION, LOGMINING) and SELECT_CATALOG_ROLE.",
			ReadOp:      readGrants,
			Read:        readUserGrantsWithDB,
			Pass:        func(d Driver, ev Evidence) bool { return len(missingGrants(ev)) == 0 },
			TierFloor:   plan.TierT2,
			FixFor:      grantsAsk,
		},
		{
			ID:          CheckTableGrants,
			Description: "The LogMiner user needs SELECT on every source table (or SELECT ANY TABLE).",
			ReadOp:      readGrants,
			Read:        readUserGrantsWithDB,
			Pass:        func(d Driver, ev Evidence) bool { return len(missingTableGrants(d, ev)) == 0 },
			TierFloor:   plan.TierT2,
			FixFor:      tableGrantsAsk,
		},
		{
			ID:          CheckSupplementalTables,
			Description: "Every source table needs an unconditional ALL COLUMNS supplemental log group (or ALL columns database-wide), or updates carry only the changed columns.",
			ReadOp:      readTableLogging,
			Read:        readTables,
			Pass:        tablesLogged,
			FixOp:       opCreatePublication,
			TierFloor:   plan.TierT2,
		},
	}
}

// ---- reads ----

func readDB(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	db, err := r.Database(ctx)
	if err != nil {
		return Evidence{}, fmt.Errorf("oracle driver: read database: %w", err)
	}
	return Evidence{DB: db}, nil
}

func readUserGrants(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	g, err := r.LogMinerGrants(ctx, d.User, d.Tables)
	if err != nil {
		return Evidence{}, fmt.Errorf("oracle driver: read grants of %s: %w", clipName(d.User), err)
	}
	return Evidence{Grants: g}, nil
}

// readUserGrantsWithDB reads the grants and the database: SET CONTAINER is
// required only in a CDB, and the ask names CONTAINER=ALL only there.
func readUserGrantsWithDB(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	ev, err := readUserGrants(ctx, r, d)
	if err != nil {
		return Evidence{}, err
	}
	db, err := readDB(ctx, r, d)
	if err != nil {
		return Evidence{}, err
	}
	ev.DB = db.DB
	return ev, nil
}

// readTables reads the tables' log groups and the database: ALL columns
// database-wide satisfies every table.
func readTables(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	tables, err := r.TableSupplementalLogging(ctx, d.Tables)
	if err != nil {
		return Evidence{}, fmt.Errorf("oracle driver: read table supplemental logging: %w", err)
	}
	db, err := readDB(ctx, r, d)
	if err != nil {
		return Evidence{}, err
	}
	return Evidence{Tables: tables, DB: db.DB}, nil
}

// ---- edition ----

// coveredEditions are the editions the driver covers (U9b: the free
// editions, plus Enterprise Edition for the 19c and 21c releases the Debezium
// Oracle connector documents and tests).
var coveredEditions = map[string]bool{
	"Free":               true,
	"Express Edition":    true,
	"Enterprise Edition": true,
}

// coveredReleases are the major releases the driver covers. 26ai reports
// release 23 (23.26) in its banner; the product token 26 is accepted for a
// banner without a release number.
var coveredReleases = map[int]bool{19: true, 21: true, 23: true, 26: true}

var (
	releaseRe = regexp.MustCompile(`Release ([0-9]+)\.`)
	productRe = regexp.MustCompile(`\b([0-9]+)(c|ai)\b`)
)

// releaseOf parses the major release from a V$VERSION banner, 0 when none.
// The banner is untrusted dictionary text: it is only matched, never echoed
// whole.
func releaseOf(banner string) int {
	if m := releaseRe.FindStringSubmatch(banner); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n
	}
	if m := productRe.FindStringSubmatch(banner); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n
	}
	return 0
}

// editionCovered reports whether the database is an edition and release the
// driver covers.
func editionCovered(db oraclient.DatabaseInfo) bool {
	return coveredEditions[db.Edition] && coveredReleases[releaseOf(db.Version)]
}

// editionAsk says which edition the driver met and that it does not cover it.
// The edition is one of the client's fixed keywords (never banner text) and
// the release a number.
func editionAsk(d Driver, ev Evidence) (string, string) {
	edition := ev.DB.Edition
	if edition == "" {
		edition = "an unknown edition"
	}
	release := "an unknown release"
	if n := releaseOf(ev.DB.Version); n > 0 {
		release = "release " + strconv.Itoa(n)
	}
	return "", "This driver covers Oracle Database Free (23ai, 26ai), Express Edition, and Enterprise Edition 19c and 21c; this database is " +
		edition + ", " + release + ", which it does not cover. The other checks still ran and their verdicts stand, but edition-specific limits " +
		"(LogMiner availability, licensing, RAC redo threads) are not checked: a DBA confirms them before the connector is created."
}

// ---- container ----

// containerReady: a CDB with a PDB the database lists, or a non-CDB with no
// PDB named.
func containerReady(d Driver, ev Evidence) bool {
	if !ev.DB.IsCDB {
		return d.PDB == ""
	}
	return d.PDB != "" && slices.Contains(ev.DB.PDBs, d.PDB)
}

// containerAsk names the configuration change. PDB names come from the
// dictionary: only those that pass the identifier rule are listed.
func containerAsk(d Driver, ev Evidence) (string, string) {
	if !ev.DB.IsCDB {
		return "", "This database is not a CDB: clear the pipeline's PDB (the connector config then has no database.pdb.name), then re-read."
	}
	var pdbs []string
	for _, p := range ev.DB.PDBs {
		if upperIdent(p) == nil {
			pdbs = append(pdbs, p)
		}
	}
	list := "none the read session can see (connect the read DSN to CDB$ROOT or to the tables' PDB)"
	if len(pdbs) > 0 {
		list = strings.Join(pdbs, ", ")
	}
	if d.PDB == "" {
		return "", "This database is a CDB: set the pipeline's PDB (database.pdb.name) to the PDB that holds the tables, one of: " + list + "; then re-read."
	}
	return "", "PDB " + d.PDB + " is not one this database lists (" + list + "): set the pipeline's PDB to the one that holds the tables, then re-read."
}

// ---- LogMiner user and grants ----

// userFix names what a failing logminer_user check needs. In a non-CDB the
// setup writer creates the user with the docs' grants
// (source_create_read_role). In a CDB it cannot: a common user is created in
// CDB$ROOT with CONTAINER=ALL grants, which the writer does not issue, so the
// person gets the statements. A user that exists without CREATE SESSION is
// never re-created (it would be re-passworded): it gets the grant to run.
func userFix(d Driver, ev Evidence) (string, string) {
	u := quoteOrName(d.User)
	exists := len(ev.Grants.SysPrivs) > 0 || len(ev.Grants.Roles) > 0 || len(ev.Grants.TablePrivs) > 0
	if exists {
		return "", "The user exists but cannot log in: no setup op changes an existing user (it is never re-passworded). Run GRANT CREATE SESSION TO " + u +
			containerAll(ev) + ", then re-read."
	}
	if !ev.DB.IsCDB {
		return opCreateReadRole, ""
	}
	return "", "No setup op creates a CDB common user (it needs CONTAINER=ALL grants from CDB$ROOT). A DBA runs, in CDB$ROOT: CREATE USER " + u +
		" IDENTIFIED BY <a password stored in the runner's secret store> CONTAINER=ALL; then the grants the logminer_grants check lists; then re-read."
}

// requiredSysPrivs are the system privileges the U9a writer grants the
// LogMiner user (the Debezium docs' list); SET CONTAINER only matters in a
// CDB.
var requiredSysPrivs = []string{"CREATE SESSION", "SET CONTAINER", "FLASHBACK ANY TABLE", "SELECT ANY TRANSACTION", "LOGMINING"}

// requiredRoles are the roles the U9a writer grants the LogMiner user.
var requiredRoles = []string{"SELECT_CATALOG_ROLE"}

// missingGrants is the privileges and roles the user lacks, in the fixed
// order of the lists above.
func missingGrants(ev Evidence) []string {
	var out []string
	for _, p := range requiredSysPrivs {
		if p == "SET CONTAINER" && !ev.DB.IsCDB {
			continue
		}
		if !slices.Contains(ev.Grants.SysPrivs, p) {
			out = append(out, p)
		}
	}
	for _, r := range requiredRoles {
		if !slices.Contains(ev.Grants.Roles, r) {
			out = append(out, r)
		}
	}
	return out
}

// grantsAsk lists the GRANT statements for what is missing. Every item is a
// fixed privilege name and the user passed the identifier rule.
func grantsAsk(d Driver, ev Evidence) (string, string) {
	missing := missingGrants(ev)
	stmts := make([]string, len(missing))
	for i, p := range missing {
		stmts[i] = "GRANT " + p + " TO " + quoteOrName(d.User) + containerAll(ev)
	}
	return "", "No setup op grants privileges to an existing user. A DBA runs" + inRoot(ev) + ": " + strings.Join(stmts, "; ") + "; then re-reads."
}

// missingTableGrants is the source tables the user cannot SELECT.
func missingTableGrants(d Driver, ev Evidence) []string {
	if slices.Contains(ev.Grants.SysPrivs, "SELECT ANY TABLE") {
		return nil
	}
	var out []string
	for _, t := range d.Tables {
		ok := false
		for _, p := range ev.Grants.TablePrivs {
			if p.Table == t && p.Privilege == "SELECT" {
				ok = true
				break
			}
		}
		if !ok {
			out = append(out, t)
		}
	}
	return out
}

// tableGrantsAsk lists the per-table GRANT statements. Table names passed the
// identifier rule in Validate.
func tableGrantsAsk(d Driver, ev Evidence) (string, string) {
	missing := missingTableGrants(d, ev)
	stmts := make([]string, 0, len(missing))
	for _, t := range missing {
		q, err := quoteTable(t)
		if err != nil {
			q = clipName(t)
		}
		stmts = append(stmts, "GRANT SELECT ON "+q+" TO "+quoteOrName(d.User))
	}
	where := ""
	if ev.DB.IsCDB && d.PDB != "" {
		where = " in PDB " + d.PDB
	}
	return "", "No setup op grants SELECT on an Oracle table to an existing user. A DBA runs" + where + ": " + strings.Join(stmts, "; ") + "; then re-reads."
}

// containerAll is the CONTAINER=ALL clause of a common user's grant.
func containerAll(ev Evidence) string {
	if ev.DB.IsCDB {
		return " CONTAINER=ALL"
	}
	return ""
}

// inRoot says where a CDB's common grants run.
func inRoot(ev Evidence) string {
	if ev.DB.IsCDB {
		return " in CDB$ROOT"
	}
	return ""
}

// quoteOrName quotes a validated name for a message, or clips it.
func quoteOrName(name string) string {
	if q, err := quoteIdent(name); err == nil {
		return q
	}
	return clipName(name)
}

// ---- supplemental logging ----

// tablesLogged reports whether every source table has an unconditional ALL
// COLUMNS log group, or the database logs ALL columns for every table.
func tablesLogged(d Driver, ev Evidence) bool {
	if ev.DB.Supplemental.All {
		return true
	}
	logged := make(map[string]bool, len(ev.Tables))
	for _, t := range ev.Tables {
		logged[t.Table] = t.AllColumns && t.Always
	}
	for _, t := range d.Tables {
		if !logged[t] {
			return false
		}
	}
	return true
}
