package main

// Oracle setup breaks (U9c): the `oracle` compose profile (dev/oracle/README.md)
// made to fail one of the three prerequisites the U9a client
// (internal/clients/oracle) reads before an Oracle pipeline can be built:
//
//   - no-archivelog: the database restarts in NOARCHIVELOG mode
//     (DatabaseInfo.LogMode), so LogMiner has no archived redo to read;
//   - no-supplemental-logging: the ALL COLUMNS log group of PIPEROOM.ORDERS
//     and the database-level minimal supplemental logging are dropped
//     (TableSupplementalLogging, SupplementalLogging.Minimal);
//   - missing-logminer-grant: LOGMINING is revoked from C##DBZUSER
//     (LogMinerGrants.SysPrivs).
//
// Each check evaluates the U9a client's own types (oracle.DatabaseInfo,
// oracle.TableSupplementalLogging, oracle.LogMinerGrants) with the grant list
// the U9a writer grants, so a break here fails at the named check of that
// client. The unit tests fill those types from a fake sqlplus; the integration
// test (setupbreaks_oracle_integration_test.go) fills them from the real U9a
// client over go-ora.
//
// devctl talks to the database the way the image's own scripts do: sqlplus
// as SYSDBA through `docker exec` (OS authentication, no password on a command
// line). Every statement is a constant below; nothing from a request or from
// the database reaches the SQL text.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients/oracle"
)

const (
	// oracleProfile is the compose profile of the Oracle service.
	oracleProfile = "oracle"
	// oracleContainer is the profile's container (project piperoom-dev).
	oracleContainer = "piperoom-dev-oracle-1"
	// oraclePDB is the pluggable database the captured table lives in.
	oraclePDB = "FREEPDB1"
	// oracleLogMinerUser is the common LogMiner user dev/oracle/cdc-setup.sh
	// creates, as the dictionary stores it.
	oracleLogMinerUser = "C##DBZUSER"
	// oracleCapturedTable is the table the connector captures, owner.table.
	oracleCapturedTable = "PIPEROOM.ORDERS"
	// oracleUpHint is how to start the profile, for the error a missing
	// container gives.
	oracleUpHint = "docker compose -f dev/docker-compose.yml --profile oracle up -d --wait oracle"

	// oracleQueryTimeout bounds a read or a grant; oracleRestartTimeout a
	// SHUTDOWN IMMEDIATE / STARTUP cycle (about 20 s on a laptop).
	oracleQueryTimeout   = 30 * time.Second
	oracleRestartTimeout = 3 * time.Minute
)

// The named checks of the Oracle scenarios.
const (
	checkOracleArchiveLog          = "oracle-archive-log-mode"
	checkOracleSupplementalLogging = "oracle-supplemental-logging"
	checkOracleLogMinerGrants      = "oracle-logminer-grants"
)

// oracleRequiredSysPrivs, oracleRequiredRoles and oracleRequiredTablePriv are
// what U9a's writer grants the LogMiner user (logminerSysPrivs, logminerRoles
// and the per-table SELECT in internal/clients/oracle/writer.go) and what its
// LogMinerGrants read reports: system privileges, roles, and SELECT on each
// captured table. The writer's V_$ and DBA_ object grants are not in the list
// because LogMinerGrants does not read them (it reads object privileges only
// on the tables it is given). A test keeps dev/oracle/cdc-setup.sh granting
// every item.
var (
	oracleRequiredSysPrivs  = []string{"CREATE SESSION", "SET CONTAINER", "FLASHBACK ANY TABLE", "SELECT ANY TRANSACTION", "LOGMINING"}
	oracleRequiredRoles     = []string{"SELECT_CATALOG_ROLE"}
	oracleRequiredTablePriv = "SELECT"
)

// errOracleProfileDown is returned when the Oracle container is not running.
var errOracleProfileDown = errors.New("the oracle profile is not running (start it with: " + oracleUpHint + ")")

// oracleSQLPlus is the fixed script `docker exec` runs: the SQL*Plus script
// arrives as $1 (a constant of this file) and goes to sqlplus on stdin, never
// through a shell parse.
const oracleSQLPlus = `printf '%s\n' "$1" | sqlplus -S -L / as sysdba`

// oracleScriptHeader makes sqlplus print bare values and exit non-zero on the
// first SQL or OS error.
const oracleScriptHeader = "SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF ECHO OFF LINESIZE 32767 TRIMSPOOL ON TAB OFF\n" +
	"WHENEVER SQLERROR EXIT FAILURE\n" +
	"WHENEVER OSERROR EXIT FAILURE\n"

// oracleScript wraps body in the header and an EXIT.
func oracleScript(body string) string {
	return oracleScriptHeader + body + "\nEXIT\n"
}

// oracleSQL runs one SQL*Plus script as SYSDBA in the Oracle container under
// its own deadline and returns the output. A non-zero exit is an error that
// carries sqlplus's output (ORA- codes, dictionary values; no secrets: the
// connection is OS-authenticated).
func (s *stack) oracleSQL(ctx context.Context, timeout time.Duration, body string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, code, err := s.execIn(ctx, oracleContainer, "bash", "-c", oracleSQLPlus, "_", oracleScript(body))
	// A deadline kills docker exec, which then reads as exit -1 (or as an
	// exec error): report the timeout, not the exit code.
	if (err != nil || code != 0) && ctx.Err() != nil {
		return "", fmt.Errorf("devctl: oracle: sqlplus timed out after %s: %w", timeout, ctx.Err())
	}
	if err != nil {
		return "", fmt.Errorf("devctl: oracle: %w", err)
	}
	if code != 0 {
		// docker's own refusal, not sqlplus output: the container is missing
		// or stopped.
		if daemon, ok := strings.CutPrefix(strings.TrimSpace(out), "Error response from daemon:"); ok &&
			(strings.Contains(daemon, "No such container") || strings.Contains(daemon, "is not running")) {
			return "", errOracleProfileDown
		}
		return "", fmt.Errorf("devctl: oracle: sqlplus exit %d: %s", code, oracleErrorLines(out))
	}
	return out, nil
}

// oracleErrorLines keeps the ORA-/SP2- lines of sqlplus output (the cause),
// or the trimmed output when there are none.
func oracleErrorLines(out string) string {
	var lines []string
	for _, l := range strings.Split(out, "\n") {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "ORA-") || strings.HasPrefix(l, "SP2-") {
			lines = append(lines, l)
		}
	}
	if len(lines) == 0 {
		return strings.TrimSpace(out)
	}
	return strings.Join(lines, "; ")
}

// ------------------------------------------------------------------- state

// sqlOracleState reads, in the PDB, what the U9a client reads: V$DATABASE's
// log mode and supplemental logging (CDB-wide values), the captured table's
// log groups, and the LogMiner user's system privileges, roles and table
// privileges. Each row is tagged so the parser needs no column positions.
const sqlOracleState = "ALTER SESSION SET CONTAINER = " + oraclePDB + ";\n" +
	"SELECT 'db|' || log_mode || '|' || supplemental_log_data_min || '|' || supplemental_log_data_all FROM v$database;\n" +
	"SELECT 'tbl|' || owner || '.' || table_name || '|' || log_group_type || '|' || always FROM dba_log_groups WHERE owner = 'PIPEROOM' AND table_name = 'ORDERS';\n" +
	"SELECT 'user|' || username FROM dba_users WHERE username = '" + oracleLogMinerUser + "';\n" +
	"SELECT 'sys|' || privilege FROM dba_sys_privs WHERE grantee = '" + oracleLogMinerUser + "';\n" +
	"SELECT 'role|' || granted_role FROM dba_role_privs WHERE grantee = '" + oracleLogMinerUser + "';\n" +
	"SELECT 'tab|' || owner || '.' || table_name || '|' || privilege FROM dba_tab_privs WHERE grantee = '" + oracleLogMinerUser + "' AND owner = 'PIPEROOM' AND table_name = 'ORDERS';"

// oracleState is the prerequisite state in the U9a client's types.
type oracleState struct {
	DB         oracle.DatabaseInfo
	Tables     []oracle.TableSupplementalLogging
	UserExists bool
	Grants     oracle.LogMinerGrants
}

// readOracleState runs sqlOracleState and parses it.
func (s *stack) readOracleState(ctx context.Context) (oracleState, error) {
	out, err := s.oracleSQL(ctx, oracleQueryTimeout, sqlOracleState)
	if err != nil {
		return oracleState{}, err
	}
	return parseOracleState(out)
}

// parseOracleState turns the tagged rows into the U9a types. The captured
// table always gets an entry (no row = no ALL COLUMNS group), the way
// TableSupplementalLogging reports a table without log groups.
func parseOracleState(out string) (oracleState, error) {
	st := oracleState{Grants: oracle.LogMinerGrants{User: oracleLogMinerUser}}
	tbl := oracle.TableSupplementalLogging{Table: oracleCapturedTable}
	sawDB := false
	for _, line := range strings.Split(out, "\n") {
		f := strings.Split(strings.TrimSpace(line), "|")
		switch {
		case f[0] == "db" && len(f) == 4:
			sawDB = true
			st.DB.LogMode = f[1]
			st.DB.Supplemental.Minimal = f[2] == "YES" // as the U9a client reads it (IMPLICIT is not YES)
			st.DB.Supplemental.All = f[3] == "YES"
		case f[0] == "tbl" && len(f) == 4:
			// OR across groups, as the U9a client does: a conditional group
			// cannot mask an unconditional one.
			if f[1] == oracleCapturedTable && f[2] == "ALL COLUMN LOGGING" {
				tbl.AllColumns = true
				if f[3] == "ALWAYS" {
					tbl.Always = true
				}
			}
		case f[0] == "user" && len(f) == 2:
			st.UserExists = f[1] == oracleLogMinerUser
		case f[0] == "sys" && len(f) == 2:
			st.Grants.SysPrivs = append(st.Grants.SysPrivs, f[1])
		case f[0] == "role" && len(f) == 2:
			st.Grants.Roles = append(st.Grants.Roles, f[1])
		case f[0] == "tab" && len(f) == 3:
			st.Grants.TablePrivs = append(st.Grants.TablePrivs, oracle.TablePrivilege{Table: f[1], Privilege: f[2]})
		}
	}
	if !sawDB {
		return oracleState{}, fmt.Errorf("devctl: oracle: no V$DATABASE row in the state read: %s", oracleErrorLines(out))
	}
	st.Tables = []oracle.TableSupplementalLogging{tbl}
	return st, nil
}

// ------------------------------------------------------------------ checks

// oracleArchiveLogCheck is the oracle-archive-log-mode check on the U9a
// client's DatabaseInfo: LogMiner reads archived redo, so the database must
// run in ARCHIVELOG mode.
func oracleArchiveLogCheck(info oracle.DatabaseInfo) (bool, string) {
	if info.LogMode != "ARCHIVELOG" {
		return false, "LOG_MODE=" + info.LogMode + "; LogMiner needs ARCHIVELOG (SHUTDOWN IMMEDIATE; STARTUP MOUNT; ALTER DATABASE ARCHIVELOG; ALTER DATABASE OPEN)"
	}
	return true, "LOG_MODE=ARCHIVELOG"
}

// oracleSupplementalCheck is the oracle-supplemental-logging check on the U9a
// client's DatabaseInfo and TableSupplementalLogging: database-level minimal
// supplemental logging, and for every captured table an unconditional ALL
// COLUMNS log group (or database-level ALL, which the connector also accepts).
func oracleSupplementalCheck(info oracle.DatabaseInfo, tables []oracle.TableSupplementalLogging) (bool, string) {
	var missing []string
	if !info.Supplemental.Minimal {
		missing = append(missing, "database-level minimal supplemental logging (SUPPLEMENTAL_LOG_DATA_MIN=NO)")
	}
	if !info.Supplemental.All {
		for _, t := range tables {
			if !t.AllColumns || !t.Always {
				missing = append(missing, "an ALL COLUMNS supplemental log group on "+t.Table)
			}
		}
	}
	if len(missing) > 0 {
		return false, "missing " + strings.Join(missing, ", ")
	}
	return true, "minimal supplemental logging on, ALL COLUMNS logged for the captured tables"
}

// oracleGrantsCheck is the oracle-logminer-grants check on the U9a client's
// LogMinerGrants: the user exists and holds every system privilege and role
// the U9a writer grants, plus SELECT on each captured table.
func oracleGrantsCheck(exists bool, g oracle.LogMinerGrants, tables []string) (bool, string) {
	if !exists {
		return false, "LogMiner user " + g.User + " does not exist"
	}
	var missing []string
	for _, p := range oracleRequiredSysPrivs {
		if !slices.Contains(g.SysPrivs, p) {
			missing = append(missing, p)
		}
	}
	for _, r := range oracleRequiredRoles {
		if !slices.Contains(g.Roles, r) {
			missing = append(missing, r)
		}
	}
	for _, t := range tables {
		ok := false
		for _, tp := range g.TablePrivs {
			if tp.Table == t && tp.Privilege == oracleRequiredTablePriv {
				ok = true
			}
		}
		if !ok {
			missing = append(missing, oracleRequiredTablePriv+" on "+t)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return false, g.User + " is missing " + strings.Join(missing, ", ")
	}
	return true, g.User + " holds the LogMiner grants"
}

func verifyOracleArchiveLog(ctx context.Context, s *stack) (bool, string, error) {
	st, err := s.readOracleState(ctx)
	if err != nil {
		return false, "", err
	}
	ok, detail := oracleArchiveLogCheck(st.DB)
	return ok, detail, nil
}

func verifyOracleSupplementalLogging(ctx context.Context, s *stack) (bool, string, error) {
	st, err := s.readOracleState(ctx)
	if err != nil {
		return false, "", err
	}
	ok, detail := oracleSupplementalCheck(st.DB, st.Tables)
	return ok, detail, nil
}

func verifyOracleLogMinerGrants(ctx context.Context, s *stack) (bool, string, error) {
	st, err := s.readOracleState(ctx)
	if err != nil {
		return false, "", err
	}
	ok, detail := oracleGrantsCheck(st.UserExists, st.Grants, []string{oracleCapturedTable})
	return ok, detail, nil
}

// ------------------------------------------------------------ break and fix

// The break and fix scripts. A log mode change needs a MOUNTed instance, so
// both restart the database (SHUTDOWN IMMEDIATE ends every session; the
// container keeps running and its healthcheck recovers once the PDB is open
// again). The supplemental logging scripts change only what is there to
// change, so a half-applied run can be repeated. ARCHIVELOG keeps the recovery
// area dev/oracle/cdc-setup.sh set in the spfile.
const (
	sqlOracleNoArchiveLog = "SHUTDOWN IMMEDIATE\n" +
		"STARTUP MOUNT\n" +
		"ALTER DATABASE NOARCHIVELOG;\n" +
		"ALTER DATABASE OPEN;\n" +
		"ALTER PLUGGABLE DATABASE ALL OPEN;"
	sqlOracleArchiveLog = "SHUTDOWN IMMEDIATE\n" +
		"STARTUP MOUNT\n" +
		"ALTER DATABASE ARCHIVELOG;\n" +
		"ALTER DATABASE OPEN;\n" +
		"ALTER PLUGGABLE DATABASE ALL OPEN;"

	// The table's log group goes first: it lives in the PDB, and the
	// database-level setting is changed from the root afterwards.
	sqlOracleDropSupplemental = "ALTER SESSION SET CONTAINER = " + oraclePDB + ";\n" +
		"DECLARE n NUMBER; BEGIN\n" +
		"  SELECT COUNT(*) INTO n FROM dba_log_groups WHERE owner = 'PIPEROOM' AND table_name = 'ORDERS' AND log_group_type = 'ALL COLUMN LOGGING';\n" +
		"  IF n > 0 THEN EXECUTE IMMEDIATE 'ALTER TABLE PIPEROOM.ORDERS DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS'; END IF;\n" +
		"END;\n/\n" +
		"ALTER SESSION SET CONTAINER = CDB$ROOT;\n" +
		"DECLARE v VARCHAR2(8); BEGIN\n" +
		"  SELECT supplemental_log_data_min INTO v FROM v$database;\n" +
		"  IF v <> 'NO' THEN EXECUTE IMMEDIATE 'ALTER DATABASE DROP SUPPLEMENTAL LOG DATA'; END IF;\n" +
		"END;\n/"
	sqlOracleAddSupplemental = "DECLARE v VARCHAR2(8); BEGIN\n" +
		"  SELECT supplemental_log_data_min INTO v FROM v$database;\n" +
		"  IF v = 'NO' THEN EXECUTE IMMEDIATE 'ALTER DATABASE ADD SUPPLEMENTAL LOG DATA'; END IF;\n" +
		"END;\n/\n" +
		"ALTER SESSION SET CONTAINER = " + oraclePDB + ";\n" +
		"DECLARE n NUMBER; BEGIN\n" +
		"  SELECT COUNT(*) INTO n FROM dba_log_groups WHERE owner = 'PIPEROOM' AND table_name = 'ORDERS' AND log_group_type = 'ALL COLUMN LOGGING' AND always = 'ALWAYS';\n" +
		"  IF n = 0 THEN EXECUTE IMMEDIATE 'ALTER TABLE PIPEROOM.ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS'; END IF;\n" +
		"END;\n/"

	// LOGMINING is the privilege without which DBMS_LOGMNR refuses to start
	// (ORA-01031), the one a setup run most often finds missing.
	sqlOracleRevokeLogMining = "REVOKE LOGMINING FROM " + oracleLogMinerUser + " CONTAINER=ALL;"
	// The fix grants every item the check reads, not only LOGMINING, so it
	// also repairs a user that lost another one. Grants are idempotent.
	sqlOracleGrantLogMiner = "GRANT CREATE SESSION, SET CONTAINER, FLASHBACK ANY TABLE, SELECT ANY TRANSACTION, LOGMINING TO " + oracleLogMinerUser + " CONTAINER=ALL;\n" +
		"GRANT SELECT_CATALOG_ROLE TO " + oracleLogMinerUser + " CONTAINER=ALL;\n" +
		"ALTER SESSION SET CONTAINER = " + oraclePDB + ";\n" +
		"GRANT SELECT ON PIPEROOM.ORDERS TO " + oracleLogMinerUser + ";"
)

func breakOracleNoArchiveLog(ctx context.Context, s *stack) error {
	if _, err := s.oracleSQL(ctx, oracleRestartTimeout, sqlOracleNoArchiveLog); err != nil {
		return fmt.Errorf("restarting the database in NOARCHIVELOG mode: %w", err)
	}
	s.log.Info("broken: the database restarted in NOARCHIVELOG mode (the setup check " + checkOracleArchiveLog + " fails)")
	return nil
}

func fixOracleNoArchiveLog(ctx context.Context, s *stack) error {
	if _, err := s.oracleSQL(ctx, oracleRestartTimeout, sqlOracleArchiveLog); err != nil {
		return fmt.Errorf("restarting the database in ARCHIVELOG mode: %w", err)
	}
	s.log.Info("fixed: the database restarted in ARCHIVELOG mode")
	return nil
}

func breakOracleNoSupplementalLogging(ctx context.Context, s *stack) error {
	if _, err := s.oracleSQL(ctx, oracleQueryTimeout, sqlOracleDropSupplemental); err != nil {
		return fmt.Errorf("dropping supplemental logging: %w", err)
	}
	s.log.Info("broken: " + oracleCapturedTable + " has no ALL COLUMNS log group and minimal supplemental logging is off (the setup check " + checkOracleSupplementalLogging + " fails)")
	return nil
}

func fixOracleNoSupplementalLogging(ctx context.Context, s *stack) error {
	if _, err := s.oracleSQL(ctx, oracleQueryTimeout, sqlOracleAddSupplemental); err != nil {
		return fmt.Errorf("adding supplemental logging: %w", err)
	}
	s.log.Info("fixed: minimal supplemental logging on, " + oracleCapturedTable + " logs ALL COLUMNS")
	return nil
}

func breakOracleMissingLogMinerGrant(ctx context.Context, s *stack) error {
	if _, err := s.oracleSQL(ctx, oracleQueryTimeout, sqlOracleRevokeLogMining); err != nil {
		return fmt.Errorf("revoking LOGMINING: %w", err)
	}
	s.log.Info("broken: " + oracleLogMinerUser + " lost LOGMINING (the setup check " + checkOracleLogMinerGrants + " fails)")
	return nil
}

func fixOracleMissingLogMinerGrant(ctx context.Context, s *stack) error {
	if _, err := s.oracleSQL(ctx, oracleQueryTimeout, sqlOracleGrantLogMiner); err != nil {
		return fmt.Errorf("granting the LogMiner privileges: %w", err)
	}
	s.log.Info("fixed: " + oracleLogMinerUser + " holds the LogMiner grants again")
	return nil
}
