package main

// Unit tests for the Oracle setup breaks (setupbreaks_oracle.go): a fake
// `docker exec … sqlplus` plays the oracle profile's database, so every
// break/fix pair, the exact SQL each one issues and every check run without
// Docker. setupbreaks_oracle_integration_test.go runs the same pairs on the
// real profile and reads the checks through the U9a client.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/clients/oracle"
)

// oracleWorld is the fake Oracle database: the prerequisite state and the
// scripts sqlplus was given.
type oracleWorld struct {
	logMode     string
	supMin      string
	supAll      string
	tableGroup  bool // PIPEROOM.ORDERS has an ALL COLUMN LOGGING group
	tableAlways bool // ... and it is ALWAYS
	userExists  bool
	sysPrivs    []string
	roles       []string
	tableSelect bool
	down        bool   // the container does not exist
	failBody    string // a script body that fails with ORA-01031
	scripts     []string
	breakCalls  int
}

// healthyOracleWorld is what dev/oracle/cdc-setup.sh leaves.
func healthyOracleWorld() *oracleWorld {
	return &oracleWorld{
		logMode:     "ARCHIVELOG",
		supMin:      "YES",
		supAll:      "NO",
		tableGroup:  true,
		tableAlways: true,
		userExists:  true,
		sysPrivs: []string{"CREATE SEQUENCE", "CREATE SESSION", "CREATE TABLE", "FLASHBACK ANY TABLE", "LOCK ANY TABLE",
			"LOGMINING", "SELECT ANY DICTIONARY", "SELECT ANY TABLE", "SELECT ANY TRANSACTION", "SET CONTAINER"},
		roles:       []string{"EXECUTE_CATALOG_ROLE", "SELECT_CATALOG_ROLE"},
		tableSelect: true,
	}
}

// stateRows is what sqlOracleState prints for the world.
func (w *oracleWorld) stateRows() string {
	var b strings.Builder
	b.WriteString("db|" + w.logMode + "|" + w.supMin + "|" + w.supAll + "\n")
	if w.tableGroup {
		always := "CONDITIONAL"
		if w.tableAlways {
			always = "ALWAYS"
		}
		b.WriteString("tbl|PIPEROOM.ORDERS|ALL COLUMN LOGGING|" + always + "\n")
	}
	if w.userExists {
		b.WriteString("user|C##DBZUSER\n")
		for _, p := range w.sysPrivs {
			b.WriteString("sys|" + p + "\n")
		}
		for _, r := range w.roles {
			b.WriteString("role|" + r + "\n")
		}
		if w.tableSelect {
			b.WriteString("tab|PIPEROOM.ORDERS|SELECT\n")
		}
	}
	return b.String()
}

// exec is the fake `docker exec`: it checks the argv shape and applies the
// script the way the database would.
func (w *oracleWorld) exec(_ context.Context, container string, argv ...string) (string, int, error) {
	if container != oracleContainer {
		return "", -1, errors.New("unexpected container " + container)
	}
	if w.down {
		return "Error response from daemon: No such container: " + oracleContainer + "\n", 1, nil
	}
	if len(argv) != 5 || argv[0] != "bash" || argv[1] != "-c" || argv[2] != oracleSQLPlus || argv[3] != "_" {
		return "", -1, errors.New("unexpected argv " + strings.Join(argv, " "))
	}
	script := argv[4]
	if !strings.HasPrefix(script, oracleScriptHeader) || !strings.HasSuffix(script, "\nEXIT\n") {
		return "", -1, errors.New("script without the header or EXIT: " + script)
	}
	body := strings.TrimSuffix(strings.TrimPrefix(script, oracleScriptHeader), "\nEXIT\n")
	w.scripts = append(w.scripts, body)
	if w.failBody != "" && body == w.failBody {
		return "ERROR at line 1:\nORA-01031: insufficient privileges\nHelp: https://docs.oracle.com/error-help/db/ora-01031/\n", 1, nil
	}
	switch body {
	case sqlOracleState:
		return w.stateRows(), 0, nil
	case sqlOracleNoArchiveLog:
		w.breakCalls++
		w.logMode = "NOARCHIVELOG"
	case sqlOracleArchiveLog:
		w.logMode = "ARCHIVELOG"
	case sqlOracleDropSupplemental:
		w.breakCalls++
		w.tableGroup, w.tableAlways, w.supMin = false, false, "NO"
	case sqlOracleAddSupplemental:
		w.tableGroup, w.tableAlways, w.supMin = true, true, "YES"
	case sqlOracleRevokeLogMining:
		w.breakCalls++
		if !slices.Contains(w.sysPrivs, "LOGMINING") {
			return "ORA-01952: system privileges not granted to 'C##DBZUSER'\n", 1, nil
		}
		w.sysPrivs = slices.DeleteFunc(w.sysPrivs, func(p string) bool { return p == "LOGMINING" })
	case sqlOracleGrantLogMiner:
		for _, p := range oracleRequiredSysPrivs {
			if !slices.Contains(w.sysPrivs, p) {
				w.sysPrivs = append(w.sysPrivs, p)
			}
		}
		for _, r := range oracleRequiredRoles {
			if !slices.Contains(w.roles, r) {
				w.roles = append(w.roles, r)
			}
		}
		w.tableSelect = true
	default:
		return "", -1, errors.New("unexpected script body: " + body)
	}
	return "", 0, nil
}

func testOracleStack(w *oracleWorld) *stack {
	return &stack{
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		execInFn: w.exec,
	}
}

func TestOracleSetupScenariosAreRegistered(t *testing.T) {
	want := []string{"missing-logminer-grant", "no-archivelog", "no-supplemental-logging"}
	if got := setupScenarioNamesFor(oracleProfile); !slices.Equal(got, want) {
		t.Fatalf("oracle scenarios = %v, want %v", got, want)
	}
	for _, n := range setupScenarioNamesFor("") {
		if slices.Contains(want, n) {
			t.Errorf("%s is listed as a base-stack scenario: the base fakes and integration test would run it", n)
		}
	}
	checks := map[string]string{
		"no-archivelog":           checkOracleArchiveLog,
		"no-supplemental-logging": checkOracleSupplementalLogging,
		"missing-logminer-grant":  checkOracleLogMinerGrants,
	}
	for name, check := range checks {
		if sc := setupScenarios[name]; sc.check != check || sc.profile != oracleProfile {
			t.Errorf("%s: check %q profile %q, want %q %q", name, sc.check, sc.profile, check, oracleProfile)
		}
	}
}

// TestOracleSetupBreakFixPair: the check passes on the healthy profile, the
// break issues exactly its SQL and makes the check fail with the named
// cause, the fix makes it pass again.
func TestOracleSetupBreakFixPair(t *testing.T) {
	cases := []struct {
		scenario, breakSQL, fixSQL, detail string
	}{
		{"no-archivelog", sqlOracleNoArchiveLog, sqlOracleArchiveLog, "LOG_MODE=NOARCHIVELOG"},
		{"no-supplemental-logging", sqlOracleDropSupplemental, sqlOracleAddSupplemental,
			"missing database-level minimal supplemental logging (SUPPLEMENTAL_LOG_DATA_MIN=NO), an ALL COLUMNS supplemental log group on PIPEROOM.ORDERS"},
		{"missing-logminer-grant", sqlOracleRevokeLogMining, sqlOracleGrantLogMiner, "C##DBZUSER is missing LOGMINING"},
	}
	for _, tc := range cases {
		t.Run(tc.scenario, func(t *testing.T) {
			w := healthyOracleWorld()
			s := testOracleStack(w)
			ctx := context.Background()
			sc := setupScenarios[tc.scenario]

			if err := setupBreakCmd(ctx, s, tc.scenario); err != nil {
				t.Fatalf("setup-break: %v", err)
			}
			if want := []string{sqlOracleState, tc.breakSQL, sqlOracleState}; !slices.Equal(w.scripts, want) {
				t.Fatalf("break scripts = %q, want %q", w.scripts, want)
			}
			ok, detail, err := sc.verify(ctx, s)
			if err != nil || ok {
				t.Fatalf("check after the break: ok=%v err=%v", ok, err)
			}
			if !strings.Contains(detail, tc.detail) {
				t.Errorf("detail %q does not contain %q", detail, tc.detail)
			}

			w.scripts = nil
			if err := setupFixCmd(ctx, s, tc.scenario); err != nil {
				t.Fatalf("setup-fix: %v", err)
			}
			if want := []string{sqlOracleState, tc.fixSQL, sqlOracleState}; !slices.Equal(w.scripts, want) {
				t.Fatalf("fix scripts = %q, want %q", w.scripts, want)
			}
			if ok, detail, err := sc.verify(ctx, s); err != nil || !ok {
				t.Fatalf("check after the fix: ok=%v %s err=%v", ok, detail, err)
			}
		})
	}
}

// TestOracleSetupBreakFixIdempotent: a second break issues no SQL beyond the
// check, and a second fix likewise.
func TestOracleSetupBreakFixIdempotent(t *testing.T) {
	for _, name := range setupScenarioNamesFor(oracleProfile) {
		t.Run(name, func(t *testing.T) {
			w := healthyOracleWorld()
			s := testOracleStack(w)
			ctx := context.Background()
			for i := 0; i < 2; i++ {
				if err := setupBreakCmd(ctx, s, name); err != nil {
					t.Fatalf("break %d: %v", i+1, err)
				}
			}
			if w.breakCalls != 1 {
				t.Fatalf("break SQL ran %d times, want once", w.breakCalls)
			}
			for i := 0; i < 2; i++ {
				if err := setupFixCmd(ctx, s, name); err != nil {
					t.Fatalf("fix %d: %v", i+1, err)
				}
			}
			changes := slices.DeleteFunc(slices.Clone(w.scripts), func(b string) bool { return b == sqlOracleState })
			if len(changes) != 2 {
				t.Fatalf("scripts that change the database = %q, want one break and one fix", changes)
			}
			if ok, detail, err := setupScenarios[name].verify(ctx, s); err != nil || !ok {
				t.Fatalf("check after two fixes: ok=%v %s err=%v", ok, detail, err)
			}
		})
	}
}

// TestOracleBreakSQL pins the statements, so the scenario docs and the U9d
// evals agree with what the stack is told.
func TestOracleBreakSQL(t *testing.T) {
	inOrder := func(t *testing.T, sql string, parts ...string) {
		t.Helper()
		rest := sql
		for _, p := range parts {
			i := strings.Index(rest, p)
			if i < 0 {
				t.Fatalf("%q not found (in order) in:\n%s", p, sql)
			}
			rest = rest[i+len(p):]
		}
	}
	inOrder(t, sqlOracleNoArchiveLog, "SHUTDOWN IMMEDIATE\n", "STARTUP MOUNT\n", "ALTER DATABASE NOARCHIVELOG;", "ALTER DATABASE OPEN;", "ALTER PLUGGABLE DATABASE ALL OPEN;")
	inOrder(t, sqlOracleArchiveLog, "SHUTDOWN IMMEDIATE\n", "STARTUP MOUNT\n", "ALTER DATABASE ARCHIVELOG;", "ALTER DATABASE OPEN;", "ALTER PLUGGABLE DATABASE ALL OPEN;")
	inOrder(t, sqlOracleDropSupplemental, "ALTER SESSION SET CONTAINER = FREEPDB1;",
		"'ALTER TABLE PIPEROOM.ORDERS DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS'", "ALTER SESSION SET CONTAINER = CDB$ROOT;",
		"'ALTER DATABASE DROP SUPPLEMENTAL LOG DATA'")
	inOrder(t, sqlOracleAddSupplemental, "'ALTER DATABASE ADD SUPPLEMENTAL LOG DATA'", "ALTER SESSION SET CONTAINER = FREEPDB1;",
		"'ALTER TABLE PIPEROOM.ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS'")
	if sqlOracleRevokeLogMining != "REVOKE LOGMINING FROM C##DBZUSER CONTAINER=ALL;" {
		t.Errorf("revoke = %q", sqlOracleRevokeLogMining)
	}
	for _, p := range oracleRequiredSysPrivs {
		if !strings.Contains(sqlOracleGrantLogMiner, p) {
			t.Errorf("the grant fix does not grant %s", p)
		}
	}
	inOrder(t, sqlOracleGrantLogMiner, "GRANT SELECT_CATALOG_ROLE TO C##DBZUSER CONTAINER=ALL;", "ALTER SESSION SET CONTAINER = FREEPDB1;", "GRANT SELECT ON PIPEROOM.ORDERS TO C##DBZUSER;")
	// PL/SQL blocks end with "/" on its own line, or sqlplus never runs them.
	for name, sql := range map[string]string{"drop": sqlOracleDropSupplemental, "add": sqlOracleAddSupplemental} {
		if strings.Count(sql, "END;\n/") != 2 {
			t.Errorf("%s: want two terminated PL/SQL blocks:\n%s", name, sql)
		}
	}
}

// TestOracleProfileDown: without the container, break and fix stop with the
// way to start the profile, and change nothing.
func TestOracleProfileDown(t *testing.T) {
	for _, name := range setupScenarioNamesFor(oracleProfile) {
		w := healthyOracleWorld()
		w.down = true
		s := testOracleStack(w)
		err := setupBreakCmd(context.Background(), s, name)
		if !errors.Is(err, errOracleProfileDown) || !strings.Contains(err.Error(), "--profile oracle up -d --wait oracle") {
			t.Errorf("%s: break with the profile down = %v", name, err)
		}
		if err := setupFixCmd(context.Background(), s, name); !errors.Is(err, errOracleProfileDown) {
			t.Errorf("%s: fix with the profile down = %v", name, err)
		}
	}
}

// TestOracleSQLErrors: sqlplus failures and docker failures are errors that
// carry the ORA- cause; a check never passes on an unreadable database.
func TestOracleSQLErrors(t *testing.T) {
	t.Run("break refused by the database", func(t *testing.T) {
		w := healthyOracleWorld()
		w.failBody = sqlOracleRevokeLogMining
		err := setupBreakCmd(context.Background(), testOracleStack(w), "missing-logminer-grant")
		if err == nil || !strings.Contains(err.Error(), "ORA-01031: insufficient privileges") || strings.Contains(err.Error(), "Help:") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("state read refused", func(t *testing.T) {
		w := healthyOracleWorld()
		w.failBody = sqlOracleState
		for _, name := range setupScenarioNamesFor(oracleProfile) {
			if ok, _, err := setupScenarios[name].verify(context.Background(), testOracleStack(w)); err == nil || ok {
				t.Errorf("%s: check with the read refused: ok=%v err=%v", name, ok, err)
			}
		}
	})
	t.Run("docker cannot run", func(t *testing.T) {
		s := &stack{log: slog.New(slog.NewTextHandler(io.Discard, nil)), execInFn: func(context.Context, string, ...string) (string, int, error) {
			return "", -1, errors.New("docker: executable file not found")
		}}
		if _, err := s.readOracleState(context.Background()); err == nil || !strings.Contains(err.Error(), "executable file not found") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("no V$DATABASE row", func(t *testing.T) {
		if _, err := parseOracleState("SP2-0640: Not connected\n"); err == nil || !strings.Contains(err.Error(), "SP2-0640") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestParseOracleState(t *testing.T) {
	st, err := parseOracleState(healthyOracleWorld().stateRows() + "tbl|OTHER.T|ALL COLUMN LOGGING|ALWAYS\nnoise\n")
	if err != nil {
		t.Fatal(err)
	}
	if st.DB.LogMode != "ARCHIVELOG" || !st.DB.Supplemental.Minimal || st.DB.Supplemental.All {
		t.Errorf("db = %+v", st.DB)
	}
	want := []oracle.TableSupplementalLogging{{Table: "PIPEROOM.ORDERS", AllColumns: true, Always: true}}
	if !slices.Equal(st.Tables, want) {
		t.Errorf("tables = %+v, want %+v", st.Tables, want)
	}
	if !st.UserExists || st.Grants.User != "C##DBZUSER" || !slices.Contains(st.Grants.SysPrivs, "LOGMINING") ||
		!slices.Equal(st.Grants.Roles, []string{"EXECUTE_CATALOG_ROLE", "SELECT_CATALOG_ROLE"}) ||
		!slices.Equal(st.Grants.TablePrivs, []oracle.TablePrivilege{{Table: "PIPEROOM.ORDERS", Privilege: "SELECT"}}) {
		t.Errorf("grants = %+v (user %v)", st.Grants, st.UserExists)
	}

	// No log group row: the table is reported without ALL COLUMNS, the way
	// the U9a client reports a table without log groups.
	st, err = parseOracleState("db|NOARCHIVELOG|IMPLICIT|NO\n")
	if err != nil {
		t.Fatal(err)
	}
	if st.DB.Supplemental.Minimal || st.UserExists || len(st.Tables) != 1 || st.Tables[0].AllColumns {
		t.Errorf("state = %+v", st)
	}
}

func TestOracleChecks(t *testing.T) {
	healthy := func() oracleState {
		st, err := parseOracleState(healthyOracleWorld().stateRows())
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	tables := []string{oracleCapturedTable}
	cases := []struct {
		name   string
		edit   func(*oracleState)
		check  string
		ok     bool
		detail string
	}{
		{"healthy archive", nil, checkOracleArchiveLog, true, "LOG_MODE=ARCHIVELOG"},
		{"noarchivelog", func(s *oracleState) { s.DB.LogMode = "NOARCHIVELOG" }, checkOracleArchiveLog, false, "LOG_MODE=NOARCHIVELOG"},
		{"healthy supplemental", nil, checkOracleSupplementalLogging, true, "ALL COLUMNS logged"},
		{"no minimal", func(s *oracleState) { s.DB.Supplemental.Minimal = false }, checkOracleSupplementalLogging, false, "SUPPLEMENTAL_LOG_DATA_MIN=NO"},
		{"conditional group", func(s *oracleState) { s.Tables[0].Always = false }, checkOracleSupplementalLogging, false, "ALL COLUMNS supplemental log group on PIPEROOM.ORDERS"},
		{"database-level ALL covers the table", func(s *oracleState) {
			s.Tables[0] = oracle.TableSupplementalLogging{Table: oracleCapturedTable}
			s.DB.Supplemental.All = true
		}, checkOracleSupplementalLogging, true, ""},
		{"healthy grants", nil, checkOracleLogMinerGrants, true, "holds the LogMiner grants"},
		{"no user", func(s *oracleState) { s.UserExists = false }, checkOracleLogMinerGrants, false, "C##DBZUSER does not exist"},
		{"no LOGMINING", func(s *oracleState) {
			s.Grants.SysPrivs = slices.DeleteFunc(s.Grants.SysPrivs, func(p string) bool { return p == "LOGMINING" })
		}, checkOracleLogMinerGrants, false, "missing LOGMINING"},
		{"no role, no table select", func(s *oracleState) { s.Grants.Roles, s.Grants.TablePrivs = nil, nil }, checkOracleLogMinerGrants, false,
			"missing SELECT on PIPEROOM.ORDERS, SELECT_CATALOG_ROLE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := healthy()
			if tc.edit != nil {
				tc.edit(&st)
			}
			var ok bool
			var detail string
			switch tc.check {
			case checkOracleArchiveLog:
				ok, detail = oracleArchiveLogCheck(st.DB)
			case checkOracleSupplementalLogging:
				ok, detail = oracleSupplementalCheck(st.DB, st.Tables)
			default:
				ok, detail = oracleGrantsCheck(st.UserExists, st.Grants, tables)
			}
			if ok != tc.ok || !strings.Contains(detail, tc.detail) {
				t.Errorf("ok=%v detail=%q, want ok=%v containing %q", ok, detail, tc.ok, tc.detail)
			}
		})
	}
}

// TestOracleSetupScriptGrantsWhatTheCheckReads keeps dev/oracle/cdc-setup.sh
// (the healthy state) and the checks in step: every item the grants check
// requires is granted there, and the archive log and supplemental logging
// steps exist.
func TestOracleSetupScriptGrantsWhatTheCheckReads(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "dev", "oracle", "cdc-setup.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := strings.ToUpper(string(raw))
	for _, p := range oracleRequiredSysPrivs {
		if !strings.Contains(script, "GRANT "+p+" TO C##DBZUSER CONTAINER=ALL;") {
			t.Errorf("cdc-setup.sh does not grant %s to C##DBZUSER in every container", p)
		}
	}
	for _, r := range oracleRequiredRoles {
		if !strings.Contains(script, "GRANT "+r+" TO C##DBZUSER CONTAINER=ALL;") {
			t.Errorf("cdc-setup.sh does not grant %s", r)
		}
	}
	for _, want := range []string{
		"GRANT SELECT ON PIPEROOM.ORDERS TO C##DBZUSER;",
		"ALTER DATABASE ARCHIVELOG;",
		"STARTUP MOUNT",
		"'ALTER DATABASE ADD SUPPLEMENTAL LOG DATA'",
		"'ALTER TABLE PIPEROOM.ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS'",
		"ALTER SESSION SET CONTAINER = ${PDB};",
		"PDB=\"${PIPEROOM_ORACLE_PDB:-" + oraclePDB + "}\"",
	} {
		if !strings.Contains(script, strings.ToUpper(want)) {
			t.Errorf("cdc-setup.sh lacks %q", want)
		}
	}
	if !strings.Contains(string(raw), "set -euo pipefail") {
		t.Error("cdc-setup.sh must stop on the first error (the container start fails with it)")
	}
}

// TestComposeOracleProfile: the service starts only with its profile, the
// image is an exact release with its digest, the setup script is mounted
// where the init hook runs it, and it reports health.
func TestComposeOracleProfile(t *testing.T) {
	root, svcs := readQ3Compose(t)
	o, ok := svcs["oracle"]
	if !ok {
		t.Fatal("no oracle service in dev/docker-compose.yml")
	}
	if !slices.Equal(o.Profiles, []string{oracleProfile}) {
		t.Errorf("profiles = %v, want [oracle]: `devctl up` must not start it", o.Profiles)
	}
	// ${ORACLE_IMAGE:-<default>}: the default is what `up` runs.
	def, ok := strings.CutPrefix(o.Image, "${ORACLE_IMAGE:-")
	if !ok || !strings.HasSuffix(def, "}") {
		t.Fatalf("image %q: want ${ORACLE_IMAGE:-<pinned default>}", o.Image)
	}
	ref, digest, _ := strings.Cut(strings.TrimSuffix(def, "}"), "@")
	if !strings.HasPrefix(ref, "gvenzl/oracle-free:") || !pinned(ref) || strings.Contains(ref, "slim") {
		t.Errorf("image %q: want an exact, non-slim gvenzl/oracle-free release", o.Image)
	}
	if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64 {
		t.Errorf("image %q: want the index digest", o.Image)
	}
	if !slices.Equal(o.Healthcheck.Test, []string{"CMD", "healthcheck.sh"}) {
		t.Errorf("healthcheck = %v", o.Healthcheck.Test)
	}
	if !slices.Equal(o.Ports, []string{"127.0.0.1:1521:1521"}) {
		t.Errorf("ports = %v, want loopback 1521 only", o.Ports)
	}
	if o.Environment["ORACLE_PASSWORD"] != "piperoom-dev" {
		t.Errorf("ORACLE_PASSWORD = %q, want the dev stack's", o.Environment["ORACLE_PASSWORD"])
	}
	wantVolumes := []string{
		"./oracle/cdc-setup.sh:/opt/piperoom/oracle/cdc-setup.sh:ro",
		"./oracle/initdb:/container-entrypoint-initdb.d:ro",
	}
	if !slices.Equal(o.Volumes, wantVolumes) {
		t.Errorf("volumes = %v, want %v", o.Volumes, wantVolumes)
	}
	for _, v := range o.Volumes {
		src, _, _ := strings.Cut(v, ":")
		if _, err := os.Stat(filepath.Join(root, "dev", src)); err != nil {
			t.Errorf("mount source %s: %v", src, err)
		}
	}
	hook, err := os.ReadFile(filepath.Join(root, "dev", "oracle", "initdb", "01-cdc-setup.sh"))
	if err != nil {
		t.Fatal(err)
	}
	// The entrypoint sources the hook: the work must run in a child bash
	// (an `exit` in a sourced file would end the entrypoint).
	if !strings.Contains(string(hook), "\nbash /opt/piperoom/oracle/cdc-setup.sh\n") {
		t.Errorf("01-cdc-setup.sh does not run the mounted script in its own bash:\n%s", hook)
	}
}
