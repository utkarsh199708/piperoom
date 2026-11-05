package oracle

import (
	"context"
	"errors"
	"strings"
	"testing"

	oraclient "piperoom/internal/clients/oracle"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/remoteclients"
)

// fakeReader answers the driver's reads from fields; err fails every call.
type fakeReader struct {
	db     oraclient.DatabaseInfo
	tables []oraclient.TableSupplementalLogging
	grants oraclient.LogMinerGrants
	err    error
	calls  []string
}

func (f *fakeReader) Database(context.Context) (oraclient.DatabaseInfo, error) {
	f.calls = append(f.calls, "database")
	return f.db, f.err
}

func (f *fakeReader) TableSupplementalLogging(_ context.Context, tables []string) ([]oraclient.TableSupplementalLogging, error) {
	f.calls = append(f.calls, "tables")
	return f.tables, f.err
}

func (f *fakeReader) LogMinerGrants(context.Context, string, []string) (oraclient.LogMinerGrants, error) {
	f.calls = append(f.calls, "grants")
	return f.grants, f.err
}

// readyReader is the evidence of the dev profile's Oracle Free 23ai CDB after
// dev/oracle/cdc-setup.sh: every check passes.
func readyReader() *fakeReader {
	return &fakeReader{
		db: oraclient.DatabaseInfo{
			Name: "FREE", IsCDB: true, Container: "FREEPDB1", PDBs: []string{"FREEPDB1"},
			LogMode: "ARCHIVELOG", ForceLog: "NO",
			Supplemental: oraclient.SupplementalLogging{Minimal: true},
			Version:      "Oracle Database 23ai Free Release 23.0.0.0.0 - Develop, Learn, and Run for Free",
			Edition:      "Free",
		},
		tables: []oraclient.TableSupplementalLogging{{Table: "PIPEROOM.ORDERS", AllColumns: true, Always: true}},
		grants: oraclient.LogMinerGrants{
			User:       "C##DBZUSER",
			Roles:      []string{"SELECT_CATALOG_ROLE"},
			SysPrivs:   []string{"CREATE SESSION", "FLASHBACK ANY TABLE", "LOGMINING", "SELECT ANY TRANSACTION", "SET CONTAINER"},
			TablePrivs: []oraclient.TablePrivilege{{Table: "PIPEROOM.ORDERS", Privilege: "SELECT"}},
		},
	}
}

func testDriver() Driver {
	return Driver{User: "C##DBZUSER", Tables: []string{"PIPEROOM.ORDERS"}, PDB: "FREEPDB1"}
}

func resultOf(t *testing.T, results []Result, id string) Result {
	t.Helper()
	for _, r := range results {
		if r.Check.ID == id {
			return r
		}
	}
	t.Fatalf("no %s result in %+v", id, results)
	return Result{}
}

func checkOf(t *testing.T, d Driver, id string) Check {
	t.Helper()
	for _, c := range d.Checks() {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("no %s check", id)
	return Check{}
}

func TestEveryCheckPassesOnAReadySource(t *testing.T) {
	f := readyReader()
	results, err := testDriver().Run(context.Background(), f)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(results) != len(testDriver().Checks()) {
		t.Fatalf("got %d results, want %d", len(results), len(testDriver().Checks()))
	}
	for _, r := range results {
		if !r.OK {
			t.Errorf("%s failed on a ready source: %v", r.Check.ID, r.Err)
		}
	}
	// Each read runs once per Run, however many checks judge it.
	count := map[string]int{}
	for _, c := range f.calls {
		count[c]++
	}
	for _, read := range []string{"database", "tables", "grants"} {
		if count[read] != 1 {
			t.Errorf("%s read %d times, want once: %v", read, count[read], f.calls)
		}
	}
}

func TestNonCDBPasses(t *testing.T) {
	f := readyReader()
	f.db.IsCDB, f.db.PDBs, f.db.Name = false, nil, "ORCL"
	f.db.Version = "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production"
	f.db.Edition = "Enterprise Edition"
	f.grants.SysPrivs = []string{"CREATE SESSION", "FLASHBACK ANY TABLE", "LOGMINING", "SELECT ANY TRANSACTION"}
	d := Driver{User: "DBZUSER", Tables: []string{"PIPEROOM.ORDERS"}}
	results, err := d.Run(context.Background(), f)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, r := range results {
		if !r.OK {
			t.Errorf("%s failed on a ready 19c non-CDB: %v", r.Check.ID, r.Err)
		}
	}
}

func TestChecksPassAndFail(t *testing.T) {
	tests := []struct {
		name   string
		id     string
		d      func(*Driver)
		mutate func(*fakeReader)
		fix    string // "" = an ask
		tier   string
		ask    []string // substrings the ask must carry
	}{
		{"noarchivelog", CheckArchiveLog, nil, func(f *fakeReader) { f.db.LogMode = "NOARCHIVELOG" }, "", plan.TierT3,
			[]string{"maintenance window", "STARTUP MOUNT", "ALTER DATABASE ARCHIVELOG"}},
		{"no minimal supplemental logging", CheckSupplementalDatabase, nil, func(f *fakeReader) { f.db.Supplemental.Minimal = false }, "", plan.TierT2,
			[]string{"ALTER DATABASE ADD SUPPLEMENTAL LOG DATA"}},
		{"table without a log group", CheckSupplementalTables, nil, func(f *fakeReader) { f.tables[0].AllColumns, f.tables[0].Always = false, false }, opCreatePublication, plan.TierT2, nil},
		{"conditional log group", CheckSupplementalTables, nil, func(f *fakeReader) { f.tables[0].Always = false }, opCreatePublication, plan.TierT2, nil},
		{"table missing from the read", CheckSupplementalTables, nil, func(f *fakeReader) { f.tables = nil }, opCreatePublication, plan.TierT2, nil},
		{"CDB without a PDB", CheckContainer, func(d *Driver) { d.PDB = "" }, func(*fakeReader) {}, "", plan.TierT0,
			[]string{"database.pdb.name", "FREEPDB1"}},
		{"unknown PDB", CheckContainer, func(d *Driver) { d.PDB = "OTHERPDB" }, func(*fakeReader) {}, "", plan.TierT0,
			[]string{"OTHERPDB", "FREEPDB1"}},
		{"PDB on a non-CDB", CheckContainer, nil, func(f *fakeReader) { f.db.IsCDB, f.db.PDBs = false, nil }, "", plan.TierT0,
			[]string{"not a CDB"}},
		{"local user in a CDB", CheckCommonUser, func(d *Driver) { d.User = "DBZUSER" }, func(*fakeReader) {}, "", plan.TierT0,
			[]string{"C##", "CONTAINER=ALL"}},
		{"missing LOGMINING", CheckLogMinerGrants, nil, func(f *fakeReader) {
			f.grants.SysPrivs = []string{"CREATE SESSION", "FLASHBACK ANY TABLE", "SELECT ANY TRANSACTION", "SET CONTAINER"}
		}, "", plan.TierT2, []string{`GRANT LOGMINING TO "C##DBZUSER" CONTAINER=ALL`, "CDB$ROOT"}},
		{"missing SELECT_CATALOG_ROLE", CheckLogMinerGrants, nil, func(f *fakeReader) { f.grants.Roles = nil }, "", plan.TierT2,
			[]string{`GRANT SELECT_CATALOG_ROLE TO "C##DBZUSER" CONTAINER=ALL`}},
		{"no SELECT on the table", CheckTableGrants, nil, func(f *fakeReader) { f.grants.TablePrivs = nil }, "", plan.TierT2,
			[]string{`GRANT SELECT ON "PIPEROOM"."ORDERS" TO "C##DBZUSER"`, "FREEPDB1"}},
		{"user exists without CREATE SESSION", CheckLogMinerUser, nil, func(f *fakeReader) { f.grants.SysPrivs = []string{"LOGMINING"} }, "", plan.TierT2,
			[]string{"GRANT CREATE SESSION", "never re-passworded"}},
		{"common user missing", CheckLogMinerUser, nil, func(f *fakeReader) { f.grants = oraclient.LogMinerGrants{User: "C##DBZUSER"} }, "", plan.TierT2,
			[]string{"CREATE USER", "CONTAINER=ALL", "secret store"}},
		{"non-CDB user missing", CheckLogMinerUser, func(d *Driver) { d.PDB, d.User = "", "DBZUSER" }, func(f *fakeReader) {
			f.db.IsCDB, f.db.PDBs = false, nil
			f.grants = oraclient.LogMinerGrants{User: "DBZUSER"}
		}, opCreateReadRole, plan.TierT2, nil},
		{"standard edition 2", CheckEdition, nil, func(f *fakeReader) {
			f.db.Version = "Oracle Database 19c Standard Edition 2 Release 19.0.0.0.0 - Production"
			f.db.Edition = "Standard Edition 2"
		}, "", plan.TierT0, []string{"Standard Edition 2", "release 19", "does not cover"}},
		{"enterprise 12c", CheckEdition, nil, func(f *fakeReader) {
			f.db.Version = "Oracle Database 12c Enterprise Edition Release 12.2.0.1.0 - 64bit Production"
			f.db.Edition = "Enterprise Edition"
		}, "", plan.TierT0, []string{"release 12"}},
		{"unknown edition", CheckEdition, nil, func(f *fakeReader) { f.db.Edition = "" }, "", plan.TierT0, []string{"unknown edition"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := testDriver()
			if tc.d != nil {
				tc.d(&d)
			}
			f := readyReader()
			tc.mutate(f)
			results, err := d.Run(context.Background(), f)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			r := resultOf(t, results, tc.id)
			if r.OK {
				t.Fatalf("%s passed on %+v", tc.id, f)
			}
			if r.Err != nil {
				t.Fatalf("%s: read error: %v", tc.id, r.Err)
			}
			fixOp, ask := r.Check.Fix(d, r.Evidence)
			if fixOp != tc.fix || r.Check.TierFloor != tc.tier {
				t.Fatalf("%s fix = %q/%q, want %q/%q", tc.id, fixOp, r.Check.TierFloor, tc.fix, tc.tier)
			}
			if fixOp == "" && ask == "" {
				t.Fatalf("%s names no fix and no ask", tc.id)
			}
			for _, want := range tc.ask {
				if !strings.Contains(ask, want) {
					t.Errorf("%s ask %q does not mention %q", tc.id, ask, want)
				}
			}
		})
	}
}

// TestEditionsCovered pins the board's coverage: the free editions (Free 23ai
// and 26ai, Express Edition) and the 19c and 21c releases.
func TestEditionsCovered(t *testing.T) {
	tests := []struct {
		banner, edition string
		ok              bool
	}{
		{"Oracle Database 23ai Free Release 23.0.0.0.0 - Develop, Learn, and Run for Free", "Free", true},
		{"Oracle AI Database 26ai Free Release 23.26.0.0.0 - Develop, Learn, and Run for Free", "Free", true},
		{"Oracle Database 23c Free, Release 23.0.0.0.0 - Developer-Release", "Free", true},
		{"Oracle Database 21c Express Edition Release 21.0.0.0.0 - Production", "Express Edition", true},
		{"Oracle Database 21c Enterprise Edition Release 21.0.0.0.0 - Production", "Enterprise Edition", true},
		{"Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production", "Enterprise Edition", true},
		{"Oracle Database 18c Express Edition Release 18.0.0.0.0 - Production", "Express Edition", false},
		{"Oracle Database 19c Standard Edition 2 Release 19.0.0.0.0 - Production", "Standard Edition 2", false},
		{"Oracle Database 11g Express Edition Release 11.2.0.2.0 - 64bit Production", "Express Edition", false},
		{"Oracle Database 21c Personal Edition Release 21.0.0.0.0 - Production", "Personal Edition", false},
	}
	for _, tc := range tests {
		got := editionCovered(oraclient.DatabaseInfo{Version: tc.banner, Edition: tc.edition})
		if got != tc.ok {
			t.Errorf("%q: covered = %v, want %v", tc.banner, got, tc.ok)
		}
	}
}

// TestArchiveLogTierFollowsTheRoom: archive log mode off is a T3 check with a
// maintenance window on a database this room did not create (a restart), and
// T2 without a window on one it did; the class still raises it.
func TestArchiveLogTierFollowsTheRoom(t *testing.T) {
	c := checkOf(t, testDriver(), CheckArchiveLog)
	if c.TierFloor != plan.TierT3 || !c.MaintenanceWindow || c.FixOp != "" {
		t.Fatalf("not created by the room: %q window=%v op=%q, want T3, window, no op", c.TierFloor, c.MaintenanceWindow, c.FixOp)
	}
	for _, class := range []string{plan.ClassLocal, plan.ClassShared, plan.ClassProduction} {
		if got, err := c.Tier(class); err != nil || got != plan.TierT3 {
			t.Fatalf("Tier(%s) = %q, %v; want T3", class, got, err)
		}
	}
	d := testDriver()
	d.CreatedByRoom = true
	c = checkOf(t, d, CheckArchiveLog)
	if c.TierFloor != plan.TierT2 || c.MaintenanceWindow {
		t.Fatalf("created by the room: %q window=%v, want T2 and no window", c.TierFloor, c.MaintenanceWindow)
	}
	if strings.Contains(c.FixAsk, "inside a recorded maintenance window") {
		t.Fatalf("created by the room, the ask still asks for a window: %q", c.FixAsk)
	}
	if got, err := c.Tier(plan.ClassProduction); err != nil || got != plan.TierT3 {
		t.Fatalf("created by the room on production: %q, %v; want T3", got, err)
	}
}

func TestTierFollowsTheTargetClass(t *testing.T) {
	c := checkOf(t, testDriver(), CheckSupplementalTables)
	for _, tc := range []struct{ class, want string }{
		{plan.ClassLocal, plan.TierT2},
		{plan.ClassShared, plan.TierT2},
		{plan.ClassProduction, plan.TierT3},
	} {
		got, err := c.Tier(tc.class)
		if err != nil || got != tc.want {
			t.Fatalf("Tier(%s) = %q, %v; want %q", tc.class, got, err, tc.want)
		}
	}
	if _, err := c.Tier("nonsense"); err == nil {
		t.Fatal("Tier accepted an unknown class")
	}
}

// TestTierRoutesThroughTheGuard: a fix op the guard table forbids on a class
// is plan.ErrGuard, never a tier.
func TestTierRoutesThroughTheGuard(t *testing.T) {
	c := Check{ID: "x", FixOp: plan.OpSourceDropSlot, TierFloor: plan.TierT2}
	if _, err := c.Tier(plan.ClassProduction); !errors.Is(err, plan.ErrGuard) {
		t.Fatalf("forbidden op on production: %v, want plan.ErrGuard", err)
	}
	if got, err := c.Tier(plan.ClassShared); err != nil || got != plan.TierT2 {
		t.Fatalf("on shared: %q, %v", got, err)
	}
}

func TestCheckFixOpsAreRegistered(t *testing.T) {
	ops := map[string]bool{}
	for _, spec := range remoteclients.Ops() {
		ops[spec.Op] = true
	}
	reads := map[string]bool{readDatabase: true, readTableLogging: true, readGrants: true}
	for _, d := range []Driver{testDriver(), {User: "DBZUSER", Tables: []string{"PIPEROOM.ORDERS"}, CreatedByRoom: true}} {
		for _, c := range d.Checks() {
			if c.Read == nil || c.Pass == nil || !reads[c.ReadOp] {
				t.Errorf("%s: read op %q is not an Oracle source read", c.ID, c.ReadOp)
			}
			if !plan.ValidTier(c.TierFloor) {
				t.Errorf("%s: tier floor %q is not a tier", c.ID, c.TierFloor)
			}
			if c.FixOp == "" && c.FixAsk == "" && c.FixFor == nil {
				t.Errorf("%s: no fix op and no documented ask", c.ID)
			}
			if c.FixOp != "" && !ops[c.FixOp] {
				t.Errorf("%s: fix op %q is not a registered runner op", c.ID, c.FixOp)
			}
		}
	}
	for _, op := range []string{opCreatePublication, opCreateReadRole} {
		if !ops[op] {
			t.Errorf("%q is not a runner op", op)
		}
	}
}

func TestRunRefusesConfigBeforeAnyRead(t *testing.T) {
	tests := []struct {
		name string
		d    Driver
	}{
		{"no tables", Driver{User: "C##DBZUSER"}},
		{"table without owner", Driver{User: "C##DBZUSER", Tables: []string{"ORDERS"}}},
		{"three-part table", Driver{User: "C##DBZUSER", Tables: []string{"FREEPDB1.PIPEROOM.ORDERS"}}},
		{"lower-case table", Driver{User: "C##DBZUSER", Tables: []string{"piperoom.orders"}}},
		{"table injection", Driver{User: "C##DBZUSER", Tables: []string{`PIPEROOM.ORDERS"--`}}},
		{"no user", Driver{Tables: []string{"PIPEROOM.ORDERS"}}},
		{"lower-case user", Driver{User: "c##dbzuser", Tables: []string{"PIPEROOM.ORDERS"}}},
		{"SYS user", Driver{User: "SYS", Tables: []string{"PIPEROOM.ORDERS"}}},
		{"PUBLIC user", Driver{User: "PUBLIC", Tables: []string{"PIPEROOM.ORDERS"}}},
		{"bad PDB", Driver{User: "C##DBZUSER", Tables: []string{"PIPEROOM.ORDERS"}, PDB: "FREE PDB"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := readyReader()
			if _, err := tc.d.Run(context.Background(), f); err == nil {
				t.Fatal("Run accepted the driver")
			}
			if len(f.calls) != 0 {
				t.Fatalf("reads ran before the refusal: %v", f.calls)
			}
		})
	}
	if _, err := testDriver().Run(context.Background(), nil); !errors.Is(err, ErrParams) {
		t.Fatalf("nil reader: %v", err)
	}
}

func TestRunReportsReadErrors(t *testing.T) {
	f := readyReader()
	f.err = errors.New("ORA-12541: no listener")
	results, err := testDriver().Run(context.Background(), f)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, r := range results {
		if r.OK || r.Err == nil {
			t.Fatalf("%s hid the read error: %+v", r.Check.ID, r)
		}
		if !errors.Is(r.Err, f.err) {
			t.Fatalf("%s: error does not wrap the read error: %v", r.Check.ID, r.Err)
		}
	}
}

// TestDatabaseWideAllColumnsSatisfiesTables: Debezium accepts ALL columns
// database-wide in place of per-table log groups.
func TestDatabaseWideAllColumnsSatisfiesTables(t *testing.T) {
	f := readyReader()
	f.tables = []oraclient.TableSupplementalLogging{{Table: "PIPEROOM.ORDERS"}}
	f.db.Supplemental = oraclient.SupplementalLogging{All: true}
	results, err := testDriver().Run(context.Background(), f)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, id := range []string{CheckSupplementalTables, CheckSupplementalDatabase} {
		if r := resultOf(t, results, id); !r.OK {
			t.Fatalf("%s failed with ALL columns database-wide: %+v", id, r)
		}
	}
}

func TestSelectAnyTableSatisfiesTableGrants(t *testing.T) {
	f := readyReader()
	f.grants.TablePrivs = nil
	f.grants.SysPrivs = append(f.grants.SysPrivs, "SELECT ANY TABLE")
	results, err := testDriver().Run(context.Background(), f)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r := resultOf(t, results, CheckTableGrants); !r.OK {
		t.Fatalf("SELECT ANY TABLE did not satisfy the table grants: %+v", r)
	}
}

// TestContainerAskListsOnlyIdentifiers: PDB names are dictionary text; a name
// outside the identifier rule never reaches the ask.
func TestContainerAskListsOnlyIdentifiers(t *testing.T) {
	d := testDriver()
	d.PDB = ""
	_, ask := containerAsk(d, Evidence{DB: oraclient.DatabaseInfo{IsCDB: true, PDBs: []string{"FREEPDB1", "X; DROP USER Y"}}})
	if !strings.Contains(ask, "FREEPDB1") || strings.Contains(ask, "DROP") {
		t.Fatalf("ask = %q", ask)
	}
}

func TestCanaryTable(t *testing.T) {
	got, err := testDriver().CanaryTable()
	if err != nil || got != "PIPEROOM.PIPEROOM_CANARY" {
		t.Fatalf("CanaryTable = %q, %v", got, err)
	}
	if _, err := (Driver{}).CanaryTable(); !errors.Is(err, ErrNoTables) {
		t.Fatalf("no tables: %v", err)
	}
}
