package sqlserver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/clients"
	sqlclient "piperoom/internal/clients/sqlserver"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/remoteclients"
	"piperoom/internal/setup/targets/host"
)

// fakeReader answers the driver's reads from fields; err fails every call.
type fakeReader struct {
	server sqlclient.ServerInfo
	agent  sqlclient.AgentState
	dbCDC  bool
	dbErr  error
	tables []sqlclient.TableCDC
	login  sqlclient.RoleInfo
	err    error
	calls  []string
}

func (f *fakeReader) ServerInfo(context.Context) (sqlclient.ServerInfo, error) {
	f.calls = append(f.calls, "server")
	return f.server, f.err
}

func (f *fakeReader) AgentState(context.Context) (sqlclient.AgentState, error) {
	f.calls = append(f.calls, "agent")
	return f.agent, f.err
}

func (f *fakeReader) DatabaseCDC(context.Context, string) (bool, error) {
	f.calls = append(f.calls, "database")
	if f.dbErr != nil {
		return false, f.dbErr
	}
	return f.dbCDC, f.err
}

func (f *fakeReader) TableCDC(context.Context, []string) ([]sqlclient.TableCDC, error) {
	f.calls = append(f.calls, "tables")
	return f.tables, f.err
}

func (f *fakeReader) RoleInfo(context.Context, string, []string) (sqlclient.RoleInfo, error) {
	f.calls = append(f.calls, "login")
	return f.login, f.err
}

// readyReader is the evidence of a source that passes every check (the U4d
// clean scenario: 2022 Developer, Agent Running, CDC on).
func readyReader() *fakeReader {
	return &fakeReader{
		server: sqlclient.ServerInfo{Version: "16.0.4135.4", Edition: "Developer Edition (64-bit)"},
		agent:  sqlclient.AgentState{Running: true, Status: "Running"},
		dbCDC:  true,
		tables: []sqlclient.TableCDC{
			{Table: "dbo.orders", Exists: true, CDCEnabled: true, HasPrimaryKey: true},
			{Table: "dbo.piperoom_canary", Exists: true, CDCEnabled: true, HasPrimaryKey: true},
		},
		login: sqlclient.RoleInfo{Login: "piperoom_cdc", Exists: true,
			HasDBUser: true, CDCSelect: true, ViewDBState: true,
			Selectable: map[string]bool{"dbo.orders": true, "dbo.piperoom_canary": true}},
	}
}

func testDriver() Driver {
	return Driver{Database: "orders", Login: "piperoom_cdc", CaptureInstance: "orders_mssql",
		Tables: []string{"dbo.orders", DefaultCanaryTable}, Target: "compose"}
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

func TestChecksPassAndFail(t *testing.T) {
	tests := []struct {
		name   string
		id     string
		d      func(*Driver)
		mutate func(*fakeReader)
		fix    string // "" = a documented ask
		ask    string // a fragment the ask must carry
		tier   string
	}{
		{"2016", CheckVersionEdition, nil, func(f *fakeReader) { f.server.Version = "13.0.5026.0" }, "", "2017", plan.TierT3},
		{"express", CheckVersionEdition, nil, func(f *fakeReader) { f.server.Edition = "Express Edition (64-bit)" }, "", "Express", plan.TierT3},
		{"unparsable version", CheckVersionEdition, nil, func(f *fakeReader) { f.server.Version = "" }, "", "Upgrade", plan.TierT3},
		{"agent stopped on compose", CheckAgentRunning, nil, func(f *fakeReader) { f.agent = sqlclient.AgentState{Status: "Stopped"} }, "", "xp_servicecontrol", plan.TierT2},
		{"agent stopped on a host target", CheckAgentRunning, func(d *Driver) { d.Target = TargetHost }, func(f *fakeReader) { f.agent = sqlclient.AgentState{Status: "Stopped"} }, opServiceRestart, "", plan.TierT2},
		{"agent stopped on RDS", CheckAgentRunning, func(d *Driver) { d.Platform = PlatformRDS; d.Target = TargetHost }, func(f *fakeReader) { f.agent = sqlclient.AgentState{Status: "Stopped"} }, "", "provider's console", plan.TierT2},
		{"agent stopped on managed instance", CheckAgentRunning, func(d *Driver) { d.Platform = PlatformAzureMI }, func(f *fakeReader) { f.agent = sqlclient.AgentState{Status: "Unknown"} }, "", "provider's console", plan.TierT2},
		{"database CDC off", CheckDatabaseCDC, nil, func(f *fakeReader) { f.dbCDC = false }, opCreatePublication, "", plan.TierT2},
		{"database CDC off on RDS", CheckDatabaseCDC, func(d *Driver) { d.Platform = PlatformRDS }, func(f *fakeReader) { f.dbCDC = false }, "", "rds_cdc_enable_db 'orders'", plan.TierT2},
		{"database missing", CheckDatabaseCDC, nil, func(f *fakeReader) { f.dbErr = fmt.Errorf("x: %w", clients.ErrNotFound) }, "", "[orders] does not exist", plan.TierT2},
		{"table untracked", CheckTableCDC, nil, func(f *fakeReader) { f.tables[0].CDCEnabled = false }, opCreatePublication, "", plan.TierT2},
		{"table missing", CheckTableCDC, nil, func(f *fakeReader) { f.tables[0] = sqlclient.TableCDC{Table: "dbo.orders"} }, "", "dbo.orders do not exist", plan.TierT2},
		{"table absent from the read", CheckTableCDC, nil, func(f *fakeReader) { f.tables = f.tables[1:] }, "", "dbo.orders", plan.TierT2},
		{"login missing", CheckCaptureLogin, nil, func(f *fakeReader) { f.login = sqlclient.RoleInfo{Login: "piperoom_cdc"} }, opCreateReadRole, "", plan.TierT2},
		{"login missing on Azure SQL", CheckCaptureLogin, func(d *Driver) { d.Platform = PlatformAzureSQL }, func(f *fakeReader) { f.login = sqlclient.RoleInfo{} }, "", "contained user", plan.TierT2},
		{"login disabled", CheckCaptureLogin, nil, func(f *fakeReader) { f.login.Disabled = true }, "", "ALTER LOGIN [piperoom_cdc] ENABLE", plan.TierT2},
		{"no SELECT", CheckCaptureLoginGrant, nil, func(f *fakeReader) { f.login.Selectable["dbo.orders"] = false }, "", "GRANT SELECT ON [dbo].[orders] TO [piperoom_cdc]", plan.TierT2},
		{"no SELECT, no login", CheckCaptureLoginGrant, nil, func(f *fakeReader) { f.login = sqlclient.RoleInfo{} }, opCreateReadRole, "", plan.TierT2},
		{"no database user", CheckCaptureLoginGrant, nil, func(f *fakeReader) { f.login.HasDBUser = false }, "", "CREATE USER [piperoom_cdc] FOR LOGIN [piperoom_cdc]", plan.TierT2},
		{"no cdc schema SELECT", CheckCaptureLoginGrant, nil, func(f *fakeReader) { f.login.CDCSelect = false }, "", "GRANT SELECT ON SCHEMA::cdc TO [piperoom_cdc]", plan.TierT2},
		{"no VIEW DATABASE STATE", CheckCaptureLoginGrant, nil, func(f *fakeReader) { f.login.ViewDBState = false }, "", "GRANT VIEW DATABASE STATE TO [piperoom_cdc]", plan.TierT2},
		{"sysadmin without a user passes the user rule but not a revoked state", CheckCaptureLoginGrant, nil, func(f *fakeReader) {
			f.login.Sysadmin = true
			f.login.HasDBUser = false
			f.login.ViewDBState = false
		}, "", "GRANT VIEW DATABASE STATE TO [piperoom_cdc]", plan.TierT2},
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
			if r.OK || r.Err != nil {
				t.Fatalf("%s: OK=%v err=%v, want a failure verdict", tc.id, r.OK, r.Err)
			}
			op, ask := r.Check.Fix(d, r.Evidence)
			if op != tc.fix || r.Check.TierFloor != tc.tier {
				t.Fatalf("%s fix = %q/%q, want %q/%q (ask %q)", tc.id, op, r.Check.TierFloor, tc.fix, tc.tier, ask)
			}
			if (op == "") == (ask == "") {
				t.Fatalf("%s: want exactly one of an op and an ask, got %q and %q", tc.id, op, ask)
			}
			if !strings.Contains(ask, tc.ask) {
				t.Fatalf("%s ask %q does not mention %q", tc.id, ask, tc.ask)
			}
		})
	}
}

func TestEveryCheckPassesOnAReadySource(t *testing.T) {
	for _, tc := range []struct {
		name   string
		d      func(*Driver)
		mutate func(*fakeReader)
	}{
		{"developer 2022", nil, nil},
		{"standard 2017", nil, func(f *fakeReader) {
			f.server = sqlclient.ServerInfo{Version: "14.0.3456.2", Edition: "Standard Edition (64-bit)"}
		}},
		{"enterprise evaluation", nil, func(f *fakeReader) { f.server.Edition = "Enterprise Evaluation Edition (64-bit)" }},
		// Azure SQL reports 12.x; Azure SQL Database has no Agent to read.
		{"azure sql database", func(d *Driver) { d.Platform = PlatformAzureSQL }, func(f *fakeReader) {
			f.server = sqlclient.ServerInfo{Version: "12.0.2000.8", Edition: "SQL Azure"}
			f.agent = sqlclient.AgentState{}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := testDriver()
			if tc.d != nil {
				tc.d(&d)
			}
			f := readyReader()
			if tc.mutate != nil {
				tc.mutate(f)
			}
			results, err := d.Run(context.Background(), f)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if len(results) != len(d.Checks()) {
				t.Fatalf("got %d results, want %d", len(results), len(d.Checks()))
			}
			for _, r := range results {
				if !r.OK {
					t.Errorf("%s failed on a ready source: %v", r.Check.ID, r.Err)
				}
			}
			if d.Platform == PlatformAzureSQL && slices.Contains(f.calls, "agent") {
				t.Fatalf("the Agent was read on Azure SQL Database: %v", f.calls)
			}
		})
	}
}

func TestRunRefusesConfigBeforeAnyRead(t *testing.T) {
	ok := testDriver()
	tests := []struct {
		name string
		mod  func(*Driver)
	}{
		{"no tables", func(d *Driver) { d.Tables = nil }},
		{"table without schema", func(d *Driver) { d.Tables = []string{"orders"} }},
		{"three-part table", func(d *Driver) { d.Tables = []string{"orders.dbo.orders"} }},
		{"bracket injection", func(d *Driver) { d.Tables = []string{"dbo.orders]; DROP TABLE x--"} }},
		{"no database", func(d *Driver) { d.Database = "" }},
		{"system database", func(d *Driver) { d.Database = "msdb" }},
		{"no login", func(d *Driver) { d.Login = "" }},
		{"sa login", func(d *Driver) { d.Login = "SA" }},
		{"fixed role login", func(d *Driver) { d.Login = "db_owner" }},
		{"guest login", func(d *Driver) { d.Login = "guest" }},
		{"cdc login", func(d *Driver) { d.Login = "cdc" }},
		{"INFORMATION_SCHEMA login", func(d *Driver) { d.Login = "INFORMATION_SCHEMA" }},
		{"database over sysname", func(d *Driver) { d.Database = strings.Repeat("d", maxIdentChars+1) }},
		{"duplicate tables, suffix over the limit", func(d *Driver) {
			d.CaptureInstance = strings.Repeat("c", 90)
			d.Tables = []string{"dbo.orders", "dbo.orders", DefaultCanaryTable}
		}},
		{"long capture instance", func(d *Driver) { d.CaptureInstance = strings.Repeat("c", 90) }},
		{"unknown platform", func(d *Driver) { d.Platform = "gcp" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := ok
			tc.mod(&d)
			f := readyReader()
			if _, err := d.Run(context.Background(), f); err == nil {
				t.Fatal("Run accepted the driver")
			}
			if len(f.calls) != 0 {
				t.Fatalf("reads ran before the refusal: %v", f.calls)
			}
		})
	}
	if _, err := ok.Run(context.Background(), nil); !errors.Is(err, ErrParams) {
		t.Fatalf("nil reader: %v, want ErrParams", err)
	}
}

func TestRunReportsReadErrors(t *testing.T) {
	f := readyReader()
	f.err = errors.New("connection refused")
	results, err := testDriver().Run(context.Background(), f)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, r := range results {
		if r.OK || !errors.Is(r.Err, f.err) {
			t.Fatalf("%s hid the read error: %+v", r.Check.ID, r)
		}
	}
}

// TestTableCheckNotesDatabaseFix: with CDC off on the database the table
// check fails too, and says the database fix enables both.
func TestTableCheckNotesDatabaseFix(t *testing.T) {
	f := readyReader()
	f.dbCDC = false
	for i := range f.tables {
		f.tables[i].CDCEnabled = false
	}
	results, err := testDriver().Run(context.Background(), f)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	r := resultOf(t, results, CheckTableCDC)
	if r.OK || !strings.Contains(r.Note, CheckDatabaseCDC) {
		t.Fatalf("table check: %+v, want a failure noting %s", r, CheckDatabaseCDC)
	}
}

// TestUntrackedTablesNamesOnlyTheGap: the fix's tables are the existing,
// untracked ones, never a table whose capture instance exists already.
func TestUntrackedTablesNamesOnlyTheGap(t *testing.T) {
	f := readyReader()
	f.tables[0].CDCEnabled = false
	f.tables = append(f.tables, sqlclient.TableCDC{Table: "dbo.other", Exists: true})
	got := UntrackedTables(testDriver(), Evidence{Tables: f.tables})
	if !slices.Equal(got, []string{"dbo.orders"}) {
		t.Fatalf("UntrackedTables = %v, want [dbo.orders]", got)
	}
}

func TestCheckFixOpsAreRegistered(t *testing.T) {
	floors := map[string]string{}
	for _, spec := range remoteclients.Ops() {
		floors[spec.Op] = spec.Tier
	}
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := host.Register(reg, host.Options{GOOS: "linux", ResolvePath: func(p string) (string, error) { return p, nil }}); err != nil {
		t.Fatalf("host.Register: %v", err)
	}
	if floor, ok := reg.Floor(opServiceRestart); ok {
		floors[opServiceRestart] = floor
	}
	reads := []string{readServerInfo, readAgentState, readDatabaseCDC, readTableCDC, readRoleInfo}
	ids := map[string]bool{}
	for _, c := range testDriver().Checks() {
		if ids[c.ID] {
			t.Errorf("duplicate check id %s", c.ID)
		}
		ids[c.ID] = true
		if c.Read == nil || !slices.Contains(reads, c.ReadOp) {
			t.Errorf("%s: read op %q is not a source read", c.ID, c.ReadOp)
		}
		if !plan.ValidTier(c.TierFloor) {
			t.Errorf("%s: tier floor %q is not a tier", c.ID, c.TierFloor)
		}
		if c.FixOp == "" {
			if c.FixAsk == "" && c.FixFor == nil {
				t.Errorf("%s: no fix op and no documented ask", c.ID)
			}
			continue
		}
		floor, ok := floors[c.FixOp]
		if !ok {
			t.Errorf("%s: fix op %q is not a registered op", c.ID, c.FixOp)
		} else if floor != c.TierFloor {
			t.Errorf("%s: floor %q, but op %q's own floor is %q", c.ID, c.TierFloor, c.FixOp, floor)
		}
	}
	// The ids devctl's breaks (U4c) and the U4d scenarios name.
	for _, id := range []string{"sqlserver-agent-running", "sqlserver-table-cdc", "sqlserver-db-cdc", "sqlserver-version-edition", "sqlserver-capture-login"} {
		if !ids[id] {
			t.Errorf("no check %s", id)
		}
	}
}

func TestTierFollowsTheTargetClass(t *testing.T) {
	for _, c := range testDriver().Checks() {
		for _, tc := range []struct{ class, want string }{
			{plan.ClassLocal, c.TierFloor},
			{plan.ClassProduction, plan.TierT3},
		} {
			if got, err := c.Tier(tc.class); err != nil || got != tc.want {
				t.Fatalf("%s Tier(%s) = %q, %v; want %q", c.ID, tc.class, got, err, tc.want)
			}
		}
		if _, err := c.Tier("nonsense"); err == nil {
			t.Fatalf("%s: Tier accepted an unknown class", c.ID)
		}
	}
}
