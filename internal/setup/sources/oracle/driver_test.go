package oracle

import (
	"context"
	"errors"
	"strings"
	"testing"

	oracleclient "piperoom/internal/clients/oracle"
	"piperoom/internal/setup/plan"
)

type fakeReader struct {
	info   oracleclient.DatabaseInfo
	tables []oracleclient.TableSupplementalLogging
	grants oracleclient.LogMinerGrants
	err    error
	wait   bool
	calls  []string
}

func readyReader() *fakeReader {
	return &fakeReader{
		info: oracleclient.DatabaseInfo{
			Name: "FREE", IsCDB: true, Container: "FREEPDB1", PDBs: []string{"FREEPDB1"},
			LogMode: "ARCHIVELOG", Edition: "Free",
			Supplemental: oracleclient.SupplementalLogging{Minimal: true},
		},
		tables: []oracleclient.TableSupplementalLogging{
			{Table: "PIPEROOM.ORDERS", AllColumns: true, Always: true},
			{Table: DefaultCanaryTable, AllColumns: true, Always: true},
		},
		grants: oracleclient.LogMinerGrants{
			User:     "C##DBZUSER",
			SysPrivs: []string{"CREATE SESSION", "SET CONTAINER", "FLASHBACK ANY TABLE", "SELECT ANY TRANSACTION", "LOGMINING"},
			Roles:    []string{"SELECT_CATALOG_ROLE"},
			TablePrivs: []oracleclient.TablePrivilege{
				{Table: "PIPEROOM.ORDERS", Privilege: "SELECT"},
				{Table: DefaultCanaryTable, Privilege: "SELECT"},
			},
		},
	}
}

func (f *fakeReader) read(ctx context.Context, call string) error {
	f.calls = append(f.calls, call)
	if f.wait {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.err
}

func (f *fakeReader) Database(ctx context.Context) (oracleclient.DatabaseInfo, error) {
	if err := f.read(ctx, "database"); err != nil {
		return oracleclient.DatabaseInfo{}, err
	}
	return f.info, nil
}

func (f *fakeReader) TableSupplementalLogging(ctx context.Context, _ []string) ([]oracleclient.TableSupplementalLogging, error) {
	if err := f.read(ctx, "tables"); err != nil {
		return nil, err
	}
	return f.tables, nil
}

func (f *fakeReader) LogMinerGrants(ctx context.Context, _ string, _ []string) (oracleclient.LogMinerGrants, error) {
	if err := f.read(ctx, "grants"); err != nil {
		return oracleclient.LogMinerGrants{}, err
	}
	return f.grants, nil
}

func testDriver() Driver {
	return Driver{Database: "FREE", PDB: "FREEPDB1", LogMinerUser: "C##DBZUSER", Tables: []string{"PIPEROOM.ORDERS"}}
}

func resultByID(t *testing.T, results []Result, id string) Result {
	t.Helper()
	for _, result := range results {
		if result.Check.ID == id {
			return result
		}
	}
	t.Fatalf("no result for check %q", id)
	return Result{}
}

func TestRunReadyAndCheckFixMetadata(t *testing.T) {
	d := testDriver()
	results, err := d.Run(context.Background(), readyReader())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(results) != 6 {
		t.Fatalf("got %d checks, want 6", len(results))
	}
	for _, result := range results {
		if !result.OK || result.Err != nil {
			t.Errorf("%s: OK=%v Err=%v", result.Check.ID, result.OK, result.Err)
		}
	}
	for _, check := range d.Checks() {
		if check.TierFloor == "" {
			t.Errorf("%s has no tier floor", check.ID)
		}
		if check.FixOp == "" && check.FixAsk == "" {
			t.Errorf("%s has neither a fixing op nor a human ask", check.ID)
		}
	}
}

func TestRunFailureFixesAndTiers(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		mutate   func(*fakeReader)
		wantFix  string
		wantAsk  string
		wantTier string
	}{
		{
			name:    "archive log disabled requires maintenance window",
			id:      CheckArchiveLog,
			mutate:  func(f *fakeReader) { f.info.LogMode = "NOARCHIVELOG" },
			wantAsk: "maintenance window", wantTier: plan.TierT3,
		},
		{
			name:    "database minimal supplemental logging disabled is DBA action",
			id:      CheckSupplementalLogging,
			mutate:  func(f *fakeReader) { f.info.Supplemental.Minimal = false },
			wantAsk: "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA", wantTier: plan.TierT2,
		},
		{
			name:    "canary table supplemental logging missing",
			id:      CheckTableSupplementalLog,
			mutate:  func(f *fakeReader) { f.tables = f.tables[:1] },
			wantFix: opCreateCanaryTable, wantTier: plan.TierT2,
		},
		{
			name:    "source table supplemental logging missing",
			id:      CheckTableSupplementalLog,
			mutate:  func(f *fakeReader) { f.tables = f.tables[1:] },
			wantFix: opCreatePublication, wantTier: plan.TierT2,
		},
		{
			name:    "all LogMiner grants missing",
			id:      CheckLogMinerGrants,
			mutate:  func(f *fakeReader) { f.grants = oracleclient.LogMinerGrants{User: "C##DBZUSER"} },
			wantAsk: "No grants were observed", wantTier: plan.TierT2,
		},
		{
			name: "partial grants need DBA action",
			id:   CheckLogMinerGrants,
			mutate: func(f *fakeReader) {
				f.grants = oracleclient.LogMinerGrants{User: "C##DBZUSER", SysPrivs: []string{"CREATE SESSION"}}
			},
			wantAsk: "DBA must grant the missing", wantTier: plan.TierT2,
		},
		{
			name:    "unsupported edition",
			id:      CheckEdition,
			mutate:  func(f *fakeReader) { f.info.Edition = "Enterprise" },
			wantAsk: "edition-specific", wantTier: plan.TierT3,
		},
		{
			name:    "database identity mismatch",
			id:      CheckDatabaseIdentity,
			mutate:  func(f *fakeReader) { f.info.Container = "OTHERPDB" },
			wantAsk: "intended service", wantTier: plan.TierT1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := readyReader()
			tt.mutate(reader)
			results, err := testDriver().Run(context.Background(), reader)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			result := resultByID(t, results, tt.id)
			if result.OK {
				t.Fatal("check passed after its evidence was changed")
			}
			fix, ask := result.Check.Fix(testDriver(), result.Evidence)
			if fix != tt.wantFix {
				t.Errorf("fix op = %q, want %q", fix, tt.wantFix)
			}
			if tt.wantAsk != "" && !strings.Contains(strings.ToLower(ask), strings.ToLower(tt.wantAsk)) {
				t.Errorf("fix ask = %q, want it to contain %q", ask, tt.wantAsk)
			}
			if tt.wantAsk == "" && ask != "" {
				t.Errorf("fix ask = %q, want none", ask)
			}
			tier, err := result.Check.Tier(plan.ClassLocal)
			if err != nil {
				t.Fatalf("Tier: %v", err)
			}
			if tier != tt.wantTier {
				t.Errorf("tier = %q, want %q", tier, tt.wantTier)
			}
		})
	}
}

func TestRunTargetDownAndTimeout(t *testing.T) {
	t.Run("target down", func(t *testing.T) {
		down := errors.New("connection refused")
		reader := readyReader()
		reader.err = down
		results, err := testDriver().Run(context.Background(), reader)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		for _, result := range results {
			if result.OK || !errors.Is(result.Err, down) {
				t.Errorf("%s: OK=%v Err=%v, want target-down error", result.Check.ID, result.OK, result.Err)
			}
		}
	})
	t.Run("timeout", func(t *testing.T) {
		reader := readyReader()
		reader.wait = true
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()
		results, err := testDriver().Run(ctx, reader)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		for _, result := range results {
			if result.OK || !errors.Is(result.Err, context.DeadlineExceeded) {
				t.Errorf("%s: OK=%v Err=%v, want deadline", result.Check.ID, result.OK, result.Err)
			}
		}
	})
}

func TestRunRejectsBadInputBeforeReads(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Driver)
	}{
		{"bad database identifier", func(d *Driver) { d.Database = "FREE; DROP" }},
		{"bad PDB identifier", func(d *Driver) { d.PDB = "PDB/OTHER" }},
		{"bad LogMiner username", func(d *Driver) { d.LogMinerUser = "C##DBZUSER;" }},
		{"no source tables", func(d *Driver) { d.Tables = nil }},
		{"bad table", func(d *Driver) { d.Tables = []string{"PIPEROOM.ORDERS;DROP"} }},
		{"bad canary", func(d *Driver) { d.CanaryTable = "PIPEROOM.CANARY.X" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := testDriver()
			tt.edit(&d)
			reader := readyReader()
			if _, err := d.Run(context.Background(), reader); err == nil {
				t.Fatal("Run succeeded with bad input")
			}
			if len(reader.calls) != 0 {
				t.Fatalf("reader called before validation: %v", reader.calls)
			}
		})
	}
}

func TestRunRejectsNilReader(t *testing.T) {
	if _, err := testDriver().Run(context.Background(), nil); !errors.Is(err, ErrParams) {
		t.Fatalf("Run error = %v, want ErrParams", err)
	}
}

func TestTablesForConnectorAddsCanaryWithoutMutatingSource(t *testing.T) {
	d := testDriver()
	got := d.TablesForConnector()
	if len(got) != 2 || got[1] != DefaultCanaryTable {
		t.Fatalf("TablesForConnector = %v", got)
	}
	if len(d.Tables) != 1 || d.Tables[0] != "PIPEROOM.ORDERS" {
		t.Fatalf("driver tables mutated: %v", d.Tables)
	}
}
