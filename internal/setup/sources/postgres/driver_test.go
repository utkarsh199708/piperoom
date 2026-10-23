package postgres

import (
	"context"
	"errors"
	"testing"

	"piperoom/internal/actions/pgactions"
	"piperoom/internal/clients"
	pgclient "piperoom/internal/clients/postgres"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/remoteclients"
)

// fakeReader answers the driver's reads from fields; err fails every call.
type fakeReader struct {
	settings    []pgclient.DBSetting
	slots       []clients.ReplicationSlot
	tables      []pgclient.TableReplication
	role        pgclient.RoleInfo
	publication []pgclient.PublicationTable
	pubErr      error
	err         error
	calls       []string
}

func (f *fakeReader) Settings(context.Context) ([]pgclient.DBSetting, error) {
	f.calls = append(f.calls, "settings")
	return f.settings, f.err
}

func (f *fakeReader) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	f.calls = append(f.calls, "slots")
	return f.slots, f.err
}

func (f *fakeReader) TableReplication(_ context.Context, tables []string) ([]pgclient.TableReplication, error) {
	f.calls = append(f.calls, "tables")
	return f.tables, f.err
}

func (f *fakeReader) RoleInfo(context.Context, string, []string) (pgclient.RoleInfo, error) {
	f.calls = append(f.calls, "role")
	return f.role, f.err
}

func (f *fakeReader) PublicationTables(context.Context, string, string, []string) ([]pgclient.PublicationTable, error) {
	f.calls = append(f.calls, "publication")
	if f.pubErr != nil {
		return nil, f.pubErr
	}
	return f.publication, f.err
}

// readyReader is the evidence of a source that passes every check.
func readyReader() *fakeReader {
	return &fakeReader{
		settings: []pgclient.DBSetting{
			{Name: "wal_level", Value: "logical"},
			{Name: "max_wal_senders", Value: "10"},
			{Name: "max_replication_slots", Value: "10"},
		},
		slots:  []clients.ReplicationSlot{{Name: "someone_else", Plugin: "pgoutput"}},
		tables: []pgclient.TableReplication{{Table: "public.orders", HasPrimaryKey: true, ReplicaIdentity: "default"}},
		role:   pgclient.RoleInfo{Role: "piperoom_ro", Exists: true, CanLogin: true, Replication: true, Selectable: map[string]bool{"public.orders": true}},
		publication: []pgclient.PublicationTable{
			{Schema: "public", Table: "orders", InPublication: true, Selectable: true},
		},
	}
}

func testDriver() Driver {
	return Driver{Publication: "dbz_publication", Slot: "debezium", Role: "piperoom_ro", Tables: []string{"public.orders"}}
}

// resultOf returns the result of one check id.
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
		mutate func(*fakeReader)
		fix    string
		tier   string
	}{
		{"wal_level replica", CheckWALLevel, func(f *fakeReader) { f.settings[0].Value = "replica" }, opAlterSystem, plan.TierT3},
		{"wal_level missing", CheckWALLevel, func(f *fakeReader) { f.settings = nil }, opAlterSystem, plan.TierT3},
		{"max_wal_senders zero", CheckMaxWALSenders, func(f *fakeReader) { f.settings[1].Value = "0" }, opAlterSystem, plan.TierT3},
		{"max_wal_senders with a unit", CheckMaxWALSenders, func(f *fakeReader) { f.settings[1].Value = "10s" }, opAlterSystem, plan.TierT3},
		{"max_replication_slots zero", CheckMaxReplicationSlots, func(f *fakeReader) { f.settings[2].Value = "0" }, opAlterSystem, plan.TierT3},
		{"slot of another plugin", CheckReplicationSlot, func(f *fakeReader) {
			f.slots = append(f.slots, clients.ReplicationSlot{Name: "debezium", Plugin: "wal2json"})
		}, opDropSlot, plan.TierT2},
		{"no room for a slot", CheckReplicationSlotRoom, func(f *fakeReader) { f.settings[2].Value = "1" }, opAlterSystem, plan.TierT3},
		{"publication missing", CheckPublication, func(f *fakeReader) { f.pubErr = clients.ErrNotFound; f.publication = nil }, opCreatePublication, plan.TierT2},
		{"table not published", CheckPublicationTables, func(f *fakeReader) { f.publication[0].InPublication = false }, opAddPublicationTable, plan.TierT2},
		{"table without key", CheckReplicaIdentity, func(f *fakeReader) {
			f.tables[0] = pgclient.TableReplication{Table: "public.orders", ReplicaIdentity: "nothing"}
		}, opSetReplicaIdentity, plan.TierT2},
		{"table missing from the read", CheckReplicaIdentity, func(f *fakeReader) { f.tables = nil }, opSetReplicaIdentity, plan.TierT2},
		{"role missing", CheckReadRole, func(f *fakeReader) { f.role = pgclient.RoleInfo{Role: "piperoom_ro"} }, opCreateReadRole, plan.TierT2},
		{"role cannot replicate", CheckReadRole, func(f *fakeReader) { f.role.Replication = false }, opCreateReadRole, plan.TierT2},
		{"role cannot select", CheckReadRoleGrants, func(f *fakeReader) { f.role.Selectable = map[string]bool{} }, opGrantSelect, plan.TierT2},
		{"rds logical replication off", CheckRDSParameterGroup, func(f *fakeReader) {
			f.settings = append(f.settings, pgclient.DBSetting{Name: "rds.logical_replication", Value: "0"})
		}, "", plan.TierT3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := readyReader()
			tc.mutate(f)
			results, err := testDriver().Run(context.Background(), f)
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
			if r.Check.FixOp != tc.fix || r.Check.TierFloor != tc.tier {
				t.Fatalf("%s fix = %q/%q, want %q/%q", tc.id, r.Check.FixOp, r.Check.TierFloor, tc.fix, tc.tier)
			}
			if tc.fix == "" && r.Check.FixAsk == "" {
				t.Fatalf("%s names no fix and no ask", tc.id)
			}
		})
	}
}

func TestEveryCheckPassesOnAReadySource(t *testing.T) {
	results, err := testDriver().Run(context.Background(), readyReader())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(results) != len(testDriver().Checks()) {
		t.Fatalf("got %d results, want %d", len(results), len(testDriver().Checks()))
	}
	for _, r := range results {
		if !r.OK {
			t.Errorf("%s failed on a ready source: %+v", r.Check.ID, r.Err)
		}
	}
}

func TestExistingOwnSlotPasses(t *testing.T) {
	f := readyReader()
	f.slots = append(f.slots, clients.ReplicationSlot{Name: "debezium", Plugin: OutputPlugin, Active: true})
	results, err := testDriver().Run(context.Background(), f)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r := resultOf(t, results, CheckReplicationSlot); !r.OK {
		t.Fatalf("the connector's own pgoutput slot failed: %+v", r)
	}
	if r := resultOf(t, results, CheckReplicationSlotRoom); !r.OK {
		t.Fatalf("room for a new slot failed: %+v", r)
	}
}

func TestRDSOnPassesAndAbsentSettingPasses(t *testing.T) {
	for _, value := range []string{"1", "on", "true", "yes"} {
		f := readyReader()
		f.settings = append(f.settings, pgclient.DBSetting{Name: "rds.logical_replication", Value: value})
		results, err := testDriver().Run(context.Background(), f)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if r := resultOf(t, results, CheckRDSParameterGroup); !r.OK {
			t.Fatalf("rds.logical_replication=%q failed", value)
		}
	}
}

func TestRunRefusesConfigBeforeAnyRead(t *testing.T) {
	tests := []struct {
		name string
		d    Driver
	}{
		{"no tables", Driver{Publication: "p", Slot: "s", Role: "r"}},
		{"table without schema", Driver{Role: "r", Tables: []string{"orders"}}},
		{"three-part table", Driver{Role: "r", Tables: []string{"db.public.orders"}}},
		{"no role", Driver{Tables: []string{"public.orders"}}},
		{"public role", Driver{Role: "public", Tables: []string{"public.orders"}}},
		{"bad publication", Driver{Publication: "1bad", Role: "r", Tables: []string{"public.orders"}}},
		{"upper-case slot", Driver{Slot: "Debezium", Role: "r", Tables: []string{"public.orders"}}},
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
}

func TestRunReportsReadErrors(t *testing.T) {
	f := readyReader()
	f.err = errors.New("connection refused")
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

func TestCheckFixOpsAreRegistered(t *testing.T) {
	ops := map[string]bool{}
	for _, spec := range remoteclients.Ops() {
		ops[spec.Op] = true
	}
	ops[pgactions.DropSlotName] = true
	ops[pgactions.AddPublicationTableName] = true
	// The reads the driver names. source_publication_tables is the one op the
	// runner does not have yet: *postgres.DB answers it, remoteclients.Source
	// gains it with that op (the PR's gap).
	reads := map[string]bool{
		"source_settings": true, "source_replication_slots": true, "source_table_replication": true,
		"source_role_info": true, "source_publication_tables": true,
	}
	for _, c := range testDriver().Checks() {
		if c.Read == nil || !reads[c.ReadOp] {
			t.Errorf("%s: read op %q is not a source read", c.ID, c.ReadOp)
		}
		if c.ReadOp != "source_publication_tables" && !ops[c.ReadOp] {
			t.Errorf("%s: read op %q is not a runner op", c.ID, c.ReadOp)
		}
		if !plan.ValidTier(c.TierFloor) {
			t.Errorf("%s: tier floor %q is not a tier", c.ID, c.TierFloor)
		}
		if c.FixOp == "" {
			if c.FixAsk == "" {
				t.Errorf("%s: no fix op and no documented ask", c.ID)
			}
			continue
		}
		if !ops[c.FixOp] {
			t.Errorf("%s: fix op %q is not a registered op or action", c.ID, c.FixOp)
		}
	}
}

func TestTierFollowsTheTargetClass(t *testing.T) {
	c := Check{TierFloor: plan.TierT2}
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
