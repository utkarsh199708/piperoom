package mongodb

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	mongoclient "piperoom/internal/clients/mongodb"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/remoteclients"
	"piperoom/internal/setup/targets/compose"
)

// fakeReader answers the driver's reads from fields; err fails every call.
type fakeReader struct {
	settings []mongoclient.DBSetting
	user     mongoclient.RoleInfo
	err      error
	calls    []string
	asked    []string // the role names RoleInfo was called with
}

func (f *fakeReader) Settings(context.Context) ([]mongoclient.DBSetting, error) {
	f.calls = append(f.calls, "settings")
	return f.settings, f.err
}

func (f *fakeReader) RoleInfo(_ context.Context, role string, _ []string) (mongoclient.RoleInfo, error) {
	f.calls = append(f.calls, "user")
	f.asked = append(f.asked, role)
	return f.user, f.err
}

// readyReader is a clean source: a PRIMARY of rs0 on MongoDB 7.0, the capture
// user able to read the oplog and both collections.
func readyReader() *fakeReader {
	return &fakeReader{
		settings: []mongoclient.DBSetting{
			{Name: "version", Value: "7.0.43"},
			{Name: "server_type", Value: "replica_set_member"},
			{Name: "replica_set", Value: "rs0"},
			{Name: "replica_set_state", Value: "PRIMARY"},
			{Name: "change_streams", Value: "supported"},
		},
		user: mongoclient.RoleInfo{Role: "admin.piperoom_cdc", Exists: true, CanLogin: true, Replication: true,
			Selectable: map[string]bool{"inventory.orders": true, "inventory.piperoom_canary": true}},
	}
}

func testDriver() Driver {
	return Driver{User: "piperoom_cdc", Collections: []string{"inventory.orders", "inventory.piperoom_canary"}, Target: "compose"}
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

func standalone(f *fakeReader) {
	f.settings[1].Value = "standalone"
	f.settings[3].Value = "STANDALONE"
	f.settings[4].Value = "not supported"
}

func TestEveryCheckPassesOnAReadySource(t *testing.T) {
	for name, mod := range map[string]func(*fakeReader){
		"replica set member": func(*fakeReader) {},
		"mongos": func(f *fakeReader) {
			f.settings[1].Value = "mongos"
			f.settings[3].Value = "MONGOS"
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := readyReader()
			mod(f)
			results, err := testDriver().Run(context.Background(), f)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if len(results) != len(testDriver().Checks()) {
				t.Fatalf("%d results, want one per check", len(results))
			}
			for _, r := range results {
				if !r.OK || r.Err != nil {
					t.Errorf("%s failed on a ready source: %v", r.Check.ID, r.Err)
				}
			}
			// The user is read in its auth database.
			for _, role := range f.asked {
				if role != "admin.piperoom_cdc" {
					t.Errorf("RoleInfo asked for %q, want admin.piperoom_cdc", role)
				}
			}
		})
	}
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
		{"standalone, not this room's", CheckChangeStreams, nil, standalone, "", "--replSet rs0", plan.TierT1},
		{"standalone this room created (by receipt)", CheckChangeStreams, func(d *Driver) { d.SourceCreatedByRoom = true }, standalone, opComposeUp, "", plan.TierT1},
		{"standalone on Atlas", CheckChangeStreams, func(d *Driver) { d.Platform = PlatformAtlas }, standalone, "", "mongodb+srv://", plan.TierT1},
		{"standalone with a named set keeps its name", CheckChangeStreams, nil, func(f *fakeReader) {
			standalone(f)
			f.settings[2].Value = "orders_rs"
		}, "", "--replSet orders_rs", plan.TierT1},
		{"user missing", CheckCaptureUser, nil, func(f *fakeReader) { f.user = mongoclient.RoleInfo{Role: "admin.piperoom_cdc"} }, opCreateReadRole, "", plan.TierT2},
		{"user missing on Atlas", CheckCaptureUser, func(d *Driver) { d.Platform = PlatformAtlas }, func(f *fakeReader) { f.user = mongoclient.RoleInfo{} }, "", "Atlas manages database users", plan.TierT2},
		{"user cannot log in", CheckCaptureUser, nil, func(f *fakeReader) { f.user.CanLogin = false }, "", "cannot log in", plan.TierT2},
		{"no oplog read", CheckOplogRead, nil, func(f *fakeReader) { f.user.Replication = false }, "", "{role: 'read', db: 'local'}", plan.TierT2},
		{"no oplog read, no user", CheckOplogRead, nil, func(f *fakeReader) { f.user = mongoclient.RoleInfo{} }, opCreateReadRole, "", plan.TierT2},
		{"a collection unreadable", CheckCollectionsReadable, nil, func(f *fakeReader) { f.user.Selectable["inventory.orders"] = false }, "", "{role: 'read', db: 'inventory'}", plan.TierT2},
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
				t.Fatalf("%s passed on a broken source", tc.id)
			}
			fix, ask := r.Check.Fix(d, r.Evidence)
			if fix != tc.fix {
				t.Errorf("fix = %q, want %q (ask %q)", fix, tc.fix, ask)
			}
			if tc.fix == "" && (ask == "" || !strings.Contains(ask, tc.ask)) {
				t.Errorf("ask = %q, want one containing %q", ask, tc.ask)
			}
			if tc.fix != "" && ask != "" {
				t.Errorf("both a fix op and an ask: %q, %q", fix, ask)
			}
			tier, err := r.Check.Tier(plan.ClassLocal)
			if err != nil || tier != tc.tier {
				t.Errorf("Tier(local) = %q, %v, want %q", tier, err, tc.tier)
			}
			for _, other := range results {
				if other.Check.ID != tc.id && other.Check.ID != CheckCaptureUser && other.Check.ID != CheckOplogRead && other.Check.ID != CheckCollectionsReadable && !other.OK {
					t.Errorf("%s also failed", other.Check.ID)
				}
			}
		})
	}
}

// A missing user fails the three user checks; the two that read through it
// carry the note that the user fix also resolves them.
func TestMissingUserNotes(t *testing.T) {
	f := readyReader()
	f.user = mongoclient.RoleInfo{Role: "admin.piperoom_cdc"}
	results, err := testDriver().Run(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{CheckOplogRead, CheckCollectionsReadable} {
		if r := resultOf(t, results, id); r.OK || !strings.Contains(r.Note, CheckCaptureUser) {
			t.Errorf("%s: ok=%v note=%q", id, r.OK, r.Note)
		}
	}
}

func TestReadErrorsAreStoredPerCheck(t *testing.T) {
	f := readyReader()
	f.err = errors.New("server selection timeout")
	results, err := testDriver().Run(context.Background(), f)
	if err != nil {
		t.Fatalf("Run returned the read error instead of storing it: %v", err)
	}
	for _, r := range results {
		if r.OK || r.Err == nil {
			t.Errorf("%s: ok=%v err=%v, want a stored read error", r.Check.ID, r.OK, r.Err)
		}
	}
}

func TestValidate(t *testing.T) {
	for name, mod := range map[string]func(*Driver){
		"no collections":     func(d *Driver) { d.Collections = nil },
		"system database":    func(d *Driver) { d.Collections = []string{"local.oplog.rs"} },
		"admin database":     func(d *Driver) { d.Collections = []string{"admin.users"} },
		"not db.collection":  func(d *Driver) { d.Collections = []string{"orders"} },
		"nested namespace":   func(d *Driver) { d.Collections = []string{"a.b.c"} },
		"injected name":      func(d *Driver) { d.Collections = []string{"inventory.orders'); db.dropDatabase(); ('"} },
		"no user":            func(d *Driver) { d.User = "" },
		"reserved user":      func(d *Driver) { d.User = "root" },
		"user not an ident":  func(d *Driver) { d.User = "a b" },
		"auth db not ident":  func(d *Driver) { d.AuthDB = "ad min" },
		"unknown platform":   func(d *Driver) { d.Platform = "cosmos" },
		"overlong user name": func(d *Driver) { d.User = strings.Repeat("u", maxNameBytes+1) },
	} {
		t.Run(name, func(t *testing.T) {
			d := testDriver()
			mod(&d)
			if err := d.Validate(); err == nil {
				t.Fatal("Validate accepted a bad driver")
			}
			f := readyReader()
			if _, err := d.Run(context.Background(), f); err == nil {
				t.Fatal("Run accepted a bad driver")
			}
			if len(f.calls) != 0 {
				t.Fatalf("a refused driver read the source %d times", len(f.calls))
			}
		})
	}
	if _, err := testDriver().Run(context.Background(), nil); !errors.Is(err, ErrParams) {
		t.Fatalf("nil reader: %v", err)
	}
	if got := testDriver().CanaryNamespace(); got != "inventory.piperoom_canary" {
		t.Fatalf("CanaryNamespace = %q", got)
	}
}

// Every fix op is a registered op at the check's floor: compose_up in the
// Compose target's catalogue, source_create_read_role in remoteclients' op
// table. Every read op is a source read remoteclients sends.
func TestCheckFixOpsAreRegistered(t *testing.T) {
	floors := map[string]string{}
	sent := map[string]bool{}
	for _, spec := range remoteclients.Ops() {
		floors[spec.Op] = spec.Tier
		sent[spec.Op] = true
	}
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := compose.Register(reg, compose.Options{}); err != nil {
		t.Fatalf("compose.Register: %v", err)
	}
	if floor, ok := reg.Floor(opComposeUp); ok {
		floors[opComposeUp] = floor
	}
	ids := map[string]bool{}
	for _, c := range testDriver().Checks() {
		if ids[c.ID] {
			t.Errorf("duplicate check id %s", c.ID)
		}
		ids[c.ID] = true
		if c.Read == nil || !slices.Contains([]string{readSettings, readRoleInfo}, c.ReadOp) || !sent[c.ReadOp] {
			t.Errorf("%s: read op %q is not a source read remoteclients sends", c.ID, c.ReadOp)
		}
		if !plan.ValidTier(c.TierFloor) {
			t.Errorf("%s: tier floor %q is not a tier", c.ID, c.TierFloor)
		}
		if c.FixOp == "" {
			t.Errorf("%s: names no fix op", c.ID)
			continue
		}
		floor, ok := floors[c.FixOp]
		if !ok {
			t.Errorf("%s: fix op %q is not a registered op", c.ID, c.FixOp)
		} else if floor != c.TierFloor {
			t.Errorf("%s: floor %q, but op %q's own floor is %q", c.ID, c.TierFloor, c.FixOp, floor)
		}
	}
}
