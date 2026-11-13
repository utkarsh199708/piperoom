package room

import (
	"encoding/json"
	"testing"

	"piperoom/internal/owners"
)

func setupListSnap() *RoomSnapshot {
	snap := listRowSnap()
	snap.Setup = &SetupSnapshot{
		Mode:    true,
		Phase:   "build",
		Runner:  &SetupRunnerState{Label: "laptop", Connected: true, Tools: map[string]string{"psql": "ok"}},
		PlanID:  "plan_1",
		Targets: []SetupTargetState{{Address: "db.acme:5432"}},
		tenant:  "a",
	}
	return snap
}

// KAN-602: a live frame's setup member is the list form (mode, phase,
// runner.connected), built on a copy; the shared snapshot keeps its full one.
func TestListRowTrimsSetupOnACopy(t *testing.T) {
	snap := setupListSnap()
	full := snap.Setup
	for _, svc := range []*Service{{}, {writeOff: true}} {
		got := svc.ListRow(snap)
		if got == snap {
			t.Fatal("ListRow returned the shared snapshot though its setup member needed trimming")
		}
		if snap.Setup != full || full.list || full.Runner.Label != "laptop" || full.PlanID != "plan_1" || len(full.Targets) != 1 {
			t.Fatalf("shared setup member was written: %+v", full)
		}
		b, err := json.Marshal(got.Setup)
		if err != nil {
			t.Fatal(err)
		}
		if want := `{"mode":true,"runner":{"connected":true},"phase":"build"}`; string(b) != want {
			t.Errorf("setup member = %s, want %s", b, want)
		}
	}
	// Blocked is still marked, once, on the same copy.
	got := (&Service{writeOff: true}).ListRow(snap)
	if got.OpenProposals[0].Blocked != BlockedWriteModeOff || snap.OpenProposals[0].Blocked != "" {
		t.Errorf("blocked: row %q, shared %q", got.OpenProposals[0].Blocked, snap.OpenProposals[0].Blocked)
	}
	// A resync row is already trimmed: it goes out as it is.
	if again := (&Service{}).ListRow((&Service{}).ListRow(snap)); again.Setup.list != true {
		t.Error("trimmed row lost its list form")
	}
	row := (&Service{}).ListRow(snap)
	if (&Service{}).ListRow(row) != row {
		t.Error("ListRow copied a row that was already trimmed")
	}
	// An incident room has no setup member to trim.
	inc := listRowSnap()
	if (&Service{}).ListRow(inc) != inc {
		t.Error("ListRow copied an incident room")
	}
}

// KAN-602: one visibility rule for a setup room, trimmed or not: its own
// tenant, which the trimmed row keeps.
func TestCanSeeRoomSetupTenantSurvivesTrim(t *testing.T) {
	own := &owners.Owners{Tenants: map[string]owners.Tenant{
		"a": {Members: owners.Principals{{Kind: owners.KindUser, Name: "alice"}}},
		"b": {Members: owners.Principals{{Kind: owners.KindUser, Name: "bob"}}},
	}}
	svc := &Service{owners: own}
	snap := setupListSnap()
	snap.PipelineID = "not-in-owners"
	trimmed := *snap
	trimSetupForList(&trimmed) // a ListRoomsFor row, which a resync passes on
	rows := map[string]*RoomSnapshot{"published": snap, "row": svc.ListRow(snap), "trimmed": &trimmed}
	for name, r := range rows {
		if !svc.CanSeeRoom(User{ID: "alice"}, r) {
			t.Errorf("%s: tenant member cannot see their setup room", name)
		}
		if svc.CanSeeRoom(User{ID: "bob"}, r) {
			t.Errorf("%s: another tenant sees the setup room", name)
		}
	}
}
