package room

import "testing"

func listRowSnap() *RoomSnapshot {
	snap := NewSnapshot()
	snap.ID = "r_1"
	snap.OpenProposals = []Proposal{
		{ProposalID: "p_done", RequiredLayers: []string{"a", "b"}, Approvals: map[string]string{"a": "approved", "b": "approved"}},
		{ProposalID: "p_wait", RequiredLayers: []string{"a", "b"}, Approvals: map[string]string{"a": "approved", "b": "pending"}},
		{ProposalID: "p_none"},
	}
	return &snap
}

// KAN-596: ListRow marks a fully approved proposal on a copy and leaves the
// shared snapshot alone; with write mode on, or nothing to mark, it returns
// the snapshot itself.
func TestListRowMarksACopy(t *testing.T) {
	snap := listRowSnap()
	got := (&Service{writeOff: true}).ListRow(snap)
	if got == snap {
		t.Fatal("ListRow returned the shared snapshot though a proposal needed marking")
	}
	if b := snap.OpenProposals[0].Blocked; b != "" {
		t.Fatalf("shared snapshot was written: blocked = %q", b)
	}
	for i, want := range []string{BlockedWriteModeOff, "", ""} {
		if g := got.OpenProposals[i].Blocked; g != want {
			t.Errorf("proposal %d blocked = %q, want %q", i, g, want)
		}
	}
	// A row that already carries it (a resync row) is left as it is.
	if again := (&Service{writeOff: true}).ListRow(got); again != got {
		t.Error("ListRow copied a row that was already marked")
	}
	if on := (&Service{}).ListRow(snap); on != snap {
		t.Error("ListRow copied with write mode on")
	}
	nothing := &RoomSnapshot{OpenProposals: snap.OpenProposals[1:]}
	if (&Service{writeOff: true}).ListRow(nothing) != nothing {
		t.Error("ListRow copied with nothing to mark")
	}
}
