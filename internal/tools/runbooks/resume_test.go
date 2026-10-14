package runbooks

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The slot-inactive incident (connector STOPPED or PAUSED, slot inactive) is
// fixed by proposing connect_resume_connector through the approval flow. The
// runbook must name that action, say who approves it, and give the rollback.
func TestSlotWalProposesResumeWithRollback(t *testing.T) {
	var rb Runbook
	found := false
	for _, r := range embedded(t) {
		if r.ID == "slot-wal" {
			rb, found = r, true
		}
	}
	if !found {
		t.Fatal("slot-wal runbook missing")
	}
	if !slices.Contains(rb.Actions, "connect_resume_connector") {
		t.Fatalf("slot-wal actions = %v, want connect_resume_connector", rb.Actions)
	}
	if !slices.Contains(rb.Actions, "connect_restart_connector") {
		t.Errorf("slot-wal actions = %v: restart for FAILED tasks must stay listed", rb.Actions)
	}
	b, err := os.ReadFile(filepath.Join("library", "slot-wal.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	for _, want := range []string{
		"Action `connect_resume_connector`",
		"approval flow",
		"kafka-connect owner approves",
		"Rollback is manual, because no pause or stop action exists",
		"`PUT /connectors/{name}/stop`",
		"`PUT /connectors/{name}/pause`",
		"Resuming a connector that someone stopped on purpose",
		"Rollback is manual and not a clean undo",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("slot-wal.md lacks %q", want)
		}
	}
}
