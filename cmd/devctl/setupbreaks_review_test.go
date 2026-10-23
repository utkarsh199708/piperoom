package main

import (
	"context"
	"strings"
	"testing"
)

// Postgres does not start at wal_level=replica while any logical slot
// exists; the break drops only the dev connector's slot, so another slot
// must stop it before anything changes.
func TestWALLevelBreakRefusesOtherLogicalSlots(t *testing.T) {
	w := healthySetupWorld()
	w.otherSlots = 1
	s := testSetupStack(t, w)
	err := setupBreakCmd(context.Background(), s, "wal-level-replica")
	if err == nil || !strings.Contains(err.Error(), "other logical slot") {
		t.Fatalf("break = %v", err)
	}
	if len(w.composeCall) != 0 || !w.slotExists || w.walLevel != "logical" {
		t.Fatalf("the refused break changed the stack: compose %v slot %v wal %q", w.composeCall, w.slotExists, w.walLevel)
	}
}

// A check that cannot run (Postgres down) does not block the fix that brings
// the component back.
func TestSetupFixRunsWhenTheCheckCannotRun(t *testing.T) {
	w := healthySetupWorld()
	s := testSetupStack(t, w)
	ctx := context.Background()
	if err := setupBreakCmd(ctx, s, "wal-level-replica"); err != nil {
		t.Fatal(err)
	}
	w.walLevel = "" // the server did not come back: SHOW wal_level is refused
	if err := setupFixCmd(ctx, s, "wal-level-replica"); err != nil {
		t.Fatalf("fix with Postgres down: %v", err)
	}
	if w.walLevel != "logical" {
		t.Fatalf("wal_level = %q after the fix", w.walLevel)
	}
}
