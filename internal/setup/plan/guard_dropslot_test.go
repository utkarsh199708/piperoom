package plan_test

import (
	"testing"

	"piperoom/internal/actions/pgactions"
	"piperoom/internal/setup/plan"
)

// TestGuardForbidsRegisteredDropSlot: the drop-slot action as it is registered
// (pgactions.DropSlotName) is forbidden on a production target. The guard
// first named only source_drop_slot, so a production step carrying the
// registered name matched no row and escaped plan.Forbidden.
func TestGuardForbidsRegisteredDropSlot(t *testing.T) {
	if plan.OpPGDropReplicationSlot != pgactions.DropSlotName {
		t.Fatalf("guard op %q != registered action %q", plan.OpPGDropReplicationSlot, pgactions.DropSlotName)
	}
	row, forbidden := plan.Forbidden(plan.GuardRequest{Op: pgactions.DropSlotName, Class: plan.ClassProduction})
	if !forbidden || row.Reason != plan.ReasonDropSlot {
		t.Fatalf("Forbidden(%s on production) = %+v, %v; want refused with %s", pgactions.DropSlotName, row, forbidden, plan.ReasonDropSlot)
	}
	for _, class := range []string{plan.ClassLocal, plan.ClassShared} {
		if _, forbidden := plan.Forbidden(plan.GuardRequest{Op: pgactions.DropSlotName, Class: class}); forbidden {
			t.Errorf("Forbidden(%s on %s) = true; the row is production only", pgactions.DropSlotName, class)
		}
	}
}
