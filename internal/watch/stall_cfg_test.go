package watch

import (
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// The owners.yaml numbers drive the reading, not code constants.
func TestSlotStallReadingUsesTunedSettings(t *testing.T) {
	sc := owners.DefaultWatch().SlotStall
	sc.StallAfter, sc.FireBytes, sc.GrowthBytes = 5*time.Minute, 2*mib, 512<<10
	var m stallMem
	row := clients.ReplicationSlot{Name: "s", Active: true, RestartLSN: "0/10", ConfirmedFlushLSN: "0/10", RetainedWALBytes: 1 * mib}
	slotStallReading(t0, sc, "s", &m, row, heartbeat{})
	row.RetainedWALBytes = 3 * mib
	if r := slotStallReading(t0.Add(4*time.Minute), sc, "s", &m, row, heartbeat{}); r.v != vClear {
		t.Fatalf("4m: verdict %v, want clear", r.v)
	}
	if r := slotStallReading(t0.Add(5*time.Minute), sc, "s", &m, row, heartbeat{}); r.v != vBreach {
		t.Fatalf("5m: verdict %v, want breach", r.v)
	}
	// Defaults would hold it: 3 MiB is under 64 MiB.
	var m2 stallMem
	d := owners.DefaultWatch().SlotStall
	slotStallReading(t0, d, "s", &m2, clients.ReplicationSlot{RestartLSN: "0/10", ConfirmedFlushLSN: "0/10", RetainedWALBytes: 1 * mib}, heartbeat{})
	if r := slotStallReading(t0.Add(time.Hour), d, "s", &m2, row, heartbeat{}); r.v != vHold {
		t.Fatalf("defaults: verdict %v, want hold", r.v)
	}
}
