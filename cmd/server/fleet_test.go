package main

import (
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// The fleet routes read the room store only: status, changes and counts from
// its fleet tables, rooms and verdicts from its room tables (X5a).
func TestFleetOptions(t *testing.T) {
	st := memstore.New()
	fs, _ := any(st).(store.FleetStore)
	f := fleetOptions(nil, st, fs, config.Watch{Mode: config.WatchRooms, Interval: 30 * time.Second}, 14)
	if f.Status == nil || f.Changes == nil || f.Counts == nil || f.Rooms == nil || f.Closed == nil {
		t.Fatalf("readers not wired: %+v", f)
	}
	if f.WatchOff || f.WatchInterval != 30*time.Second || f.ChangesRetentionDays != 14 {
		t.Fatalf("settings = %+v", f)
	}
	off := fleetOptions(nil, st, nil, config.Watch{Mode: config.WatchOff}, 30)
	if !off.WatchOff || off.Status != nil || off.Changes != nil || off.Counts != nil {
		t.Fatalf("watch off / no fleet store = %+v", off)
	}
}
