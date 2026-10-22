package main

import (
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/store"
)

// fleetOptions wires the fleet routes (X5a) to the room store: the watcher's
// status rows and the change pages come from fs (nil when the store has no
// fleet tables: health is then unknown and the change pages empty), open
// rooms and last verdicts from the room tables. No customer client is
// passed: the fleet never reads one.
func fleetOptions(own *owners.Owners, st store.Store, fs store.FleetStore, watch config.Watch, changesDays int) httpapi.Fleet {
	f := httpapi.Fleet{
		Owners:               own,
		Rooms:                st,
		WatchOff:             watch.Mode == config.WatchOff,
		WatchInterval:        watch.Interval,
		ChangesRetentionDays: changesDays,
	}
	if closed, ok := st.(store.ClosedRoomReader); ok {
		f.Closed = closed
	}
	if fs != nil {
		f.Status, f.Changes, f.Counts = fs, fs, fs
	}
	return f
}
