package main

import (
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/watch"
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

// watchStores gives the watcher the room-store tables it writes, each only
// when the store has it: samples (X3a), status rows (X5a, read by the
// fleet), connector config versions (X5c) and the change feed (F8c), so the
// fleet's changes_24h and a room's checklist include the connector config and
// state changes the watcher sees. The change feed diffs only what the tick
// already reads (connector status and config); Registry and DDL stay unset,
// so wiring it adds no read of a customer system.
func watchStores(d *watch.Deps, st store.Store) {
	if s, ok := st.(store.SampleStore); ok {
		d.Samples = s
	}
	if s, ok := st.(store.FleetStore); ok {
		d.Status = s
	}
	if s, ok := st.(store.ConfigVersionStore); ok {
		d.ConfigVersions = s
	}
	if s, ok := st.(clients.ChangeWriter); ok {
		d.Changes = s
	}
}
