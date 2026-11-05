package main

import (
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/watch"
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

// The watcher writes every room-store table it has: samples, status rows,
// config versions and the change feed, so changes_24h counts the connector
// changes it sees. A store without them leaves each off.
func TestWatchStores(t *testing.T) {
	var d watch.Deps
	watchStores(&d, memstore.New())
	if d.Samples == nil || d.Status == nil || d.ConfigVersions == nil || d.Changes == nil {
		t.Fatalf("watch stores not wired: %+v", d)
	}
	if d.Registry != nil || d.DDL != nil {
		t.Fatal("the change feed must not add registry or DDL reads")
	}
	var bare watch.Deps
	watchStores(&bare, roomOnlyStore{})
	if bare.Samples != nil || bare.Status != nil || bare.ConfigVersions != nil || bare.Changes != nil {
		t.Fatalf("a store without the tables wired %+v", bare)
	}
}

// roomOnlyStore is a store.Store with none of the watcher's tables.
type roomOnlyStore struct{ store.Store }
