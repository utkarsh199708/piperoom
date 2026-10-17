package main

// Q3 extensions of the watcher adapter: what a closed room carried (its
// incident keys) and whether its title is flagged by injectscan.

import "piperoom/internal/watch"

// watchQ3Service is the part of *room.Service the Q3 adapter methods use.
type watchQ3Service interface {
	RoomSignalKeys(roomID string) ([]string, bool)
	RoomTitleFlags(roomID string) ([]string, bool)
}

var (
	_ watch.ClosedRoomKeysSink = watchRooms{}
	_ watch.RoomTitleFlagsSink = watchRooms{}
)

// RoomSignalKeys returns the incident keys the room carried.
func (r watchRooms) RoomSignalKeys(roomID string) ([]string, bool) {
	return r.svc.RoomSignalKeys(roomID)
}

// RoomTitleFlags returns the injectscan kinds of the room's title.
func (r watchRooms) RoomTitleFlags(roomID string) ([]string, bool) {
	return r.svc.RoomTitleFlags(roomID)
}
