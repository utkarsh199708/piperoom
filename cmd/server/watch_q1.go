package main

// Q1 (QA G1) extensions of the watcher adapter: the open room's age and
// origin for the join rule, evidence as data for the agent, and a different
// incident joining a room (which wakes or starts its agent).

import (
	"context"
	"encoding/json"

	"piperoom/internal/room"
	"piperoom/internal/watch"
)

// watchIncidentService is the part of *room.Service the Q1 adapter methods use.
type watchIncidentService interface {
	OpenRoomInfo(pipelineID string) (room.WatchRoomInfo, bool)
	PostRoomEvidence(ctx context.Context, roomID, text string, evidence json.RawMessage) error
	PostIncidentJSON(ctx context.Context, roomID, text string, evidence json.RawMessage) error
}

var (
	_ watch.RoomInfoSink = watchRooms{}
	_ watch.EvidenceSink = watchRooms{}
	_ watch.IncidentSink = watchRooms{}
)

// OpenRoomInfo describes the pipeline's open room for the watcher's join rule.
func (r watchRooms) OpenRoomInfo(pipelineID string) (watch.RoomInfo, bool) {
	info, ok := r.svc.OpenRoomInfo(pipelineID)
	if !ok {
		return watch.RoomInfo{}, false
	}
	return watch.RoomInfo{
		RoomID:      info.RoomID,
		AlertSource: info.AlertSource,
		OpenedAt:    info.OpenedAt,
		LastEventAt: info.LastEventAt,
	}, true
}

// PostEvidenceJSON posts a note with its evidence, which the room's agent
// gets as a quiet alert update. Errors map as in PostEvidence.
func (r watchRooms) PostEvidenceJSON(ctx context.Context, roomID, text string, evidence json.RawMessage) error {
	return watchPostErr(r.svc.PostRoomEvidence(ctx, roomID, text, evidence))
}

// PostIncidentJSON posts a note for a different incident joining the room; the
// service wakes the room's agent or starts a run. Errors map as in
// PostEvidence.
func (r watchRooms) PostIncidentJSON(ctx context.Context, roomID, text string, evidence json.RawMessage) error {
	return watchPostErr(r.svc.PostIncidentJSON(ctx, roomID, text, evidence))
}
