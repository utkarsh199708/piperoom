package watch

import (
	"context"
	"encoding/json"
	"time"
)

// RoomInfo describes a pipeline's open room for the join decision (Q1, QA G1).
type RoomInfo struct {
	RoomID string
	// AlertSource is room.created.alert_source: "watcher", a signed source or
	// "webhook"; "" for a room a person opened.
	AlertSource string
	OpenedAt    time.Time
	// LastEventAt is the time of the room's newest event.
	LastEventAt time.Time
}

// RoomInfoSink is an optional RoomSink extension (Q1). With it the watcher
// joins an open room only when its LastEventAt is younger than
// PIPEROOM_WATCH_JOIN_MAX_AGE and it came from an alert or the watcher, or a
// person opened it less than that long ago; otherwise the watcher opens its
// own room. Without it the watcher keeps the old rule (OpenRoomForPipeline).
type RoomInfoSink interface {
	OpenRoomInfo(pipelineID string) (RoomInfo, bool)
}

// IncidentSink is an optional RoomSink extension (Q1): a join whose signals
// are new to the room (a different incident). The service feeds the evidence
// to the agent as an alert_update with NewIncident set, which wakes an idle
// agent and starts a new run when none is running. evidence may be nil. Same
// errors as PostEvidence. Repeats of signals the room already carries keep
// using PostEvidenceJSON / PostEvidence.
type IncidentSink interface {
	PostIncidentJSON(ctx context.Context, roomID, text string, evidence json.RawMessage) error
}
