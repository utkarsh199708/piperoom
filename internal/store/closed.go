package store

import "piperoom/internal/events"

// closedRefused are the event types a closed room no longer takes: everything
// a person does to steer, decide or hand over the investigation. Close-out events still land after room.closed: the agent's last
// status and messages (the run is being cancelled), the executor's results of a
// step already running, retention and log-cap notices, room.extended and
// room.deleted (admin actions on closed rooms) and the shadow reveal, label
// and grade.
var closedRefused = map[events.Type]bool{
	events.CommentAdded:     true,
	events.SteerSuggested:   true,
	events.SteerSent:        true,
	events.ControlRequested: true,
	events.ControlGranted:   true,
	events.ProposalCreated:  true,
	events.ApprovalGiven:    true,
	events.ApprovalRejected: true,
	events.HandoffCreated:   true,
	events.HandoffAccepted:  true,
	events.TracePinned:      true,
}

// RefusedWhenClosed reports whether Append refuses typ once the room is
// closed. The check runs in the same locked UPDATE of the rooms row as the seq
// assignment, and room.closed sets the status in that same UPDATE, so no
// replica can append one of these after room.closed has landed.
func RefusedWhenClosed(typ events.Type) bool { return closedRefused[typ] }
