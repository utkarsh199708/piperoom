package room

import "errors"

// ErrResolvedAlertNoRoom is returned by CreateRoomFromAlert for an alert the
// source reports resolved that matches no open room. A resolved notice never
// opens a room, so nothing is stored, no agent starts and no hook runs. The
// HTTP layer answers 202 Accepted with "ignored": true (docs/api.md).
var ErrResolvedAlertNoRoom = errors.New("resolved alert with no open room")

// Error is a domain error that the HTTP layer maps to a status code and a
// stable machine-readable code (docs/api.md).
type Error struct {
	Status int    // HTTP status
	Code   string // machine code, e.g. "not_driver"
	Msg    string // human message
}

func (e *Error) Error() string { return e.Msg }

// Error codes from docs/api.md.
const (
	codeValidation     = "validation"
	codeNotDriver      = "not_driver"
	codeNotLayerOwner  = "not_layer_owner"
	codeNotFound       = "not_found"
	codeAlreadyDecided = "already_decided"
	codeRoomClosed     = "room_closed"
	codeStaleHandoff   = "stale_handoff"
	codeLeaseHeld      = "lease_held"
)

// errConflict is 409 conflict: another replica changed the room between the
// check and the write of a checked command three times in a row
// (docs/design/replicas.md section 9.1). The client may retry.
func errConflict() *Error {
	return &Error{Status: 409, Code: "conflict", Msg: "the room changed on another server while the request was checked; retry"}
}

func validation(msg string) *Error {
	return &Error{Status: 400, Code: codeValidation, Msg: msg}
}

func notFound(msg string) *Error {
	return &Error{Status: 404, Code: codeNotFound, Msg: msg}
}

// ErrNotDriver is "Only the driver can send steers."
func ErrNotDriver() *Error {
	return &Error{Status: 403, Code: codeNotDriver, Msg: "Only the driver can do that."}
}

func notLayerOwner(msg string) *Error {
	return &Error{Status: 403, Code: codeNotLayerOwner, Msg: msg}
}

func notRecipient(msg string) *Error {
	return &Error{Status: 403, Code: "not_recipient", Msg: msg}
}

func alreadyDecided(msg string) *Error {
	return &Error{Status: 409, Code: codeAlreadyDecided, Msg: msg}
}

func staleHandoff(msg string) *Error {
	return &Error{Status: 409, Code: codeStaleHandoff, Msg: msg}
}

// handoffInProgress is 409 handoff_in_progress: a hand-off request of the room
// is in flight (docs/api.md, Hand-off requests).
func handoffInProgress() *Error {
	return &Error{Status: 409, Code: "handoff_in_progress", Msg: "a hand-off for this room is already being written; wait for it to finish"}
}

func roomClosed(msg string) *Error {
	return &Error{Status: 409, Code: codeRoomClosed, Msg: msg}
}

// leaseHeld is "control/grant by a non-driver while another user's lease is
// still live" (docs/api.md error table).
func leaseHeld(msg string) *Error {
	return &Error{Status: 409, Code: codeLeaseHeld, Msg: msg}
}

// paramsMismatch is "the params_hash the approver sent is not the proposal's"
// (docs/design/auth.md section 3.2). The stored steps are frozen, so this means
// a stale or wrong client; the fix is to reload the room and review again.
func paramsMismatch(msg string) *Error {
	return &Error{Status: 409, Code: "params_mismatch", Msg: msg}
}
