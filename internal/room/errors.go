package room

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

func roomClosed(msg string) *Error {
	return &Error{Status: 409, Code: codeRoomClosed, Msg: msg}
}

// leaseHeld is "control/grant by a non-driver while another user's lease is
// still live" (docs/api.md error table).
func leaseHeld(msg string) *Error {
	return &Error{Status: 409, Code: codeLeaseHeld, Msg: msg}
}
