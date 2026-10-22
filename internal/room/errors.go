package room

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// compensateCtx is the context for undoing a side-table write whose event
// failed to append. The usual cause of that failure is the request context
// ending (client gone, request timeout); undoing on that same context fails at
// once and leaves the row with no event, e.g. an approval row that answers
// already_decided to every retry. Caller defers cancel.
func compensateCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

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
	// kind marks an Error that code must tell apart without reading Msg
	// (errors.Is); nil for most.
	kind error
}

func (e *Error) Error() string { return e.Msg }

// Unwrap returns the Error's kind marker, if any.
func (e *Error) Unwrap() error { return e.kind }

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

// afterAppend is the one rule for a write that follows an event already in the
// room log: the log is the record, so once the event has landed the command
// has happened. A failed follow-up (a side-table row such as the driver,
// handoff, suggestion or proposal status, or an event that only completes the
// first one) is logged with the room id and the call carries on: an error
// there would make the client retry against a room that already changed. What
// the write missed is reconciled from the log by whoever reads it next
// (AcceptHandoff for handoff rows, the snapshot for suggestions, rebuild for
// the driver). Only an event that is itself the call's answer (handoff.accepted)
// returns its error, and the retry finishes the command. A write made before
// the event is the opposite case: it fails the call, and the row is compensated.
func (s *Service) afterAppend(roomID, what string, err error, attrs ...any) {
	if err == nil {
		return
	}
	s.log.Warn("room: "+what+" failed after the event was logged", append([]any{"room", roomID, "err", err}, attrs...)...)
}

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

// noHypothesis is 409 no_hypothesis: the agent has not stated one yet (F9).
func noHypothesis() *Error {
	return &Error{Status: 409, Code: "no_hypothesis", Msg: "there is no hypothesis to decide on yet"}
}

// alreadyNudged is 409 already_nudged; msg says how long ago (F9).
func alreadyNudged(msg string) *Error {
	return &Error{Status: 409, Code: "already_nudged", Msg: msg}
}

// tooManyProposals is 429 too_many_proposals: a sixth open driver-authored
// proposal in one room (docs/api.md; P3c).
func tooManyProposals() *Error {
	return &Error{Status: http.StatusTooManyRequests, Code: "too_many_proposals", Msg: "at most 5 open driver-authored proposals per room"}
}

// ---- Setup rooms (U wave; docs/api.md, "Setup rooms") ----

// setupDisabled is 404 setup_disabled: every setup route and
// /api/runners/ws while PIPEROOM_SETUP_ENABLED is off.
func setupDisabled() *Error {
	return &Error{Status: http.StatusNotFound, Code: "setup_disabled", Msg: "setup is not enabled on this server"}
}

// notTenantMember is 403 not_tenant_member: POST /api/setup for a tenant the
// caller is not in (or one no tenants: block declares, which is the same
// answer so a tenant name does not leak).
func notTenantMember() *Error {
	return &Error{Status: http.StatusForbidden, Code: "not_tenant_member", Msg: "you are not a member of that tenant"}
}

// pipelineExists is 409 pipeline_exists: POST /api/setup for a pipeline
// owners.yaml already defines, or one an open setup room is already building.
func pipelineExists(id string) *Error {
	return &Error{Status: http.StatusConflict, Code: "pipeline_exists", Msg: "pipeline " + id + " already exists"}
}

// notSetupRoom is 400 not_setup_room: a setup route on an incident room.
func notSetupRoom() *Error {
	return &Error{Status: http.StatusBadRequest, Code: "not_setup_room", Msg: "this room is not a setup room"}
}

// addressRefused is 400 address_refused: a target address on the denylist of
// design §2.6. The message carries no part of the address (a URL form may hold
// credentials).
func addressRefused() *Error {
	return &Error{Status: http.StatusBadRequest, Code: "address_refused", Msg: "the target address is not allowed: cloud metadata endpoints, link-local, multicast, unspecified and loopback addresses of another host are refused, as are this server's own addresses"}
}

// classLowered is 400 class_lowered: a class below the one the server derived
// (a class is a floor that only rises).
func classLowered(msg string) *Error {
	return &Error{Status: http.StatusBadRequest, Code: "class_lowered", Msg: msg}
}

// windowTooLong is 400 window_too_long: a maintenance window over 4 hours or
// more than 24 hours ahead.
func windowTooLong(msg string) *Error {
	return &Error{Status: http.StatusBadRequest, Code: "window_too_long", Msg: msg}
}
