package room

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"piperoom/internal/store"
)

// Postgres stores room.created in a jsonb column and the room title in a text
// column. Both refuse some input that Go's JSON handling accepts: jsonb rejects
// a \u0000 escape and an unpaired surrogate escape, and neither type holds a
// NUL byte or invalid UTF-8. A body like that must be refused with a 400 before
// any write, otherwise the room row is inserted and the event append then
// fails on real Postgres (memstore accepts everything and hides this).
//
// Two layers cover this. store.CheckJSON is the fast path for the string and
// encoding classes (NUL, UTF-8, surrogates): it refuses before any write. It
// does not try to model jsonb's numeric limits (a number like 1e131072 is
// "value overflows numeric format"); those and anything else Postgres rejects
// as bad input are caught when room.created is appended, and mapUnstorable
// turns that error into the same 400. The room row written just before is
// removed by discardNewRoomLocked.

// mapUnstorable turns a store error caused by an unstorable value into a 400
// validation error; any other error is returned unchanged. The Postgres message
// is not echoed: it can quote the value. SQLSTATE 22P02 is not mapped: after
// the pre-checks it means our own bug and stays an internal error.
func mapUnstorable(err error) error {
	if store.IsUnstorablePGError(err) {
		return validation("alert body cannot be stored (value out of range or not valid text)")
	}
	if errors.Is(err, store.ErrUnstorable) {
		return validation(unstorableTextMsg)
	}
	return err
}

// checkJSONBStorable reports why raw cannot be stored in a jsonb column, or nil.
// raw is JSON text (the alert body as received). The string and encoding
// classes come from store.CheckJSON, the same check the store runs before an
// INSERT, so the two cannot drift apart.
func checkJSONBStorable(raw []byte) error {
	if err := store.CheckJSON(raw); err != nil {
		return validation("alert body cannot be stored (NUL, invalid UTF-8 or an unpaired surrogate)")
	}
	// jsonb stores 1e400, but it is not finite as a float64: the audit export's
	// canonical form (RFC 8785) writes numbers as doubles, so refuse it here.
	// Rooms that already hold such a number still export (see service_export.go).
	if hasOutOfRangeNumber(raw) {
		return validation("alert body contains a number outside the double range (for example 1e400)")
	}
	return nil
}

// unstorableTextMsg is the 400 message for user text Postgres cannot hold.
const unstorableTextMsg = "text contains characters that cannot be stored"

// replacementChar stands in for bytes agent text cannot store (U+FFFD).
const replacementChar = string(rune(0xFFFD))

// rejectUnstorable is the policy for text a person typed (comment, steer,
// resolution, trace key, reject reason, room title): if any field holds a NUL or
// invalid UTF-8 the request is refused with a 400 validation error before any
// write. Nothing is changed, because the person should see that the text did
// not go through. The message never quotes the text.
func rejectUnstorable(fields ...string) error {
	if err := store.CheckText(fields...); err != nil {
		return validation(unstorableTextMsg)
	}
	return nil
}

// sanitizeAgentText is the policy for text a model or a tool produced (agent
// steps, hypothesis, proposal fields, hand-off summary, action output): NUL and
// invalid UTF-8 become U+FFFD, so an agent run never fails on bad tool output.
func sanitizeAgentText(s string) string {
	if err := store.CheckText(s); err == nil {
		return s
	}
	return strings.ReplaceAll(strings.ToValidUTF8(s, replacementChar), "\x00", replacementChar)
}

// sanitizeAgentPayload applies the agent policy to an event payload of any
// shape: it is marshalled and every NUL byte, \u0000 escape or lone-surrogate escape and
// every invalid UTF-8 byte in the JSON becomes U+FFFD. The result is the
// marshalled payload as a json.RawMessage, ready for Append. This also covers
// json.RawMessage fields (tool input, tool result data), which pass through
// json.Marshal unchanged.
func sanitizeAgentPayload(payload any) (any, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("room: marshal agent payload: %w", err)
	}
	return json.RawMessage(store.SanitizeJSON(raw)), nil
}
