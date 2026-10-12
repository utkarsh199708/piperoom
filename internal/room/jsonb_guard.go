package room

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
)

// Postgres stores room.created in a jsonb column and the room title in a text
// column. Both refuse some input that Go's JSON handling accepts: jsonb rejects
// a \u0000 escape and an unpaired surrogate escape, and neither type holds a
// NUL byte or invalid UTF-8. A body like that must be refused with a 400 before
// any write, otherwise the room row is inserted and the event append then
// fails on real Postgres (memstore accepts everything and hides this).
//
// Two layers cover this. The scanner below is the fast path for the string and
// encoding classes (NUL, UTF-8, surrogates): it refuses before any write. It
// does not try to model jsonb's numeric limits (a number like 1e131072 is
// "value overflows numeric format"); those and anything else Postgres rejects
// as bad input are caught when room.created is appended, and mapUnstorable
// turns that error into the same 400. The room row written just before is
// removed by discardNewRoomLocked.

// unstorableCodes are the Postgres SQLSTATEs for "this value cannot be stored":
// invalid text representation, unsupported Unicode escape, invalid byte
// sequence, numeric value out of range.
var unstorableCodes = map[string]bool{"22P02": true, "22P05": true, "22021": true, "22003": true}

// mapUnstorable turns a store error caused by an unstorable value into a 400
// validation error; any other error is returned unchanged. The Postgres message
// is not echoed: it can quote the value.
func mapUnstorable(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && unstorableCodes[pgErr.Code] {
		return validation("alert body cannot be stored (value out of range or not valid text)")
	}
	return err
}

// checkJSONBStorable reports why raw cannot be stored in a jsonb column, or nil.
// raw is JSON text (the alert body as received).
func checkJSONBStorable(raw []byte) error {
	if !utf8.Valid(raw) {
		return validation("alert body is not valid UTF-8")
	}
	if bytes.IndexByte(raw, 0) >= 0 {
		return validation("alert body contains a NUL byte")
	}
	// jsonb stores 1e400, but it is not finite as a float64: the audit export's
	// canonical form (RFC 8785) writes numbers as doubles, so refuse it here.
	// Rooms that already hold such a number still export (see service_export.go).
	if hasOutOfRangeNumber(raw) {
		return validation("alert body contains a number outside the double range (for example 1e400)")
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		if i+1 >= len(raw) {
			return nil // a trailing backslash is not valid JSON; the caller rejects that
		}
		if raw[i+1] != 'u' {
			i++ // skip the escaped character, so "\\u0000" (an escaped backslash) is not seen as an escape
			continue
		}
		code, ok := hexEscape(raw, i+2)
		if !ok {
			continue // malformed escape: not valid JSON, rejected elsewhere
		}
		switch {
		case code == 0:
			return validation(`alert body contains a \u0000 escape`)
		case code >= 0xD800 && code <= 0xDBFF:
			low, ok := uint16(0), false
			if i+11 < len(raw) && raw[i+6] == '\\' && raw[i+7] == 'u' {
				var l int
				l, ok = hexEscape(raw, i+8)
				low = uint16(l)
			}
			if !ok || low < 0xDC00 || low > 0xDFFF {
				return validation("alert body contains an unpaired surrogate escape")
			}
			i += 11 // both escapes of the pair
		case code >= 0xDC00 && code <= 0xDFFF:
			return validation("alert body contains an unpaired surrogate escape")
		default:
			i += 5
		}
	}
	return nil
}

// hexEscape reads the 4 hex digits at raw[at:at+4].
func hexEscape(raw []byte, at int) (int, bool) {
	if at+4 > len(raw) {
		return 0, false
	}
	n, err := strconv.ParseUint(string(raw[at:at+4]), 16, 16)
	if err != nil {
		return 0, false
	}
	return int(n), true
}

// checkTextStorable reports why s cannot go into a Postgres text column or a
// jsonb string, or nil: no NUL and valid UTF-8.
func checkTextStorable(field, s string) error {
	if !utf8.ValidString(s) {
		return validation(fmt.Sprintf("%s is not valid UTF-8", field))
	}
	if strings.IndexByte(s, 0) >= 0 {
		return validation(fmt.Sprintf("%s contains a NUL character", field))
	}
	return nil
}
