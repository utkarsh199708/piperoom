package store

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/events"
)

// ErrUnstorable is returned (wrapped) by a store write whose content Postgres
// cannot hold: a NUL (raw byte or \u0000 escape), invalid UTF-8, or a lone
// surrogate escape in JSON. jsonb refuses all three and text columns refuse the
// first two. The check runs before the INSERT in both *PG and memstore, so a
// refused write changes nothing (no seq is reserved, no row is written) and the
// two stores behave the same. The error text never quotes the content.
var ErrUnstorable = errors.New("store: content cannot be stored (NUL, invalid UTF-8 or lone surrogate)")

// unstorableCodes are the Postgres SQLSTATEs for "this value cannot be stored":
// invalid text representation, unsupported Unicode escape, invalid byte
// sequence, numeric value out of range. A write that gets past the pre-check
// and still fails with one of them is wrapped in ErrUnstorable too.
var unstorableCodes = map[string]bool{"22P02": true, "22P05": true, "22021": true, "22003": true}

// classify wraps err in ErrUnstorable when it is a Postgres "bad value" error.
// The original error stays in the chain (errors.As finds the *pgconn.PgError).
func classify(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && unstorableCodes[pgErr.Code] {
		return fmt.Errorf("%w: %w", ErrUnstorable, err)
	}
	return err
}

// CheckText returns an error wrapping ErrUnstorable if any of fields is not
// valid UTF-8 or holds a NUL. It is the pre-check for text columns.
func CheckText(fields ...string) error {
	for _, f := range fields {
		if !utf8.ValidString(f) {
			return fmt.Errorf("%w: text is not valid UTF-8", ErrUnstorable)
		}
		for i := 0; i < len(f); i++ {
			if f[i] == 0 {
				return fmt.Errorf("%w: text contains a NUL", ErrUnstorable)
			}
		}
	}
	return nil
}

// CheckAppend is the pre-check for an event write: room id, type, and the
// marshalled payload and actor. Both stores call it before they assign a seq.
func CheckAppend(roomID string, typ events.Type, payload, actor []byte) error {
	if err := CheckText(roomID, string(typ)); err != nil {
		return err
	}
	if err := CheckJSON(payload); err != nil {
		return err
	}
	return CheckJSON(actor)
}

// Check is the pre-check for a proposals row: every text column, the layer
// list and the steps JSON.
func (p ProposalRow) Check() error {
	if err := CheckText(p.ProposalID, p.RoomID, p.Title, p.Status, p.Risk, p.Rollback, p.ParamsHash); err != nil {
		return err
	}
	if err := CheckText(p.RequiredLayers...); err != nil {
		return err
	}
	return CheckJSON(p.Steps)
}

// Check is the pre-check for an approvals row.
func (a ApprovalRow) Check() error {
	return CheckText(a.ProposalID, a.Layer, a.Decision, a.Reason, a.ParamsHash,
		a.IdentityIss, a.IdentitySub, a.IdentitySessionID, a.IdentityMethod, a.IdentityGrant)
}

// Check is the pre-check for a suggestions row.
func (r SuggestionRow) Check() error {
	return CheckText(r.SuggestionID, r.RoomID, r.FromID, r.FromName, r.Text, r.Status)
}

// Check is the pre-check for a handoffs row.
func (h HandoffRow) Check() error {
	if err := CheckText(h.HandoffID, h.RoomID, h.FromID, h.FromName, h.ToID, h.ToName, h.Status); err != nil {
		return err
	}
	return CheckJSON(h.Summary)
}

// CheckJSON returns an error wrapping ErrUnstorable if raw (JSON text) holds a
// NUL byte, a \u0000 escape, invalid UTF-8 or a lone surrogate escape: what
// jsonb refuses. It does not model jsonb's numeric limits. A nil or empty raw
// is fine.
func CheckJSON(raw []byte) error {
	if _, bad := scanJSON(raw, false); bad {
		return fmt.Errorf("%w: JSON holds a NUL, invalid UTF-8 or a lone surrogate", ErrUnstorable)
	}
	return nil
}

// SanitizeJSON returns raw with everything CheckJSON refuses replaced by
// U+FFFD (a \u0000 or lone-surrogate escape becomes the escape for U+FFFD, a
// NUL or invalid UTF-8 byte becomes the three bytes of U+FFFD). raw is returned
// as is, not copied, when it needs no change. An escaped backslash followed by
// "u0000" is text, not an escape, and is left alone.
func SanitizeJSON(raw []byte) []byte {
	out, _ := scanJSON(raw, true)
	return out
}

const (
	fffdBytes  = "\xEF\xBF\xBD"
	fffdEscape = "\\" + "ufffd" // the six characters: backslash, u, f, f, f, d
)

// scanJSON finds what jsonb cannot hold. With fix it also returns raw with each
// finding replaced by U+FFFD; without fix the returned slice is raw.
func scanJSON(raw []byte, fix bool) ([]byte, bool) {
	var out []byte
	bad := false
	last := 0 // raw[:last] is already in out
	replace := func(from, to int, with string) {
		bad = true
		if fix {
			out = append(out, raw[last:from]...)
			out = append(out, with...)
			last = to
		}
	}
	for i := 0; i < len(raw); {
		c := raw[i]
		switch {
		case c == 0:
			replace(i, i+1, fffdBytes)
			i++
		case c == '\\':
			if i+1 >= len(raw) || raw[i+1] != 'u' {
				i += 2 // an escaped character, so "\\u0000" (escaped backslash) is not an escape
				continue
			}
			code, ok := hex4(raw, i+2)
			switch {
			case !ok: // malformed escape: not valid JSON, rejected elsewhere
				i += 2
			case code == 0 || (code >= 0xDC00 && code <= 0xDFFF):
				replace(i, i+6, fffdEscape)
				i += 6
			case code >= 0xD800 && code <= 0xDBFF:
				if i+11 < len(raw) && raw[i+6] == '\\' && raw[i+7] == 'u' {
					if low, ok := hex4(raw, i+8); ok && low >= 0xDC00 && low <= 0xDFFF {
						i += 12 // a valid pair
						continue
					}
				}
				replace(i, i+6, fffdEscape)
				i += 6
			default:
				i += 6
			}
		case c < utf8.RuneSelf:
			i++
		default:
			r, size := utf8.DecodeRune(raw[i:])
			if r == utf8.RuneError && size == 1 {
				replace(i, i+1, fffdBytes)
			}
			i += size
		}
	}
	if !fix || !bad {
		return raw, bad
	}
	return append(out, raw[last:]...), true
}

// hex4 reads the four hex digits at raw[at:at+4].
func hex4(raw []byte, at int) (int, bool) {
	if at+4 > len(raw) {
		return 0, false
	}
	n := 0
	for _, c := range raw[at : at+4] {
		switch {
		case c >= '0' && c <= '9':
			n = n<<4 | int(c-'0')
		case c >= 'a' && c <= 'f':
			n = n<<4 | int(c-'a'+10)
		case c >= 'A' && c <= 'F':
			n = n<<4 | int(c-'A'+10)
		default:
			return 0, false
		}
	}
	return n, true
}
