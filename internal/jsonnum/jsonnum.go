// Package jsonnum finds number tokens in JSON text and handles the ones a
// double cannot hold. It is shared by the room service (alert-body guard,
// canonical hash) and the agent (tool data) so both use one scanner.
package jsonnum

import (
	"errors"
	"strconv"
)

// maxStorableTokenLen is the longest number token left as a number by Quote. A
// jsonb number holds at most 16383 digits after the decimal point; a longer
// token may be refused with "value overflows numeric format" (SQLSTATE 22003).
// Tokens this long are not real measurements, so they become strings.
const maxStorableTokenLen = 16000

// numberChar reports whether c can be part of a JSON number token.
func numberChar(c byte) bool {
	return c >= '0' && c <= '9' || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E'
}

// Scan calls fn for every number token in JSON text, outside strings, with the
// token's [start, end) offsets. It is a tolerant scanner, not a validator: text
// that is not valid JSON never makes it fail or panic, it only may yield odd
// tokens, which callers ignore when they do not parse.
func Scan(raw []byte, fn func(start, end int)) {
	for i := 0; i < len(raw); {
		switch c := raw[i]; {
		case c == '"':
			i++
			for i < len(raw) && raw[i] != '"' {
				if raw[i] == '\\' {
					i++
				}
				i++
			}
			i++ // closing quote
		case c == '-' || c >= '0' && c <= '9':
			j := i + 1
			for j < len(raw) && numberChar(raw[j]) {
				j++
			}
			fn(i, j)
			i = j
		default:
			i++
		}
	}
}

// MantissaIsZero reports whether the digits before any exponent are all zero.
func MantissaIsZero(tok string) bool {
	for i := 0; i < len(tok); i++ {
		switch c := tok[i]; {
		case c == 'e' || c == 'E':
			return true
		case c >= '1' && c <= '9':
			return false
		}
	}
	return true
}

// OutOfRange reports whether the number token tok is outside the double range:
// not finite as a float64 (1e400, and the 1e131072 that jsonb refuses), not zero
// but underflowing to zero (1e-400, and the 1e-16384 that jsonb refuses), or so
// long that jsonb may refuse its scale. A token that does not parse as a number
// is not out of range (it is not a number).
func OutOfRange(tok string) bool {
	if len(tok) > maxStorableTokenLen {
		return true
	}
	f, err := strconv.ParseFloat(tok, 64)
	if err != nil {
		return errors.Is(err, strconv.ErrRange)
	}
	return f == 0 && !MantissaIsZero(tok)
}

// Quote returns raw with every out-of-range number token (see OutOfRange)
// replaced by a JSON string holding the token's exact text, so
// {"n":1e131072} becomes {"n":"1e131072"}. n is the number of tokens replaced.
// raw is returned as is, not copied, when nothing is replaced. A token holds
// only digits, signs, dots and e/E, so the text needs no escaping.
func Quote(raw []byte) (out []byte, n int) {
	last := 0
	Scan(raw, func(s, e int) {
		if !OutOfRange(string(raw[s:e])) {
			return
		}
		out = append(out, raw[last:s]...)
		out = append(out, '"')
		out = append(out, raw[s:e]...)
		out = append(out, '"')
		last = e
		n++
	})
	if n == 0 {
		return raw, 0
	}
	return append(out, raw[last:]...), n
}
