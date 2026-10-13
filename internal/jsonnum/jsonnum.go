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

// MaxMag and MinMag bound the magnitude of a finite, non-zero float64. A value
// with significant digits d and exponent e (value = d * 10^e) lies in
// [10^(mag-1), 10^mag) for mag = e + len(d). The largest double is 1.8e308
// (mag 309) and the smallest denormal is 4.9e-324 (mag -323), so mag above
// MaxMag is beyond every double and mag below MinMag rounds to zero. Only the
// narrow band between needs a real parse.
const (
	MaxMag = 310
	MinMag = -325
)

// maxExpClamp stands in for an exponent too large for an int; it keeps the
// magnitude arithmetic far from overflow and is far beyond MaxMag/MinMag.
const maxExpClamp = 1 << 40

// Significant splits a number token into its significant digits (no sign, no
// leading zeros, no trailing zeros) and a decimal exponent, so the value is
// digits * 10^exp10; zero in any spelling is "0", 0. ok is false when the token
// is not a plain JSON number. An exponent that does not fit an int is clamped
// to +/-2^40 (its sign is kept), which is far outside the double range. The
// work is linear in the token length; it never builds a big number, and it does
// not depend on how ParseFloat treats a very long exponent or mantissa (which
// it cuts off at about 10000 digits).
func Significant(tok string) (digits string, exp10 int, ok bool) {
	i := 0
	if i < len(tok) && (tok[i] == '-' || tok[i] == '+') {
		i++
	}
	start := i
	for i < len(tok) && tok[i] >= '0' && tok[i] <= '9' {
		i++
	}
	intPart := tok[start:i]
	fracPart := ""
	if i < len(tok) && tok[i] == '.' {
		i++
		fs := i
		for i < len(tok) && tok[i] >= '0' && tok[i] <= '9' {
			i++
		}
		fracPart = tok[fs:i]
	}
	exp := 0
	if i < len(tok) && (tok[i] == 'e' || tok[i] == 'E') {
		n, err := strconv.Atoi(tok[i+1:])
		switch {
		case err == nil:
			exp = n
		case errors.Is(err, strconv.ErrRange):
			exp = maxExpClamp
			if tok[i+1] == '-' {
				exp = -maxExpClamp
			}
		default:
			return "", 0, false
		}
		i = len(tok)
	}
	if i != len(tok) || intPart == "" && fracPart == "" {
		return "", 0, false
	}
	all := intPart + fracPart
	lead := 0
	for lead < len(all) && all[lead] == '0' {
		lead++
	}
	end := len(all)
	for end > lead && all[end-1] == '0' {
		end--
	}
	if lead == end {
		return "0", 0, true // zero in any spelling
	}
	exp10 = exp - len(fracPart) + (len(all) - end)
	if exp10 > maxExpClamp {
		exp10 = maxExpClamp
	} else if exp10 < -maxExpClamp {
		exp10 = -maxExpClamp
	}
	return all[lead:end], exp10, true
}

// OutOfRange reports whether the number token tok is outside the double range:
// not finite as a float64 (1e400, and the 1e131072 that jsonb refuses), not zero
// but underflowing to zero (1e-400, and the 1e-16384 that jsonb refuses), or so
// long that jsonb may refuse its scale. A token that does not parse as a number
// is not out of range (it is not a number). The decision uses the significant
// form (see Significant), so a token padded with thousands of zeros is judged by
// its value, not by how strconv cuts off a long exponent.
func OutOfRange(tok string) bool {
	if len(tok) > maxStorableTokenLen {
		return true
	}
	digits, exp10, ok := Significant(tok)
	if !ok || digits == "0" {
		return false
	}
	switch mag := exp10 + len(digits); {
	case mag > MaxMag, mag < MinMag:
		return true
	}
	f, err := strconv.ParseFloat(digits+"e"+strconv.Itoa(exp10), 64)
	if err != nil {
		return errors.Is(err, strconv.ErrRange)
	}
	return f == 0
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
