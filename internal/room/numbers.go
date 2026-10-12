package room

import (
	"errors"
	"math/big"
	"strconv"

	"github.com/gowebpki/jcs"
)

// numberChar reports whether c can be part of a JSON number token.
func numberChar(c byte) bool {
	return c >= '0' && c <= '9' || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E'
}

// scanNumbers calls fn for every number token in JSON text, outside strings,
// with the token's [start, end) offsets. It is a tolerant scanner, not a
// validator: text that is not valid JSON never makes it fail or panic, it only
// may yield odd tokens, which callers ignore when they do not parse.
func scanNumbers(raw []byte, fn func(start, end int)) {
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

// numberClass says how a number token fares as a float64.
type numberClass int

const (
	numberExact      numberClass = iota // a double holds it exactly (as JCS will write it)
	numberInexact                       // finite, but JCS writes a different value (integer above 2^53, more than 17 significant digits, underflow)
	numberOutOfRange                    // not finite as float64 (for example 1e400); jsonb accepts it, JCS cannot write it
	numberIgnore                        // does not parse as a number; ignore it
)

// classifyNumber classifies one number token.
func classifyNumber(tok string) numberClass {
	f, err := strconv.ParseFloat(tok, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return numberOutOfRange
		}
		return numberIgnore
	}
	want, ok := new(big.Rat).SetString(tok)
	if !ok {
		return numberInexact // too large an exponent to model; treat as not exact
	}
	written, err := jcs.NumberToJSON(f)
	if err != nil {
		return numberOutOfRange
	}
	got, ok := new(big.Rat).SetString(written)
	if !ok || got.Cmp(want) != 0 {
		return numberInexact
	}
	return numberExact
}

// hasOutOfRangeNumber reports whether raw holds a number that is not finite as
// a float64. The ingest guard refuses such alert bodies: jsonb would store them
// but the audit export's canonical form cannot represent them as numbers.
func hasOutOfRangeNumber(raw []byte) bool {
	found := false
	scanNumbers(raw, func(s, e int) {
		if !found && classifyNumber(string(raw[s:e])) == numberOutOfRange {
			found = true
		}
	})
	return found
}

// hashForm returns exact (compact JSON) rewritten for the hash: every number a
// double cannot hold finitely is replaced by a JSON string with its exact text
// (so jcs.Transform cannot fail on it). inexact counts the numbers whose hashed
// form differs from the exact value: out of range or not exactly representable.
func hashForm(exact []byte) (out []byte, inexact int) {
	out = make([]byte, 0, len(exact)+8)
	last := 0
	scanNumbers(exact, func(s, e int) {
		switch classifyNumber(string(exact[s:e])) {
		case numberInexact:
			inexact++
		case numberOutOfRange:
			inexact++
			out = append(out, exact[last:s]...)
			out = append(out, '"')
			out = append(out, exact[s:e]...)
			out = append(out, '"')
			last = e
		}
	})
	if last == 0 && inexact == 0 {
		return exact, 0
	}
	out = append(out, exact[last:]...)
	return out, inexact
}
