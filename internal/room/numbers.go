package room

import (
	"errors"
	"math/big"
	"strconv"

	"github.com/gowebpki/jcs"

	"piperoom/internal/jsonnum"
)

// maxExactSigDigits caps the significant digits of a number that is compared
// exactly with big.Rat; a number with more is classified inexact. The exact
// decimal expansion of any double has at most 767 significant digits, so 800
// keeps every value a double can hold exactly. Significant digits are the
// digits of the value without sign, leading zeros, trailing zeros or exponent,
// so the count is the same before and after a jsonb round trip.
const maxExactSigDigits = 800

// significantForm splits a finite number token into its significant digits
// (no sign, no leading zeros, no trailing zeros) and a decimal exponent, so the
// value is digits * 10^exp10. ok is false when the token is not a plain JSON
// number or its exponent does not fit an int. The work is linear in the token
// length and allocates at most that much; it never builds a big number.
func significantForm(tok string) (digits string, exp10 int, ok bool) {
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
		if err != nil {
			return "", 0, false
		}
		exp = n
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
	return all[lead:end], exp - len(fracPart) + (len(all) - end), true
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
//
// The cost is bounded by the token length. big.Rat is only used once
// strconv.ParseFloat has shown the value is a finite, non-zero double; for such
// a token the decimal exponent cannot be larger than about 330 plus the number
// of digits, so building 10^exponent is cheap. A token such as 1e-999999 parses
// to 0 (underflow) and is classified without big.Rat: it used to build
// 10^999999, about 27 ms a token.
func classifyNumber(tok string) numberClass {
	f, err := strconv.ParseFloat(tok, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return numberOutOfRange
		}
		return numberIgnore
	}
	if f == 0 {
		// Zero, or a non-zero value that underflowed to zero.
		if jsonnum.MantissaIsZero(tok) {
			return numberExact
		}
		return numberInexact
	}
	// The cap is on significant digits, not on token length. jsonb keeps the
	// decimal scale, so the same value comes back with a different length
	// (1.<700 zeros>e-320 is 707 characters in and 1022 after the round trip),
	// and a length cap would class one value two ways. Significant digits are
	// the same for every spelling of a value.
	digits, exp10, ok := significantForm(tok)
	if !ok || len(digits) > maxExactSigDigits {
		// A double's exact decimal expansion has at most 767 significant
		// digits; with more, the token is never the same value as any double.
		return numberInexact
	}
	// want = digits * 10^exp10. The token is a finite non-zero double, so
	// 5e-324 <= want <= 1.8e308 and digits has at most maxExactSigDigits
	// digits: exp10 is between about -1125 and 308, so building 10^|exp10| is
	// cheap whatever the token's length or spelling.
	want, ok := new(big.Rat).SetString(digits + "e" + strconv.Itoa(exp10))
	if !ok {
		return numberInexact // too large an exponent to model; treat as not exact
	}
	if f < 0 {
		want.Neg(want)
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
	jsonnum.Scan(raw, func(s, e int) {
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
	jsonnum.Scan(exact, func(s, e int) {
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
