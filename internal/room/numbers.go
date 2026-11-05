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
// The decision is made on the significant form of the token (jsonnum.Significant:
// digits without sign, leading and trailing zeros, and a decimal exponent), never
// on the raw text. strconv.ParseFloat is not reliable for a token padded with
// zeros: it stops adding exponent digits at about 10000, so
// "0.<10098 zeros>1e1009998" parses as 1 while its value is 1e-9 * 10^1009998.
// Building 10^1009998 with big.Rat takes about 32 ms a token.
//
// With mag = exp10 + len(digits) the value lies in [10^(mag-1), 10^mag):
//
//   - mag above jsonnum.MaxMag is beyond every double: out of range;
//   - mag below jsonnum.MinMag rounds to zero: inexact (underflow);
//   - otherwise exp10 is between about -1125 and 310 (the digits are capped at
//     maxExactSigDigits), so ParseFloat of the short significant form is cheap,
//     and so is the big.Rat built from it, whatever the token's length.
func classifyNumber(tok string) numberClass {
	digits, exp10, ok := jsonnum.Significant(tok)
	if !ok {
		return numberIgnore
	}
	if digits == "0" {
		return numberExact // zero stays zero whatever the exponent
	}
	mag := exp10 + len(digits)
	if mag > jsonnum.MaxMag {
		return numberOutOfRange
	} else if mag < jsonnum.MinMag {
		return numberInexact
	}
	sign := ""
	if tok[0] == '-' {
		sign = "-"
	}
	if len(digits) > maxExactSigDigits {
		// Never equal to a double, so inexact, unless it lies beyond the
		// largest double: jsonb accepts that, JCS cannot write it, and the
		// ingest guard refuses it. The largest double is an integer of 309
		// digits, so it lies on the 800-digit grid, and the value is at least
		// it exactly when its first 800 digits are. ParseFloat is given only
		// those digits, with the exponent that keeps the magnitude; the full
		// string would have its exponent misread (see above).
		top := sign + digits[:maxExactSigDigits] + "e" + strconv.Itoa(mag-maxExactSigDigits)
		if _, err := strconv.ParseFloat(top, 64); errors.Is(err, strconv.ErrRange) {
			return numberOutOfRange
		}
		return numberInexact
	}
	// ParseFloat sees at most maxExactSigDigits digits and an exponent within
	// about 1200, never the padding of the raw token.
	norm := sign + digits + "e" + strconv.Itoa(exp10)
	f, err := strconv.ParseFloat(norm, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return numberOutOfRange
		}
		return numberIgnore
	}
	if f == 0 {
		return numberInexact // a non-zero value that underflowed to zero
	}
	// The cap is on significant digits, not on token length. jsonb keeps the
	// decimal scale, so the same value comes back with a different length
	// (1.<700 zeros>e-320 is 707 characters in and 1022 after the round trip),
	// and a length cap would class one value two ways. Significant digits are
	// the same for every spelling of a value.
	// want = digits * 10^exp10, with 5e-324 <= |want| <= 1.8e308 and at most
	// maxExactSigDigits digits.
	want, ok := new(big.Rat).SetString(norm)
	if !ok {
		return numberInexact
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
	// Capacity len(exact): append grows the slice for the few quoted numbers.
	out = make([]byte, 0, len(exact))
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
