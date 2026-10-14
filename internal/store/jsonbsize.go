package store

import (
	"strconv"
	"strings"

	"piperoom/internal/jsonnum"
)

// maxRenderedNumber caps the size one number is counted at. Postgres refuses
// a numeric with more than 131072 digits before the decimal point or 16383
// after it, so no stored number renders longer than this.
const maxRenderedNumber = 131072 + 16383 + 2

// StoredSize estimates the bytes a JSON payload takes once stored as jsonb and
// read back as text: len(raw) plus the growth of every number written with an
// exponent. jsonb keeps numbers as numeric and renders them without one, so
// "1e308" (5 bytes) comes back as a 1 followed by 308 zeros. The log byte cap
// counts this size, so a body of such tokens cannot store 50 times its cap
// (QA sweep 2026-10-14). Other jsonb rendering differences (a space after ':'
// and ',') are small and not counted.
func StoredSize(raw []byte) int64 {
	size := int64(len(raw))
	jsonnum.Scan(raw, func(s, e int) {
		size += numberGrowth(string(raw[s:e]))
	})
	return size
}

// numberGrowth is how many bytes longer the numeric rendering of tok is than
// tok, or 0. Only a token with an exponent can grow: numeric_out writes the
// integer digits (at least "0"), then a point and dscale fraction digits,
// where dscale is the token's fraction length minus its exponent.
func numberGrowth(tok string) int64 {
	ei := strings.IndexAny(tok, "eE")
	if ei < 0 {
		return 0
	}
	digits, exp10, ok := jsonnum.Significant(tok)
	if !ok {
		return 0
	}
	exp, err := strconv.Atoi(tok[ei+1:])
	if err != nil {
		// Beyond int: Postgres refuses it ("value overflows numeric format").
		return 0
	}
	// Far beyond what Postgres stores; clamped so the arithmetic below
	// cannot overflow.
	exp = min(max(exp, -maxRenderedNumber), maxRenderedNumber)
	mant := tok[:ei]
	frac := 0
	if dot := strings.IndexByte(mant, '.'); dot >= 0 {
		frac = len(mant) - dot - 1
	}
	dscale := max(0, frac-exp)
	intDigits := 1
	if digits != "0" {
		intDigits = max(1, min(exp10+len(digits), maxRenderedNumber))
	}
	rendered := intDigits
	if dscale > 0 {
		rendered += 1 + dscale
	}
	if mant != "" && mant[0] == '-' && digits != "0" {
		rendered++
	}
	rendered = min(rendered, maxRenderedNumber)
	return int64(max(0, rendered-len(tok)))
}
