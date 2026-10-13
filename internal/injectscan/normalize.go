package injectscan

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// normalize undoes the cheap tricks that hide an instruction from a plain
// pattern match. The standard library has no NFKC, so this is a small, fixed
// mapping:
//   - format characters (Unicode Cf: zero-width space/joiners, word joiner,
//     BOM, soft hyphen, bidi embeddings/overrides/isolates) and variation
//     selectors are dropped;
//   - fullwidth ASCII (U+FF01..U+FF5E) and the small-form angle brackets map
//     to ASCII;
//   - unusual spaces (no-break, ideographic, en/em spaces) become ' ';
//   - common Cyrillic and Greek lookalikes of Latin letters map to Latin.
//
// origin[i] is the byte offset in s of the rune that produced byte i of the
// result; origin[len(result)] is len(s). changed reports whether any rune was
// mapped or dropped.
func normalize(s string) (out string, origin []int, changed bool) {
	var b strings.Builder
	b.Grow(len(s))
	origin = make([]int, 0, len(s)+1)
	for i, r := range s {
		m, keep := mapRune(r)
		if !keep {
			changed = true
			continue
		}
		if m != r {
			changed = true
		}
		n := utf8.RuneLen(m)
		if n < 0 { // invalid rune: write the replacement character
			m, n = utf8.RuneError, utf8.RuneLen(utf8.RuneError)
		}
		b.WriteRune(m)
		for range n {
			origin = append(origin, i)
		}
	}
	origin = append(origin, len(s))
	return b.String(), origin, changed
}

// mapRune returns the replacement for r, or keep=false to drop it.
func mapRune(r rune) (rune, bool) {
	switch {
	case r < 0x80:
		return r, true
	case unicode.Is(unicode.Cf, r),
		r >= 0xFE00 && r <= 0xFE0F, // variation selectors
		r == 0x180E, r == 0x034F:   // Mongolian vowel separator, combining grapheme joiner
		return 0, false
	case r >= 0xFF01 && r <= 0xFF5E: // fullwidth ASCII
		return r - 0xFEE0, true
	case r == 0xFE64:
		return '<', true
	case r == 0xFE65:
		return '>', true
	case r == 0x3000 || r == 0x00A0 || r == 0x202F || r == 0x205F || (r >= 0x2000 && r <= 0x200A):
		return ' ', true
	}
	if m, ok := lookalikes[r]; ok {
		return m, true
	}
	return r, true
}

// lookalikes maps Cyrillic and Greek letters that render like Latin ones.
var lookalikes = map[rune]rune{
	// Cyrillic lower case
	'а': 'a', 'с': 'c', 'е': 'e', 'һ': 'h', 'і': 'i', 'ј': 'j', 'о': 'o', 'р': 'p', 'ԛ': 'q',
	'ѕ': 's', 'ԝ': 'w', 'х': 'x', 'у': 'y', 'ԁ': 'd', 'ɡ': 'g',
	// Cyrillic upper case
	'А': 'A', 'В': 'B', 'С': 'C', 'Е': 'E', 'Н': 'H', 'І': 'I', 'Ј': 'J', 'К': 'K', 'М': 'M',
	'О': 'O', 'Р': 'P', 'Ѕ': 'S', 'Т': 'T', 'Х': 'X', 'У': 'Y',
	// Greek
	'ο': 'o', 'α': 'a', 'ν': 'v', 'ι': 'i', 'Α': 'A', 'Β': 'B', 'Ε': 'E', 'Η': 'H', 'Ι': 'I',
	'Κ': 'K', 'Μ': 'M', 'Ν': 'N', 'Ο': 'O', 'Ρ': 'P', 'Τ': 'T', 'Χ': 'X', 'Υ': 'Y', 'Ζ': 'Z',
}
