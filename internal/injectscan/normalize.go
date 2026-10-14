package injectscan

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// normalize undoes the cheap tricks that hide an instruction from a plain
// pattern match. The standard library has no NFKC, so this is a small, fixed
// mapping:
//   - Unicode tag characters U+E0020..U+E007E map to the ASCII they shadow
//     (U+E0069 reads "i"), so instructions smuggled in tags are scanned;
//   - other format characters (Unicode Cf: zero-width space/joiners, word joiner,
//     BOM, soft hyphen, bidi embeddings/overrides/isolates) and variation
//     selectors are dropped, and so are combining marks (Mn), so "i̇gnore"
//     reads "ignore";
//   - fullwidth ASCII (U+FF01..U+FF5E), Mathematical Alphanumeric letters and
//     digits (U+1D400..U+1D7FF) and the small-form angle brackets map to
//     ASCII;
//   - unusual spaces (no-break, ideographic, en/em spaces) become ' ', and
//     whitespace runs collapse (keeping at most two newlines);
//   - common Cyrillic, Greek and Armenian lookalikes and the Latin small
//     capitals map to Latin letters.
//
// origin[i] is the byte offset in s of the rune that produced byte i of the
// result; origin[len(result)] is len(s). changed reports whether any rune was
// mapped or dropped. With cfAsSpace, format characters become ' ' instead of
// being dropped, for text that uses them as word separators
// ("IGNORE<U+2060>ALL"); hadCf reports whether there were any.
func normalize(s string, cfAsSpace bool) (out string, origin []int, changed, hadCf bool) {
	var b strings.Builder
	b.Grow(len(s))
	origin = make([]int, 0, len(s)+1)
	inSpace, newlines := false, 0
	inTag := false
	for i, r := range s {
		// A run of tag characters is a separate hidden message: it is read
		// as its own words, so "ok"+tags("ignore...") does not glue into
		// "okignore".
		if tag := isTagRune(r); tag != inTag {
			inTag = tag
			if b.Len() > 0 && !inSpace {
				b.WriteByte(' ')
				origin = append(origin, i)
				inSpace, newlines = true, 0
				changed = true
			}
		}
		m, keep := mapRune(r)
		if !keep {
			changed = true
			if !unicode.Is(unicode.Cf, r) {
				continue
			}
			hadCf = true
			if !cfAsSpace {
				continue
			}
			m = ' '
		}
		if m != r {
			changed = true
		}
		n := utf8.RuneLen(m)
		if n < 0 { // invalid rune: write the replacement character
			m, n = utf8.RuneError, utf8.RuneLen(utf8.RuneError)
		}
		// Whitespace runs collapse to their first character, keeping up to
		// two newlines (a blank line still separates chat turns), so padding
		// cannot push the end of a phrase out of a rule's window.
		if isSpaceRune(m) {
			if inSpace {
				if m != '\n' || newlines >= 2 {
					changed = true
					continue
				}
				newlines++
			} else {
				inSpace, newlines = true, 0
				if m == '\n' {
					newlines = 1
				}
			}
		} else {
			inSpace = false
		}
		b.WriteRune(m)
		for range n {
			origin = append(origin, i)
		}
	}
	origin = append(origin, len(s))
	return b.String(), origin, changed, hadCf
}

// isTagRune reports the Unicode tag block (U+E0000..U+E007F).
func isTagRune(r rune) bool { return r >= 0xE0000 && r <= 0xE007F }

func isSpaceRune(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\v' || r == '\f'
}

// mapRune returns the replacement for r, or keep=false to drop it.
func mapRune(r rune) (rune, bool) {
	switch {
	case r < 0x80:
		return r, true
	case r >= 0xE0020 && r <= 0xE007E: // tags: see isTagRune
		// Unicode tag characters render as nothing but each one shadows a
		// printable ASCII character ("ASCII smuggling"); models read them.
		// They become that ASCII so the hidden text is scanned.
		return r - 0xE0000, true
	case unicode.Is(unicode.Cf, r),
		r >= 0xFE00 && r <= 0xFE0F, // variation selectors
		r == 0x180E, r == 0x034F,   // Mongolian vowel separator, combining grapheme joiner
		unicode.Is(unicode.Mn, r):
		return 0, false
	case r >= 0x1D400 && r <= 0x1D6A3: // Mathematical Alphanumeric letters: 13 styles of A-Z a-z
		i := (r - 0x1D400) % 52
		if i < 26 {
			return 'A' + i, true
		}
		return 'a' + i - 26, true
	case r >= 0x1D7CE && r <= 0x1D7FF: // Mathematical digits: 5 styles of 0-9
		return '0' + (r-0x1D7CE)%10, true
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
	// Armenian
	'օ': 'o', 'ս': 'u', 'հ': 'h', 'ո': 'n', 'ց': 'g',
	// Latin small capitals
	'ᴀ': 'a', 'ʙ': 'b', 'ᴄ': 'c', 'ᴅ': 'd', 'ᴇ': 'e', 'ɢ': 'g', 'ʜ': 'h', 'ɪ': 'i', 'ᴊ': 'j',
	'ᴋ': 'k', 'ʟ': 'l', 'ᴍ': 'm', 'ɴ': 'n', 'ᴏ': 'o', 'ᴘ': 'p', 'ʀ': 'r', 'ꜱ': 's', 'ᴛ': 't',
	'ᴜ': 'u', 'ᴠ': 'v', 'ᴡ': 'w', 'ʏ': 'y', 'ᴢ': 'z',
	// Greek
	'ο': 'o', 'α': 'a', 'ν': 'v', 'ι': 'i', 'Α': 'A', 'Β': 'B', 'Ε': 'E', 'Η': 'H', 'Ι': 'I',
	'Κ': 'K', 'Μ': 'M', 'Ν': 'N', 'Ο': 'O', 'Ρ': 'P', 'Τ': 'T', 'Χ': 'X', 'Υ': 'Y', 'Ζ': 'Z',
}
