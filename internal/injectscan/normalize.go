package injectscan

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// normalize undoes the cheap tricks that hide an instruction from a plain
// pattern match. The standard library has no NFKC, so this is a small, fixed
// mapping:
//   - Unicode tag characters that mirror ASCII (U+E0020..U+E007E) map to
//     that ASCII, so text hidden in them is scanned like visible text;
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
	for i, r := range s {
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

func isSpaceRune(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\v' || r == '\f'
}

// mapRune returns the replacement for r, or keep=false to drop it.
func mapRune(r rune) (rune, bool) {
	switch {
	case r < 0x80:
		return r, true
	case isTagText(r):
		// Unicode tag characters render as nothing but spell ASCII, which a
		// model reads: decode them so the rules see the hidden words.
		return r - tagBase, true
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

// tagBase is the offset of the Unicode tag block (U+E0000..U+E007F) from ASCII.
const tagBase = 0xE0000

// isTagText reports the tag characters that mirror printable ASCII
// (U+E0020..U+E007E). U+E0001 (language tag) and U+E007F (cancel tag) carry
// no text and are dropped like other format characters.
func isTagText(r rune) bool {
	return r >= tagBase+0x20 && r <= tagBase+0x7E
}

// blackFlag (U+1F3F4) opens the one legitimate use of tag characters, a
// subdivision flag emoji such as England's (U+1F3F4, tags "gbeng", U+E007F).
const blackFlag = 0x1F3F4

// maxFlagTags is the most tag characters a subdivision flag holds (a region
// code and a subdivision of up to three letters); a longer run after the
// flag is hidden text that borrowed the flag as cover.
const maxFlagTags = 6

// scanHiddenTags reports each run of tag text that is not part of a flag
// emoji: invisible in any UI, readable by a model, and so hidden text in its
// own right whatever it says.
func scanHiddenTags(s string) []Finding {
	var out []Finding
	start, end, inFlag, flagTags := -1, 0, false, 0
	var run strings.Builder
	flush := func() {
		if start >= 0 && run.Len() > 0 && !inFlag {
			out = append(out, Finding{Kind: KindHiddenText, Offset: start, End: end, Excerpt: excerpt("tag text: " + run.String())})
		}
		start = -1
		run.Reset()
	}
	for i, r := range s {
		switch {
		case r == blackFlag:
			flush()
			inFlag, flagTags = true, 0
		case isTagText(r):
			if start < 0 {
				start = i
			}
			end = i + utf8.RuneLen(r)
			run.WriteRune(r - tagBase)
			if inFlag {
				if flagTags++; flagTags > maxFlagTags {
					inFlag = false // too long for a flag: report the whole run
				}
			}
		default:
			flush()
			inFlag = false
		}
	}
	flush()
	return out
}

// cancelTag (U+E007F) closes the tag sequence of a flag emoji.
const cancelTag = tagBase + 0x7F

// rgiFlags are the tag sequences (cancel tag included) of the only
// subdivision flags Unicode recommends for interchange: England, Scotland
// and Wales. No platform draws any other tag sequence as a flag, so its tags
// are as invisible as any other hidden text.
var rgiFlags = [...]string{tagSeq("gbeng"), tagSeq("gbsct"), tagSeq("gbwls")}

// tagSeq writes code in tag characters and closes it with a cancel tag.
func tagSeq(code string) string {
	var b strings.Builder
	for _, r := range code {
		b.WriteRune(tagBase + r)
	}
	b.WriteRune(cancelTag)
	return b.String()
}

// rgiFlagLen returns the byte length of the flag tag sequence s starts
// with, or 0 when it starts with none.
func rgiFlagLen(s string) int {
	for _, f := range rgiFlags {
		if strings.HasPrefix(s, f) {
			return len(f)
		}
	}
	return 0
}

// scanStrayTags reports every run of tag characters (U+E0000..U+E007F)
// outside a real flag emoji (#443). It runs after scanHiddenTags and only
// adds findings: that scan lets any run of up to maxFlagTags tag characters
// after a black flag pass, so text split into such runs, one flag each, went
// unreported. Here a black flag that does not open a real flag sequence
// does not end a run, so the pieces join into one finding, and runs of
// characters scanHiddenTags drops (cancel, language tag, U+E0000) count too.
func scanStrayTags(s string) []Finding {
	var out []Finding
	start, end := -1, 0
	var run strings.Builder
	flush := func() {
		if start >= 0 {
			out = append(out, Finding{Kind: KindHiddenText, Offset: start, End: end, Excerpt: excerpt("tag text: " + run.String())})
		}
		start = -1
		run.Reset()
	}
	for i := 0; i < len(s); {
		r, n := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == blackFlag:
			if k := rgiFlagLen(s[i+n:]); k > 0 {
				flush()
				i += n + k
				continue
			}
		case r >= tagBase && r <= cancelTag:
			if start < 0 {
				start = i
			}
			end = i + n
			if isTagText(r) {
				run.WriteRune(r - tagBase)
			}
		default:
			flush()
		}
		i += n
	}
	flush()
	return out
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
