package injectscan

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// KindHiddenText is text a reader cannot see but a model reads: Unicode tag
// characters (U+E0000–U+E007F, which spell out ASCII invisibly), zero-width
// spaces and non-joiners, a zero-width joiner outside an emoji sequence, the
// word joiner and invisible operators, a run of soft hyphens, or a U+FEFF
// inside the text. A title can read "orders lag high" on screen and carry a
// whole instruction to the model (Q6 U1). Normalisation decodes tag
// characters to ASCII, so the hidden words are scanned by the other rules too.
// (KindHiddenText itself is declared with the other kinds in injectscan.go.)

// isTagRune reports a Unicode tag character, U+E0000–U+E007F.
func isTagRune(r rune) bool {
	return r >= 0xE0000 && r <= 0xE007F
}

// tagASCII maps a printable tag character (U+E0020–U+E007E) to the ASCII
// character it mirrors; ok is false for the other tag characters (language
// tag, cancel tag, the unassigned controls), which carry no letter.
func tagASCII(r rune) (rune, bool) {
	if r >= 0xE0020 && r <= 0xE007E {
		return r - 0xE0000, true
	}
	return 0, false
}

// emojiLike reports a rune a zero-width joiner legitimately sits between in an
// emoji sequence (👩‍💻, 🏳️‍🌈): a symbol, a variation selector or a skin-tone
// modifier. The standard library has no Extended_Pictographic table; So plus
// these is close enough, and a miss only flags an emoji, it never hides text.
func emojiLike(r rune) bool {
	return unicode.Is(unicode.So, r) || r == 0xFE0F || (r >= 0x1F3FB && r <= 0x1F3FF)
}

// hiddenAt reports whether the rune r at byte offset i of s is hidden text
// (KindHiddenText). Context matters for three of them: a ZWJ between emoji is
// part of the emoji, a single soft hyphen is a hyphenation hint, and a U+FEFF
// at the very start is a byte-order mark.
func hiddenAt(s string, i int, r rune) bool {
	switch {
	case isTagRune(r):
		return true
	case r == 0x200B, r == 0x200C, r == 0x2060, r >= 0x2061 && r <= 0x2064:
		return true
	case r == 0x200D:
		prev, _ := utf8.DecodeLastRuneInString(s[:i])
		next, _ := utf8.DecodeRuneInString(s[i+utf8.RuneLen(r):])
		return !(emojiLike(prev) && emojiLike(next))
	case r == 0xFEFF:
		return i > 0
	case r == 0x00AD:
		prev, _ := utf8.DecodeLastRuneInString(s[:i])
		next, _ := utf8.DecodeRuneInString(s[i+utf8.RuneLen(r):])
		return prev == 0x00AD || next == 0x00AD
	}
	return false
}

// firstHidden returns the byte offset and rune of the first hidden character
// of s, or -1.
func firstHidden(s string) (int, rune) {
	for i, r := range s {
		if r < 0x80 {
			continue
		}
		if hiddenAt(s, i, r) {
			return i, r
		}
	}
	return -1, 0
}

// HasHiddenText reports whether s holds a KindHiddenText character anywhere
// (the whole string, unlike Scan's windows). The room service refuses typed
// text with one, as it does a bidi control.
func HasHiddenText(s string) bool {
	i, _ := firstHidden(s)
	return i >= 0
}

// StripHidden returns s without its KindHiddenText characters: tag characters,
// zero-width spaces and non-joiners, a ZWJ outside an emoji sequence, the word
// joiner and invisible operators, soft-hyphen runs and a U+FEFF after the
// start. What a person sees is unchanged. For machine text that is shown or
// passed on (an alert title), not for evidence the investigation reads.
func StripHidden(s string) string {
	if !HasHiddenText(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i, r := range s {
		if r >= 0x80 && hiddenAt(s, i, r) {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// scanHidden returns one finding at the first hidden character of s that is
// not a tag character, or none. The excerpt names the code point, never the
// text around it.
func scanHidden(s string) []Finding {
	flag := false // inside a subdivision flag: U+1F3F4, tag letters, cancel tag
	for i, r := range s {
		switch {
		case r == blackFlag:
			flag = true
			continue
		case isTagText(r):
			// Tag letters are scanHiddenTags' (normalize.go): it reports each
			// run with the text it spells and leaves emoji flags alone.
			continue
		case r == 0xE007F && flag:
			flag = false // the cancel tag that ends a flag
			continue
		}
		flag = false
		if r < 0x80 {
			continue
		}
		if hiddenAt(s, i, r) {
			return []Finding{{Kind: KindHiddenText, Offset: i, Excerpt: fmt.Sprintf("U+%04X", r)}}
		}
	}
	return nil
}
