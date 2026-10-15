package injectscan

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// KindBidi is a bidirectional embedding, override or isolate character
// (U+202A–U+202E, U+2066–U+2069). It makes text render in another order than
// it is stored, so a title or a log line can read differently on screen than
// to the model or to a grep ("Trojan Source"). Q5 T8.
const KindBidi Kind = "bidi_override"

// isBidiControl reports the characters KindBidi flags. The plain direction
// marks U+200E and U+200F only set the direction of neutral characters next
// to them and reorder nothing, so they are not included.
func isBidiControl(r rune) bool {
	return (r >= 0x202A && r <= 0x202E) || (r >= 0x2066 && r <= 0x2069)
}

// HasBidiOverride reports whether s holds a KindBidi character anywhere (the
// whole string, unlike Scan's windows). The room service refuses typed text
// with one.
func HasBidiOverride(s string) bool {
	return strings.IndexFunc(s, isBidiControl) >= 0
}

// scanBidi returns one finding at the first bidi control of s, or none. The
// excerpt names the code point, never the text around it.
func scanBidi(s string) []Finding {
	i := strings.IndexFunc(s, isBidiControl)
	if i < 0 {
		return nil
	}
	r, _ := utf8.DecodeRuneInString(s[i:])
	return []Finding{{Kind: KindBidi, Offset: i, Excerpt: fmt.Sprintf("U+%04X", r)}}
}
