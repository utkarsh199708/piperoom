package auth

import (
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// Display-name look-alikes (docs/design/auth.md section 2.1, security audit
// 2026-10 A-ID-2). An IdP user can set their own name claim; a member who got in
// through an owners.yaml group must not be able to show up in the room as
// "Priya", or as "Priya" spelt with a Cyrillic a (U+0430). Skeleton maps a name to a
// comparison form in the spirit of Unicode TR39's skeleton, so two names that
// render alike compare equal. A false match only makes the server append the
// person's email to their name, so the mapping errs on the wide side; a missed
// match lets a look-alike through, so the table covers the Latin, Greek,
// Cyrillic, Armenian, Cherokee and Lisu letters that pass for Latin ones, and
// ShownName (package room) also suffixes any name that mixes Latin with another
// script.

// upperLookalikes maps Greek and Cyrillic capitals whose lower-case forms look
// different from the Latin letter they imitate (Greek capital eta U+0397 is an
// H, small eta U+03B7 an n), so they are mapped before case folding. Keys are
// escapes because the letters are, by design, indistinguishable in review.
var upperLookalikes = map[rune]rune{
	// Greek
	'\u0391': 'A', '\u0392': 'B', '\u0395': 'E', '\u0396': 'Z', '\u0397': 'H', '\u0399': 'I', '\u039a': 'K', '\u039c': 'M',
	'\u039d': 'N', '\u039f': 'O', '\u03a1': 'P', '\u03a4': 'T', '\u03a5': 'Y', '\u03a7': 'X', '\u03f9': 'C', '\u037f': 'J', '\u03dc': 'F',
	// Cyrillic
	'\u0410': 'A', '\u0412': 'B', '\u0415': 'E', '\u0405': 'S', '\u0406': 'I', '\u0408': 'J', '\u041a': 'K', '\u041c': 'M',
	'\u041d': 'H', '\u041e': 'O', '\u0420': 'P', '\u0421': 'C', '\u0422': 'T', '\u0423': 'Y', '\u0425': 'X', '\u04ae': 'Y',
	'\u051a': 'Q', '\u051c': 'W', '\u04c0': 'I', '\u0474': 'V', '\u042c': 'b',
}

// lowerLookalikes maps lower-case letters that pass for a Latin letter, after
// case folding. It also catches the capitals above in their folded form.
var lowerLookalikes = map[rune]rune{
	// Cyrillic
	'\u0430': 'a', '\u0432': 'b', '\u0433': 'r', '\u0435': 'e', '\u0456': 'i', '\u0458': 'j', '\u043a': 'k', '\u043c': 'm',
	'\u043d': 'h', '\u043e': 'o', '\u043f': 'n', '\u0440': 'p', '\u0441': 'c', '\u0442': 't', '\u0443': 'y', '\u0445': 'x',
	'\u0455': 's', '\u04bb': 'h', '\u0501': 'd', '\u051b': 'q', '\u051d': 'w', '\u04cf': 'l', '\u04af': 'y', '\u044c': 'b',
	'\u0461': 'w', '\u0475': 'v',
	// Greek
	'\u03b1': 'a', '\u03b2': 'b', '\u03b3': 'y', '\u03b5': 'e', '\u03b7': 'n', '\u03b9': 'i', '\u03ba': 'k', '\u03bd': 'v',
	'\u03bf': 'o', '\u03c1': 'p', '\u03c4': 't', '\u03c5': 'u', '\u03c7': 'x', '\u03c9': 'w', '\u03f2': 'c', '\u03f3': 'j',
	'\u03bc': 'u', '\u03b6': 'z',
	// Latin letters NFKD leaves alone: dotless i, IPA and small capitals, and
	// letters with a stroke (no decomposition, so the mark cannot be dropped).
	'\u0131': 'i', '\u0269': 'i', '\u026a': 'i', '\u0251': 'a', '\u0261': 'g', '\u1d00': 'a', '\u0299': 'b', '\u1d04': 'c',
	'\u1d05': 'd', '\u1d07': 'e', '\u0262': 'g', '\u029c': 'h', '\u1d0a': 'j', '\u1d0b': 'k', '\u029f': 'l', '\u1d0d': 'm',
	'\u0274': 'n', '\u1d0f': 'o', '\u1d18': 'p', '\u0280': 'r', '\ua731': 's', '\u1d1b': 't', '\u1d1c': 'u', '\u1d20': 'v',
	'\u1d21': 'w', '\u028f': 'y', '\u1d22': 'z', '\u0142': 'l', '\u00f8': 'o', '\u0111': 'd', '\u0127': 'h', '\u0167': 't',
	'\u0180': 'b', '\u01c0': 'l',
	// Armenian (case folding has already turned the capitals OH and SEH
	// into these small letters).
	'\u0585': 'o', '\u0570': 'h', '\u057d': 'u', '\u0578': 'n', '\u0566': 'q', '\u0581': 'g', '\u0575': 'j',
	// Cherokee capitals, which look like Latin capitals, and their small
	// letters (capital + 0x97D0, U+AB70 on): Unicode case folding goes from
	// small to capital for Cherokee but x/text/cases folds capital to small,
	// so both are listed.
	'\u13a0': 'd', '\u13a1': 'r', '\u13a2': 't', '\u13a5': 'i', '\u13a9': 'y', '\u13aa': 'a', '\u13ab': 'j', '\u13ac': 'e',
	'\u13b3': 'w', '\u13b7': 'm', '\u13bb': 'h', '\u13bd': 'y', '\u13c0': 'g', '\u13c2': 'h', '\u13c3': 'z', '\u13cf': 'b',
	'\u13d2': 'r', '\u13d3': 'w', '\u13d4': 'w', '\u13d9': 'v', '\u13da': 's', '\u13de': 'l', '\u13df': 'c', '\u13e2': 'p',
	'\u13e6': 'k', '\u13e7': 'd', '\u13f4': 'b', '\uab70': 'd', '\uab71': 'r', '\uab72': 't', '\uab75': 'i', '\uab79': 'y',
	'\uab7a': 'a', '\uab7b': 'j', '\uab7c': 'e', '\uab83': 'w', '\uab87': 'm', '\uab8b': 'h', '\uab8d': 'y', '\uab90': 'g',
	'\uab92': 'h', '\uab93': 'z', '\uab9f': 'b', '\uaba2': 'r', '\uaba3': 'w', '\uaba4': 'w', '\uaba9': 'v', '\uabaa': 's',
	'\uabae': 'l', '\uabaf': 'c', '\uabb2': 'p', '\uabb6': 'k', '\uabb7': 'd', '\u13fc': 'b', // small YV is U+13FC
	// Lisu (no case; the letters are shaped like Latin capitals).
	'\ua4d0': 'b', '\ua4d1': 'p', '\ua4d3': 'd', '\ua4d4': 't', '\ua4d6': 'g', '\ua4d7': 'k', '\ua4d9': 'j', '\ua4da': 'c',
	'\ua4dc': 'z', '\ua4dd': 'f', '\ua4df': 'm', '\ua4e0': 'n', '\ua4e1': 'l', '\ua4e2': 's', '\ua4e3': 'r', '\ua4e6': 'v',
	'\ua4e7': 'h', '\ua4ea': 'w', '\ua4eb': 'x', '\ua4ec': 'y', '\ua4ee': 'a', '\ua4f2': 'i', '\ua4f3': 'o', '\ua4f4': 'u',
	// Digits and a symbol that pass for letters.
	'0': 'o', '1': 'l', '|': 'l',
}

// ignorable reports whether r renders as nothing: a format character (Cf), a
// variation selector, or another default-ignorable code point such as the
// Hangul fillers U+115F, U+1160, U+3164 and U+FFA0, which are letters (Lo) and
// would otherwise survive the letter filter.
func ignorable(r rune) bool {
	return unicode.In(r, unicode.Cf, unicode.Variation_Selector, unicode.Other_Default_Ignorable_Code_Point)
}

// Skeleton returns the comparison form of a display name: NFKD (fullwidth and
// other compatibility forms become plain letters, accents split off),
// default-ignorable code points dropped, Greek, Cyrillic, Armenian, Cherokee and
// Lisu look-alikes mapped to Latin, Unicode case folding, i and l folded
// together (a capital I and a small l are the same stroke), every rune that is
// not a letter or digit dropped (the accents NFKD split off, spaces,
// punctuation, symbols such as a check mark), then "rn" read as "m" and "vv" as
// "w". Two names with the same non-empty skeleton look alike.
func Skeleton(s string) string {
	s = norm.NFKD.String(s)
	var b strings.Builder
	for _, r := range s {
		if ignorable(r) {
			continue
		}
		if m, ok := upperLookalikes[r]; ok {
			r = m
		}
		b.WriteRune(r)
	}
	s = cases.Fold().String(b.String())
	b.Reset()
	for _, r := range s {
		if m, ok := lowerLookalikes[r]; ok {
			r = m
		}
		if r == 'i' {
			r = 'l'
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	// One pass after the spaces are gone, so "r n" reads as "m" too.
	return strings.NewReplacer("rn", "m", "vv", "w").Replace(b.String())
}

// LooksLike reports whether two display names render alike: their skeletons
// are equal and not empty.
func LooksLike(a, b string) bool {
	sa := Skeleton(a)
	return sa != "" && sa == Skeleton(b)
}

// WithoutBrackets returns the name with every bracketed part removed, brackets
// included, so "Priya (priya@example.com)" compares as "Priya". Any opening
// and closing punctuation counts (parentheses, square and curly brackets,
// angle and CJK brackets), after NFKC so fullwidth and small forms count too;
// nesting is followed, and an unmatched closing bracket is dropped.
func WithoutBrackets(s string) string {
	var b strings.Builder
	depth := 0
	for _, r := range norm.NFKC.String(s) {
		switch {
		case unicode.Is(unicode.Ps, r):
			depth++
		case unicode.Is(unicode.Pe, r):
			if depth > 0 {
				depth--
			}
		case depth == 0:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// HasSuffixShape reports whether the name holds a bracketed part that looks
// like the suffix the server appends to a look-alike name: an email ("@"
// inside) or a derived id ("u_" first). Such a name could otherwise forge the
// server's own marking ("Priya (priya@example.com)"), so it is suffixed too.
// Compared after NFKC, so a fullwidth "@" or bracket counts.
func HasSuffixShape(s string) bool {
	depth := 0
	var inner strings.Builder
	for _, r := range norm.NFKC.String(s) {
		switch {
		case unicode.Is(unicode.Ps, r):
			if depth == 0 {
				inner.Reset()
			}
			depth++
		case unicode.Is(unicode.Pe, r) && depth > 0:
			depth--
			if depth == 0 && suffixLike(inner.String()) {
				return true
			}
		case depth > 0:
			inner.WriteRune(r)
		}
	}
	// An unclosed bracket at the end ("Priya (priya@example.com") reads the
	// same to a person.
	return depth > 0 && suffixLike(inner.String())
}

func suffixLike(inner string) bool {
	inner = strings.TrimFunc(inner, func(r rune) bool { return unicode.IsSpace(r) || ignorable(r) })
	return strings.Contains(inner, "@") || strings.HasPrefix(strings.ToLower(inner), "u_")
}

// MixesLatin reports whether the name has a Latin rune and a rune of another
// script. Common (digits, punctuation, spaces, symbols) and Inherited (combining
// marks) count for neither. A name such as "Priya" with one Cyrillic letter is
// a classic spoof; a real name that mixes scripts only gets a suffix.
func MixesLatin(s string) bool {
	latin, other := false, false
	for _, r := range s {
		switch {
		case unicode.In(r, unicode.Latin):
			latin = true
		case unicode.In(r, unicode.Common, unicode.Inherited):
		default:
			other = true
		}
	}
	return latin && other
}
