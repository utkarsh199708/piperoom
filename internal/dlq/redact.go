package dlq

import "unicode"

// MaxMessageRunes is the cap on a Connect error-header value (the same 300
// runes as watch.clean). RedactMessage applies it first, because the part of
// a message that holds a record snippet is usually inside it.
const MaxMessageRunes = 300

// Redacted replaces every removed part of a message.
const Redacted = "<redacted>"

var redactedRunes = []rune(Redacted)

// tailMarkers start a tail that is dropped to the end of the message:
// Jackson's "[Source: (byte[])"{...}"; line: 1, column: 2]" snippet, and the
// PostgreSQL JDBC "Detail: Key (id)=(5) already exists." / "Where: ..." lines.
var tailMarkers = [][]rune{[]rune("[source:"), []rune("detail:"), []rune("where:")}

// RedactMessage removes the parts of a Connect exception message (or the
// first line of a stack trace) that can carry record bytes. Converters and
// drivers quote the failing record in their messages, and a 300-rune cap
// keeps exactly the part where the snippet sits, so:
//
//  1. cap at 300 runes;
//  2. drop everything from "[Source:", "Detail:" or "Where:" (any case) to
//     the end;
//  3. replace every balanced {...} and [...] (an unbalanced opener takes the
//     rest of the text) and every double-quoted string with "<redacted>".
//
// Control characters and runs of spaces become one space. The result is at
// most 300 runes and RedactMessage is idempotent. Single-quoted text and
// text in parentheses is left alone (see the design doc, section 4.2, for
// the honest limit).
func RedactMessage(s string) string {
	// The first pass removes everything the rules name; the loop only runs
	// again when a removal joined text into something a rule matches.
	out := s
	for i := 0; i < 8; i++ {
		next := redactOnce(out)
		if next == out {
			break
		}
		out = next
	}
	return out
}

func redactOnce(s string) string {
	rs := []rune(s)
	if len(rs) > MaxMessageRunes {
		rs = rs[:MaxMessageRunes]
	}
	rs = tidy(rs)
	rs = cutTail(rs)
	rs = scrub(rs)
	rs = tidy(rs)
	// Markers can make the text longer than what went in ("" -> "<redacted>").
	if len(rs) > MaxMessageRunes {
		rs = tidy(rs[:MaxMessageRunes])
	}
	return string(rs)
}

// tidy turns control and space characters into single spaces, drops
// invisible format characters (zero-width joiners, bidi overrides) and trims.
func tidy(rs []rune) []rune {
	out := make([]rune, 0, len(rs))
	space := true // true at the start, so leading spaces are dropped
	for _, r := range rs {
		switch {
		case unicode.IsSpace(r) || unicode.IsControl(r):
			if !space {
				out = append(out, ' ')
				space = true
			}
		case unicode.Is(unicode.Cf, r):
			// dropped
		default:
			out = append(out, r)
			space = false
		}
	}
	if n := len(out); n > 0 && out[n-1] == ' ' {
		out = out[:n-1]
	}
	return out
}

// cutTail replaces everything from the first tail marker with one Redacted.
func cutTail(rs []rune) []rune {
	for i := range rs {
		if i > 0 && isWordRune(rs[i-1]) {
			continue // "Elsewhere:" is not "Where:"
		}
		for _, m := range tailMarkers {
			if hasPrefixFold(rs[i:], m) {
				out := append([]rune(nil), rs[:i]...)
				if len(out) > 0 && out[len(out)-1] != ' ' {
					out = append(out, ' ')
				}
				return append(out, redactedRunes...)
			}
		}
	}
	return rs
}

func isWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }

// hasPrefixFold reports whether rs starts with the lower-case pattern p,
// ignoring case. It compares rune by rune, so the index never shifts.
func hasPrefixFold(rs, p []rune) bool {
	if len(rs) < len(p) {
		return false
	}
	for i, pr := range p {
		if unicode.ToLower(rs[i]) != pr {
			return false
		}
	}
	return true
}

// scrub replaces double-quoted strings and balanced braces/brackets.
func scrub(rs []rune) []rune {
	out := make([]rune, 0, len(rs))
	for i := 0; i < len(rs); {
		end := -1
		switch rs[i] {
		case '"':
			end = quoteEnd(rs, i)
		case '{', '[':
			end = groupEnd(rs, i)
		case '}', ']':
			// A closer with no opener: nothing to hide, but a space keeps the
			// neighbours from fusing into a word a rule would match.
			out = append(out, ' ')
			i++
			continue
		default:
			out = append(out, rs[i])
			i++
			continue
		}
		out = append(out, redactedRunes...)
		if end < 0 {
			break // unterminated: the rest of the text is inside it
		}
		i = end + 1
	}
	return out
}

// quoteEnd returns the index of the quote that closes the one at i, or -1.
func quoteEnd(rs []rune, i int) int {
	for k := i + 1; k < len(rs); k++ {
		switch rs[k] {
		case '\\':
			k++ // skip the escaped rune
		case '"':
			return k
		}
	}
	return -1
}

// groupEnd returns the index of the closer that balances the opener at i, or
// -1. Braces and brackets share one depth counter, and quoted strings inside
// are skipped, so {"a":"}"} is one group.
func groupEnd(rs []rune, i int) int {
	depth := 0
	for k := i; k < len(rs); k++ {
		switch rs[k] {
		case '"':
			e := quoteEnd(rs, k)
			if e < 0 {
				return -1
			}
			k = e
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				return k
			}
		}
	}
	return -1
}
