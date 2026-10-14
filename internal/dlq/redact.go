package dlq

import (
	"strings"
	"unicode"
)

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
//     rest of the text) and every double-quoted string with "<redacted>";
//  4. replace single-quoted spans ('jane@example.com', 'abc 123', '5') unless
//     the body looks like an identifier ('amount', 'users.email',
//     'VALUE_CONVERTER', 'null'); the quote must open at a word start and have
//     a closer, so "can't" is not a quote;
//  5. replace the operand after "Unrecognized token" (Jackson quotes the
//     offending bytes there, even when they look like an identifier) and the
//     tuples after VALUES (batch SQL with the row's values).
//
// Control characters and runs of spaces become one space. The result is at
// most 300 runes and RedactMessage is idempotent.
//
// Limits: a bare identifier-shaped value in single quotes ('jane') is kept,
// because 'amount' (a column name) looks the same. Typographic and fullwidth
// quotes are not recognised.
//
// Unquoted values are masked too (rule 6): e-mail addresses, card numbers
// (Luhn-checked), SSN-like and phone-like numbers, and the values of
// key=value / key: value credentials (password, secret, token, api key, ...),
// plus everything internal/redact finds. Offsets, LSNs, epoch timestamps and
// dates are kept.
func RedactMessage(s string) string {
	return redactFixed(s, messageMode)
}

// MaxTraceLineRunes caps one line of RedactTrace's output.
const MaxTraceLineRunes = 1000

// RedactTrace is RedactMessage for one line of a Java stack trace or a
// Connect task trace, where the structure is evidence: {...} and [...]
// groups are kept (their quoted contents are still scrubbed), a
// double-quoted identifier such as a subject name ("pg.public.orders-value")
// is kept, and the cap is MaxTraceLineRunes. Everything else is as in
// RedactMessage: the tail after "Detail:", "Where:" or "[Source:", quoted
// values, Jackson token operands, VALUES tuples and unquoted PII are removed.
func RedactTrace(line string) string {
	return redactFixed(line, traceMode)
}

// mode selects how much structure the scrub keeps.
type mode struct {
	cap int
	// keepStructure keeps {...} and [...] groups and identifier-shaped
	// double-quoted strings.
	keepStructure bool
}

var (
	messageMode = mode{cap: MaxMessageRunes}
	traceMode   = mode{cap: MaxTraceLineRunes, keepStructure: true}
)

func redactFixed(s string, m mode) string {
	// The first pass removes everything the rules name; the loop only runs
	// again when a removal joined text into something a rule matches.
	out := s
	for i := 0; i < 8; i++ {
		next := redactOnce(out, m)
		if next == out {
			break
		}
		out = next
	}
	return out
}

func redactOnce(s string, m mode) string {
	rs := capRunes([]rune(s), m.cap)
	rs = tidy(rs)
	rs = cutTail(rs)
	rs = scrub(rs, m)
	rs = []rune(maskPII(string(rs)))
	// Markers can make the text longer than what went in ("" -> "<redacted>"),
	// so the cap runs again at the end.
	return string(capRunes(tidy(rs), m.cap))
}

// capRunes is the one capping mechanism. It never leaves half a marker
// ("<redacted") or a trailing space at the cut.
func capRunes(rs []rune, n int) []rune {
	if len(rs) <= n {
		return rs
	}
	rs = rs[:n]
	for n := len(redactedRunes) - 1; n >= 1; n-- {
		if len(rs) >= n && string(rs[len(rs)-n:]) == string(redactedRunes[:n]) {
			rs = rs[:len(rs)-n]
			break
		}
	}
	for len(rs) > 0 && rs[len(rs)-1] == ' ' {
		rs = rs[:len(rs)-1]
	}
	return rs
}

// tidy turns white space (tab, newline, Unicode spaces) into single spaces,
// drops every other control character and invisible format character
// (zero-width joiners, bidi overrides) and trims. Dropping, not spacing, the
// others means "Det\x00ail:" still reads as "Detail:" to the tail rule.
func tidy(rs []rune) []rune {
	out := make([]rune, 0, len(rs))
	space := true // true at the start, so leading spaces are dropped
	for _, r := range rs {
		switch {
		case unicode.IsSpace(r):
			if !space {
				out = append(out, ' ')
				space = true
			}
		case unicode.IsControl(r), unicode.Is(unicode.Cf, r):
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

// scrub replaces quoted strings, balanced groups, the Jackson token operand
// and VALUES tuples.
func scrub(rs []rune, m mode) []rune {
	out := make([]rune, 0, len(rs))
	for i := 0; i < len(rs); {
		r := rs[i]
		switch {
		case m.keepStructure && (r == '{' || r == '[' || r == '}' || r == ']'):
			out = append(out, r)
			i++
		case m.keepStructure && r == '"':
			end := quoteEnd(rs, i)
			if end < 0 {
				return append(out, redactedRunes...)
			}
			if isIdentifier(rs[i+1 : end]) {
				out = append(out, rs[i:end+1]...)
			} else {
				out = append(out, redactedRunes...)
			}
			i = end + 1
		case r == '"' || r == '{' || r == '[':
			end := quoteEnd(rs, i)
			if r != '"' {
				end = groupEnd(rs, i)
			}
			out = append(out, redactedRunes...)
			if end < 0 {
				return out // unterminated: the rest of the text is inside it
			}
			i = end + 1
		case r == '}' || r == ']':
			// A closer with no opener: nothing to hide, but a space keeps the
			// neighbours from fusing into a word a rule would match.
			out = append(out, ' ')
			i++
		case r == '\'' && wordStart(rs, i):
			end := apostropheEnd(rs, i)
			switch {
			case end < 0:
				out = append(out, r) // an apostrophe, not a quote
				i++
			case isIdentifier(rs[i+1 : end]):
				out = append(out, rs[i:end+1]...)
				i = end + 1
			default:
				out = append(out, redactedRunes...)
				i = end + 1
			}
		case wordStart(rs, i) && hasPrefixFold(rs[i:], tokenPhrase):
			var done bool
			out, i, done = redactTokenOperand(out, rs, i)
			if done {
				return out
			}
		case wordStart(rs, i) && hasPrefixFold(rs[i:], valuesWord) && (i+len(valuesWord) == len(rs) || !isWordRune(rs[i+len(valuesWord)])):
			var done bool
			out, i, done = redactValues(out, rs, i)
			if done {
				return out
			}
		default:
			out = append(out, r)
			i++
		}
	}
	return out
}

var (
	tokenPhrase = []rune("unrecognized token")
	valuesWord  = []rune("values")
)

func wordStart(rs []rune, i int) bool { return i == 0 || !isWordRune(rs[i-1]) }

// apostropheEnd returns the index of the single quote that closes the one at
// i: the next one not followed by a letter or digit (so "it's" inside a span
// does not end it), or -1.
func apostropheEnd(rs []rune, i int) int {
	for k := i + 1; k < len(rs); k++ {
		if rs[k] == '\'' && (k+1 == len(rs) || !isWordRune(rs[k+1])) {
			return k
		}
	}
	return -1
}

// isIdentifier matches ^[A-Za-z_][A-Za-z0-9_.$-]{0,63}$: a column, table,
// class or enum name, or a keyword such as null.
func isIdentifier(b []rune) bool {
	if len(b) == 0 || len(b) > 64 {
		return false
	}
	for i, r := range b {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case i > 0 && (r >= '0' && r <= '9' || r == '.' || r == '$' || r == '-'):
		default:
			return false
		}
	}
	return true
}

// redactTokenOperand handles "Unrecognized token <operand>". The operand is
// the offending bytes from the record, so it is removed even when it is
// identifier-shaped. It returns the new output and index; done is true when
// the rest of the text was consumed.
func redactTokenOperand(out, rs []rune, i int) ([]rune, int, bool) {
	j := i + len(tokenPhrase)
	out = append(out, rs[i:j]...)
	if j < len(rs) && rs[j] == ' ' {
		out = append(out, ' ')
		j++
	}
	switch {
	case j >= len(rs):
		return out, j, true
	case strings.HasPrefix(string(rs[j:]), Redacted), rs[j] == '"', rs[j] == '{', rs[j] == '[':
		return out, j, false // already hidden, or the quote/group rules take it
	case rs[j] == '\'':
		end := -1
		for k := j + 1; k+1 < len(rs); k++ {
			if rs[k] == '\'' && rs[k+1] == ':' {
				end = k
				break
			}
		}
		if end < 0 {
			end = apostropheEnd(rs, j)
		}
		out = append(out, redactedRunes...)
		if end < 0 {
			return out, len(rs), true
		}
		return out, end + 1, false
	}
	k := j
	for k < len(rs) && rs[k] != ' ' && rs[k] != ':' {
		k++
	}
	return append(out, redactedRunes...), k, false
}

// redactValues handles "VALUES (..), (..)": the tuples hold a row's values.
func redactValues(out, rs []rune, i int) ([]rune, int, bool) {
	j := i + len(valuesWord)
	k := j
	for k < len(rs) && rs[k] == ' ' {
		k++
	}
	if k >= len(rs) || rs[k] != '(' {
		return append(out, rs[i:j]...), j, false // "values" in plain prose
	}
	out = append(out, rs[i:k]...)
	for {
		e := parenEnd(rs, k)
		if e < 0 {
			return append(out, redactedRunes...), len(rs), true
		}
		next := e + 1
		m := next
		for m < len(rs) && rs[m] == ' ' {
			m++
		}
		if m < len(rs) && rs[m] == ',' {
			m++
			for m < len(rs) && rs[m] == ' ' {
				m++
			}
			if m < len(rs) && rs[m] == '(' {
				k = m
				continue
			}
		}
		return append(out, redactedRunes...), next, false
	}
}

// parenEnd returns the index of the ')' that balances the '(' at i, or -1.
// Quoted strings inside are skipped; in single quotes a doubled ” is an
// escaped quote.
func parenEnd(rs []rune, i int) int {
	depth := 0
	for k := i; k < len(rs); k++ {
		switch rs[k] {
		case '"':
			e := quoteEnd(rs, k)
			if e < 0 {
				return -1
			}
			k = e
		case '\'':
			for m := k + 1; m < len(rs); m++ {
				if rs[m] != '\'' {
					continue
				}
				if m+1 < len(rs) && rs[m+1] == '\'' {
					m++
					continue
				}
				k = m
				break
			}
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return k
			}
		}
	}
	return -1
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
