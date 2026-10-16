package dlq

import (
	"strings"
	"unicode"
	"unicode/utf8"
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
//  1. cap at 300 runes; a value that step 6's own patterns match (not one
//     only internal/redact finds) and that the cut would split is replaced
//     whole, also when tabs or runs of spaces separate its groups; else a
//     word of up to 160 runes the cut would split is dropped;
//  2. drop everything from "[Source:", "Detail:" or "Where:" (any case) to
//     the end;
//  3. replace every balanced {...} and [...] (an unbalanced opener takes the
//     rest of the text) and every double-quoted string with "<redacted>";
//  4. replace single-quoted spans ('jane@example.com', 'abc 123', '5') unless
//     the body looks like an identifier ('amount', 'users.email',
//     'VALUE_CONVERTER', 'null'); the quote must open at a word start and have
//     a closer, so "can't" is not a quote;
//  5. replace the operand after "Unrecognized token" (Jackson quotes the
//     offending bytes there, even when they look like an identifier), the
//     tuples after VALUES (batch SQL with the row's values) and the tuple
//     after "Failing row contains" (PostgreSQL's row, which some drivers
//     print without a Detail: prefix; its values are unquoted, so everything
//     after the phrase is replaced);
//  6. replace unquoted values: e-mail addresses, Luhn-valid card numbers,
//     SSN-like and phone-like numbers, the value after a credential key
//     (password=, token:, api_key=, ...) and what internal/redact finds (a
//     URL's user:password@, bearer tokens, cloud keys).
//
// Then, on that output, the values of a PostgreSQL "Key (cols)=(values)"
// detail without a "Detail:" label are replaced (maskKeyValues), and so are
// those of MySQL's "Duplicate entry 'v' for key", SQL Server's "The duplicate
// key value is (v)" and PostgreSQL details printed with "= (" or "＝（"
// (maskKeyForms); column and key names stay.
//
// Control characters and runs of spaces become one space. The result is at
// most 300 runes and RedactMessage is idempotent.
//
// Limits: a bare identifier-shaped value in single quotes ('jane') is kept,
// because 'amount' (a column name) looks the same, and so is an unquoted name
// or address ("name=Jane Doe"). Typographic and fullwidth quotes are not
// recognised (rule 6 still masks an e-mail address inside them).
func RedactMessage(s string) string {
	out := redactRules(s)
	// Additive passes (docs/agents/org.md §12): they only add masks on the
	// output of the rules above, which then run again on the result.
	for i := 0; i < maxRedactRounds; i++ {
		next := redactRules(string(capRunes(tidy([]rune(maskKeyForms(maskKeyValues(out)))))))
		if next == out {
			break
		}
		out = next
	}
	return out
}

// maxRedactRounds bounds both fixed-point loops. Each round only removes text
// or replaces it with Redacted, so one or two rounds settle real messages.
const maxRedactRounds = 8

// redactRules is main's RedactMessage, unchanged: the additive passes build on
// its output, so a new rule can over-mask but never undo what it masks.
func redactRules(s string) string {
	// The first pass removes everything the rules name; the loop only runs
	// again when a removal joined text into something a rule matches.
	out := s
	for i := 0; i < maxRedactRounds; i++ {
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
	// A cut can drop the closing quote of a value: then an open single quote
	// is a value, not an apostrophe (scrub).
	cut := len(rs) > MaxMessageRunes
	if cut {
		rs = safeCut(rs)
	}
	rs = capRunes(rs)
	rs = tidy(rs)
	rs = cutTail(rs)
	rs = scrub(rs, cut)
	rs = []rune(maskUnquoted(string(rs)))
	// Markers can make the text longer than what went in ("" -> "<redacted>"),
	// so the cap runs again at the end.
	return string(capRunes(tidy(rs)))
}

// urlTokenSpans are the URLs safeCut replaces whole, found by the literal
// "://" alone (no scheme rule: "my_postgres://" and a scheme glued to a long
// word are URLs too): each runs from the last white space, quote or bracket
// before the "://" (or the start) to the next white space or quote after it.
// The start never goes before the scheme past a '=' or a ':' not followed by
// '/' ("url=http://", "jdbc:postgresql://") unless the text it would skip
// holds an '@'. #539: only a URL that may carry a credential is a span: one
// with an '@' after its "://" (userinfo; internal/redact allows '/', '?', '#'
// and ':' in the password, so no split inside it is safe), a credential key
// or "sig=" in it, or one that runs to the end of s when open says s was cut
// short (its '@' may come later). A credential-free URL is cut by the word
// rule, so its path stays.
func urlTokenSpans(s string, open bool) [][]int {
	var spans [][]int
	for off := 0; ; {
		k := strings.Index(s[off:], "://")
		if k < 0 {
			return spans
		}
		k += off
		start := strings.LastIndexAny(s[:k], urlTokenStops+"([{<") + 1
		for m := k - 1; m >= start; m-- {
			if s[m] == '=' || s[m] == ':' && s[m+1] != '/' {
				if !strings.Contains(s[start:m+1], "@") {
					start = m + 1
				}
				break
			}
		}
		end := len(s)
		if e := strings.IndexAny(s[k:], urlTokenStops); e >= 0 {
			end = k + e
		}
		tok := s[k:end]
		if strings.Contains(tok, "@") || kvSecretRe.MatchString(tok) ||
			strings.Contains(strings.ToLower(tok), "sig=") || open && end == len(s) {
			spans = append(spans, []int{start, end})
		}
		off = end
		if off <= k {
			off = k + 3
		}
	}
}

// urlTokenStops end a URL token: white space and quotes.
const urlTokenStops = " \t\n\r\v\f'\""

// Look-ahead of safeCut past the cap: enough for the longest value a mask
// rule matches (a 128-rune e-mail local part plus its domain).
const (
	cutLookahead = 256
	// maxCutToken is the longest word the cut drops whole. A longer run
	// without spaces is cut where the cap falls (it is not a value a mask
	// rule knows, and dropping it could empty the message).
	maxCutToken = 160
)

// safeCut makes the first cut of rs (len(rs) > MaxMessageRunes). A cut value
// no longer matches its mask ("jane.doe@exam", "123-45-678") and went out in
// part. So a value that maskUnquoted's own patterns match, or a URL that may
// carry a credential (urlTokenSpans), and that
// straddles the cap (also one with spaces, "4111 1111 1111 1111") is replaced by
// Redacted from its start; otherwise the cut moves back to the start of a word
// that straddles the cap. The values are found in the tidied window, as the
// masks see it ("555\t123\t4567" is a phone), and mapped back to rs. The word
// rule does not follow the value rule: it would drop the key of
// "password=<value>" and could empty the message.
func safeCut(rs []rune) []rune {
	at := MaxMessageRunes
	tw, raw := tidyIndex(rs[:min(len(rs), MaxMessageRunes+cutLookahead)])
	capT := len(tw) // tidied runes that come from before the cap
	for j, i := range raw {
		if i >= MaxMessageRunes {
			capT = j
			break
		}
	}
	win := string(tw)
	capByte := len(string(tw[:capT]))
	// A URL that may carry a credential is one value from its scheme to the
	// next white space or quote: no split inside it is safe
	// ("postgres://app:S3cr/t9x@db..." cut after "/").
	open := len(rs) > MaxMessageRunes+cutLookahead
	spans := append(piiSpans(win), urlTokenSpans(win, open)...)
	for _, sp := range spans {
		if sp[0] < capByte && sp[1] > capByte {
			at = min(at, raw[utf8.RuneCountInString(win[:sp[0]])])
		}
	}
	if at < MaxMessageRunes {
		return append(rs[:at:at], redactedRunes...)
	}
	if at > 0 && at < len(rs) && !unicode.IsSpace(rs[at-1]) && !unicode.IsSpace(rs[at]) {
		start := at
		for start > 0 && !unicode.IsSpace(rs[start-1]) {
			start--
		}
		if at-start <= maxCutToken {
			at = start
		}
	}
	return rs[:at]
}

// capRunes is the one capping mechanism. It never leaves half a marker
// ("<redacted") or a trailing space at the cut.
func capRunes(rs []rune) []rune {
	if len(rs) <= MaxMessageRunes {
		return rs
	}
	rs = rs[:MaxMessageRunes]
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
	out, _ := tidyIndex(rs)
	return out
}

// tidyIndex is tidy that also returns, for each rune of the result, its index
// in rs.
func tidyIndex(rs []rune) ([]rune, []int) {
	out := make([]rune, 0, len(rs))
	idx := make([]int, 0, len(rs))
	space := true // true at the start, so leading spaces are dropped
	for i, r := range rs {
		switch {
		case unicode.IsSpace(r):
			if !space {
				out, idx = append(out, ' '), append(idx, i)
				space = true
			}
		case unicode.IsControl(r), unicode.Is(unicode.Cf, r):
			// dropped
		default:
			out, idx = append(out, r), append(idx, i)
			space = false
		}
	}
	if n := len(out); n > 0 && out[n-1] == ' ' {
		out, idx = out[:n-1], idx[:n-1]
	}
	return out, idx
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
// and VALUES tuples. cut says the text was cut at the cap: a single quote that
// opens a value and has no closer then takes the rest of the text, because
// its closer was past the cut.
func scrub(rs []rune, cut bool) []rune {
	out := make([]rune, 0, len(rs))
	for i := 0; i < len(rs); {
		r := rs[i]
		switch {
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
			case end < 0 && cut:
				return append(out, redactedRunes...)
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
		case wordStart(rs, i) && hasPrefixFold(rs[i:], failingRowPhrase):
			var done bool
			out, i, done = redactFailingRow(out, rs, i)
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

	failingRowPhrase = []rune("failing row contains")
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

// redactFailingRow handles "Failing row contains (7, jane@example.com, -1)".
// PostgreSQL prints the row's values unquoted, so no rule can tell where the
// tuple ends (a value may hold ")" or ", "): the rest of the text goes, as in
// the "Detail:" tail. Text that is already redacted stays, which keeps the
// function idempotent.
func redactFailingRow(out, rs []rune, i int) ([]rune, int, bool) {
	j := i + len(failingRowPhrase)
	if j < len(rs) && isWordRune(rs[j]) {
		return append(out, rs[i:j]...), j, false // "failing row containsX" is not the phrase
	}
	k := j
	for k < len(rs) && rs[k] == ' ' {
		k++
	}
	switch {
	case k >= len(rs):
		return append(out, rs[i:j]...), j, false // phrase at the end: nothing to hide
	case strings.HasPrefix(string(rs[k:]), Redacted):
		return append(out, rs[i:k]...), k, false
	}
	out = append(out, rs[i:j]...)
	return append(append(out, ' '), redactedRunes...), len(rs), true
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
