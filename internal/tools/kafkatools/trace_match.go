package kafkatools

import "strings"

// Literal matchers for the regexps on sanitizeTrace's hot path (#533). Each
// gives exactly the answer of the regexp it stands in for. Go's (?i) folds
// the ASCII letters k and s with two non-ASCII runes, KELVIN SIGN U+212A and
// LONG S U+017F, and no other rune with an ASCII letter. (?i) folds \w the
// same way, but \b is ASCII-only and \s and \d hold no letters. So the
// matchers search foldLower's copy, where those two runes become k and s,
// and test \b with its nonWord set: for \b the original runes are not word
// characters.

// folded is foldLower's result.
type folded struct {
	low     string
	nonWord map[int]bool // offsets in low of a k or s that was a fold rune
}

// foldLower lowers ASCII letters and maps U+212A to k and U+017F to s.
// Offsets in low are offsets in s only when s holds neither rune.
func foldLower(s string) folded {
	if !strings.Contains(s, "\u212a") && !strings.Contains(s, "\u017f") {
		return folded{low: asciiLower(s)}
	}
	var b strings.Builder
	f := folded{nonWord: map[int]bool{}}
	for i := 0; i < len(s); i++ {
		switch {
		case strings.HasPrefix(s[i:], "\u212a"):
			f.nonWord[b.Len()] = true
			b.WriteByte('k')
			i += len("\u212a") - 1
		case strings.HasPrefix(s[i:], "\u017f"):
			f.nonWord[b.Len()] = true
			b.WriteByte('s')
			i += len("\u017f") - 1
		case 'A' <= s[i] && s[i] <= 'Z':
			b.WriteByte(s[i] + 'a' - 'A')
		default:
			b.WriteByte(s[i])
		}
	}
	f.low = b.String()
	return f
}

// word reports whether \b counts offset i of f.low as a word character.
func (f folded) word(i int) bool {
	return isREWord(f.low[i]) && !f.nonWord[i]
}

// boundary is \b before offset i of f.low.
func (f folded) boundary(i int) bool {
	before := i > 0 && f.word(i-1)
	return before != (i < len(f.low) && f.word(i))
}

// isRESpace is Go regexp's \s: [\t\n\f\r ].
func isRESpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\f' || c == '\r'
}

// skipRESpace returns the first index at or after i that is not \s.
func skipRESpace(s string, i int) int {
	for i < len(s) && isRESpace(s[i]) {
		i++
	}
	return i
}

// isREWord is Go regexp's \w: [0-9A-Za-z_].
func isREWord(c byte) bool {
	return isWordByte(c) || 'A' <= c && c <= 'Z'
}

// recordSnippetLiterals are the alternatives of recordSnippet that are a
// plain literal with no \b in front.
var recordSnippetLiterals = []string{
	"[source:", "unrecognized token", "invalid input syntax", "for input string",
	"failing row contains", "cannot parse", "duplicate entry", "invalid input value",
	"preview of field", "duplicate key value is",
}

// matchesRecordSnippet is recordSnippet.MatchString(s).
func matchesRecordSnippet(s string) bool {
	f := foldLower(s)
	low := f.low
	for _, lit := range recordSnippetLiterals {
		if strings.Contains(low, lit) {
			return true
		}
	}
	// wordRun returns the end of the \w run starting at i. Under (?i) Go
	// folds the class too, so \w matches the fold runes (\b does not).
	wordRun := func(i int) int {
		for i < len(low) && isREWord(low[i]) {
			i++
		}
		return i
	}
	return f.anyAtBoundary("detail:", nil) ||
		f.anyAtBoundary("where:", nil) ||
		f.anyAtBoundary("values", func(e int) bool { e = skipRESpace(low, e); return e < len(low) && low[e] == '(' }) ||
		f.anyAtBoundary("incorrect ", func(e int) bool {
			w := wordRun(e)
			return w > e && strings.HasPrefix(low[w:], " value")
		}) ||
		f.anyAtBoundary("batch entry ", func(e int) bool { return e < len(low) && '0' <= low[e] && low[e] <= '9' }) ||
		f.anyAtBoundary("set", func(e int) bool {
			w := skipRESpace(low, e)
			if w == e {
				return false
			}
			if w < len(low) && low[w] == '"' {
				w++
			}
			v := wordRun(w)
			if v == w {
				return false
			}
			if v < len(low) && low[v] == '"' {
				v++
			}
			v = skipRESpace(low, v)
			return v < len(low) && low[v] == '='
		}) ||
		f.anyAtBoundary("struct{", nil) ||
		f.anyAtBoundary("sinkrecord{", nil) ||
		f.anyAtBoundary("connectrecord{", nil) ||
		f.anyAtBoundary("sourcerecord{", nil) ||
		f.anyAtBoundary("record", func(e int) bool { e = skipRESpace(low, e); return e < len(low) && low[e] == '{' }) ||
		f.matchesKeyTuple() ||
		f.matchesValueOutOfRange()
}

// anyAtBoundary reports whether lit occurs in f.low after a \b and, when
// tail is not nil, tail accepts the index just past it.
func (f folded) anyAtBoundary(lit string, tail func(end int) bool) bool {
	for i := 0; ; {
		j := strings.Index(f.low[i:], lit)
		if j < 0 {
			return false
		}
		at := i + j
		if f.boundary(at) && (tail == nil || tail(at+len(lit))) {
			return true
		}
		i = at + 1
	}
}

// matchesSpanned reports a match of \b<lit>\s*<open>.*<close><after>, with
// \s+ in place of \s* when needSpace: open must follow lit, the .* runs to a
// close byte on open's line, and after(q) accepts the text past a close
// byte at q. The first accepted close and the first newline past an opener
// are found once and reused for later openers, so the check stays linear on
// a line of repeated openers.
func (f folded) matchesSpanned(lit string, needSpace bool, open, close byte, after func(q int) bool) bool {
	low := f.low
	next := -1 // the first accepted close past the last opener, or len(low)
	nl := -1   // the first '\n' past the last opener, or len(low)
	for i := 0; ; {
		j := strings.Index(low[i:], lit)
		if j < 0 {
			return false
		}
		at := i + j
		i = at + 1
		if !f.boundary(at) {
			continue
		}
		e := at + len(lit)
		o := skipRESpace(low, e)
		if needSpace && o == e || o >= len(low) || low[o] != open {
			continue
		}
		if next <= o {
			next = len(low)
			for q := o + 1; q < len(low); q++ {
				if low[q] == close && after(q) {
					next = q
					break
				}
			}
		}
		if nl <= o {
			nl = len(low)
			if k := strings.IndexByte(low[o+1:], '\n'); k >= 0 {
				nl = o + 1 + k
			}
		}
		if next < nl {
			return true
		}
	}
}

// matchesKeyTuple is recordSnippet's \bkey\s*\(.*\)\s*=\s*\( alternative.
func (f folded) matchesKeyTuple() bool {
	low := f.low
	return f.matchesSpanned("key", false, '(', ')', func(q int) bool {
		e := skipRESpace(low, q+1)
		if e >= len(low) || low[e] != '=' {
			return false
		}
		e = skipRESpace(low, e+1)
		return e < len(low) && low[e] == '('
	})
}

// matchesValueOutOfRange is recordSnippet's
// \bvalue\s+".*"\s+is out of range alternative.
func (f folded) matchesValueOutOfRange() bool {
	low := f.low
	return f.matchesSpanned("value", true, '"', '"', func(q int) bool {
		e := skipRESpace(low, q+1)
		return e > q+1 && strings.HasPrefix(low[e:], "is out of range")
	})
}

// foldWordLen returns the byte length of the (?i)\w rune at s[i:] (an ASCII
// word byte, U+212A or U+017F), or 0.
func foldWordLen(s string, i int) int {
	switch {
	case isREWord(s[i]):
		return 1
	case strings.HasPrefix(s[i:], "K"):
		return len("K")
	case strings.HasPrefix(s[i:], "ſ"):
		return len("ſ")
	}
	return 0
}

// braceKeys are bracedPassword's keywords, reversed for matching back from
// the end of a word run.
var braceKeys = []string{"drowssap", "dwp", "terces"}

// endsWithBraceKey reports whether the word run s[rs:re] ends with a
// keyword under (?i): ASCII case-insensitively, with U+017F for s.
func endsWithBraceKey(s string, rs, re int) (keyStart int, ok bool) {
	for _, rev := range braceKeys {
		j := re
		matched := true
		for k := 0; k < len(rev) && matched; k++ {
			c := rev[k]
			switch {
			case j > rs && (s[j-1]|0x20) == c && 'a' <= c && c <= 'z':
				j--
			case c == 's' && j-len("ſ") >= rs && s[j-len("ſ"):j] == "ſ":
				j -= len("ſ")
			default:
				matched = false
			}
		}
		if matched {
			return j, true
		}
	}
	return 0, false
}

// maskBracedPasswords is bracedPassword.ReplaceAllString(s,
// "${1}"+redactedPassword) without the regexp (#533). A match is a (?i)\w
// run ending in password, pwd or secret, from its first \b, then
// \s*=\s*\{, then one or more of a byte other than '}' and '\n' or a "}}"
// pair, then a '}' or the end of the line. A run with no \b before the
// keyword, or a brace with an empty body, is no match.
func maskBracedPasswords(s string) string {
	var b strings.Builder
	last := 0
	for i := 0; i < len(s); {
		if foldWordLen(s, i) == 0 {
			i++
			continue
		}
		rs := i
		for i < len(s) {
			n := foldWordLen(s, i)
			if n == 0 {
				break
			}
			i += n
		}
		re := i
		ks, ok := endsWithBraceKey(s, rs, re)
		if !ok {
			continue
		}
		// The match starts at the run's first \b at or before the keyword:
		// the run start when it is an ASCII word byte, else a switch between
		// an ASCII word byte and a fold rune.
		start := -1
		for q := rs; q <= ks; {
			before := q > 0 && isREWord(s[q-1])
			if before != isREWord(s[q]) {
				start = q
				break
			}
			if q == ks {
				break
			}
			q += foldWordLen(s, q)
		}
		if start < 0 {
			continue
		}
		j := skipRESpace(s, re)
		if j >= len(s) || s[j] != '=' {
			continue
		}
		j = skipRESpace(s, j+1)
		if j >= len(s) || s[j] != '{' {
			continue
		}
		brace := j
		j++
		body := j
		for j < len(s) {
			if s[j] != '}' && s[j] != '\n' {
				j++
			} else if strings.HasPrefix(s[j:], "}}") {
				j += 2
			} else {
				break
			}
		}
		if j == body {
			continue // no body: the scan goes on from the run's end
		}
		if j < len(s) && s[j] == '}' {
			j++
		}
		b.WriteString(s[last:brace])
		b.WriteString(redactedPassword)
		last, i = j, j
	}
	if last == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

// mayHoldOracleThin is false only when oracleThinCreds cannot match s. None
// of the letters of "jdbc:oracle:" folds to a non-ASCII rune.
func mayHoldOracleThin(s string) bool {
	return strings.Contains(asciiLower(s), "jdbc:oracle:")
}
