package redact

import "strings"

// openValue is a secret value left open at the end of a line: a quoted value
// whose closing quote is on a later line, or a bare value that runs into the
// line break. Text is scanned one line at a time, so the next line's head is
// masked from this state before the line rules run on it.
type openValue struct {
	ok bool
	// quote closes the value: `"`, `'`, `\"`, `\\\"`; "" for a bare value.
	quote string
	// explicit is set when the line ended with a backslash: the value goes
	// on for sure (a shell or string continuation), not only perhaps (a
	// wrapped line).
	explicit bool
	// closes is set when a quoted value was seen to close properly within
	// maxQuotedLines lines; whole tells how many lines lie wholly inside it
	// before the closing one.
	closes bool
	whole  int
}

// maxQuotedLines bounds how many lines a quoted value is followed over
// without a backslash continuation.
const maxQuotedLines = 4

// openFrom is a cheap test that line could end inside a value that next
// continues, so most lines skip openAtEOL. It returns where the key of such a
// value can start at the earliest, or -1: before the last of an odd number of
// double or single quotes (an unclosed quoted value), or before the last
// three tokens when one of them holds a separator (key=value, key: value,
// key = value, padded keys) and either line ends in a backslash or next
// starts with what continuation would take as a credential tail.
func openFrom(line, next string) int {
	e := len(line)
	if e > 0 && line[e-1] == '\r' {
		e--
	}
	if e == 0 || next == "" {
		return -1
	}
	from := -1
	for _, q := range []byte{'"', '\''} {
		if strings.Count(line, string(q))%2 == 1 {
			i := strings.LastIndexByte(line, q)
			from = minFrom(from, min(keyStartBefore(line, i), i-72))
		}
	}
	if line[e-1] != '\\' && !tailFollows(next) {
		return from
	}
	k := e
	for range 3 {
		i := strings.LastIndexAny(line[:k], " \t")
		tok := line[i+1 : k]
		if strings.ContainsAny(tok, "=:") || strings.Contains(tok, "%3") || strings.Contains(tok, "%253") {
			from = minFrom(from, min(keyStartBefore(line, i+1), i+1-8))
			break
		}
		if i < 0 {
			break
		}
		k = i
		for k > 0 && (line[k-1] == ' ' || line[k-1] == '\t') {
			k--
		}
	}
	return from
}

// keyStartBefore returns where a key that precedes position i can start:
// back over blanks, one separator, blanks again (column-aligned config dumps
// pad keys with many), quotes and backslashes, then the key name.
func keyStartBefore(line string, i int) int {
	i = min(max(i, 0), len(line))
	skip := func(set string, limit int) {
		for n := 0; i > 0 && n < limit && strings.IndexByte(set, line[i-1]) >= 0; n++ {
			i--
		}
	}
	skip(" \t", len(line))
	skip(`"'\`, 4)
	skip("=:", 1)
	skip(" \t", len(line))
	skip(`"'\`, 4)
	for i > 0 && isKeyChar(line[i-1]) {
		i--
	}
	return i
}

func minFrom(a, b int) int {
	b = max(b, 0)
	if a < 0 {
		return b
	}
	return min(a, b)
}

// tailFollows reports whether next starts with a token continuation could
// take as the tail of a bare value.
func tailFollows(next string) bool {
	k := 0
	for k < len(next) && !bareStops[next[k]] {
		k++
	}
	return credentialTail(next[:k]) && !blankAt(next, k)
}

func blankAt(s string, k int) bool { return k < len(s) && (s[k] == ' ' || s[k] == '\t') }

// openAtEOL reports whether line ends inside the value of a secret key/value
// pair. prev is returned when the line holds no pair at its end.
func openAtEOL(line, lower string, prev openValue) openValue {
	ms := kvFind(line, lower)
	if len(ms) == 0 {
		return prev
	}
	m := ms[len(ms)-1]
	e := len(line)
	if e > 0 && line[e-1] == '\r' {
		e--
	}
	if m[1] < e-1 {
		return openValue{}
	}
	g := -1
	for _, k := range kvValueGroups {
		if m[2*k] >= 0 {
			g = k
			break
		}
	}
	if g < 0 {
		return openValue{}
	}
	val := line[m[2*g]:m[2*g+1]]
	if isMasked(val) || val != "" && isPlaceholder(val) {
		return openValue{}
	}
	backslash := e > 0 && line[e-1] == '\\'
	// A value cut off by truncation ends in an ellipsis; nothing of it
	// is on the next line.
	if !backslash && strings.HasSuffix(val, "…") {
		return openValue{}
	}
	switch g {
	case kvBare:
		v1 := m[2*g+1]
		switch {
		case v1 == e && len(val) > 1:
			return openValue{ok: true}
		case v1 == e-1 && backslash:
			return openValue{ok: true, explicit: true}
		}
	case kvOpenEsc:
		return openValue{ok: true, quote: `\"`, explicit: backslash && !strings.HasSuffix(line[:e], `\\`)}
	case kvOpenEsc3:
		return openValue{ok: true, quote: `\\\"`, explicit: backslash && !strings.HasSuffix(line[:e], `\\`)}
	case kvOpenDouble:
		return openValue{ok: true, quote: `"`, explicit: backslash}
	case kvOpenSingle:
		return openValue{ok: true, quote: `'`, explicit: backslash}
	}
	return openValue{}
}

// lookAhead decides how far an open quoted value without a backslash goes:
// to the first quote in the next maxQuotedLines lines when that quote ends
// the value (followed by the end of the line, a blank or , ; } ] )). Any
// other first quote, or a blank line, means the value does not go on there.
func lookAhead(open openValue, rest string) openValue {
	if !open.ok || open.quote == "" || open.explicit || open.closes {
		return open
	}
	for n := range maxQuotedLines {
		if rest == "" {
			break
		}
		line := rest
		if i := strings.IndexByte(rest, '\n'); i >= 0 {
			line, rest = rest[:i], rest[i+1:]
		} else {
			rest = ""
		}
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			break
		}
		if i := closingQuote(line, open.quote); i >= 0 {
			if closesValue(line, i+len(open.quote)) {
				open.closes, open.whole = true, n
			}
			break
		}
	}
	return open
}

func closesValue(line string, k int) bool {
	return k >= len(line) || strings.IndexByte(" \t,;})]", line[k]) >= 0
}

// continuation masks the head of line that continues an open value and
// returns the state for the next line.
//
// A quoted value continued with a backslash runs to its closing quote or
// over the whole line. A quoted value without one runs to its closing quote
// when lookAhead found it; otherwise the line is treated like the tail of a
// bare value. A bare value continues into the line's first token only when
// the line does not start with whitespace and, unless the previous line
// ended in a backslash, the token reads like the tail of a credential (see
// credentialTail) and is not followed by more words: a plain word, a
// timestamp, a log prefix or a name after a password line is evidence.
func continuation(line string, open openValue, st *Stats) (string, openValue) {
	e := len(line)
	if e > 0 && line[e-1] == '\r' {
		e--
	}
	if open.quote != "" {
		switch {
		case open.explicit:
			if i := closingQuote(line[:e], open.quote); i >= 0 {
				return maskHead(line, i, st), openValue{}
			}
			k := e
			next := openValue{}
			if k > 0 && line[k-1] == '\\' {
				k--
				next = open
			}
			return maskHead(line, k, st), next
		case open.closes && open.whole > 0:
			open.whole--
			return maskHead(line, e, st), open
		case open.closes:
			return maskHead(line, max(closingQuote(line[:e], open.quote), 0), st), openValue{}
		}
	}
	if e == 0 || line[0] == ' ' || line[0] == '\t' {
		return line, openValue{}
	}
	k := 0
	for k < e && !bareStops[line[k]] {
		k++
	}
	if !open.explicit && (!credentialTail(line[:k]) || blankAt(line, k)) {
		return line, openValue{}
	}
	next := openValue{}
	if k == e-1 && line[k] == '\\' && open.quote == "" {
		next = openValue{ok: true, explicit: true}
	}
	return maskHead(line, k, st), next
}

// maskHead masks line[:k] as one secret fragment. Fragments keep no
// characters: they are pieces of a password, often short. A one-byte head
// is never masked: it gives nothing away, and is far more often a bracket.
func maskHead(line string, k int, st *Stats) string {
	head := line[:k]
	if len(head) < 2 || isMasked(head) || isPlaceholder(head) {
		return line
	}
	st.add(KindKeyValue)
	return mask(head, styleSecret) + line[k:]
}

// closingQuote returns where quote q first closes in s, or -1. A double quote
// preceded by an odd number of backslashes is escaped.
func closingQuote(s, q string) int {
	switch q {
	case `"`:
		for i := 0; i < len(s); i++ {
			if s[i] != '"' {
				continue
			}
			n := 0
			for j := i - 1; j >= 0 && s[j] == '\\'; j-- {
				n++
			}
			if n%2 == 0 {
				return i
			}
		}
		return -1
	default:
		return strings.Index(s, q)
	}
}

// bareStops marks the bytes that end a bare value: the negated class of
// kvAnchored group 6 (\s there is [\t\n\f\r ]).
var bareStops = func() (t [256]bool) {
	for _, c := range []byte(" \t\r\n\f,;&\"'})]<>\\") {
		t[c] = true
	}
	return t
}()

// credentialTail reports whether t, the first token of a line after a bare
// password at the end of the previous line, reads like the rest of that
// password rather than new text: at least two characters, no separator
// (':' or a '=' other than base64 padding) and no '@', no leading bracket,
// path or flag character, not hex digits and dashes only (ids, UUIDs,
// commits), and mixed like a generated value (lower case with a digit or an
// inner capital, or capitals with a digit). Plain words, numbers, IPs,
// timestamps and dotted names stay.
func credentialTail(t string) bool {
	if len(t) < 2 || strings.IndexByte("[({<#/-*@.", t[0]) >= 0 {
		return false
	}
	u, l, d := classes(t)
	innerUpper := u > 0 && !(u == 1 && t[0] >= 'A' && t[0] <= 'Z')
	if !(l > 0 && (d > 0 || innerUpper) || u > 0 && d > 0) {
		return false
	}
	hexOnly := true
	for i := 0; i < len(t); i++ {
		switch c := t[i]; {
		case c == ':' || c == '@':
			return false
		case c == '=':
			if strings.TrimLeft(t[i:], "=") != "" {
				return false
			}
		case !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' || c == '-'):
			hexOnly = false
		}
	}
	return !hexOnly
}
