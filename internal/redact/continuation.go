package redact

import "strings"

// openValue is a secret value left open at the end of a line: a quoted value
// whose closing quote is on a later line, or a bare value that runs into the
// line break. Text is scanned one line at a time, so the next line's head is
// masked from this state before the line rules run on it.
type openValue struct {
	ok bool
	// quote closes the value: `"`, `'`, `\"`; "" for a bare value.
	quote string
	// explicit is set when the line ended with a backslash: the value goes
	// on for sure (a shell or string continuation), not only perhaps (a
	// wrapped line).
	explicit bool
}

// openFrom is a cheap test that line could end inside a value that next
// continues, so most lines skip openAtEOL. It returns where the key of such a
// value can start at the earliest, or -1: before the last of an odd number of
// double or single quotes (an unclosed quoted value), or before the last
// three tokens when one of them holds a separator (key=value, key: value,
// key = value) and either line ends in a backslash or next starts with what
// continuation would take as a credential tail. A key is at most 64 bytes
// plus quotes and blanks.
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
			from = minFrom(from, strings.LastIndexByte(line, q)-72)
		}
	}
	if line[e-1] != '\\' {
		k := 0
		for k < len(next) && !isBareStop(next[k]) {
			k++
		}
		if !credentialTail(next[:k]) {
			return keyBoundary(line, from)
		}
	}
	k := e
	for range 3 {
		i := strings.LastIndexAny(line[:k], " \t")
		tok := line[i+1 : k]
		if strings.ContainsAny(tok, "=:") || strings.Contains(tok, "%3") {
			from = minFrom(from, i+1-8)
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
	return keyBoundary(line, from)
}

// keyBoundary moves from back to the start of a key name, so kvFind never
// starts inside one.
func keyBoundary(line string, from int) int {
	for from > 0 && isKeyChar(line[from-1]) {
		from--
	}
	return from
}

func minFrom(a, b int) int {
	b = max(b, 0)
	if a < 0 {
		return b
	}
	return min(a, b)
}

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
	case kvOpenDouble:
		return openValue{ok: true, quote: `"`, explicit: backslash}
	case kvOpenSingle:
		return openValue{ok: true, quote: `'`, explicit: backslash}
	}
	return openValue{}
}

// continuation masks the head of line that continues an open value and
// returns the state for the next line.
//
// A quoted value runs to its closing quote. Without one on this line, a value
// continued with a backslash runs over the whole line; otherwise the line is
// treated like the tail of a bare value. A bare value continues into the
// line's first token only when the line does not start with whitespace and,
// unless the previous line ended in a backslash, the token reads like the
// tail of a credential (see credentialTail): a plain word, a timestamp, a
// log prefix or a dotted name after a password line is evidence.
func continuation(line string, open openValue, st *Stats) (string, openValue) {
	e := len(line)
	if e > 0 && line[e-1] == '\r' {
		e--
	}
	if open.quote != "" {
		if i := closingQuote(line[:e], open.quote); i >= 0 {
			return maskHead(line, i, st), openValue{}
		}
		if open.explicit {
			k := e
			next := openValue{}
			if k > 0 && line[k-1] == '\\' {
				k--
				next = open
			}
			return maskHead(line, k, st), next
		}
	}
	if e == 0 || line[0] == ' ' || line[0] == '\t' {
		return line, openValue{}
	}
	k := 0
	for k < e && !isBareStop(line[k]) {
		k++
	}
	if !open.explicit && !credentialTail(line[:k]) {
		return line, openValue{}
	}
	next := openValue{}
	if k == e-1 && line[k] == '\\' && open.quote == "" {
		next = openValue{ok: true, explicit: true}
	}
	return maskHead(line, k, st), next
}

// maskHead masks line[:k] as one secret fragment. Fragments keep no
// characters: they are pieces of a password, often short.
func maskHead(line string, k int, st *Stats) string {
	head := line[:k]
	if head == "" || isMasked(head) || isPlaceholder(head) {
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

// bareStops marks the bytes that end a bare value (kvAnchored group 6).
var bareStops = func() (t [256]bool) {
	for _, c := range []byte(" \t\r\n\f\v,;&\"'})]<>\\") {
		t[c] = true
	}
	return t
}()

func isBareStop(c byte) bool { return bareStops[c] }

// credentialTail reports whether t, the first token of a line after a bare
// password at the end of the previous line, reads like the rest of that
// password rather than new text: at least two characters, no separator
// (':' or a '=' other than base64 padding), no leading bracket, path or
// flag character, and mixed like a generated value (lower case with a digit
// or an inner capital, or capitals with a digit). Plain words, numbers, IPs,
// timestamps and dotted names stay. Over-masks a name such as orders_v2.
func credentialTail(t string) bool {
	if len(t) < 2 || strings.IndexByte("[({<#/-*@.", t[0]) >= 0 || strings.IndexByte(t, ':') >= 0 {
		return false
	}
	if i := strings.IndexByte(t, '='); i >= 0 && strings.Trim(t[i:], "=") != "" {
		return false
	}
	u, l, d := classes(t)
	innerUpper := u > 0 && !(u == 1 && t[0] >= 'A' && t[0] <= 'Z')
	return l > 0 && (d > 0 || innerUpper) || u > 0 && d > 0
}
