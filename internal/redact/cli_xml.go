package redact

import "strings"

// flagRule masks a secret given as the argument of a command-line flag
// (psql --password X, --sasl.password "X Y", confluent login --api-secret X)
// or as curl's -u/--user user:password.
var flagRule = rule{
	kind:   KindKeyValue,
	find:   flagFind,
	groups: []int{1},
	style:  styleSecret,
	pre:    containsAny("pass", "secret", "token", "-u"),
}

// xmlRule masks the text of an XML element whose name ends in a secret word:
// <password>X</password>, with attributes, CDATA, padding, the value on its
// own lines, or one child element (<password><value>X</value></password>).
// Like pemRule it runs over the whole text, since the value may sit on a line
// of its own.
var xmlRule = rule{
	kind:   KindKeyValue,
	find:   xmlFind,
	groups: []int{1},
	style:  styleSecret,
	pre:    func(s, _ string) bool { return strings.Contains(s, "</") || strings.Contains(s, `<\/`) },
}

// flagWords end a flag or element name that names a secret. A name that only
// starts with one (--password-file, --token-ttl, <passwordPolicy>) is not.
var flagWords = []string{"password", "passwd", "passphrase", "secret", "token"}

func endsInFlagWord(lowerName string) bool {
	for _, w := range flagWords {
		if strings.HasSuffix(lowerName, w) {
			return true
		}
	}
	return false
}

// flagBefore reports characters a flag may follow: a blank, an opening quote
// or bracket of an argument list (args: ["--password", "x"]), or '=' of an
// environment variable holding options.
func flagBefore(c byte) bool {
	return strings.IndexByte(" \t\"'[(,=", c) >= 0
}

// flagFind finds secret flag arguments in one line. Each step moves forward
// past what it read, so a hostile line costs time linear in its length.
func flagFind(s, _ string) [][]int {
	var out [][]int
	for i := 0; i < len(s); {
		j := strings.IndexByte(s[i:], '-')
		if j < 0 {
			break
		}
		p := i + j
		if p > 0 && !flagBefore(s[p-1]) {
			i = p + 1
			continue
		}
		ns := p + 1
		if ns < len(s) && s[ns] == '-' {
			ns++
		}
		ne := ns
		for ne < len(s) && isKeyChar(s[ne]) {
			ne++
		}
		i = max(ne, p+1)
		if ne == ns || !isLetter(s[ns]) || ne-ns > 64 {
			continue
		}
		if m := flagMatch(s, p, ns, ne); m != nil {
			out = append(out, m)
			i = m[1]
		}
	}
	return out
}

// flagMatch reads the argument of the flag s[ns:ne] (dashes from p) and
// returns its match, or nil when it holds no secret.
func flagMatch(s string, p, ns, ne int) []int {
	name := s[ns:ne]
	double := ns-p == 2
	if (name == "u" && !double) || (name == "user" && double) {
		return userMatch(s, p, ne)
	}
	lname := asciiLower(name)
	if strings.HasPrefix(lname, "no-") || strings.HasPrefix(lname, "no_") {
		return nil
	}
	if !endsInFlagWord(strings.TrimRight(lname, "._-")) {
		return nil
	}
	strength := secretKey(name)
	if strength == keyNone {
		return nil
	}
	v, quotedOnly := flagSep(s, ne)
	if v < 0 {
		return nil
	}
	vs, ve, end, quoted := flagValue(s, v)
	if ve <= vs || (quotedOnly && !quoted) {
		return nil
	}
	val := s[vs:ve]
	switch {
	case strength == keyWeak && !looksRandom(val[:min(len(val), 1024)], 12):
		return nil
	case strength == keyStrong && !quoted && plainWord(val):
		// "--password requires an argument": prose, not a password.
		return nil
	}
	return []int{p, end, vs, ve}
}

// flagSep returns where the argument after a flag ending at i starts: after
// '=', after blanks, or after the closing quote and comma of an argument list
// ("--password", "x"), where the value must be quoted too. It returns -1 when
// no argument follows.
func flagSep(s string, i int) (int, bool) {
	if i >= len(s) {
		return -1, false
	}
	switch s[i] {
	case '=':
		return i + 1, false
	case ' ', '\t':
		for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
		return i, false
	case '"', '\'':
		j := skipBlanks(s, i+1)
		if j < len(s) && s[j] == ',' {
			return skipBlanks(s, j+1), true
		}
	}
	return -1, false
}

func skipBlanks(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	return i
}

// flagValue reads one shell argument at v: \"...\" (inside a JSON string or
// a Go %q error), "..." with backslash escapes, '...', or a bare word that
// does not start with '-' (that is the next flag). A quote left open runs to
// the end of the line (a truncated summary). It returns the value's bounds,
// the end of the argument, and whether it was quoted.
func flagValue(s string, v int) (vs, ve, end int, quoted bool) {
	eol := len(s)
	if strings.HasSuffix(s, "\r") {
		eol--
	}
	if v >= eol {
		return v, v, v, false
	}
	switch {
	case strings.HasPrefix(s[v:], `\"`):
		vs = v + 2
		if k := strings.Index(s[vs:eol], `\"`); k >= 0 {
			return vs, vs + k, vs + k + 2, true
		}
		return vs, eol, eol, true
	case s[v] == '"':
		vs = v + 1
		for k := vs; k < eol; k++ {
			switch s[k] {
			case '\\':
				k++
			case '"':
				return vs, k, k + 1, true
			}
		}
		return vs, eol, eol, true
	case s[v] == '\'':
		vs = v + 1
		if k := strings.IndexByte(s[vs:eol], '\''); k >= 0 {
			return vs, vs + k, vs + k + 1, true
		}
		return vs, eol, eol, true
	case s[v] == '-':
		return v, v, v, false
	}
	ve = v
	for ve < eol && !isBlankOrQuote(s[ve]) {
		ve++
	}
	return v, ve, ve, false
}

func isBlankOrQuote(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '"' || c == '\''
}

// userMatch reads curl's -u/--user user:password at i, quoted or bare.
// docker's -u user:group and uid:gid look the same, so a password that is a
// plain word or a number is kept.
func userMatch(s string, p, i int) []int {
	v, _ := flagSep(s, i)
	if v < 0 || v >= len(s) {
		return nil
	}
	q := byte(0)
	if s[v] == '"' || s[v] == '\'' {
		q = s[v]
		v++
	}
	k := v
	for k < len(s) && s[k] != ':' && !isBlankOrQuote(s[k]) {
		k++
	}
	if k == v || k >= len(s) || s[k] != ':' {
		return nil
	}
	vs, ve := k+1, k+1
	if q != 0 {
		for ve < len(s) && s[ve] != q && s[ve] != '\r' && s[ve] != '\n' {
			ve++
		}
	} else {
		for ve < len(s) && !isBlankOrQuote(s[ve]) {
			ve++
		}
	}
	val := s[vs:ve]
	if val == "" || plainWord(val) || allDigits(val) {
		return nil
	}
	return []int{p, ve, vs, ve}
}

// plainWord reports a value of lower-case ASCII letters only, allowing
// trailing punctuation ("requires", "instead."): English, not a password.
func plainWord(v string) bool {
	v = strings.TrimRight(v, ".,;:!?)")
	if v == "" {
		return false
	}
	for i := 0; i < len(v); i++ {
		if v[i] < 'a' || v[i] > 'z' {
			return false
		}
	}
	return true
}

func isXMLNameChar(c byte) bool { return isKeyChar(c) || c == ':' }

// xmlFind finds the text of secret-named XML elements. Every scan stops at
// the next '<' and the search moves on from there; the one search that may
// pass a '<' (for the end of a CDATA section) is cached, so hostile text costs
// time linear in its length.
func xmlFind(s, _ string) [][]int {
	var out [][]int
	cdata := cdataEnds{s: s, next: -1}
	for i := 0; i < len(s); {
		j := strings.IndexByte(s[i:], '<')
		if j < 0 {
			break
		}
		p := i + j
		ns := p + 1
		ne := ns
		for ne < len(s) && isXMLNameChar(s[ne]) {
			ne++
		}
		i = max(ne, p+1)
		if ne == ns || !(isLetter(s[ns]) || s[ns] == '_') || ne-ns > 64 {
			continue
		}
		if !endsInFlagWord(asciiLower(s[ns:ne])) {
			continue
		}
		k, ok := tagEnd(s, ne)
		if !ok {
			i = max(i, k)
			continue
		}
		vs, ve, end, ok := xmlContent(s, k, &cdata)
		if !ok {
			i = max(i, end)
			continue
		}
		out = append(out, []int{p, end, vs, ve})
		i = end
	}
	return out
}

// tagEnd returns the index after the '>' that closes a start tag whose name
// ends at i. Attribute values may hold '>' when quoted; none may hold '<', so
// the scan stops there. A self-closing tag has no text and is not ok.
func tagEnd(s string, i int) (int, bool) {
	if i >= len(s) {
		return i, false
	}
	if s[i] == '>' {
		return i + 1, true
	}
	if !isXMLSpace(s[i]) {
		return i, false
	}
	q := byte(0)
	for k := i; k < len(s); k++ {
		c := s[k]
		switch {
		case c == '<':
			return k, false
		case q != 0:
			if c == q {
				q = 0
			}
		case c == '"' || c == '\'':
			q = c
		case c == '>':
			return k + 1, s[k-1] != '/'
		}
	}
	return len(s), false
}

func isXMLSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }

// skipXMLSpace skips blanks, line breaks and their JSON escapes (\n, \r, \t)
// from i.
func skipXMLSpace(s string, i int) int {
	for i < len(s) {
		switch {
		case isXMLSpace(s[i]):
			i++
		case s[i] == '\\' && i+1 < len(s) && strings.IndexByte("nrt", s[i+1]) >= 0:
			i += 2
		default:
			return i
		}
	}
	return i
}

// trimXMLSpace moves e back over what skipXMLSpace skips, not before vs.
func trimXMLSpace(s string, vs, e int) int {
	for e > vs {
		switch {
		case isXMLSpace(s[e-1]):
			e--
		case e-2 >= vs && s[e-2] == '\\' && strings.IndexByte("nrt", s[e-1]) >= 0:
			e -= 2
		default:
			return e
		}
	}
	return e
}

// cdataEnds finds "]]>" at or after a position that only grows, scanning s
// once in all.
type cdataEnds struct {
	s    string
	next int // last "]]>" found; -1 unknown, -2 none left
}

func (c *cdataEnds) from(i int) int {
	if c.next == -2 {
		return -1
	}
	if c.next < i {
		k := strings.Index(c.s[i:], "]]>")
		if k < 0 {
			c.next = -2
			return -1
		}
		c.next = i + k
	}
	return c.next
}

// xmlContent reads an element's text from k, after its start tag: padding,
// then a CDATA section, one child element or plain text, which must be
// followed by an end tag. It returns the value's bounds, where the match
// ends, and whether there is one; when there is none, end is where the
// search may go on.
func xmlContent(s string, k int, cdata *cdataEnds) (vs, ve, end int, ok bool) {
	pos := skipXMLSpace(s, k)
	// One child element: <password><value>X</value></password>.
	if pos+1 < len(s) && s[pos] == '<' && isLetter(s[pos+1]) {
		ce := pos + 1
		for ce < len(s) && isXMLNameChar(s[ce]) {
			ce++
		}
		k2, ok2 := tagEnd(s, ce)
		if !ok2 {
			return 0, 0, max(pos, k2), false
		}
		pos = skipXMLSpace(s, k2)
	}
	// "<[CDATA[" is not valid XML, but it is how the section often reaches us
	// after an exclamation mark was lost.
	for _, open := range []string{"<![CDATA[", "<[CDATA["} {
		if strings.HasPrefix(s[pos:], open) {
			vs = pos + len(open)
			e := cdata.from(vs)
			if e < 0 {
				return 0, 0, vs, false
			}
			return vs, e, e + 3, e > vs
		}
	}
	vs = pos
	e := strings.IndexByte(s[vs:], '<')
	if e < 0 {
		return 0, 0, len(s), false
	}
	e += vs
	if !strings.HasPrefix(s[e:], "</") && !strings.HasPrefix(s[e:], `<\/`) {
		return 0, 0, e, false
	}
	ve = trimXMLSpace(s, vs, e)
	return vs, ve, e, ve > vs
}
