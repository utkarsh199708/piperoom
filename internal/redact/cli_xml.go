package redact

import (
	"regexp"
	"strings"
)

// The rules of #372 (the flag and XML element entries of baseRules) run
// first, unchanged. The rules below run after every other rule, on its
// output, and only add masks for the shapes #372 did not read:
// quoted values with spaces, flag names with '_' or '.', argument lists, XML
// attributes, CDATA and values on their own lines (#383). A value that is
// already masked is skipped, so nothing #372 masked can come back.

// addFlagRule masks a secret given as the argument of a command-line flag
// (--sasl.password "X Y", ["--password", "X"]) or as curl's
// -u/--user/-U/--proxy-user user:password, in the shapes the #372 rule does
// not read. It runs one line at a time.
var addFlagRule = rule{
	kind:   KindKeyValue,
	find:   flagFind,
	groups: []int{1},
	style:  styleSecret,
	pre:    containsAny("pass", "secret", "token", "-u"),
	remask: true,
}

// addXMLRule masks the text of an XML element whose name ends in a secret
// word, in the shapes the #372 rule does not read: attributes, CDATA,
// padding, the value on its own lines, or in child elements (<secret><key>k
// </key><value>X</value></secret>). The text counts only when the element's
// own end tag follows it. Like pemRule it runs over the whole text.
var addXMLRule = rule{
	kind:   KindKeyValue,
	find:   xmlFind,
	groups: []int{1},
	style:  styleSecret,
	pre:    func(s, _ string) bool { return strings.Contains(s, "</") || strings.Contains(s, `<\/`) },
	remask: true,
}

// extend runs the added rules over the output of every other rule.
func (r *Redactor) extend(s string, st *Stats) string {
	if s == "" {
		return s
	}
	lower := asciiLower(s)
	var b strings.Builder
	changed := false
	for start := 0; start <= len(s); {
		end := strings.IndexByte(s[start:], '\n')
		if end < 0 {
			end = len(s)
		} else {
			end += start
		}
		line := s[start:end]
		out := applyCounted(&addFlagRule, line, lower[start:end], st)
		if out != line && !changed {
			changed = true
			b.Grow(len(s) + 64)
			b.WriteString(s[:start])
		}
		if changed {
			b.WriteString(out)
			if end < len(s) {
				b.WriteByte('\n')
			}
		}
		start = end + 1
	}
	if changed {
		s = b.String()
	}
	for range maxPasses {
		out := applyCounted(&addXMLRule, s, "", st)
		if out == s {
			break
		}
		s = out
	}
	return s
}

// maskOnly matches a value that is one mask and nothing else.
var maskOnly = regexp.MustCompile(`^(?:[^\s…]{1,4}…)?\[redacted[^\]]*\](?:…[^\s…]{1,2})?$`)

// heldMask reports whether a value already holds a mask. A value that is a
// whole quoted argument or element text may be masked again as one when the
// mask covers only part of it; any other value is left to the rule that
// masked it.
func heldMask(v string, whole bool) (skip bool) {
	if !strings.Contains(v, marker) {
		return false
	}
	return !whole || maskOnly.MatchString(strings.TrimSpace(v))
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

// flagBefore reports whether a flag may start at p: at the start of the
// line, after a blank (CR and form feed too: progress-bar output), an
// opening quote, bracket, brace or backtick, a shell
// ; | & or <, '=' of an environment variable holding options, or a
// JSON-escaped line break or tab ({"cmd":"run\n--password X"}).
func flagBefore(s string, p int) bool {
	if p == 0 || strings.IndexByte(" \t\r\f\"'[({,=`;|&<", s[p-1]) >= 0 {
		return true
	}
	return p >= 2 && s[p-2] == '\\' && strings.IndexByte("ntr", s[p-1]) >= 0
}

// flagScan remembers what flagFind has measured on one line, so the next
// flag's value scan reuses it. Without it, every rejected flag on a line
// with no blanks (-u=x=-u=x=...) rescans the rest of the line.
type flagScan struct {
	s string
	// [runFrom, runEnd) has no blank or quote; runEnd is one or the end.
	runFrom, runEnd int
	// The first ':' in [colonFrom, colonTo) is at colonAt, -1 if none.
	colonFrom, colonTo, colonAt int
	// s[:tickTo] holds ticks backticks.
	tickTo, ticks int
}

// openTick reports whether a backtick opened before p is still open.
func (f *flagScan) openTick(p int) bool {
	if p < f.tickTo {
		f.tickTo, f.ticks = 0, 0
	}
	f.ticks += strings.Count(f.s[f.tickTo:p], "`")
	f.tickTo = p
	return f.ticks%2 == 1
}

// bareEnd returns the end of the bare word that starts at or covers v.
func (f *flagScan) bareEnd(v int) int {
	if v >= f.runFrom && v < f.runEnd {
		return f.runEnd
	}
	e := v
	for e < len(f.s) && !isBlankOrQuote(f.s[e]) {
		e++
	}
	f.runFrom, f.runEnd = v, e
	return e
}

// colon returns the first ':' in s[v:to], or -1.
func (f *flagScan) colon(v, to int) int {
	if v >= f.colonFrom && to == f.colonTo && (f.colonAt < 0 || f.colonAt >= v) {
		return f.colonAt
	}
	c := strings.IndexByte(f.s[v:to], ':')
	if c >= 0 {
		c += v
	}
	f.colonFrom, f.colonTo, f.colonAt = v, to, c
	return c
}

// flagFind finds secret flag arguments in one line. Each step moves forward
// past what it read, and value scans that may pass the next flag are cached
// in flagScan, so a hostile line costs time linear in its length.
func flagFind(s, _ string) [][]int {
	var out [][]int
	f := &flagScan{s: s, colonAt: -1}
	for i := 0; i < len(s); {
		j := strings.IndexByte(s[i:], '-')
		if j < 0 {
			break
		}
		p := i + j
		if !flagBefore(s, p) {
			i = p + 1
			continue
		}
		ns := p + 1
		for ns < len(s) && s[ns] == '-' {
			ns++
		}
		ne := ns
		for ne < len(s) && isKeyChar(s[ne]) {
			ne++
		}
		i = max(ne, p+1)
		if ne == ns || !(isLetter(s[ns]) || s[ns] >= '0' && s[ns] <= '9') {
			continue
		}
		m, skip := f.match(p, ns, ne)
		if m != nil {
			out = append(out, m)
			i = m[1]
		} else {
			// A rejected quoted argument is skipped whole: it may run over
			// several quotes, and reading it again for each flag inside it
			// would cost time quadratic in the line.
			i = max(i, skip)
		}
	}
	return out
}

// match reads the argument of the flag s[ns:ne] (dashes from p) and returns
// its match, or nil when it holds no secret, with where the search may go on.
func (f *flagScan) match(p, ns, ne int) ([]int, int) {
	m, end, quoted := f.matchArg(p, ns, ne)
	if m == nil && quoted {
		return nil, end
	}
	return m, ne
}

// matchArg is match; it also reports where the argument it read ends and
// whether it was quoted.
func (f *flagScan) matchArg(p, ns, ne int) ([]int, int, bool) {
	s := f.s
	name := s[ns:ne]
	lname := asciiLower(name)
	if ns-p >= 2 && (lname == "user" || lname == "proxy-user") || ns-p == 1 && lname == "u" {
		return f.user(p, ne), 0, false
	}
	if strings.HasPrefix(lname, "no-") || strings.HasPrefix(lname, "no_") {
		return nil, 0, false
	}
	// "with --password. Still failing": a flag named at the end of a
	// sentence takes no argument.
	if strings.TrimRight(lname, "._-") != lname || !endsInFlagWord(lname) {
		return nil, 0, false
	}
	v, listForm := flagSep(s, p, ne)
	if v < 0 {
		return nil, 0, false
	}
	vs, ve, end, quoted := f.value(p, v)
	if ve <= vs || (listForm && !quoted) {
		return nil, end, quoted
	}
	val := s[vs:ve]
	if heldMask(val, quoted) {
		return nil, end, quoted
	}
	// In an argument list ["psql", "--password", "-h"] each item is quoted,
	// so the next item is read like a bare word.
	bare := !quoted || listForm
	// Usage text: [--token TOKEN], (--password PASSWD), -W|--password.
	usage := !quoted && p > 0 && strings.IndexByte("[(|", s[p-1]) >= 0
	switch {
	case isReference(val, s[v] == '\''):
		// --password $PGPASSWORD names where the secret is, not the secret.
		return nil, end, quoted
	case listForm && (val[0] == '-' || lowerIdent(val) || strings.HasPrefix(val, "<")):
		// The next flag, or a name: --password took no argument.
		return nil, end, quoted
	case bare && plainWord(val):
		// "--password requires an argument": prose, not a password.
		return nil, end, quoted
	case bare && val == strings.ToUpper(strings.ReplaceAll(name, "-", "_")):
		// usage: tool [--password PASSWORD]: argparse's metavar.
		return nil, end, quoted
	case usage && (capsMetavar(val) || lowerIdent(val)):
		return nil, end, quoted
	case !quoted && s[ne] == '=' && secretKey(name) == keyWeak:
		// --token=v2 is the key/value pair token=v2, which that rule has
		// already masked if its value looks generated.
		return nil, end, quoted
	}
	return []int{p, end, vs, ve}, end, quoted
}

// lowerIdent reports a lower-case identifier (orders_v2, orders-db).
func lowerIdent(v string) bool {
	if v == "" || v[0] < 'a' || v[0] > 'z' {
		return false
	}
	for i := 0; i < len(v); i++ {
		if c := v[i]; !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// capsMetavar reports an upper-case usage placeholder (TOKEN, SASL_PASSWORD).
func capsMetavar(v string) bool {
	if v == "" || v[0] < 'A' || v[0] > 'Z' {
		return false
	}
	for i := 0; i < len(v); i++ {
		if c := v[i]; !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

// flagSep returns where the argument after a flag (dashes at p, name ending
// at i) starts: after '=', after blanks, or after the closing quote and comma
// of an argument list ("--password", "x"), where the flag is a whole quoted
// item and the value must be quoted too. It returns -1 when no argument
// follows.
func flagSep(s string, p, i int) (int, bool) {
	if i >= len(s) {
		return -1, false
	}
	switch s[i] {
	case '=':
		return i + 1, false
	case ' ', '\t':
		return skipBlanks(s, i), false
	case '"', '\'':
		if p == 0 || s[p-1] != s[i] {
			return -1, false
		}
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

// value reads one shell argument at v of the flag at p: \"...\" (inside a
// JSON string or a Go %q error), "..." with backslash escapes, '...', or a
// bare word that does not start with '-' (that is the next flag). A quote
// left open runs to the end of the line (a truncated summary). It returns the
// value's bounds, the end of the argument, and whether it was quoted.
func (f *flagScan) value(p, v int) (vs, ve, end int, quoted bool) {
	s := f.s
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
				// "Zx9\"hunter x": the #372 rule masks up to the escaped
				// quote and takes its backslash, so a quote glued to more of
				// the word, with another quote later on the line, does not
				// end the argument. The rule reads the text alone, not
				// whether it holds a mask, so a second pass reads the same
				// argument and Text stays idempotent.
				if k+1 < eol && isAlnum(s[k+1]) && strings.IndexByte(s[k+1:eol], '"') >= 0 {
					continue
				}
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
	ve = f.closerEnd(p, v, min(f.bareEnd(v), eol))
	return v, ve, ve, false
}

// closerEnd moves a bare word's end e back over the ) ] } or > that closes
// the bracket just before the flag at p, (--password X), or over a backtick
// that closes one still open before it, `psql --password X`. Any other
// trailing closer is part of the word: psql --password Zx9)).
func (f *flagScan) closerEnd(p, v, e int) int {
	s := f.s
	if e <= v {
		return e
	}
	switch c := s[e-1]; {
	case c == ')' && p > 0 && s[p-1] == '(',
		c == ']' && p > 0 && s[p-1] == '[',
		c == '}' && p > 0 && s[p-1] == '{',
		c == '>' && p > 0 && s[p-1] == '<',
		c == '`' && f.openTick(p):
		e--
	}
	return e
}

func isAlnum(c byte) bool { return isLetter(c) || c >= '0' && c <= '9' }

func isBlankOrQuote(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '"' || c == '\''
}

// user reads curl's user:password after -u/--user/-U/--proxy-user (name
// ending at i), quoted or bare. docker's -u user:group and uid:gid look the
// same, so a password equal to its user, digits after a numeric user, a
// plain word or a reference is kept.
func (f *flagScan) user(p, i int) []int {
	s := f.s
	v, _ := flagSep(s, p, i)
	if v < 0 || v >= len(s) {
		return nil
	}
	q := byte(0)
	if s[v] == '"' || s[v] == '\'' {
		q = s[v]
		v++
	}
	if v >= len(s) || isBlankOrQuote(s[v]) {
		return nil
	}
	end := f.bareEnd(v)
	k := f.colon(v, end)
	if k <= v {
		return nil
	}
	vs, ve := k+1, f.closerEnd(p, k+1, end)
	if q != 0 {
		ve = vs
		for ve < len(s) && s[ve] != q && s[ve] != '\r' && s[ve] != '\n' {
			ve++
		}
	}
	val, user := s[vs:ve], s[v:k]
	if val == "" || heldMask(val, q != 0) || val == user || allDigits(val) && allDigits(user) ||
		plainWord(val) || isReference(val, q == '\'') {
		return nil
	}
	return []int{p, ve, vs, ve}
}

// maxPlainWord bounds plainWord: requires, argument and deprecated fit, a
// passphrase such as correcthorsebatterystaple does not.
const maxPlainWord = 12

// plainWord reports a value of at most maxPlainWord lower-case ASCII
// letters, allowing trailing punctuation ("requires", "instead."): English,
// not a password. "letmein!" is not plain.
func plainWord(v string) bool {
	v = strings.TrimRight(v, ".,;:)")
	if v == "" || len(v) > maxPlainWord {
		return false
	}
	for i := 0; i < len(v); i++ {
		if v[i] < 'a' || v[i] > 'z' {
			return false
		}
	}
	return true
}

// isReference reports a value that names where a secret is rather than
// holding it: an environment variable ($PGPASSWORD, ${API_TOKEN}; not inside
// single quotes, where the shell expands nothing), a command substitution
// ($(cat /run/secrets/pw), `...`) or a template ({{ .Values.token }}).
func isReference(v string, singleQuoted bool) bool {
	switch {
	case strings.HasPrefix(v, "{{"), strings.HasPrefix(v, "$("), strings.HasPrefix(v, "`"):
		return true
	case singleQuoted || len(v) < 2 || v[0] != '$':
		return false
	}
	v = v[1:]
	if strings.HasPrefix(v, "{") && strings.HasSuffix(v, "}") {
		v = v[1 : len(v)-1]
	}
	if v == "" || !(v[0] >= 'A' && v[0] <= 'Z' || v[0] == '_') {
		return false
	}
	for i := 0; i < len(v); i++ {
		if c := v[i]; !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
			return false
		}
	}
	return true
}

func isXMLNameChar(c byte) bool { return isKeyChar(c) || c == ':' }

// localName drops an XML namespace prefix (ns:password → password).
func localName(n string) string {
	if i := strings.LastIndexByte(n, ':'); i >= 0 {
		return n[i+1:]
	}
	return n
}

// xmlName returns the end of the element name starting at i.
func xmlName(s string, i int) int {
	for i < len(s) && isXMLNameChar(s[i]) {
		i++
	}
	return i
}

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
		ne := xmlName(s, ns)
		i = max(ne, p+1)
		if ne == ns || !(isLetter(s[ns]) || s[ns] == '_') {
			continue
		}
		name := s[ns:ne]
		if !endsInFlagWord(asciiLower(name)) {
			continue
		}
		k, ok := tagEnd(s, ne)
		if !ok {
			i = max(i, k)
			continue
		}
		ms, end := xmlContent(s, k, name, &cdata)
		out = append(out, ms...)
		i = max(i, end)
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

// endTagAt returns the index after </name> (or <\/name>, the JSON escape) at
// i, comparing names without namespace prefix or case, or -1.
func endTagAt(s string, i int, name string) int {
	switch {
	case strings.HasPrefix(s[i:], "</"):
		i += 2
	case strings.HasPrefix(s[i:], `<\/`):
		i += 3
	default:
		return -1
	}
	e := xmlName(s, i)
	if !strings.EqualFold(localName(s[i:e]), localName(name)) {
		return -1
	}
	e = skipXMLSpace(s, e)
	if e >= len(s) || s[e] != '>' {
		return -1
	}
	return e + 1
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

// maxXMLChildren bounds how many child elements of one secret element are
// read.
const maxXMLChildren = 16

// oneLine reports text with no raw line break. Element text that spans
// lines is prose between two tags, not a value: "replace <token> ...
// </token>". JSON-escaped breaks (\n) do not count: that text is one line.
func oneLine(t string) bool {
	return !strings.ContainsAny(t, "\r\n")
}

// padProse reports padded element text that reads as prose or a
// placeholder: "between <password> and </password>", "<password> TODO
// </password>". A random value stays a value however it is padded.
func padProse(t string) bool {
	if capsMetavar(t) {
		// TODO, CHANGE_ME; JBSWY3DPEHPK3PXP is a secret.
		return !looksRandom(t, 12)
	}
	for _, w := range strings.Split(t, " ") {
		if !plainWord(w) {
			return false
		}
	}
	return true
}

// padValue reports padded text that padProse took for prose but that is a
// value all the same (#510): text on its own line or padded with tabs
// ("<password>\n  debezium\n</password>"; prose that spans lines fails
// oneLine), capitals between blanks (HUNTER2), or a single plain word
// between blanks (<password> debezium </password>). A placeholder (TODO,
// CHANGE_ME) or a function word (and, the) stays prose. It only narrows
// padProse, so the text it lets through is masked on top of the #383 rule.
func padValue(pad, t string) bool {
	if placeholderText(t) || functionWords[asciiLower(t)] {
		return false
	}
	return strings.ContainsAny(pad, "\r\n\t") || strings.Contains(pad, `\n`) || strings.Contains(pad, `\r`) ||
		strings.Contains(pad, `\t`) || capsMetavar(t) || !strings.ContainsAny(t, " \t") && plainWord(t)
}

// placeholderWords make up element text that stands in for a secret.
var placeholderWords = map[string]bool{
	"todo": true, "tbd": true, "fixme": true, "change": true, "changeme": true, "me": true, "replace": true,
	"replaceme": true, "placeholder": true, "password": true, "passwd": true, "pwd": true, "secret": true,
	"token": true, "redacted": true, "hidden": true, "masked": true, "your": true, "here": true, "setme": true,
	"set": true, "unset": true, "notset": true, "none": true, "null": true, "nil": true, "empty": true,
	"example": true, "insert": true, "enter": true, "value": true,
}

// functionWords are English words that read as prose between two tags.
var functionWords = map[string]bool{
	"a": true, "an": true, "and": true, "or": true, "the": true, "to": true, "of": true, "in": true, "on": true,
	"at": true, "by": true, "is": true, "it": true, "be": true, "see": true, "not": true, "no": true, "with": true,
	"for": true, "from": true, "as": true,
}

// placeholderText reports text that names a missing secret: made only of
// placeholder words joined by '_', '-', '.' or blanks (TODO, CHANGE_ME,
// YOUR_PASSWORD, changeme), maybe in <...>, ${...} or %...%, or a run of x
// or '*'. A real value that only contains such a word (changeit,
// supersecret, MYSECRET1) is not one.
func placeholderText(t string) bool {
	l := asciiLower(strings.TrimSpace(t))
	for _, w := range [][2]string{{"<", ">"}, {"${", "}"}, {"%", "%"}} {
		if len(l) > len(w[0])+len(w[1]) && strings.HasPrefix(l, w[0]) && strings.HasSuffix(l, w[1]) {
			l = l[len(w[0]) : len(l)-len(w[1])]
			break
		}
	}
	words := strings.FieldsFunc(l, func(r rune) bool { return r == '_' || r == '-' || r == '.' || r == ' ' || r == '\t' })
	if len(words) == 0 {
		return false
	}
	for _, w := range words {
		if !placeholderWords[w] && !(len(w) >= 3 && strings.Trim(w, "x") == "") && strings.Trim(w, "*") != "" {
			return false
		}
	}
	return true
}

// xmlContent reads the text of element name from k, after its start tag:
// padding, then a CDATA section, plain text on one line, or child elements,
// each followed by its own end tag, and then </name>. It returns the values
// to mask (none when the shape does not hold) and where the search may go
// on.
func xmlContent(s string, k int, name string, cdata *cdataEnds) ([][]int, int) {
	pos := skipXMLSpace(s, k)
	// "<[CDATA[" is not valid XML, but it is how the section often reaches us
	// after an exclamation mark was lost.
	for _, open := range []string{"<![CDATA[", "<[CDATA["} {
		if strings.HasPrefix(s[pos:], open) {
			vs := pos + len(open)
			e := cdata.from(vs)
			// The section counts only when the element ends right after it;
			// an open one would otherwise run to any later "]]>".
			if e <= vs {
				return nil, vs
			}
			end := endTagAt(s, skipXMLSpace(s, e+3), name)
			if end < 0 {
				return nil, vs
			}
			if heldMask(s[vs:e], true) {
				return nil, end
			}
			return [][]int{{vs, e, vs, e}}, end
		}
	}
	if pos+1 < len(s) && s[pos] == '<' && isLetter(s[pos+1]) {
		return xmlChildren(s, pos, name)
	}
	vs := pos
	e := strings.IndexByte(s[vs:], '<')
	if e < 0 {
		return nil, len(s)
	}
	e += vs
	end := endTagAt(s, e, name)
	if end < 0 {
		return nil, e
	}
	ve := trimXMLSpace(s, vs, e)
	if ve <= vs || !oneLine(s[vs:ve]) || heldMask(s[vs:ve], true) ||
		(vs > k || ve < e) && padProse(s[vs:ve]) && !padValue(s[k:vs]+s[ve:e], s[vs:ve]) {
		return nil, end
	}
	return [][]int{{vs, ve, vs, ve}}, end
}

// xmlChildren reads child elements of element name from start, each with
// one-line text only, then </name>. The text of a child named value, or one
// whose own name ends in a secret word, is a value; <key>, <type> or
// <expires_in> are not. When the shape does not hold, it returns start, so
// xmlFind reads the children one by one and a secret child is still masked
// on its own; each child is then read at most twice.
func xmlChildren(s string, start int, name string) ([][]int, int) {
	var ms [][]int
	pos := start
	for range maxXMLChildren {
		cn := pos + 1
		ce := xmlName(s, cn)
		k, ok := tagEnd(s, ce)
		if ce == cn || !ok {
			return nil, start
		}
		vs := skipXMLSpace(s, k)
		e := strings.IndexByte(s[vs:], '<')
		if e < 0 {
			return nil, start
		}
		e += vs
		next := endTagAt(s, e, s[cn:ce])
		if next < 0 {
			return nil, start
		}
		child := asciiLower(localName(s[cn:ce]))
		if ve := trimXMLSpace(s, vs, e); ve > vs && oneLine(s[vs:ve]) && !heldMask(s[vs:ve], true) &&
			(child == "value" || endsInFlagWord(child)) {
			ms = append(ms, []int{vs, ve, vs, ve})
		}
		pos = skipXMLSpace(s, next)
		if end := endTagAt(s, pos, name); end >= 0 {
			return ms, end
		}
		if !(pos+1 < len(s) && s[pos] == '<' && isLetter(s[pos+1])) {
			return nil, start
		}
	}
	return nil, start
}
