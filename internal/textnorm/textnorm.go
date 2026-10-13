// Package textnorm turns pipeline error text (Kafka Connect, Debezium, Kafka
// clients, Postgres, Confluent Schema Registry, Java stack traces) into a
// Template with the volatile parts masked, and hashes that template into a
// short stable Fingerprint. Two occurrences of the same failure then get the
// same fingerprint even though their LSNs, offsets, hosts, ids and timestamps
// differ, so alerts and rooms about one failure can be grouped.
//
// Templates are meant to be logged and shown, so record values are masked too:
// Postgres "Key (col)=(value)" and "Failing row contains (...)" groups,
// Debezium "Struct{...}" and "{field=value}" bodies, e-mail addresses, and any
// quoted text that is not identifier-shaped or that follows ':' or '='.
//
// Cost is bounded in three ways, because the input is untrusted text:
//   - the input is cut to MaxInputBytes and walked line by line with IndexByte;
//   - each line is cut to 2*MaxMessageBytes before any pattern runs, and the
//     text that is masked in total is limited to a budget (the final cause is
//     always kept and is outside the budget);
//   - patterns are RE2 (linear, no backtracking); quotes and numbers use plain
//     byte scanners. The worst 64 KiB inputs in worst_test.go take under 5 ms (measured, see the PR).
//
// The package never panics on any input (see FuzzNormalize) and keeps no state.
package textnorm

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Limits. They bound work and the size of a Template.
const (
	// MaxInputBytes is how much of the input is read; the rest is ignored.
	MaxInputBytes = 64 << 10
	// DefaultFrames is how many stack frames Normalize keeps per cause.
	DefaultFrames = 5
	// MaxCauses is the longest cause chain kept: the first MaxCauses-1 causes
	// and always the final one, which is the root cause.
	MaxCauses = 32
	// MaxMessageBytes caps one masked message, so a huge details blob does
	// not matter and a cut-off tail does not change the fingerprint.
	MaxMessageBytes = 200
	// RootFrames is how many frames of the root cause count in the
	// fingerprint. Only the first: tools.Truncate may cut the rest, and the
	// cut partial line is dropped, so frame one is the last safe one.
	RootFrames = 1
	// FingerprintLen is the number of hex characters in a fingerprint.
	FingerprintLen = 16

	maxLineBytes   = 2 * MaxMessageBytes
	maxFrameBytes  = 160
	maxQuoteBytes  = 200
	budgetBytes    = 8 << 10
	truncatedLabel = "…[truncated]"
)

// Cause is one exception (or error line) of a chain.
type Cause struct {
	// Class is the exception class ("org.postgresql.util.PSQLException"), or ""
	// when the line is plain text.
	Class string `json:"class,omitempty"`
	// Message is the masked message template.
	Message string `json:"message,omitempty"`
	// Frames are the top stack frames, "pkg.Class.method", without file:line.
	Frames []string `json:"frames,omitempty"`
}

// Template is a normalised error text: the chain of causes, outermost first.
type Template struct {
	Causes []Cause `json:"causes"`
}

// Patterns are compiled once and never modified. None uses counted repetition
// with a large bound (that makes big RE2 programs).
var (
	reLogPrefix  = regexp.MustCompile(`^\[?\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?\]?\s*(?:(?:TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL)\b)?\s*`)
	reMoreFrames = regexp.MustCompile(`^\.\.\.\s*\d+\s+(?:more|common frames omitted)\b`)
	reClass      = regexp.MustCompile(`^([A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)*\.[A-Za-z_$][\w$]*)(?::\s*(.*))?$`)
	reModule     = regexp.MustCompile(`^[A-Za-z0-9_.@-]+//?`)
	// reLogfmt unwraps msg=/err=/error= values so their text is parsed as a
	// message. Other keys, including level=, stay in the text as they are.
	reLogfmt = regexp.MustCompile(`\b(?:msg|err|error)="((?:[^"\\]|\\.)*)"`)

	reEmail   = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+`)
	reTS      = regexp.MustCompile(`\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?|\b\d{2}:\d{2}:\d{2}(?:[.,]\d+)?\b`)
	reUUID    = regexp.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`)
	reMember  = regexp.MustCompile(`\b((?:consumer|producer|connector-consumer|connector-producer|adminclient|rdkafka)[A-Za-z0-9._-]*?)(?:-\d+)?-<uuid>`)
	reLSNWrap = regexp.MustCompile(`LSN\{[^}\n]*\}`)
	reLSN     = regexp.MustCompile(`\b[0-9A-F]{1,8}/[0-9A-F]{1,8}\b`)
	reIP      = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?\b`)
	reHost    = regexp.MustCompile(`\b[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.(?:local|internal|svc|com|net|io|org)\b(?::\d{1,5})?|\blocalhost(?::\d{1,5})?\b|\b[A-Za-z][A-Za-z0-9.-]*:\d{4,5}\b`)
	// A digit run of 16 or more is <hex> (it also matches this pattern), a
	// shorter one is <n>; both are volatile, the split only follows the pattern.
	reHex      = regexp.MustCompile(`\b0[xX][0-9a-fA-F]+\b|\b[0-9a-fA-F]{16,}\b`)
	hostNotKey = []string{"timeout", "offset", "interval", "size", "lag", "limit", "count", "bytes", "delay", "retries", "version", "port", "code", ".ms", "_ms"}
)

// Normalize parses trace and returns its template with DefaultFrames frames per
// cause.
func Normalize(trace string) Template { return NormalizeFrames(trace, DefaultFrames) }

type rawCause struct {
	line   string
	frames []string
}

// NormalizeFrames is Normalize with a chosen number of frames kept per cause.
func NormalizeFrames(trace string, frames int) Template {
	if frames < 0 {
		frames = 0
	}
	trace = prepare(trace)
	var head []*rawCause
	var tail *rawCause
	var cur *rawCause
	budget := budgetBytes
	for len(trace) > 0 {
		var line string
		if i := strings.IndexByte(trace, '\n'); i >= 0 {
			line, trace = trace[:i], trace[i+1:]
		} else {
			line, trace = trace, ""
		}
		line = strings.TrimLeft(line, " \t\r")
		line = strings.TrimRight(clip(line, maxLineBytes), " \t\r")
		if line == "" {
			continue
		}
		if m, ok := frameMethod(line); ok {
			// Frames before any cause line belong to an anonymous cause.
			if cur == nil {
				cur = &rawCause{}
				head = append(head, cur)
			}
			if len(cur.frames) < frames && (cur == tail || budget > 0) {
				cur.frames = append(cur.frames, m)
				if cur != tail {
					budget -= len(m)
				}
			}
			continue
		}
		if reMoreFrames.MatchString(line) {
			continue
		}
		if len(head) < MaxCauses-1 && budget > 0 {
			cur = &rawCause{line: line}
			head = append(head, cur)
			budget -= len(line)
		} else {
			// Keep only the newest cause past the cap: the root is the last.
			tail = &rawCause{line: line}
			cur = tail
		}
	}
	if tail != nil {
		head = append(head, tail)
	}
	t := Template{Causes: make([]Cause, 0, len(head))}
	for _, rc := range head {
		c := parseCause(rc.line)
		for _, f := range rc.frames {
			c.Frames = append(c.Frames, maskFrame(f))
		}
		t.Causes = append(t.Causes, c)
	}
	return t
}

// prepare applies the input cap and handles the tools.Truncate marker. A trace
// that ends with the marker was cut at an arbitrary byte, so the last line is
// partial (a cut before a frame's "(" would look like a bogus cause) and is
// dropped, unless the cut fell exactly on a line start or there is only one line.
func prepare(trace string) string {
	trimmed := strings.TrimRight(trace, " \t\r\n")
	truncated := strings.HasSuffix(trimmed, truncatedLabel)
	if truncated {
		trace = strings.TrimSuffix(trimmed, truncatedLabel)
		trace = strings.TrimSuffix(trace, "\n")
	}
	trace = clip(trace, MaxInputBytes)
	if truncated && !strings.HasSuffix(trace, "\n") {
		if i := strings.LastIndexByte(trace, '\n'); i >= 0 {
			trace = trace[:i+1]
		}
	}
	return trace
}

// frameMethod returns the method of a stack-frame line ("at pkg.Class.m(F.java:1)").
func frameMethod(line string) (string, bool) {
	if !strings.HasPrefix(line, "at ") {
		return "", false
	}
	rest := strings.TrimLeft(line[3:], " \t")
	n := 0
	for n < len(rest) && isFrameByte(rest[n]) {
		n++
	}
	if n == 0 || n >= len(rest) || rest[n] != '(' {
		return "", false
	}
	return clip(rest[:n], maxFrameBytes), true
}

func isFrameByte(c byte) bool {
	return isWord(c) || c == '$' || c == '.' || c == '<' || c == '>' || c == '/' || c == '@' || c == '-'
}

func isWord(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}

func parseCause(line string) Cause {
	if line == "" {
		return Cause{}
	}
	line = reLogPrefix.ReplaceAllString(line, "")
	line = strings.TrimPrefix(line, "Caused by:")
	line = strings.TrimPrefix(line, "Suppressed:")
	line = strings.TrimLeft(line, " \t")
	if strings.Contains(line, `="`) {
		line = reLogfmt.ReplaceAllString(line, "$1")
	}
	if m := reClass.FindStringSubmatch(line); m != nil && looksLikeClass(m[1]) {
		return Cause{Class: m[1], Message: maskMessage(m[2])}
	}
	return Cause{Message: maskMessage(line)}
}

// looksLikeClass accepts a dotted name whose last segment starts upper-case
// ("org.x.FooException"), which keeps "kafka.broker" style words out.
func looksLikeClass(s string) bool {
	last := s[strings.LastIndexByte(s, '.')+1:]
	r, _ := utf8.DecodeRuneInString(last)
	return unicode.IsUpper(r)
}

func maskMessage(s string) string {
	s = clip(s, maxLineBytes)
	s = maskBodies(s)
	if strings.Contains(s, "@") {
		s = reEmail.ReplaceAllString(s, "<email>")
	}
	s = maskQuotes(s)
	s = maskVolatile(s)
	return clip(strings.Join(strings.Fields(s), " "), MaxMessageBytes)
}

func maskFrame(method string) string {
	method = reModule.ReplaceAllString(method, "")
	return maskVolatile(method)
}

// maskQuotes replaces quoted literals that look like values. Identifier-shaped
// text ("orders", 'pg.public.orders-value') names a thing and is kept, unless it
// follows ':' or '=' where it is a value ("name":"bob"). A single quote only
// opens a literal after a non-word byte, so apostrophes are left alone.
func maskQuotes(s string) string {
	if !strings.ContainsAny(s, `"'`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		c := s[i]
		if c != '"' && c != '\'' || c == '\'' && i > 0 && isWord(s[i-1]) {
			b.WriteByte(c)
			i++
			continue
		}
		end := closingQuote(s, i)
		if end < 0 {
			b.WriteByte(c)
			i++
			continue
		}
		if isIdent(s[i+1:end]) && !afterAssign(s, i) {
			b.WriteString(s[i : end+1])
		} else {
			b.WriteByte(c)
			b.WriteString("<str>")
			b.WriteByte(c)
		}
		i = end + 1
	}
	return b.String()
}

// closingQuote finds the quote that closes the one at s[i], looking at most
// maxQuoteBytes ahead. Backslash escapes only count inside double quotes.
func closingQuote(s string, i int) int {
	q := s[i]
	for j := i + 1; j < len(s) && j-i <= maxQuoteBytes; j++ {
		switch {
		case s[j] == '\\' && q == '"':
			j++
		case s[j] == q:
			return j
		}
	}
	return -1
}

func isIdent(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		first := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_'
		if i == 0 && !first || !first && !(c >= '0' && c <= '9' || c == '.' || c == '$' || c == '-') {
			return false
		}
	}
	return true
}

// afterAssign reports whether the quote at s[i] follows ':' or '=' or the word
// "value", where the quoted text is a value, not a name.
func afterAssign(s string, i int) bool {
	j := i - 1
	for j >= 0 && s[j] == ' ' {
		j--
	}
	if j < 0 {
		return false
	}
	if s[j] == ':' || s[j] == '=' {
		return true
	}
	return j >= 4 && strings.EqualFold(s[j-4:j+1], "value")
}

// maskBodies masks the bodies that carry record values: Debezium
// "Struct{...}", "{field=value}" maps, Postgres "Failing row contains (...)" and
// the value group of "Key (col)=(value)". Each body is masked up to its matching
// closer, counting nesting, or to the end of the line when it has none (the line
// was cut); so nested bodies and cut bodies leak nothing.
func maskBodies(s string) string {
	if !strings.ContainsAny(s, "{(") {
		return s
	}
	const rowOpen = "Failing row contains "
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		switch {
		case strings.HasPrefix(s[i:], "Struct{"):
			i = balancedEnd(s, i+len("Struct"), '{', '}')
			b.WriteString("Struct{<fields>}")
		case s[i] == '{' && startsKeyValue(s, i+1):
			i = balancedEnd(s, i, '{', '}')
			b.WriteString("{<fields>}")
		case strings.HasPrefix(s[i:], rowOpen+"("):
			i = balancedEnd(s, i+len(rowOpen), '(', ')')
			b.WriteString(rowOpen + "(<row>)")
		case strings.HasPrefix(s[i:], ")=("):
			i = balancedEnd(s, i+2, '(', ')')
			b.WriteString(")=(<val>)")
		default:
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String()
}

// balancedEnd returns the index after the closer that matches the opener at
// s[start], or len(s) when there is none.
func balancedEnd(s string, start int, open, closer byte) int {
	depth := 0
	for j := start; j < len(s); j++ {
		switch s[j] {
		case open:
			depth++
		case closer:
			depth--
			if depth == 0 {
				return j + 1
			}
		}
	}
	return len(s)
}

// startsKeyValue reports whether s[i:] starts with "identifier=".
func startsKeyValue(s string, i int) bool {
	j := i
	for j < len(s) && (isWord(s[j]) || s[j] == '.' || s[j] == '-') {
		j++
	}
	return j > i && j < len(s) && s[j] == '='
}

// maskVolatile replaces timestamps, ids, hosts, LSNs and numbers. Most
// patterns are skipped by a cheap byte check when they cannot match.
func maskVolatile(s string) string {
	s = reTS.ReplaceAllString(s, "<ts>")
	if strings.IndexByte(s, '-') >= 0 {
		s = reUUID.ReplaceAllString(s, "<uuid>")
		if strings.Contains(s, "<uuid>") {
			s = reMember.ReplaceAllString(s, "$1-<member>")
		}
	}
	if strings.Contains(s, "LSN{") {
		s = reLSNWrap.ReplaceAllString(s, "<lsn>")
	}
	if strings.IndexByte(s, '.') >= 0 {
		s = reIP.ReplaceAllString(s, "<ip>")
	}
	if strings.ContainsAny(s, ".:") || strings.Contains(s, "localhost") {
		s = replaceIf(s, reHost, hostRepl)
	}
	if strings.IndexByte(s, '/') >= 0 {
		s = replaceIf(s, reLSN, lsnRepl)
	}
	s = reHex.ReplaceAllString(s, "<hex>")
	return maskNumbers(s)
}

// replaceIf replaces each match of re with fn(s, start, end); fn returns the
// match itself to keep it.
func replaceIf(s string, re *regexp.Regexp, fn func(s string, i, j int) string) string {
	idx := re.FindAllStringIndex(s, -1)
	if len(idx) == 0 {
		return s
	}
	var b strings.Builder
	last := 0
	for _, m := range idx {
		b.WriteString(s[last:m[0]])
		b.WriteString(fn(s, m[0], m[1]))
		last = m[1]
	}
	b.WriteString(s[last:])
	return b.String()
}

func hostRepl(s string, i, j int) string {
	m := s[i:j]
	if k := strings.LastIndexByte(m, ':'); k >= 0 && !strings.HasPrefix(m, "localhost") {
		// "word:12345" is a host only when word is not a setting name.
		word := strings.ToLower(m[:k])
		for _, bad := range hostNotKey {
			if strings.HasSuffix(word, bad) {
				return m
			}
		}
	}
	// "javax.net.ssl.X" is a class, not the host "javax.net".
	if j+1 < len(s) && s[j] == '.' && isWord(s[j+1]) {
		return m
	}
	return "<host>"
}

// lsnRepl accepts "0/5A3B2F8" shapes: upper-case hex, and either a hex letter or
// a right part of 5+ digits, so "Task 1/3" and "ab12/cd34" stay as they are. A
// preceding "lsn" accepts any hex pair.
func lsnRepl(s string, i, j int) string {
	m := s[i:j]
	k := strings.IndexByte(m, '/')
	right := m[k+1:]
	hasLetter := strings.ContainsAny(m, "ABCDEF")
	if len(right) >= 5 || hasLetter || strings.HasSuffix(strings.ToLower(strings.TrimRight(s[:i], " :=")), "lsn") {
		return "<lsn>"
	}
	return m
}

// maskNumbers replaces standalone digit runs with <n>, except evidence codes.
func maskNumbers(s string) string {
	var b strings.Builder
	last := 0
	for i := 0; i < len(s); {
		// '_' delimits a number ("orders_2026_10_13"); other word bytes do not.
		if s[i] < '0' || s[i] > '9' || i > 0 && isWord(s[i-1]) && s[i-1] != '_' {
			i++
			continue
		}
		j := i
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		if j < len(s) && isWord(s[j]) && s[j] != '_' || keepNumber(s[:i], s[i:j]) {
			i = j
			continue
		}
		b.WriteString(s[last:i])
		b.WriteString("<n>")
		last, i = j, j
	}
	if last == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

// keepNumber reports whether the digit run is an evidence code, which says
// what failed and must tell incidents apart. It is decided by position and
// shape, so a volatile number after the same word is still masked:
//
//	SQLSTATE            exactly 5 digits (23505, 40001); SQLSTATEs with a
//	                    letter (42P01, 22P02) are not standalone numbers, so
//	                    they are never masked and need no rule
//	error code, errorCode   3 to 5 digits (409, 40401)
//	HTTP, status, status code, response code (any spelling)
//	                    3 digits, 100 to 599 (503)
//	error               5 digits starting 4 or 5 (registry "error 40401")
func keepNumber(before, digits string) bool {
	if len(before) > 24 {
		before = before[len(before)-24:]
	}
	look := strings.ToLower(strings.TrimRight(before, " :=\"'[]("))
	n := len(digits)
	switch {
	case strings.HasSuffix(look, "sqlstate"):
		return n == 5
	case strings.HasSuffix(look, "error code"), strings.HasSuffix(look, "error_code"), strings.HasSuffix(look, "errorcode"):
		return n >= 3 && n <= 5
	case hasAnySuffix(look, statusKeys), httpVersionEnd(look):
		return n == 3 && digits[0] >= '1' && digits[0] <= '5'
	case strings.HasSuffix(look, "error"):
		return n == 5 && (digits[0] == '4' || digits[0] == '5')
	}
	return false
}

var statusKeys = []string{"status", "http", "status code", "statuscode", "status_code", "response code", "responsecode", "response_code"}

func hasAnySuffix(s string, suffixes []string) bool {
	for _, x := range suffixes {
		if strings.HasSuffix(s, x) {
			return true
		}
	}
	return false
}

// httpVersionEnd reports whether look ends with "http/1.1", "http/2" and the like.
func httpVersionEnd(look string) bool {
	i := strings.LastIndex(look, "http/")
	return i >= 0 && len(look)-i <= 10
}

// String is the canonical text of the template, one cause per block.
func (t Template) String() string { return t.canonical(false) }

// canonical renders the template. For the fingerprint (forHash) the root cause
// shows only RootFrames frames.
func (t Template) canonical(forHash bool) string {
	var b strings.Builder
	// The root is the last cause that names an exception class, so a trailing
	// plain log line does not take its place.
	root := len(t.Causes) - 1
	for i := root; i >= 0; i-- {
		if t.Causes[i].Class != "" {
			root = i
			break
		}
	}
	for i, c := range t.Causes {
		// Plain log lines after the root are left out of the hash: a trace and
		// the same trace with a trailing log line are one failure.
		if forHash && i > root && c.Class == "" {
			continue
		}
		if i > 0 {
			b.WriteString("\nCaused by: ")
		}
		b.WriteString(c.Class)
		if c.Class != "" && c.Message != "" {
			b.WriteString(": ")
		}
		b.WriteString(c.Message)
		frames := c.Frames
		if forHash && i == root && len(frames) > RootFrames {
			frames = frames[:RootFrames]
		}
		for _, f := range frames {
			b.WriteString("\n  at ")
			b.WriteString(f)
		}
	}
	return b.String()
}

// Fingerprint is a stable id of the template: the first FingerprintLen hex
// characters of the SHA-256 of its canonical text. All frames of the outer
// causes count, but only the first RootFrames frames of the root cause, because
// tools.Truncate can cut the rest.
func (t Template) Fingerprint() string {
	sum := sha256.Sum256([]byte(t.canonical(true)))
	return hex.EncodeToString(sum[:])[:FingerprintLen]
}

// Fingerprint normalises trace and returns the template fingerprint.
func Fingerprint(trace string) string { return Normalize(trace).Fingerprint() }

// clip returns at most n bytes of s without splitting a UTF-8 rune.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
