// Package redact masks secrets in untrusted text (tool summaries, error
// traces, record values) before it reaches a model prompt, the room log, Slack
// or an export.
//
// It is tuned to keep incident evidence intact: LSNs, offsets, topic,
// connector and slot names, SQLSTATEs, exception classes, schema ids and
// timestamps are never touched. A mask keeps the key name, a length class and,
// for long tokens, the first and last characters, so an engineer can still
// tell two credentials apart without seeing either one:
//
//	sk-a…[redacted 48]…9Q
//	postgres://app:[redacted 16]@db:5432/orders
//
// Passwords and short values keep no characters at all.
//
// Values that run past a line break are followed onto the next lines. A
// quoted value continued with a backslash runs to its closing quote; one
// without runs to its closing quote when that quote ends the value (end of
// line, blank, or , ; } ] )) within four lines. A bare value runs into the
// next line's first token when the previous line ended in a backslash, or
// when that token reads like a credential's tail and is not followed by
// more words. A value ending in "…" was truncated and is not followed. A URL
// password may hold a raw '@' or '/', and a URL user one '@' (user@server);
// a key/value pair may be URL-encoded once or twice (password%3D...,
// %22password%22%3A%22...%22, password+%3D+...); quotes may carry up to three
// backslashes (two JSON levels). In Kafka Connect's REST form
// {"name":"database.password","value":"..."} the sibling name is the key.
// An Oracle JDBC password (jdbc:oracle:thin:user/password@host, the user or
// password maybe quoted) runs to the last '@' of its token and is masked
// after every other rule, so it cannot eat a sibling credential's key.
//
// Known gaps (most also leak without the rules above):
//   - an unquoted password with spaces is masked only up to its first
//     space ("password=a b" reads the same as "password=a retries=3"); a
//     bare password holding ; , ) or a backslash ends there too;
//   - a wrapped tail stays when it is indented, a plain word, a number,
//     hex digits and dashes, starts with - / . @ # or a bracket, holds ':'
//     or '@', is followed by more words on its line, or is one byte long; an
//     unquoted value is followed for one line only (backslash continuations
//     chain); a weak-key token (token=, session=) under 12 characters split
//     across lines is not seen as random;
//   - a value that starts on the next line (password=\n..., YAML
//     "password:\n  ..." or ">-") is not followed;
//   - an unclosed quoted value is followed onto the next line only when its
//     own line holds an odd number of that quote, and not past a lone CR;
//   - quotes with more than three backslashes, and \\" or \\\' forms;
//   - an encoded '&' (%26) inside a URL-encoded password ends it there;
//   - a URL password holding '@' then '?' or '#', or an all-digit segment
//     before a '/' (postgres://app:2024/Spring7x@db/..., read as a port);
//     a URL cut before its "@host" is masked to its first blank or quote,
//     except a password of one to five digits alone or before / ; , ? # :
//     . ) ] } & | or a backslash (postgres://app:1234;x, read as a port);
//   - key names longer than 64 characters
//     (io.confluent.connect.jdbc.sink.JdbcSinkConnectorConfig.conn.password=);
//   - a Bearer token split across a line; .pgpass lines
//     (host:5432:db:user:secret); a CLI password glued to its flag
//     (mysql -pSecret);
//   - command-line flags and XML elements (#372 masks the common forms;
//     rules added for #383 run after it and only mask more): a flag after
//     ':' or a non-ASCII blank, inside element text, quoted together with
//     its value ('--password' 'X'), in a JSON-escaped or %q argument list
//     or an unquoted flow list, or with its value on the next line or after
//     a CR or --; a whole quoted "--password=X Y" (masked to its first
//     blank); names ending in pass (-storepass, --pass) and docker login
//     -p; -u with a quoted part ("svc user:X"), an empty user or glued to
//     the flag; shell-escaped quotes; a \\\" opener; an XML secret in an
//     attribute, entity- or \u003c-escaped, more than one child deep, in
//     CDATA or text around a child, in a Hadoop <name>/<value> pair or a
//     Maven <arg> list, in text that spans lines, after a start tag holding
//     "/>" or "<x>", or closed by an escaped end tag of another element;
//   - a generated lower-case token after a flag in an argument list or
//     usage brackets (["--token", "d41d8cd98f00b204e9800998ecf8427e"]) reads
//     as a name and is kept;
//   - an Oracle JDBC password with an unquoted blank, or an unclosed quote,
//     is masked only up to the blank (jdbc:oracle:thin:u/a b@host); a
//     misspelt scheme (jdbc:oralce:); an MS SQL Server braced value split
//     across lines (password={a\n;b});
//   - an environment assignment with ANSI-C quoting
//     (export PGPASSWORD=$'a b'); passes added for #510, #511 and #534 run
//     after every other rule and only add masks (more.go): XML text on its
//     own line, secrets named by key= or name= attributes, shell-quoted
//     flag values ($'...', a\ b, glued quote runs, typographic quotes),
//     Windows "set NAME=a b" and braced values (password={a;b});
//   - over-masking, not leaks: a URL whose path holds "@host" may lose the
//     host to the mask, a scheme://word:text token with no '@' and no
//     numeric port is masked after the ':' (http://host:${PORT}/x), and a wrapped tail such as orders_v2 after a
//     password line is masked although it is evidence; the word after a
//     #372 flag is masked whatever it is (--password requires an argument,
//     --no-password orders, -u postgres:postgres; #490), and so are a $VAR,
//     %VAR%, $(...) or db name there, a user:gid pair, and a closing
//     backtick or tag after a bare value; the shapes read for #383 mask the
//     same way (--db_password orders_v2, --sasl.password Required, a quoted
//     "two words"); text glued after an Oracle JDBC URL up to a later '@'
//     ({"url":"jdbc:oracle:thin:u/p@h:1521:S","owner":"ops@corp"}) is masked
//     with the password.
package redact

import (
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Kind names one class of secret, as counted in Stats.
type Kind string

// Kinds reported in Stats.
const (
	KindPrivateKey Kind = "private_key"
	KindURLPass    Kind = "url_password"
	KindAuthHeader Kind = "auth_header"
	KindBearer     Kind = "bearer_token"
	KindJWT        Kind = "jwt"
	KindSnowflake  Kind = "snowflake_token"
	KindAWSKeyID   Kind = "aws_access_key_id"
	KindAWSSecret  Kind = "aws_secret_key"
	KindGCP        Kind = "gcp_key"
	KindAzure      Kind = "azure_key"
	KindAPIKey     Kind = "api_key"
	KindKeyValue   Kind = "secret_value"
	KindEmail      Kind = "email"
	KindIP         Kind = "ip"
	KindSlackHook  Kind = "slack_webhook"
	// KindOverflow counts text past MaxScan, masked whole.
	KindOverflow Kind = "overflow"
)

// Stats reports what one call masked.
type Stats struct {
	// Total is the number of values masked.
	Total int
	// ByKind counts masks per Kind; nil when nothing was masked.
	ByKind map[Kind]int
}

func (s *Stats) add(k Kind) {
	if s.ByKind == nil {
		s.ByKind = map[Kind]int{}
	}
	s.ByKind[k]++
	s.Total++
}

func (s *Stats) merge(o Stats) {
	for k, n := range o.ByKind {
		if s.ByKind == nil {
			s.ByKind = map[Kind]int{}
		}
		s.ByKind[k] += n
	}
	s.Total += o.Total
}

// Options selects the optional detectors.
type Options struct {
	// Emails masks the local part of e-mail addresses (domain kept).
	Emails bool
	// IPs masks IPv4 and IPv6 addresses. Off by default: worker and broker
	// addresses are evidence far more often than they are sensitive.
	IPs bool
}

// DefaultOptions masks e-mails and keeps IP addresses.
func DefaultOptions() Options { return Options{Emails: true} }

// Redactor masks secrets. It is immutable and safe for concurrent use.
type Redactor struct {
	rules []rule
}

// New builds a Redactor with the given optional detectors.
func New(o Options) *Redactor {
	rs := append([]rule(nil), baseRules...)
	if o.Emails {
		rs = append(rs, emailRule)
	}
	if o.IPs {
		rs = append(rs, ipv4Rule, ipv6Rule)
	}
	return &Redactor{rules: rs}
}

// def is the Redactor behind the package-level functions. It is never
// modified after init.
var def = New(DefaultOptions())

// Text masks secrets in s with the default options.
func Text(s string) (string, Stats) { return def.Text(s) }

// JSON masks secrets in the string values of v with the default options.
func JSON(v any) any { return def.JSON(v) }

// JSONStats is JSON that also reports what it masked.
func JSONStats(v any) (any, Stats) { return def.JSONStats(v) }

// MaxScan is the most text Text scans in one string. Tool summaries are far
// smaller; the cap bounds the cost of uncapped tool data strings and error
// texts. Anything past it is masked whole, never passed through unscanned.
const MaxScan = 64 << 10

// overflowSuffix ends the mask that replaces text past MaxScan.
const overflowSuffix = " bytes past the scan limit]"

// maxPasses bounds the fixed-point loops in Text. One pass is enough for
// every input seen so far; the loop only guards idempotency against a mask
// that happens to complete a new match with its neighbours.
const maxPasses = 3

// Text masks secrets in s. Masking is idempotent: Text of its own output
// returns that output unchanged.
func (r *Redactor) Text(s string) (string, Stats) {
	var st Stats
	if s == "" {
		return s, st
	}
	// Text past the scan limit is replaced by one mask. Output that already
	// ends in that mask is rescanned up to it and the mask kept, so a second
	// call returns the first call's output. Masks can grow short values up
	// to about 3x, so such a head is at most 4*MaxScan; anything longer is
	// treated as fresh input and capped again.
	if i := strings.LastIndex(s, "…"+marker+" "); i >= 0 && i <= 4*MaxScan && strings.HasSuffix(s, overflowSuffix) &&
		allDigits(s[i+len("…"+marker+" "):len(s)-len(overflowSuffix)]) {
		head, hst := r.scan(s[:i])
		hst.merge(st)
		return head + s[i:], hst
	}
	if len(s) > MaxScan {
		cut := scanCut(s)
		head, hst := r.scan(s[:cut])
		hst.add(KindOverflow)
		return head + "…" + marker + " " + strconv.Itoa(len(s)-cut) + overflowSuffix, hst
	}
	return r.scan(s)
}

// scanCut picks where to stop scanning a string longer than MaxScan. The cut
// must not split a secret, or its first half would pass through in the head:
// it goes after the last line break in the final 4 KiB before the limit, else
// after the last whitespace or delimiter there, so the partial token at the
// limit lands in the overflow mask. Only a single token longer than 4 KiB is
// cut inside, at a rune boundary. A secret XML element left open at the cut
// moves it back before the element (openTagCut).
func scanCut(s string) int {
	return openTagCut(s, lineCut(s))
}

// lineCut is scanCut without the XML check.
func lineCut(s string) int {
	win := s[MaxScan-4096 : MaxScan]
	if i := strings.LastIndexByte(win, '\n'); i >= 0 {
		return MaxScan - 4096 + i + 1
	}
	if i := strings.LastIndexAny(win, " \t\r,;\"'<>()[]{}|"); i >= 0 {
		return MaxScan - 4096 + i + 1
	}
	cut := MaxScan - 4096
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return cut
}

// scan masks s without the size cap.
func (r *Redactor) scan(s string) (string, Stats) {
	var st Stats
	if s == "" {
		return s, st
	}
	orig := s
	for range maxPasses {
		out := applyCounted(&pemRule, s, "", &st)
		if out == s {
			break
		}
		s = out
	}

	// Every other secret sits on one line. Running the regexps per line, and
	// only on lines whose prefilter hits, keeps them fast: most lines of a
	// tool summary carry no candidate at all, and RE2 on a short input uses
	// its backtracker instead of the much slower NFA over the whole text.
	lower := asciiLower(s)
	var b strings.Builder
	changed := false
	var open openValue
	for start := 0; start <= len(s); {
		end := strings.IndexByte(s[start:], '\n')
		if end < 0 {
			end = len(s)
		} else {
			end += start
		}
		line, lowerLine := s[start:end], lower[start:end]
		if open.ok {
			// The previous line left a secret value open: mask its tail here
			// first, then run the line rules over the result.
			line, open = continuation(line, open, &st)
			if line != s[start:end] {
				lowerLine = asciiLower(line)
			}
		}
		next := s[min(len(s), end+1):min(len(s), end+1+128)]
		if end < len(s) {
			if from := openFrom(line, next); from >= 0 && kvPre(line[from:], lowerLine[from:]) {
				open = openAtEOL(line[from:], lowerLine[from:], open)
			}
		}
		out := r.lineFixed(line, lowerLine, &st)
		// A rule may mask a token that had swallowed a later key; that key's
		// value is then open only in the output, and a second pass would
		// follow it. Decide on the output too, so one pass is enough.
		if end < len(s) && !open.ok && out != line {
			if from := openFrom(out, next); from >= 0 {
				lo := asciiLower(out[from:])
				if kvPre(out[from:], lo) {
					open = openAtEOL(out[from:], lo, open)
				}
			}
		}
		if end < len(s) {
			open = lookAhead(open, s[end+1:])
		}
		if out != s[start:end] && !changed {
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
	// The Oracle pass goes last of the rules above and only adds masks
	// (oracleRule); the passes in more.go run on its output and only add
	// masks too.
	return more(orig, maskOracle(r.extend(s, &st), &st), &st), st
}

// lineFixed masks one line until nothing more changes.
func (r *Redactor) lineFixed(s, lower string, st *Stats) string {
	for range maxPasses {
		out := r.line(s, lower, st)
		if out == s {
			break
		}
		s, lower = out, asciiLower(out)
	}
	return s
}

// line runs every line rule once over one line. lower is the line in ASCII
// lower case, for the prefilters.
func (r *Redactor) line(s, lower string, st *Stats) string {
	for i := range r.rules {
		ru := &r.rules[i]
		if !ru.pre(s, lower) {
			continue
		}
		out := applyCounted(ru, s, lower, st)
		if out != s {
			s, lower = out, asciiLower(out)
		}
	}
	if out, n := maskAWSSecrets(s); n > 0 {
		for range n {
			st.add(KindAWSSecret)
		}
		s = out
	}
	return s
}

// applyCounted runs one rule when its prefilter hits and counts its masks.
func applyCounted(ru *rule, s, lower string, st *Stats) string {
	if !ru.pre(s, lower) {
		return s
	}
	out, n := ru.apply(s, lower)
	for range n {
		st.add(ru.kind)
	}
	return out
}

// asciiLower lower-cases ASCII letters only, so byte offsets stay the same
// as in s (strings.ToLower may change the length of non-ASCII text).
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

// rule is one regexp detector. groups name the submatch holding the secret;
// everything else in the match is kept as is.
type rule struct {
	kind Kind
	re   *regexp.Regexp
	// groups lists candidate submatches; the first one that took part in the
	// match is the secret (alternatives such as quoted and bare values).
	groups []int
	style  maskStyle
	// pre is a cheap filter that skips the regexp when it cannot match.
	pre func(s, lower string) bool
	// ok, when set, decides whether a match is really a secret.
	ok func(s string, m []int) bool
	// styleOf, when set, overrides style per match.
	styleOf func(s string, m []int) maskStyle
	// find, when set, replaces re: it returns matches laid out like
	// FindAllStringSubmatchIndex.
	find func(s, lower string) [][]int
	// remask lets a value that holds a mask be masked again as a whole;
	// find has already dropped values that are a mask and nothing else.
	remask bool
}

func (ru *rule) apply(s, lower string) (string, int) {
	var ms [][]int
	if ru.find != nil {
		ms = ru.find(s, lower)
	} else {
		ms = ru.re.FindAllStringSubmatchIndex(s, -1)
	}
	if len(ms) == 0 {
		return s, 0
	}
	var b strings.Builder
	last, n := 0, 0
	for _, m := range ms {
		g0, g1 := -1, -1
		for _, g := range ru.groups {
			if m[2*g] >= 0 {
				g0, g1 = m[2*g], m[2*g+1]
				break
			}
		}
		if g0 < 0 || g0 == g1 {
			continue
		}
		val := s[g0:g1]
		// A one-character "value" is a parse artifact (a stray quote or
		// backslash), never a credential worth a mask.
		if len(val) <= 1 || (isMasked(val) && !ru.remask) || isPlaceholder(val) {
			continue
		}
		if ru.ok != nil && !ru.ok(s, m) {
			continue
		}
		if n == 0 {
			b.Grow(len(s))
		}
		b.WriteString(s[last:g0])
		style := ru.style
		if ru.styleOf != nil {
			style = ru.styleOf(s, m)
		}
		b.WriteString(mask(val, style))
		last = g1
		n++
	}
	if n == 0 {
		return s, 0
	}
	b.WriteString(s[last:])
	return b.String(), n
}

// maskStyle says how much of a value a mask may keep.
type maskStyle int

const (
	// styleToken keeps the first 4 and last 2 characters of a long token. A
	// random token of 20+ characters keeps plenty of entropy after that.
	styleToken maskStyle = iota
	// styleSecret keeps no characters: passwords are short and guessable.
	styleSecret
	// styleWhole replaces the value with a fixed label (private keys).
	styleWhole
	// styleBare replaces the value with the marker alone, "[redacted]": the
	// form the local Oracle mask in internal/tools/kafkatools writes and
	// skips, so that pass stays a no-op on this package's output.
	styleBare
)

const marker = "[redacted"

func isMasked(v string) bool { return strings.Contains(v, marker) }

// isPlaceholder reports values that are already hidden or are not a secret
// at all: "***", "<hidden>", "${file:...}", "null", "none".
func isPlaceholder(v string) bool {
	t := strings.Trim(v, `"'`)
	if t == "" {
		return true
	}
	if strings.Trim(t, "*•x.") == "" && strings.ContainsAny(t, "*•") {
		return true
	}
	if strings.HasPrefix(t, "${") || (strings.HasPrefix(t, "<") && strings.HasSuffix(t, ">")) {
		return true
	}
	switch strings.ToLower(t) {
	case "null", "none", "nil", "true", "false", "redacted", "[redacted]", "hidden", "<hidden>", "<redacted>", "changeme_placeholder":
		return true
	}
	return false
}

func mask(v string, style maskStyle) string {
	n := len(v)
	cls := strconv.Itoa(lengthClass(n))
	switch style {
	case styleWhole:
		return marker + " private key " + cls + "]"
	case styleBare:
		return marker + "]"
	case styleToken:
		if n >= 20 {
			return headRunes(v, 4) + "…" + marker + " " + cls + "]…" + tailRunes(v, 2)
		}
	}
	return marker + " " + cls + "]"
}

// lengthClass rounds a length up to a coarse bucket, so the mask tells a
// 12-character password from a 40-character key without giving the exact
// length.
func lengthClass(n int) int {
	for _, c := range []int{8, 16, 24, 32, 48, 64, 128, 256, 512, 1024, 2048, 4096} {
		if n <= c {
			return c
		}
	}
	return 8192
}

func headRunes(s string, k int) string {
	for i := range s {
		if k == 0 {
			return s[:i]
		}
		k--
	}
	return s
}

func tailRunes(s string, k int) string {
	i := len(s)
	for k > 0 && i > 0 {
		i--
		for i > 0 && s[i]&0xC0 == 0x80 {
			i--
		}
		k--
	}
	return s[i:]
}

// entropy is the Shannon entropy of s in bits per byte.
func entropy(s string) float64 {
	if s == "" {
		return 0
	}
	var counts [256]int
	for i := 0; i < len(s); i++ {
		counts[s[i]]++
	}
	var h float64
	n := float64(len(s))
	for _, c := range counts {
		if c == 0 {
			continue
		}
		p := float64(c) / n
		h -= p * math.Log2(p)
	}
	return h
}

// classes counts upper-case letters, lower-case letters and digits in s.
func classes(s string) (upper, lower, digit int) {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 'A' && c <= 'Z':
			upper++
		case c >= 'a' && c <= 'z':
			lower++
		case c >= '0' && c <= '9':
			digit++
		}
	}
	return
}

// looksRandom reports whether v reads like a generated credential rather than
// a word, a name or a number.
func looksRandom(v string, minLen int) bool {
	if len(v) < minLen {
		return false
	}
	u, l, d := classes(v)
	kinds := 0
	for _, c := range []int{u, l, d} {
		if c > 0 {
			kinds++
		}
	}
	if kinds < 2 {
		return false
	}
	return entropy(v) >= 3.0
}
