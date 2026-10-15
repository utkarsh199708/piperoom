package redact

import (
	"regexp"
	"slices"
	"strings"
	"unicode"
)

// Every regexp below is compiled once and never changed; regexp.Regexp is
// safe for concurrent use, so these are constants in all but name.

func containsAny(subs ...string) func(s, lower string) bool {
	return func(_, lower string) bool {
		for _, x := range subs {
			if strings.Contains(lower, x) {
				return true
			}
		}
		return false
	}
}

func containsCase(subs ...string) func(s, lower string) bool {
	return func(s, _ string) bool {
		for _, x := range subs {
			if strings.Contains(s, x) {
				return true
			}
		}
		return false
	}
}

// randomGroup accepts a match only when its secret group reads as generated.
func randomGroup(g, minLen int) func(s string, m []int) bool {
	return func(s string, m []int) bool {
		return looksRandom(s[m[2*g]:m[2*g+1]], minLen)
	}
}

// pemRule finds PEM private keys, including ones inside JSON strings ("\n"
// escapes) and ones cut off by truncation (no END line). It is the only rule
// that spans lines; the line rules below run one line at a time.
var pemRule = rule{
	kind:   KindPrivateKey,
	find:   pemFind,
	groups: []int{1},
	style:  styleWhole,
	pre:    containsCase("PRIVATE KEY-----"),
}

// baseRules match within one line.
var baseRules = []rule{
	// user:password@ in URLs and DSNs (postgres://, jdbc:mysql://, amqp://...).
	{
		kind:   KindURLPass,
		find:   urlFind,
		groups: []int{2},
		style:  styleSecret,
		pre:    containsAny("://"),
		ok: func(s string, m []int) bool {
			// host:port@... is not a credential.
			pass := s[m[4]:m[5]]
			return !(allDigits(pass) && strings.Contains(s[m[2]:m[3]], "."))
		},
	},
	// Authorization headers, any scheme, including Snowflake Token="ver:...".
	{
		kind: KindAuthHeader,
		re: regexp.MustCompile(`(?i)\b(?:proxy-)?authorization["']?\s*[:=]\s*["']?` +
			`(?:(bearer|basic|digest|token|negotiate|aws4-hmac-sha256|snowflake\s+token\s*=)\s*["']?)?([^\s"',;]+)`),
		groups: []int{2},
		style:  styleToken,
		pre:    containsAny("authorization"),
		// Basic credentials are base64 of user:password; the last
		// characters would leak password bytes, so they keep none.
		styleOf: func(s string, m []int) maskStyle {
			if m[2] >= 0 && strings.EqualFold(s[m[2]:m[3]], "basic") {
				return styleSecret
			}
			return styleToken
		},
	},
	// Slack incoming webhooks: the last path segment is the secret.
	{
		kind: KindSlackHook,
		re: regexp.MustCompile(`hooks\.slack\.com/(?:services/T[A-Z0-9]+/B[A-Z0-9]+/([A-Za-z0-9]{8,})` +
			`|(?:workflows|triggers)/([A-Za-z0-9/]{16,}))`),
		groups: []int{1, 2},
		style:  styleSecret,
		pre:    containsCase("hooks.slack.com/"),
	},
	{
		kind:   KindBearer,
		re:     regexp.MustCompile(`(?i)\bbearer\s+([A-Za-z0-9._~+/=-]{16,})`),
		groups: []int{1},
		style:  styleToken,
		pre:    containsAny("bearer"),
		ok:     randomGroup(1, 16),
	},
	{
		kind:   KindJWT,
		re:     regexp.MustCompile(`\b(eyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]*)`),
		groups: []int{1},
		style:  styleToken,
		pre:    containsCase("eyJ"),
	},
	// Snowflake session and master tokens: ver:1-hint:1234-ETMsDg...
	{
		kind:   KindSnowflake,
		re:     regexp.MustCompile(`\b(ver:[0-9]+-[A-Za-z0-9:+/=_-]{16,})`),
		groups: []int{1},
		style:  styleToken,
		pre:    containsCase("ver:"),
	},
	{
		kind:   KindAWSKeyID,
		re:     regexp.MustCompile(`\b((?:AKIA|ASIA|ABIA|ACCA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA|APKA)[0-9A-Z]{16})\b`),
		groups: []int{1},
		style:  styleToken,
		pre:    hasAWSPrefix,
	},
	{
		kind:   KindGCP,
		re:     regexp.MustCompile(`\b(AIza[0-9A-Za-z_-]{35}|ya29\.[0-9A-Za-z_-]{20,}|GOCSPX-[0-9A-Za-z_-]{20,})`),
		groups: []int{1},
		style:  styleToken,
		pre:    containsCase("AIza", "ya29.", "GOCSPX-"),
	},
	// Azure AD client secrets (xxxNQ~...) and SAS signatures (sig=...).
	{
		kind:   KindAzure,
		re:     regexp.MustCompile(`\b([A-Za-z0-9_.-]{3}[0-9]Q~[A-Za-z0-9_~.-]{31,34})|[?&;]sig=([A-Za-z0-9%+/=]{20,})`),
		groups: []int{1, 2},
		style:  styleToken,
		pre:    containsCase("Q~", "sig="),
	},
	// API keys with a well-known prefix.
	{
		kind: KindAPIKey,
		re: regexp.MustCompile(`\b(sk-ant-[A-Za-z0-9_-]{20,}|sk-(?:proj-|live-|test-)?[A-Za-z0-9_-]{20,}` +
			`|[sr]k_(?:live|test)_[A-Za-z0-9]{16,}|xox[abposre]-[A-Za-z0-9-]{10,}|xapp-[A-Za-z0-9-]{10,}` +
			`|gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{22,}|glpat-[A-Za-z0-9_-]{20,}` +
			`|npm_[A-Za-z0-9]{36}|hf_[A-Za-z0-9]{30,})`),
		groups: []int{1},
		style:  styleToken,
		pre:    containsCase("sk-", "k_live_", "k_test_", "xox", "xapp-", "ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_", "glpat-", "npm_", "hf_"),
		ok:     randomGroup(1, 10),
	},
	// Kafka Connect's REST form {"name":"database.password","value":"..."}:
	// the sibling "name" is the key.
	{
		kind:    KindKeyValue,
		find:    nameValueFind,
		groups:  []int{2, 3},
		pre:     nameValuePre,
		ok:      nameValueOK,
		styleOf: func(s string, m []int) maskStyle { return keyStyle(s[m[2]:m[3]]) },
	},
	// key=value, key: value, "key": "value", key="value" whose key names a
	// secret. The key decides; see secretKey.
	{
		kind:    KindKeyValue,
		find:    kvFind,
		groups:  kvValueGroups,
		pre:     kvPre,
		ok:      kvOK,
		styleOf: kvStyle,
	},
	// A secret given as the next argument of a command-line flag
	// (psql --password X, confluent login --api-secret X) or as curl's
	// -u/--user user:password. A flag that only ends in such a word is read,
	// so --password-file or --token-ttl keep their values.
	{
		kind: KindKeyValue,
		re: regexp.MustCompile(`(?i)(?:^|\s)--?[a-z0-9-]*(?:password|passwd|passphrase|secret|token)[ \t]+(?:"([^"\s]+)"|'([^'\s]+)'|([^\s"'-][^\s"']*))` +
			`|(?:^|\s)(?:-u|--user)[ \t]+["']?[^\s:"']+:([^\s"']+)`),
		groups: []int{1, 2, 3, 4},
		style:  styleSecret,
		pre:    containsAny("pass", "secret", "token", "-u"),
	},
	// An XML element whose name names a secret: <password>X</password>.
	{
		kind:   KindKeyValue,
		re:     regexp.MustCompile(`(?i)<(?:[a-z][a-z0-9_.:-]*)?(?:password|passwd|passphrase|secret|token)>([^<\s][^<]*)</`),
		groups: []int{1},
		style:  styleSecret,
		pre:    containsAny("pass", "secret", "token"),
	},
}

// kvAnchored matches one key/value pair starting at a candidate key: group 1
// is the key, groups 2-10 the value. Closed values come first: escaped with
// three backslashes (\\\"...\\\", JSON two levels deep), escaped-quoted
// (\"...\" inside a JSON string or a Go %q error), double-quoted,
// single-quoted, bare. Then quoted values whose closing quote is not on this
// line: they run to the end of the line (a truncated summary, or a value
// continued on the next line; see continuation). URL-encoded pairs are parsed
// by kvEncoded instead.
//
// A bare value ends at whitespace, so an unquoted password with spaces is
// masked only up to its first space: "password=a b c" and "password=abc
// retries=3" cannot be told apart, and key=value lines are far more common.
var kvAnchored = regexp.MustCompile(`^(?:\\{0,3}["'])?([A-Za-z][A-Za-z0-9_.-]{1,63})(?:\\{0,3}["'])?[ \t]*(?:=|:)[ \t]*` +
	`(?:\\\\\\"((?:[^"\\\n]|\\[^"\n])*?)\\\\\\"|\\"((?:[^"\\\n]|\\[^"\n])*)\\"|"((?:[^"\\\n]|\\.)*)"|'([^'\n]*)'|([^\s,;&"'}\])<>\\]+)` +
	`|\\"((?:[^"\\\r\n]|\\[^"\r\n])*)\\?\r?$|"((?:[^"\\\r\n]|\\[^\r\n])*)\\?\r?$|'([^'\r\n]*)\r?$` +
	`|\\\\\\"((?:[^"\\\r\n]|\\[^"\r\n])*)\\?\r?$)`)

// kvValueGroups are the value groups of kvAnchored, in order.
var kvValueGroups = []int{2, 3, 4, 5, 6, 7, 8, 9, 10}

// Value groups of kvAnchored that need care: the bare value, and the quoted
// values left open at the end of the line.
const (
	kvDouble     = 4
	kvSingle     = 5
	kvBare       = 6
	kvOpenEsc    = 7
	kvOpenDouble = 8
	kvOpenSingle = 9
	kvOpenEsc3   = 10
)

// kvWindow is how much of a value kvAnchored first sees. Values
// that reach it are measured again: a bare one by its run of value bytes, a
// quoted one with a window twice as wide. Without the bound, a line of many
// keys whose values are rejected (token=a token=a ...) costs a regexp run to
// the end of the line per key.
const kvWindow = 256

// kvWords are the fragments a secret key name contains; secretKey makes the
// real decision.
var kvWords = []string{"pass", "pwd", "secret", "token", "credential", "key", "auth", "session", "cookie", "sas"}

func isKeyChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-'
}

func isLetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }

// kvStart is a candidate key: where its match starts, the key, where its
// separator is, and for a URL-encoded pair where its value starts (-1
// otherwise).
type kvStart struct{ ks, keyS, keyE, sep, vs int }

// kvFind finds key/value pairs whose key holds a kvWords fragment. Instead of
// scanning the whole line with one unanchored regexp (slow: every "a: b" is a
// candidate), it locates each fragment, widens it to the whole key, checks a
// separator follows, and only then matches there. Every step is bounded or
// cached, so a hostile line costs time linear in its length.
func kvFind(s, lower string) [][]int {
	var starts []kvStart
	for _, w := range kvWords {
		for off := 0; off < len(lower); {
			i := strings.Index(lower[off:], w)
			if i < 0 {
				break
			}
			p := off + i
			ks, ke := p, p+len(w)
			for ks > 0 && isKeyChar(s[ks-1]) {
				ks--
			}
			for ke < len(s) && isKeyChar(s[ke]) {
				ke++
			}
			off = ke
			for ks < p && !isLetter(s[ks]) {
				ks++
			}
			if ke-ks > 64 {
				continue
			}
			if vs := encodedSepAt(lower, ke); vs >= 0 {
				starts = append(starts, kvStart{ks, ks, ke, ke, vs})
				continue
			}
			j := ke
			// A quoted key may be escaped up to three times.
			for b := 0; b < 3 && j+1 < len(s) && s[j] == '\\'; b++ {
				j++
			}
			if j < len(s) && (s[j] == '"' || s[j] == '\'') {
				j++
			} else if j > ke && s[j-1] == '\\' {
				continue
			}
			for j < len(s) && (s[j] == ' ' || s[j] == '\t') {
				j++
			}
			if j >= len(s) || (s[j] != '=' && s[j] != ':') {
				continue
			}
			st := ks
			if st > 0 && (s[st-1] == '"' || s[st-1] == '\'') {
				st--
				if st > 0 && s[st-1] == '\\' {
					st--
				}
			}
			starts = append(starts, kvStart{st, ks, ke, j, -1})
		}
	}
	if len(starts) == 0 {
		return nil
	}
	slices.SortFunc(starts, func(a, b kvStart) int { return a.ks - b.ks })
	ends := valueEnds{s: s, lower: lower}
	var out [][]int
	last := 0
	// rejFrom/rejTo hold the bare value of the last pair that was not a
	// secret. A pair inside it can hold no quote or blank, so it is just
	// key, separator and value to the same end: no regexp needed. Many
	// keys in one long value (token=a token=a ...) stay linear.
	rejFrom, rejTo := -1, -1
	for _, c := range starts {
		if c.ks < last {
			continue
		}
		var m []int
		switch {
		case c.vs >= 0:
			m = kvEncoded(c, &ends)
		case c.keyS >= rejFrom && c.keyS == c.ks && c.keyE == c.sep && c.sep+1 < rejTo && c.keyE-c.keyS >= 2:
			m = make([]int, 2*(kvOpenEsc3+1))
			for k := range m {
				m[k] = -1
			}
			g := 2 * kvBare
			m[0], m[1], m[2], m[3], m[g], m[g+1] = c.ks, rejTo, c.keyS, c.keyE, c.sep+1, rejTo
		default:
			m = kvMatch(s, c, &ends)
		}
		if m == nil {
			continue
		}
		// A pair that is not a secret must not hide the pairs inside its
		// value: "sasl.jaas.config": "... password=\"x\"".
		if !kvOK(s, m) {
			if g := 2 * kvBare; m[g] >= 0 && c.vs < 0 {
				rejFrom, rejTo = m[g], m[g+1]
			}
			continue
		}
		out = append(out, m)
		last = m[1]
	}
	return out
}

// kvMatch runs kvAnchored at c over a window that ends kvWindow bytes past
// the separator; see kvWindow.
func kvMatch(s string, c kvStart, ends *valueEnds) []int {
	ks := c.ks
	we := min(len(s), c.sep+kvWindow)
	for {
		m := kvAnchored.FindStringSubmatchIndex(s[ks:we])
		if m == nil {
			return nil
		}
		for k := range m {
			if m[k] >= 0 {
				m[k] += ks
			}
		}
		if m[1] < we || we == len(s) {
			return m
		}
		// The value reaches the window end, so it may go on.
		if g := 2 * kvBare; m[g] >= 0 {
			m[g+1] = ends.bare(m[g+1])
			m[1] = m[g+1]
			return m
		}
		we = min(len(s), ks+2*(we-ks))
	}
}

// valueEnds finds where values end, caching what it scanned so many keys
// inside one long value do not rescan it.
type valueEnds struct {
	s, lower         string
	runFrom, runTo   int // a scanned run of bare value bytes
	next26, next2526 int // next "%26" / "%2526" at or after the last query
	ok26, ok2526     bool
}

// bare returns where the run of bare value bytes containing x ends.
func (v *valueEnds) bare(x int) int {
	if v.runTo > 0 && x >= v.runFrom && x <= v.runTo {
		return v.runTo
	}
	e := x
	for e < len(v.s) && !bareStops[v.s[e]] {
		e++
	}
	v.runFrom, v.runTo = x, e
	return e
}

// encoded returns where an unquoted URL-encoded value starting at x ends: at
// the end of its bare run or at an encoded '&' (once or twice encoded),
// where the next parameter starts. A raw '%' in a plain value is just a
// byte, so only encoded pairs end there.
func (v *valueEnds) encoded(x int) int {
	e := v.bare(x)
	if !v.ok26 || v.next26 < x && v.next26 >= 0 {
		v.next26, v.ok26 = indexFrom(v.lower, "%26", x), true
	}
	if !v.ok2526 || v.next2526 < x && v.next2526 >= 0 {
		v.next2526, v.ok2526 = indexFrom(v.lower, "%2526", x), true
	}
	for _, n := range []int{v.next26, v.next2526} {
		if n >= x && n < e {
			e = n
		}
	}
	return e
}

func indexFrom(s, sub string, x int) int {
	if i := strings.Index(s[x:], sub); i >= 0 {
		return x + i
	}
	return -1
}

// URL-encoded pieces around a key, once or twice encoded, lower case.
var (
	encQuotes = []string{"%22", "%27", "%2522", "%2527"}
	encBlanks = []string{"+", "%20", "%2520"}
	encSeps   = []string{"%3d", "%3a", "%253d", "%253a"}
)

func skipAny(lower string, j int, pieces []string, many bool) int {
	for {
		k := j
		for _, p := range pieces {
			if strings.HasPrefix(lower[j:], p) {
				k = j + len(p)
				break
			}
		}
		if k == j || !many {
			return k
		}
		j = k
	}
}

// encodedSepAt reports whether a URL-encoded separator follows the key that
// ends at j ("password%3D", "%22password%22%3A", "password%20%3D%20",
// "password%253D") and returns where the value starts, or -1.
func encodedSepAt(lower string, j int) int {
	j = skipAny(lower, j, encQuotes, false)
	j = skipAny(lower, j, encBlanks, true)
	k := skipAny(lower, j, encSeps, false)
	if k == j {
		return -1
	}
	return skipAny(lower, k, encBlanks, true)
}

// kvEncoded builds the match of a URL-encoded pair laid out like kvAnchored's,
// its value in the bare group. A value in encoded quotes ends at the closing
// one; any other at an encoded '&'.
func kvEncoded(c kvStart, ends *valueEnds) []int {
	m := make([]int, 2*(kvOpenEsc3+1))
	for k := range m {
		m[k] = -1
	}
	m[0], m[2], m[3] = c.ks, c.keyS, c.keyE
	vs, ve := c.vs, -1
	if vs < len(ends.s) && (ends.s[vs] == '"' || ends.s[vs] == '\'') {
		// A raw quote after an encoded separator: the value runs to the
		// closing quote, or stays open to the end of the line.
		q := ends.s[vs]
		e := indexFrom(ends.s, "\n", vs)
		if e < 0 {
			e = len(ends.s)
		}
		closed, open := kvDouble, kvOpenDouble
		if q == '\'' {
			closed, open = kvSingle, kvOpenSingle
		}
		if i := strings.IndexByte(ends.s[vs+1:e], q); i >= 0 {
			m[2*closed], m[2*closed+1], m[1] = vs+1, vs+1+i, vs+2+i
			return m
		}
		if e > vs+1 && ends.s[e-1] == '\r' {
			e--
		}
		g := 2 * open
		m[g], m[g+1], m[1] = vs+1, e, e
		return m
	}
	if q := skipAny(ends.lower, vs, encQuotes, false); q > vs {
		quote := ends.lower[vs:q]
		vs = q
		if i := indexFrom(ends.lower[:ends.bare(vs)], quote, vs); i >= 0 {
			ve = i
		}
	}
	if ve < 0 {
		ve = ends.encoded(vs)
	}
	g := 2 * kvBare
	m[g], m[g+1], m[1] = vs, ve, ve
	return m
}

// nameValue matches a "name"/"value" pair at a "name" key, raw or escaped
// once (JSON inside a JSON string): group 1 is the name, 2 or 3 the value.
var nameValue = regexp.MustCompile(`^(?:"name"[ \t]*:[ \t]*"([A-Za-z][A-Za-z0-9_.-]{1,63})"[ \t]*,[ \t]*"value"[ \t]*:[ \t]*"((?:[^"\\\n]|\\.)*)"` +
	`|\\"name\\"[ \t]*:[ \t]*\\"([A-Za-z][A-Za-z0-9_.-]{1,63})\\"[ \t]*,[ \t]*\\"value\\"[ \t]*:[ \t]*\\"((?:[^"\\\n]|\\[^"\n])*)\\")`)

func nameValuePre(s, _ string) bool {
	return strings.Contains(s, `name`) && strings.Contains(s, `value`)
}

// nameValueFind runs nameValue at each "name" key and lays matches out as
// [match, name, raw value, escaped value].
func nameValueFind(s, _ string) [][]int {
	var out [][]int
	for off := 0; off < len(s); {
		i := strings.Index(s[off:], `name`)
		if i < 0 {
			break
		}
		p := off + i
		off = p + 4
		st := p - 1
		if st < 0 || s[st] != '"' {
			continue
		}
		if st > 0 && s[st-1] == '\\' {
			st--
		}
		m := nameValue.FindStringSubmatchIndex(s[st:])
		if m == nil {
			continue
		}
		for k := range m {
			if m[k] >= 0 {
				m[k] += st
			}
		}
		if m[2] < 0 { // the escaped form: move its groups down
			m[2], m[3], m[4], m[5], m[6], m[7] = m[6], m[7], -1, -1, m[8], m[9]
		}
		out = append(out, m[:8])
		off = m[1]
	}
	return out
}

func nameValueOK(s string, m []int) bool {
	key := s[m[2]:m[3]]
	g := 4
	if m[g] < 0 {
		g = 6
	}
	val := s[m[g]:m[g+1]]
	switch secretKey(key) {
	case keyStrong:
		return true
	case keyWeak:
		return looksRandom(val, 12)
	}
	return false
}

// kvPre is the prefilter of the key/value rule.
var kvPre = containsAny(kvWords...)

var emailRule = rule{
	kind: KindEmail,
	find: anchoredAt("@", isLocalChar, 64,
		regexp.MustCompile(`^([A-Za-z0-9._%+-]{1,64})@((?:[A-Za-z0-9-]+\.)+([A-Za-z]{2,24}))\b`)),
	groups: []int{1},
	style:  styleSecret,
	pre:    containsCase("@"),
	ok: func(s string, m []int) bool {
		// Only real-looking domains: "connector@pg.public.orders" is a name,
		// not an address. user@host:path is an SSH remote, not an address.
		if e := m[1]; e+1 < len(s) && s[e] == ':' && s[e+1] != ' ' {
			return false
		}
		return emailTLD(strings.ToLower(s[m[6]:m[7]]))
	},
}

var ipv4Rule = rule{
	kind:   KindIP,
	re:     regexp.MustCompile(`\b((?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(?:\.(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])){3})\b`),
	groups: []int{1},
	style:  styleSecret,
	pre:    containsCase("."),
}

var ipv6Rule = rule{
	kind: KindIP,
	re: regexp.MustCompile(`(?i)(?:^|[^0-9a-f:])((?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}` +
		`|(?:[0-9a-f]{1,4}:){1,6}:(?:[0-9a-f]{1,4}(?::[0-9a-f]{1,4}){0,5})?)(?:$|[^0-9a-z:])`),
	groups: []int{1},
	style:  styleSecret,
	pre:    containsCase("::", ":"),
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}

// emailTLD reports whether tld is plausibly a real top-level domain. Any
// two-letter code counts (country domains); longer ones come from a short
// list, so dotted Kafka and table names never look like addresses.
func emailTLD(tld string) bool {
	if len(tld) == 2 {
		return true
	}
	switch tld {
	case "com", "org", "net", "edu", "gov", "mil", "int", "info", "biz", "dev", "app", "cloud",
		"tech", "xyz", "io", "ai", "internal", "local", "corp", "email", "online", "site":
		return true
	}
	return false
}

// keyStrength classifies a key name.
type keyStrength int

const (
	keyNone keyStrength = iota
	// keyWeak names hold a secret only when the value looks generated
	// (token, auth, session, cookie).
	keyWeak
	// keyStrong names always hold a secret (password, secret, api key).
	keyStrong
)

// strongWords are key words whose value is always a secret.
var strongWords = map[string]bool{
	"password": true, "passwd": true, "passphrase": true, "pwd": true, "pass": true,
	"secret": true, "secrets": true, "credential": true, "credentials": true,
	"apikey": true, "accesskey": true, "secretkey": true, "privatekey": true,
	"accountkey": true, "signingkey": true, "masterkey": true, "sharedkey": true,
	"sas": true,
}

// weakWords are key words whose value is a secret only when it looks random.
var weakWords = map[string]bool{
	"token": true, "tokens": true, "auth": true, "session": true, "sessionid": true,
	"cookie": true, "bearer": true, "signature": true, "sig": true,
	"accesstoken": true, "refreshtoken": true, "sessiontoken": true, "authtoken": true,
}

// keyQualifiers turn a following "key" into a secret key name: api key,
// access key, private key...
var keyQualifiers = map[string]bool{
	"api": true, "access": true, "secret": true, "private": true, "account": true,
	"signing": true, "encryption": true, "master": true, "client": true, "auth": true,
	"shared": true, "storage": true, "subscription": true, "primary": true, "secondary": true,
	"license": true, "service": true,
}

// metaWords end key names that describe a secret without holding one:
// password_encryption, token_type, auth.method, secret_name, key_id...
var metaWords = map[string]bool{
	"name": true, "type": true, "method": true, "mechanism": true, "encryption": true,
	"policy": true, "required": true, "file": true, "path": true, "location": true,
	"expiry": true, "expires": true, "expiration": true, "ttl": true, "length": true,
	"count": true, "enabled": true, "provider": true, "version": true, "kind": true,
	"ref": true, "url": true, "uri": true, "endpoint": true, "id": true, "ids": true,
	"mode": true, "source": true, "header": true, "scheme": true, "format": true,
	"age": true, "at": true, "used": true, "size": true, "limit": true, "max": true,
	"min": true, "store": true, "class": true, "protocol": true, "timeout": true,
	"ms": true, "seconds": true, "hash": true, "algorithm": true, "user": true,
	"username": true, "prefix": true, "label": true, "hint": true, "field": true,
}

// keyWords splits a key into lower-case words on '_', '.', '-' and
// lower→upper case changes (clientSecret → client, secret).
func keyWords(k string) []string {
	var words []string
	start := 0
	flush := func(end int) {
		if end > start {
			words = append(words, strings.ToLower(k[start:end]))
		}
	}
	for i := 0; i < len(k); i++ {
		c := rune(k[i])
		switch {
		case c == '_' || c == '.' || c == '-':
			flush(i)
			start = i + 1
		case i > start && unicode.IsUpper(c) && unicode.IsLower(rune(k[i-1])):
			flush(i)
			start = i
		}
	}
	flush(len(k))
	return words
}

// secretKey classifies key k.
func secretKey(k string) keyStrength {
	words := keyWords(k)
	if len(words) == 0 {
		return keyNone
	}
	last := words[len(words)-1]
	if metaWords[last] && !(last == "id" && len(words) > 1 && words[len(words)-2] == "session") {
		return keyNone
	}
	best := keyNone
	for i, w := range words {
		switch {
		case strongWords[w]:
			return keyStrong
		case w == "key" && i > 0 && keyQualifiers[words[i-1]]:
			return keyStrong
		case w == "id" && i > 0 && words[i-1] == "session":
			best = keyWeak
		case weakWords[w]:
			best = keyWeak
		case strings.Contains(w, "password") || strings.Contains(w, "passwd") ||
			strings.Contains(w, "secret") || strings.HasSuffix(w, "apikey"):
			return keyStrong
		case strings.HasSuffix(w, "token"):
			best = keyWeak
		}
	}
	return best
}

func kvGroups(s string, m []int) (key, val string) {
	key = s[m[2]:m[3]]
	for _, g := range kvValueGroups {
		if m[2*g] >= 0 {
			return key, s[m[2*g]:m[2*g+1]]
		}
	}
	return key, ""
}

func kvOK(s string, m []int) bool {
	key, val := kvGroups(s, m)
	switch secretKey(key) {
	case keyStrong:
		return true
	case keyWeak:
		// The first KiB decides: a long value holding many keys is
		// otherwise rescanned once per key.
		return looksRandom(val[:min(len(val), 1024)], 12)
	}
	return false
}

func kvStyle(s string, m []int) maskStyle {
	key, _ := kvGroups(s, m)
	return keyStyle(key)
}

// keyStyle keeps no characters of a password (short, guessable) and the
// usual head and tail of anything else.
func keyStyle(key string) maskStyle {
	for _, w := range keyWords(key) {
		if w == "pwd" || w == "pass" || strings.Contains(w, "passw") || strings.Contains(w, "passphrase") {
			return styleSecret
		}
	}
	return styleToken
}

// maskAWSSecrets masks standalone 40-character AWS secret access keys: the
// one secret with no prefix that often appears without a key name (CSV
// credential exports, "AKIA... wJalr..." pairs). Hex digests and paths do not
// qualify: the token needs upper, lower and digits and high entropy.
func maskAWSSecrets(s string) (string, int) {
	if len(s) < 40 {
		return s, 0
	}
	var b strings.Builder
	last, n := 0, 0
	isTok := func(c byte) bool {
		return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '/' || c == '+'
	}
	for i := 0; i < len(s); {
		if !isTok(s[i]) {
			i++
			continue
		}
		j := i
		for j < len(s) && isTok(s[j]) {
			j++
		}
		tok := strings.TrimRight(s[i:j], "=")
		if len(tok) == 40 && awsSecretLike(tok) && !afterIDWord(s, i) {
			if n == 0 {
				b.Grow(len(s))
			}
			b.WriteString(s[last:i])
			b.WriteString(mask(tok, styleToken))
			last = i + len(tok)
			n++
		}
		i = j
	}
	if n == 0 {
		return s, 0
	}
	b.WriteString(s[last:])
	return b.String(), n
}

func awsSecretLike(t string) bool {
	u, l, d := classes(t)
	if u < 2 || l < 2 || d < 2 {
		return false
	}
	if strings.Count(t, "/") > 3 {
		return false
	}
	return entropy(t) >= 4.0
}

func isSchemeChar(c byte) bool {
	return isLetter(c) || c >= '0' && c <= '9' || c == '+' || c == '.' || c == '-'
}

func isLocalChar(c byte) bool {
	return isLetter(c) || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '%' || c == '+' || c == '-'
}

// anchoredAt finds matches of re (anchored with ^) that start up to maxBack
// characters of class back before each occurrence of needle. Looking for the
// rare needle first ("://", "@") and matching only there is far cheaper than
// an unanchored regexp over the whole line.
func anchoredAt(needle string, back func(byte) bool, maxBack int, re *regexp.Regexp) func(s, lower string) [][]int {
	return func(s, _ string) [][]int {
		var out [][]int
		for off := 0; off < len(s); {
			i := strings.Index(s[off:], needle)
			if i < 0 {
				break
			}
			p := off + i
			ks := p
			for ks > 0 && p-ks < maxBack && back(s[ks-1]) {
				ks--
			}
			// The match starts at the first letter, as \b plus a letter did.
			for ks < p && !isLetter(s[ks]) && needle == "://" {
				ks++
			}
			m := re.FindStringSubmatchIndex(s[ks:])
			if m == nil {
				off = p + len(needle)
				continue
			}
			for k := range m {
				if m[k] >= 0 {
					m[k] += ks
				}
			}
			out = append(out, m)
			off = max(m[1], p+len(needle))
		}
		return out
	}
}

// urlUserinfo matches the scheme and user of a URL with a password; urlFind
// decides where the password ends. The user may hold one '@' (Azure's
// user@server form); a following ':' and '@' are still needed.
// An IPv6 host ("http://[::1]:8083/...") starts with '[', so the user never
// holds brackets.
var urlUserinfo = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9+.-]*://([^\s:/@?#"'<>\[\]]*(?:@[^\s:/@?#"'<>\[\]]+)?):`)

// urlFind finds user:password@ in URLs, laid out as [match, user, password].
// A password may hold a raw '@' or '/' (postgres://app:p@ss/w@db/x): such a
// URL no longer parses one way, so the password runs to the last '@' that is
// followed by a plausible host, and once a '/' or a host was seen a '?' or
// '#' ends the search. That can over-mask a path holding "@host" (never
// under-mask). host:port/path is not a password: when the text before the
// first '/' is all digits there is none. With no plausible host at all, the
// password ends at the first '@', as long as no '/' comes before it. The
// search stops at the next "://" and after 512 bytes, so a line of many
// URL-like prefixes costs linear time.
func urlFind(s, lower string) [][]int {
	var out [][]int
	last := 0
	for _, m := range urlStarts(s, lower) {
		if m[0] < last {
			continue
		}
		ps := m[1]
		limit := min(len(s), ps+512)
		if k := strings.Index(s[ps:limit], "://"); k >= 0 {
			limit = ps + k
		}
		te := ps
		for te < limit && !isURLStop(s[te]) {
			te++
		}
		if i := strings.IndexByte(s[ps:te], '/'); i >= 0 && allDigits(s[ps:ps+i]) {
			continue
		}
		end := -1
		slash := false
		for i := ps; i < te; i++ {
			switch s[i] {
			case '/':
				slash = true
			case '?', '#':
				if end >= 0 || slash {
					te = i
				}
			case '@':
				// "/@" inside a path (npm's /@scope/) is not where a
				// password ends once a host was found before it.
				if i > ps && !(s[i-1] == '/' && end >= 0) && urlHost(s[i+1:te]) {
					end = i
				}
			}
		}
		if end < 0 {
			i := strings.IndexByte(s[ps:te], '@')
			if i <= 0 || strings.IndexByte(s[ps:ps+i], '/') >= 0 {
				continue
			}
			end = ps + i
		}
		out = append(out, []int{m[0], end + 1, m[2], m[3], ps, end})
		last = end + 1
	}
	return out
}

// urlStarts finds scheme://user: prefixes.
var urlStarts = anchoredAt("://", isSchemeChar, 30, urlUserinfo)

func isURLStop(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '"' || c == '\'' || c == '<' || c == '>'
}

// urlHost reports whether h starts with a host list (host[:port][,host...],
// IPv6 in brackets) followed by the end, '/', '?' or '#'. An empty host
// counts when a path follows (postgres://u:p@/db?host=/sock).
func urlHost(h string) bool {
	i := 0
	for {
		st := i
		if i < len(h) && h[i] == '[' {
			j := strings.IndexByte(h[i:min(len(h), i+64)], ']')
			if j < 0 {
				return false
			}
			i += j + 1
		} else {
			for i < len(h) && (isLetter(h[i]) || h[i] >= '0' && h[i] <= '9' || h[i] == '.' || h[i] == '-' || h[i] == '_') {
				i++
			}
		}
		if i < len(h) && h[i] == ':' {
			i++
			for i < len(h) && h[i] >= '0' && h[i] <= '9' {
				i++
			}
		}
		if i == st && !(i < len(h) && h[i] == '/') {
			return false
		}
		if i < len(h) && h[i] == ',' {
			i++
			continue
		}
		return i == len(h) || h[i] == '/' || h[i] == '?' || h[i] == '#'
	}
}

// hasAWSPrefix reports whether s holds one of the AWS key id prefixes.
func hasAWSPrefix(s, _ string) bool {
	for off := 0; ; {
		i := strings.IndexByte(s[off:], 'A')
		if i < 0 || off+i+4 > len(s) {
			return false
		}
		switch s[off+i : off+i+4] {
		case "AKIA", "ASIA", "ABIA", "ACCA", "AGPA", "AIDA", "AROA", "AIPA", "ANPA", "ANVA", "APKA":
			return true
		}
		off += i + 1
	}
}

// recordWords name things whose key or id is data, not a credential.
var recordWords = map[string]bool{
	"record": true, "message": true, "msg": true, "partition": true, "row": true,
	"kafka": true, "idempotency": true, "dedup": true, "request": true, "event": true,
	"object": true, "correlation": true, "trace": true, "span": true, "document": true,
}

// afterIDWord reports whether the token at i follows a name for a record's
// key or id ("record key ...", "message_id=...", "partition key: ..."). A
// 40-character Kafka key is evidence. Any credential word before the key or
// id ("AWS Secret Access Key:", "api key", "private key") means the token is
// a secret; so does a bare "key=" with nothing naming a record before it.
func afterIDWord(s string, i int) bool {
	j := i
	for n := 0; j > 0 && n < 4 && strings.IndexByte(" \t:=\"',", s[j-1]) >= 0; n++ {
		j--
	}
	// Up to three tokens before the value, nearest last.
	var words []string
	first := true
	compound := false
	for range 3 {
		w := j
		for w > 0 && isKeyChar(s[w-1]) {
			w--
		}
		if w == j {
			break
		}
		ws := keyWords(s[w:j])
		if first {
			compound = len(ws) > 1
			first = false
		}
		words = append(ws, words...)
		j = w
		sp := 0
		for j > 0 && sp < 2 && (s[j-1] == ' ' || s[j-1] == '\t') {
			j--
			sp++
		}
		if sp == 0 {
			break
		}
	}
	if len(words) == 0 {
		return false
	}
	last := words[len(words)-1]
	if last != "key" && last != "id" {
		return false
	}
	for _, w := range words[:len(words)-1] {
		if keyQualifiers[w] || strongWords[w] || weakWords[w] {
			return false
		}
	}
	if compound {
		return true
	}
	return len(words) > 1 && recordWords[words[len(words)-2]]
}

// pemFind finds PEM private key blocks in linear time. A block runs from its
// BEGIN line to the matching END line when only PEM characters lie between;
// otherwise (truncated output) it runs over the base64 chunks of 16+
// characters that follow the BEGIN line. Facts about the current run of PEM
// characters (where it ends, where its next END line is) are kept across
// BEGIN lines, so repeated BEGIN lines cost no rescans.
func pemFind(s, _ string) [][]int {
	const begin, end = "-----BEGIN ", "-----END "
	var out [][]int
	runTo := -1   // end of the run of PEM characters being looked at
	nextEnd := -1 // next END line in that run; -1 unknown, -2 none left
	for off := 0; off < len(s); {
		i := strings.Index(s[off:], begin)
		if i < 0 {
			break
		}
		start := off + i
		body, ok := pemLabel(s, start+len(begin))
		if !ok {
			off = start + len(begin)
			continue
		}
		if body >= runTo {
			runTo = body
			for runTo < len(s) && isPEMChar(s[runTo]) {
				runTo++
			}
			nextEnd = -1
		}
		stop := -1
		for nextEnd != -2 {
			if nextEnd < body {
				j := strings.Index(s[body:runTo], end)
				if j < 0 {
					nextEnd = -2
					break
				}
				nextEnd = body + j
			}
			if e, ok := pemLabel(s, nextEnd+len(end)); ok {
				stop = e
				break
			}
			// An END line of something else (a certificate): look further.
			j := strings.Index(s[nextEnd+len(end):runTo], end)
			if j < 0 {
				nextEnd = -2
				break
			}
			nextEnd = nextEnd + len(end) + j
		}
		if stop < 0 {
			stop = pemChunks(s, body)
		}
		out = append(out, []int{start, stop, start, stop})
		off = stop
	}
	return out
}

// pemLabel checks that s[i:] starts with "[A-Z0-9 ]*PRIVATE KEY-----" and
// returns the index after it.
func pemLabel(s string, i int) (int, bool) {
	k := i
	for k < len(s) && k-i < 64 && (s[k] >= 'A' && s[k] <= 'Z' || s[k] >= '0' && s[k] <= '9' || s[k] == ' ') {
		k++
	}
	if !strings.HasSuffix(s[i:k], "PRIVATE KEY") || !strings.HasPrefix(s[k:], "-----") {
		return 0, false
	}
	return k + 5, true
}

// isPEMChar reports characters that may appear between BEGIN and END lines:
// base64, whitespace, the header punctuation of encrypted keys and the
// backslash of JSON "\n" escapes.
func isPEMChar(c byte) bool {
	return isLetter(c) || c >= '0' && c <= '9' || strings.IndexByte("+/= \t\r\n:,.-\\", c) >= 0
}

// pemChunks returns where the base64 chunks of 16+ characters after i end.
func pemChunks(s string, i int) int {
	pos := i
	for {
		q := pos
		for q < len(s) {
			switch {
			case s[q] == ' ' || s[q] == '\t' || s[q] == '\r' || s[q] == '\n':
				q++
				continue
			case s[q] == '\\' && q+1 < len(s) && (s[q+1] == 'n' || s[q+1] == 'r'):
				q += 2
				continue
			}
			break
		}
		r := q
		for r < len(s) && (isLetter(s[r]) || s[r] >= '0' && s[r] <= '9' || s[r] == '+' || s[r] == '/' || s[r] == '=') {
			r++
		}
		if r-q < 16 {
			return pos
		}
		pos = r
	}
}
