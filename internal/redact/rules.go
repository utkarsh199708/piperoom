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
	kind: KindPrivateKey,
	re: regexp.MustCompile(`(-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----` +
		`(?:(?:[A-Za-z0-9+/=\s:,.-]|\\[nr])*?-----END [A-Z0-9 ]*PRIVATE KEY-----` +
		`|(?:(?:\s|\\[nr])*[A-Za-z0-9+/=]{16,})*))`),
	groups: []int{1},
	style:  styleWhole,
	pre:    containsCase("PRIVATE KEY-----"),
}

// baseRules match within one line.
var baseRules = []rule{
	// user:password@ in URLs and DSNs (postgres://, jdbc:mysql://, amqp://...).
	{
		kind: KindURLPass,
		find: anchoredAt("://", isSchemeChar, 30,
			regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9+.-]*://([^\s:/@?#"'<>]*):([^\s@/"'<>]+)@`)),
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
			`(?:(?:bearer|basic|digest|token|negotiate|aws4-hmac-sha256|snowflake\s+token\s*=)\s*["']?)?([^\s"',;]+)`),
		groups: []int{1},
		style:  styleToken,
		pre:    containsAny("authorization"),
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
	// key=value, key: value, "key": "value", key="value" whose key names a
	// secret. The key decides; see secretKey.
	{
		kind:    KindKeyValue,
		find:    kvFind,
		groups:  []int{2, 3, 4},
		pre:     containsAny(kvWords...),
		ok:      kvOK,
		styleOf: kvStyle,
	},
}

// kvAnchored matches one key/value pair starting at a candidate key: group 1
// is the key, groups 2-4 the value (double-quoted, single-quoted, bare).
var kvAnchored = regexp.MustCompile(`^["']?([A-Za-z][A-Za-z0-9_.-]{1,63})["']?[ \t]*(?:=|:)[ \t]*` +
	`(?:"((?:[^"\\\n]|\\.)*)"|'([^'\n]*)'|([^\s,;&"'}\])<>]+))`)

// kvWords are the fragments a secret key name contains; secretKey makes the
// real decision.
var kvWords = []string{"pass", "pwd", "secret", "token", "credential", "key", "auth", "session", "cookie", "sas"}

func isKeyChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-'
}

func isLetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }

// kvFind finds key/value pairs whose key holds a kvWords fragment. Instead of
// scanning the whole line with one unanchored regexp (slow: every "a: b" is a
// candidate), it locates each fragment, widens it to the whole key, checks a
// separator follows, and only then runs the anchored regexp there.
func kvFind(s, lower string) [][]int {
	var starts []int
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
			j := ke
			if j < len(s) && (s[j] == '"' || s[j] == '\'') {
				j++
			}
			for j < len(s) && (s[j] == ' ' || s[j] == '\t') {
				j++
			}
			if j >= len(s) || (s[j] != '=' && s[j] != ':') {
				continue
			}
			for ks < p && !isLetter(s[ks]) {
				ks++
			}
			if ks > 0 && (s[ks-1] == '"' || s[ks-1] == '\'') {
				ks--
			}
			starts = append(starts, ks)
		}
	}
	if len(starts) == 0 {
		return nil
	}
	slices.Sort(starts)
	var out [][]int
	last := 0
	for _, ks := range starts {
		if ks < last {
			continue
		}
		m := kvAnchored.FindStringSubmatchIndex(s[ks:])
		if m == nil {
			continue
		}
		for k := range m {
			if m[k] >= 0 {
				m[k] += ks
			}
		}
		out = append(out, m)
		last = m[1]
	}
	return out
}

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
	for _, g := range []int{2, 3, 4} {
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
		return looksRandom(val, 12)
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
		return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '/' || c == '+' || c == '='
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
		if len(tok) == 40 && awsSecretLike(tok) {
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
