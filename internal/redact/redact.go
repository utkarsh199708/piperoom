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
	for start := 0; start <= len(s); {
		end := strings.IndexByte(s[start:], '\n')
		if end < 0 {
			end = len(s)
		} else {
			end += start
		}
		line := s[start:end]
		out := r.lineFixed(line, lower[start:end], &st)
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
	return s, st
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
		if isMasked(val) || isPlaceholder(val) {
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
