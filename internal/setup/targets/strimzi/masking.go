package strimzi

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"piperoom/internal/redact"
)

// ---- Masking (round 10: one union of spans) ----
//
// Every read (k8s_logs, k8s_get, k8s_events, strimzi_build_status) masks
// with maskCredentials. Three detectors run independently over the same
// sanitized text and each returns the byte ranges it would mask:
//
//   - credentialValueSpans: the values the runner wrote to (or read back
//     from) the db-credentials Secret, in every form and decoded copy;
//   - credentialKeySpans: the value of every credential key (key=value,
//     key: value, JSON at any escaping level, continued and multi-line
//     quoted values);
//   - redact.Spans: every rule of the generic redactor, PEM blocks and
//     continuation lines included.
//
// The ranges are merged and the text is rendered once. Earlier rounds
// chained the layers (mask, then redact, then the key rule, or the other
// way), and each order let one layer's rewrite hide a secret from the
// next: a key rule that filled "privateKey: -----BEGIN ..." hid the PEM
// start from redact. With a union no detector can pre-empt another: the
// output is masked wherever any one of them alone masks.

// credentialMask replaces every merged range that holds one of this
// package's own spans; a range only redact found keeps redact's mask.
const credentialMask = "[masked:credential]"

// maskSpan is a byte range [a, b) of a text to mask. repl is redact's own
// mask for a range redact found; "" marks one of this package's.
type maskSpan struct {
	a, b int
	repl string
}

// sanitizeText replaces each byte of invalid UTF-8 and each 0x01 with '?'.
// It keeps every byte offset, so a range found in the raw text is the
// same range of the sanitized one (round 10).
func sanitizeText(s string) string {
	if utf8.ValidString(s) && strings.IndexByte(s, 0x01) < 0 {
		return s
	}
	b := []byte(s)
	for i := 0; i < len(b); {
		r, n := utf8.DecodeRune(b[i:])
		if (r == utf8.RuneError && n == 1) || b[i] == 0x01 {
			b[i] = '?'
			i++
			continue
		}
		i += n
	}
	return string(b)
}

// perByteFFFD is s with each byte of invalid UTF-8 as U+FFFD: a JSON
// string the API returns decodes so.
func perByteFFFD(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		r, n := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && n == 1 {
			b.WriteString("�")
		} else {
			b.WriteString(s[i : i+n])
		}
		i += n
	}
	return b.String()
}

// maskPlan returns the sanitized text and the union of every detector's
// ranges over it.
func maskPlan(raw string, written []string) (string, []maskSpan) {
	t := sanitizeText(raw)
	var spans []maskSpan
	for _, sp := range credentialValueSpans(raw, t, written) {
		spans = append(spans, maskSpan{sp[0], sp[1], ""})
	}
	for _, sp := range credentialKeySpans(t) {
		spans = append(spans, maskSpan{sp[0], sp[1], ""})
	}
	for _, r := range redact.Spans(t) {
		spans = append(spans, maskSpan{r.Start, r.End, r.Mask})
	}
	return t, spans
}

// renderSpans writes t with each merged group of spans (overlapping or
// touching) replaced once: by credentialMask when the group holds one of
// this package's spans, else by redact's own masks in order.
func renderSpans(t string, spans []maskSpan) string {
	if len(spans) == 0 {
		return t
	}
	sort.SliceStable(spans, func(i, j int) bool { return spans[i].a < spans[j].a })
	var out strings.Builder
	at := 0
	for i := 0; i < len(spans); {
		a, b := spans[i].a, spans[i].b
		ours, repl := false, ""
		for ; i < len(spans) && spans[i].a <= b; i++ {
			b = max(b, spans[i].b)
			if spans[i].repl == "" {
				ours = true
			} else {
				repl += spans[i].repl
			}
		}
		if a < at {
			a = at
		}
		out.WriteString(t[at:a])
		if ours || repl == "" {
			out.WriteString(credentialMask)
		} else {
			out.WriteString(repl)
		}
		at = b
	}
	out.WriteString(t[at:])
	return out.String()
}

// maskCredentials masks text for a read: the union of the written values'
// ranges, the credential keys' and redact's, rendered once.
func maskCredentials(text string, written []string) string {
	t, spans := maskPlan(text, written)
	return renderSpans(t, spans)
}

// maskOwn renders only this package's detectors (written values and the
// key rule), for tests of those rules alone.
func maskOwn(text string, written []string) string {
	t := sanitizeText(text)
	var spans []maskSpan
	for _, sp := range credentialValueSpans(text, t, written) {
		spans = append(spans, maskSpan{sp[0], sp[1], ""})
	}
	for _, sp := range credentialKeySpans(t) {
		spans = append(spans, maskSpan{sp[0], sp[1], ""})
	}
	return renderSpans(t, spans)
}

// ---- written values ----

// credentialValueSpans are the ranges of the text that hold a written
// value (never a user name: see rememberSecret). raw is the text as read
// and t its sanitized form (sanitizeText, the same length). Each value is
// searched in every form it may be written in (credentialForms), for the
// value as written, sanitized and with U+FFFD per invalid byte (round 10:
// a 0x01 or invalid UTF-8 run matches in every copy), in four copies of
// raw and of t: as is, %XX decoded, %XX and '+' decoded, and JSON
// unescaped. A value shorter than 4 runes, unless it is 3 runes holding a
// non-ASCII one (round 10: ééé, €€€), is matched only as a whole token.
func credentialValueSpans(raw, t string, written []string) [][2]int {
	bases := []string{raw}
	if t != raw {
		bases = append(bases, t)
	}
	var copies []textCopy
	for _, s := range bases {
		copies = append(copies,
			decodedCopy(s, false, false, false),
			decodedCopy(s, true, false, false),
			decodedCopy(s, true, true, false),
			decodedCopy(s, false, false, true))
	}
	var spans [][2]int
	for _, w := range written {
		if w == "" {
			continue
		}
		n := utf8.RuneCountInString(w)
		short := n < 4 && !(n >= 3 && hasNonASCII(w))
		var forms []string
		for _, v := range []string{w, sanitizeText(w), perByteFFFD(w)} {
			forms = append(forms, credentialForms(v)...)
		}
		slices.Sort(forms)
		forms = slices.Compact(forms)
		for _, f := range forms {
			if f == "" {
				continue
			}
			for _, c := range copies {
				for off := 0; ; {
					i := strings.Index(c.s[off:], f)
					if i < 0 {
						break
					}
					a, b := off+i, off+i+len(f)
					if !short || (tokenStart(c.s, a) && tokenEnd(c.s, b)) {
						spans = append(spans, [2]int{c.src[a][0], c.src[b-1][1]})
					}
					off = a + 1
				}
			}
		}
	}
	return spans
}

func hasNonASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return true
		}
	}
	return false
}

// tokenStart and tokenEnd report whether s[a:b] stands as a whole token in
// s: between non-word bytes or the text's ends, or next to an escape
// sequence (\n, &, %20) in the raw copy.
func tokenStart(s string, a int) bool {
	if a == 0 || !isWordByte(s[a-1]) {
		return true
	}
	if a >= 2 && s[a-2] == '\\' {
		return true
	}
	if a >= 3 && s[a-3] == '%' && isHex(s[a-2]) && isHex(s[a-1]) {
		return true
	}
	return a >= 6 && s[a-6] == '\\' && s[a-5] == 'u' && isHex(s[a-4]) && isHex(s[a-3]) && isHex(s[a-2]) && isHex(s[a-1])
}

func tokenEnd(s string, b int) bool {
	return b == len(s) || !isWordByte(s[b]) || s[b] == '%'
}

// isWordByte reports whether c continues a token: a letter, digit, '_' or
// any byte of a non-ASCII rune (round 10: é next to ééé is a word).
func isWordByte(c byte) bool {
	return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '_' || c >= 0x80
}

// ---- the key rule ----

// keyKind is what a key names, and so which of its values are kept.
type keyKind int

const (
	notCredential keyKind = iota
	// credPlain: a password, secret, token, ...: only keptValue's
	// common values are kept.
	credPlain
	// credAuth: a key ending in auth (httpAuth, masterauth): also a plain
	// boolean or one mechanism word (authMechanisms).
	credAuth
	// credSwitch: an exempt name (NOAUTH, OAUTH, useBasicAuth,
	// requireAuth, tokenizer): only a boolean, a number, none or null.
	credSwitch
	// credCursor: a pagination cursor (nexttoken, pageToken,
	// continuationToken, next_page_token): one opaque token of at most 64
	// characters without spaces.
	credCursor
	// credResume: Debezium's resume token: only the next single token.
	credResume
	// credPath: pwd, PWD, OLDPWD: also an absolute path.
	credPath
)

// credentialSuffixes end a credential key's last segment, or its last two
// segments joined (pass_word, se_cret, auth_orization: round 10), compared
// case-insensitively: database.password, DBPassword, PGPASSWORD,
// SASLPassword, AWSSecret, APIToken, X-Auth-Token, Authorization, apiKey,
// privateKey, requirepass. auth is matched apart (credAuth).
var credentialSuffixes = []string{
	"password", "passwd", "secret", "token", "authorization", "bearer",
	"apikey", "credential", "credentials", "privatekey", "requirepass",
}

var (
	cursorKeys = map[string]bool{"nexttoken": true, "pagetoken": true, "continuationtoken": true, "nextpagetoken": true}
	switchKeys = map[string]bool{"noauth": true, "oauth": true, "usebasicauth": true, "requireauth": true, "tokenizer": true}
)

// authMechanisms are the single words an auth key's value may be and stay
// (round 10, D): a mechanism or a requirement, no secret.
var authMechanisms = map[string]bool{
	"required": true, "requested": true, "none": true, "plain": true, "scram-sha-256": true, "scram-sha-512": true,
	"gssapi": true, "oauthbearer": true, "md5": true, "basic": true, "digest": true, "kerberos": true,
}

// credentialKeyKind classifies a key. The pagination, switch and resume
// names are matched with every separator removed (nextPageToken,
// next_page_token, resume.token).
func credentialKeyKind(key string) keyKind {
	l := strings.ToLower(key)
	norm := strings.NewReplacer(".", "", "_", "", "-", "").Replace(l)
	switch {
	case cursorKeys[norm]:
		return credCursor
	case switchKeys[norm]:
		return credSwitch
	case norm == "resumetoken":
		return credResume
	case l == "jaas.config" || strings.HasSuffix(l, ".jaas.config"):
		return credPlain
	}
	segs := strings.FieldsFunc(key, func(r rune) bool { return r == '.' || r == '_' || r == '-' })
	if len(segs) == 0 {
		return notCredential
	}
	last := segs[len(segs)-1]
	cands := []string{strings.ToLower(last)}
	if len(segs) > 1 {
		cands = append(cands, strings.ToLower(segs[len(segs)-2])+cands[0])
	}
	if strings.HasSuffix(last, "Pwd") || strings.HasSuffix(last, "PWD") {
		return credPath // dbPwd, DBPWD, OLDPWD, PWD
	}
	if (strings.HasSuffix(last, "Pw") || strings.HasSuffix(last, "PW")) && len(last) > 2 {
		return credPlain // dbPw, DBPW
	}
	for _, c := range cands {
		switch {
		case c == "pwd" || c == "oldpwd":
			return credPath // pwd, MYSQL_PWD, MYSQL-PWD, p_wd
		case c == "pw":
			return credPlain
		}
		for _, kw := range credentialSuffixes {
			if strings.HasSuffix(c, kw) {
				return credPlain
			}
		}
		if strings.HasSuffix(c, "auth") || c == "masterauth" {
			return credAuth
		}
	}
	return notCredential
}

// isCredentialKey reports whether a key names a credential.
func isCredentialKey(key string) bool { return credentialKeyKind(key) != notCredential }

// credentialKeyRE is a key and its separator on a line: key=value, key:
// value, or a quoted key with = ("password" = "x"). A quoted key with : is
// JSON's (credentialJSONKeyRE).
var credentialKeyRE = regexp.MustCompile(`([A-Za-z0-9._-]+)("[ \t]*=[ \t]*|[ \t]*[=:][ \t]*)`)

// credentialJSONKeyRE is a JSON object key and its colon at any escaping
// level: "key":, \"key\": (JSON in a JSON string), \\\"key\\\": (JSON in
// a JSON string in a JSON string: round 10). The quotes' backslash runs
// must match (credentialKeySpans).
var credentialJSONKeyRE = regexp.MustCompile(`(\\*")([A-Za-z0-9._-]+)(\\*")(\s*:\s*)`)

// resumePrefixRE ends a line's text before "token" in Debezium's "resume
// token: ...", "resume" as a word (round 10: not "presume").
var resumePrefixRE = regexp.MustCompile(`(?i)(?:^|[^A-Za-z])resume $`)

// keptRefRE is a value that is one config provider reference and nothing
// else (${secrets:ns/name:key}).
var keptRefRE = regexp.MustCompile(`^\$\{[^{}\s]*\}$`)

// runnerSecretNameRE is the name of the runner's own db-credentials Secret
// object (dbSecretName: <pipeline>-db-credentials): a secret: key naming
// it names no credential (round 7, C4). A written value of that shape is
// still masked by value.
var runnerSecretNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?-db-credentials$`)

// keptLiterals are whole values that only say whether a credential is set
// or how it failed (round 8).
var keptLiterals = map[string]bool{"null": true, "none": true, "failed": true, "expired": true, "rejected": true}

var (
	numberRE      = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)
	cursorValueRE = regexp.MustCompile(`^\S{1,64}$`)
	absPathRE     = regexp.MustCompile(`^/\S*$`)
)

// keptValue reports whether a credential key's whole value (the rest of
// a key=value line, or a JSON value) is no secret for its kind. Common to
// every kind (trailing space and one pair of quotes aside): Kafka's own
// "[hidden]", one ${...} reference, the runner's Secret object's name, and
// MySQL's "(using password: YES)", YES) or NO) and nothing after it.
// Everything else is masked: a kept word followed by more is no longer
// kept (YES) Zq9..., none Zq9...).
func keptValue(kind keyKind, v string) bool {
	t := strings.TrimRight(v, " \t\r")
	if u := strings.TrimRight(t, `"'`); u == "YES)" || u == "NO)" {
		return true
	}
	if len(t) >= 2 && (t[0] == '"' || t[0] == '\'') && t[len(t)-1] == t[0] {
		t = t[1 : len(t)-1]
	}
	lt := strings.ToLower(t)
	if t == "[hidden]" || keptRefRE.MatchString(t) || runnerSecretNameRE.MatchString(t) {
		return true
	}
	switch kind {
	case credSwitch:
		return lt == "true" || lt == "false" || lt == "none" || lt == "null" || numberRE.MatchString(t)
	case credCursor:
		return keptLiterals[lt] || cursorValueRE.MatchString(t)
	case credResume:
		return cursorValueRE.MatchString(t)
	case credAuth:
		return keptLiterals[lt] || lt == "true" || lt == "false" || authMechanisms[lt]
	case credPath:
		return keptLiterals[lt] || absPathRE.MatchString(t)
	}
	return keptLiterals[lt]
}

// maxContinuation bounds how many lines a continued or open-quoted value
// is followed into.
const maxContinuation = 64

// credentialKeySpans are the ranges of the values of every credential key
// in t: a JSON value of any type at any escaping level, and on each line
// the rest of the line after a key=value or key: value's separator,
// followed into the next lines while the value is an open quote, or the
// line ends in a backslash or a split %XX (round 10). One pass: a JSON key
// inside a value already found is skipped, and each line's end is found
// once.
func credentialKeySpans(t string) [][2]int {
	var spans, values [][2]int
	done, lineEnd := 0, -1
	for _, m := range credentialJSONKeyRE.FindAllStringSubmatchIndex(t, -1) {
		if m[0] < done {
			continue
		}
		open, closeQ := t[m[2]:m[3]], t[m[6]:m[7]]
		if open != closeQ {
			continue
		}
		kind := credentialKeyKind(t[m[4]:m[5]])
		if kind == notCredential {
			continue
		}
		a := m[1]
		if a >= lineEnd {
			if lineEnd = strings.IndexByte(t[a:], '\n'); lineEnd < 0 {
				lineEnd = len(t)
			} else {
				lineEnd += a
			}
		}
		k := len(open) - 1
		b := jsonValueEnd(t, a, lineEnd, k)
		done = b
		values = append(values, [2]int{a, b})
		if strings.HasPrefix(t[a:b], open) && strings.HasSuffix(t[a:b], open) && b-a >= 2*len(open) {
			a, b = a+len(open), b-len(open)
		}
		if b > a && !keptValue(kind, unescapeLevel(t[a:b], k)) {
			spans = append(spans, [2]int{a, b})
		}
	}
	for ls := 0; ls < len(t); {
		le := strings.IndexByte(t[ls:], '\n')
		if le < 0 {
			le = len(t)
		} else {
			le += ls
		}
		line := t[ls:le]
		for _, m := range credentialKeyRE.FindAllStringSubmatchIndex(line, -1) {
			v := strings.TrimRight(line[m[1]:], "\r")
			if v == "" || inSpans(values, ls+m[2]) {
				// A key inside a credential's JSON value is part of that
				// value, which the JSON pass already decided.
				continue
			}
			key := line[m[2]:m[3]]
			kind := credentialKeyKind(key)
			if strings.EqualFold(key, "token") && resumePrefixRE.MatchString(line[:m[2]]) && strings.Contains(line[m[4]:m[5]], ":") {
				kind = credResume // "resume token: <position>"
			}
			if kind == notCredential {
				continue
			}
			vs := ls + m[1]
			if kind == credResume {
				// Only the next single token is the position.
				tok, _, _ := strings.Cut(v, " ")
				if cursorValueRE.MatchString(tok) {
					if rest := strings.TrimLeft(v[len(tok):], " \t"); rest != "" {
						spans = append(spans, [2]int{vs + len(v) - len(rest), continuedEnd(t, vs, le)})
					}
					break
				}
			} else if keptValue(kind, v) {
				// The kept value is the whole rest of the line: a key
				// inside it (the reference's ns/name:key) is no key.
				break
			}
			spans = append(spans, [2]int{vs, continuedEnd(t, vs, le)})
			break
		}
		ls = le + 1
	}
	return spans
}

// continuedEnd is the end of a key's value that starts at t[vs] on the
// line ending at le: the line's end, or a later line's while the value is
// an open quote (" or ' with no closing one yet), or the line ends in a
// backslash or in a % or %X that a split %XX continues (round 10), at most
// maxContinuation lines on.
func continuedEnd(t string, vs, le int) int {
	end := le
	v := t[vs:le]
	var q byte
	if len(v) > 0 && (v[0] == '"' || v[0] == '\'') && !hasUnescaped(v[1:], v[0]) {
		q = v[0]
	}
	cur := v
	for n := 0; n < maxContinuation && end < len(t); n++ {
		c := strings.TrimRight(cur, "\r")
		open := q != 0
		cont := strings.HasSuffix(c, `\`) || strings.HasSuffix(c, "%") || (len(c) >= 2 && c[len(c)-2] == '%' && isHex(c[len(c)-1]))
		if !open && !cont {
			break
		}
		next := strings.IndexByte(t[end+1:], '\n')
		if next < 0 {
			next = len(t)
		} else {
			next += end + 1
		}
		cur = t[end+1 : next]
		end = next
		if open && hasUnescaped(cur, q) {
			q = 0
		}
	}
	return end
}

// inSpans reports whether i lies in one of the (sorted) spans.
func inSpans(spans [][2]int, i int) bool {
	j := sort.Search(len(spans), func(j int) bool { return spans[j][1] > i })
	return j < len(spans) && spans[j][0] <= i
}

// hasUnescaped reports whether s holds q not escaped by a backslash.
func hasUnescaped(s string, q byte) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			i++
			continue
		}
		if s[i] == q {
			return true
		}
	}
	return false
}

// unescapeLevel removes k backslashes of escaping from a JSON string
// value found at that level, enough for keptValue's comparisons.
func unescapeLevel(s string, k int) string {
	if k == 0 {
		return s
	}
	return strings.ReplaceAll(s, strings.Repeat(`\`, k)+`"`, `"`)
}

// jsonValueEnd is the end of the JSON value that starts at text[a], at
// most lineEnd, at escaping level k: its quotes are k backslashes and ".
// A string ends at a quote whose backslash run is exactly k; a longer run
// is a quote escaped inside it (\\\" in \"...\"). An array or object ends
// at its matching close, strings inside skipped; a bare number or literal
// at the next , } ] or space (or backslash when escaped). A value cut off
// ends at lineEnd. One pass over the value.
func jsonValueEnd(text string, a, lineEnd, k int) int {
	q := strings.Repeat(`\`, k) + `"`
	skipString := func(i int) int {
		for i < lineEnd {
			if text[i] == '\\' {
				r := 0
				for i+r < lineEnd && text[i+r] == '\\' {
					r++
				}
				if i+r < lineEnd && text[i+r] == '"' {
					// At level k an escaped backslash of the string is
					// 2(k+1) backslashes: the run closes the string when
					// what is left of it is exactly k (\\" at k=1 is
					// an escaped backslash, then the close).
					if r%(2*(k+1)) == k {
						return i + r + 1
					}
					i += r + 1
					continue
				}
				if k == 0 && r%2 == 1 {
					r++ // \n, \u...: the escaped character
				}
				i += r
				continue
			}
			if text[i] == '"' && k == 0 {
				return i + 1
			}
			i++
		}
		return lineEnd
	}
	if strings.HasPrefix(text[a:], q) {
		return min(skipString(a+len(q)), lineEnd)
	}
	if a < lineEnd && (text[a] == '[' || text[a] == '{') {
		depth := 0
		for i := a; i < lineEnd; {
			switch {
			case strings.HasPrefix(text[i:], q) && (i == 0 || k > 0 || text[i-1] != '\\'):
				i = skipString(i + len(q))
				continue
			case text[i] == '[' || text[i] == '{':
				depth++
			case text[i] == ']' || text[i] == '}':
				depth--
				if depth == 0 {
					return i + 1
				}
			}
			i++
		}
		return lineEnd
	}
	for i := a; i < lineEnd; i++ {
		switch text[i] {
		case ',', '}', ']', ' ', '\t', '\r':
			return i
		case '\\':
			if k > 0 {
				return i
			}
		}
	}
	return lineEnd
}

// ---- shared helpers ----

// textCopy is a decoded copy of a text: for each of its bytes, the span of
// the text it came from.
type textCopy struct {
	s   string
	src [][2]int
}

// decodedCopy decodes text: %XX (either hex case) to its byte when pct, '+'
// to a space when plus, JSON string escapes (\" \\ \/ \b \f \n \r \t
// \uXXXX) when jsonEsc.
func decodedCopy(text string, pct, plus, jsonEsc bool) textCopy {
	var b strings.Builder
	src := make([][2]int, 0, len(text))
	put := func(s string, a, z int) {
		for i := 0; i < len(s); i++ {
			b.WriteByte(s[i])
			src = append(src, [2]int{a, z})
		}
	}
	for i := 0; i < len(text); {
		c := text[i]
		switch {
		case pct && c == '%' && i+2 < len(text) && isHex(text[i+1]) && isHex(text[i+2]):
			put(string([]byte{unhex(text[i+1])<<4 | unhex(text[i+2])}), i, i+3)
			i += 3
		case plus && c == '+':
			put(" ", i, i+1)
			i++
		case jsonEsc && c == '\\' && i+1 < len(text):
			if r, n := jsonEscape(text[i:]); n > 0 {
				put(r, i, i+n)
				i += n
				continue
			}
			put(`\`, i, i+1)
			i++
		default:
			// One byte, not string(c): that would be c's rune, two
			// bytes for a byte of a multi-byte character.
			b.WriteByte(c)
			src = append(src, [2]int{i, i + 1})
			i++
		}
	}
	return textCopy{b.String(), src}
}

// jsonEscape decodes the JSON string escape at the start of s, returning
// its text and length (0 when it is none).
func jsonEscape(s string) (string, int) {
	switch s[1] {
	case '"', '\\', '/':
		return s[1:2], 2
	case 'b':
		return "\b", 2
	case 'f':
		return "\f", 2
	case 'n':
		return "\n", 2
	case 'r':
		return "\r", 2
	case 't':
		return "\t", 2
	case 'u':
		if len(s) < 6 {
			return "", 0
		}
		r, ok := hex4(s[2:6])
		if !ok {
			return "", 0
		}
		if utf16.IsSurrogate(r) && len(s) >= 12 && s[6] == '\\' && s[7] == 'u' {
			if r2, ok := hex4(s[8:12]); ok {
				if p := utf16.DecodeRune(r, r2); p != utf8.RuneError {
					return string(p), 12
				}
			}
		}
		return string(r), 6
	}
	return "", 0
}

// hex4 decodes four hex digits by hand (round 8: no integer conversion of
// a wider parse), at most 0xFFFF.
func hex4(h string) (rune, bool) {
	var r rune
	for i := 0; i < 4; i++ {
		if !isHex(h[i]) {
			return 0, false
		}
		r = r<<4 | rune(unhex(h[i]))
	}
	return r, true
}

var upperHexRE = regexp.MustCompile(`%[0-9A-F]{2}`)

// credentialForms are the ways a value is written into a text: itself, its
// JSON string escapes (Go's, which also escapes <, > and &, and Java's:
// Jackson, Log4j), and its URL encodings (Go's QueryEscape and PathEscape,
// Java's URLEncoder: ~ as %7E, * kept), each also with lower-case hex.
func credentialForms(v string) []string {
	g, _ := json.Marshal(v)
	java := strings.ReplaceAll(strings.ReplaceAll(url.QueryEscape(v), "~", "%7E"), "%2A", "*")
	forms := []string{v, strings.Trim(string(g), `"`), javaJSONEscape(v)}
	for _, f := range []string{url.QueryEscape(v), url.PathEscape(v), java} {
		forms = append(forms, f, upperHexRE.ReplaceAllStringFunc(f, strings.ToLower))
	}
	slices.Sort(forms)
	return slices.Compact(forms)
}

// shortCredentialWarning is the note a read's summary carries when the
// db-credentials Secret holds a credential under minCredentialLen (one
// written before k8s_secret_apply refused it: round 8, S5). One of 4 to 7
// characters is still masked wherever it appears; one under 4 only as a
// whole token (credentialValueSpans). Both lengths count runes (round 9).
func shortCredentialWarning(written []string) string {
	for _, v := range written {
		if utf8.RuneCountInString(v) < minCredentialLen {
			return fmt.Sprintf("; warning: the db-credentials Secret holds a credential shorter than %d characters: rotate it (k8s_secret_apply refuses one that short; reads mask one under 4 characters only where it stands as a whole word)", minCredentialLen)
		}
	}
	return ""
}

// maskJSON applies maskCredentials to every string of a decoded JSON value
// (an object a get op returns), and masks the whole value of a credential
// key (credentialKeyKind) unless keptValue keeps it: the object's own
// structure is the key rule here.
func maskJSON(v any, written []string) any {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			if kind := credentialKeyKind(k); kind != notCredential {
				switch xv := x.(type) {
				case nil, bool:
				case string:
					if !keptValue(kind, xv) {
						t[k] = credentialMask
						continue
					}
				default:
					t[k] = credentialMask
					continue
				}
			}
			t[k] = maskJSON(x, written)
		}
		return t
	case []any:
		for i, x := range t {
			t[i] = maskJSON(x, written)
		}
		return t
	case string:
		return maskCredentials(t, written)
	}
	return v
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

func unhex(c byte) byte {
	switch {
	case c >= 'a':
		return c - 'a' + 10
	case c >= 'A':
		return c - 'A' + 10
	}
	return c - '0'
}

// javaJSONEscape is v as Jackson and Log4j's JSON layout write it inside a
// string: " and \ escaped, control characters as \n, \t, ... or \u00XX,
// everything else (<, >, & included) as itself.
func javaJSONEscape(v string) string {
	var b strings.Builder
	for _, r := range v {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	return b.String()
}
