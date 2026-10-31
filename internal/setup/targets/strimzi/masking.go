package strimzi

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode"
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

// sanitizeText replaces each byte of invalid UTF-8 and each 0x01 with
// '_'. It keeps every byte offset, so a range found in the raw text is the
// same range of the sanitized one (round 10). The substitute is no
// delimiter of any rule (round 11: a '?' read as the start of a URL query
// hid postgres://u?:<password>@h from redact's userinfo rule). 0x01 is
// replaced too, so the control byte never reaches a read's output.
func sanitizeText(s string) string {
	if utf8.ValidString(s) && strings.IndexByte(s, 0x01) < 0 {
		return s
	}
	b := []byte(s)
	for i := 0; i < len(b); {
		r, n := utf8.DecodeRune(b[i:])
		if (r == utf8.RuneError && n == 1) || b[i] == 0x01 {
			b[i] = '_'
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
			b.WriteString("\ufffd")
		} else {
			b.WriteString(s[i : i+n])
		}
		i += n
	}
	return b.String()
}

// maskPlan returns the sanitized text and the union of every detector's
// ranges over it. This package's detectors scan at most redact.MaxScan
// bytes of a string, as redact does (round 11); redact masks the text
// past its own cut (at most MaxScan) whole, and anything past MaxScan that
// it does not is masked here.
func maskPlan(raw string, written []string) (string, []maskSpan) {
	t := sanitizeText(raw)
	n := min(len(t), redact.MaxScan)
	// redact's cut may fall before MaxScan, inside a value that runs
	// past it: the written values are searched far enough past MaxScan to
	// hold the longest form of one, and the key rule a window more, so a
	// value straddling the cut is found whole (round 12, F1).
	nv, nk := min(len(t), n+maxFormLen(written)), min(len(t), n+keyStraddle)
	var spans []maskSpan
	for _, sp := range credentialValueSpans(raw[:nv], t[:nv], written) {
		spans = append(spans, maskSpan{sp[0], sp[1], ""})
	}
	for _, sp := range credentialKeySpans(t[:nk]) {
		spans = append(spans, maskSpan{sp[0], sp[1], ""})
	}
	tailMasked := n == len(t)
	for _, r := range redact.Spans(t) {
		spans = append(spans, maskSpan{r.Start, r.End, r.Mask})
		if r.Start <= n && r.End == len(t) {
			tailMasked = true
		}
	}
	if !tailMasked {
		spans = append(spans, maskSpan{n, len(t), ""})
	}
	return t, spans
}

// keyStraddle is how far past redact.MaxScan the key rule reads, so a
// value that starts before the cut is judged whole.
const keyStraddle = 4096

// maxFormLen bounds the length of any form credentialValueSpans searches
// for a written value: at most 9 bytes per byte (an invalid byte as
// U+FFFD, percent-encoded), and a little more.
func maxFormLen(written []string) int {
	n := 0
	for _, w := range written {
		n = max(n, 9*len(w)+16)
	}
	return n
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
	seen := map[string]bool{}
	for _, s := range bases {
		for _, c := range []textCopy{
			decodedCopy(s, false, false, false),
			decodedCopy(s, true, false, false),
			decodedCopy(s, true, true, false),
			decodedCopy(s, false, false, true),
		} {
			// A copy that decodes nothing is the text again (round 11).
			if !seen[c.s] {
				seen[c.s] = true
				copies = append(copies, c)
			}
		}
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
				last := -1 // the index in spans of this copy's last range
				for off := 0; ; {
					i := strings.Index(c.s[off:], f)
					if i < 0 {
						break
					}
					a, b := off+i, off+i+len(f)
					if !short || (tokenStart(c.s, a) && tokenEnd(c.s, b)) {
						sp := [2]int{c.src[a][0], c.src[b-1][1]}
						if last >= 0 && sp[0] <= spans[last][1] {
							// Overlapping matches (aaaaaaaaa) are one range
							// (round 11).
							spans[last][1] = max(spans[last][1], sp[1])
						} else {
							spans = append(spans, sp)
							last = len(spans) - 1
						}
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
	// credPass: the bare key pass (a test runner's "pass: 3 fail: 0"):
	// also a number (round 12).
	credPass
	// credSessionID: session, sessionId, session.id: also all digits
	// (Kafka's fetch-session ids, round 13); JSESSIONID is plain.
	credSessionID
)

// credentialSuffixes end a credential key's last segment, or its last two
// or three segments joined (pass_word, se_cret, auth_orization: round 10;
// secret_access_key: round 11), compared case-insensitively:
// database.password, DBPassword, PGPASSWORD, SASLPassword, AWSSecret,
// APIToken, X-Auth-Token, Authorization, apiKey, privateKey, requirepass,
// ssl.key.passphrase, SECRET_KEY, AccountKey, Set-Cookie,
// basic.auth.user.info. auth is matched apart (credAuth).
var credentialSuffixes = []string{
	"password", "passwd", "secret", "token", "authorization", "bearer",
	"apikey", "credential", "credentials", "privatekey", "requirepass",
	// Round 11.
	"passphrase", "passcode", "secretkey", "secretaccesskey", "accountkey", "sharedaccesskey",
	"sharedaccesssignature", "signingkey", "hmackey", "encryptionkey", "keystorekey",
	"jwt", "cookie", "userinfo", "jsessionid",
}

// wholeSegmentKeys name a credential only as a whole last segment (round
// 11): pass (CONNECT_PASS, db.pass, not bypass) and sig (a SAS signature).
var wholeSegmentKeys = map[string]bool{"pass": true, "sig": true}

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

// keySeparators are removed from a key before it is compared with the
// pagination, switch and resume names (built once: round 11).
var keySeparators = strings.NewReplacer(".", "", "_", "", "-", "")

// credentialKeyKind classifies a key. The pagination, switch and resume
// names are matched with every separator removed (nextPageToken,
// next_page_token, resume.token). A command-line flag's dashes
// (--password) and a system property's -D are no part of the key.
func credentialKeyKind(key string) keyKind {
	flag := strings.HasPrefix(key, "-")
	if len(key) > 2 && strings.HasPrefix(key, "-D") {
		key = key[2:] // a Java system property: -Dpassword=
	}
	// A flag's one or two dashes are no part of the key; a key with more
	// is never an exempt name (round 12, F5).
	key = strings.TrimPrefix(strings.TrimPrefix(key, "-"), "-")
	exempt := !strings.HasPrefix(key, "-")
	key = strings.TrimLeft(key, "-")
	l := strings.ToLower(key)
	norm := keySeparators.Replace(l)
	switch {
	case exempt && cursorKeys[norm]:
		return credCursor
	case exempt && switchKeys[norm]:
		return credSwitch
	case exempt && norm == "resumetoken":
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
	if len(segs) > 2 {
		cands = append(cands, strings.ToLower(segs[len(segs)-3])+cands[1])
	}
	if strings.HasSuffix(last, "Pwd") || strings.HasSuffix(last, "PWD") {
		return credPath // dbPwd, DBPWD, OLDPWD, PWD
	}
	if (strings.HasSuffix(last, "Pw") || strings.HasSuffix(last, "PW")) && len(last) > 2 {
		return credPlain // dbPw, DBPW
	}
	if cands[0] == "pass" && len(segs) == 1 && !flag {
		return credPass // never a flag: --pass 482193 (round 13, C)
	}
	for _, c := range cands {
		if (strings.HasSuffix(c, "sessionid") && !strings.HasSuffix(c, "jsessionid")) ||
			(strings.HasSuffix(c, "session") && !strings.HasSuffix(c, "jsession")) {
			return credSessionID
		}
	}
	if wholeSegmentKeys[cands[0]] {
		return credPlain
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

// credentialKeyRE is a key and its separator on a line. The separator
// says the form (keySepForm): key=value, key: value, key => value, key :=
// value, a quoted key with = or : ("password" = "x", 'password': 'x'), a
// tab, "key is value", "key [value]", "key 'value'", a CSV field
// (u,password,value) or a flag's space (--password value). A double-quoted
// key with : is JSON's (credentialJSONKeyRE).
var credentialKeyRE = regexp.MustCompile(`([A-Za-z0-9._-]+)(["']?[ \t]*(?:=>|:=|=|:)[ \t]*|\t+| +is +| +\[| +["']| +|,)`)

// keyValueStartRE is a key and an = or : at the start of a text: what may
// follow a kept value (cleanAfter).
var keyValueStartRE = regexp.MustCompile(`^["']?([A-Za-z0-9._-]+)["']?[ \t]*(?:=>|:=|=|:)`)

// mysqlLineRE names a mysql client (mysql, mysqldump, mysqladmin) on a
// line, and mysqlPasswordRE is a -p<password> after it: every -p<value>
// on the line, case-sensitive (round 13, D: -P3306 is the port; mysql's
// own options that start with p are long ones, --port, so -port is
// masked too).
var (
	mysqlLineRE     = regexp.MustCompile(`(?i)\bmysql[a-z]*\b`)
	mysqlPasswordRE = regexp.MustCompile(`\s-p(?:'([^'\n]*)'|"([^"\n]*)"|([^\s'"]+))`)
)

// credentialJSONKeyRE is a JSON object key and its colon at any escaping
// level: "key":, \"key\": (JSON in a JSON string), \\\"key\\\": (JSON in
// a JSON string in a JSON string: round 10). The quotes' backslash runs
// must match (credentialKeySpans).
var credentialJSONKeyRE = regexp.MustCompile(`(\\*")([A-Za-z0-9._-]+)(\\*")(\s*:\s*)`)

// jsonEnvValueRE is what follows an env entry's "name":"<key>" before
// its value: ,"value": at the same escaping level (round 11).
var jsonEnvValueRE = regexp.MustCompile(`^\s*,\s*(\\*")value(\\*")\s*:\s*`)

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

// keptIsWords are the words "password is ..." may go on with and stay
// (round 11): what a message says about a credential, not the credential.
var keptIsWords = map[string]bool{
	"required": true, "missing": true, "invalid": true, "incorrect": true, "wrong": true, "expired": true,
	"empty": true, "not": true, "too": true, "set": true, "unset": true, "null": true, "none": true, "blank": true,
	"correct": true, "valid": true, "ok": true, "mandatory": true, "optional": true, "configured": true,
	"present": true, "absent": true, "closed": true, "being": true, "now": true, "no": true, "a": true, "an": true, "the": true,
	"stored": true, "provided": true, "given": true, "changed": true, "rotated": true, "deprecated": true,
}

// existingMaskRE is a mask redact wrote: a read of text already masked
// leaves it as it is (round 12).
var existingMaskRE = regexp.MustCompile(`^\[redacted( [0-9]+| private key [0-9]+)?\]$`)

var (
	numberRE      = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)
	digitsRE      = regexp.MustCompile(`^[0-9]+$`)
	cursorValueRE = regexp.MustCompile(`^\S{1,64}$`)
	absPathRE     = regexp.MustCompile(`^/\S*$`)
)

// keptValue reports whether one value of a credential key (a token of a
// key=value line, or a JSON value) is no secret for its kind. Common to
// every kind (trailing space and one pair of quotes aside): Kafka's own
// "[hidden]", one ${...} reference, the runner's Secret object's name, and
// MySQL's YES) or NO). A value holding another credential key and its
// separator is never kept (round 11: a kept value never hides a key).
func keptValue(kind keyKind, v string) bool {
	t := strings.TrimRight(v, " \t\r")
	// Inside a ${provider:path:key} reference a colon is the provider's;
	// only key= counts there.
	if credentialKeyIn(t, !strings.Contains(t, "${")) >= 0 {
		return false
	}
	if u := strings.TrimRight(t, `"'`); u == "YES)" || u == "NO)" {
		return true
	}
	if len(t) >= 2 && (t[0] == '"' || t[0] == '\'') && t[len(t)-1] == t[0] {
		t = t[1 : len(t)-1]
	}
	lt := strings.ToLower(t)
	if t == "[hidden]" || t == credentialMask || existingMaskRE.MatchString(t) || keptRefRE.MatchString(t) || runnerSecretNameRE.MatchString(t) {
		return true
	}
	switch kind {
	case credSwitch:
		return lt == "true" || lt == "false" || lt == "none" || lt == "null" || lt == "enabled" || lt == "disabled" || numberRE.MatchString(t)
	case credCursor:
		return keptLiterals[lt] || cursorValueRE.MatchString(t)
	case credResume:
		return cursorValueRE.MatchString(t)
	case credAuth:
		return keptLiterals[lt] || lt == "true" || lt == "false" || lt == "enabled" || lt == "disabled" || authMechanisms[lt]
	case credPath:
		return keptLiterals[lt] || absPathRE.MatchString(t)
	case credPass:
		return keptLiterals[lt] || numberRE.MatchString(t)
	case credSessionID:
		return keptLiterals[lt] || digitsRE.MatchString(t)
	}
	return keptLiterals[lt]
}

// keyWindow and keyOverlap bound each credentialKeyRE search (findKey):
// the regexp's cost per call grows with its input, so a line of many short
// keys searched to its end each time is quadratic (round 11).
const (
	keyWindow  = 512
	keyOverlap = 128
)

// findKey is credentialKeyRE's first match in s at or after off, its
// indices into s, searched keyWindow bytes at a time: a window that
// starts inside a key goes back to the key's start, and one that ends
// inside a match is widened.
func findKey(s string, off int) []int {
	win := keyWindow
	for off < len(s) {
		end := min(len(s), off+win)
		m := credentialKeyRE.FindStringSubmatchIndex(s[off:end])
		if m == nil {
			if end == len(s) {
				return nil
			}
			off, win = end-keyOverlap, keyWindow
			continue
		}
		if start := off + m[0]; start > 0 && isKeyByte(s[start-1]) {
			// The window began inside a longer key: classify it from
			// its real start (round 12, F5: not ---...nexttoken's tail).
			for start > 0 && isKeyByte(s[start-1]) {
				start--
			}
			// The window from there must reach past this match.
			win = max(win, off+m[1]-start+keyWindow)
			off = start
			continue
		}
		if m[1] == end-off && end < len(s) {
			// The match may go on past the window (a long key, or blanks
			// before its value): search again from its start, wider
			// (round 12, F4).
			off += m[0]
			win *= 2
			continue
		}
		for i := range m {
			if m[i] >= 0 {
				m[i] += off
			}
		}
		return m
	}
	return nil
}

// isKeyByte reports whether c may be part of a key (credentialKeyRE).
func isKeyByte(c byte) bool {
	return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '.' || c == '_' || c == '-'
}

// credentialKeyIn is the offset in s of the first credential key followed
// by = (or by :, when colon is set), or -1.
func credentialKeyIn(s string, colon bool) int {
	seps := "="
	if colon {
		seps = "=:"
	}
	if strings.IndexAny(s, seps) < 0 {
		return -1
	}
	// One match at a time: the first credential key ends the search
	// (round 11: never all the matches of a long token first).
	for off := 0; off < len(s); {
		m := findKey(s, off)
		if m == nil {
			break
		}
		if sep := s[m[4]:m[5]]; keySepForm(sep) == formNormal && strings.ContainsAny(sep, seps) && credentialKeyKind(s[m[2]:m[3]]) != notCredential {
			return m[2]
		}
		off = m[1]
	}
	return -1
}

// maxContinuation bounds how many lines a continued or open-quoted value
// is followed into; a value still open after it is masked to the end of
// the text (round 11).
const maxContinuation = 64

// sepForm is the form a credentialKeyRE separator stands for.
type sepForm int

const (
	formNormal  sepForm = iota // = : => := or a tab
	formJSON                   // "key": (the JSON pass's)
	formIs                     // key is value
	formBracket                // key [value]
	formQuote                  // key 'value' (SQL)
	formSpace                  // --key value
	formCSV                    // ,key,value
)

func keySepForm(sep string) sepForm {
	switch {
	case sep == ",":
		return formCSV
	case strings.Trim(sep, " ") == "":
		return formSpace
	case strings.HasSuffix(sep, "[") && strings.Trim(sep, " [") == "":
		return formBracket
	case strings.Trim(sep, " ") == "is":
		return formIs
	case sep[0] == ' ' && (strings.HasSuffix(sep, `"`) || strings.HasSuffix(sep, "'")) && strings.Trim(sep, ` "'`) == "":
		return formQuote
	case sep[0] == '"' && strings.Contains(sep, ":") && !strings.Contains(sep, "="):
		return formJSON
	}
	return formNormal
}

// credentialKeySpans are the ranges of the values of every credential key
// in t. Invisible format characters (zero-width spaces and joiners, the
// byte order mark, bidi controls: Unicode Cf) and U+FFFD are removed
// before keys are matched, and the ranges mapped back onto t (round 11).
func credentialKeySpans(t string) [][2]int {
	s, pos := stripFormatChars(t)
	ks := &keyScan{t: s, pos: pos, kinds: map[string]keyKind{}}
	ks.linePass(ks.jsonPass())
	if pos != nil {
		for i, sp := range ks.spans {
			ks.spans[i] = [2]int{pos[sp[0]], pos[sp[1]]}
		}
	}
	return ks.spans
}

// stripFormatChars is t without its Unicode format (Cf) characters, and
// for each byte of it (and its end) the offset in t it came from; pos is
// nil when t holds none.
func stripFormatChars(t string) (string, []int) {
	if !hasFormatChar(t) {
		return t, nil
	}
	var b strings.Builder
	pos := make([]int, 0, min(len(t), redact.MaxScan)+1)
	for i := 0; i < len(t); {
		r, n := utf8.DecodeRuneInString(t[i:])
		if isKeyNoise(r, n) {
			i += n
			continue
		}
		b.WriteString(t[i : i+n])
		for j := 0; j < n; j++ {
			pos = append(pos, i+j)
		}
		i += n
	}
	pos = append(pos, len(t))
	return b.String(), pos
}

// isKeyNoise reports whether the rune r of n bytes is removed before keys
// are matched: a format (Cf) character, or U+FFFD itself (an invalid byte
// a JSON decoder replaced: pass\ufffdword, as pass_word in a log).
func isKeyNoise(r rune, n int) bool {
	return (r == utf8.RuneError && n == 3) || (r != utf8.RuneError && unicode.Is(unicode.Cf, r))
}

// hasFormatChar reports whether t holds a character isKeyNoise removes.
func hasFormatChar(t string) bool {
	for i := 0; i < len(t); i++ {
		if t[i] < 0x80 {
			continue
		}
		r, n := utf8.DecodeRuneInString(t[i:])
		if isKeyNoise(r, n) {
			return true
		}
		i += n - 1
	}
	return false
}

// keyScan is one credentialKeySpans pass over t.
type keyScan struct {
	t     string
	pos   []int // stripFormatChars' map back onto the text, or nil
	spans [][2]int
	kinds map[string]keyKind
	// maskedLine and maskedTo: the start of the line a continued value was
	// masked from, and its end; the lines in between need no scan.
	maskedLine, maskedTo int
	// csvCol, csvFields and csvKind: the credential column of the last
	// CSV header seen, its field count and its key's kind (csvLine).
	csvCol, csvFields int
	csvKind           keyKind
	csvHeader         bool // the line being scanned is a CSV header
	// stopFrom, stopAt, stopLe cache tokenAt: the first stop byte at or
	// after any offset in [stopFrom, stopAt] of the line ending at stopLe
	// is at stopAt (round 11: pw=pw=pw=... scans the line once).
	stopFrom, stopAt, stopLe int
}

// tokenAt is the end of the token starting at t[a] on the line ending at
// le (tokenLen), found once per run of tokens sharing it.
func (ks *keyScan) tokenAt(a, le int) int {
	if le == ks.stopLe && ks.stopFrom <= a && a <= ks.stopAt {
		return ks.stopAt
	}
	ks.stopFrom, ks.stopAt, ks.stopLe = a, a+tokenLen(ks.t[a:le]), le
	return ks.stopAt
}

func (ks *keyScan) kind(key string) keyKind {
	if k, ok := ks.kinds[key]; ok {
		return k
	}
	k := credentialKeyKind(key)
	if len(ks.kinds) < 4096 && len(key) <= 128 {
		ks.kinds[key] = k
	}
	return k
}

// kindAt is the kind of the key t[a:b]. A key that had a format character
// removed from it is never an exempt or partly exempt name: a credential
// key of any kind is a plain one (round 12, F6: page\u200btoken is no
// cursor).
func (ks *keyScan) kindAt(key string, a, b int) keyKind {
	k := ks.kind(key)
	if k != notCredential && ks.stripped(a, b) {
		return credPlain
	}
	return k
}

// stripped reports whether a format character was removed from t[a:b].
func (ks *keyScan) stripped(a, b int) bool {
	return ks.pos != nil && b > a && ks.pos[b-1]-ks.pos[a] != b-1-a
}

func (ks *keyScan) add(a, b int) {
	if b > a {
		ks.spans = append(ks.spans, [2]int{a, b})
	}
}

// jsonPass masks a JSON value of any type at any escaping level, and an
// env entry's value whose name is a credential key ({"name":"DB_PASSWORD",
// "value":"..."}: round 11). It returns every value it judged: a key found
// inside one later is part of it.
func (ks *keyScan) jsonPass() [][2]int {
	t := ks.t
	var values [][2]int
	done, lineEnd := 0, -1
	for _, m := range credentialJSONKeyRE.FindAllStringSubmatchIndex(t, -1) {
		if m[0] < done {
			continue
		}
		open, closeQ := t[m[2]:m[3]], t[m[6]:m[7]]
		if open != closeQ {
			continue
		}
		key := t[m[4]:m[5]]
		kind := ks.kindAt(key, m[4], m[5])
		isName := kind == notCredential && strings.EqualFold(key, "name")
		if kind == notCredential && !isName {
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
		if isName {
			ia, ib, quoted := jsonStringInner(t, a, b, open)
			if !quoted {
				continue
			}
			kind = ks.kind(unescapeLevel(t[ia:ib], k))
			mm := jsonEnvValueRE.FindStringSubmatchIndex(t[b:lineEnd])
			if kind == notCredential || mm == nil || t[b+mm[2]:b+mm[3]] != open || t[b+mm[4]:b+mm[5]] != open {
				continue
			}
			a = b + mm[1]
			b = jsonValueEnd(t, a, lineEnd, k)
		}
		done = b
		values = append(values, [2]int{a, b})
		ia, ib, _ := jsonStringInner(t, a, b, open)
		if ib > ia && !keptValue(kind, unescapeLevel(t[ia:ib], k)) {
			ks.add(ia, ib)
		}
	}
	return values
}

// jsonStringInner is a JSON value's range without its quotes, if it is a string.
func jsonStringInner(t string, a, b int, q string) (int, int, bool) {
	if strings.HasPrefix(t[a:b], q) && strings.HasSuffix(t[a:b], q) && b-a >= 2*len(q) {
		return a + len(q), b - len(q), true
	}
	return a, b, false
}

// envName is the credential kind of the last env entry name seen
// (name: DB_PASSWORD), and its line: a value key on that line or the next
// is that credential's value (round 11).
type envName struct {
	kind keyKind
	line int
}

// linePass judges every key=value of every line, left to right, the way
// keyValue says.
func (ks *keyScan) linePass(values [][2]int) {
	t := ks.t
	env := envName{line: -2}
	ks.maskedLine, ks.maskedTo = -1, -1
	ks.csvCol = -1
	for ls, n := 0, 0; ls < len(t); n++ {
		le := strings.IndexByte(t[ls:], '\n')
		if le < 0 {
			le = len(t)
		} else {
			le += ls
		}
		if ls > ks.maskedLine && le <= ks.maskedTo {
			ls = le + 1
			continue
		}
		ks.line(ls, le, n, values, &env)
		ls = le + 1
	}
}

// line judges the keys of the line t[ls:le] in order. Each key's value is
// one token (to whitespace or a separator, cut before any credential key:
// round 11) or one quoted string. A kept token is left and the scan goes
// on after it, as it does after a masked pagination or switch token (F1)
// or a masked quoted string; any other credential's value is masked to
// the line's end and followed into the next lines while it is continued
// (continuedEnd). A kept or masked token must be followed by the line's
// end, a separator or another key; anything else is masked with the rest
// of the line, the token too (none Zq9...).
func (ks *keyScan) line(ls, le, n int, values [][2]int, env *envName) {
	t := ks.t
	line := t[ls:le]
	if strings.Contains(line, "-p") {
		if l := mysqlLineRE.FindStringIndex(line); l != nil {
			for _, m := range mysqlPasswordRE.FindAllStringSubmatchIndex(line[l[1]:], -1) {
				for g := 2; g <= 6; g += 2 {
					if m[g] >= 0 {
						ks.add(ls+l[1]+m[g], ls+l[1]+m[g+1])
					}
				}
			}
		}
	}
	ks.csvLine(ls, le)
	for pos := 0; pos < len(line); {
		m := findKey(line, pos)
		if m == nil {
			return
		}
		pos = m[1]
		key, sep := line[m[2]:m[3]], line[m[4]:m[5]]
		form := keySepForm(sep)
		if inSpans(values, ls+m[2]) {
			// A key inside a credential's JSON value is part of that
			// value, which the JSON pass already decided.
			continue
		}
		if form == formJSON {
			// The JSON pass decided it, unless it is a YAML key in
			// double quotes whose value is the block below ("password":
			// |, round 13, E).
			if yamlBlockValue(line[m[1]:]) && yamlKeyStart(line[:m[2]]) && ks.kindAt(key, ls+m[2], ls+m[3]) != notCredential {
				ks.yamlBlock(ls, le)
				return
			}
			continue
		}
		kind := ks.kindAt(key, ls+m[2], ls+m[3])
		if strings.EqualFold(key, "token") && form == formNormal && strings.Contains(sep, ":") && resumeBefore(line, m[2]) && !ks.stripped(ls+max(0, m[2]-7), ls+m[3]) {
			kind = credResume // "resume token: <position>"
		}
		if kind == notCredential {
			switch {
			case form == formNormal && strings.EqualFold(key, "name"):
				if v := ks.firstValue(ls+m[1], le); len(v) <= 128 && ks.kind(v) != notCredential {
					k := ks.kind(v)
					*env = envName{k, n}
				}
				continue
			case form == formNormal && strings.EqualFold(key, "value") && env.kind != notCredential && n-env.line <= 1:
				kind = env.kind
			default:
				continue
			}
		}
		vs := m[1]
		switch form {
		case formIs:
			w := line[vs:]
			w = strings.TrimRight(w[:tokenLen(w)], ".,;:!)\r")
			if keptIsWords[strings.ToLower(w)] || keptValue(kind, w) {
				continue
			}
			ks.maskRest(ls, ls+vs, le)
			return
		case formBracket:
			c := strings.IndexByte(line[vs:], ']')
			if c < 0 {
				ks.maskRest(ls, ls+vs, le)
				return
			}
			if v := line[vs : vs+c]; !keptValue(kind, v) && !keptValue(kind, "["+v+"]") {
				ks.add(ls+vs, ls+vs+c)
			}
			pos = vs + c + 1
			continue
		case formCSV:
			// A credential key as a CSV cell: the next cell is its value,
			// unless the line is a header (csvLine), whose column is
			// masked in the rows under it (round 13).
			if ks.csvHeader || vs >= len(line) || (m[2] > 0 && line[m[2]-1] != ',' && line[m[2]-1] != ' ') {
				continue
			}
			e := strings.IndexByte(line[vs:], ',')
			if e < 0 {
				e = len(line) - vs
			}
			if v := strings.TrimRight(line[vs:vs+e], "\r"); strings.TrimSpace(v) != "" && !keptValue(kind, strings.TrimSpace(v)) {
				ks.add(ls+vs, ls+vs+len(v))
			}
			pos = vs + e
			continue
		case formSpace:
			if !strings.HasPrefix(key, "-") {
				if kind == credPass {
					// "pass 482193": a long number after a bare pass
					// is a PIN, not a test count (round 13, C).
					if w := line[vs : vs+tokenLen(line[vs:])]; longDigitsRE.MatchString(strings.TrimRight(w, "\r")) {
						ks.add(ls+vs, ls+vs+len(w))
					}
				}
				continue
			}
		case formQuote:
			if key == "token" && jacksonBefore(line[:m[2]]) && jacksonShapeRE.MatchString(line) {
				// Jackson's "Unrecognized token 'x': was expecting" or
				// "Unexpected token ... at [Source:" quotes the text it
				// could not parse, which may be the credential: only a
				// JSON keyword or punctuation is skipped, that one
				// quoted token (round 13, B).
				if j := closingQuote(line[vs-1:], line[vs-1]); j > 0 && jsonKeywordValue(line[vs:vs-1+j]) {
					pos = vs + j
					continue
				}
			}
			vs-- // the value starts at its quote
		}
		yaml := form == formNormal && strings.Contains(sep, ":") && !strings.Contains(sep, "=") && yamlKeyStart(line[:m[2]])
		next, stop := ks.keyValue(kind, ls, le, vs, yaml)
		if stop {
			return
		}
		pos = next
	}
}

// jacksonShapeRE is the rest of a Jackson parse error's message.
var jacksonShapeRE = regexp.MustCompile(`': was expecting|" ?: was expecting|at \[Source: `)

// longDigitsRE is a number of six or more digits.
var longDigitsRE = regexp.MustCompile(`^[0-9]{6,}$`)

// jsonKeywordValue reports whether v, the text a Jackson parse error
// quotes, is no secret: a JSON keyword or one character (}, ]).
func jsonKeywordValue(v string) bool {
	switch v {
	case "null", "true", "false":
		return true
	}
	return utf8.RuneCountInString(v) <= 1
}

// jacksonBefore reports whether a token key follows Jackson's own
// "Unrecognized " or "Unexpected " (capitalized, as Jackson writes it).
func jacksonBefore(before string) bool {
	return strings.HasSuffix(before, "Unrecognized ") || strings.HasSuffix(before, "Unexpected ")
}

// csvHeaderFieldRE is a CSV header cell: a name, no blanks or colons
// (round 13: not "keys: user, password, host"). The first cell may carry
// a log prefix ("build: user").
var (
	csvHeaderFieldRE = regexp.MustCompile(`^["']?[A-Za-z0-9._-]+["']?$`)
	csvPrefixRE      = regexp.MustCompile(`^[A-Za-z0-9._-]+: `)
)

// csvHeaderWords are the names a CSV header's other cells may be; a
// header is a line of them and credential keys only, so a value after a
// credential key makes the line a row (round 13).
var csvHeaderWords = map[string]bool{
	"user": true, "username": true, "user_name": true, "login": true, "host": true, "hostname": true, "port": true,
	"database": true, "db": true, "dbname": true, "schema": true, "table": true, "name": true, "id": true, "key": true,
	"value": true, "type": true, "kind": true, "role": true, "email": true, "url": true, "uri": true, "address": true,
	"created": true, "updated": true, "created_at": true, "updated_at": true, "comment": true, "description": true,
	"enabled": true, "status": true, "owner": true, "group": true, "account": true, "tenant": true, "env": true,
	"environment": true, "service": true, "server": true, "cluster": true, "namespace": true, "topic": true, "note": true,
}

// csvLine masks, in a row under a CSV header with a credential column
// (user,password,host), that row's cell of the column: a row has as many
// fields as the header, and any other line ends the table. A header is a
// line of known header words and credential keys only (round 13); in any
// other line a credential key's next cell is masked (formCSV).
func (ks *keyScan) csvLine(ls, le int) {
	ks.csvHeader = false
	line := strings.TrimRight(ks.t[ls:le], "\r")
	if strings.IndexByte(line, ',') < 0 || len(line) > 4096 {
		ks.csvCol = -1
		return
	}
	fields := strings.Split(line, ",")
	if ks.csvCol >= 0 && len(fields) == ks.csvFields {
		a := ls
		for _, f := range fields[:ks.csvCol] {
			a += len(f) + 1
		}
		if v := fields[ks.csvCol]; strings.TrimSpace(v) != "" && !keptValue(ks.csvKind, strings.TrimSpace(v)) {
			ks.add(a, a+len(v))
		}
		return
	}
	col, kind, header := ks.csvHeaderLine(ls, fields)
	if !header {
		if ks.csvCol >= 0 {
			// A row under a credential header with another field count
			// (a quoted comma, an extra cell): its cells from the
			// column's on are masked (round 13, A).
			a := ls
			for _, f := range fields[:min(ks.csvCol, len(fields)-1)] {
				a += len(f) + 1
			}
			ks.add(a, ls+len(line))
		}
		return
	}
	ks.csvHeader = true
	ks.csvCol = -1
	if col >= 0 {
		ks.csvCol, ks.csvFields, ks.csvKind = col, len(fields), kind
	}
}

// csvHeaderLine reports whether the CSV line of fields, which starts at
// ls, is a header, and its first credential column and that key's kind.
// A cell's kind is kindAt's (round 13, A: page\u200btoken is no cursor).
func (ks *keyScan) csvHeaderLine(ls int, fields []string) (int, keyKind, bool) {
	col, kind := -1, notCredential
	a := ls
	for i, f := range fields {
		fa := a
		a += len(f) + 1
		if i == 0 {
			if p := csvPrefixRE.FindString(f); p != "" {
				f, fa = f[len(p):], fa+len(p)
			}
		}
		if !csvHeaderFieldRE.MatchString(f) {
			return -1, notCredential, false
		}
		q := 0
		if f[0] == '"' || f[0] == '\'' {
			q = 1
		}
		name := strings.Trim(f, `"'`)
		k := ks.kindAt(name, fa+q, fa+q+len(name))
		if k == notCredential && !csvHeaderWords[strings.ToLower(name)] {
			return -1, notCredential, false // a value: the line is a row
		}
		if k != notCredential && col < 0 {
			col, kind = i, k
		}
	}
	return col, kind, true
}

// yamlKeyStart reports whether the text before a key is a YAML line's
// indentation (and a list item's "- ", or outer keys): the key starts a
// YAML mapping entry, not a message ending in "token:" (round 12).
func yamlKeyStart(before string) bool {
	return yamlPrefixRE.MatchString(before)
}

// yamlPrefixRE is what may come before a YAML key on its line:
// indentation (tabs too), a list item's "- ", a flow mapping's "{", outer
// keys ("build: password:") and the key's own opening quote (round 13,
// E: 'password': |).
var yamlPrefixRE = regexp.MustCompile(`^[ \t]*(?:- +|\{ *|["']?[A-Za-z0-9._-]+["']?: +)*["']?$`)

// yamlBlockValue reports whether v, a YAML key's value on its line, is
// empty or a block scalar's indicator (|, >, |-, >+).
func yamlBlockValue(v string) bool {
	v = strings.TrimRight(v, " \t\r")
	if v == "" {
		return true
	}
	return (v[0] == '|' || v[0] == '>') && strings.Trim(v[1:], "-+123456789") == ""
}

// resumeBefore reports whether "resume " (a word: not "presume") ends
// line[:i].
func resumeBefore(line string, i int) bool {
	if i < 7 || !strings.EqualFold(line[i-7:i], "resume ") {
		return false
	}
	return i == 7 || !('a' <= line[i-8]|0x20 && line[i-8]|0x20 <= 'z')
}

// tokenLen is the length of the token v starts with: to whitespace, a
// list separator or a quote (x"y is x, then "y).
func tokenLen(v string) int {
	if i := strings.IndexAny(v, " \t\r,;&|\"'"); i >= 0 {
		return i
	}
	return len(v)
}

// firstValue is the value t[a:le] starts with: a quoted string without
// its quotes, or a token.
func (ks *keyScan) firstValue(a, le int) string {
	v := ks.t[a:le]
	if v != "" && (v[0] == '"' || v[0] == '\'') {
		// An env name is short: a quote not closed soon names no key.
		if j := closingQuote(v[:min(len(v), 256)], v[0]); j > 0 {
			return v[1:j]
		}
		return ""
	}
	return ks.t[a:ks.tokenAt(a, le)]
}

// keyValue judges the value of a credential key of kind that starts at
// line offset vs on the line t[ls:le]. yaml is set for a key: separator,
// whose empty or block-scalar value (|, >) is the indented lines below
// (round 11). It returns where the scan of the line goes on, or stop when
// the rest of the line is masked or done.
func (ks *keyScan) keyValue(kind keyKind, ls, le, vs int, yaml bool) (int, bool) {
	t := ks.t
	line := t[ls:le]
	v := strings.TrimRight(line[vs:], "\r")
	if tv := strings.TrimRight(v, " \t"); tv == "" || (yaml && yamlBlockValue(tv)) {
		if yaml {
			ks.yamlBlock(ls, le)
		}
		return len(line), true
	}
	var tok string
	quoted := v[0] == '"' || v[0] == '\''
	if quoted {
		j := closingQuote(v, v[0])
		if j < 0 {
			ks.maskRest(ls, ls+vs, le)
			return 0, true
		}
		tok = v[:j+1]
	} else if j := strings.IndexByte(v, ']'); v[0] == '[' && j > 0 && keptValue(kind, v[:j+1]) {
		tok = v[:j+1] // [hidden], or a mask already in the text (round 12)
	} else if j := strings.IndexByte(v, '}'); strings.HasPrefix(v, "${") && j > 0 {
		tok = v[:j+1] // a reference: keptValue judges it whole
	} else {
		tok = v[:min(len(v), ks.tokenAt(ls+vs, le)-ls-vs)]
		if i := credentialKeyIn(tok, true); i >= 0 {
			tok = tok[:i]
		}
	}
	end := vs + len(tok)
	switch {
	case tok == "":
		return vs, false
	case keptValue(kind, tok):
		if kind == credResume {
			// The position is kept; anything after it on the line is not.
			if r := strings.TrimLeft(line[end:], " \t"); strings.TrimRight(r, "\r") != "" {
				ks.maskRest(ls, le-len(r), le)
			}
			return 0, true
		}
	case kind == credSwitch || kind == credCursor || (quoted && kind != credResume):
		ks.add(ls+vs, ls+end)
	default:
		ks.maskRest(ls, ls+vs, le)
		return 0, true
	}
	if !cleanAfter(line[end:]) {
		// A kept word followed by more is no longer kept (none Zq9...).
		ks.maskRest(ls, ls+vs, le)
		return 0, true
	}
	return end, false
}

// closingQuote is the index in v of the unescaped quote q that closes the
// one v starts with, or -1.
func closingQuote(v string, q byte) int {
	for i := 1; i < len(v); i++ {
		if v[i] == '\\' {
			i++
			continue
		}
		if v[i] == q {
			return i
		}
	}
	return -1
}

// cleanAfter reports whether r, the text after a judged token, starts a
// new item: closing brackets and quotes, then the line's end or another
// key= or key: (right away when the token was cut before it; after blanks;
// after one list separator and blanks; or right after & or |, a query
// string's). A separator followed by anything else is more of the value
// (round 12, F2: null,Zq9...), and so is a separator followed by a
// closing bracket or, for , and ;, a key that names no credential with no
// blank between (round 13, F: null,)Zq9..., null,X=y).
func cleanAfter(r string) bool {
	r = strings.TrimRight(r, "\r")
	r = strings.TrimLeft(r, ")]}\"'")
	if r == "" || keyValueStartRE.MatchString(r) {
		return true
	}
	switch r[0] {
	case ',', ';':
		r = r[1:]
		if r == "" {
			return true
		}
		if r[0] != ' ' && r[0] != '\t' {
			// Right after the separator, only another credential key
			// starts a new item (PWD=/opt,masterauth=...): its own
			// value is judged on its own.
			m := keyValueStartRE.FindStringSubmatch(r)
			return m != nil && credentialKeyKind(m[1]) != notCredential
		}
	case '&', '|':
		r = r[1:]
	case ' ', '\t':
	default:
		return false
	}
	r = strings.TrimLeft(r, " \t")
	return r == "" || keyValueStartRE.MatchString(r)
}

// maskRest masks t[a:] to the end of its value (continuedEnd) from the
// line starting at ls that ends at le.
func (ks *keyScan) maskRest(ls, a, le int) {
	end := continuedEnd(ks.t, a, le)
	ks.add(a, end)
	ks.maskedLine, ks.maskedTo = ls, end
}

// yamlBlock masks the lines after the key line t[ls:le] that are indented
// deeper than it (blank lines among them): a YAML block scalar or a value
// on the next line (password:\n  <value>).
func (ks *keyScan) yamlBlock(ls, le int) {
	t := ks.t
	indent := func(s string) int { return len(s) - len(strings.TrimLeft(s, " \t")) }
	ind := indent(t[ls:le])
	first, last := -1, -1
	for p := le + 1; p < len(t); {
		q := strings.IndexByte(t[p:], '\n')
		if q < 0 {
			q = len(t)
		} else {
			q += p
		}
		l := strings.TrimRight(t[p:q], "\r")
		if strings.TrimSpace(l) != "" {
			// Every deeper line is the credential's, tab-indented or
			// shaped like a stack frame too (round 13): the key starting
			// its line (yamlKeyStart) is what tells a block from a
			// message ending in "token:".
			if indent(l) <= ind {
				break
			}
			if first < 0 {
				first = p + indent(l)
			}
			last = p + len(l)
		}
		p = q + 1
	}
	if first >= 0 {
		ks.add(first, last)
		ks.maskedLine, ks.maskedTo = ls, last
	}
}

// continuedEnd is the end of a key's value that starts at t[vs] on the
// line ending at le: the line's end, or a later line's while the value is
// in an open quote (one it starts with, or a " opened inside it after a
// blank or separator: round 11), or the line ends in a backslash or in a %
// or %X that a split %XX continues (round 10). A value still open after
// maxContinuation lines is masked to the end of t (round 11).
func continuedEnd(t string, vs, le int) int {
	end := le
	cur := t[vs:le]
	q := quoteState(cur, 0, true)
	for n := 0; end < len(t); n++ {
		c := strings.TrimRight(cur, "\r")
		cont := strings.HasSuffix(c, `\`) || strings.HasSuffix(c, "%") || (len(c) >= 2 && c[len(c)-2] == '%' && isHex(c[len(c)-1]))
		if q == 0 && !cont {
			break
		}
		if n == maxContinuation {
			return len(t)
		}
		next := strings.IndexByte(t[end+1:], '\n')
		if next < 0 {
			next = len(t)
		} else {
			next += end + 1
		}
		cur = t[end+1 : next]
		end = next
		q = quoteState(cur, q, false)
	}
	return end
}

// quoteState is the quote left open at the end of s when q is open at its
// start. A " opens at the start or after a blank or separator (x "y, not
// the closing quote of an outer string: abc"); a ' only as the first byte
// of a value (first).
func quoteState(s string, q byte, first bool) byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if q != 0 {
			if c == '\\' {
				i++
			} else if c == q {
				q = 0
			}
			continue
		}
		if c == '"' && (i == 0 || strings.IndexByte(" \t=:([{,", s[i-1]) >= 0) || c == '\'' && first && i == 0 {
			q = c
		}
	}
	return q
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
		// A container env entry whose name is a credential key holds the
		// credential in its value (round 11).
		envValue := false
		if name, ok := t["name"].(string); ok {
			if kind := credentialKeyKind(name); kind != notCredential {
				if v, ok := t["value"].(string); ok && !keptValue(kind, v) {
					t["value"] = credentialMask
					envValue = true
				}
			}
		}
		for k, x := range t {
			if envValue && k == "value" {
				continue
			}
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
