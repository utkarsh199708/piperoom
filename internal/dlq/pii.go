package dlq

import (
	"regexp"
	"strings"

	"piperoom/internal/redact"
)

// Unquoted values a converter or driver may print from a record. Every
// pattern is anchored on non-word boundaries so offsets, LSNs (0/16B3748),
// dates (2026-10-14) and epoch-millisecond timestamps are left alone.
var (
	// credential keys followed by = or : and a value up to white space or a
	// delimiter. internal/redact covers long tokens; this also takes short
	// ones ("token=xyz").
	kvSecretRe = regexp.MustCompile(`(?i)\b((?:[a-z0-9_.-]*?)(?:password|passwd|pwd|secret|token|api[_.-]?key|access[_.-]?key|private[_.-]?key|credentials?))(\s*[=:]\s*)([^\s,;&)'"]+)`)
	// e-mail addresses, whole (internal/redact keeps the domain).
	emailRe = regexp.MustCompile(`\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,24}\b`)
	// SSN-like: 3-2-4 digits.
	ssnRe = regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)
	// card-like: 13-19 digits, optionally grouped by single spaces or dashes.
	cardRe = regexp.MustCompile(`\b\d(?:[ -]?\d){12,18}\b`)
	// phone-like: optional +country, then 3-3-4 digits with separators.
	phoneRe = regexp.MustCompile(`(?:\+\d{1,3}[ .-]?)?\(?\b\d{3}\)?[ .-]\d{3}[ .-]\d{4}\b`)
	// the value after a personal-data key, unquoted: "ssn 987654321",
	// "customer_name=Jane Doe", "iban: DE89...". After = or : the value is
	// one token plus following capitalised words (a name, a street); after a
	// bare space it must hold a digit, so prose ("the name of") is kept.
	piiKVRe = regexp.MustCompile(`(?i)\b((?:[a-z0-9]+[_.-])*)(ssn|iban|name|e-?mail|phone|card|dob|address)((?:[_.-]?(?:number|num|no))?)` +
		`(?:(\s*[=:]\s*)(` + piiVal + `(?:\s+(?-i:[A-Z])` + piiCont + `)*)` +
		`|(\s+)(` + piiCont + `\d` + piiCont + `(?:\s+(?-i:[A-Z])` + piiCont + `)*))`)
)

const (
	piiVal  = `[^\s,;&)}\]'"]+`
	piiCont = `[^\s,;&)}\]'"=:]*`
)

// piiKeyNotPersonal lists key prefixes that make "name" or "address" a
// pipeline identifier ("topic_name=orders", "host.address=10.0.0.5"), which
// is evidence, not personal data.
var piiKeyNotPersonal = map[string]bool{
	"topic": true, "table": true, "column": true, "schema": true, "subject": true,
	"connector": true, "class": true, "field": true, "file": true, "host": true,
	"ip": true, "mac": true, "broker": true, "server": true, "remote": true,
	"local": true, "socket": true, "listener": true, "inet": true, "bootstrap": true,
	"db": true, "database": true, "index": true, "constraint": true, "task": true,
	"group": true, "cluster": true, "type": true, "record": true, "method": true,
	"thread": true, "node": true, "slot": true, "publication": true, "plugin": true,
	"transform": true, "converter": true, "config": true, "metric": true, "partition": true,
}

// maskPIIValue replaces the value of one piiKVRe match, keeping the key and
// separator, unless the key prefix names a pipeline object.
func maskPIIValue(m string) string {
	g := piiKVRe.FindStringSubmatch(m)
	for _, seg := range strings.FieldsFunc(strings.ToLower(g[1]), func(r rune) bool { return r == '_' || r == '.' || r == '-' }) {
		if piiKeyNotPersonal[seg] {
			return m
		}
	}
	key := g[1] + g[2] + g[3]
	if g[4] != "" || g[5] != "" {
		return key + g[4] + Redacted
	}
	return key + g[6] + Redacted
}

// maskPII replaces the unquoted values listed above with Redacted, then runs
// internal/redact for the credentials it knows (URL passwords, bearer tokens,
// cloud keys, JWTs, ...).
func maskPII(s string) string {
	s = kvSecretRe.ReplaceAllString(s, "${1}${2}"+Redacted)
	s = emailRe.ReplaceAllString(s, Redacted)
	s = ssnRe.ReplaceAllString(s, Redacted)
	s = cardRe.ReplaceAllStringFunc(s, func(m string) string {
		if looksLikeCard(m) {
			return Redacted
		}
		return m
	})
	s = phoneRe.ReplaceAllString(s, Redacted)
	s = piiKVRe.ReplaceAllStringFunc(s, maskPIIValue)
	out, _ := redact.Text(s)
	return out
}

// looksLikeCard accepts a Luhn-valid number. An ungrouped run must also have
// a card length (15 or 16) and a card network's first digit (3-6), so that a
// 13-digit epoch-millisecond timestamp or a long offset is not taken for one.
func looksLikeCard(m string) bool {
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, m)
	if len(digits) < 13 || len(digits) > 19 || !luhn(digits) {
		return false
	}
	if digits == m { // no separators
		return (len(digits) == 15 || len(digits) == 16) && digits[0] >= '3' && digits[0] <= '6'
	}
	return true
}

func luhn(digits string) bool {
	sum := 0
	double := false
	for i := len(digits) - 1; i >= 0; i-- {
		d := int(digits[i] - '0')
		if double {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return sum%10 == 0
}
