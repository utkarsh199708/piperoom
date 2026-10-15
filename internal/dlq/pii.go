package dlq

import (
	"regexp"
	"strings"

	"piperoom/internal/redact"
)

// Unquoted values a converter or driver may print from a record or a
// connection string. The quote and group rules of scrub never see them, so
// "failed for jane@example.com" or "password=hunter2" went out whole. Every
// pattern is bounded by \b, so offsets, LSNs (0/16B3748), dates and
// epoch-millisecond timestamps are left alone.
var (
	// The value after a credential key and = or :. internal/redact masks long
	// values only; a record's or a URL's short password must go too.
	kvSecretRe = regexp.MustCompile(`(?i)\b([a-z0-9_.-]*?(?:password|passwd|pwd|secret|token|api[_.-]?key|access[_.-]?key|private[_.-]?key|credentials?))(\s*[=:]\s*)([^\s,;&)'"]+)`)
	// A whole e-mail address (internal/redact keeps the domain).
	emailRe = regexp.MustCompile(`\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,24}\b`)
	// SSN-like: 3-2-4 digits.
	ssnRe = regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)
	// Card-like: 13-19 digits, optionally grouped by single spaces or dashes;
	// looksLikeCard decides.
	cardRe = regexp.MustCompile(`\b\d(?:[ -]?\d){12,18}\b`)
	// Phone-like: optional +country, then 3-3-4 digits with separators.
	phoneRe = regexp.MustCompile(`(?:\+\d{1,3}[ .-]?)?\(?\b\d{3}\)?[ .-]\d{3}[ .-]\d{4}\b`)
)

// maskUnquoted replaces the unquoted values above with Redacted, then runs
// internal/redact for the credentials it knows (URL user:password@, bearer
// tokens, cloud keys, JWTs). Its own "[redacted N]" marks are brackets, which
// the next pass of RedactMessage turns into Redacted.
func maskUnquoted(s string) string {
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
	sum, double := 0, false
	for i := len(digits) - 1; i >= 0; i-- {
		d := int(digits[i] - '0')
		if double {
			if d *= 2; d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return sum%10 == 0
}
