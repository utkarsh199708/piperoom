package dlq

import (
	"regexp"
	"strings"

	"piperoom/internal/redact"
)

// Unquoted values a converter or driver may print from a record or a
// connection string. The quote and group rules of scrub never see them, so
// "failed for jane@example.com" or "password=hunter2" went out whole. The
// number patterns are bounded by \b and the e-mail one needs an @, so offsets,
// LSNs (0/16B3748), dates and epoch-millisecond timestamps are left alone.
var (
	// The value after a credential key and = or :, with an optional scheme word
	// ("token: Bearer abc"). internal/redact masks long values only; a
	// record's or a URL's short password must go too.
	kvSecretRe = regexp.MustCompile(`(?i)\b([a-z0-9_.-]*?(?:password|passwd|pwd|secret|token|api[_.-]?key|access[_.-]?key|private[_.-]?key|credentials?))(\s*[=:]\s*)((?:(?:bearer|basic|token)\s+)?[^\s,;&)'"]+)`)
	// A whole e-mail address (internal/redact keeps the domain), with
	// non-ASCII letters in the local part and the domain ("jané@exämple.com").
	// No \b: it is ASCII-only in Go, and the classes already stop at a space.
	emailRe = regexp.MustCompile(`[\p{L}\p{N}._%+-]{1,128}@(?:[\p{L}\p{N}-]+\.)+\p{L}{2,24}`)
	// internal/redact keeps the first 4 and last 2 runes of a long token
	// ("sk_l…[redacted 32]…Uv"); a DLQ message keeps none of it.
	partialMaskRe = regexp.MustCompile(`[^\s…]{0,4}…\[redacted[^\]]*\]…[^\s…]{0,2}`)
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
// tokens, cloud keys, JWTs) and drops the head and tail it keeps of a long
// token. Its own "[redacted N]" marks are brackets, which the next pass of
// RedactMessage turns into Redacted.
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
	return partialMaskRe.ReplaceAllString(out, Redacted)
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

// MaskPII masks e-mail addresses and SSN-like, card-like and phone-like
// numbers, with the same rules as RedactMessage, and nothing else: no
// credential-key rule (in SQL, "token = NULL" or "token::text" is not a
// key and value), no cap, no quote or group rules. It is for text that must
// keep its shape, such as a DDL statement in the watcher's change log; the
// caller still runs internal/redact for credentials.
func MaskPII(s string) string {
	s = emailRe.ReplaceAllString(s, Redacted)
	s = ssnRe.ReplaceAllString(s, Redacted)
	s = cardRe.ReplaceAllStringFunc(s, func(m string) string {
		if looksLikeCard(m) {
			return Redacted
		}
		return m
	})
	return phoneRe.ReplaceAllString(s, Redacted)
}

// piiSpans returns the byte spans of the values maskUnquoted masks with its
// own patterns (not what internal/redact finds). The cap uses them so that it
// never cuts such a value in half.
func piiSpans(s string) [][]int {
	var spans [][]int
	for _, m := range kvSecretRe.FindAllStringSubmatchIndex(s, -1) {
		spans = append(spans, m[6:8])
	}
	spans = append(spans, emailRe.FindAllStringIndex(s, -1)...)
	spans = append(spans, ssnRe.FindAllStringIndex(s, -1)...)
	for _, m := range cardRe.FindAllStringIndex(s, -1) {
		if looksLikeCard(s[m[0]:m[1]]) {
			spans = append(spans, m)
		}
	}
	return append(spans, phoneRe.FindAllStringIndex(s, -1)...)
}
