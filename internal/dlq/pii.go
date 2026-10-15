package dlq

import (
	"regexp"
	"strconv"
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
	// Phone-like: optional +country, then 3-3-4 digits with separators; an
	// E.164 number (+15551234567); or +country and 2-5 groups of 1-4 digits
	// (+44 20 7946 0958). A leading + is what tells the last two from an
	// offset range, and maskPhones wants 8 digits or more.
	phoneRe = regexp.MustCompile(`(?:\+\d{1,3}[ .-]?)?\(?\b\d{3}\)?[ .-]\d{3}[ .-]\d{4}\b|\+\d{8,15}\b|\+\d{1,3}(?:[ .-]\d{1,4}){2,5}\b`)
	// A bearer token whatever key (or none) precedes it: "auth header bearer
	// abc". kvSecretRe takes it only after a credential key.
	bearerRe = regexp.MustCompile(`(?i)\b(bearer\s+)[^\s,;&)'"<]+`)
	// The word before a number that makes it a Kafka position, not a phone:
	// "offset 555 123 4567", "partitions 0-3".
	positionWordRe = regexp.MustCompile(`(?i)\b(?:offsets?|partitions?)[\s:=]*$`)
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
	s = maskPhones(s)
	s = bearerRe.ReplaceAllString(s, "${1}"+Redacted)
	out, _ := redact.Text(s)
	return partialMaskRe.ReplaceAllString(out, Redacted)
}

// MaskPII masks the unquoted values RedactMessage masks (e-mail addresses,
// card, SSN and phone numbers, credential values, what internal/redact finds)
// and nothing else: no cap, no quote or group rules. It is for text that must
// keep its shape, such as a DDL statement in the watcher's change log.
func MaskPII(s string) string { return maskUnquoted(s) }

// maskPhones replaces phoneRe's matches with Redacted, except one right after
// "offset" or "partition" (the digits are a Kafka position) and one with fewer
// than 8 digits ("+1 2 3").
func maskPhones(s string) string {
	var b strings.Builder
	last := 0
	for _, m := range phoneRe.FindAllStringIndex(s, -1) {
		if positionWordRe.MatchString(s[:m[0]]) || digitCount(s[m[0]:m[1]]) < 8 {
			continue
		}
		b.WriteString(s[last:m[0]])
		b.WriteString(Redacted)
		last = m[1]
	}
	b.WriteString(s[last:])
	return b.String()
}

func digitCount(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			n++
		}
	}
	return n
}

// piiSpans returns the byte spans of the values maskUnquoted masks with its
// own patterns. The cap uses them to avoid cutting a value in half.
func piiSpans(s string) [][]int {
	var spans [][]int
	for _, re := range []*regexp.Regexp{emailRe, ssnRe, phoneRe, bearerRe} {
		spans = append(spans, re.FindAllStringIndex(s, -1)...)
	}
	for _, m := range kvSecretRe.FindAllStringSubmatchIndex(s, -1) {
		spans = append(spans, m[6:8])
	}
	for _, m := range cardRe.FindAllStringIndex(s, -1) {
		if looksLikeCard(s[m[0]:m[1]]) {
			spans = append(spans, m)
		}
	}
	return spans
}

// cardGroups are the groupings card networks print: 4-4-4-4 (Visa,
// Mastercard), 4-6-5 (Amex), 4-6-4 (Diners), 4-4-4-4-3 (19 digits).
var cardGroups = map[string]bool{"4-4-4-4": true, "4-6-5": true, "4-6-4": true, "4-4-4-4-3": true}

// looksLikeCard accepts a Luhn-valid number with a card network's first
// digit (3-6). An ungrouped run must also have a card length (15 or 16), so
// that a 13-digit epoch-millisecond timestamp or a long offset is not taken
// for one; a grouped one must use a card grouping, so that an offset range
// (1234520-1234619) is not.
func looksLikeCard(m string) bool {
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, m)
	if len(digits) < 13 || len(digits) > 19 || !luhn(digits) || digits[0] < '3' || digits[0] > '6' {
		return false
	}
	if digits == m { // no separators
		return len(digits) == 15 || len(digits) == 16
	}
	return cardGroups[groupShape(m)]
}

// groupShape turns "4111 1111 1111 1111" into "4-4-4-4".
func groupShape(m string) string {
	var parts []string
	for _, g := range strings.FieldsFunc(m, func(r rune) bool { return r == ' ' || r == '-' }) {
		parts = append(parts, strconv.Itoa(len(g)))
	}
	return strings.Join(parts, "-")
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
