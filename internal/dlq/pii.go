package dlq

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

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
	// record's or a URL's short password must go too. The value never starts
	// with ':', so a Postgres cast ("token::varchar") is not a key and value.
	kvSecretRe = regexp.MustCompile(`(?i)\b([a-z0-9_.-]*?(?:password|passwd|pwd|secret|token|api[_.-]?key|access[_.-]?key|private[_.-]?key|credentials?))(\s*[=:]\s*)((?:(?:bearer|basic|token)\s+)?[^\s,;&)'":][^\s,;&)'"]*)`)
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
	// A Kafka position word right before a number: "partition 0 offset 555
	// 123 4567" is an offset, not a phone. The word must start the text or
	// follow white space, so "phone.offset: 555-123-4567" is still a phone.
	positionWordRe = regexp.MustCompile(`(?i)(?:^|\s)(?:offsets?|partitions?)[\s:=]*$`)
)

// The #426 pass. It runs on the output of the rules above and only adds
// masks, so it cannot unmask what they mask (docs/agents/org.md §12).
var (
	// An international phone number: +country and 2-5 groups of 1-5 digits
	// (+44 20 7946 0958, +91-98765-43210), or E.164 without separators
	// (+15551234567). intlPhoneSpans filters the matches.
	intlPhoneRe = regexp.MustCompile(`\+\d{1,3}(?:[ .-]\d{1,5}){2,5}\b|\+\d{8,15}\b`)
	// A date inside a match: "+1 2026-10-15" is a time zone offset and a day.
	dateRe = regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)
	// A bare +digits run is a phone only after a phone word ("phone
	// +15551234567"); "lag delta +1234567890" is a count.
	phoneWordRe = regexp.MustCompile(`(?i)\b(?:phone|tel|telephone|mobile|cell|fax|call|sms|msisdn|contact|whatsapp)[a-z]*\W{0,3}$`)
	// The credential after an Authorization key, whatever its form:
	// "Authorization: Bearer x" (the key is not one kvSecretRe knows).
	authRe = regexp.MustCompile(`(?i)\b(authorization\s*[:=]\s*)((?:bearer|basic|token)\s+)?([^\s,;&)'"<]+)`)
	// A bearer token without a key ("auth header bearer abcdef123456").
	// bearerSpans keeps prose ("bearer token expired", "Bearer realm=api").
	bearerRe = regexp.MustCompile(`(?i)\bbearer\s+([A-Za-z0-9._~+/-]+)(=*)`)
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
	s = maskSpans(s, phoneSpans(s))
	out, _ := redact.Text(s)
	out = partialMaskRe.ReplaceAllString(out, Redacted)
	out = maskSpans(out, intlPhoneSpans(out))
	out = maskSpans(out, authSpans(out))
	return maskSpans(out, bearerSpans(out))
}

// MaskPII masks the unquoted values RedactMessage masks (e-mail addresses,
// card, SSN and phone numbers, credential values, what internal/redact finds)
// and nothing else: no cap, no quote or group rules. It is for text that must
// keep its shape, such as a DDL statement in the watcher's change log.
func MaskPII(s string) string { return maskUnquoted(s) }

// maskSpans replaces the byte spans (sorted, not overlapping) with Redacted.
func maskSpans(s string, spans [][]int) string {
	if len(spans) == 0 {
		return s
	}
	var b strings.Builder
	last := 0
	for _, sp := range spans {
		b.WriteString(s[last:sp[0]])
		b.WriteString(Redacted)
		last = sp[1]
	}
	b.WriteString(s[last:])
	return b.String()
}

// phoneSpans are phoneRe's matches, except an unsigned one right after a
// Kafka position word (#426).
func phoneSpans(s string) [][]int {
	var out [][]int
	for _, m := range phoneRe.FindAllStringIndex(s, -1) {
		if s[m[0]] != '+' && positionWordRe.MatchString(s[:m[0]]) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// intlPhoneSpans are intlPhoneRe's matches that read as a phone: the + starts
// a word (not "UTC+1", "2.7.0+2026"), no date or time is part of it, it has 8
// digits or more, and a bare +digits run follows a phone word.
func intlPhoneSpans(s string) [][]int {
	var out [][]int
	for _, m := range intlPhoneRe.FindAllStringIndex(s, -1) {
		v := s[m[0]:m[1]]
		if r, _ := utf8.DecodeLastRuneInString(s[:m[0]]); m[0] > 0 && (unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.') {
			continue
		}
		if rest := s[m[1]:]; len(rest) > 1 && (rest[0] == '.' || rest[0] == ':') && rest[1] >= '0' && rest[1] <= '9' {
			continue
		}
		if dateRe.MatchString(v) || digitCount(v) < 8 {
			continue
		}
		if !strings.ContainsAny(v, " .-") && !phoneWordRe.MatchString(s[:m[0]]) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// authSpans are the credentials after an Authorization key. A bare scheme word
// with nothing after it ("Authorization: Bearer <redacted>") is not one.
func authSpans(s string) [][]int {
	var out [][]int
	for _, m := range authRe.FindAllStringSubmatchIndex(s, -1) {
		if m[4] < 0 {
			switch strings.ToLower(s[m[6]:m[7]]) {
			case "bearer", "basic", "token":
				continue
			}
		}
		out = append(out, m[6:8])
	}
	return out
}

// bearerSpans are the tokens after a bare "bearer": at least 8 runes with a
// digit or one of ._~+/=-, and not the key of a key=value ("Bearer
// realm=api"), so prose ("bearer token expired") stays.
func bearerSpans(s string) [][]int {
	var out [][]int
	for _, m := range bearerRe.FindAllStringSubmatchIndex(s, -1) {
		tok, end := s[m[2]:m[3]], m[5]
		if m[5] > m[4] && end < len(s) && isWordByte(s[end]) {
			continue // "realm=api": a key, not padding
		}
		if len(tok) < 8 || !strings.ContainsAny(tok, "0123456789._~+/-") && m[5] == m[4] {
			continue
		}
		out = append(out, []int{m[2], end})
	}
	return out
}

func isWordByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
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

// piiSpans returns the byte spans of every value maskUnquoted masks with its
// own patterns. The cap uses them so that it never cuts a value in half.
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
	spans = append(spans, phoneSpans(s)...)
	spans = append(spans, intlPhoneSpans(s)...)
	spans = append(spans, authSpans(s)...)
	return append(spans, bearerSpans(s)...)
}

// looksLikeCard accepts a Luhn-valid number. An ungrouped run must also have
// a card length (15 or 16) and a card network's first digit (3-6), so that a
// 13-digit epoch-millisecond timestamp or a long offset is not taken for one.
// Two dash-joined runs of 6 digits or more are an offset range
// (1234520-1234619), a grouping no card uses (#426).
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
	if a, b, ok := strings.Cut(m, "-"); ok && !strings.ContainsAny(b, " -") && !strings.Contains(a, " ") && len(a) >= 6 && len(b) >= 6 {
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
