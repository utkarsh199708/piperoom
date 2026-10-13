// Package textnorm turns pipeline error text (Kafka Connect, Debezium, Kafka
// clients, Postgres, Confluent Schema Registry, Java stack traces) into a
// Template with the volatile parts masked, and hashes that template into a
// short stable Fingerprint. Two occurrences of the same failure then get the
// same fingerprint even though their LSNs, offsets, hosts, ids and timestamps
// differ, so alerts and rooms about one failure can be grouped.
//
// Safety: input is cut to MaxInputBytes before anything else; every pattern is
// an RE2 expression (Go's regexp), so matching is linear in the input; the
// package never panics on any input (see FuzzNormalize) and keeps no state.
package textnorm

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Limits. They bound work and the size of a Template.
const (
	// MaxInputBytes is how much of the input is read; the rest is ignored.
	// tools.Truncate output is far smaller than this.
	MaxInputBytes = 64 << 10
	// DefaultFrames is how many stack frames Normalize keeps per cause.
	DefaultFrames = 5
	// MaxCauses is the longest cause chain kept.
	MaxCauses = 32
	// MaxMessageBytes caps one masked message, so a huge details blob does
	// not matter and a cut-off tail does not change the fingerprint.
	MaxMessageBytes = 200
	// maxLineBytes bounds the text a single line contributes to matching.
	maxLineBytes = 2048
	// FingerprintLen is the number of hex characters in a fingerprint.
	FingerprintLen = 16
)

// Cause is one exception (or error line) of a chain.
type Cause struct {
	// Class is the exception class ("org.postgresql.util.PSQLException"), or ""
	// when the line is plain text.
	Class string `json:"class,omitempty"`
	// Message is the masked message template.
	Message string `json:"message,omitempty"`
	// Frames are the top stack frames, "pkg.Class.method", without file:line.
	Frames []string `json:"frames,omitempty"`
}

// Template is a normalised error text: the chain of causes, outermost first.
type Template struct {
	Causes []Cause `json:"causes"`
}

// Patterns are compiled once and never modified.
var (
	reLogPrefix  = regexp.MustCompile(`^\s*\[?\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?\]?\s*(?:(?:TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL)\b)?\s*`)
	reLogfmt     = regexp.MustCompile(`\b(?:msg|err|error)="((?:[^"\\]|\\.)*)"`)
	reCausedBy   = regexp.MustCompile(`^(?:Caused by|Suppressed):\s*`)
	reFrame      = regexp.MustCompile(`^at\s+([\w$.<>/@-]+)\(`)
	reModule     = regexp.MustCompile(`^[A-Za-z0-9_.@-]+//?`)
	reMoreFrames = regexp.MustCompile(`^\.\.\.\s*\d+\s+(?:more|common frames omitted)\b`)
	reClass      = regexp.MustCompile(`^([A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)*\.[A-Za-z_$][\w$]*)(?::\s*(.*))?$`)

	reDQuote = regexp.MustCompile(`"(?:[^"\\\n]|\\.){0,200}"`)
	// A single quote only opens a literal after a non-word byte, so
	// apostrophes ("can't") are left alone.
	reSQuote = regexp.MustCompile(`(^|[^\w])'[^'\n]{0,200}'`)
	reTS     = regexp.MustCompile(`\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?|\b\d{2}:\d{2}:\d{2}(?:[.,]\d+)?\b`)
	reUUID   = regexp.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`)
	// Kafka client and member ids: consumer-<group>-<n>-<uuid>.
	reMember  = regexp.MustCompile(`\b(?:consumer|producer|connector-consumer|connector-producer|adminclient|rdkafka)[A-Za-z0-9._-]*?-?<uuid>`)
	reLSNWrap = regexp.MustCompile(`LSN\{[^}\n]{0,64}\}`)
	reLSN     = regexp.MustCompile(`\b[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}\b`)
	reIP      = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?\b`)
	reHost    = regexp.MustCompile(`\b[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.(?:local|internal|svc|com)\b(?::\d{1,5})?|\blocalhost(?::\d{1,5})?\b|\b[A-Za-z][A-Za-z0-9.-]*:\d{4,5}\b`)
	reHex     = regexp.MustCompile(`\b0[xX][0-9a-fA-F]+\b|\b[0-9a-fA-F]{16,}\b`)
	reNum     = regexp.MustCompile(`\b\d+\b`)
	reSpace   = regexp.MustCompile(`\s+`)
)

// Normalize parses trace and returns its template with DefaultFrames frames per
// cause.
func Normalize(trace string) Template { return NormalizeFrames(trace, DefaultFrames) }

// NormalizeFrames is Normalize with a chosen number of frames kept per cause.
func NormalizeFrames(trace string, frames int) Template {
	if frames < 0 {
		frames = 0
	}
	trace = clip(trace, MaxInputBytes)
	var t Template
	var cur *Cause
	for _, raw := range strings.Split(trace, "\n") {
		line := strings.TrimSpace(clip(raw, maxLineBytes))
		if line == "" || strings.HasSuffix(line, "[truncated]") || reMoreFrames.MatchString(line) {
			continue
		}
		if m := reFrame.FindStringSubmatch(line); m != nil {
			// Frames before any cause line belong to an anonymous cause.
			if cur == nil {
				t.Causes = append(t.Causes, Cause{})
				cur = &t.Causes[len(t.Causes)-1]
			}
			if len(cur.Frames) < frames {
				cur.Frames = append(cur.Frames, maskFrame(m[1]))
			}
			continue
		}
		if len(t.Causes) >= MaxCauses {
			break
		}
		t.Causes = append(t.Causes, parseCause(line))
		cur = &t.Causes[len(t.Causes)-1]
	}
	return t
}

func parseCause(line string) Cause {
	line = reLogPrefix.ReplaceAllString(line, "")
	line = reCausedBy.ReplaceAllString(line, "")
	line = reLogfmt.ReplaceAllString(line, "$1")
	if m := reClass.FindStringSubmatch(line); m != nil && looksLikeClass(m[1]) {
		return Cause{Class: m[1], Message: maskMessage(m[2])}
	}
	return Cause{Message: maskMessage(line)}
}

// looksLikeClass accepts a dotted name whose last segment starts upper-case
// ("org.x.FooException"), which keeps "kafka.broker" style words out.
func looksLikeClass(s string) bool {
	last := s[strings.LastIndexByte(s, '.')+1:]
	r, _ := utf8.DecodeRuneInString(last)
	return unicode.IsUpper(r)
}

func maskMessage(s string) string {
	s = mask(s, true)
	s = strings.TrimSpace(reSpace.ReplaceAllString(s, " "))
	return clip(s, MaxMessageBytes)
}

func maskFrame(method string) string {
	method = reModule.ReplaceAllString(method, "")
	return mask(method, false)
}

// mask replaces volatile fields. Order matters: quotes first (their content is
// opaque), then the most specific shapes, then bare numbers.
func mask(s string, quotes bool) string {
	if quotes {
		s = reDQuote.ReplaceAllString(s, `"<str>"`)
		s = reSQuote.ReplaceAllString(s, `${1}'<str>'`)
	}
	s = reTS.ReplaceAllString(s, "<ts>")
	s = reUUID.ReplaceAllString(s, "<uuid>")
	s = reMember.ReplaceAllString(s, "<member>")
	s = reLSNWrap.ReplaceAllString(s, "<lsn>")
	s = reIP.ReplaceAllString(s, "<ip>")
	s = reHost.ReplaceAllString(s, "<host>")
	s = reLSN.ReplaceAllStringFunc(s, func(m string) string {
		// "0/5A3B2F8" yes; "feed/dead" and "a/b" are words, not LSNs.
		if lsnShaped(m) {
			return "<lsn>"
		}
		return m
	})
	s = reHex.ReplaceAllString(s, "<hex>")
	return reNum.ReplaceAllString(s, "<n>")
}

func lsnShaped(m string) bool {
	i := strings.IndexByte(m, '/')
	return i > 0 && strings.ContainsAny(m[:i], "0123456789") && strings.ContainsAny(m[i+1:], "0123456789")
}

// String is the canonical text of the template, one cause per block.
func (t Template) String() string { return t.canonical(true) }

func (t Template) canonical(allFrames bool) string {
	var b strings.Builder
	for i, c := range t.Causes {
		if i > 0 {
			b.WriteString("\nCaused by: ")
		}
		b.WriteString(c.Class)
		if c.Class != "" && c.Message != "" {
			b.WriteString(": ")
		}
		b.WriteString(c.Message)
		if allFrames || i < len(t.Causes)-1 {
			for _, f := range c.Frames {
				b.WriteString("\n  at ")
				b.WriteString(f)
			}
		}
	}
	return b.String()
}

// Fingerprint is a stable id of the template: the first FingerprintLen hex
// characters of the SHA-256 of its canonical text. The frames of the last
// (root) cause are left out on purpose: that is the part tools.Truncate cuts
// off, and the root class and message already identify it. Frames of the
// outer causes are kept.
func (t Template) Fingerprint() string {
	sum := sha256.Sum256([]byte(t.canonical(false)))
	return hex.EncodeToString(sum[:])[:FingerprintLen]
}

// Fingerprint normalises trace and returns the template fingerprint.
func Fingerprint(trace string) string { return Normalize(trace).Fingerprint() }

// clip returns at most n bytes of s without splitting a UTF-8 rune.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
