package room

import (
	"regexp"
	"strings"
	"unicode"
)

// Markdown safety for the post-mortem draft (docs/design/postmortem.md). Every
// string from an event is untrusted text: a comment, a tool summary or an
// agent sentence can carry a heading, a link, a table cell or an HTML tag. It
// goes through mdText, which makes it one plain line that cannot start any
// block and cannot open a link, an image, a span or a tag.

// pmTextMax is the byte cap of one string in the draft (truncateDetail).
// pmLineMax is the cap of agent, tool and action text in a timeline row.
const pmLineMax = 200

// mdLeadMarker matches what would turn the start of a line into a list item:
// bullets and "1." / "1)" ordered markers, repeated.
var mdLeadMarker = regexp.MustCompile(`^(?:[-+*]+|\d+[.)])\s*`)

// mdSpecials are escaped with a backslash. & stops entity references, ~ and $
// stop strike-through and math in dialects that have them.
const mdSpecials = "\\`*_#>|[]<&~$!"

// mdText renders s as one escaped line of at most 500 bytes.
func mdText(s string) string { return mdTextN(s, 0) }

// mdTextN is mdText with a tighter byte cap when max > 0.
func mdTextN(s string, max int) string {
	s = truncateDetail(sanitizeAgentText(s))
	if max > 0 && len(s) > max {
		cut := max
		for cut > 0 && s[cut]&0xC0 == 0x80 {
			cut--
		}
		s = s[:cut] + "…"
	}
	// One line: every line break, tab and other control or separator
	// character becomes a space.
	s = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' || r == 0x85 || r == 0x2028 || r == 0x2029 || unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	for {
		t := strings.TrimSpace(mdLeadMarker.ReplaceAllString(s, ""))
		if t == s {
			break
		}
		s = t
	}
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune(mdSpecials, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// mdCode renders an id or a name as an inline code span: no backtick, line
// break or control character can end the span early.
func mdCode(s string) string {
	s = strings.Map(func(r rune) rune {
		if r == '`' || r == '|' || unicode.IsControl(r) || r == 0x2028 || r == 0x2029 {
			return -1
		}
		return r
	}, sanitizeAgentText(s))
	if len(s) > 120 {
		cut := 120
		for cut > 0 && s[cut]&0xC0 == 0x80 {
			cut--
		}
		s = s[:cut]
	}
	if s == "" {
		return "–"
	}
	return "`" + s + "`"
}

// mdFence wraps content in a fenced block whose fence is longer than any run
// of backticks in the content, so the content cannot close it.
func mdFence(info, content string) string {
	content = sanitizeAgentText(content)
	longest, run := 0, 0
	for _, r := range content {
		if r == '`' {
			run++
			longest = max(longest, run)
		} else {
			run = 0
		}
	}
	fence := strings.Repeat("`", max(3, longest+1))
	return fence + info + "\n" + content + "\n" + fence
}
