package slackbridge

import (
	"strings"
	"unicode/utf8"
)

const ellipsis = "…"

// Slack documents only these three characters as control characters in text
// (https://docs.slack.dev/messaging/formatting-message-text#escaping).
// Escaping them blocks <!channel>, <@U…> and <url|fake label> in event text.
var escaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// clean drops invalid UTF-8 and control characters except \n and \t.
func clean(s string) string {
	s = strings.ToValidUTF8(s, "")
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) {
			return -1
		}
		return r
	}, s)
}

// clip cleans and escapes untrusted text, then cuts it so the ESCAPED result
// is at most maxBytes, on a rune boundary, ending in "…" when it cut. Bytes
// are a safe upper bound for Slack's character limits. Text that is empty
// after cleaning becomes "(empty)": Slack text objects need 1–3000 characters
// (https://docs.slack.dev/reference/block-kit/composition-objects/text-object).
func clip(s string, maxBytes int) string {
	s = clean(s)
	if strings.TrimSpace(s) == "" {
		return "(empty)"
	}
	if full := escaper.Replace(s); len(full) <= maxBytes {
		return full
	}
	budget := maxBytes - len(ellipsis)
	var b strings.Builder
	for _, r := range s {
		piece := escaper.Replace(string(r))
		if b.Len()+len(piece) > budget {
			break
		}
		b.WriteString(piece)
	}
	out := strings.TrimRight(b.String(), " \n\t")
	if out == "" || !utf8.ValidString(out) {
		return "(empty)"
	}
	return out + ellipsis
}

// clipCode is clip for text placed inside a ``` block: backticks become ' so
// the text cannot close the block.
func clipCode(s string, maxBytes int) string {
	return clip(strings.ReplaceAll(s, "`", "'"), maxBytes-6) // room for the fences
}

func codeBlock(s string, maxBytes int) string {
	return "```" + clipCode(s, maxBytes) + "```"
}

// inlineCode wraps a short untrusted value in single backticks.
func inlineCode(s string, maxBytes int) string {
	return "`" + clip(strings.ReplaceAll(s, "`", "'"), maxBytes) + "`"
}
