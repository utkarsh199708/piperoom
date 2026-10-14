// Package injectscan flags text inside untrusted data (tool results, record
// values, error traces) that reads like instructions to the agent: prompt
// injection attempts. It only reports; it never changes or drops the data,
// because the text may be the very evidence the investigation needs.
//
// The heuristics are tuned against this repo's operations text (scenario
// fixtures, runbooks, Connect/Postgres/Kafka messages) and a list of
// realistic near-misses, but they are heuristics: they miss paraphrased or
// translated injections and can still flag unusual ops text. A finding is a
// hint for the model and for humans, not a verdict, and no finding does not
// mean the data is safe.
package injectscan

import (
	"encoding/base64"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Kind names one family of instruction-like text.
type Kind string

// Finding kinds.
const (
	// KindOverride is an attempt to replace the agent's instructions
	// ("ignore previous instructions", "your new task", "system prompt").
	KindOverride Kind = "override"
	// KindAddressed is text that speaks to the assistant directly
	// ("note to the AI", "as the assistant").
	KindAddressed Kind = "addressed"
	// KindToolRequest asks for a tool call, a fix, an approval or a write
	// ("propose connect_restart_connector" next to text addressed to the AI).
	KindToolRequest Kind = "tool_request"
	// KindRoleTag is chat-format markup (<system>, [INST], "Human:" turns).
	KindRoleTag Kind = "role_tag"
	// KindHiddenComment is an HTML comment that addresses the AI.
	KindHiddenComment Kind = "hidden_comment"
	// KindWrapperTag is one of the agent's own untrusted-data wrapper tags
	// (<untrusted>, </tool_output>) or the "[verification]" verdict tag:
	// data carrying them tries to close the wrapper or forge a verdict.
	KindWrapperTag Kind = "wrapper_tag"
	// KindObfuscated marks a finding that was only visible after stripping
	// invisible characters or mapping lookalike letters.
	KindObfuscated Kind = "obfuscated"
	// KindEncoded is a base64 (also line-wrapped), hex, %-encoded or HTML
	// character-reference run that decodes to instruction-like text.
	KindEncoded Kind = "encoded"
)

// Finding is one match.
type Finding struct {
	Kind Kind `json:"kind"`
	// Offset is the byte offset of the match in the scanned string.
	Offset int `json:"offset"`
	// Excerpt is a short, normalised copy of the matched text (at most
	// MaxExcerptBytes). It is untrusted data too: never log it.
	Excerpt string `json:"excerpt"`
}

const (
	// WindowBytes is how much of a long input is scanned at each end: inputs
	// up to 2*WindowBytes are scanned whole, longer ones only in their first
	// and last WindowBytes. The agent scans the truncated summary the model
	// sees (tools.MaxSummaryBytes), which fits in one window pair.
	WindowBytes = 8 << 10
	// MaxFindings caps the findings returned for one input.
	MaxFindings = 32
	// MaxExcerptBytes caps Finding.Excerpt.
	MaxExcerptBytes = 80

	maxBlobs       = 8    // base64 candidates decoded per window
	maxBlobBytes   = 2048 // encoded bytes decoded per candidate
	maxComments    = 16   // HTML comments inspected per window
	commentBytes   = 400  // bytes of an HTML comment inspected
	contextBytes   = 300  // how close an addressee must be to a conditional match
	printableRatio = 0.9  // share of printable runes a decoded blob needs
)

// Scan returns the instruction-like spans in s, ordered by offset. It never
// panics and returns at most MaxFindings. Time is linear in min(len(s),
// 2*WindowBytes).
func Scan(s string) []Finding {
	if len(s) <= 2*WindowBytes {
		return finish(scanWindow(s, 0))
	}
	head := cutBefore(s, WindowBytes)
	tailStart := len(s) - WindowBytes
	for tailStart < len(s) && !utf8.RuneStart(s[tailStart]) {
		tailStart++
	}
	found := scanWindow(head, 0)
	found = append(found, scanWindow(s[tailStart:], tailStart)...)
	return finish(found)
}

// Kinds returns the distinct kinds of fs, sorted, for logs that must not carry
// the matched text.
func Kinds(fs []Finding) []string {
	seen := map[Kind]bool{}
	var out []string
	for _, f := range fs {
		if !seen[f.Kind] {
			seen[f.Kind] = true
			out = append(out, string(f.Kind))
		}
	}
	sort.Strings(out)
	return out
}

// scanWindow scans one window; base is its offset in the whole input.
func scanWindow(s string, base int) []Finding {
	norm, origin, changed, hadCf := normalize(s, false)
	found := scanText(norm, origin)
	if len(found) == 0 && hadCf {
		// Format characters may have been word separators rather than
		// splitters: read them as spaces.
		norm, origin, _, _ = normalize(s, true)
		found = scanText(norm, origin)
	}
	if changed && len(found) > 0 {
		// Obfuscation: a (kind, offset) the normalised text matched that the
		// raw text alone did not.
		raw := map[[2]any]bool{}
		for _, f := range scanText(s, identity(len(s))) {
			raw[[2]any{f.Kind, f.Offset}] = true
		}
		for _, f := range found {
			if !raw[[2]any{f.Kind, f.Offset}] {
				found = append(found, Finding{Kind: KindObfuscated, Offset: f.Offset, Excerpt: f.Excerpt})
				break
			}
		}
	}
	found = append(found, scanBase64(norm, origin)...)
	found = append(found, scanDecoded(norm, origin)...)
	for i := range found {
		found[i].Offset += base
	}
	return found
}

// scanBase64 decodes up to maxBlobs candidate blobs and scans the decoded text
// with the plain rules (no further decoding, so nesting cannot multiply work).
func scanBase64(norm string, origin []int) []Finding {
	var out []Finding
	for _, loc := range base64Runs(norm, maxBlobs) {
		blob := norm[loc[0]:loc[1]]
		if len(blob) > maxBlobBytes {
			blob = blob[:maxBlobBytes]
		}
		dec, ok := decodeBase64(blob)
		if !ok {
			continue
		}
		inner, innerOrigin, _, _ := normalize(dec, false)
		hits := scanText(inner, innerOrigin)
		if len(hits) == 0 {
			continue
		}
		out = append(out, Finding{Kind: KindEncoded, Offset: origin[loc[0]], Excerpt: excerpt("base64: " + hits[0].Excerpt)})
	}
	return out
}

// decodeBase64 tries the standard and URL alphabets, padded or not, after
// trimming the blob to whole groups, and accepts only output that is mostly
// printable UTF-8 text.
func decodeBase64(blob string) (string, bool) {
	trimmed := strings.TrimRight(blob, "=")
	if len(trimmed)%4 == 1 {
		trimmed = trimmed[:len(trimmed)-1]
	}
	for _, enc := range []*base64.Encoding{base64.RawStdEncoding, base64.RawURLEncoding} {
		b, err := enc.DecodeString(trimmed)
		if err != nil {
			continue
		}
		if printable(b) {
			return string(b), true
		}
	}
	return "", false
}

// printable reports whether b is UTF-8 with at least printableRatio of its
// runes printable (newlines and tabs count as printable).
func printable(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	total, ok := 0, 0
	for _, r := range string(b) {
		total++
		if unicode.IsPrint(r) || r == '\n' || r == '\t' || r == '\r' {
			ok++
		}
	}
	return total > 0 && float64(ok) >= printableRatio*float64(total)
}

// finish sorts by offset, drops duplicates (same kind, same offset) and caps
// the count.
func finish(fs []Finding) []Finding {
	if len(fs) == 0 {
		return nil
	}
	sort.SliceStable(fs, func(i, j int) bool {
		if fs[i].Offset != fs[j].Offset {
			return fs[i].Offset < fs[j].Offset
		}
		return fs[i].Kind < fs[j].Kind
	})
	out := fs[:0]
	for i, f := range fs {
		if i > 0 && f.Kind == fs[i-1].Kind && f.Offset == fs[i-1].Offset {
			continue
		}
		out = append(out, f)
		if len(out) == MaxFindings {
			break
		}
	}
	return out
}

func excerpt(s string) string {
	s = strings.ToValidUTF8(s, "")
	s = strings.Join(strings.Fields(s), " ")
	return cutBefore(s, MaxExcerptBytes)
}

// cutBefore returns the longest prefix of s of at most n bytes that ends on a
// rune boundary.
func cutBefore(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func identity(n int) []int {
	m := make([]int, n+1)
	for i := range m {
		m[i] = i
	}
	return m
}
