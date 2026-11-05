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
	// KindControlToken is a chat-template control token or a fake tool-call
	// tag (<|im_start|>, <|eot_id|>, <start_of_turn>, [INST], [TOOL_CALLS],
	// <tool_call>, <function=name>). On a local or open-weight model served
	// through a chat template, data holding one can end the data turn and open
	// a forged one; the agent neutralises them (V16, model-providers.md
	// section 10.1). It is reported next to KindRoleTag for the tokens both
	// rules know.
	KindControlToken Kind = "control_token"
	// KindHiddenComment is an HTML comment that addresses the AI.
	KindHiddenComment Kind = "hidden_comment"
	// KindWrapperTag is one of the agent's own untrusted-data wrapper tags
	// (<untrusted>, </tool_output>) or the "[verification]" verdict tag:
	// data carrying them tries to close the wrapper or forge a verdict.
	KindWrapperTag Kind = "wrapper_tag"
	// KindObfuscated marks a finding that was only visible after stripping
	// invisible characters or mapping lookalike letters.
	KindObfuscated Kind = "obfuscated"
	// KindEncoded is a base64 blob that decodes to instruction-like text.
	KindEncoded Kind = "encoded"
	// KindHiddenText is text written in Unicode tag characters (outside a
	// flag emoji): nothing renders, but a model reads it.
	KindHiddenText Kind = "hidden_text"
)

// Block is the enclosing unit of untrusted data around a finding's span: the
// whole record or trace block (the maximal run of non-blank lines) that holds
// the span, or the whole comment, blob or tag run for kinds whose span is
// already the whole unit. It lets a caller compare values against a wider
// region than the clause when an injection puts its action in one sentence and
// its value in the next. The paragraph block is capped at WindowBytes on each
// side of the span, cut on a rune boundary, so it stays bounded however long
// the input is. Start and End are byte offsets into the string the caller
// passed to ScanNames (rune boundaries, Start <= Offset <= End <= len(s)); the
// zero value means the span is its own block. It is untrusted data too: never
// log it.
type Block struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// Finding is one match.
type Finding struct {
	Kind Kind `json:"kind"`
	// Offset is the byte offset of the match in the scanned string.
	Offset int `json:"offset"`
	// End is the byte offset in the scanned string just past the flagged
	// span, so s[Offset:End] is the span (both are rune boundaries of the
	// string the caller passed, not of the normalised text). For a pattern
	// match the span runs from the match to the end of the clause that holds
	// it (the sentence or line, at most SpanTailBytes past the match), so it
	// carries the names and values the instruction refers to ("propose
	// connect_restart_connector for pg-orders-connector"); a hidden comment,
	// a base64 blob and a run of tag characters span the whole comment, blob
	// or run. Invisible characters the normalisation dropped stay inside the
	// span. It is untrusted data too: never log it.
	End int `json:"end"`
	// Excerpt is a short, normalised copy of the matched text (at most
	// MaxExcerptBytes). It is untrusted data too: never log it.
	Excerpt string `json:"excerpt"`
	// Names are the caller's names (actions, connectors, topics, groups)
	// that occur inside the span, in the caller's order. Only ScanNames
	// fills it; Scan leaves it nil.
	Names []string `json:"names,omitempty"`
	// Block is the enclosing unit of untrusted data the span sits in (see
	// the Block type). Only ScanNames fills it; Scan leaves it zero, which
	// means "the span is its own block". A caller comparing a value the model
	// typed against the block must Normalize both sides first.
	Block Block `json:"block,omitzero"`
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
	// SpanTailBytes is how far past a pattern match Finding.End may reach
	// when no clause end comes first.
	SpanTailBytes = 400

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

// ScanNames is Scan with Finding.Names filled: for each finding, the entries
// of names that occur inside its span s[Offset:End], compared ASCII
// case-insensitively on the span after the normalisation Scan applies, and
// word-bounded: a name does not match inside a longer identifier ("orders"
// in "pg-orders-connector", "pg.public.orders" in "pg.public.orders-value").
// Names come back in the order given, each once, empty entries skipped; the
// findings are otherwise exactly Scan's.
func ScanNames(s string, names []string) []Finding {
	fs := Scan(s)
	if len(fs) == 0 {
		return fs
	}
	for i, f := range fs {
		if f.Offset < 0 || f.End > len(s) || f.End < f.Offset {
			continue // cannot happen; never slice out of range on untrusted text
		}
		fs[i].Block = blockFor(s, f)
		if len(names) > 0 {
			fs[i].Names = namesIn(s[f.Offset:f.End], names)
		}
	}
	return fs
}

// blockFor returns the enclosing unit of untrusted text around a finding's
// span. A hidden comment, an encoded blob or a run of hidden tag text is
// already its own unit, so its block is its span. Anything else widens to the
// enclosing paragraph (the maximal run of non-blank lines), so an injection
// whose action and value sit in different sentences of the same record or
// trace block still lands them in one block.
func blockFor(s string, f Finding) Block {
	if f.Kind == KindHiddenComment || f.Kind == KindEncoded || f.Kind == KindHiddenText {
		return Block{Start: f.Offset, End: f.End}
	}
	start, end := paragraphBounds(s, f.Offset, f.End)
	return Block{Start: start, End: end}
}

// paragraphBounds returns the enclosing paragraph of the span [start, end):
// the maximal run of non-blank lines around it, capped at WindowBytes past the
// span on each side so the block stays bounded however long s is. Both
// returned offsets are rune boundaries of s and the block encloses the span.
func paragraphBounds(s string, start, end int) (int, int) {
	floor := max(0, start-WindowBytes)
	ceil := min(len(s), end+WindowBytes)

	// The span's own line, never more than WindowBytes past the span.
	lo := start
	for lo > floor && s[lo-1] != '\n' {
		lo--
	}
	hi := end
	for hi < ceil && s[hi] != '\n' {
		hi++
	}

	// Grow up over adjacent non-blank lines; a blank line or the cap ends it.
	for lo > floor {
		p := lo - 1
		for p > floor && s[p-1] != '\n' {
			p--
		}
		if p == floor && p > 0 && s[p-1] != '\n' {
			lo = floor // the line above runs past the cap
			break
		}
		if isBlankLine(s[p : lo-1]) {
			break
		}
		lo = p
	}
	// Grow down over adjacent non-blank lines.
	for hi < ceil {
		n := hi + 1
		for n < ceil && s[n] != '\n' {
			n++
		}
		if n == ceil && n < len(s) && s[n] != '\n' {
			hi = ceil // the line below runs past the cap
			break
		}
		if isBlankLine(s[hi+1 : n]) {
			break
		}
		hi = n
	}

	// Both edges land on rune boundaries.
	for lo < start && !utf8.RuneStart(s[lo]) {
		lo++
	}
	for hi > end && hi < len(s) && !utf8.RuneStart(s[hi]) {
		hi--
	}
	return lo, hi
}

// isBlankLine reports whether line is empty or only horizontal whitespace: a
// blank line, the paragraph break between records and trace blocks.
func isBlankLine(line string) bool {
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case ' ', '\t', '\r':
		default:
			return false
		}
	}
	return true
}

// namesIn returns the entries of names that occur word-bounded in span,
// normalised as Scan normalises it (both readings of format characters).
func namesIn(span string, names []string) []string {
	norm, _, _, hadCf := normalize(span, false)
	texts := []string{lowerASCII(norm)}
	if hadCf {
		spaced, _, _, _ := normalize(span, true)
		texts = append(texts, lowerASCII(spaced))
	}
	var out []string
	seen := map[string]bool{}
	for _, name := range names {
		key := lowerASCII(name)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		for _, text := range texts {
			if containsWord(text, key) {
				out = append(out, name)
				break
			}
		}
	}
	return out
}

// containsWord reports whether key occurs in text with neither end joined to
// a longer identifier.
func containsWord(text, key string) bool {
	for from := 0; from < len(text); {
		i := strings.Index(text[from:], key)
		if i < 0 {
			return false
		}
		start := from + i
		if !joinsBefore(text, start) && !joinsAfter(text, start+len(key)) {
			return true
		}
		from = start + 1
	}
	return false
}

// joinsBefore reports whether text[i] continues an identifier: the byte
// before it is a word byte, or a '-' or '.' that itself follows one
// ("pg-orders", "pg.public.orders"). A trailing '-' or '.' before a space or
// the end is punctuation, not part of a name.
func joinsBefore(text string, i int) bool {
	if i == 0 {
		return false
	}
	c := text[i-1]
	if isWordByte(c) {
		return true
	}
	return (c == '-' || c == '.') && i >= 2 && isWordByte(text[i-2])
}

// joinsAfter is joinsBefore for the byte at i, the one after a candidate.
func joinsAfter(text string, i int) bool {
	if i >= len(text) {
		return false
	}
	c := text[i]
	if isWordByte(c) {
		return true
	}
	return (c == '-' || c == '.') && i+1 < len(text) && isWordByte(text[i+1])
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
				found = append(found, Finding{Kind: KindObfuscated, Offset: f.Offset, End: f.End, Excerpt: f.Excerpt})
				break
			}
		}
	}
	found = append(found, scanBase64(norm, origin)...)
	found = append(found, scanHiddenTags(s)...)
	found = append(found, scanStrayTags(s)...)
	for i := range found {
		found[i].Offset, found[i].End = runeSpan(s, found[i].Offset, found[i].End)
		found[i].Offset += base
		found[i].End += base
	}
	return found
}

// runeSpan widens [start, end) of s to rune starts. origin maps each invalid
// byte to its own offset, so a span next to a stray continuation byte can
// start or end on it; widening (never narrowing) keeps the matched text inside
// the span. The window s starts and ends on rune starts of the whole input, so
// the result is a rune boundary there too.
func runeSpan(s string, start, end int) (int, int) {
	start = min(max(start, 0), len(s))
	end = min(max(end, start), len(s))
	for start > 0 && !utf8.RuneStart(s[start]) {
		start--
	}
	for end < len(s) && !utf8.RuneStart(s[end]) {
		end++
	}
	return start, end
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
		out = append(out, Finding{Kind: KindEncoded, Offset: origin[loc[0]], End: origin[loc[1]], Excerpt: excerpt("base64: " + hits[0].Excerpt)})
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
	m := make([]int, 0, prealloc(n))
	for i := 0; i <= n; i++ {
		m = append(m, i)
	}
	return m
}
