// Package injectscan flags text inside untrusted data (tool results, record
// values, error traces) that reads like instructions to the agent: prompt
// injection attempts. It only reports; it never changes or drops the data,
// because the text may be the very evidence the investigation needs.
//
// The heuristics are deliberately narrow so ordinary operations text (stack
// traces, DDL, connector configs, lag tables) gives no findings. A finding is
// a hint for the model and for humans, not a verdict.
package injectscan

import (
	"encoding/base64"
	"regexp"
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
	// ("ignore previous instructions", "you are now", "system prompt").
	KindOverride Kind = "override"
	// KindAddressed is text that speaks to the assistant directly
	// ("note to the AI", "as the assistant").
	KindAddressed Kind = "addressed"
	// KindToolRequest asks for a tool call, a fix, an approval or a write
	// ("propose connect_restart_connector", "please reset offsets").
	KindToolRequest Kind = "tool_request"
	// KindRoleTag is chat-format markup (<system>, [INST], "Human:").
	KindRoleTag Kind = "role_tag"
	// KindWrapperTag is one of the agent's own untrusted-data wrapper tags
	// (<untrusted>, </tool_output>) or the "[verification]" verdict tag:
	// data carrying them tries to close the wrapper or forge a verdict.
	KindWrapperTag Kind = "wrapper_tag"
	// KindObfuscated marks that the findings were only visible after
	// stripping invisible characters or mapping lookalike letters.
	KindObfuscated Kind = "obfuscated"
	// KindEncoded is a base64 blob that decodes to instruction-like text.
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
	// MaxInputBytes caps how much of the input is scanned; the rest is
	// ignored. Tool summaries are far smaller (tools.MaxSummaryBytes).
	MaxInputBytes = 64 << 10
	// MaxFindings caps the findings returned for one input.
	MaxFindings = 32
	// MaxExcerptBytes caps Finding.Excerpt.
	MaxExcerptBytes = 80

	maxBlobs     = 16   // base64 candidates decoded per input
	maxBlobBytes = 4096 // encoded bytes decoded per candidate
)

// actionAndToolNames are the repo's real action and tool names (A2's actions,
// A2/A3's read tools, A4's room tools). An imperative in front of one of these
// in tool data is a request to the agent, never operations text.
const actionAndToolNames = `connect_update_connector_config|connect_restart_task|connect_restart_connector|connect_resume_connector|` +
	`schema_set_compatibility|schema_register|` +
	`propose_fix|update_hypothesis|post_message|` +
	`connect_list_connectors|connect_status|connect_config|kafka_topic_offsets|kafka_list_groups|kafka_consumer_lag|` +
	`kafka_find_by_key|kafka_topic_config|schema_registry_subjects|schema_registry_versions|schema_registry_diff|` +
	`pg_replication_slots|pg_recent_ddl|pg_long_transactions|pg_row|pg_publication_tables|` +
	`warehouse_freshness|warehouse_row|pipeline_map|trace_record|runbook_lookup`

type rule struct {
	kind Kind
	re   *regexp.Regexp
	// nounGuard drops a match whose first capture group matched: a determiner
	// before the verb makes it a noun ("the same call connect_resume_connector
	// makes"), which is operations text, not a request.
	nounGuard bool
}

// rules run on the normalised text. Go's regexp is RE2, so each runs in time
// linear in the input.
var rules = []rule{
	{KindOverride, regexp.MustCompile(`(?i)\b(?:ignore|disregard|forget|override|bypass)\s+(?:(?:all|any|the|your|these|those|of|every|my)\s+)*(?:(?:previous|prior|above|earlier|preceding|former|system|safety|original|existing|other)\s+)?(?:instructions?|rules|prompts?|directions|directives|guidelines|guardrails|constraints|policies|context)\b`), false},
	{KindOverride, regexp.MustCompile(`(?i)\bdisregard\s+(?:all|any|everything|the\s+above|previous|prior|your|earlier|what)\b`), false},
	{KindOverride, regexp.MustCompile(`(?i)\byou\s+are\s+now\b|\bfrom\s+now\s+on,?\s+(?:you|the\s+(?:assistant|agent))\b|\bnew\s+instructions?\s*:|\b(?:system|developer|hidden)\s+prompt\b|\b(?:developer|god|jailbreak|dan)\s+mode\b`), false},
	{KindOverride, regexp.MustCompile(`(?i)\bdo\s+not\s+(?:mention|reveal|disclose|report|tell\s+(?:anyone|the\s+(?:user|humans?|engineers?|driver)))\b`), false},
	{KindAddressed, regexp.MustCompile(`(?i)\b(?:as|you\s+are)\s+(?:the|an?)\s+(?:ai|assistant|llm|language\s+model|claude|chatbot)\b`), false},
	{KindAddressed, regexp.MustCompile(`(?i)\b(?:to|for|attention|dear|hey)\s*:?\s+(?:the\s+)?(?:ai(?:\s+(?:agent|assistant|model|investigator))?|assistant|llm|claude|language\s+model|chatbot|automated\s+(?:agent|investigator))\b`), false},
	{KindToolRequest, regexp.MustCompile(`(?i)(\b(?:the|same|a|an|this|that|each|every|one|which|its|first|last)\s+)?\b(?:call|invoke|run|use|execute|propose|trigger|perform|submit|issue|apply|approve|do)\s+(?:the\s+|a\s+|an\s+)?(?:tool\s+|action\s+)?(?:` + actionAndToolNames + `)\b`), true},
	{KindToolRequest, regexp.MustCompile(`(?i)\b(?:please|now|immediately|you\s+(?:must|should|need\s+to|have\s+to|are\s+(?:required|authori[sz]ed)\s+to))\s*,?\s+(?:(?:also|just|then|go\s+ahead\s+and)\s+)?(?:restart|resume|reset|register|approve|propose|call|run|execute|skip|delete|drop|truncate|set)\b`), false},
	{KindToolRequest, regexp.MustCompile(`(?i)\b(?:pre-?approved|auto-?approve[ds]?|approval\s+is\s+not\s+(?:needed|required)|without\s+(?:waiting\s+for\s+)?(?:approval|review|asking))\b`), false},
	{KindRoleTag, regexp.MustCompile(`(?i)<\s*/?\s*(?:system|assistant|human|user|instructions?|im_start|im_end|function_results|function_calls|tool_result|tool_use|antml:[a-z_]+)\s*/?\s*>|<\|im_(?:start|end)\|>|\[/?INST\]|<<\s*/?\s*SYS\s*>>|\[/?(?:SYSTEM|ASSISTANT)\]`), false},
	{KindRoleTag, regexp.MustCompile(`(?im)^[ \t]*(?:human|assistant|system)[ \t]*:`), false},
	{KindWrapperTag, regexp.MustCompile(`(?i)<\s*/?\s*(?:untrusted|tool_output)\b|\[verification\]`), false},
}

// base64Candidate finds runs that may be base64 or base64url. 24 encoded
// characters decode to 18 bytes: shorter blobs cannot hold an instruction.
var base64Candidate = regexp.MustCompile(`[A-Za-z0-9+/_-]{24,}={0,2}`)

// Scan returns the instruction-like spans in s, ordered by offset. It never
// panics, scans at most MaxInputBytes and returns at most MaxFindings.
func Scan(s string) []Finding {
	if len(s) > MaxInputBytes {
		s = s[:MaxInputBytes]
	}
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	norm, origin, changed := normalize(s)
	found := matchRules(norm, origin)

	// Report obfuscation once, when the normalised text matched something the
	// raw text alone did not.
	if changed && len(found) > 0 {
		raw := matchRules(s, identity(len(s)))
		if len(raw) < len(found) {
			first := found[0]
			found = append(found, Finding{Kind: KindObfuscated, Offset: first.Offset, Excerpt: first.Excerpt})
		}
	}
	found = append(found, scanBase64(norm, origin)...)
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

func matchRules(text string, origin []int) []Finding {
	var out []Finding
	for _, r := range rules {
		for _, loc := range r.re.FindAllStringSubmatchIndex(text, MaxFindings) {
			if r.nounGuard && len(loc) >= 4 && loc[2] >= 0 {
				continue
			}
			out = append(out, Finding{Kind: r.kind, Offset: origin[loc[0]], Excerpt: excerpt(text[loc[0]:loc[1]])})
		}
	}
	return out
}

// scanBase64 decodes up to maxBlobs candidate blobs and scans the decoded text
// with the plain rules (no further decoding, so nesting cannot multiply work).
func scanBase64(norm string, origin []int) []Finding {
	var out []Finding
	for _, loc := range base64Candidate.FindAllStringIndex(norm, maxBlobs) {
		blob := norm[loc[0]:loc[1]]
		if len(blob) > maxBlobBytes {
			blob = blob[:maxBlobBytes]
		}
		dec, ok := decodeBase64(blob)
		if !ok {
			continue
		}
		inner, _, _ := normalize(dec)
		hits := matchRules(inner, identity(len(inner)))
		if len(hits) == 0 {
			continue
		}
		out = append(out, Finding{Kind: KindEncoded, Offset: origin[loc[0]], Excerpt: excerpt("base64: " + hits[0].Excerpt)})
	}
	return out
}

// decodeBase64 tries the standard and URL alphabets, padded or not, after
// trimming the blob to whole 4-character groups, and accepts only output that
// is mostly printable UTF-8 text.
func decodeBase64(blob string) (string, bool) {
	trimmed := strings.TrimRight(blob, "=")
	if n := len(trimmed) % 4; n == 1 {
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
	return total > 0 && ok*10 >= total*9
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
	if len(s) <= MaxExcerptBytes {
		return s
	}
	n := MaxExcerptBytes
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
