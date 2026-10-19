package injectscan

import (
	"regexp"
	"sort"
	"strings"
)

// toolNames are the repo's real action and tool names (A2's actions, A2/A3's
// read tools, C11's runbook tool, A4's room tools). Rules capture any
// snake_case word and check it here, which keeps the regexps small and fast.
var toolNames = setOf(
	"connect_update_connector_config", "connect_restart_task", "connect_restart_connector", "connect_resume_connector",
	"schema_set_compatibility", "schema_register",
	"propose_fix", "update_hypothesis", "post_message",
	"connect_list_connectors", "connect_status", "connect_config", "kafka_topic_offsets", "kafka_list_groups",
	"kafka_consumer_lag", "kafka_find_by_key", "kafka_topic_config", "schema_registry_subjects",
	"schema_registry_versions", "schema_registry_diff", "pg_replication_slots", "pg_recent_ddl",
	"pg_long_transactions", "pg_row", "pg_publication_tables", "warehouse_freshness", "warehouse_row",
	"pipeline_map", "trace_record", "runbook_lookup",
)

// roomToolNames exist only inside the agent: operations text never tells
// anyone to run them.
var roomToolNames = setOf("propose_fix", "update_hypothesis", "post_message")

// addresseePattern names the AI. It makes the conditional rules count: "you
// must restart the connector" is ordinary advice in a Postgres HINT, but not
// next to "note to the AI agent". Words that are common in ops text on their
// own ("assistant", "ai") count only in phrases that address someone ("the
// assistant", "dear assistant", "AI agent", a line starting "assistant:");
// the rarer names (claude, llm, gpt, chatbot) count bare. Any match that
// touches - _ . @ / is a hostname, service, topic or e-mail and is dropped
// (see touchesIdent).
const addresseePattern = `\b(?:the|an|dear|hey|to|for|attention)\s*:?\s+(?:ai|assistant|chatbot)\b` +
	`|\b(?:ai|a\.i\.)\s+(?:agent|assistant|model|investigator)s?\b` +
	`|(?m:^[ \t]*(?:ai|assistant)[ \t]*[:,])` +
	`|\b(?:claude|llm|chatgpt|gpt|chatbot)\b` +
	`|\b(?:automated|investigating|investigation|incident|autonomous)\s+(?:agent|investigator)\b` +
	`|\b(?:as|you\s+are)\s+(?:the|an?)\s+(?:ai|assistant|llm|language\s+model|claude|chatbot)\b` +
	`|\byou\s+are\s+now\b`

// determiner before a verb makes it a noun ("the same call x makes").
const determiner = `(\b(?:the|same|a|an|this|that|each|every|one|which|its|first|last)\s+)?`

// snakeName captures a snake_case identifier (checked against toolNames).
const snakeName = `([a-z]+(?:_[a-z]+)+)`

// rule is one pattern. Patterns run on the normalised text lowered to ASCII
// lower case (byte offsets are unchanged), unless cased is set. Go's regexp is
// RE2, so each runs in time linear in the input.
type rule struct {
	kind Kind
	re   *regexp.Regexp
	// keys are literals one of which must occur for the rule to run; the
	// rule then runs only on windows around their occurrences.
	keys []string
	// cased runs the rule on the normalised text without lowering.
	cased bool
	// conditional keeps a match only when an addressee (addresseeWords) or
	// an unconditional finding lies within contextBytes of it. These rules
	// run only on windows around such anchors.
	conditional bool
	// nounGuard drops a match whose first capture group matched (a
	// determiner before the verb).
	nounGuard bool
	// names, when set, requires capture group 2 to be one of these names.
	names map[string]bool
	// whole runs the rule on the whole text: its pattern is anchored to the
	// start of the text or of a line, which a window start would fake.
	whole bool
	// identGuard drops a match that touches - _ . @ / at either end: part of
	// an identifier, not words addressed to someone.
	identGuard bool
	// distinctRoles keeps the matches only when at least two different role
	// labels (capture group 1) match: a dialogue, not one "system: ..." log
	// line.
	distinctRoles bool
}

var rules = []rule{
	// ---- override ----
	{kind: KindOverride, keys: []string{"ignore", "disregard", "forget", "override", "bypass"},
		re: regexp.MustCompile(`\b(?:ignore|disregard|forget|override|bypass)\s+(?:(?:all|any|the|your|these|those|of|every|my)\s+)*(?:(?:previous|prior|above|earlier|preceding|former|system|safety|original|existing|other)\s+)?(?:instructions?|rules|prompts?|directions|directives|guidelines|guidance|guardrails|constraints|policies|context)\b` +
			`|\bdisregard\s+(?:everything|the\s+above|all\s+(?:of\s+)?(?:the\s+)?above|what\s+(?:you|i)\s+(?:were|was|have\s+been)\s+told)\b`)},
	{kind: KindOverride, keys: []string{"you are now", "from now on", "new ", "your ", "prompt", "message", " mode"},
		re: regexp.MustCompile(`\byou\s+are\s+now\s+(?:a|an|the|my|acting|operating|free|unrestricted|jailbroken|in\s+\w+\s+mode|\w*gpt)\b` +
			`|\bfrom\s+now\s+on,?\s+(?:you|the\s+(?:assistant|agent|ai|model))\b` +
			`|\bnew\s+(?:instructions?|task|orders|directives?|system\s+message)\s*:` +
			`|\byour\s+(?:real|new|actual|true|only|updated|current)\s+(?:job|task|goal|instructions?|mission|role|purpose|orders)\b` +
			`|\b(?:your|reveal|print|show|repeat|ignore|override|leak|output|dump)\s+(?:the\s+|your\s+)?(?:system|hidden|developer|initial)\s+(?:prompt|message)\b` +
			`|\b(?:developer|god|jailbreak|dan)\s+mode\b`)},
	{kind: KindOverride, conditional: true,
		re: regexp.MustCompile(`\b(?:do\s+not|don't)\s+(?:mention|reveal|disclose|report|tell\s+(?:anyone|the\s+(?:user|humans?|engineers?|driver)))\b` +
			`|\bdisregard\s+(?:all|any|previous|prior|your|earlier)\b`)},

	// ---- addressed ----
	{kind: KindAddressed, keys: addresseeKeys,
		re: regexp.MustCompile(`\b(?:as|you\s+are)\s+(?:the|an?)\s+(?:ai|assistant|llm|language\s+model|claude|chatbot)\b` +
			`|\b(?:to|for|attention|dear|hey)\s*:?\s+(?:the\s+)?(?:ai(?:\s+(?:agent|assistant|model|investigator))?|assistant|llm|claude|chatbot|chatgpt|gpt|(?:automated|investigating|investigation|incident|autonomous)\s+(?:agent|investigator))\b`), identGuard: true},

	// ---- tool requests ----
	{kind: KindToolRequest, nounGuard: true, names: roomToolNames, keys: []string{"propose_fix", "update_hypothesis", "post_message"},
		re: regexp.MustCompile(determiner + `\b(?:call|invoke|run|use|execute|trigger|submit)\s+(?:the\s+)?(?:tool\s+)?` + snakeName + `\b`)},
	{kind: KindToolRequest, names: toolNames, keys: []string{`instruction"`, `instructions"`, `next_action"`, `next_step"`, `action_required"`, `todo"`},
		re: regexp.MustCompile(`(")(?:instructions?|next_action|next_step|action_required|todo)"\s*:\s*"\s*` + snakeName + `\b`)},
	{kind: KindToolRequest, conditional: true, nounGuard: true, names: toolNames,
		re: regexp.MustCompile(determiner + `\b(?:call|invoke|run|use|execute|propose|trigger|perform|submit|issue|apply|approve|do)\s+(?:the\s+|a\s+|an\s+)?(?:tool\s+|action\s+)?` + snakeName + `\b`)},
	{kind: KindToolRequest, conditional: true,
		re: regexp.MustCompile(`\b(?:please|now|immediately|you\s+(?:must|should|need\s+to|have\s+to|are\s+(?:required|authori[sz]ed)\s+to))\s*,?\s+(?:(?:also|just|then|go\s+ahead\s+and)\s+)?(?:restart|resume|reset|register|approve|propose|call|run|execute|skip|delete|drop|truncate|set)\b` +
			`|\b(?:pre-?approved|auto-?approve[ds]?|approval\s+is\s+not\s+(?:needed|required)|without\s+(?:waiting\s+for\s+)?(?:approval|review|asking))\b`)},

	// ---- role tags ----
	{kind: KindRoleTag, keys: []string{"system", "assistant", "human", "user", "instruction", "im_", "function_", "tool_", "antml", "inst]", "sys"},
		re: regexp.MustCompile(`<\s*/?\s*(?:system|assistant|human|user|instructions?|im_start|im_end|function_results|function_calls|tool_result|tool_use|antml:[a-z_]+)\s*/?\s*>|<\|im_(?:start|end)\|>|\[/?inst\]|<<\s*/?\s*sys\s*>>|\[/?(?:system|assistant)\]`)},
	{kind: KindRoleTag, cased: true, whole: true, keys: []string{"Human", "Assistant"},
		re: regexp.MustCompile(`(?:\A|\n[ \t]*\n)[ \t]*(?:Human|Assistant)[ \t]*:`)},
	{kind: KindRoleTag, distinctRoles: true, whole: true, keys: []string{"human", "assistant", "system", "user"},
		re: regexp.MustCompile(`(?m)^[ \t]*(human|assistant|system|user)[ \t]*:`)},

	// ---- wrapper tags ----
	{kind: KindWrapperTag, keys: []string{"untrusted", "tool_output", "[verification]"},
		re: regexp.MustCompile(`<\s*/?\s*(?:untrusted|tool_output)\b|\[verification\]`)},
}

// addressee marks where the text names the AI (context for conditional rules).
var addressee = regexp.MustCompile(addresseePattern)

// addresseeKeys prefilter the addressee search.
var addresseeKeys = []string{"ai", "a.i.", "assistant", "llm", "claude", "chatbot", "gpt", "language", "investigat", "you are now"}

// minBlob is the shortest base64 run worth decoding: 24 encoded characters
// decode to 18 bytes, too short for anything but a fragment.
const minBlob = 24

// base64Runs returns [start, end) of up to max runs of base64 or base64url
// characters (with up to two trailing '=') at least minBlob long. A plain
// byte loop: a regexp here was the slowest part of a scan.
func base64Runs(s string, limit int) [][2]int {
	var out [][2]int
	start := -1
	for i := 0; i <= len(s) && len(out) < limit; i++ {
		if i < len(s) && isB64(s[i]) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 && i-start >= minBlob {
			end := i
			for n := 0; n < 2 && end < len(s) && s[end] == '='; n++ {
				end++
			}
			out = append(out, [2]int{start, end})
		}
		start = -1
	}
	return out
}

func isB64(c byte) bool {
	return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '+' || c == '/' || c == '_' || c == '-'
}

const (
	keyBefore = 128 // window bytes scanned before a key occurrence
	keyAfter  = 256 // and after it
)

func hasAny(s string, keys []string) bool {
	for _, k := range keys {
		if strings.Contains(s, k) {
			return true
		}
	}
	return false
}

func setOf(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

// lowerASCII lowers only ASCII letters, so byte offsets stay the same.
func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

// scanText runs every rule on text; origin maps text offsets to input offsets.
func scanText(text string, origin []int) []Finding {
	lower := lowerASCII(text)
	var out []Finding
	var anchors []int
	if hasAny(lower, addresseeKeys) {
		// Whole text: the pattern has a line-start branch that a window
		// start would fake.
		for _, loc := range addressee.FindAllStringIndex(lower, MaxFindings) {
			if !touchesIdent(lower, loc[0], loc[1]) {
				anchors = append(anchors, loc[0])
			}
		}
	}
	for _, r := range rules {
		if r.conditional {
			continue
		}
		subject := lower
		if r.cased {
			subject = text
		}
		var spans [][2]int
		switch {
		case !hasAny(subject, r.keys):
			continue
		case r.whole:
			spans = [][2]int{{0, len(subject)}}
		default:
			spans = windows(subject, keyOffsets(subject, r.keys), keyBefore, keyAfter)
		}
		fs := runRule(r, subject, text, spans)
		for _, f := range fs {
			anchors = append(anchors, f.Offset)
		}
		out = append(out, fs...)
	}
	out = append(out, hiddenComments(lower, text)...)
	if len(anchors) > 0 {
		// Conditional rules run only near anchors, and keep only matches
		// that start within contextBytes of one.
		sort.Ints(anchors)
		spans := windows(lower, anchors, contextBytes+keyBefore, contextBytes+keyAfter)
		for _, r := range rules {
			if !r.conditional {
				continue
			}
			for _, f := range runRule(r, lower, text, spans) {
				if near(anchors, f.Offset) {
					out = append(out, f)
				}
			}
		}
	}
	for i := range out {
		out[i].Offset = origin[out[i].Offset]
		out[i].End = origin[out[i].End]
	}
	return out
}

// runRule runs r on the given windows of subject and returns its findings with
// offsets into subject.
func runRule(r rule, subject, text string, spans [][2]int) []Finding {
	var locs [][]int
	for _, sp := range spans {
		for _, loc := range r.re.FindAllStringSubmatchIndex(subject[sp[0]:sp[1]], MaxFindings) {
			for i := range loc {
				if loc[i] >= 0 {
					loc[i] += sp[0]
				}
			}
			locs = append(locs, loc)
		}
	}
	if r.distinctRoles && !distinctRoleLabels(subject, locs) {
		return nil
	}
	var out []Finding
	for _, loc := range locs {
		if r.nounGuard && len(loc) >= 4 && loc[2] >= 0 {
			continue
		}
		if r.identGuard && touchesIdent(subject, loc[0], loc[1]) {
			continue
		}
		if r.names != nil && (len(loc) < 6 || loc[4] < 0 || !r.names[subject[loc[4]:loc[5]]]) {
			continue
		}
		out = append(out, Finding{Kind: r.kind, Offset: loc[0], End: clauseEnd(subject, loc[1]), Excerpt: excerpt(text[loc[0]:loc[1]])})
	}
	return out
}

// clauseEnd returns the end of the clause a match ending at end belongs to:
// the next line break, sentence stop (. ! ? ; followed by a space, a closing
// quote or bracket, or the end), comment close or closing tag, at most
// SpanTailBytes further (then widened to the next word boundary, so a name
// is never cut in half).
func clauseEnd(s string, end int) int {
	limit := min(len(s), end+SpanTailBytes)
	for i := end; i < limit; i++ {
		switch c := s[i]; {
		case c == '\n':
			return i
		case c == '.' || c == '!' || c == '?' || c == ';':
			if i+1 == len(s) || strings.IndexByte(" \t\r\n'\")]}>", s[i+1]) >= 0 {
				return i
			}
		case c == '-' && strings.HasPrefix(s[i:], "-->"),
			c == '<' && strings.HasPrefix(s[i:], "</"):
			return i
		}
	}
	for n := 0; limit < len(s) && n < 64 && isWordByte(s[limit]); n++ {
		limit++
	}
	return limit
}

// keyOffsets returns the sorted offsets of every occurrence of keys in s,
// at most maxKeyHits of them (past that the whole text is scanned anyway).
func keyOffsets(s string, keys []string) []int {
	const maxKeyHits = 512
	var offs []int
	for _, k := range keys {
		for from := 0; len(offs) < maxKeyHits; {
			i := strings.Index(s[from:], k)
			if i < 0 {
				break
			}
			offs = append(offs, from+i)
			from += i + len(k)
		}
	}
	if len(offs) >= maxKeyHits {
		// Too many to be worth it: offsets that make windows cover s.
		offs = offs[:0]
		for o := 0; o < len(s); o += keyAfter {
			offs = append(offs, o)
		}
		return append(offs, len(s))
	}
	sort.Ints(offs)
	return offs
}

// windows turns sorted offsets into merged [lo, hi) spans reaching before and
// after bytes around each, widened to the nearest word boundary so a span
// never starts or ends inside a word.
func windows(s string, offs []int, before, after int) [][2]int {
	var out [][2]int
	for _, o := range offs {
		lo, hi := max(0, o-before), min(len(s), o+after)
		for n := 0; lo > 0 && n < 64 && isWordByte(s[lo-1]); n++ {
			lo--
		}
		for n := 0; hi < len(s) && n < 64 && isWordByte(s[hi]); n++ {
			hi++
		}
		if len(out) > 0 && lo <= out[len(out)-1][1] {
			out[len(out)-1][1] = max(out[len(out)-1][1], hi)
			continue
		}
		out = append(out, [2]int{lo, hi})
	}
	return out
}

func isWordByte(c byte) bool {
	return c == '_' || c >= 0x80 || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')
}

// distinctRoleLabels reports a dialogue: at least two different role labels,
// one of them human or assistant ("user: alice\nsystem: linux" is a config
// listing, not a chat transcript).
func distinctRoleLabels(s string, locs [][]int) bool {
	roles := map[string]bool{}
	for _, loc := range locs {
		if len(loc) >= 4 && loc[2] >= 0 {
			roles[s[loc[2]:loc[3]]] = true
		}
	}
	return len(roles) >= 2 && (roles["human"] || roles["assistant"])
}

// touchesIdent reports whether s[start:end] has - _ . @ or / right before or
// after it, which makes it part of a hostname, service, topic or e-mail.
func touchesIdent(s string, start, end int) bool {
	const ident = "-_.@/"
	return (start > 0 && strings.IndexByte(ident, s[start-1]) >= 0) ||
		(end < len(s) && strings.IndexByte(ident, s[end]) >= 0)
}

// near reports whether a sorted anchor lies within contextBytes of off.
func near(anchors []int, off int) bool {
	i := sort.SearchInts(anchors, off-contextBytes)
	return i < len(anchors) && anchors[i] <= off+contextBytes
}

// hiddenComments flags <!-- ... --> comments whose first commentBytes name the
// AI: a comment is invisible when the text is rendered, so it has no business
// addressing an assistant.
func hiddenComments(lower, text string) []Finding {
	var out []Finding
	from := 0
	for n := 0; n < maxComments; n++ {
		i := strings.Index(lower[from:], "<!--")
		if i < 0 {
			break
		}
		start := from + i
		end := min(len(lower), start+commentBytes)
		spanEnd := end
		if j := strings.Index(lower[start+4:end], "-->"); j >= 0 {
			end = start + 4 + j
			spanEnd = end + len("-->")
		}
		if addressee.MatchString(lower[start+4 : max(start+4, end)]) {
			out = append(out, Finding{Kind: KindHiddenComment, Offset: start, End: spanEnd, Excerpt: excerpt(text[start:end])})
		}
		from = start + 4
	}
	return out
}
