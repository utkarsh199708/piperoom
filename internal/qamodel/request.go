package qamodel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// apiRequest is the part of a Messages API request the puppet reads. Unknown
// fields are ignored: the SDK adds new ones over time.
type apiRequest struct {
	Model        string          `json:"model"`
	MaxTokens    int             `json:"max_tokens"`
	System       json.RawMessage `json:"system"`
	Messages     []apiMessage    `json:"messages"`
	Tools        []apiTool       `json:"tools"`
	ToolChoice   json.RawMessage `json:"tool_choice"`
	Stream       bool            `json:"stream"`
	Thinking     *apiThinking    `json:"thinking"`
	OutputConfig *apiOutput      `json:"output_config"`
}

type apiMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type apiTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type apiThinking struct {
	Type string `json:"type"`
}

type apiOutput struct {
	Effort string          `json:"effort"`
	Format json.RawMessage `json:"format"`
}

// block is one content block of a message. Fields not used by the block's type
// stay empty.
type block struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Thinking  string          `json:"thinking"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	IsError   bool            `json:"is_error"`
	Content   json.RawMessage `json:"content"`
	raw       json.RawMessage
}

// blocks reads a message content: a plain string is one text block.
func blocks(content json.RawMessage) []block {
	var s string
	if json.Unmarshal(content, &s) == nil {
		return []block{{Type: "text", Text: s}}
	}
	var raws []json.RawMessage
	if json.Unmarshal(content, &raws) != nil {
		return nil
	}
	out := make([]block, 0, len(raws))
	for _, r := range raws {
		b := block{raw: r}
		if err := json.Unmarshal(r, &b); err != nil {
			b = block{Type: "invalid", raw: r}
		}
		out = append(out, b)
	}
	return out
}

// textOf joins the text of a content that is a string or a list of blocks (a
// tool_result's content, a system prompt).
func textOf(content json.RawMessage) string {
	var parts []string
	for _, b := range blocks(content) {
		switch b.Type {
		case "text":
			parts = append(parts, b.Text)
		case "image", "document":
			parts = append(parts, "["+b.Type+"]")
		}
	}
	return strings.Join(parts, "\n")
}

func (r *apiRequest) systemText() string { return textOf(r.System) }

func (r *apiRequest) toolNames() []string {
	names := make([]string, 0, len(r.Tools))
	for _, t := range r.Tools {
		if t.Name != "" {
			names = append(names, t.Name)
		}
	}
	return names
}

// requiredFields maps each tool to the input fields its schema requires.
func (r *apiRequest) requiredFields() map[string][]string {
	out := map[string][]string{}
	for _, t := range r.Tools {
		var schema struct {
			Required []string `json:"required"`
		}
		if json.Unmarshal(t.InputSchema, &schema) == nil && len(schema.Required) > 0 {
			out[t.Name] = schema.Required
		}
	}
	return out
}

// thinkingOn reports whether the request lets the model think, which is when a
// thinking block in the reply is legal.
func (r *apiRequest) thinkingOn() bool {
	return r.Thinking != nil && (r.Thinking.Type == "enabled" || r.Thinking.Type == "adaptive")
}

// structured reports whether the request demands JSON output (hand-off,
// post-mortem and first-look verdict requests do).
func (r *apiRequest) structured() bool {
	if r.OutputConfig == nil {
		return false
	}
	f := bytes.TrimSpace(r.OutputConfig.Format)
	return len(f) > 0 && !bytes.Equal(f, []byte("null"))
}

// firstUserText is the text of the first user message.
func (r *apiRequest) firstUserText() string {
	for _, m := range r.Messages {
		if m.Role == "user" {
			return textOf(m.Content)
		}
	}
	return ""
}

var (
	roomLine  = regexp.MustCompile(`(?m)^Room: (\S+)`)
	alertLine = regexp.MustCompile(`(?m)^Alert: (.*)$`)
)

// roomHint names the room a request belongs to, so a tester can tell rooms
// apart: the room id and alert title from the agent's opening message, or the
// first line of the first user message for requests that have neither (a
// hand-off carries the room's events).
func roomHint(r *apiRequest) string {
	first := r.firstUserText()
	var parts []string
	if m := roomLine.FindStringSubmatch(first); m != nil {
		parts = append(parts, m[1])
	}
	if m := alertLine.FindStringSubmatch(first); m != nil {
		title := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(m[1]), "<untrusted>"), "</untrusted>")
		if title = strings.TrimSpace(title); title != "" {
			parts = append(parts, `"`+cutRunes(title, 80)+`"`)
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, " ")
	}
	line, _, _ := strings.Cut(strings.TrimSpace(first), "\n")
	return cutRunes(line, 80)
}

// lastMessageExcerpt is a short plain rendering of the final message, kept in
// the log after the request itself is released.
func lastMessageExcerpt(r *apiRequest) string {
	if len(r.Messages) == 0 {
		return ""
	}
	var b strings.Builder
	renderMessage(&b, r.Messages[len(r.Messages)-1], 0)
	return cutRunes(strings.TrimSpace(b.String()), 600)
}

// cutRunes shortens s to at most n bytes on a rune boundary, adding an ellipsis
// when it cut.
func cutRunes(s string, n int) string {
	head, rest := cut(s, n)
	if rest == 0 {
		return s
	}
	return head + "..."
}

// cut splits s at a rune boundary at or before n bytes.
func cut(s string, n int) (head string, rest int) {
	if n < 0 || len(s) <= n {
		return s, 0
	}
	i := n
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return s[:i], len(s) - i
}

// clip shortens s for the text rendering and says how to get all of it.
func clip(s string, limit int) string {
	if limit <= 0 {
		return s
	}
	head, rest := cut(s, limit)
	if rest == 0 {
		return s
	}
	return fmt.Sprintf("%s\n... [%d more bytes; add ?full=1]", head, rest)
}

// renderOpts selects what the text rendering of a turn contains.
type renderOpts struct {
	system bool // include the system prompt
	tools  bool // tool descriptions and input schemas, not only signatures
	full   bool // do not clip blocks of earlier messages
	from   int  // first message index to show
}

// clipBytes is how much of one block of an earlier message the default
// rendering shows. The last message is always shown whole.
const clipBytes = 2000

// renderTurn is the readable form of a pending turn's request. The request
// carries tool output (record values, error traces), which is untrusted, so
// control characters are shown as escapes: raw ones could drive the terminal of
// whoever runs curl.
func renderTurn(t *turn, req *apiRequest, opts renderOpts) string {
	return visibleControls(renderTurnRaw(t, req, opts))
}

// visibleControls replaces control characters other than newline and tab with
// \xNN (or \uNNNN for the C1 range).
func visibleControls(s string) string {
	if !strings.ContainsFunc(s, isControl) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 16)
	for _, r := range s {
		switch {
		case !isControl(r):
			b.WriteRune(r)
		case r < 0x80:
			fmt.Fprintf(&b, `\x%02x`, r)
		default:
			fmt.Fprintf(&b, `\u%04x`, r)
		}
	}
	return b.String()
}

func isControl(r rune) bool {
	return (r < 0x20 && r != '\n' && r != '\t') || (r >= 0x7f && r <= 0x9f)
}

func renderTurnRaw(t *turn, req *apiRequest, opts renderOpts) string {
	var b strings.Builder
	fmt.Fprintf(&b, "turn %s  model %s  room %s\n", t.id, t.model, orDash(t.roomHint))
	fmt.Fprintf(&b, "received %s, reply by %s\n", t.received.UTC().Format("15:04:05Z"), t.deadline.UTC().Format("15:04:05Z"))
	fmt.Fprintf(&b, "messages %d, tools %d, max_tokens %d, thinking %s, effort %s, tool_choice %s\n",
		len(req.Messages), len(t.tools), req.MaxTokens, thinkingLabel(req), effortLabel(req), toolChoiceLabel(req))
	if t.structured {
		b.WriteString("This request demands JSON output: the reply text must be a JSON document matching the schema below.\n")
		b.WriteString("output schema: " + compactJSON(req.OutputConfig.Format) + "\n")
	}
	fmt.Fprintf(&b, "reply with: POST /qa/turns/%s/reply\n", t.id)

	if opts.system {
		sys := req.systemText()
		fmt.Fprintf(&b, "\n== SYSTEM (%d bytes) ==\n%s\n", len(sys), sys)
	}

	from := min(max(opts.from, 0), len(req.Messages))
	if from > 0 {
		fmt.Fprintf(&b, "\n== MESSAGES %d..%d of %d (earlier ones skipped by ?from=) ==\n", from, len(req.Messages)-1, len(req.Messages))
	} else {
		fmt.Fprintf(&b, "\n== MESSAGES (%d) ==\n", len(req.Messages))
	}
	for i := from; i < len(req.Messages); i++ {
		fmt.Fprintf(&b, "--- [%d] %s ---\n", i, req.Messages[i].Role)
		limit := clipBytes
		if opts.full || i == len(req.Messages)-1 {
			limit = 0
		}
		renderMessage(&b, req.Messages[i], limit)
	}

	fmt.Fprintf(&b, "\n== TOOLS (%d) ==\n", len(req.Tools))
	if opts.tools {
		for _, tl := range req.Tools {
			fmt.Fprintf(&b, "%s: %s\n  input_schema: %s\n", tl.Name, tl.Description, compactJSON(tl.InputSchema))
		}
	} else {
		for _, tl := range req.Tools {
			b.WriteString(toolSignature(tl) + "\n")
		}
		if len(req.Tools) > 0 {
			b.WriteString("(name(params), * = required; add ?tools=1 for descriptions and input schemas)\n")
		}
	}
	return b.String()
}

// renderMessage writes the blocks of one message; limit > 0 clips each block.
func renderMessage(b *strings.Builder, m apiMessage, limit int) {
	for _, bl := range blocks(m.Content) {
		switch bl.Type {
		case "text":
			b.WriteString("[text]\n" + clip(bl.Text, limit) + "\n")
		case "thinking":
			b.WriteString("[thinking]\n" + clip(bl.Thinking, limit) + "\n")
		case "tool_use":
			fmt.Fprintf(b, "[tool_use %s %s] %s\n", bl.ID, bl.Name, clip(compactJSON(bl.Input), limit))
		case "tool_result":
			status := "ok"
			if bl.IsError {
				status = "error"
			}
			fmt.Fprintf(b, "[tool_result %s %s]\n%s\n", bl.ToolUseID, status, clip(textOf(bl.Content), limit))
		default:
			// A block type the puppet does not know (a server-side compaction
			// block, for one): show what is in it rather than hide it.
			text := textOf(bl.Content)
			if text == "" {
				text = compactJSON(bl.raw)
			}
			fmt.Fprintf(b, "[%s]\n%s\n", bl.Type, clip(text, limit))
		}
	}
}

// toolSignature is name(param, required*) built from the input schema.
func toolSignature(t apiTool) string {
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	_ = json.Unmarshal(t.InputSchema, &schema) // a schema we cannot read shows no params
	req := map[string]bool{}
	for _, r := range schema.Required {
		req[r] = true
	}
	names := make([]string, 0, len(schema.Properties))
	for n := range schema.Properties {
		names = append(names, n)
	}
	sort.Strings(names)
	for i, n := range names {
		if req[n] {
			names[i] = n + "*"
		}
	}
	return t.Name + "(" + strings.Join(names, ", ") + ")"
}

func compactJSON(raw json.RawMessage) string {
	var out bytes.Buffer
	if err := json.Compact(&out, raw); err != nil {
		return strings.TrimSpace(string(raw))
	}
	return out.String()
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func thinkingLabel(r *apiRequest) string {
	if r.Thinking == nil || r.Thinking.Type == "" {
		return "off"
	}
	return r.Thinking.Type
}

func effortLabel(r *apiRequest) string {
	if r.OutputConfig == nil || r.OutputConfig.Effort == "" {
		return "-"
	}
	return r.OutputConfig.Effort
}

func toolChoiceLabel(r *apiRequest) string {
	var tc struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(r.ToolChoice, &tc) != nil || tc.Type == "" {
		return "-"
	}
	return tc.Type
}
