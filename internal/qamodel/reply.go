package qamodel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Stop reasons a reply may carry. The agent loop handles each of them; the
// last four are the ways a QA run reaches its less common paths.
const (
	StopEndTurn   = "end_turn"
	StopToolUse   = "tool_use"
	StopMaxTokens = "max_tokens"
	StopPause     = "pause_turn"
	StopRefusal   = "refusal"
	StopContext   = "model_context_window_exceeded"
)

var stopReasons = map[string]bool{
	StopEndTurn: true, StopToolUse: true, StopMaxTokens: true,
	StopPause: true, StopRefusal: true, StopContext: true,
}

// maxUsageTokens bounds a usage override; it only has to fit comfortably in an
// int64 once the agent sums turns.
const maxUsageTokens = 100_000_000

// ReplyRequest is the body of POST /qa/turns/{id}/reply.
type ReplyRequest struct {
	// Text is the model's text. Hand-off, post-mortem and first-look verdict
	// requests need it to be JSON matching their schema.
	Text string `json:"text,omitempty"`
	// Thinking is sent as a thinking block when the request enabled thinking,
	// and ignored otherwise.
	Thinking string `json:"thinking,omitempty"`
	// ToolCalls are the tools the model calls this turn, in order.
	ToolCalls []ToolCallRequest `json:"tool_calls,omitempty"`
	// Stop is the stop reason. Default tool_use when ToolCalls is not empty,
	// else end_turn.
	Stop string `json:"stop,omitempty"`
	// Usage overrides the token counts, to exercise budgets and cost.
	Usage *UsageOverride `json:"usage,omitempty"`
	// Error makes the request fail with an HTTP error instead of a message.
	Error *ErrorRequest `json:"error,omitempty"`
}

// ToolCallRequest is one tool call of a reply.
type ToolCallRequest struct {
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input,omitempty"`
}

// UsageOverride sets token counts; a nil field keeps the default.
type UsageOverride struct {
	InputTokens              *int64 `json:"input_tokens,omitempty"`
	OutputTokens             *int64 `json:"output_tokens,omitempty"`
	CacheCreationInputTokens *int64 `json:"cache_creation_input_tokens,omitempty"`
	CacheReadInputTokens     *int64 `json:"cache_read_input_tokens,omitempty"`
}

// ErrorRequest describes an HTTP error answer.
type ErrorRequest struct {
	// Status is the HTTP status, 400 to 599; default 500.
	Status int `json:"status,omitempty"`
	// Type is the error type of the body; default follows Status.
	Type    string `json:"type,omitempty"`
	Message string `json:"message,omitempty"`
	// Retry lets the SDK retry the request, which parks a new turn for each
	// attempt. Default false: one turn, one failure.
	Retry bool `json:"retry,omitempty"`
}

// Reply is a reply as the puppet resolved it: defaults filled, ids assigned.
type Reply struct {
	Text      string     `json:"text,omitempty"`
	Thinking  string     `json:"thinking,omitempty"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
	Stop      string     `json:"stop,omitempty"`
	Usage     *Usage     `json:"usage,omitempty"`
	Error     *HTTPError `json:"error,omitempty"`
	// Fallback is true for the text the puppet sent when the deadline passed.
	Fallback bool `json:"fallback,omitempty"`
}

// ToolCall is a tool_use block of a resolved reply.
type ToolCall struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

// Usage is the token usage a reply reports.
type Usage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
}

// HTTPError is a resolved error answer.
type HTTPError struct {
	Status  int    `json:"status"`
	Type    string `json:"type"`
	Message string `json:"message"`
	Retry   bool   `json:"retry"`
}

// replyError is a reply the tester must fix; Extra goes into the JSON error
// body (the tool list on a typo, for one).
type replyError struct {
	msg   string
	extra map[string]any
}

func (e *replyError) Error() string { return e.msg }

// errorTypes names the error type of the statuses the API uses.
var errorTypes = map[int]string{
	400: "invalid_request_error", 401: "authentication_error", 403: "permission_error",
	404: "not_found_error", 413: "request_too_large", 429: "rate_limit_error",
	500: "api_error", 529: "overloaded_error",
}

// resolveReply validates rr against the turn it answers and fills in the
// defaults. It also returns notes for things it ignored.
func resolveReply(t *turn, rr ReplyRequest) (*Reply, []string, error) {
	var notes []string
	if rr.Error != nil {
		if rr.Text != "" || rr.Thinking != "" || len(rr.ToolCalls) > 0 || rr.Stop != "" || rr.Usage != nil {
			return nil, nil, &replyError{msg: "error cannot be combined with text, thinking, tool_calls, stop or usage"}
		}
		e := *rr.Error
		if e.Status == 0 {
			e.Status = 500
		}
		if e.Status < 400 || e.Status > 599 {
			return nil, nil, &replyError{msg: fmt.Sprintf("error.status %d: want 400 to 599", e.Status)}
		}
		if e.Type == "" {
			e.Type = errorTypes[e.Status]
			if e.Type == "" {
				e.Type = "api_error"
			}
		}
		if e.Message == "" {
			e.Message = "qamodel: injected error"
		}
		return &Reply{Error: &HTTPError{Status: e.Status, Type: e.Type, Message: e.Message, Retry: e.Retry}}, nil, nil
	}

	if len(rr.ToolCalls) > 0 && len(t.tools) == 0 {
		return nil, nil, &replyError{msg: "this request offers no tools, so tool_calls must be empty"}
	}
	rep := &Reply{Text: rr.Text, ToolCalls: make([]ToolCall, 0, len(rr.ToolCalls))}
	if rr.Thinking != "" {
		if t.thinking {
			rep.Thinking = rr.Thinking
		} else {
			notes = append(notes, "thinking ignored: the request did not enable thinking")
		}
	}
	for i, c := range rr.ToolCalls {
		if c.Name == "" {
			return nil, nil, &replyError{msg: fmt.Sprintf("tool_calls[%d]: name is required", i), extra: map[string]any{"tools": t.tools}}
		}
		if !t.toolSet[c.Name] {
			return nil, nil, &replyError{
				msg:   fmt.Sprintf("tool_calls[%d]: unknown tool %q; this request offers the tools listed in \"tools\"", i, c.Name),
				extra: map[string]any{"tools": t.tools},
			}
		}
		in := bytes.TrimSpace(c.Input)
		if len(in) == 0 || bytes.Equal(in, []byte("null")) {
			in = []byte("{}")
		}
		if in[0] != '{' || !json.Valid(in) {
			return nil, nil, &replyError{msg: fmt.Sprintf("tool_calls[%d] (%s): input must be a JSON object", i, c.Name)}
		}
		// Not checked against the tool's input schema: a bad input is a case
		// worth sending, the agent validates it itself.
		var compact bytes.Buffer
		if err := json.Compact(&compact, in); err != nil {
			return nil, nil, &replyError{msg: fmt.Sprintf("tool_calls[%d] (%s): input: %v", i, c.Name, err)}
		}
		if missing := missingFields(t.required[c.Name], in); len(missing) > 0 {
			notes = append(notes, fmt.Sprintf("tool_calls[%d] (%s): input lacks required field(s) %s; sent anyway",
				i, c.Name, strings.Join(missing, ", ")))
		}
		rep.ToolCalls = append(rep.ToolCalls, ToolCall{
			ID:    fmt.Sprintf("toolu_qa_%s_%d", t.id, i),
			Name:  c.Name,
			Input: json.RawMessage(compact.Bytes()),
		})
	}
	if !utf8.ValidString(rep.Text) || !utf8.ValidString(rep.Thinking) {
		return nil, nil, &replyError{msg: "text and thinking must be valid UTF-8"}
	}

	rep.Stop = rr.Stop
	if rep.Stop == "" {
		rep.Stop = StopEndTurn
		if len(rep.ToolCalls) > 0 {
			rep.Stop = StopToolUse
		}
	}
	if !stopReasons[rep.Stop] {
		return nil, nil, &replyError{
			msg:   fmt.Sprintf("stop %q: want one of end_turn, tool_use, max_tokens, pause_turn, refusal, model_context_window_exceeded", rep.Stop),
			extra: map[string]any{"stop_reasons": []string{StopEndTurn, StopToolUse, StopMaxTokens, StopPause, StopRefusal, StopContext}},
		}
	}
	if rep.Stop == StopToolUse && len(rep.ToolCalls) == 0 {
		return nil, nil, &replyError{msg: "stop tool_use needs at least one entry in tool_calls", extra: map[string]any{"tools": t.tools}}
	}
	if rep.Text == "" && len(rep.ToolCalls) == 0 && rep.Stop != StopRefusal {
		return nil, nil, &replyError{msg: "empty reply: give text or tool_calls (or stop refusal)"}
	}

	rep.Usage = defaultUsage(t.messagesCount, rep)
	if u := rr.Usage; u != nil {
		fields := []struct {
			name string
			in   *int64
			dst  *int64
		}{
			{"input_tokens", u.InputTokens, &rep.Usage.InputTokens},
			{"output_tokens", u.OutputTokens, &rep.Usage.OutputTokens},
			{"cache_creation_input_tokens", u.CacheCreationInputTokens, &rep.Usage.CacheCreationInputTokens},
			{"cache_read_input_tokens", u.CacheReadInputTokens, &rep.Usage.CacheReadInputTokens},
		}
		for _, f := range fields {
			if f.in == nil {
				continue
			}
			if *f.in < 0 || *f.in > maxUsageTokens {
				return nil, nil, &replyError{msg: fmt.Sprintf("usage.%s %d: want 0 to %d", f.name, *f.in, maxUsageTokens)}
			}
			*f.dst = *f.in
		}
	}
	return rep, notes, nil
}

// missingFields lists the required fields absent from the JSON object in.
func missingFields(required []string, in []byte) []string {
	if len(required) == 0 {
		return nil
	}
	var have map[string]json.RawMessage
	if json.Unmarshal(in, &have) != nil {
		return nil
	}
	var missing []string
	for _, f := range required {
		if _, ok := have[f]; !ok {
			missing = append(missing, f)
		}
	}
	return missing
}

// defaultUsage gives small counts that depend only on the request's length and
// the reply, so a run's token totals are repeatable and stay far below any
// room budget.
func defaultUsage(messages int, rep *Reply) *Usage {
	out := int64(5 + (len(rep.Text)+len(rep.Thinking))/4 + 25*len(rep.ToolCalls))
	return &Usage{InputTokens: int64(100 + 10*messages), OutputTokens: out}
}

// chunkString splits s into pieces of at most n bytes without cutting a rune.
func chunkString(s string, n int) []string {
	var out []string
	for len(s) > 0 {
		head, rest := cut(s, n)
		if head == "" { // n is smaller than the first rune: take that rune whole
			_, size := utf8.DecodeRuneInString(s)
			head, rest = s[:size], len(s)-size
		}
		out = append(out, head)
		s = s[len(s)-rest:]
	}
	return out
}

// signature is the fixed signature of a thinking block. The real API signs
// thinking; the puppet is the only reader of what the agent sends back.
const signature = "qamodel-signature"

// renderSSE writes the whole Messages stream of rep, event by event as the
// real API does, so the SDK's stream accumulator reads it like a real turn.
func renderSSE(t *turn, rep *Reply) []byte {
	var b bytes.Buffer
	ev := func(name string, v any) {
		data, _ := json.Marshal(v) // maps, strings and numbers: cannot fail
		fmt.Fprintf(&b, "event: %s\ndata: %s\n\n", name, data)
	}
	usage := func(out int64) map[string]any {
		return map[string]any{
			"input_tokens":                rep.Usage.InputTokens,
			"output_tokens":               out,
			"cache_creation_input_tokens": rep.Usage.CacheCreationInputTokens,
			"cache_read_input_tokens":     rep.Usage.CacheReadInputTokens,
		}
	}
	ev("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id": "msg_qa_" + t.id, "type": "message", "role": "assistant", "model": t.model,
			"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
			"usage": usage(1),
		},
	})
	idx := 0
	start := func(cb map[string]any) {
		ev("content_block_start", map[string]any{"type": "content_block_start", "index": idx, "content_block": cb})
	}
	delta := func(d map[string]any) {
		ev("content_block_delta", map[string]any{"type": "content_block_delta", "index": idx, "delta": d})
	}
	stop := func() {
		ev("content_block_stop", map[string]any{"type": "content_block_stop", "index": idx})
		idx++
	}
	const chunk = 256
	if rep.Thinking != "" {
		start(map[string]any{"type": "thinking", "thinking": "", "signature": ""})
		for _, c := range chunkString(rep.Thinking, chunk) {
			delta(map[string]any{"type": "thinking_delta", "thinking": c})
		}
		delta(map[string]any{"type": "signature_delta", "signature": signature})
		stop()
	}
	if rep.Text != "" {
		start(map[string]any{"type": "text", "text": ""})
		for _, c := range chunkString(rep.Text, chunk) {
			delta(map[string]any{"type": "text_delta", "text": c})
		}
		stop()
	}
	for _, c := range rep.ToolCalls {
		// Like the API: the block starts with an empty input and the JSON
		// arrives as a partial_json delta.
		start(map[string]any{"type": "tool_use", "id": c.ID, "name": c.Name, "input": map[string]any{}})
		delta(map[string]any{"type": "input_json_delta", "partial_json": string(c.Input)})
		stop()
	}
	ev("message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": rep.Stop, "stop_sequence": nil},
		"usage": usage(rep.Usage.OutputTokens),
	})
	ev("message_stop", map[string]any{"type": "message_stop"})
	return b.Bytes()
}
