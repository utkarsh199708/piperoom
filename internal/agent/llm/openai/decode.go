package openai

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"piperoom/internal/agent/llm"
)

// The chat completions response, as much of it as the adapter reads.
type wireResponse struct {
	Model   string       `json:"model"`
	Choices []wireChoice `json:"choices"`
	Usage   *wireUsage   `json:"usage"`
}

type wireChoice struct {
	Message struct {
		Content   json.RawMessage `json:"content"` // a string, null, or text parts
		Refusal   *string         `json:"refusal"`
		ToolCalls []struct {
			ID       string `json:"id"`
			Function struct {
				Name string `json:"name"`
				// Arguments is a JSON string on OpenAI; some local servers
				// send the object itself.
				Arguments json.RawMessage `json:"arguments"`
			} `json:"function"`
		} `json:"tool_calls"`
	} `json:"message"`
	FinishReason string `json:"finish_reason"`
	// ContentFilterResults is Azure's per-category filter verdict.
	ContentFilterResults map[string]wireFilterResult `json:"content_filter_results"`
}

type wireFilterResult struct {
	Filtered bool `json:"filtered"`
}

type wireUsage struct {
	PromptTokens        int64 `json:"prompt_tokens"`
	CompletionTokens    int64 `json:"completion_tokens"`
	PromptTokensDetails *struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	CompletionTokensDetails *struct {
		ReasoningTokens int64 `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
}

// decode reads a 200 answer into the neutral response. reqBytes is the size
// of the request sent, for the usage estimate.
func (m *Model) decode(req llm.Request, raw []byte, reqBytes int) (llm.Response, error) {
	var w wireResponse
	if err := json.Unmarshal(raw, &w); err != nil {
		return llm.Response{}, fmt.Errorf("llm/openai: decode response: %w", err)
	}
	if len(w.Choices) == 0 {
		return llm.Response{}, fmt.Errorf("llm/openai: the response has no choice")
	}
	c := w.Choices[0]
	resp := llm.Response{
		Message:      llm.Message{Role: llm.RoleAssistant},
		ProviderStop: c.FinishReason,
		Model:        w.Model,
		Usage:        usage(w.Usage, reqBytes, len(raw)),
	}
	if resp.Model == "" {
		resp.Model = req.Model
	}
	text, err := contentText(c.Message.Content)
	if err != nil {
		return llm.Response{}, err
	}
	if text != "" {
		resp.Message.Parts = append(resp.Message.Parts, llm.Part{Kind: llm.PartText, Text: text})
	}
	for _, tc := range c.Message.ToolCalls {
		id := tc.ID
		if id == "" {
			// Some local servers send no id; the history needs one to pair
			// the result with its call.
			id = newCallID()
		}
		resp.Message.Parts = append(resp.Message.Parts, llm.Part{Kind: llm.PartToolCall, ToolCall: &llm.ToolCall{
			ID: id, Name: tc.Function.Name, Input: arguments(tc.Function.Arguments),
		}})
	}

	refusal := c.Message.Refusal != nil && *c.Message.Refusal != ""
	switch {
	case refusal:
		resp.Stop, resp.StopDetail = llm.StopRefusal, *c.Message.Refusal
	case c.FinishReason == "content_filter":
		resp.Stop, resp.StopDetail = llm.StopRefusal, filterDetail(c.ContentFilterResults)
	case len(c.Message.ToolCalls) > 0:
		resp.Stop = llm.StopToolUse
	case c.FinishReason == "stop":
		resp.Stop = llm.StopEndTurn
	case c.FinishReason == "length":
		resp.Stop = llm.StopMaxTokens
	case c.FinishReason == "tool_calls", c.FinishReason == "function_call":
		// The finish reason says tools, but no call came: nothing to run.
		resp.Stop = llm.StopOther
	default:
		resp.Stop = llm.StopOther
	}
	return resp, nil
}

// contentText reads message.content: a string, null, or an array of text
// parts (some compatible servers).
func contentText(raw json.RawMessage) (string, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", nil
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", fmt.Errorf("llm/openai: decode content: %w", err)
		}
		return s, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", fmt.Errorf("llm/openai: decode content: %w", err)
	}
	var b strings.Builder
	for _, p := range parts {
		if p.Type == "text" {
			b.WriteString(p.Text)
		}
	}
	return b.String(), nil
}

// arguments returns a tool call's arguments as the model wrote them: the
// string's text, or a raw object from a server that sends one. Text that is
// not JSON is kept: the agent answers it with an error tool result
// (rejected invalid_json) so the call and its result stay paired.
func arguments(raw json.RawMessage) json.RawMessage {
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			if strings.TrimSpace(s) == "" {
				return json.RawMessage(`{}`)
			}
			return json.RawMessage(s)
		}
	}
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return json.RawMessage(`{}`)
	}
	return append(json.RawMessage(nil), raw...)
}

// usage normalises the reported usage (design 3): prompt_tokens includes
// cached tokens, which move to CacheRead; completion_tokens includes
// reasoning. With no usage, or an all-zero one, it estimates (design 8.3).
func usage(u *wireUsage, reqBytes, respBytes int) llm.Usage {
	if u == nil || (u.PromptTokens == 0 && u.CompletionTokens == 0) {
		return llm.Usage{Input: estimateTokens(reqBytes), Output: estimateTokens(respBytes), Estimated: true}
	}
	var cached, reasoning int64
	if u.PromptTokensDetails != nil {
		cached = min(max(u.PromptTokensDetails.CachedTokens, 0), u.PromptTokens)
	}
	if u.CompletionTokensDetails != nil {
		reasoning = max(u.CompletionTokensDetails.ReasoningTokens, 0)
	}
	return llm.Usage{
		Input:     u.PromptTokens - cached,
		CacheRead: cached,
		Output:    u.CompletionTokens,
		Reasoning: reasoning,
	}
}

// filterDetail names the categories a content filter blocked (Azure), else
// says only that the filter stopped the answer.
func filterDetail(results map[string]wireFilterResult) string {
	var hit []string
	for k, r := range results {
		if r.Filtered {
			hit = append(hit, k)
		}
	}
	if len(hit) == 0 {
		return "content_filter"
	}
	sort.Strings(hit)
	return "content_filter: " + strings.Join(hit, ", ")
}

// newCallID makes a tool call id for a server that sent none.
func newCallID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "call_" + hex.EncodeToString(b[:])
}
