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
		Content json.RawMessage `json:"content"` // a string, null, or text parts
		Refusal *string         `json:"refusal"`
		// ReasoningContent is DeepSeek's thinking (deepseek-reasoner).
		ReasoningContent string `json:"reasoning_content"`
		ToolCalls        []struct {
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
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	// PromptCacheHitTokens is DeepSeek's cached share of prompt_tokens
	// (prompt_tokens = hit + miss).
	PromptCacheHitTokens int64 `json:"prompt_cache_hit_tokens"`
	PromptTokensDetails  *struct {
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
	if m.flavor.name == FlavorDeepSeek && c.Message.ReasoningContent != "" {
		// Kept as provider state, never as text: the room shows the answer,
		// and the thinking goes back only by the rule of deepSeekReasoning.
		resp.Message.Parts = append(resp.Message.Parts, deepSeekReasoningPart(c.Message.ReasoningContent, req.Model))
	}
	if text != "" {
		resp.Message.Parts = append(resp.Message.Parts, llm.Part{Kind: llm.PartText, Text: text})
	}
	for _, tc := range c.Message.ToolCalls {
		id := tc.ID
		if id == "" {
			// Some local servers send no id; the history needs one to pair
			// the result with its call.
			id = newCallID(m.flavor.name)
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
		if resp.StopDetail == "content_filter" && text != "" {
			// A host without categories (DeepSeek, DashScope, Mistral)
			// explains the stop in the content.
			resp.StopDetail += ": " + text
		}
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
		cached = u.PromptTokensDetails.CachedTokens
	}
	// DeepSeek reports its cache as prompt_cache_hit_tokens; a host that
	// sends both fields counts the same tokens twice, so the larger wins.
	cached = min(max(cached, u.PromptCacheHitTokens, 0), u.PromptTokens)
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

// newCallID makes a tool call id for a server that sent none. Mistral
// accepts only ids of 9 letters and digits.
func newCallID(flavorName string) string {
	if flavorName == FlavorMistral {
		const alnum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		var b [9]byte
		_, _ = rand.Read(b[:])
		for i := range b {
			b[i] = alnum[int(b[i])%len(alnum)]
		}
		return string(b[:])
	}
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "call_" + hex.EncodeToString(b[:])
}

// deepSeekNative is the Native.Raw of a DeepSeek reasoning part: the
// flavor, so no other host of this provider gets it back, and the text.
type deepSeekNative struct {
	Flavor           string `json:"flavor"`
	ReasoningContent string `json:"reasoning_content"`
}

// deepSeekReasoningPart keeps DeepSeek's reasoning_content as provider
// state, bound to the model that made it.
func deepSeekReasoningPart(text, model string) llm.Part {
	raw, _ := json.Marshal(deepSeekNative{Flavor: FlavorDeepSeek, ReasoningContent: text})
	return llm.Part{Kind: llm.PartReasoning, Native: &llm.Native{Provider: llm.ProviderOpenAI, Model: model, Raw: raw}}
}

// deepSeekReasoning returns the reasoning_content of p when p is a DeepSeek
// reasoning part this adapter made for model.
//
// The rule its fixture proves (TestDeepSeekReasoningRule): DeepSeek's
// thinking mode with tool calls wants the reasoning_content of the turns
// that answer the current question sent back while the tool loop runs, and
// the reasoning of earlier questions left out (the API documents that it
// ignores or refuses them). So encode sends it back only on the assistant
// turns after the last user message that asks a new question
// (currentQuestion), and only to the model that made it.
func deepSeekReasoning(p llm.Part, model string) (string, bool) {
	if p.Native == nil || p.Native.Provider != llm.ProviderOpenAI || p.Native.Model != model {
		return "", false
	}
	var n deepSeekNative
	if json.Unmarshal(p.Native.Raw, &n) != nil || n.Flavor != FlavorDeepSeek || n.ReasoningContent == "" {
		return "", false
	}
	return n.ReasoningContent, true
}
