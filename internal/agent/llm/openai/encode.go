package openai

import (
	"bytes"
	"encoding/json"
	"fmt"

	"piperoom/internal/agent/llm"
)

// The chat completions request, as much of it as the adapter sends.
type wireRequest struct {
	Model               string              `json:"model"`
	Messages            []wireMessage       `json:"messages"`
	Tools               []wireTool          `json:"tools,omitempty"`
	ToolChoice          string              `json:"tool_choice,omitempty"`
	MaxCompletionTokens int64               `json:"max_completion_tokens,omitempty"`
	MaxTokens           int64               `json:"max_tokens,omitempty"`
	ReasoningEffort     string              `json:"reasoning_effort,omitempty"`
	ResponseFormat      *wireResponseFormat `json:"response_format,omitempty"`
}

type wireMessage struct {
	Role string `json:"role"`
	// Content is a string, a []wireText, or nil (omitted) on an assistant
	// message that only calls tools.
	Content    any            `json:"content,omitempty"`
	ToolCalls  []wireToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type wireText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type wireToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function wireFunction `json:"function"`
}

type wireFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type wireTool struct {
	Type     string       `json:"type"`
	Function wireToolSpec `json:"function"`
}

type wireToolSpec struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters"`
	Strict      bool           `json:"strict,omitempty"`
}

type wireResponseFormat struct {
	Type       string         `json:"type"`
	JSONSchema wireJSONSchema `json:"json_schema"`
}

type wireJSONSchema struct {
	Name   string         `json:"name"`
	Schema map[string]any `json:"schema"`
	Strict bool           `json:"strict,omitempty"`
}

// outputSchemaName names the response_format schema; the API wants
// ^[a-zA-Z0-9_-]{1,64}$.
const outputSchemaName = "output"

// encode renders req as a chat completions body (design 7.3). Text and tool
// results go on the wire as the agent built them; the encoder never
// HTML-escapes, so the decoded text is byte for byte the neutral one.
func (m *Model) encode(req llm.Request) ([]byte, error) {
	caps := m.Capabilities(req.Model)
	w := wireRequest{Model: req.Model}

	if req.System != "" {
		role := "system"
		if m.flavor.name == FlavorOpenAI && reasoningModel(req.Model) {
			role = "developer"
		}
		w.Messages = append(w.Messages, wireMessage{Role: role, Content: req.System})
	}
	for i, msg := range req.Messages {
		out, err := encodeMessage(msg)
		if err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
		w.Messages = append(w.Messages, out...)
	}
	for _, t := range req.Tools {
		params := t.Schema
		if params == nil {
			params = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		w.Tools = append(w.Tools, wireTool{Type: "function", Function: wireToolSpec{
			Name:        t.Name,
			Description: t.Description,
			Parameters:  params,
			// Strict only where the provider enforces it and the schema is
			// already in its subset; no adapter rewrites a schema (7.2).
			Strict: t.Strict && caps.StrictTools && strictSchema(params),
		}})
	}
	if len(w.Tools) > 0 {
		switch req.ToolChoice {
		case llm.ToolChoiceNone:
			w.ToolChoice = "none"
		case llm.ToolChoiceRequired:
			w.ToolChoice = "required"
		}
	}
	if req.MaxTokens > 0 {
		if m.flavor.hosted {
			w.MaxCompletionTokens = req.MaxTokens
		} else {
			w.MaxTokens = req.MaxTokens
		}
	}
	if caps.Effort {
		w.ReasoningEffort = reasoningEffort(req.Effort)
	}
	if req.OutputSchema != nil && caps.StructuredOutput {
		w.ResponseFormat = &wireResponseFormat{Type: "json_schema", JSONSchema: wireJSONSchema{
			Name:   outputSchemaName,
			Schema: req.OutputSchema,
			Strict: m.flavor.hosted && strictSchema(req.OutputSchema),
		}}
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(w); err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// reasoningEffort maps the agent's effort to reasoning_effort: low, medium
// and high as they are; xhigh and max to high, the highest every reasoning
// model takes; anything else is dropped.
func reasoningEffort(e string) string {
	switch e {
	case "low", "medium", "high":
		return e
	case "xhigh", "max":
		return "high"
	}
	return ""
}

// encodeMessage renders one neutral message. A user message's tool results
// go first, one tool message each, then its text in one user message; an
// assistant message is one message with its text and tool calls. A
// reasoning part is provider state this adapter never makes; it is dropped.
func encodeMessage(msg llm.Message) ([]wireMessage, error) {
	var texts []string
	var out []wireMessage
	switch msg.Role {
	case llm.RoleUser:
		for _, p := range msg.Parts {
			switch p.Kind {
			case llm.PartToolResult:
				if p.ToolResult == nil {
					return nil, fmt.Errorf("a tool_result part without its result")
				}
				// IsError has no wire flag: the agent's wrapped text already
				// says error (design 7.3).
				out = append(out, wireMessage{Role: "tool", ToolCallID: p.ToolResult.CallID, Content: p.ToolResult.Content})
			case llm.PartText:
				texts = append(texts, p.Text)
			case llm.PartToolCall:
				return nil, fmt.Errorf("a tool call in a user message")
			}
		}
		if len(texts) > 0 {
			out = append(out, wireMessage{Role: "user", Content: textContent(texts)})
		}
		return out, nil
	case llm.RoleAssistant:
		a := wireMessage{Role: "assistant"}
		for _, p := range msg.Parts {
			switch p.Kind {
			case llm.PartText:
				texts = append(texts, p.Text)
			case llm.PartToolCall:
				if p.ToolCall == nil {
					return nil, fmt.Errorf("a tool_call part without its call")
				}
				a.ToolCalls = append(a.ToolCalls, wireToolCall{ID: p.ToolCall.ID, Type: "function", Function: wireFunction{
					Name: p.ToolCall.Name,
					// The arguments go back exactly as the model sent them.
					Arguments: string(p.ToolCall.Input),
				}})
			case llm.PartToolResult:
				return nil, fmt.Errorf("a tool result in an assistant message")
			}
		}
		switch {
		case len(texts) > 0:
			a.Content = textContent(texts)
		case len(a.ToolCalls) == 0:
			a.Content = ""
		}
		return []wireMessage{a}, nil
	}
	return nil, fmt.Errorf("unknown role %q", msg.Role)
}

// textContent is one text as a string, several as content parts, so no
// part's bytes change by joining.
func textContent(texts []string) any {
	if len(texts) == 1 {
		return texts[0]
	}
	parts := make([]wireText, len(texts))
	for i, t := range texts {
		parts[i] = wireText{Type: "text", Text: t}
	}
	return parts
}

// strictKeywords are the JSON Schema keywords strict mode takes (OpenAI
// structured outputs, 2026-10). A schema with any other keyword is sent
// non-strict.
var strictKeywords = map[string]bool{
	"type": true, "properties": true, "required": true, "additionalProperties": true,
	"items": true, "enum": true, "const": true, "anyOf": true, "description": true, "title": true,
	"$ref": true, "$defs": true, "definitions": true,
	"pattern": true, "format": true,
	"minimum": true, "maximum": true, "exclusiveMinimum": true, "exclusiveMaximum": true, "multipleOf": true,
	"minItems": true, "maxItems": true,
}

// strictSchema reports a schema already in the strict subset: every object
// lists every property in required and sets additionalProperties false, and
// only strictKeywords appear, all the way down.
func strictSchema(s map[string]any) bool {
	for k := range s {
		if !strictKeywords[k] {
			return false
		}
	}
	props, hasProps := s["properties"].(map[string]any)
	if hasProps || typeIs(s["type"], "object") {
		if ap, ok := s["additionalProperties"].(bool); !ok || ap {
			return false
		}
		req := stringSet(s["required"])
		for name, sub := range props {
			if !req[name] {
				return false
			}
			m, ok := sub.(map[string]any)
			if !ok || !strictSchema(m) {
				return false
			}
		}
	}
	if items, ok := s["items"]; ok {
		m, ok := items.(map[string]any)
		if !ok || !strictSchema(m) {
			return false
		}
	}
	if anyOf, ok := s["anyOf"]; ok {
		list, ok := anyOf.([]any)
		if !ok {
			return false
		}
		for _, sub := range list {
			m, ok := sub.(map[string]any)
			if !ok || !strictSchema(m) {
				return false
			}
		}
	}
	for _, k := range []string{"$defs", "definitions"} {
		if defs, ok := s[k]; ok {
			dm, ok := defs.(map[string]any)
			if !ok {
				return false
			}
			for _, sub := range dm {
				m, ok := sub.(map[string]any)
				if !ok || !strictSchema(m) {
					return false
				}
			}
		}
	}
	return true
}

func typeIs(v any, want string) bool {
	switch t := v.(type) {
	case string:
		return t == want
	case []any:
		for _, e := range t {
			if s, ok := e.(string); ok && s == want {
				return true
			}
		}
	case []string:
		for _, s := range t {
			if s == want {
				return true
			}
		}
	}
	return false
}

func stringSet(v any) map[string]bool {
	out := map[string]bool{}
	switch t := v.(type) {
	case []string:
		for _, s := range t {
			out[s] = true
		}
	case []any:
		for _, e := range t {
			if s, ok := e.(string); ok {
				out[s] = true
			}
		}
	}
	return out
}
