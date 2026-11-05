package anthropic

import (
	"encoding/json"
	"fmt"

	sdk "github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/agent/llm"
)

// EncodeRequest turns a neutral request into the Beta Messages request the
// agent sent before the switch-over, field for field:
//
//   - System is one text block, with a cache breakpoint when Cache.System;
//   - every tool is a custom tool with eager input streaming, strict when
//     ToolDef.Strict, and the last one carries a cache breakpoint when
//     Cache.Tools;
//   - Cache.History is the top-level cache_control (a breakpoint on the last
//     cacheable block, so each turn re-reads the history from cache);
//   - Effort and OutputSchema go in output_config, Thinking is adaptive
//     thinking;
//   - with tools, ToolChoiceAuto is sent as an explicit {"type":"auto"};
//   - Compaction is the compact-2026-01-12 beta with its context management
//     edit and instructions.
//
// A part this adapter decoded (Part.Native from a Claude provider) goes back
// as it came, so thinking signatures and compaction blocks stay valid; any
// other part is built from its neutral fields.
func EncodeRequest(req llm.Request) (sdk.BetaMessageNewParams, error) {
	p := sdk.BetaMessageNewParams{
		Model:     req.Model,
		MaxTokens: req.MaxTokens,
	}
	if req.Compaction != nil {
		p.Betas = []sdk.AnthropicBeta{sdk.AnthropicBetaCompact2026_01_12}
	}
	if req.System != "" {
		sys := sdk.BetaTextBlockParam{Text: req.System}
		if req.Cache.System {
			sys.CacheControl = sdk.NewBetaCacheControlEphemeralParam()
		}
		p.System = []sdk.BetaTextBlockParam{sys}
	}
	p.Tools = EncodeTools(req.Tools, req.Cache.Tools)
	msgs, err := encodeMessages(req.Messages)
	if err != nil {
		return sdk.BetaMessageNewParams{}, err
	}
	p.Messages = msgs
	if req.Effort != "" || req.OutputSchema != nil {
		p.OutputConfig = sdk.BetaOutputConfigParam{Effort: sdk.BetaOutputConfigEffort(req.Effort)}
		if req.OutputSchema != nil {
			p.OutputConfig.Format = sdk.BetaJSONOutputFormatParam{Schema: req.OutputSchema}
		}
	}
	if req.Thinking {
		p.Thinking = sdk.BetaThinkingConfigParamUnion{OfAdaptive: &sdk.BetaThinkingConfigAdaptiveParam{}}
	}
	if len(req.Tools) > 0 {
		switch req.ToolChoice {
		case llm.ToolChoiceAuto:
			p.ToolChoice = sdk.BetaToolChoiceUnionParam{OfAuto: &sdk.BetaToolChoiceAutoParam{}}
		case llm.ToolChoiceNone:
			p.ToolChoice = sdk.BetaToolChoiceUnionParam{OfNone: &sdk.BetaToolChoiceNoneParam{}}
		case llm.ToolChoiceRequired:
			p.ToolChoice = sdk.BetaToolChoiceUnionParam{OfAny: &sdk.BetaToolChoiceAnyParam{}}
		default:
			return sdk.BetaMessageNewParams{}, fmt.Errorf("llm/anthropic: unknown tool choice %q", req.ToolChoice)
		}
	}
	if req.Compaction != nil {
		p.ContextManagement = sdk.BetaContextManagementConfigParam{
			Edits: []sdk.BetaContextManagementConfigEditUnionParam{
				{OfCompact20260112: &sdk.BetaCompact20260112EditParam{
					Instructions: sdk.String(req.Compaction.Instructions),
				}},
			},
		}
	}
	if req.Cache.History {
		p.CacheControl = sdk.NewBetaCacheControlEphemeralParam()
	}
	return p, nil
}

// EncodeTools turns the tool definitions into Messages API tools, in order.
// cache puts a breakpoint after the last tool. A nil or empty list is nil.
//
// The schema's properties and required go in their own fields and every other
// key except "type" (always "object") goes in as an extra field, which is how
// the agent built input_schema before the switch-over; the order of the keys
// on the wire therefore stays properties, required, type, then the rest.
func EncodeTools(defs []llm.ToolDef, cache bool) []sdk.BetaToolUnionParam {
	if len(defs) == 0 {
		return nil
	}
	out := make([]sdk.BetaToolUnionParam, 0, len(defs))
	for i, d := range defs {
		tp := sdk.BetaToolParam{
			Name:                d.Name,
			Description:         sdk.String(d.Description),
			InputSchema:         inputSchema(d.Schema),
			EagerInputStreaming: sdk.Bool(true),
			Type:                sdk.BetaToolTypeCustom,
		}
		if d.Strict {
			tp.Strict = sdk.Bool(true)
		}
		if cache && i == len(defs)-1 {
			tp.CacheControl = sdk.NewBetaCacheControlEphemeralParam()
		}
		out = append(out, sdk.BetaToolUnionParam{OfTool: &tp})
	}
	return out
}

// inputSchema splits a JSON Schema object into the SDK's input_schema fields.
func inputSchema(schema map[string]any) sdk.BetaToolInputSchemaParam {
	var s sdk.BetaToolInputSchemaParam
	var extra map[string]any
	for k, v := range schema {
		switch k {
		case "type":
			// Always "object" on the wire (the SDK's constant).
		case "properties":
			s.Properties = v
		case "required":
			s.Required = requiredNames(v)
		default:
			if extra == nil {
				extra = map[string]any{}
			}
			extra[k] = v
		}
	}
	s.ExtraFields = extra
	return s
}

// requiredNames reads a schema's required list, as []string or []any.
func requiredNames(v any) []string {
	switch r := v.(type) {
	case []string:
		return r
	case []any:
		var out []string
		for _, x := range r {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func encodeMessages(msgs []llm.Message) ([]sdk.BetaMessageParam, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	out := make([]sdk.BetaMessageParam, 0, len(msgs))
	for i, m := range msgs {
		mp := sdk.BetaMessageParam{Role: sdk.BetaMessageParamRole(m.Role)}
		if m.Role == llm.RoleAssistant {
			// A decoded assistant turn went back with one block per content
			// block (BetaMessage.ToParam), never a nil slice.
			mp.Content = make([]sdk.BetaContentBlockParamUnion, 0, len(m.Parts))
		}
		for j, part := range m.Parts {
			block, ok, err := encodePart(part)
			if err != nil {
				return nil, fmt.Errorf("llm/anthropic: message %d part %d: %w", i, j, err)
			}
			if ok {
				mp.Content = append(mp.Content, block)
			}
		}
		out = append(out, mp)
	}
	return out, nil
}

// encodePart builds one content block. ok is false for a part this adapter
// drops: reasoning state another provider made.
func encodePart(part llm.Part) (block sdk.BetaContentBlockParamUnion, ok bool, err error) {
	if own(part.Native) {
		if !json.Valid(part.Native.Raw) {
			return block, false, fmt.Errorf("native block is not JSON")
		}
		var b sdk.BetaContentBlockUnion
		if err := b.UnmarshalJSON(part.Native.Raw); err != nil {
			return block, false, fmt.Errorf("native block: %w", err)
		}
		return b.ToParam(), true, nil
	}
	switch part.Kind {
	case llm.PartText:
		return sdk.NewBetaTextBlock(part.Text), true, nil
	case llm.PartToolResult:
		if part.ToolResult == nil {
			return block, false, fmt.Errorf("tool_result part without a result")
		}
		r := part.ToolResult
		return sdk.NewBetaToolResultBlock(r.CallID, r.Content, r.IsError), true, nil
	case llm.PartToolCall:
		if part.ToolCall == nil {
			return block, false, fmt.Errorf("tool_call part without a call")
		}
		c := part.ToolCall
		input := c.Input
		if !json.Valid(input) {
			input = json.RawMessage(`{}`)
		}
		return sdk.BetaContentBlockParamUnion{OfToolUse: &sdk.BetaToolUseBlockParam{
			ID: c.ID, Name: c.Name, Input: input,
		}}, true, nil
	case llm.PartReasoning:
		// Another provider's reasoning state means nothing to Claude.
		return block, false, nil
	}
	return block, false, fmt.Errorf("unknown part kind %q", part.Kind)
}

// own reports whether a native block came from this adapter: any of the
// three Claude providers, which share the Messages API wire format. A
// thinking block of one Claude model goes back to another unchanged; the API
// ignores what it cannot verify, as it did before the switch-over.
func own(n *llm.Native) bool {
	if n == nil || len(n.Raw) == 0 {
		return false
	}
	switch n.Provider {
	case llm.ProviderAnthropic, llm.ProviderBedrock, llm.ProviderVertex:
		return true
	}
	return false
}
