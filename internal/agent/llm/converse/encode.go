package converse

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"piperoom/internal/agent/llm"
)

// outputToolName is the forced tool Turn adds for a request that carries
// OutputSchema (a caller outside the agent's own fallback, such as the evals
// judge). The agent's fallback strips OutputSchema first and names its tool
// emit_<what> itself.
const outputToolName = "emit_output"

// encodeRequest builds the Converse input. It changes no text: every
// PartText and ToolResult.Content goes on the wire byte for byte (design
// 10.1), and untrusted text is only ever in a user text or a tool result.
func encodeRequest(req llm.Request, fam family) (*bedrockruntime.ConverseInput, error) {
	in := &bedrockruntime.ConverseInput{ModelId: aws.String(req.Model)}
	if req.MaxTokens > 0 {
		in.InferenceConfig = &types.InferenceConfiguration{MaxTokens: aws.Int32(clampInt32(req.MaxTokens))}
	}
	if req.System != "" {
		in.System = []types.SystemContentBlock{&types.SystemContentBlockMemberText{Value: req.System}}
		if req.Cache.System && fam.cacheSystem {
			in.System = append(in.System, &types.SystemContentBlockMemberCachePoint{Value: cachePoint()})
		}
	}
	// Effort and Thinking are dropped: Converse has no common field for them
	// (Capabilities.Effort and .Reasoning are false).

	msgs, usesTools, err := encodeMessages(req.Messages, fam)
	if err != nil {
		return nil, err
	}
	if req.Cache.History && fam.cacheMessages && len(msgs) > 0 {
		last := &msgs[len(msgs)-1]
		last.Content = append(last.Content, &types.ContentBlockMemberCachePoint{Value: cachePoint()})
	}
	in.Messages = msgs

	tools := req.Tools
	choice := req.ToolChoice
	if req.OutputSchema != nil {
		emit := llm.ToolDef{
			Name:        outputToolName,
			Description: "Return your answer as this tool's input. Call it exactly once; its input is the whole answer.",
			Schema:      req.OutputSchema,
		}
		if fam.forcedTool {
			tools = append(append([]llm.ToolDef(nil), tools...), emit)
		} else {
			// The choice cannot be forced on this family: the emit tool is
			// the only one offered, so calling it is the only tool call the
			// model can make, and a caller still reads a JSON object from the
			// text when the model answers in prose (as V2a's fallback does).
			tools = []llm.ToolDef{emit}
		}
		choice = llm.ToolChoiceRequired
	}
	if choice == llm.ToolChoiceNone && !usesTools {
		// Converse has no "none": with no tool blocks in the history the
		// tools can be left out; with them, Converse needs toolConfig, so the
		// tools stay and no toolChoice is sent (the model decides).
		tools = nil
	}
	if len(tools) > 0 {
		tc, err := encodeTools(tools, choice == llm.ToolChoiceRequired && fam.forcedTool, req.Cache.Tools && fam.cacheTools)
		if err != nil {
			return nil, err
		}
		in.ToolConfig = tc
	}
	return in, nil
}

func cachePoint() types.CachePointBlock {
	return types.CachePointBlock{Type: types.CachePointTypeDefault}
}

func clampInt32(n int64) int32 {
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n)
}

// encodeTools builds toolConfig. toolChoice is sent only to force a tool
// (any) on a family that takes it: auto is Converse's default, and some
// families (Meta Llama) reject an explicit toolChoice.
func encodeTools(defs []llm.ToolDef, force, cache bool) (*types.ToolConfiguration, error) {
	tc := &types.ToolConfiguration{}
	for _, d := range defs {
		schema, err := docValue(d.Schema)
		if err != nil {
			return nil, fmt.Errorf("llm/converse: tool %s: schema: %w", d.Name, err)
		}
		spec := types.ToolSpecification{
			Name:        aws.String(d.Name),
			InputSchema: &types.ToolInputSchemaMemberJson{Value: document.NewLazyDocument(schema)},
		}
		if d.Description != "" {
			spec.Description = aws.String(d.Description)
		}
		// Strict is not sent: design 3, Converse has no strict schemas
		// (Capabilities.StrictTools is false); the agent validates every
		// input in code.
		tc.Tools = append(tc.Tools, &types.ToolMemberToolSpec{Value: spec})
	}
	if cache {
		tc.Tools = append(tc.Tools, &types.ToolMemberCachePoint{Value: cachePoint()})
	}
	if force {
		tc.ToolChoice = &types.ToolChoiceMemberAny{Value: types.AnyToolChoice{}}
	}
	return tc, nil
}

// encodeMessages translates the history (design 7.3). Converse wants roles
// that alternate and blocks that are not empty, so a message left with no
// block (only another provider's reasoning) is dropped and two neighbours of
// the same role are merged. usesTools reports a toolUse or toolResult block,
// which makes toolConfig required.
func encodeMessages(msgs []llm.Message, fam family) ([]types.Message, bool, error) {
	var out []types.Message
	usesTools := false
	for i, m := range msgs {
		var role types.ConversationRole
		switch m.Role {
		case llm.RoleUser:
			role = types.ConversationRoleUser
		case llm.RoleAssistant:
			role = types.ConversationRoleAssistant
		default:
			return nil, false, fmt.Errorf("llm/converse: message %d: unknown role %q", i, m.Role)
		}
		// Tool results first, then the rest in order: every toolUse must be
		// answered before any other user text (design 7.3).
		var results, rest []types.ContentBlock
		for j, p := range m.Parts {
			switch p.Kind {
			case llm.PartText:
				if p.Text != "" {
					rest = append(rest, &types.ContentBlockMemberText{Value: p.Text})
				}
			case llm.PartToolCall:
				if p.ToolCall == nil {
					return nil, false, fmt.Errorf("llm/converse: message %d part %d: tool_call without its call", i, j)
				}
				rest = append(rest, &types.ContentBlockMemberToolUse{Value: types.ToolUseBlock{
					ToolUseId: aws.String(p.ToolCall.ID),
					Name:      aws.String(p.ToolCall.Name),
					Input:     document.NewLazyDocument(toolInput(p.ToolCall.Input)),
				}})
				usesTools = true
			case llm.PartToolResult:
				r := p.ToolResult
				if r == nil {
					return nil, false, fmt.Errorf("llm/converse: message %d part %d: tool_result without its result", i, j)
				}
				block := types.ToolResultBlock{
					ToolUseId: aws.String(r.CallID),
					Content:   []types.ToolResultContentBlock{&types.ToolResultContentBlockMemberText{Value: r.Content}},
				}
				if fam.toolStatus {
					block.Status = types.ToolResultStatusSuccess
					if r.IsError {
						block.Status = types.ToolResultStatusError
					}
				}
				results = append(results, &types.ContentBlockMemberToolResult{Value: block})
				usesTools = true
			case llm.PartReasoning:
				// Dropped (design 7.3): Converse reasoning blocks are not sent
				// back, and another provider's are not ours.
			default:
				return nil, false, fmt.Errorf("llm/converse: message %d part %d: unknown kind %q", i, j, p.Kind)
			}
		}
		content := append(results, rest...)
		if len(content) == 0 {
			continue
		}
		if n := len(out); n > 0 && out[n-1].Role == role {
			out[n-1].Content = append(out[n-1].Content, content...)
			continue
		}
		out = append(out, types.Message{Role: role, Content: content})
	}
	return out, usesTools, nil
}

// toolInput is a tool call's arguments as a document value. Arguments that
// are not a JSON object (the agent answered such a call with an error
// result, design 7.3) go back as an empty object: Converse wants an object
// and the call must stay paired with its result.
func toolInput(raw json.RawMessage) any {
	v, err := jsonValue(raw)
	if err != nil {
		return map[string]any{}
	}
	if _, ok := v.(map[string]any); !ok {
		return map[string]any{}
	}
	return v
}

// docValue turns a Go value (a schema map) into a document value through its
// JSON form, so every number keeps its exact value.
func docValue(v any) (any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return jsonValue(raw)
}

// jsonValue decodes raw into maps, slices and scalars the document encoder
// writes back as JSON numbers. The encoder would write a json.Number as a
// string, so an integer becomes an int64 (a *big.Int beyond it), exact, and
// any other number a float64.
func jsonValue(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("trailing data after the JSON value")
	}
	return numbers(v)
}

func numbers(v any) (any, error) {
	switch t := v.(type) {
	case map[string]any:
		for k, e := range t {
			n, err := numbers(e)
			if err != nil {
				return nil, err
			}
			t[k] = n
		}
		return t, nil
	case []any:
		for i, e := range t {
			n, err := numbers(e)
			if err != nil {
				return nil, err
			}
			t[i] = n
		}
		return t, nil
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, nil
		}
		if b, ok := new(big.Int).SetString(t.String(), 10); ok {
			return b, nil
		}
		f, err := t.Float64()
		if err != nil {
			return nil, fmt.Errorf("number %q: %w", t.String(), err)
		}
		return f, nil
	}
	return v, nil
}
