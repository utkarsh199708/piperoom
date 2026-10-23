package converse

import (
	"encoding/json"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"piperoom/internal/agent/llm"
)

// decodeOutput turns a Converse answer into a neutral response. Text and
// toolUse blocks fill their neutral fields; reasoning and every other block
// are dropped (design 7.3: Converse reasoning is not sent back). No part
// carries Native: the neutral fields are what goes back. Converse does not
// name the model that served the turn, so Response.Model is the request's.
func decodeOutput(model string, out *bedrockruntime.ConverseOutput, size *wireSize) llm.Response {
	resp := llm.Response{
		Message:      llm.Message{Role: llm.RoleAssistant},
		ProviderStop: string(out.StopReason),
		Model:        model,
	}
	var text []string
	if msg, ok := out.Output.(*types.ConverseOutputMemberMessage); ok {
		for _, b := range msg.Value.Content {
			switch v := b.(type) {
			case *types.ContentBlockMemberText:
				resp.Message.Parts = append(resp.Message.Parts, llm.Part{Kind: llm.PartText, Text: v.Value})
				text = append(text, v.Value)
			case *types.ContentBlockMemberToolUse:
				resp.Message.Parts = append(resp.Message.Parts, llm.Part{Kind: llm.PartToolCall, ToolCall: &llm.ToolCall{
					ID:    aws.ToString(v.Value.ToolUseId),
					Name:  aws.ToString(v.Value.Name),
					Input: toolUseInput(v.Value),
				}})
			}
		}
	}
	resp.Stop = stopReason(out.StopReason, hasToolCall(resp.Message.Parts))
	if resp.Stop == llm.StopRefusal {
		// A guardrail answers with its blocked-message text and a content
		// filter often with the model's explanation: that text is the
		// detail, after the reason.
		resp.StopDetail = string(out.StopReason)
		if t := strings.TrimSpace(strings.Join(text, "\n")); t != "" {
			resp.StopDetail += ": " + t
		}
	}
	resp.Usage = decodeUsage(out.Usage, size)
	return resp
}

// toolUseInput is the call's arguments as JSON. A document that cannot be
// re-encoded becomes invalid JSON on purpose, so the agent answers the call
// with its "arguments are not valid JSON" error result (design 7.3).
func toolUseInput(b types.ToolUseBlock) json.RawMessage {
	if b.Input == nil {
		return json.RawMessage(`{}`)
	}
	raw, err := b.Input.MarshalSmithyDocument()
	if err != nil {
		return json.RawMessage(`{`)
	}
	return raw
}

func hasToolCall(parts []llm.Part) bool {
	for _, p := range parts {
		if p.Kind == llm.PartToolCall {
			return true
		}
	}
	return false
}

// stopReason maps the Converse stop reason. A turn that holds a tool call is
// tool_use whatever the reason says, unless the output was cut, refused or
// overflowed. guardrail_intervened and content_filtered are refusals;
// malformed output or tool use and an unknown reason are StopOther, which
// the agent continues like tool_use.
func stopReason(r types.StopReason, toolCall bool) llm.StopReason {
	switch r {
	case types.StopReasonMaxTokens:
		return llm.StopMaxTokens
	case types.StopReasonGuardrailIntervened, types.StopReasonContentFiltered:
		return llm.StopRefusal
	case types.StopReasonModelContextWindowExceeded:
		return llm.StopContextExceeded
	}
	if toolCall {
		return llm.StopToolUse
	}
	switch r {
	case types.StopReasonEndTurn, types.StopReasonStopSequence:
		return llm.StopEndTurn
	case types.StopReasonToolUse:
		return llm.StopToolUse
	}
	return llm.StopOther
}

// decodeUsage normalises Converse usage: Input excludes cached tokens. An
// answer without usage gets the byte estimate of design 8.3.
//
// ASSUMPTION, to verify on the first live Nova call (V10 gate run, record
// the raw usage in the PR): Bedrock reports inputTokens without the cache
// reads and writes, and totalTokens as the sum of all four. The heuristic
// below also covers the other reading: a total that leaves the cache out
// (total == input + output) is taken to mean the cache was counted inside
// inputTokens, so it is subtracted. The fixtures are hand-written, not
// captured, so neither reading is proven yet.
func decodeUsage(u *types.TokenUsage, size *wireSize) llm.Usage {
	if u == nil || (u.InputTokens == nil && u.OutputTokens == nil) {
		return llm.Usage{
			Input:     ceilDiv(size.request(), 3),
			Output:    ceilDiv(size.response(), 3),
			Estimated: true,
		}
	}
	in := int64(aws.ToInt32(u.InputTokens))
	out := int64(aws.ToInt32(u.OutputTokens))
	read := int64(aws.ToInt32(u.CacheReadInputTokens))
	write := int64(aws.ToInt32(u.CacheWriteInputTokens))
	if cached := read + write; cached > 0 && u.TotalTokens != nil && int64(*u.TotalTokens) == in+out && in >= cached {
		in -= cached
	}
	return llm.Usage{Input: in, Output: out, CacheRead: read, CacheWrite: write}
}

func ceilDiv(n, d int64) int64 {
	if n <= 0 {
		return 0
	}
	return (n + d - 1) / d
}
