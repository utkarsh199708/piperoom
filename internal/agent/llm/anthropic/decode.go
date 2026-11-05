package anthropic

import (
	"encoding/json"

	sdk "github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/agent/llm"
)

// DecodeMessage turns an assistant message into a neutral response. Every
// part keeps the block as received in Native, so EncodeRequest sends it back
// byte for byte (history is append-only: thinking signatures and compaction
// blocks must return unchanged). Text and tool_use blocks also fill their
// neutral fields; every other block (thinking, redacted thinking, compaction)
// is a PartReasoning.
//
// provider is the adapter's provider and model the model the request named;
// the response's own model id, when it has one, is the one recorded.
func DecodeMessage(provider, model string, msg sdk.BetaMessage) llm.Response {
	served := msg.Model
	if served == "" {
		served = model
	}
	resp := llm.Response{
		Message:      llm.Message{Role: llm.RoleAssistant},
		Stop:         stopReason(msg.StopReason),
		ProviderStop: string(msg.StopReason),
		Usage:        decodeUsage(msg.Usage),
		Model:        msg.Model,
	}
	if resp.Stop == llm.StopRefusal {
		resp.StopDetail = refusalDetail(msg.StopDetails)
	}
	if len(msg.Content) > 0 {
		resp.Message.Parts = make([]llm.Part, 0, len(msg.Content))
	}
	for _, b := range msg.Content {
		part := llm.Part{Native: &llm.Native{Provider: provider, Model: served, Raw: rawBlock(b)}}
		switch b.Type {
		case "text":
			part.Kind = llm.PartText
			part.Text = b.Text
		case "tool_use":
			part.Kind = llm.PartToolCall
			part.ToolCall = &llm.ToolCall{ID: b.ID, Name: b.Name, Input: append(json.RawMessage(nil), b.Input...)}
		default:
			part.Kind = llm.PartReasoning
		}
		resp.Message.Parts = append(resp.Message.Parts, part)
	}
	return resp
}

// rawBlock is the block as received. A block built in code (a test's struct
// literal) has no raw JSON; its request form stands in, which decodes to the
// same block.
func rawBlock(b sdk.BetaContentBlockUnion) json.RawMessage {
	if raw := b.RawJSON(); raw != "" {
		return json.RawMessage(raw)
	}
	raw, err := json.Marshal(b.ToParam())
	if err != nil {
		return nil
	}
	return raw
}

// stopReason maps the Messages API stop reason. stop_sequence ends the turn
// like end_turn; an unknown or missing reason is StopOther, which the agent
// continues like tool_use, as it did before the switch-over.
func stopReason(r sdk.BetaStopReason) llm.StopReason {
	switch r {
	case sdk.BetaStopReasonEndTurn, sdk.BetaStopReasonStopSequence:
		return llm.StopEndTurn
	case sdk.BetaStopReasonToolUse:
		return llm.StopToolUse
	case sdk.BetaStopReasonMaxTokens:
		return llm.StopMaxTokens
	case sdk.BetaStopReasonPauseTurn:
		return llm.StopPause
	case sdk.BetaStopReasonRefusal:
		return llm.StopRefusal
	case sdk.BetaStopReasonModelContextWindowExceeded:
		return llm.StopContextExceeded
	}
	return llm.StopOther
}

// refusalDetail renders the structured refusal details as one short string:
// "category: explanation", or whichever of the two is set, or "".
func refusalDetail(d sdk.BetaRefusalStopDetails) string {
	switch {
	case d.Category == "":
		return d.Explanation
	case d.Explanation == "":
		return string(d.Category)
	}
	return string(d.Category) + ": " + d.Explanation
}

// decodeUsage reads the four counters. A server-side compaction pass is
// billed but reported outside the top-level fields, as usage iterations of
// type "compaction"; their sum is Usage.Compaction (nil when none ran).
func decodeUsage(u sdk.BetaUsage) llm.Usage {
	out := llm.Usage{
		Input:      u.InputTokens,
		CacheWrite: u.CacheCreationInputTokens,
		CacheRead:  u.CacheReadInputTokens,
		Output:     u.OutputTokens,
	}
	for _, it := range u.Iterations {
		if it.Type != "compaction" {
			continue
		}
		if out.Compaction == nil {
			out.Compaction = &llm.Usage{}
		}
		out.Compaction.Input += it.InputTokens
		out.Compaction.CacheWrite += it.CacheCreationInputTokens
		out.Compaction.CacheRead += it.CacheReadInputTokens
		out.Compaction.Output += it.OutputTokens
	}
	return out
}
