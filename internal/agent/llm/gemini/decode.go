package gemini

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"piperoom/internal/agent/llm"
)

type wireResponse struct {
	Candidates []struct {
		Content struct {
			Parts []json.RawMessage `json:"parts"`
		} `json:"content"`
		FinishReason  string `json:"finishReason"`
		FinishMessage string `json:"finishMessage"`
		SafetyRatings []struct {
			Category string `json:"category"`
			Blocked  bool   `json:"blocked"`
		} `json:"safetyRatings"`
	} `json:"candidates"`
	PromptFeedback *struct {
		BlockReason        string `json:"blockReason"`
		BlockReasonMessage string `json:"blockReasonMessage"`
	} `json:"promptFeedback"`
	UsageMetadata *struct {
		PromptTokenCount        int64 `json:"promptTokenCount"`
		CandidatesTokenCount    int64 `json:"candidatesTokenCount"`
		ThoughtsTokenCount      int64 `json:"thoughtsTokenCount"`
		CachedContentTokenCount int64 `json:"cachedContentTokenCount"`
		ToolUsePromptTokenCount int64 `json:"toolUsePromptTokenCount"`
	} `json:"usageMetadata"`
	ModelVersion string `json:"modelVersion"`
}

type wirePart struct {
	Text             *string           `json:"text"`
	Thought          bool              `json:"thought"`
	ThoughtSignature string            `json:"thoughtSignature"`
	FunctionCall     *wireFunctionCall `json:"functionCall"`
}

// refusalReasons are the finish reasons that mean the model's output was
// blocked (design 3).
var refusalReasons = map[string]bool{
	"SAFETY": true, "PROHIBITED_CONTENT": true, "BLOCKLIST": true, "SPII": true,
	"RECITATION": true, "IMAGE_SAFETY": true,
}

// maxStopDetail caps StopDetail and a synthesised call's input.
const maxStopDetail = 2 << 10

// decodeResponse turns a generateContent answer into the neutral turn. A
// response without usageMetadata comes back with Usage.Estimated set and the
// counts left to the caller (it has the bytes).
func decodeResponse(model string, req llm.Request, payload []byte) (llm.Response, error) {
	var w wireResponse
	if err := json.Unmarshal(payload, &w); err != nil {
		return llm.Response{}, fmt.Errorf("llm/gemini: decode response: %w", err)
	}
	resp := llm.Response{Message: llm.Message{Role: llm.RoleAssistant}, Model: model}
	if w.ModelVersion != "" {
		resp.Model = w.ModelVersion
	}
	if u := w.UsageMetadata; u != nil {
		// promptTokenCount includes the cached tokens; thoughts are billed
		// as output (design 3, usage row).
		resp.Usage = llm.Usage{
			Input:     max(u.PromptTokenCount-u.CachedContentTokenCount, 0) + u.ToolUsePromptTokenCount,
			CacheRead: u.CachedContentTokenCount,
			Output:    u.CandidatesTokenCount + u.ThoughtsTokenCount,
			Reasoning: u.ThoughtsTokenCount,
		}
	} else {
		resp.Usage.Estimated = true
	}

	if len(w.Candidates) == 0 {
		if pf := w.PromptFeedback; pf != nil && pf.BlockReason != "" {
			resp.Stop, resp.ProviderStop = llm.StopRefusal, pf.BlockReason
			resp.StopDetail = capDetail(joinDetail("prompt blocked: "+pf.BlockReason, pf.BlockReasonMessage))
			return resp, nil
		}
		resp.Stop, resp.ProviderStop = llm.StopOther, "NO_CANDIDATE"
		return resp, nil
	}
	cand := w.Candidates[0]
	resp.ProviderStop = cand.FinishReason
	turn := 1
	for _, m := range req.Messages {
		if m.Role == llm.RoleAssistant {
			turn++
		}
	}
	calls := 0
	for _, raw := range cand.Content.Parts {
		var p wirePart
		if err := json.Unmarshal(raw, &p); err != nil {
			return llm.Response{}, fmt.Errorf("llm/gemini: decode part: %w", err)
		}
		native := &llm.Native{Provider: llm.ProviderGemini, Model: model, Raw: append(json.RawMessage(nil), raw...)}
		switch {
		case p.FunctionCall != nil:
			calls++
			id := p.FunctionCall.ID
			if id == "" {
				// Not every model gives ids: make one; the result goes back
				// without it and Gemini pairs it by position.
				id = fmt.Sprintf("call_%d_%d", turn, calls)
			}
			input := p.FunctionCall.Args
			if len(input) == 0 || string(input) == "null" {
				input = json.RawMessage(`{}`)
			}
			resp.Message.Parts = append(resp.Message.Parts, llm.Part{
				Kind:     llm.PartToolCall,
				ToolCall: &llm.ToolCall{ID: id, Name: p.FunctionCall.Name, Input: input},
				Native:   native,
			})
		case p.Thought || (p.Text != nil && *p.Text == "" && p.ThoughtSignature != "") || p.Text == nil:
			// A thought, a bare signature, or a part Piperoom does not read:
			// kept only to go back verbatim.
			resp.Message.Parts = append(resp.Message.Parts, llm.Part{Kind: llm.PartReasoning, Native: native})
		default:
			resp.Message.Parts = append(resp.Message.Parts, llm.Part{Kind: llm.PartText, Text: *p.Text, Native: native})
		}
	}

	switch {
	case cand.FinishReason == "MALFORMED_FUNCTION_CALL":
		// The model tried to call a tool and wrote it wrong. The call becomes
		// a tool call whose input is not JSON, so the agent answers it with
		// an error tool_result (rejected: invalid_json) and the model can try
		// again; it goes back as text, never as a functionCall Gemini did
		// not make.
		calls++
		name, text := malformedCall(cand.FinishMessage, req.Tools)
		raw, _ := json.Marshal(map[string]string{malformedMarker: name})
		resp.Message.Parts = append(resp.Message.Parts, llm.Part{
			Kind: llm.PartToolCall,
			ToolCall: &llm.ToolCall{
				ID: fmt.Sprintf("call_%d_%d", turn, calls), Name: name,
				Input: json.RawMessage("malformed function call: " + text),
			},
			Native: &llm.Native{Provider: llm.ProviderGemini, Model: model, Raw: raw},
		})
		resp.Stop = llm.StopToolUse
	case calls > 0:
		resp.Stop = llm.StopToolUse
	case refusalReasons[cand.FinishReason]:
		resp.Stop = llm.StopRefusal
		detail := cand.FinishReason
		for _, r := range cand.SafetyRatings {
			if r.Blocked {
				detail += " " + r.Category
			}
		}
		resp.StopDetail = capDetail(joinDetail(detail, cand.FinishMessage))
	case cand.FinishReason == "STOP" || cand.FinishReason == "":
		resp.Stop = llm.StopEndTurn
	case cand.FinishReason == "MAX_TOKENS":
		resp.Stop = llm.StopMaxTokens
	default:
		resp.Stop = llm.StopOther
	}
	return resp, nil
}

// callNameRe finds the function in a MALFORMED_FUNCTION_CALL finish message
// ("Malformed function call: print(default_api.connect_status(...))").
var callNameRe = regexp.MustCompile(`(?:default_api\.)?([A-Za-z_][A-Za-z0-9_.:-]{0,63})\s*\(`)

// malformedCall returns the declared tool the message names (or "" when none
// is found, which the agent answers as an unknown tool) and the call text,
// capped.
func malformedCall(msg string, tools []llm.ToolDef) (string, string) {
	declared := map[string]bool{}
	for _, t := range tools {
		declared[t.Name] = true
	}
	name := ""
	for _, m := range callNameRe.FindAllStringSubmatch(msg, -1) {
		n := strings.TrimPrefix(m[1], "default_api.")
		if declared[n] {
			name = n
			break
		}
	}
	text := strings.TrimSpace(strings.TrimPrefix(msg, "Malformed function call:"))
	return name, capDetail(text)
}

func joinDetail(a, b string) string {
	if b == "" {
		return a
	}
	return a + ": " + b
}

func capDetail(s string) string {
	if len(s) <= maxStopDetail {
		return s
	}
	return strings.ToValidUTF8(s[:maxStopDetail], "")
}

// estimate is design 8.3: request and response bytes / 3, rounded up.
func estimate(req, resp []byte) llm.Usage {
	return llm.Usage{Input: (int64(len(req)) + 2) / 3, Output: (int64(len(resp)) + 2) / 3}
}

// truncated reports a prompt the server cut (design 6.2): reported prompt
// tokens below half of the lower-bound estimate of the content sent.
func truncated(u llm.Usage, lower int64) bool {
	if u.Estimated || lower == 0 {
		return false
	}
	return 2*(u.Input+u.CacheRead+u.CacheWrite) < lower
}
