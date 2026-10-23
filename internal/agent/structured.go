package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"piperoom/internal/agent/llm"
)

// Structured output on a model without it (design 6, V2a). The hand-off, the
// post-mortem, the first-look verdict and the hand-off judge ask for a JSON
// object with Request.OutputSchema. A model whose Capabilities say
// StructuredOutput gets that request unchanged (Claude: the V1a-0 goldens).
// Any other model gets one of two fallbacks:
//
//	(a) with tool calls: one tool emit_<what> whose input schema is the
//	    output schema, with ToolChoice required; the object is the call's
//	    input (or, when the server ignored the forced choice, the first JSON
//	    object of the text);
//	(b) without: the schema in the system prompt and the first JSON object
//	    of the answer's text.
//
// Either way the object is checked against the schema in code. A parse or
// schema failure gets one retry, with the bad answer replayed and a note
// naming the error; a second failure is an error, which takes the caller's
// existing failure path (postmortem.failed, triage reason model_error, the
// judge's verdict left unjudged).

// turnFunc sends one request: Model.Turn, or the runner's callModel, which
// also reports usage.
type turnFunc func(context.Context, llm.Request) (llm.Response, error)

// structuredAttempts is the fallback's first try plus its one retry.
const structuredAttempts = 2

// maxJSONStarts bounds how many '{' firstJSONObject tries, so a long text of
// braces cannot make it quadratic for long.
const maxJSONStarts = 64

// emitToolName is the forced tool of fallback (a) for what.
func emitToolName(what string) string { return "emit_" + what }

// structuredTurn sends req, which carries OutputSchema, and returns the
// answer and its JSON text. On a model with StructuredOutput it is one plain
// turn and the text is the answer's text, as before V2a; the caller parses it.
// On any other model it runs the fallback above; the text it returns is one
// JSON object that passed the schema. A refusal is returned as the response
// with no text and no error, so the caller reports it as it always has.
func structuredTurn(ctx context.Context, turn turnFunc, caps llm.Capabilities, req llm.Request, what string) (llm.Response, string, error) {
	if caps.StructuredOutput || req.OutputSchema == nil {
		resp, err := turn(ctx, req)
		if err != nil {
			return resp, "", err
		}
		return resp, extractText(resp.Message.Parts), nil
	}
	schema := req.OutputSchema
	fb, err := fallbackRequest(req, caps, what)
	if err != nil {
		return llm.Response{}, "", err
	}
	var lastErr error
	for attempt := 0; attempt < structuredAttempts; attempt++ {
		resp, err := turn(ctx, fb)
		if err != nil {
			return resp, "", err
		}
		if resp.Stop == llm.StopRefusal {
			return resp, "", nil
		}
		obj, perr := structuredObject(resp, schema, what)
		if perr == nil {
			return resp, string(obj), nil
		}
		lastErr = perr
		fb.Messages = append(append([]llm.Message(nil), fb.Messages...), retryTurns(resp, perr, what)...)
	}
	return llm.Response{}, "", fmt.Errorf("agent: %s: no valid JSON object after %d attempts without structured output: %w", what, structuredAttempts, lastErr)
}

// fallbackRequest is req for a model without structured output: (a) a
// forced emit tool when it has tool calls, else (b) the schema in the system
// prompt. The schema is the agent's own, never untrusted text.
func fallbackRequest(req llm.Request, caps llm.Capabilities, what string) (llm.Request, error) {
	schema := req.OutputSchema
	req.OutputSchema = nil
	if caps.ToolCalls {
		req.Tools = []llm.ToolDef{{
			Name:        emitToolName(what),
			Description: "Return your answer as this tool's input. Call it exactly once; its input is the whole answer.",
			Schema:      schema,
			Strict:      caps.StrictTools,
		}}
		req.ToolChoice = llm.ToolChoiceRequired
		return req, nil
	}
	b, err := json.Marshal(schema)
	if err != nil {
		return llm.Request{}, fmt.Errorf("agent: %s: marshal output schema: %w", what, err)
	}
	req.Tools = nil
	req.ToolChoice = llm.ToolChoiceAuto
	req.System += "\n\nAnswer with exactly one JSON object that matches this JSON Schema, and no other text:\n" + string(b)
	return req, nil
}

// structuredObject finds the answer's JSON object (the emit tool's input,
// else the first JSON object of its text) and checks it against schema.
func structuredObject(resp llm.Response, schema map[string]any, what string) (json.RawMessage, error) {
	var obj json.RawMessage
	for _, p := range resp.Message.Parts {
		if p.Kind == llm.PartToolCall && p.ToolCall != nil && p.ToolCall.Name == emitToolName(what) {
			obj = bytes.TrimSpace(p.ToolCall.Input)
			break
		}
	}
	if obj == nil {
		o, ok := firstJSONObject(extractText(resp.Message.Parts))
		if !ok {
			return nil, fmt.Errorf("the answer holds no JSON object")
		}
		obj = o
	}
	if !json.Valid(obj) || len(obj) == 0 || obj[0] != '{' {
		return nil, fmt.Errorf("the answer is not a JSON object")
	}
	if err := validateInput(schema, obj); err != nil {
		return nil, fmt.Errorf("the answer does not match the schema: %w", err)
	}
	return obj, nil
}

// firstJSONObject returns the first complete JSON object in s: prose or a
// code fence around it is skipped, and so is a '{' that does not start one.
func firstJSONObject(s string) (json.RawMessage, bool) {
	for i, tries := 0, 0; i < len(s) && tries < maxJSONStarts; tries++ {
		j := strings.IndexByte(s[i:], '{')
		if j < 0 {
			break
		}
		i += j
		var raw json.RawMessage
		if err := json.NewDecoder(strings.NewReader(s[i:])).Decode(&raw); err == nil {
			return raw, true
		}
		i++
	}
	return nil, false
}

// retryTurns are the turns that ask again after a bad answer: the answer as
// the model gave it (history is append-only), an error tool_result for each
// of its tool calls so every call stays paired, and a note naming the error.
// The error can quote a key the model wrote, so it goes in the untrusted
// wrapper.
func retryTurns(resp llm.Response, perr error, what string) []llm.Message {
	note := "Your answer was not usable: " + wrapUntrusted(perr.Error()) + ". Answer again with only the corrected JSON object."
	var results []llm.Part
	for _, p := range resp.Message.Parts {
		if p.Kind == llm.PartToolCall && p.ToolCall != nil {
			results = append(results, llm.Part{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{
				CallID:  p.ToolCall.ID,
				Name:    p.ToolCall.Name,
				Content: wrapToolOutput(p.ToolCall.Name, "", "error: "+perr.Error()),
				IsError: true,
			}})
		}
	}
	if len(results) > 0 {
		note = "Your " + emitToolName(what) + " call was not usable: " + wrapUntrusted(perr.Error()) + ". Call " + emitToolName(what) + " again with the corrected object."
	}
	var out []llm.Message
	if len(resp.Message.Parts) > 0 {
		out = append(out, llm.Message{Role: llm.RoleAssistant, Parts: resp.Message.Parts})
	}
	return append(out, llm.Message{Role: llm.RoleUser, Parts: append(results, textPart(note))})
}
