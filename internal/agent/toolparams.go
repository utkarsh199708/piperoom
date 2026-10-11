package agent

import (
	"encoding/json"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/tools"
)

const whyProperty = "why"

const whyDescription = "one short sentence: what you are checking and why"

// toolEntry is a tool the agent can execute, keyed by name.
type toolEntry struct {
	tool    tools.Tool
	schema  map[string]any // original schema, without the injected why property
	isRoom  bool
	wrapWhy bool
}

// toolList is the stable, ordered set of tools sent to the model plus the
// lookup table used to execute tool_use blocks.
type toolList struct {
	params []anthropic.BetaToolUnionParam
	byName map[string]toolEntry
}

// maxStrictTools caps how many tools are sent with strict: true. The API
// compiles every strict schema into one grammar per request and enforces two
// limits on it: a count ("Too many strict tools (21). The maximum number of
// strict tools supported is 20.") and a size ("The compiled grammar is too
// large ... reduce the number of strict tools"). With our schemas the size
// limit bites well below 20, so the cap is 8: room tools take the strict
// slots first, because propose_fix carries the nested action params, where a
// malformed input costs the most; read tools fill the remaining slots in
// registration order. The rest are sent without strict; validateInput still
// checks every read-tool input in code before Tool.Run, so nothing goes
// unvalidated. Raise this only after a live run proves the grammar fits.
const maxStrictTools = 8

// buildToolList assembles the model-facing tool definitions. Registry (read)
// tools keep their registration order; room tools follow in a fixed order. The
// order is stable so the tool list is byte-identical across requests and the
// prompt cache stays warm.
func buildToolList(reg *tools.Registry, room []tools.Tool) toolList {
	tl := toolList{byName: map[string]toolEntry{}}
	var reads []tools.Tool
	if reg != nil {
		reads = reg.All()
	}
	// Room tools claim strict slots first; read tools get what is left.
	readStrict := max(maxStrictTools-len(room), 0)
	for i, t := range reads {
		tl.byName[t.Name()] = toolEntry{tool: t, schema: t.InputSchema(), wrapWhy: true}
		tl.params = append(tl.params, toolParam(t, true, i < readStrict))
	}
	for i, t := range room {
		tl.byName[t.Name()] = toolEntry{tool: t, schema: t.InputSchema(), isRoom: true}
		tl.params = append(tl.params, toolParam(t, false, i < maxStrictTools))
	}
	// Put a cache breakpoint after the tool list (the last tool).
	if n := len(tl.params); n > 0 {
		if tp := tl.params[n-1].OfTool; tp != nil {
			tp.CacheControl = anthropic.NewBetaCacheControlEphemeralParam()
			tl.params[n-1] = anthropic.BetaToolUnionParam{OfTool: tp}
		}
	}
	return tl
}

// toolParam converts one Piperoom tool into an SDK tool definition. When
// wrapWhy is set, an extra required string property "why" is injected into the
// schema; the agent strips it before calling Tool.Run and uses it as the step
// title. strict asks the API to validate inputs against the schema (see
// maxStrictTools); the schema is shaped the same either way, so what the model
// sees does not depend on which slot the tool landed in.
func toolParam(t tools.Tool, wrapWhy, strict bool) anthropic.BetaToolUnionParam {
	schema := t.InputSchema()

	props := map[string]any{}
	if p, ok := schema["properties"].(map[string]any); ok {
		for k, v := range p {
			props[k] = cloneSchema(v)
		}
	}
	var required []string
	switch r := schema["required"].(type) {
	case []string:
		required = append(required, r...)
	case []any:
		for _, x := range r {
			if s, ok := x.(string); ok {
				required = append(required, s)
			}
		}
	}
	if wrapWhy {
		props[whyProperty] = map[string]any{"type": "string", "description": whyDescription}
		required = append(required, whyProperty)
	}

	// Strict tool schemas must set additionalProperties:false on every object,
	// including nested ones, or every real request is rejected with HTTP 400.
	for _, v := range props {
		addAdditionalPropertiesFalse(v)
	}

	tp := anthropic.BetaToolParam{
		Name:        t.Name(),
		Description: anthropic.String(t.Description()),
		InputSchema: anthropic.BetaToolInputSchemaParam{
			Properties:  props,
			Required:    required,
			ExtraFields: map[string]any{"additionalProperties": false},
		},
		EagerInputStreaming: anthropic.Bool(true),
		Type:                anthropic.BetaToolTypeCustom,
	}
	if strict {
		tp.Strict = anthropic.Bool(true)
	}
	return anthropic.BetaToolUnionParam{OfTool: &tp}
}

// cloneSchema deep-copies a JSON Schema fragment so adding strictness flags to
// the model-facing schema never mutates the tool's own schema.
func cloneSchema(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = cloneSchema(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, el := range t {
			out[i] = cloneSchema(el)
		}
		return out
	default:
		return v
	}
}

// isObjectSchema reports whether a schema node describes a JSON object.
func isObjectSchema(s map[string]any) bool {
	if typ, _ := s["type"].(string); typ == "object" {
		return true
	}
	_, hasProps := s["properties"]
	return hasProps
}

// addAdditionalPropertiesFalse walks a schema fragment and ensures every object
// node carries additionalProperties:false (leaving an explicit setting alone).
func addAdditionalPropertiesFalse(v any) {
	switch t := v.(type) {
	case map[string]any:
		if isObjectSchema(t) {
			if _, ok := t["additionalProperties"]; !ok {
				t["additionalProperties"] = false
			}
		}
		for _, val := range t {
			addAdditionalPropertiesFalse(val)
		}
	case []any:
		for _, el := range t {
			addAdditionalPropertiesFalse(el)
		}
	}
}

// stripWhy removes the injected why property from a tool input and returns the
// cleaned input plus the why text (empty when absent or not a string). It
// round-trips through map[string]json.RawMessage so integers (offsets, LSNs)
// keep their full precision instead of being coerced to float64.
func stripWhy(raw json.RawMessage) (json.RawMessage, string, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return raw, "", err
	}
	why := ""
	if whyRaw, ok := m[whyProperty]; ok {
		_ = json.Unmarshal(whyRaw, &why) // non-string why becomes empty
		delete(m, whyProperty)
	}
	out, err := json.Marshal(m)
	if err != nil {
		return raw, "", err
	}
	return out, why, nil
}
