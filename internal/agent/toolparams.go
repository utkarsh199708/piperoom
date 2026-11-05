package agent

import (
	"encoding/json"
	"math"

	"piperoom/internal/agent/llm"
	anthropicllm "piperoom/internal/agent/llm/anthropic"
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
// lookup table used to execute tool calls. The request asks the provider to
// cache through the last tool (llm.CacheHints.Tools).
type toolList struct {
	defs   []llm.ToolDef
	byName map[string]toolEntry
}

// maxStrictTools caps the tools sent with strict: true on Claude, the Claude
// adapter's Capabilities.MaxStrictTools (see anthropicllm.MaxStrictTools for
// the live bisect behind it). Room tools take the slots first: propose_fix
// guards the write path, where a malformed input costs the most. Its strict
// schema holds only an enum of action names and the params as JSON text; each
// action's own params are checked in code (see proposeFixTool.checkStep), so
// the schema does not grow with every action.
// strictReadTools fill the rest. Tools sent without strict are still checked in
// code: validateInput runs on every read-tool input before Tool.Run, so nothing
// goes unvalidated.
const maxStrictTools = anthropicllm.MaxStrictTools

// maxStrictSchemaBytes caps the summed size of the marshalled input_schema of
// every strict tool (anthropicllm.MaxStrictSchemaBytes, the Claude adapter's
// Capabilities.MaxStrictBytes). Bytes are only a proxy for the compiled
// grammar size the API limits, but they grow with it. The budget sits just
// under the largest sum the API is known to accept (liveAcceptedSchemaBytes);
// configurations that were never sent live do not count.
//
// History of the shipped cap-8 set (production registry, the real actions):
// 4491 bytes on 2026-10-12, which ran live in LEAD-ARCH's M2 check; 5319 bytes
// once more actions were registered and propose_fix nested each one's params;
// 3088 bytes now that propose_fix holds only an enum of action names and the
// params as JSON text. A new action adds about its name length to that enum
// (TestProposeFixStrictBytesDoNotGrowWithActions holds the line). The current
// shape has not been sent live yet: re-run TestLiveStrictToolCap once.
const maxStrictSchemaBytes = anthropicllm.MaxStrictSchemaBytes

// strictCap is how many tools the model may get with strict set: none when
// the provider has no strict tools, every one when it sets no cap.
func strictCap(c llm.Capabilities) int {
	switch {
	case !c.StrictTools:
		return 0
	case c.MaxStrictTools <= 0:
		return math.MaxInt
	}
	return c.MaxStrictTools
}

// liveAcceptedSchemaBytes is the largest strict schema sum the API accepted in
// the live bisect of 2026-10-12 (TestLiveStrictToolCap, cap 16; that run used
// an empty actions registry, so propose_fix was 625 bytes, not 2180). The
// next measured point, 6461 bytes at cap 20, was rejected as too large.
// Re-run the bisect with the production registry before raising the budget.
const liveAcceptedSchemaBytes = 5484

// strictReadTools take the strict slots left after the room tools, in this
// order: the read tools the eval scenarios (evals/scenarios/*.yaml) call most.
// Slots left over go to the other read tools in registration order.
func strictReadTools() []string {
	return []string{
		"schema_registry_diff", "pipeline_map", "trace_record", "schema_registry_versions", "connect_status",
	}
}

// buildToolList assembles the model-facing tool definitions with Claude's
// strict cap; the loop uses buildToolListWithCap with its model's. Registry (read)
// tools keep their registration order; room tools follow in a fixed order. The
// order is stable so the tool list is byte-identical across requests and the
// prompt cache stays warm.
func buildToolList(reg *tools.Registry, room []tools.Tool) toolList {
	return buildToolListWithCap(reg, room, maxStrictTools)
}

// buildToolListWithCap is buildToolList with the strict cap as a parameter, so
// the live claude-tagged test can vary only the cap while reusing the exact
// strict-set selection and schema shaping production sends. cap < len(room)
// leaves the last room tools non-strict; cap > len(room) hands the leftover
// slots to read tools via strictReadSet exactly as buildToolList does.
func buildToolListWithCap(reg *tools.Registry, room []tools.Tool, cap int) toolList {
	tl := toolList{byName: map[string]toolEntry{}}
	var reads []tools.Tool
	if reg != nil {
		reads = reg.All()
	}
	// Room tools claim strict slots first; read tools get what is left.
	strictReads := strictReadSet(reads, max(cap-len(room), 0))
	for _, t := range reads {
		tl.byName[t.Name()] = toolEntry{tool: t, schema: t.InputSchema(), wrapWhy: true}
		tl.defs = append(tl.defs, toolParam(t, true, strictReads[t.Name()]))
	}
	for i, t := range room {
		schema := t.InputSchema()
		if v, ok := t.(interface{ validationSchema() map[string]any }); ok {
			schema = v.validationSchema() // a tool whose Run checks some fields itself
		}
		tl.byName[t.Name()] = toolEntry{tool: t, schema: schema, isRoom: true}
		tl.defs = append(tl.defs, toolParam(t, false, i < cap))
	}
	return tl
}

// strictReadSet picks the read tools that get the n strict slots: the
// registered strictReadTools in list order, then the others in registration
// order. The choice depends only on the registry, so the tool list stays
// byte-identical across requests.
func strictReadSet(reads []tools.Tool, n int) map[string]bool {
	registered := make(map[string]bool, len(reads))
	for _, t := range reads {
		registered[t.Name()] = true
	}
	set := make(map[string]bool, n)
	for _, name := range strictReadTools() {
		if len(set) >= n {
			break
		}
		if registered[name] {
			set[name] = true
		}
	}
	for _, t := range reads {
		if len(set) >= n {
			break
		}
		set[t.Name()] = true
	}
	return set
}

// toolParam converts one Piperoom tool into a model tool definition. When
// wrapWhy is set, an extra required string property "why" is injected into the
// schema; the agent strips it before calling Tool.Run and uses it as the step
// title. strict asks the API to validate inputs against the schema (see
// maxStrictTools); the schema is shaped the same either way, so what the model
// sees does not depend on which slot the tool landed in.
func toolParam(t tools.Tool, wrapWhy, strict bool) llm.ToolDef {
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

	// A strict schema needs additionalProperties:false at the top too.
	schema = map[string]any{"type": "object", "properties": props}
	if required != nil {
		schema["required"] = required
	}
	schema["additionalProperties"] = false
	return llm.ToolDef{
		Name:        t.Name(),
		Description: t.Description(),
		Schema:      schema,
		Strict:      strict,
	}
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
