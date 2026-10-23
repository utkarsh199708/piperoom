package agent

import (
	"context"
	"encoding/json"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// minimalValue is a value of the type a schema property asks for: the first
// enum value when there is one.
func minimalValue(prop map[string]any) any {
	if e := schemaEnum(prop); len(e) > 0 {
		return e[0]
	}
	switch prop["type"] {
	case "array":
		return []any{}
	case "object":
		obj := map[string]any{}
		props, _ := prop["properties"].(map[string]any)
		for _, name := range requiredOf(prop) {
			sub, _ := props[name].(map[string]any)
			obj[name] = minimalValue(sub)
		}
		return obj
	case "integer", "number":
		return 1
	case "boolean":
		return true
	default:
		return "x"
	}
}

// wrongValue is a value of another type than prop asks for.
func wrongValue(prop map[string]any) any {
	if prop["type"] == "string" {
		return 5
	}
	return "not-the-type"
}

func requiredOf(schema map[string]any) []string {
	switch r := schema["required"].(type) {
	case []string:
		return r
	case []any:
		out := make([]string, 0, len(r))
		for _, v := range r {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// Every room tool's input is checked against its schema in code before Run
// (V1t did it; V2a's Done-when asks for the table per room tool): a missing
// required field, a wrong type and an extra field are each an error
// tool_result marked rejected: invalid_input, paired with its tool_call, and
// the tool does nothing.
func TestRoomToolInputRejectedPerTool(t *testing.T) {
	probe := readRunner(t, &fakeSink{}, &recordingTool{name: "connect_status", schema: testSchema("connector")})
	var names []string
	for name, e := range probe.toolList.byName {
		if e.isRoom {
			names = append(names, name)
		}
	}
	if len(names) != 3 {
		t.Fatalf("room tools = %v, want update_hypothesis, propose_fix, post_message", names)
	}
	for _, name := range names {
		schema := probe.toolList.byName[name].schema
		req := requiredOf(schema)
		if len(req) == 0 {
			t.Fatalf("%s: no required field", name)
		}
		props, _ := schema["properties"].(map[string]any)
		base := map[string]any{}
		for _, f := range req {
			sub, _ := props[f].(map[string]any)
			base[f] = minimalValue(sub)
		}
		with := func(edit func(map[string]any)) json.RawMessage {
			m := map[string]any{}
			for k, v := range base {
				m[k] = v
			}
			edit(m)
			b, _ := json.Marshal(m)
			return b
		}
		first := req[0]
		firstProp, _ := props[first].(map[string]any)
		cases := map[string]json.RawMessage{
			"missing required": with(func(m map[string]any) { delete(m, first) }),
			"wrong type":       with(func(m map[string]any) { m[first] = wrongValue(firstProp) }),
			"extra field":      with(func(m map[string]any) { m["zz_not_in_schema"] = "x" }),
		}
		for cname, input := range cases {
			t.Run(name+"/"+cname, func(t *testing.T) {
				sink := &fakeSink{}
				r := readRunner(t, sink, &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}})
				got := r.runRoomTool(context.Background(), toolUse{id: "tu_r", name: name, input: input})
				if !got.isError {
					t.Fatalf("input %s accepted", input)
				}
				res := onlyResult(t, sink)
				if res.Rejected != events.ToolRejectedInvalidInput || res.Tool != name || res.ToolUseID != "tu_r" {
					t.Fatalf("tool_result = %+v, want %s rejected invalid_input", res, name)
				}
				assertStepPaired(t, sink, res)
				for _, typ := range []events.Type{events.AgentMessage, events.AgentHypothesis, events.ProposalCreated} {
					if n := len(sink.ofType(typ)); n != 0 {
						t.Fatalf("a refused %s emitted %d %s", name, n, typ)
					}
				}
			})
		}
	}
}
