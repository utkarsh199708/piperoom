package kafkatools

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// maxJSONSchemaDepth bounds the walk into nested object properties.
const maxJSONSchemaDepth = 8

// diffJSONSchema compares the properties of two JSON Schemas: top-level and
// nested object properties ("a.b"), array item properties ("a[].b"), each
// with its type, format and whether it is required.
func diffJSONSchema(fromSchema, toSchema string) ([]fieldChange, error) {
	from, err := jsonSchemaProps(fromSchema)
	if err != nil {
		return nil, fmt.Errorf("from schema: %w", err)
	}
	to, err := jsonSchemaProps(toSchema)
	if err != nil {
		return nil, fmt.Errorf("to schema: %w", err)
	}
	var changes []fieldChange
	for name, toType := range to {
		fromType, ok := from[name]
		switch {
		case !ok:
			changes = append(changes, fieldChange{Field: name, Kind: "added", To: toType})
		case fromType != toType:
			changes = append(changes, fieldChange{Field: name, Kind: "changed", From: fromType, To: toType})
		}
	}
	for name, fromType := range from {
		if _, ok := to[name]; !ok {
			changes = append(changes, fieldChange{Field: name, Kind: "removed", From: fromType})
		}
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Field < changes[j].Field })
	return changes, nil
}

func jsonSchemaProps(schema string) (map[string]string, error) {
	var root map[string]any
	if err := json.Unmarshal([]byte(schema), &root); err != nil {
		return nil, err
	}
	out := map[string]string{}
	walkJSONProps(root, "", out, 0)
	return out, nil
}

func walkJSONProps(node map[string]any, prefix string, out map[string]string, depth int) {
	if depth > maxJSONSchemaDepth {
		return
	}
	props, _ := node["properties"].(map[string]any)
	required := map[string]bool{}
	if req, ok := node["required"].([]any); ok {
		for _, r := range req {
			if s, ok := r.(string); ok {
				required[s] = true
			}
		}
	}
	for name, raw := range props {
		p, _ := raw.(map[string]any)
		path := prefix + name
		desc := jsonPropType(p)
		if required[name] {
			desc += " required"
		}
		out[path] = desc
		if p == nil {
			continue
		}
		walkJSONProps(p, path+".", out, depth+1)
		if items, ok := p["items"].(map[string]any); ok {
			walkJSONProps(items, path+"[].", out, depth+1)
		}
	}
}

// jsonPropType renders a property's type compactly: "string(date-time)",
// "integer|null", "array<string>", "$ref:#/defs/x" or "oneOf[...]".
func jsonPropType(p map[string]any) string {
	if p == nil {
		return "any"
	}
	var t string
	switch v := p["type"].(type) {
	case string:
		t = v
	case []any:
		parts := make([]string, 0, len(v))
		for _, x := range v {
			parts = append(parts, fmt.Sprint(x))
		}
		sort.Strings(parts)
		t = strings.Join(parts, "|")
	}
	if ref, ok := p["$ref"].(string); ok {
		t = "$ref:" + ref
	}
	for _, k := range []string{"oneOf", "anyOf", "allOf"} {
		if alts, ok := p[k].([]any); ok {
			parts := make([]string, 0, len(alts))
			for _, a := range alts {
				am, _ := a.(map[string]any)
				parts = append(parts, jsonPropType(am))
			}
			t = k + "[" + strings.Join(parts, ", ") + "]"
		}
	}
	if t == "" {
		t = "any"
	}
	if f, ok := p["format"].(string); ok {
		t += "(" + f + ")"
	}
	if t == "array" {
		if items, ok := p["items"].(map[string]any); ok {
			t = "array<" + jsonPropType(items) + ">"
		}
	}
	if enum, ok := p["enum"].([]any); ok {
		b, _ := json.Marshal(enum)
		t += " enum" + string(b)
	}
	return t
}
