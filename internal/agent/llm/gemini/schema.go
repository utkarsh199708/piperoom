package gemini

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// jsonSchema returns a copy of s for parametersJsonSchema or
// responseJsonSchema (Gemini 2.5 and later take JSON Schema as is). The copy
// goes through JSON, so the agent's map is never shared with the encoder.
// Only a schema that is not an object at the top is refused.
func jsonSchema(s map[string]any) (map[string]any, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("schema is not JSON: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("schema is not a JSON object: %w", err)
	}
	if t, ok := out["type"]; ok && t != "object" {
		return nil, fmt.Errorf("schema type %v: want object", t)
	}
	delete(out, "$schema")
	return out, nil
}

// openAPIKeep are the keywords the OpenAPI 3.0 subset (Gemini's Schema)
// carries over as they are.
var openAPIKeep = map[string]bool{
	"description": true, "title": true, "format": true, "nullable": true, "default": true, "example": true,
	"minItems": true, "maxItems": true, "minimum": true, "maximum": true,
	"minLength": true, "maxLength": true, "pattern": true,
	"minProperties": true, "maxProperties": true, "required": true,
}

// openAPIRefuse are constructs the subset cannot express; a schema that holds
// one fails start with the tool named (design 7.2). Constraints the subset
// merely lacks (additionalProperties, $schema, const on a non-string) are
// dropped instead: the agent validates every input in code.
var openAPIRefuse = map[string]bool{
	"$ref": true, "$defs": true, "definitions": true, "allOf": true, "not": true,
	"patternProperties": true, "if": true, "then": true, "else": true,
	"dependentSchemas": true, "dependentRequired": true, "prefixItems": true,
}

var openAPITypes = map[string]string{
	"string": "STRING", "number": "NUMBER", "integer": "INTEGER",
	"boolean": "BOOLEAN", "array": "ARRAY", "object": "OBJECT",
}

// openAPISchema converts a JSON Schema to Gemini's OpenAPI 3.0 subset (the
// parameters and responseSchema fields of models before 2.5). path names the
// node in an error.
func openAPISchema(s map[string]any, path string) (map[string]any, error) {
	at := func() string {
		if path == "" {
			return "the schema root"
		}
		return path
	}
	out := map[string]any{}
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := s[k]
		switch {
		case openAPIRefuse[k]:
			return nil, fmt.Errorf("%s: %s cannot be expressed in Gemini's schema subset", at(), k)
		case openAPIKeep[k]:
			out[k] = v
		case k == "type":
			t, nullable, err := openAPIType(v)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", at(), err)
			}
			out["type"] = t
			if nullable {
				out["nullable"] = true
			}
		case k == "enum":
			vals, ok := v.([]any)
			if !ok {
				if ss, ok2 := v.([]string); ok2 {
					for _, x := range ss {
						vals = append(vals, x)
					}
					ok = true
				}
			}
			if !ok {
				return nil, fmt.Errorf("%s: enum is not a list", at())
			}
			strs := make([]string, 0, len(vals))
			for _, x := range vals {
				str, isStr := x.(string)
				if !isStr {
					strs = nil
					break
				}
				strs = append(strs, str)
			}
			if strs != nil {
				// The subset has enums of strings only; another enum is
				// dropped and stays enforced by the code validation.
				out["enum"] = strs
			}
		case k == "const":
			if str, ok := v.(string); ok {
				out["enum"] = []string{str}
			}
		case k == "properties":
			props, ok := v.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%s: properties is not an object", at())
			}
			conv := make(map[string]any, len(props))
			for name, ps := range props {
				pm, ok := ps.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("%s.%s: not a schema object", at(), name)
				}
				c, err := openAPISchema(pm, joinPath(path, "properties."+name))
				if err != nil {
					return nil, err
				}
				conv[name] = c
			}
			out["properties"] = conv
		case k == "items":
			im, ok := v.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%s: items is not a single schema", at())
			}
			c, err := openAPISchema(im, joinPath(path, "items"))
			if err != nil {
				return nil, err
			}
			out["items"] = c
		case k == "anyOf" || k == "oneOf":
			branches, ok := v.([]any)
			if !ok {
				return nil, fmt.Errorf("%s: %s is not a list", at(), k)
			}
			conv := make([]any, 0, len(branches))
			for i, b := range branches {
				bm, ok := b.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("%s: %s[%d] is not a schema object", at(), k, i)
				}
				// oneOf means exactly one; the subset only has anyOf. Over
				// scalars the two agree for the model's purpose; over objects
				// they do not, and the meaning would change.
				if k == "oneOf" && isObjectSchema(bm) {
					return nil, fmt.Errorf("%s: oneOf over objects cannot be expressed in Gemini's schema subset", at())
				}
				c, err := openAPISchema(bm, joinPath(path, fmt.Sprintf("%s[%d]", k, i)))
				if err != nil {
					return nil, err
				}
				conv = append(conv, c)
			}
			out["anyOf"] = conv
		default:
			// additionalProperties, $schema, $id, $comment, examples and
			// other annotations: dropped (enforced, where they constrain, by
			// the code validation).
		}
	}
	if _, ok := out["type"]; !ok {
		if _, hasProps := out["properties"]; hasProps {
			out["type"] = "OBJECT"
		}
	}
	return out, nil
}

func joinPath(path, next string) string {
	if path == "" {
		return next
	}
	return path + "." + next
}

func isObjectSchema(s map[string]any) bool {
	if _, ok := s["properties"]; ok {
		return true
	}
	switch t := s["type"].(type) {
	case string:
		return t == "object"
	case []any:
		for _, x := range t {
			if x == "object" {
				return true
			}
		}
	}
	return false
}

// openAPIType maps a JSON Schema type (a name, or a pair with "null") to the
// subset's upper-case type and a nullable flag.
func openAPIType(v any) (string, bool, error) {
	var names []string
	switch t := v.(type) {
	case string:
		names = []string{t}
	case []any:
		for _, x := range t {
			s, ok := x.(string)
			if !ok {
				return "", false, fmt.Errorf("type %v is not a name", x)
			}
			names = append(names, s)
		}
	case []string:
		names = t
	default:
		return "", false, fmt.Errorf("type %v is not a name", v)
	}
	nullable := false
	var kept []string
	for _, n := range names {
		if n == "null" {
			nullable = true
			continue
		}
		kept = append(kept, n)
	}
	if len(kept) != 1 {
		return "", false, fmt.Errorf("type %s: a union of types cannot be expressed in Gemini's schema subset", strings.Join(names, "|"))
	}
	up, ok := openAPITypes[kept[0]]
	if !ok {
		return "", false, fmt.Errorf("type %q is not a JSON Schema type", kept[0])
	}
	return up, nullable, nil
}
