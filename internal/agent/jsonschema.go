package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// validateInput checks raw JSON against a JSON Schema object. It covers the
// subset Piperoom tools use: object (properties, required, additionalProperties),
// string, integer, number, boolean, array (items) and enum. It exists so the
// agent can reject truncated or malformed tool input (eager input streaming
// may deliver partial JSON) before handing it to a tool.
func validateInput(schema map[string]any, raw json.RawMessage) error {
	if !json.Valid(raw) {
		return fmt.Errorf("input is not valid JSON")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return fmt.Errorf("input is not valid JSON: %w", err)
	}
	if err := validateValue(schema, v); err != nil {
		return err
	}
	return nil
}

func validateValue(schema map[string]any, v any) error {
	switch typ, _ := schema["type"].(string); typ {
	case "object":
		return validateObject(schema, v)
	case "string":
		if _, ok := v.(string); !ok {
			return fmt.Errorf("expected string, got %T", v)
		}
	case "integer":
		if err := checkInteger(v); err != nil {
			return err
		}
	case "number":
		switch v.(type) {
		case json.Number, float64, float32, int, int64, int32:
		default:
			return fmt.Errorf("expected number, got %T", v)
		}
	case "boolean":
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", v)
		}
	case "array":
		return validateArray(schema, v)
	case "":
		// no type constraint
	default:
		// unknown type keyword is not something our tools emit; ignore it
	}
	if enum := schemaEnum(schema); enum != nil {
		if !inEnum(enum, v) {
			return fmt.Errorf("value %v is not one of the allowed values", v)
		}
	}
	return nil
}

// schemaEnum extracts the enum values, accepting both the []string form (built
// in Go code) and the []any form (unmarshalled from JSON).
func schemaEnum(schema map[string]any) []any {
	switch e := schema["enum"].(type) {
	case []any:
		return e
	case []string:
		out := make([]any, len(e))
		for i, s := range e {
			out[i] = s
		}
		return out
	default:
		return nil
	}
}

func validateObject(schema map[string]any, v any) error {
	obj, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("expected object, got %T", v)
	}
	props, _ := schema["properties"].(map[string]any)
	if required, ok := schema["required"].([]any); ok {
		for _, r := range required {
			name, ok := r.(string)
			if !ok {
				continue
			}
			if _, present := obj[name]; !present {
				return fmt.Errorf("missing required field %q", name)
			}
		}
	}
	if required, ok := schema["required"].([]string); ok {
		for _, name := range required {
			if _, present := obj[name]; !present {
				return fmt.Errorf("missing required field %q", name)
			}
		}
	}
	for key, val := range obj {
		sub, ok := props[key]
		if !ok {
			if ap, ok := schema["additionalProperties"].(bool); ok && !ap {
				return fmt.Errorf("unknown field %q", key)
			}
			continue
		}
		subSchema, ok := sub.(map[string]any)
		if !ok {
			continue
		}
		if err := validateValue(subSchema, val); err != nil {
			return fmt.Errorf("field %q: %w", key, err)
		}
	}
	return nil
}

func validateArray(schema map[string]any, v any) error {
	arr, ok := v.([]any)
	if !ok {
		return fmt.Errorf("expected array, got %T", v)
	}
	items, ok := schema["items"].(map[string]any)
	if !ok {
		return nil
	}
	for i, el := range arr {
		if err := validateValue(items, el); err != nil {
			return fmt.Errorf("item %d: %w", i, err)
		}
	}
	return nil
}

func checkInteger(v any) error {
	switch n := v.(type) {
	case json.Number:
		if strings.ContainsAny(n.String(), ".eE") {
			return fmt.Errorf("expected integer, got %s", n)
		}
		return nil
	case int, int8, int16, int32, int64:
		return nil
	case float64:
		if n == float64(int64(n)) {
			return nil
		}
		return fmt.Errorf("expected integer, got %v", n)
	default:
		return fmt.Errorf("expected integer, got %T", v)
	}
}

func inEnum(enum []any, v any) bool {
	for _, e := range enum {
		if jsonEqual(e, v) {
			return true
		}
	}
	return false
}

// jsonEqual compares two values unmarshalled from JSON. Numbers compare by
// numeric value; everything else compares with == after normalizing types.
func jsonEqual(a, b any) bool {
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	return bytes.Equal(aj, bj)
}
