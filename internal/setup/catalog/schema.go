package catalog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"piperoom/internal/setup/protocol"
)

// ErrParams wraps every refusal of a params set by its op's schema. The setup
// tools wrap it in tools.ErrBadInput, so the model sees a tool error it can
// fix, not a failed target.
var ErrParams = errors.New("catalog: params do not match the op's schema")

// maxParamsBytes caps the params one op may carry before they are decoded: the
// largest op (file_write) takes 256 KiB of content, so 320 KiB leaves room for
// its path and the JSON escaping without accepting an unbounded payload.
const maxParamsBytes = 320 << 10

// keywords is the JSON Schema subset the runner's ops declare, and so the
// subset Check understands. A schema with any other keyword is refused when
// the catalogue is built (New): a keyword Check silently ignored would be a
// rule the server never enforced.
var keywords = map[string]bool{
	"type": true, "properties": true, "required": true, "additionalProperties": true,
	"enum": true, "pattern": true, "minLength": true, "maxLength": true,
	"minimum": true, "maximum": true, "items": true, "minItems": true, "maxItems": true,
	"minProperties": true, "maxProperties": true,
}

// schemaNode is one decoded schema object.
type schemaNode map[string]any

// checkSchema refuses a schema with a keyword outside the subset, at any depth,
// and a pattern that does not compile.
func checkSchema(s any, path string) error {
	node, ok := s.(map[string]any)
	if !ok {
		return fmt.Errorf("catalog: schema at %s is not an object", path)
	}
	for k, v := range node {
		if !keywords[k] {
			return fmt.Errorf("catalog: schema at %s uses %q, which the server does not check", path, k)
		}
		switch k {
		case "properties":
			props, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("catalog: schema at %s: properties is not an object", path)
			}
			for name, sub := range props {
				if err := checkSchema(sub, path+"."+name); err != nil {
					return err
				}
			}
		case "items":
			if err := checkSchema(v, path+"[]"); err != nil {
				return err
			}
		case "additionalProperties":
			if _, isBool := v.(bool); !isBool {
				if err := checkSchema(v, path+".*"); err != nil {
					return err
				}
			}
		case "pattern":
			p, ok := v.(string)
			if !ok {
				return fmt.Errorf("catalog: schema at %s: pattern is not a string", path)
			}
			if _, err := regexp.Compile(p); err != nil {
				return fmt.Errorf("catalog: schema at %s: pattern: %w", path, err)
			}
		}
	}
	return nil
}

// Check validates params against schema, the server's half of "validated
// against the op's schema on both sides" (protocol.OpRequest.Params). It does
// no I/O. It refuses params over the size cap, params that are not a JSON
// object, keys that differ only in letter case (protocol.CheckParamKeys, which
// the runner refuses too), and every rule of the schema subset. Errors wrap
// ErrParams and name the offending path, never a value.
func Check(schema map[string]any, params json.RawMessage) error {
	raw := bytes.TrimSpace(params)
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}
	if len(raw) > maxParamsBytes {
		return fmt.Errorf("%w: params are %d bytes (at most %d)", ErrParams, len(raw), maxParamsBytes)
	}
	if err := protocol.CheckParamKeys(raw); err != nil {
		return fmt.Errorf("%w: %w", ErrParams, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return fmt.Errorf("%w: params are not JSON", ErrParams)
	}
	if dec.More() {
		return fmt.Errorf("%w: trailing data after the params object", ErrParams)
	}
	if _, ok := v.(map[string]any); !ok {
		return fmt.Errorf("%w: params must be a JSON object", ErrParams)
	}
	return checkValue(schema, v, "params")
}

// checkValue validates one value against one schema node.
func checkValue(s schemaNode, v any, path string) error {
	if t, ok := s["type"].(string); ok {
		if !hasType(t, v) {
			return fmt.Errorf("%w: %s must be %s", ErrParams, path, article(t))
		}
	}
	if enum, ok := s["enum"].([]any); ok {
		if !slices.ContainsFunc(enum, func(e any) bool { return equalJSON(e, v) }) {
			return fmt.Errorf("%w: %s is not one of the allowed values", ErrParams, path)
		}
	}
	switch x := v.(type) {
	case string:
		return checkString(s, x, path)
	case json.Number:
		return checkNumber(s, x, path)
	case []any:
		return checkArray(s, x, path)
	case map[string]any:
		return checkObject(s, x, path)
	}
	return nil
}

func checkString(s schemaNode, x, path string) error {
	n := utf8.RuneCountInString(x)
	if lim, ok := intOf(s["minLength"]); ok && n < lim {
		return fmt.Errorf("%w: %s is shorter than %d characters", ErrParams, path, lim)
	}
	if lim, ok := intOf(s["maxLength"]); ok && n > lim {
		return fmt.Errorf("%w: %s is longer than %d characters", ErrParams, path, lim)
	}
	if p, ok := s["pattern"].(string); ok {
		re, err := regexp.Compile(p)
		if err != nil || !re.MatchString(x) {
			return fmt.Errorf("%w: %s does not match the pattern %s", ErrParams, path, p)
		}
	}
	return nil
}

func checkNumber(s schemaNode, x json.Number, path string) error {
	f, err := x.Float64()
	if err != nil {
		return fmt.Errorf("%w: %s is not a number", ErrParams, path)
	}
	if lim, ok := floatOf(s["minimum"]); ok && f < lim {
		return fmt.Errorf("%w: %s is below %v", ErrParams, path, lim)
	}
	if lim, ok := floatOf(s["maximum"]); ok && f > lim {
		return fmt.Errorf("%w: %s is above %v", ErrParams, path, lim)
	}
	return nil
}

func checkArray(s schemaNode, x []any, path string) error {
	if lim, ok := intOf(s["minItems"]); ok && len(x) < lim {
		return fmt.Errorf("%w: %s has fewer than %d items", ErrParams, path, lim)
	}
	if lim, ok := intOf(s["maxItems"]); ok && len(x) > lim {
		return fmt.Errorf("%w: %s has more than %d items", ErrParams, path, lim)
	}
	if items, ok := s["items"].(map[string]any); ok {
		for i, item := range x {
			if err := checkValue(items, item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkObject(s schemaNode, x map[string]any, path string) error {
	if lim, ok := intOf(s["minProperties"]); ok && len(x) < lim {
		return fmt.Errorf("%w: %s has fewer than %d keys", ErrParams, path, lim)
	}
	if lim, ok := intOf(s["maxProperties"]); ok && len(x) > lim {
		return fmt.Errorf("%w: %s has more than %d keys", ErrParams, path, lim)
	}
	if req, ok := s["required"].([]any); ok {
		for _, r := range req {
			name, _ := r.(string)
			if _, present := x[name]; !present {
				return fmt.Errorf("%w: %s.%s is required", ErrParams, path, clipKey(name))
			}
		}
	}
	props, _ := s["properties"].(map[string]any)
	keys := make([]string, 0, len(x))
	for k := range x {
		keys = append(keys, k)
	}
	slices.Sort(keys) // a stable first error, whatever the map order
	for _, k := range keys {
		sub := path + "." + clipKey(k)
		if p, ok := props[k].(map[string]any); ok {
			if err := checkValue(p, x[k], sub); err != nil {
				return err
			}
			continue
		}
		switch ap := s["additionalProperties"].(type) {
		case bool:
			if !ap {
				return fmt.Errorf("%w: %s is not a parameter of this op", ErrParams, sub)
			}
		case map[string]any:
			if err := checkValue(ap, x[k], sub); err != nil {
				return err
			}
		}
	}
	return nil
}

// hasType reports whether v is of JSON Schema type t.
func hasType(t string, v any) bool {
	switch t {
	case "object":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	case "string":
		_, ok := v.(string)
		return ok
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "number":
		_, ok := v.(json.Number)
		return ok
	case "integer":
		n, ok := v.(json.Number)
		if !ok {
			return false
		}
		if _, err := n.Int64(); err == nil {
			return true
		}
		f, err := n.Float64()
		return err == nil && f == math.Trunc(f) && !math.IsInf(f, 0)
	case "null":
		return v == nil
	}
	return false
}

// equalJSON compares an enum member (decoded without UseNumber, so numbers
// are float64) with a value decoded with UseNumber.
func equalJSON(enum, v any) bool {
	if n, ok := v.(json.Number); ok {
		f, err := n.Float64()
		ef, eok := enum.(float64)
		return err == nil && eok && f == ef
	}
	return enum == v
}

// intOf reads a non-negative integer keyword.
func intOf(v any) (int, bool) {
	f, ok := floatOf(v)
	if !ok || f < 0 || f != math.Trunc(f) {
		return 0, false
	}
	return int(f), true
}

// floatOf reads a numeric keyword: the schema is decoded with encoding/json,
// so a number is a float64.
func floatOf(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	}
	return 0, false
}

// article renders a type name for an error.
func article(t string) string {
	switch t {
	case "object", "array", "integer":
		return "an " + t
	}
	return "a " + t
}

// clipKey cuts a key from the model's params for an error message.
func clipKey(k string) string {
	k = strings.ToValidUTF8(k, "?")
	if len(k) <= 64 {
		return k
	}
	cut := 64
	for cut > 0 && !utf8.RuneStart(k[cut]) {
		cut--
	}
	return k[:cut] + "…"
}
