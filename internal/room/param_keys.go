package room

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode"
)

// Go's encoding/json matches object keys to struct fields without regard to
// case, so {"key":"shown","Key":"hidden"} (or the same with the Kelvin sign
// U+212A for k) decodes into one field and which of the two wins is the
// decoder's business: an action could run a value the approver never read, and
// two stores (memstore and jsonb) could disagree. A proposal's params are
// therefore refused unless every object key is exactly, case-sensitively, a
// name the action's ParamsSchema lists, and no object at any depth holds two
// keys that are equal under Unicode case folding.

// checkParamKeys returns a validation error when params break the rule above.
// schema is the action's ParamsSchema(); a schema with no "properties" member
// (or an object with no schema at all, deeper down) is only checked for case
// folded duplicates.
func checkParamKeys(schema map[string]any, params json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(params))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil // not our concern here: Validate and the hash step report bad JSON
	}
	return walkParamKeys(schema, v, "params")
}

func walkParamKeys(schema map[string]any, v any, path string) error {
	switch x := v.(type) {
	case map[string]any:
		folded := make(map[string]string, len(x))
		for k := range x {
			f := foldKey(k)
			if other, dup := folded[f]; dup {
				return fmt.Errorf("%s has the keys %q and %q, which differ only in letter case; use exactly one", path, other, k)
			}
			folded[f] = k
		}
		props, hasProps := schema["properties"].(map[string]any)
		for k, child := range x {
			var sub map[string]any
			if hasProps {
				ps, ok := props[k]
				if !ok {
					return fmt.Errorf("%s has the key %q, which is not a parameter of this action (keys are case-sensitive)", path, k)
				}
				sub, _ = ps.(map[string]any)
			}
			if err := walkParamKeys(sub, child, path+"."+k); err != nil {
				return err
			}
		}
	case []any:
		var items map[string]any
		if schema != nil {
			items, _ = schema["items"].(map[string]any)
		}
		for i, child := range x {
			if err := walkParamKeys(items, child, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// foldKey maps a key to a representative of its Unicode case-folding orbit, so
// two keys that strings.EqualFold treats as equal (including the Kelvin sign
// and the long s) get the same string. It is linear in the key length.
func foldKey(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		min := r
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			if f < min {
				min = f
			}
		}
		out = append(out, min)
	}
	return string(out)
}
