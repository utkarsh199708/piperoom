package redact

import (
	"bytes"
	"encoding/json"
)

// JSON masks secrets in the string values of v. Maps and slices are walked;
// keys are kept. A string under a key that names a secret (password,
// client_secret, api_key...) is masked whole, by the same key rules Text
// applies to key=value pairs; every other string goes through Text.
//
// v is never modified. When nothing is masked, v itself is returned, so a
// caller's typed value (and its JSON field order) survives. Any other type
// (structs, typed maps) is converted through encoding/json first, with
// numbers kept as their exact text; if that fails, v is returned as is.
func (r *Redactor) JSON(v any) any {
	switch v.(type) {
	case nil, bool, float64, float32, int, int64, int32, uint, uint64, uint32, json.Number:
		return v
	case string, map[string]any, []any, map[string]string, []string:
		out, changed := r.walk(v)
		if !changed {
			return v
		}
		return out
	}
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var g any
	if err := dec.Decode(&g); err != nil {
		return v
	}
	out, changed := r.walk(g)
	if !changed {
		return v
	}
	return out
}

func (r *Redactor) walk(v any) (any, bool) {
	switch t := v.(type) {
	case string:
		s, st := r.Text(t)
		return s, st.Total > 0
	case map[string]any:
		out := make(map[string]any, len(t))
		changed := false
		for k, x := range t {
			nx, c := r.walkField(k, x)
			out[k] = nx
			changed = changed || c
		}
		return out, changed
	case map[string]string:
		out := make(map[string]string, len(t))
		changed := false
		for k, x := range t {
			nx, c := r.walkField(k, x)
			out[k] = nx.(string)
			changed = changed || c
		}
		return out, changed
	case []any:
		out := make([]any, len(t))
		changed := false
		for i, x := range t {
			nx, c := r.walk(x)
			out[i] = nx
			changed = changed || c
		}
		return out, changed
	case []string:
		out := make([]string, len(t))
		changed := false
		for i, x := range t {
			nx, c := r.walk(x)
			out[i] = nx.(string)
			changed = changed || c
		}
		return out, changed
	}
	return v, false
}

// walkField redacts one map value, using the key to decide on bare strings.
func (r *Redactor) walkField(key string, v any) (any, bool) {
	s, ok := v.(string)
	if !ok {
		return r.walk(v)
	}
	if s != "" && !isMasked(s) && !isPlaceholder(s) {
		switch secretKey(key) {
		case keyStrong:
			return mask(s, keyStyle(key)), true
		case keyWeak:
			if looksRandom(s, 12) {
				return mask(s, styleToken), true
			}
		}
	}
	return r.walk(s)
}
