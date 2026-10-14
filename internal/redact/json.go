package redact

import (
	"bytes"
	"encoding/json"
	"strings"
)

// JSON masks secrets in the string values of v; see JSONStats.
func (r *Redactor) JSON(v any) any {
	out, _ := r.JSONStats(v)
	return out
}

// JSONStats masks secrets in the string values of v and reports what it
// masked. v is first turned into a generic JSON tree (encoding/json, numbers
// kept as their exact text), so typed values at any depth are covered:
// structs, typed slices, []map[string]any, json.RawMessage inside a map.
// Maps and slices are walked and keys kept. Under a key that names a secret
// (password, client_secret, api_key, credentials...) every scalar below it is
// masked, numbers included, through nested lists and objects. Under a key
// that only may name one (token, auth, session) a random-looking string is
// masked. Under a key that names PII (ssn, phone, card_number), a string
// holding a digit or a number is masked. Every other string goes through Text.
//
// v is never modified. When nothing is masked, v itself is returned, so a
// caller's typed value (and its JSON field order) survives. If v cannot be
// encoded it is returned as is: encoding it for the room log fails the same
// way, so nothing of it is stored.
func (r *Redactor) JSONStats(v any) (any, Stats) {
	var st Stats
	switch t := v.(type) {
	case nil, bool, float64, float32, int, int64, int32, uint, uint64, uint32, json.Number:
		return v, st
	case string:
		s, st := r.Text(t)
		return s, st
	}
	b, err := json.Marshal(v)
	if err != nil {
		return v, st
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var g any
	if err := dec.Decode(&g); err != nil {
		return v, st
	}
	out := r.walk(g, false, &st)
	if st.Total == 0 {
		return v, st
	}
	return out, st
}

// walk redacts a generic JSON tree. secret is true below a secret-named key.
func (r *Redactor) walk(v any, secret bool, st *Stats) any {
	switch t := v.(type) {
	case string:
		if secret {
			if t == "" || isMasked(t) || isPlaceholder(t) {
				return t
			}
			st.add(KindKeyValue)
			return mask(t, styleSecret)
		}
		s, ls := r.Text(t)
		st.merge(ls)
		return s
	case json.Number:
		if secret {
			st.add(KindKeyValue)
			return mask(string(t), styleSecret)
		}
		return t
	case bool:
		if secret {
			st.add(KindKeyValue)
			return marker + " 8]"
		}
		return t
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = r.walkField(k, x, secret, st)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = r.walk(x, secret, st)
		}
		return out
	}
	return v
}

// walkField redacts one map value, letting the key decide.
func (r *Redactor) walkField(key string, v any, secret bool, st *Stats) any {
	if secret {
		return r.walk(v, true, st)
	}
	if r.pii && piiKey(key) {
		switch t := v.(type) {
		case string:
			if strings.ContainsAny(t, "0123456789") && !isMasked(t) {
				st.add(KindPIIValue)
				return mask(t, styleSecret)
			}
		case json.Number:
			st.add(KindPIIValue)
			return mask(string(t), styleSecret)
		}
	}
	switch secretKey(key) {
	case keyStrong:
		return r.walk(v, true, st)
	case keyWeak:
		if s, ok := v.(string); ok && !isMasked(s) && looksRandom(s, 12) {
			st.add(KindKeyValue)
			return mask(s, styleToken)
		}
	}
	return r.walk(v, false, st)
}
