package kafka

import (
	"bytes"
	"encoding/json"
	"strings"
)

// keyMatcher compares a caller's key with decoded record keys. decodeKey
// renders a one-field Avro key as its bare value and a multi-field key as
// JSON with sorted field names, while a caller may type the fields in any
// order or give a one-field key as JSON; both sides are canonicalised.
type keyMatcher struct {
	raw    string
	canon  string // canonical JSON of raw; "" when raw is not a JSON object or array
	single string // bare value of a one-field JSON object; "" otherwise
}

func newKeyMatcher(key string) keyMatcher {
	m := keyMatcher{raw: key}
	v, ok := decodeJSON(key)
	if !ok {
		return m
	}
	if b, err := json.Marshal(v); err == nil {
		m.canon = string(b)
	}
	if obj, ok := v.(map[string]any); ok && len(obj) == 1 {
		for _, val := range obj {
			m.single = bareValue(val)
		}
	}
	return m
}

func (m keyMatcher) matches(decoded string) bool {
	if decoded == m.raw {
		return true
	}
	if m.single != "" && decoded == m.single {
		return true
	}
	if m.canon == "" {
		return false
	}
	return canonicalJSON(decoded) == m.canon
}

// canonicalJSON re-encodes a JSON object or array with sorted keys and
// numbers kept literally; "" when s is not one.
func canonicalJSON(s string) string {
	v, ok := decodeJSON(s)
	if !ok {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// decodeJSON decodes s when it is a single JSON object or array. Numbers stay
// json.Number so large ids keep every digit.
func decodeJSON(s string) (any, bool) {
	t := strings.TrimSpace(s)
	if t == "" || (t[0] != '{' && t[0] != '[') {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(t)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		return nil, false
	}
	return v, true
}

// bareValue renders a JSON value the way decodeKey renders a one-field key.
func bareValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case nil:
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}
