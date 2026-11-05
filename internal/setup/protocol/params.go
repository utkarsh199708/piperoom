package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode"
)

// ErrParamKeys reports params with two object keys, at any depth, that are
// equal under Unicode case folding (exact duplicates included). Go's
// encoding/json matches keys to struct fields without regard to case and lets
// the last one win, while the params hash (JCS) and a map read keep both: so
// {"name":"wal_level","NAME":"archive_command"} would hash, guard and print as
// one value and execute as the other. Every layer that reads step params
// (proposal creation, the room's guard, the setup clients, the runner) refuses
// them with CheckParamKeys.
var ErrParamKeys = errors.New("setup: params hold two keys that differ only in letter case")

// CheckParamKeys returns ErrParamKeys (wrapped, naming the path) when an object
// anywhere in raw holds two keys that FoldKey maps to the same string. It reads
// the raw tokens, so duplicates a map decode would collapse are seen too. Raw
// that is not JSON is not this check's concern and passes (the decoders refuse
// it).
func CheckParamKeys(raw json.RawMessage) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var stack []*keyFrame
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return nil
		}
		var top *keyFrame
		if len(stack) > 0 {
			top = stack[len(stack)-1]
		}
		if top != nil && top.keys != nil && top.expectKey {
			if d, ok := tok.(json.Delim); ok && d == '}' {
				stack = stack[:len(stack)-1]
				markValueDone(stack)
				continue
			}
			key, _ := tok.(string)
			f := FoldKey(key)
			if other, dup := top.keys[f]; dup {
				return fmt.Errorf("%w: %q and %q", ErrParamKeys, clipKey(other), clipKey(key))
			}
			top.keys[f] = key
			top.expectKey = false
			continue
		}
		switch tok {
		case json.Delim('{'):
			stack = append(stack, &keyFrame{keys: map[string]string{}, expectKey: true})
		case json.Delim('['):
			stack = append(stack, &keyFrame{})
		case json.Delim(']'), json.Delim('}'):
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			markValueDone(stack)
		default:
			markValueDone(stack)
		}
	}
}

// keyFrame is one open container of CheckParamKeys: an object's folded keys,
// or nil keys for an array. expectKey says the object's next token is a key.
type keyFrame struct {
	keys      map[string]string
	expectKey bool
}

// markValueDone records that the innermost object's current value ended, so
// its next token is a key.
func markValueDone(stack []*keyFrame) {
	if n := len(stack); n > 0 && stack[n-1].keys != nil {
		stack[n-1].expectKey = true
	}
}

// FoldKey maps a key to a representative of its Unicode case-folding orbit, so
// two keys that strings.EqualFold (and so encoding/json) treats as equal,
// including the Kelvin sign and the long s, get the same string.
func FoldKey(s string) string {
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

// clipKey bounds a key quoted in an error.
func clipKey(k string) string {
	if len(k) > 64 {
		return k[:64] + "…"
	}
	return k
}
