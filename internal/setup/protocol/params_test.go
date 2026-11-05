package protocol_test

import (
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/setup/protocol"
)

// TestCheckParamKeys: keys equal under case folding are refused at any depth,
// exact duplicates included; distinct keys, the same key in sibling objects
// and keys inside string values pass.
func TestCheckParamKeys(t *testing.T) {
	bad := []string{
		`{"name":"wal_level","NAME":"archive_command","value":"x"}`, // the probe
		`{"name":"wal_level","name":"archive_command"}`,
		`{"a":{"Key":1,"key":2}}`,
		`{"a":[{"x":1},{"y":1,"Y":2}]}`,
		`{"kind":1,"Kind":2}`, // the Kelvin sign folds to k
		`[{"s":1,"ſ":2}]`,     // the long s folds to s
		`{"a":{"b":[1,2,{"c":{"d":1,"D":2}}]},"e":3}`,
	}
	for _, raw := range bad {
		if err := protocol.CheckParamKeys(json.RawMessage(raw)); !errors.Is(err, protocol.ErrParamKeys) {
			t.Fatalf("CheckParamKeys(%s) = %v, want ErrParamKeys", raw, err)
		}
	}
	good := []string{
		`{}`, ``, `[]`, `"x"`, `42`,
		`{"name":"wal_level","value":"logical"}`,
		`{"a":{"k":1},"b":{"k":2}}`,
		`{"a":[{"k":1},{"k":2}],"k":3}`,
		`{"a":"{\"k\":1,\"K\":2}"}`,
		`{"nested":{"x":{"y":1}},"x":{"y":2}}`,
	}
	for _, raw := range good {
		if err := protocol.CheckParamKeys(json.RawMessage(raw)); err != nil {
			t.Fatalf("CheckParamKeys(%s) = %v, want nil", raw, err)
		}
	}
}
