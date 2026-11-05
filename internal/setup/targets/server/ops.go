package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// ---- shared ----

// decodeStrict fills dst from params, refusing unknown fields, trailing JSON
// and keys that differ only in letter case (protocol.CheckParamKeys). Empty
// params are the empty object.
func decodeStrict(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if err := protocol.CheckParamKeys(raw); err != nil {
		return fmt.Errorf("%w: %v", ErrParams, err)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: %v", ErrParams, err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing JSON", ErrParams)
	}
	return nil
}

// now is the injected clock, defaulted for an Env built by hand.
func now(env ops.Env) time.Time {
	if env.Now == nil {
		return time.Now()
	}
	return env.Now()
}
