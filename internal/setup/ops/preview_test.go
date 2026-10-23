package ops

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// fakeWriteOp is a T1 op that counts Execute calls; withPreview adds Preview.
type fakeWriteOp struct {
	name     string
	executed *int
	target   string
}

func (o fakeWriteOp) Name() string   { return o.name }
func (o fakeWriteOp) Target() string { return o.target }
func (fakeWriteOp) Tier() string     { return protocol.TierT1 }
func (fakeWriteOp) Layer() string    { return "environment" }
func (fakeWriteOp) Describe() string { return "a fake write" }
func (fakeWriteOp) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false,
		"properties": map[string]any{"path": map[string]any{"type": "string"}}}
}
func (fakeWriteOp) Validate(params json.RawMessage, _ Env) error {
	var p struct {
		Path string `json:"path"`
	}
	if err := decodeParams(params, &p); err != nil {
		return err
	}
	if p.Path == "bad" {
		return errors.New("ops: refused path bad")
	}
	return nil
}
func (o fakeWriteOp) Execute(context.Context, json.RawMessage, Env) (Result, error) {
	*o.executed++
	return Result{Summary: "wrote"}, nil
}

type fakePreviewOp struct{ fakeWriteOp }

func (fakePreviewOp) Preview(_ context.Context, params json.RawMessage, _ Env) (events.PreviewResult, error) {
	var p struct {
		Path string `json:"path"`
	}
	_ = json.Unmarshal(params, &p)
	return events.PreviewResult{Target: p.Path, Current: "absent", Expected: "+ one line", Reversible: true, Rollback: "file_delete"}, nil
}

func TestOpPreview(t *testing.T) {
	r := testRegistry(t)
	var executed int
	if err := r.Register(fakeWriteOp{name: "plain_write", executed: &executed}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(fakePreviewOp{fakeWriteOp{name: "previewed_write", executed: &executed, target: "compose"}}); err != nil {
		t.Fatal(err)
	}

	run := func(params string) (protocol.OpResult, error) {
		return r.Execute(context.Background(), protocol.OpRequest{ID: "p1", Op: OpPreview, Params: json.RawMessage(params), Tier: protocol.TierT0})
	}

	res, err := run(`{"op":"previewed_write","params":{"path":"notes.txt"}}`)
	if err != nil || !res.OK {
		t.Fatalf("preview = %+v, %v", res, err)
	}
	var data previewData
	if err := json.Unmarshal(res.Data, &data); err != nil || data.Preview == nil {
		t.Fatalf("data = %s, %v", res.Data, err)
	}
	if data.Op != "previewed_write" || data.Preview.Target != "notes.txt" || !data.Preview.Reversible {
		t.Fatalf("preview = %+v", data)
	}
	if got := r.Target(protocol.OpRequest{Op: OpPreview, Params: json.RawMessage(`{"op":"previewed_write"}`)}); got != "compose" {
		t.Errorf("Target = %q, want the previewed op's kind", got)
	}

	// An op without Preview answers not_supported, and nothing runs.
	res, err = run(`{"op":"plain_write","params":{"path":"a"}}`)
	if !errors.Is(err, ErrNoPreview) || res.OK || !strings.Contains(string(res.Data), `"error_code":"not_supported"`) {
		t.Fatalf("no preview = %+v, %v", res, err)
	}

	for _, tc := range []struct{ name, params string }{
		{"unknown op", `{"op":"no_such_op"}`},
		{"itself", `{"op":"op_preview","params":{"op":"previewed_write"}}`},
		{"a read", `{"op":"file_read","params":{"path":"a"}}`},
		{"inner validate", `{"op":"previewed_write","params":{"path":"bad"}}`},
		{"inner unknown field", `{"op":"previewed_write","params":{"rm":"-rf"}}`},
		{"no op", `{}`},
		{"unknown field", `{"op":"previewed_write","run":true}`},
	} {
		if res, err := run(tc.params); err == nil || res.OK {
			t.Errorf("%s: preview accepted: %+v", tc.name, res)
		}
	}
	if executed != 0 {
		t.Fatalf("a preview executed the op %d times", executed)
	}
}
