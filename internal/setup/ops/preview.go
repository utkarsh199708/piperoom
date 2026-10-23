package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// OpPreview is the runner's preview op: a T0 read that asks a T1+ op what its
// Execute would change, without changing anything. The setup action adapters
// (internal/setup/actions) send it from actions.Preview, so the approval card
// shows what the runner sees (a file diff, a rendered unit) and the server
// never runs a write to find out.
const OpPreview = "op_preview"

// ErrNoPreview reports an op that does not implement Previewer. The result
// carries the error code "not_supported", so the server reads it as
// clients.ErrNotSupported (a step without a preview, never a failed target).
var ErrNoPreview = errors.New("ops: op has no preview")

// Previewer is implemented by an op that can say, without writing, what its
// Execute would do (design §2.5, Op.Preview). Preview must only read: it runs
// under the same confinement and redaction as every other op, and the runner
// executes it as a T0 op with no proposal.
type Previewer interface {
	Preview(ctx context.Context, params json.RawMessage, env Env) (events.PreviewResult, error)
}

// previewParams is op_preview's params: the op to preview and its params,
// exactly as a proposal step would carry them.
type previewParams struct {
	Op     string          `json:"op"`
	Params json.RawMessage `json:"params,omitempty"`
}

// previewData is op_preview's result Data.
type previewData struct {
	Op        string                `json:"op"`
	Preview   *events.PreviewResult `json:"preview,omitempty"`
	ErrorCode string                `json:"error_code,omitempty"`
}

// previewOp is op_preview. It holds the registry so it sees the ops a driver
// registered after New.
type previewOp struct{ r *Registry }

func (previewOp) Name() string   { return OpPreview }
func (previewOp) Target() string { return "" }
func (previewOp) Tier() string   { return protocol.TierT0 }
func (previewOp) Layer() string  { return "environment" }
func (previewOp) Describe() string {
	return "say what a T1 or higher op would change, without changing anything"
}

func (previewOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"op"},
		"properties": map[string]any{
			"op":     map[string]any{"type": "string", "minLength": 1, "maxLength": maxLoggedNameBytes},
			"params": map[string]any{"type": "object"},
		},
	}
}

// inner decodes params and returns the op to preview after its own Validate:
// an op this catalogue has, never op_preview itself and never a T0 op (a read
// has nothing to preview, and previewing one would be a second way to run it).
func (o previewOp) inner(params json.RawMessage, env Env) (Op, previewParams, error) {
	var p previewParams
	if err := decodeParams(params, &p); err != nil {
		return nil, p, err
	}
	if p.Op == "" || p.Op == OpPreview {
		return nil, p, fmt.Errorf("%w: op_preview needs the op to preview", ErrParams)
	}
	op, ok := o.r.Get(p.Op)
	if !ok {
		return nil, p, fmt.Errorf("%w: %q", ErrUnknownOp, clipName(p.Op))
	}
	if op.Tier() == protocol.TierT0 {
		return nil, p, fmt.Errorf("%w: %s is a read; only a T1 or higher op has a preview", ErrParams, op.Name())
	}
	if len(p.Params) == 0 {
		p.Params = json.RawMessage(`{}`)
	}
	if err := op.Validate(p.Params, env); err != nil {
		return nil, p, err
	}
	return op, p, nil
}

func (o previewOp) Validate(params json.RawMessage, env Env) error {
	_, _, err := o.inner(params, env)
	return err
}

// ParamTarget is the previewed op's target kind, so the runner raises the
// request's class to the floor of what the preview reads.
func (o previewOp) ParamTarget(params json.RawMessage, env Env) string {
	var p previewParams
	if decodeParams(params, &p) != nil || p.Op == OpPreview {
		return ""
	}
	op, ok := o.r.Get(p.Op)
	if !ok {
		return ""
	}
	if pt, ok := op.(ParamTargeter); ok {
		return pt.ParamTarget(p.Params, env)
	}
	return op.Target()
}

func (o previewOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	op, p, err := o.inner(params, env)
	if err != nil {
		return Result{}, err
	}
	pv, ok := op.(Previewer)
	if !ok {
		return Result{
			Summary: "no preview: " + op.Name() + " cannot say what it would change before it runs",
			Data:    previewData{Op: op.Name(), ErrorCode: "not_supported"},
		}, fmt.Errorf("%w: %s", ErrNoPreview, op.Name())
	}
	res, err := pv.Preview(ctx, p.Params, env)
	if err != nil {
		return Result{}, fmt.Errorf("ops: preview of %s: %w", op.Name(), err)
	}
	return Result{
		Summary: "preview of " + op.Name() + ": " + clip(res.Target) + ": " + clip(res.Expected),
		Data:    previewData{Op: op.Name(), Preview: &res},
	}, nil
}
