package setupactions

import (
	"context"
	"encoding/json"
	"fmt"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/remoteclients"
	"piperoom/internal/tools"
)

// PreviewOpName is the name of the T0 op that previews a setup action:
// "preview_" + the action's op. The runner catalogue registers one per setup
// action; its Execute renders what the write would do (the op's own Preview,
// design §2.5) and changes nothing. A spec may name another T0 op instead
// (compose_write names compose_config), but it must never name the write op:
// New refuses that.
func PreviewOpName(op string) string { return "preview_" + op }

// Preview renders what Execute would do, without writing: exactly one T0
// request to the spec's preview op, with the step's params and no proposal
// field. It never sends the write op and never a second op.
//
// The result is the runner's Data decoded as an events.PreviewResult (the same
// shape the room stores on proposal.previewed). A result the runner cut is
// refused: a cut preview is not the endpoint's answer. The scope rule is
// checked first, so an out-of-scope target never reaches the runner.
func (a *Adapter) Preview(ctx context.Context, params json.RawMessage) (actions.PreviewResult, error) {
	if err := a.validateParams(params); err != nil {
		return actions.PreviewResult{}, err
	}
	if a.spec.PreviewOp == "" {
		return actions.PreviewResult{}, fmt.Errorf("setupactions: %s: this action has no preview", a.spec.Name)
	}
	p, ok := tools.PipelineFrom(ctx)
	if !ok {
		return actions.PreviewResult{}, fmt.Errorf("setupactions: %s: preview: %w", a.spec.Name, tools.ErrNoScope)
	}
	if err := a.spec.Scope(p, params); err != nil {
		return actions.PreviewResult{}, fmt.Errorf("setupactions: %s: preview: %w", a.spec.Name, err)
	}
	previewParams := params
	if a.spec.PreviewParams != nil {
		mapped, err := a.spec.PreviewParams(params)
		if err != nil {
			return actions.PreviewResult{}, fmt.Errorf("%w: %s: preview params: %v", ErrBadInput, a.spec.Name, err)
		}
		previewParams = mapped
	}
	if err := a.validateParams(previewParams); err != nil {
		return actions.PreviewResult{}, err
	}
	class := remoteclients.ClassFrom(ctx)
	if !plan.ValidClass(class) {
		// A preview is a read: the lowest class never lowers the runner's own
		// floor for the target, so an unknown class is sent as local.
		class = protocol.ClassLocal
	}
	res, err := a.send(ctx, protocol.OpRequest{
		Op:     a.spec.PreviewOp,
		Params: append(json.RawMessage(nil), previewParams...),
		Tier:   protocol.TierT0,
		Class:  class,
	})
	if err != nil {
		return actions.PreviewResult{}, err
	}
	if res.Truncated {
		return actions.PreviewResult{}, fmt.Errorf("%w: %s: the runner cut the preview", ErrBadPreview, a.spec.Name)
	}
	if len(res.Data) == 0 {
		return actions.PreviewResult{}, fmt.Errorf("%w: %s: no preview data", ErrBadPreview, a.spec.Name)
	}
	var out events.PreviewResult
	if err := json.Unmarshal(res.Data, &out); err != nil {
		return actions.PreviewResult{}, fmt.Errorf("%w: %s: %v", ErrBadPreview, a.spec.Name, err)
	}
	return out, nil
}
