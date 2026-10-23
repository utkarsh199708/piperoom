package setupactions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/remoteclients"
	"piperoom/internal/tools"
)

// PreviewOpName is the name of the T0 op that would preview a setup action:
// "preview_" + the action's op, rendering what the write would do (the op's
// own Preview, design §2.5) without changing anything.
//
// The runner catalogue does not register these preview ops yet
// (previewOpsPendingRunner in the tests lists the ones the specs name). Until
// it does, the runner refuses each as unknown_op and Preview of that action
// fails closed: it returns an error, never an empty result that could pass. A
// spec may name another T0 op instead (compose_up, compose_restart and
// compose_down name compose_config, with PreviewDecode mapping its result),
// but it must never name the write op: New refuses that.
func PreviewOpName(op string) string { return "preview_" + op }

// Preview renders what Execute would do, without writing: exactly one T0
// request to the spec's preview op, with the step's params and no proposal
// field. It never sends the write op and never a second op.
//
// The result is the runner's Data mapped by the spec's PreviewDecode, or
// decoded as an events.PreviewResult (the shape the room stores on
// proposal.previewed) when the spec has none. A result the runner cut is
// refused: a cut preview is not the endpoint's answer. A decoded result with
// neither a Target nor an Expected is refused too, so Data of another shape
// (which decodes to the zero result) can never count as a preview that passed.
// The scope rule is checked first, so an out-of-scope target never reaches the
// runner.
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
	decode := a.spec.PreviewDecode
	if decode == nil {
		decode = decodePreviewResult
	}
	out, err := decode(res.Data)
	if err != nil {
		return actions.PreviewResult{}, fmt.Errorf("%w: %s: %v", ErrBadPreview, a.spec.Name, err)
	}
	if strings.TrimSpace(out.Target) == "" && strings.TrimSpace(out.Expected) == "" {
		return actions.PreviewResult{}, fmt.Errorf("%w: %s: the preview names no target and no expected state", ErrBadPreview, a.spec.Name)
	}
	return out, nil
}

// decodePreviewResult decodes Data that is an events.PreviewResult itself.
func decodePreviewResult(data json.RawMessage) (events.PreviewResult, error) {
	var out events.PreviewResult
	if err := json.Unmarshal(data, &out); err != nil {
		return events.PreviewResult{}, err
	}
	return out, nil
}

// composeConfigData is the Data of the runner's compose_config op
// (internal/setup/targets/compose: the rendered file, its masked body, the
// render's size, the masked keys and whether the body was cut).
type composeConfigData struct {
	File       string   `json:"file"`
	Body       string   `json:"body"`
	Bytes      int      `json:"bytes"`
	MaskedKeys []string `json:"masked_keys,omitempty"`
	Truncated  bool     `json:"truncated,omitempty"`
}

// composeConfigPreview maps a compose_config result to the preview of a
// Compose stack op: the target is the rendered file and the expected state is
// the effective config compose would run. Data of another shape (an unknown
// field), a cut body, or a render with no file or no body is refused.
func composeConfigPreview(data json.RawMessage) (events.PreviewResult, error) {
	var in composeConfigData
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return events.PreviewResult{}, fmt.Errorf("not a compose_config result: %v", err)
	}
	if in.Truncated {
		return events.PreviewResult{}, errors.New("the compose_config render was cut")
	}
	if strings.TrimSpace(in.File) == "" || strings.TrimSpace(in.Body) == "" {
		return events.PreviewResult{}, errors.New("the compose_config render names no file or has no body")
	}
	return events.PreviewResult{Target: in.File, Expected: in.Body}, nil
}
