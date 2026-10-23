package setupactions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

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
// The runner catalogue does not register any preview_* op yet, so no spec in
// EnvironmentSpecs names one: file_write, file_delete, compose_write,
// properties_write and plugin_install leave PreviewOp empty, and their
// Preview answers "this action has no preview" without a runner round trip
// (previewPendingRunner in the tests lists them and fails once the runner
// registers the op, so the spec is switched over then). A spec may name
// another T0 op instead (compose_up, compose_restart and compose_down name
// compose_config, with PreviewDecode mapping its result), but it must never
// name the write op: New refuses that.
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
	out, err := decode(params, res.Data)
	if err != nil {
		return actions.PreviewResult{}, fmt.Errorf("%w: %s: %v", ErrBadPreview, a.spec.Name, err)
	}
	if strings.TrimSpace(out.Target) == "" && strings.TrimSpace(out.Expected) == "" {
		return actions.PreviewResult{}, fmt.Errorf("%w: %s: the preview names no target and no expected state", ErrBadPreview, a.spec.Name)
	}
	return out, nil
}

// decodePreviewResult decodes Data that is an events.PreviewResult itself.
func decodePreviewResult(_, data json.RawMessage) (events.PreviewResult, error) {
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

const (
	// maxDryRunText caps DryRun.Text, the bound events.DryRunResult
	// documents (the system's messages, redacted, ≤ 2 KiB).
	maxDryRunText = 2 << 10
	// maxPreviewFile caps the file name a preview sentence quotes.
	maxPreviewFile = 128
	// composeConfigSource is DryRun.Source of a Compose stack preview. No
	// Compose stack op has a dry run: the text is the config render only, and
	// the source says so, so OK never reads as "the op was tried and passed".
	composeConfigSource = "compose config (render only)"
	// truncatedMarker is the last line of a render that was cut.
	truncatedMarker = "… (truncated)"
	// maxStackServices mirrors the runner's cap on named services
	// (targets/compose maxStackServices, the stack schema's maxItems).
	maxStackServices = 16
)

// composeServicePattern is the runner's service-name vocabulary, copied
// exactly from internal/setup/targets/compose (serviceNameRE in
// templates.go, unexported there): importing that package would pull the
// runner's targets into the server's action layer. A test parses the
// runner's source and fails if the two ever differ. A step names services
// only when every one has it, so a preview sentence on the approval card
// never carries markup or a control byte the model wrote.
const composeServicePattern = `^[a-z0-9][a-z0-9_-]{0,62}$`

var composeServiceRE = regexp.MustCompile(composeServicePattern)

// composeConfigPreview returns the PreviewDecode of a Compose stack op
// ("up", "restart" or "down") previewed through compose_config:
//
//   - Target is the rendered file;
//   - Expected is one short sentence on the state after the op, built from
//     the op and the step's services ("start services kafka, connect from
//     compose.yaml in the background", "stop and remove the containers and
//     networks of the stack of compose.yaml; volumes kept"), never the
//     render itself, because events.PreviewResult
//     defines Current and Expected as short human sentences;
//   - Current names the read the preview made ("compose config
//     --no-interpolate of compose.yaml");
//   - the masked render goes into DryRun{Source: "compose config (render
//     only)", OK: true} with its Text cut to maxDryRunText (2 KiB) by
//     clipRender: whole lines only, so a secret on the cut line never
//     survives in part (the room's redaction matches whole shapes, such as a
//     URL with a password, and a fragment could slip past it).
//
// Data of another shape (an unknown field), a cut body, a render with no file
// or no body, and a service name that is not a Compose service name are
// refused.
func composeConfigPreview(op string) func(params, data json.RawMessage) (events.PreviewResult, error) {
	return func(params, data json.RawMessage) (events.PreviewResult, error) {
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
		services, err := stackServices(params)
		if err != nil {
			return events.PreviewResult{}, err
		}
		file := previewLine(in.File, maxPreviewFile)
		expected, err := composeExpected(op, file, services)
		if err != nil {
			return events.PreviewResult{}, err
		}
		text := clipRender(in.Body, maxDryRunText)
		return events.PreviewResult{
			Target:   file,
			Current:  "compose config --no-interpolate of " + file,
			Expected: expected,
			DryRun:   &events.DryRunResult{OK: true, Source: composeConfigSource, Text: text},
		}, nil
	}
}

// composeExpected is the one-sentence expected state after a Compose op.
func composeExpected(op, file string, services []string) (string, error) {
	named := ""
	if len(services) > 0 {
		named = "services " + strings.Join(services, ", ")
	}
	switch op {
	case "up":
		if named == "" {
			return "start every service of " + file + " in the background", nil
		}
		return "start " + named + " from " + file + " in the background", nil
	case "restart":
		if named == "" {
			return "restart every service of " + file + " in place; volumes kept", nil
		}
		return "restart " + named + " of " + file + " in place; volumes kept", nil
	case "down":
		if named == "" {
			return "stop and remove the containers and networks of the stack of " + file + "; volumes kept", nil
		}
		return "stop and remove the containers of " + named + " of " + file + "; volumes kept", nil
	}
	return "", fmt.Errorf("no preview sentence for compose %q", op)
}

// checkStackParams is the CheckParams of the Compose stack ops: at most
// maxStackServices services, each a runner service name. It runs in
// validateParams, so Validate, Preview and Execute all refuse such params
// before anything is sent.
func checkStackParams(params json.RawMessage) error {
	_, err := stackServices(params)
	return err
}

// stackServices reads the services a Compose step names: at most
// maxStackServices, each a runner service name. The runner validates them
// again before it runs.
func stackServices(params json.RawMessage) ([]string, error) {
	if len(bytes.TrimSpace(params)) == 0 {
		return nil, nil
	}
	var in struct {
		Services []string `json:"services"`
	}
	if err := json.Unmarshal(params, &in); err != nil {
		return nil, fmt.Errorf("the step's services: %v", err)
	}
	if len(in.Services) > maxStackServices {
		return nil, fmt.Errorf("the step names %d services (at most %d)", len(in.Services), maxStackServices)
	}
	for _, svc := range in.Services {
		if !composeServiceRE.MatchString(svc) {
			return nil, errors.New("the step names a service that is not a Compose service name")
		}
	}
	return in.Services, nil
}

// previewLine is untrusted runner text made fit for one line of a preview:
// valid UTF-8, with control characters (Cc), format characters (Cf: zero
// width, BOM, bidi overrides) and line and paragraph separators (Zl, Zp)
// turned into spaces, and at most limit bytes including the "…" a cut adds.
func previewLine(s string, limit int) string {
	s = strings.ToValidUTF8(s, "?")
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
			return ' '
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	if len(s) <= limit {
		return s
	}
	const ellipsis = "…"
	out, _ := clipBytes(s, limit-len(ellipsis))
	return out + ellipsis
}

// clipRender cuts a render to at most limit bytes of whole lines: when it is
// longer, everything after the last newline that fits (with the marker) is
// dropped, partial line included, and truncatedMarker is added as the last
// line. A cut never splits a line, so never a secret's shape on it, and the
// result is always valid UTF-8.
func clipRender(s string, limit int) string {
	s = strings.ToValidUTF8(s, "?")
	if len(s) <= limit {
		return s
	}
	budget := limit - len(truncatedMarker)
	if budget < 0 {
		budget = 0
	}
	head := s[:budget]
	if i := strings.LastIndexByte(head, '\n'); i >= 0 {
		return head[:i+1] + truncatedMarker
	}
	return truncatedMarker
}

// clipBytes cuts valid UTF-8 s to at most limit bytes on a rune boundary and
// reports whether it cut.
func clipBytes(s string, limit int) (string, bool) {
	if len(s) <= limit {
		return s, false
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut], true
}
