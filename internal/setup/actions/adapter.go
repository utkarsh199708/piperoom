package setupactions

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/setup/errcode"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/remoteclients"
	"piperoom/internal/tools"
)

// ErrNoRunner reports that no runner is paired to the call's room: the
// executor is nil, or neither the adapter nor the call names a room. It is
// protocol.ErrNoRunner and is answered before any OpRequest is built.
var ErrNoRunner = protocol.ErrNoRunner

// ErrBadSpec reports a Spec New refuses: an empty name, an unknown tier, a
// schema that is not a closed object, no scope rule, or a preview op that is
// the write op itself.
var ErrBadSpec = errors.New("setupactions: invalid action spec")

// ErrBadInput reports params that are not the op's bounded input object. It
// wraps tools.ErrBadInput, the sentinel the agent reports to the model.
var ErrBadInput = tools.ErrBadInput

// ErrNoStep reports a write outside a setup proposal step: the call's context
// carries no room.SetupStep. Nothing is sent.
var ErrNoStep = errors.New("setupactions: the call carries no setup proposal step")

// ErrStepMismatch reports a write whose op, params, proposal id or step index
// are not the frozen proposal step's. Sending another write under a step's
// approval is exactly what this adapter must never do, so it refuses before
// anything is sent.
var ErrStepMismatch = errors.New("setupactions: the write is not the frozen proposal step's write")

// ErrBadPreview reports a preview result this package cannot use: the runner
// cut it, it carries no data, the data does not decode to an
// events.PreviewResult, or the decoded result names neither a target nor an
// expected state (an empty preview is never shown as one that passed).
var ErrBadPreview = errors.New("setupactions: unusable preview result")

const (
	// writeTimeout bounds a T1 or T2 write; a T3 op waits for a person's "y"
	// on the runner's terminal and gets minutes. The room's own step bound
	// still wins when it is shorter.
	writeTimeout = 30 * time.Second
	t3Timeout    = 5 * time.Minute
	// longOpTimeout is Spec.Timeout of the long environment ops
	// (compose_up, plugin_install): above the runner's own 5 minute download
	// budget for one plugin archive (targets/compose downloadTimeout), so
	// that budget is reachable, and long enough for a first image pull.
	longOpTimeout = 10 * time.Minute
	// maxSpecTimeout caps Spec.Timeout: no setup write holds a request open
	// longer than this.
	maxSpecTimeout = 15 * time.Minute
	// previewTimeout bounds one preview call (the room allows 10 s per step).
	previewTimeout = 10 * time.Second
	// maxParamsBytes caps one params object before it is decoded.
	maxParamsBytes = 64 << 10
	// maxErrorText caps runner text inside an error.
	maxErrorText = 512
)

// actionNameRE is the shape of an op and action name: snake_case, as
// tools.Tool and actions.Action require.
var actionNameRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// ScopeFunc decides, from params and the pipeline alone, whether a step
// targets only that pipeline. It must not do I/O. A refusal wraps
// tools.ErrOutOfScope; bad params wrap tools.ErrBadInput.
type ScopeFunc func(p owners.Pipeline, params json.RawMessage) error

// Spec describes one setup-only action: one typed runner op and how the room
// may check it.
type Spec struct {
	// Name is the action and op name (they are the same string, so the
	// runner's StepAction mapping is the identity).
	Name string
	// Tier is the op's floor (protocol.TierT0..TierT3 as plan.Tier*). A setup
	// action is a write, so T1 or higher in practice.
	Tier string
	// Layer is the owners layer the op belongs to ("environment", ...).
	Layer string
	// Describe explains the action for the model and the approval card.
	Describe string
	// Schema is the op's bounded JSON Schema: an object, no additional
	// properties, with a required list (possibly empty).
	Schema map[string]any
	// Scope is the no-I/O scope rule. It is required: use ScopeFree for an
	// op that names no pipeline resource, so the decision is explicit.
	Scope ScopeFunc
	// PreviewOp is the T0 op the runner runs to render what Execute would do
	// without changing anything. Empty means the action has no preview and
	// Preview answers an error. It must differ from Name: a preview never
	// sends the write op.
	PreviewOp string
	// PreviewParams maps the step's params to the preview op's params. Nil
	// sends the step's params unchanged.
	PreviewParams func(json.RawMessage) (json.RawMessage, error)
	// PreviewDecode maps the step's params and the preview op's result Data
	// to an events.PreviewResult. Nil decodes Data as an events.PreviewResult
	// itself; an op whose Data has another shape (compose_config) must set
	// it, or its preview is refused as empty.
	PreviewDecode func(params, data json.RawMessage) (events.PreviewResult, error)
	// CheckParams is an optional no-I/O check of the params beyond their
	// shape (a cap or a vocabulary the schema states but nothing validates
	// here). validateParams runs it, so Validate, Tier, CheckScope, Preview
	// and Execute all refuse such params before anything is sent; a refusal
	// is ErrBadInput.
	CheckParams func(json.RawMessage) error
	// Timeout bounds one write request of this action (OpRequest.Deadline,
	// the time at which the runner stops the op). Zero uses the tier's
	// default (writeTimeout for T1/T2, t3Timeout for T3); a T3 step gets the
	// larger of the two. It is for ops that legitimately run for minutes
	// (compose_up pulling images, plugin_install downloading an archive) and
	// is capped at maxSpecTimeout. The room's own step bound still applies
	// on top of it, so a longer Timeout takes effect only once the room
	// honours it for setup steps (a known gap of U1-7).
	Timeout time.Duration
}

// Deps are the collaborators of an Adapter.
type Deps struct {
	// Runner sends one op to the room's paired setup runner. nil makes every
	// call answer ErrNoRunner.
	Runner protocol.RunnerExecutor
	// RoomID is the room the runner is paired to. Empty is allowed: the
	// call's context may carry the room (room.RoomID) instead.
	RoomID string
	// Now is the clock for tests; nil uses time.Now.
	Now func() time.Time
}

// Adapter is one setup-only action over one runner op.
type Adapter struct {
	spec   Spec
	runner protocol.RunnerExecutor
	roomID string
	now    func() time.Time
}

var (
	_ actions.Action    = (*Adapter)(nil)
	_ actions.Scoped    = (*Adapter)(nil)
	_ actions.Preview   = (*Adapter)(nil)
	_ actions.Tiered    = (*Adapter)(nil)
	_ actions.Receipted = (*Adapter)(nil)
)

// New validates spec and returns the adapter over deps. A nil Runner is
// allowed and makes every call answer ErrNoRunner, so a room whose runner was
// lost still has its actions and refuses cleanly.
func New(spec Spec, deps Deps) (*Adapter, error) {
	if err := checkSpec(spec); err != nil {
		return nil, err
	}
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	return &Adapter{spec: spec, runner: deps.Runner, roomID: deps.RoomID, now: now}, nil
}

// checkSpec refuses a spec the room could not safely use.
func checkSpec(spec Spec) error {
	if !actionNameRE.MatchString(spec.Name) {
		return fmt.Errorf("%w: name %q is not snake_case", ErrBadSpec, clipText(spec.Name))
	}
	if !plan.ValidTier(spec.Tier) {
		return fmt.Errorf("%w: %s: tier %q is not T0..T3", ErrBadSpec, spec.Name, clipText(spec.Tier))
	}
	if strings.TrimSpace(spec.Layer) == "" || len(spec.Layer) > 64 {
		return fmt.Errorf("%w: %s: a layer is required", ErrBadSpec, spec.Name)
	}
	if strings.TrimSpace(spec.Describe) == "" || len(spec.Describe) > 500 {
		return fmt.Errorf("%w: %s: a description is required", ErrBadSpec, spec.Name)
	}
	if spec.Scope == nil {
		return fmt.Errorf("%w: %s: a scope rule is required (ScopeFree for a targetless op)", ErrBadSpec, spec.Name)
	}
	if err := checkSchema(spec.Schema); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrBadSpec, spec.Name, err)
	}
	if spec.Timeout < 0 || spec.Timeout > maxSpecTimeout {
		return fmt.Errorf("%w: %s: timeout %s is outside 0..%s", ErrBadSpec, spec.Name, spec.Timeout, maxSpecTimeout)
	}
	if spec.PreviewOp != "" {
		if !actionNameRE.MatchString(spec.PreviewOp) {
			return fmt.Errorf("%w: %s: preview op %q is not snake_case", ErrBadSpec, spec.Name, clipText(spec.PreviewOp))
		}
		if spec.PreviewOp == spec.Name {
			return fmt.Errorf("%w: %s: the preview op must not be the write op", ErrBadSpec, spec.Name)
		}
	}
	return nil
}

// checkSchema requires a closed object schema with a required list.
func checkSchema(schema map[string]any) error {
	if schema == nil {
		return errors.New("a params schema is required")
	}
	if t, _ := schema["type"].(string); t != "object" {
		return errors.New(`the params schema must have "type": "object"`)
	}
	if ap, ok := schema["additionalProperties"].(bool); !ok || ap {
		return errors.New("the params schema must set additionalProperties to false")
	}
	if _, ok := schema["required"].([]string); !ok {
		return errors.New(`the params schema must carry a "required" list`)
	}
	return nil
}

// Name is the action name the proposal step names and the runner op it sends.
func (a *Adapter) Name() string { return a.spec.Name }

// Layer is the pipeline layer whose owner approves the action.
func (a *Adapter) Layer() string { return a.spec.Layer }

// Describe explains the action for the model and the approval UI.
func (a *Adapter) Describe() string { return a.spec.Describe }

// ParamsSchema is the op's bounded JSON Schema.
func (a *Adapter) ParamsSchema() map[string]any { return cloneSchema(a.spec.Schema) }

// Validate checks the params' shape without I/O: one bounded JSON object with
// no ambiguous keys. It does not validate params against ParamsSchema (this
// repository has no JSON Schema validator), so params that are an object but
// break the schema (an unknown key, a missing required field, a wrong type)
// pass here; the runner validates them against the op's schema and its own
// rules before it executes, and refuses them there.
func (a *Adapter) Validate(_ context.Context, params json.RawMessage) error {
	return a.validateParams(params)
}

// Tier returns the step's tier: the op's floor raised to the class of the
// endpoint the room resolved from params (plan.TierOf). It does no I/O; an
// unknown class is an error, never a silent tier.
func (a *Adapter) Tier(params json.RawMessage, class string) (string, error) {
	if err := a.validateParams(params); err != nil {
		return "", err
	}
	tier, err := plan.TierOf(a.spec.Tier, class)
	if err != nil {
		return "", fmt.Errorf("setupactions: %s: %w", a.spec.Name, err)
	}
	return tier, nil
}

// CheckScope applies the spec's scope rule: a target outside the provisional
// pipeline is refused with tools.ErrOutOfScope. It does no I/O and is called
// by the room when a proposal is created and again right before Execute.
func (a *Adapter) CheckScope(p owners.Pipeline, params json.RawMessage) error {
	if err := a.validateParams(params); err != nil {
		return err
	}
	if err := a.spec.Scope(p, params); err != nil {
		return fmt.Errorf("setupactions: %s: %w", a.spec.Name, err)
	}
	return nil
}

// Execute sends the frozen proposal step's op and params to the runner and
// returns its summary. The room's pipeline must be in ctx (tools.WithPipeline,
// which a setup room's action context always carries); an out-of-scope target
// is refused before anything is sent.
func (a *Adapter) Execute(ctx context.Context, params json.RawMessage) (string, error) {
	res, err := a.sendStep(ctx, params)
	if err != nil {
		return "", err
	}
	return summaryOf(res, a.spec.Name), nil
}

// ExecuteReceipt is Execute with the runner's own receipt. The receipt is
// returned exactly as the runner sent it (events.SetupReceipt, with its OutMAC
// over the redacted result); this package never builds or alters one.
func (a *Adapter) ExecuteReceipt(ctx context.Context, params json.RawMessage) (string, events.SetupReceipt, error) {
	res, err := a.sendStep(ctx, params)
	if err != nil {
		return "", events.SetupReceipt{}, err
	}
	return summaryOf(res, a.spec.Name), res.Receipt, nil
}

// sendStep validates, scopes and binds the call to the frozen proposal step,
// then sends exactly one op.
func (a *Adapter) sendStep(ctx context.Context, params json.RawMessage) (protocol.OpResult, error) {
	if err := a.validateParams(params); err != nil {
		return protocol.OpResult{}, err
	}
	p, ok := tools.PipelineFrom(ctx)
	if !ok {
		// Fail closed: without the pipeline neither the scope nor the target
		// a step reaches can be checked, so no write runs.
		return protocol.OpResult{}, fmt.Errorf("setupactions: %s: %w", a.spec.Name, tools.ErrNoScope)
	}
	if err := a.spec.Scope(p, params); err != nil {
		return protocol.OpResult{}, fmt.Errorf("setupactions: %s: %w", a.spec.Name, err)
	}
	step, ok := room.SetupStepFrom(ctx)
	if !ok {
		return protocol.OpResult{}, fmt.Errorf("setupactions: %s: %w", a.spec.Name, ErrNoStep)
	}
	return a.sendStepRequest(ctx, step, params)
}

// sendStepRequest builds the OpRequest from step and sends it once. Every field
// the runner re-checks travels: the op is the step's action, the params are the
// step's params byte for byte, and ProposalID, StepIndex, ParamsHash, Tier and
// Class are the room's decision.
func (a *Adapter) sendStepRequest(ctx context.Context, step room.SetupStep, params json.RawMessage) (protocol.OpResult, error) {
	if step.ProposalID == "" || step.ParamsHash == "" || step.StepIndex < 0 {
		return protocol.OpResult{}, fmt.Errorf("setupactions: %s: the step has no proposal id, params hash or index: %w", a.spec.Name, ErrStepMismatch)
	}
	if step.Action != a.spec.Name {
		return protocol.OpResult{}, fmt.Errorf("setupactions: %s: the proposal step runs %q: %w", a.spec.Name, clipText(step.Action), ErrStepMismatch)
	}
	if !sameParams(step.Params, params) {
		return protocol.OpResult{}, fmt.Errorf("setupactions: %s: the params differ from the proposal step's: %w", a.spec.Name, ErrStepMismatch)
	}
	class := step.Class
	switch class {
	case protocol.ClassLocal, protocol.ClassShared, protocol.ClassProduction:
	default:
		return protocol.OpResult{}, fmt.Errorf("setupactions: %s: the step has no valid endpoint class: %w", a.spec.Name, ErrStepMismatch)
	}
	// The op's floor raised to the step's class (plan.TierOf), so a step the
	// room left without a tier still goes out at least at its class's tier
	// rather than the bare floor; a tier the room set can only raise it.
	tier, err := plan.TierOf(a.spec.Tier, class)
	if err != nil {
		return protocol.OpResult{}, fmt.Errorf("setupactions: %s: %v: %w", a.spec.Name, err, ErrStepMismatch)
	}
	if step.Tier != "" {
		if !plan.ValidTier(step.Tier) {
			return protocol.OpResult{}, fmt.Errorf("setupactions: %s: the step has tier %q: %w", a.spec.Name, clipText(step.Tier), ErrStepMismatch)
		}
		tier = raiseTier(tier, step.Tier)
	}
	idx := step.StepIndex
	req := protocol.OpRequest{
		Op:         a.spec.Name,
		Params:     append(json.RawMessage(nil), step.Params...),
		Tier:       tier,
		Class:      class,
		ProposalID: step.ProposalID,
		StepIndex:  &idx,
		ParamsHash: step.ParamsHash,
	}
	if prop := remoteclients.ProposalFrom(ctx); len(prop) > 0 {
		// The runner recomputes ParamsHash over the frozen proposal it is
		// shown with the first request of a proposal, so the bytes travel
		// unchanged. A caller must put them in ctx with
		// remoteclients.WithProposal beside the setup step. The room's
		// executor does not do that yet (it adds only WithSetupStep), so
		// today the runner refuses the first write of a proposal for want of
		// a frozen proposal; that is the executor's ask in the U1-7 PR's
		// known gaps (U1-10/U1-8b), not something this adapter can supply.
		req.Proposal = prop
	}
	return a.send(ctx, req)
}

// send gives req its id and tier deadline and executes it once.
func (a *Adapter) send(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error) {
	roomID, err := a.route(ctx)
	if err != nil {
		return protocol.OpResult{}, wrapOp(req.Op, err)
	}
	id, err := newOpID()
	if err != nil {
		return protocol.OpResult{}, wrapOp(req.Op, err)
	}
	req.ID = id
	callCtx, cancel := context.WithTimeout(ctx, a.timeoutFor(req.Tier))
	defer cancel()
	if dl, ok := callCtx.Deadline(); ok {
		req.Deadline = dl
	}
	res, err := a.runner.Execute(callCtx, roomID, req)
	if err != nil {
		if errors.Is(err, protocol.ErrNoRunner) {
			return res, wrapOp(req.Op, ErrNoRunner)
		}
		return res, wrapOp(req.Op, err)
	}
	if !res.OK {
		return res, wrapOp(req.Op, refused(res))
	}
	return res, nil
}

// route returns the room whose runner answers: the adapter's bound room, or
// the call's when none is bound. A call for another room is refused, so one
// room's reads and writes never reach another room's runner.
func (a *Adapter) route(ctx context.Context) (string, error) {
	if a == nil || a.runner == nil {
		return "", ErrNoRunner
	}
	roomID := a.roomID
	if ctxRoom := room.RoomID(ctx); roomID == "" {
		roomID = ctxRoom
	} else if ctxRoom != "" && ctxRoom != roomID {
		return "", fmt.Errorf("the adapter is bound to another room than the call's: %w", ErrNoRunner)
	}
	if roomID == "" {
		return "", ErrNoRunner
	}
	return roomID, nil
}

// validateParams checks that params is one bounded JSON object with no
// ambiguous keys. The op's own schema is enforced by the runner and described
// by ParamsSchema; this is the shape check every call makes before it sends.
func (a *Adapter) validateParams(params json.RawMessage) error {
	raw := params
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if len(raw) > maxParamsBytes {
		return fmt.Errorf("%w: %s: params are %d bytes (at most %d)", ErrBadInput, a.spec.Name, len(raw), maxParamsBytes)
	}
	if err := protocol.CheckParamKeys(raw); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrBadInput, a.spec.Name, err)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return fmt.Errorf("%w: %s: params must be a JSON object: %v", ErrBadInput, a.spec.Name, err)
	}
	if obj == nil {
		return fmt.Errorf("%w: %s: params must be a JSON object", ErrBadInput, a.spec.Name)
	}
	if a.spec.CheckParams != nil {
		if err := a.spec.CheckParams(raw); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrBadInput, a.spec.Name, err)
		}
	}
	return nil
}

// sameParams reports whether a and b encode the same JSON value, ignoring
// object key order. An undecodable side equals nothing.
func sameParams(a, b json.RawMessage) bool {
	if len(bytes.TrimSpace(a)) == 0 || len(bytes.TrimSpace(b)) == 0 {
		return false
	}
	return bytes.Equal(canonicalJSON(a), canonicalJSON(b))
}

// canonicalJSON re-encodes raw with sorted object keys and json.Number
// numbers, so two encodings of the same value compare equal byte for byte.
func canonicalJSON(raw []byte) []byte {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return raw
	}
	out, err := json.Marshal(v)
	if err != nil {
		return raw
	}
	return out
}

// raiseTier returns the higher of two tiers: a step routed above the op's
// floor keeps its higher tier.
func raiseTier(a, b string) string {
	if rankOf(b) > rankOf(a) {
		return b
	}
	return a
}

// rankOf orders tiers; an unknown tier ranks 0.
func rankOf(t string) int {
	rank, _ := plan.TierRank(t)
	return rank
}

// timeoutFor is the deadline of one request by tier: a T0 request (a
// preview) always gets previewTimeout; a write gets the spec's Timeout when
// it sets one, never less than its tier's default.
func (a *Adapter) timeoutFor(tier string) time.Duration {
	if tier == protocol.TierT0 {
		return previewTimeout
	}
	d := writeTimeout
	if tier == protocol.TierT3 {
		d = t3Timeout
	}
	if a.spec.Timeout > d {
		d = a.spec.Timeout
	}
	return d
}

// summaryOf is the action's short result: the runner's summary, capped, or a
// fixed sentence when the runner sent none.
func summaryOf(res protocol.OpResult, name string) string {
	s := strings.TrimSpace(res.Summary)
	if s == "" {
		return name + ": done"
	}
	return clipText(s)
}

// refused turns a failed result into an error: the runner's redacted summary
// plus the sentinel its error_code names.
func refused(res protocol.OpResult) error {
	text := strings.TrimSpace(res.Summary)
	if text == "" {
		text = "the runner refused the op"
	}
	text = clipText(text)
	switch errorCode(res.Data) {
	case errcode.OutOfScope:
		return fmt.Errorf("%s: %w", text, tools.ErrOutOfScope)
	case errcode.NotFound:
		return fmt.Errorf("%s: %w", text, clients.ErrNotFound)
	case errcode.NotSupported:
		return fmt.Errorf("%s: %w", text, clients.ErrNotSupported)
	}
	return errors.New(text)
}

// errorCode reads Data's error_code, or "".
func errorCode(data json.RawMessage) string {
	if len(data) == 0 {
		return ""
	}
	var env struct {
		Code string `json:"error_code"`
	}
	if json.Unmarshal(data, &env) != nil {
		return ""
	}
	return env.Code
}

// wrapOp names the op in an error.
func wrapOp(op string, err error) error {
	return fmt.Errorf("setupactions: %s: %w", op, err)
}

// newOpID returns one request id; results, refusals and the runner's step
// ledger are keyed by it.
func newOpID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("setupactions: generating an op id: %w", err)
	}
	return "sa_" + hex.EncodeToString(b[:]), nil
}

// cloneSchema copies s and its required list so a caller cannot edit the
// adapter's schema in place.
func cloneSchema(s map[string]any) map[string]any {
	if s == nil {
		return nil
	}
	out := make(map[string]any, len(s))
	for k, v := range s {
		out[k] = v
	}
	if req, ok := s["required"].([]string); ok {
		out["required"] = append([]string(nil), req...)
	}
	return out
}

// clipText cuts untrusted text for a summary or an error on a rune boundary.
func clipText(s string) string {
	s = strings.ToValidUTF8(s, "?")
	if len(s) <= maxErrorText {
		return s
	}
	cut := maxErrorText
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
