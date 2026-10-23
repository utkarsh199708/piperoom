// Package setupactions holds the setup-only action adapters of a setup room
// (docs/design/setup-agent.md §1, §2.2, §2.5, §3.3; board row U1-7): one
// actions.Action per T1 or T2 setup op of the catalogue (file_write,
// compose_up, plugin_install, source_create_publication, ...), each
// forwarding exactly one approved proposal step to the runner paired to the
// room. Every adapter implements:
//
//   - actions.Scoped: CheckScope refuses a step that names a table or host
//     outside the room's pipeline (the provisional pipeline before
//     registration, owners.Provisional), and a source op on a pipeline that
//     has no source database;
//   - actions.Tiered: the op's floor raised to the class of the endpoint the
//     room resolved (plan.TierOf); an unknown class is an error, never T0;
//   - actions.Preview: one T0 op_preview request to the runner, and nothing
//     else; the runner asks the op what it would change without running it;
//   - actions.Receipted: ExecuteReceipt sends the frozen step (room.SetupStep:
//     its action, params byte for byte, proposal id, step index, params hash,
//     tier and class) and returns the runner's events.SetupReceipt.
//
// The package is named setupactions so a file can import it next to
// piperoom/internal/actions.
//
// Two kinds of setup op get no adapter: a T0 op is a tool, never a proposal
// (internal/setup/tools), and a T3-floor op (source_alter_system) waits for
// the runner's confirm redesign (design §9: T3 is off on the remote runner).
// A T1 or T2 op that a production target raises to T3 is refused at Execute
// with ErrT3Disabled before anything is sent, as remoteclients does.
package setupactions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/setup/catalog"
	"piperoom/internal/setup/forward"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/remoteclients"
	"piperoom/internal/tools"
)

// PreviewOp is the runner's preview op (internal/setup/ops.OpPreview; a test
// pins the two together). The server does not import the runner's catalogue.
const PreviewOp = "op_preview"

// Deadlines when the call's context has none: the room bounds every step
// itself (30 s), and a preview (10 s).
const (
	writeTimeout   = 30 * time.Second
	previewTimeout = 10 * time.Second
)

// Errors callers branch on with errors.Is.
var (
	// ErrNoProposal: Execute was called without the proposal step the room
	// puts in the context (room.SetupStep with a proposal id, a step index, a
	// params hash and a class). Nothing is sent.
	ErrNoProposal = errors.New("setupactions: a setup write needs its proposal step in the call's context")
	// ErrStepMismatch: the step in the context runs another action, or other
	// params than the call's. Nothing is sent.
	ErrStepMismatch = errors.New("setupactions: the call is not the proposal step's approved write")
	// ErrT3Disabled: the step routes T3, which the remote runner does not run
	// until its confirm redesign lands. Nothing is sent.
	ErrT3Disabled = errors.New("setupactions: T3 ops are not enabled on the remote runner yet")
	// ErrNoReceipt: the runner reported success without a receipt (no output
	// MAC). The step may have run; it is reported failed so nobody treats an
	// unproven write as done.
	ErrNoReceipt = errors.New("setupactions: the runner returned no receipt for the step")
	// ErrBadPreview: the preview op's result has no preview.
	ErrBadPreview = errors.New("setupactions: the runner's preview result has no preview")
)

// ProposalSource returns the frozen proposal (events.ProposalCreatedPayload as
// JSON) a step belongs to, which the runner recomputes the params hash over
// on the first request of a proposal (protocol.OpRequest.Proposal). The room
// service implements it over its log.
type ProposalSource interface {
	FrozenProposal(ctx context.Context, proposalID string) (json.RawMessage, error)
}

// Config configures the adapters.
type Config struct {
	// Executor reaches the room's runner (protocol.RunnerExecutor). Nil
	// builds adapters whose Preview and Execute answer protocol.ErrNoRunner.
	Executor forward.Executor
	// RoomID binds the adapters to one room; "" takes it from each call's
	// context (room.RoomID), which the room service sets on every action call.
	RoomID string
	// Catalogue lists the ops; nil means catalog.Builtin().
	Catalogue *catalog.Catalogue
	// Proposals supplies the frozen proposal when the call's context does not
	// carry it (remoteclients.WithProposal). Nil sends the request without
	// it: a runner that has not seen the proposal then refuses it as
	// params_hash.
	Proposals ProposalSource
}

// Actions returns one adapter per T1 and T2 setup op of the catalogue.
func Actions(cfg Config) ([]actions.Action, error) {
	cat := cfg.Catalogue
	if cat == nil {
		var err error
		if cat, err = catalog.Builtin(); err != nil {
			return nil, err
		}
	}
	sender := forward.New(cfg.Executor, cfg.RoomID)
	var out []actions.Action
	for _, s := range cat.Writes() {
		if s.Tier == plan.TierT3 {
			continue // T3 is off on the remote runner (package doc)
		}
		out = append(out, &adapter{spec: s, sender: sender, proposals: cfg.Proposals})
	}
	return out, nil
}

// Register adds the adapters to reg.
func Register(reg *actions.Registry, cfg Config) error {
	as, err := Actions(cfg)
	if err != nil {
		return err
	}
	for _, a := range as {
		if err := reg.Register(a); err != nil {
			return fmt.Errorf("setupactions: %w", err)
		}
	}
	return nil
}

// adapter forwards one setup op.
type adapter struct {
	spec      catalog.Spec
	sender    *forward.Sender
	proposals ProposalSource
}

var (
	_ actions.Action    = (*adapter)(nil)
	_ actions.Scoped    = (*adapter)(nil)
	_ actions.Tiered    = (*adapter)(nil)
	_ actions.Preview   = (*adapter)(nil)
	_ actions.Receipted = (*adapter)(nil)
)

func (a *adapter) Name() string  { return a.spec.Name }
func (a *adapter) Layer() string { return a.spec.Layer }

func (a *adapter) Describe() string {
	return a.spec.Describe + ". Runs on the paired setup runner; floor " + a.spec.Tier + ", raised by the class of the target it reaches."
}

func (a *adapter) ParamsSchema() map[string]any { return a.spec.SchemaMap() }

// Validate checks params against the op's schema, without I/O; the runner
// checks them again, with its own Validate, before anything runs.
func (a *adapter) Validate(ctx context.Context, params json.RawMessage) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("setupactions: %s: %w", a.spec.Name, err)
	}
	if err := a.spec.Check(params); err != nil {
		return fmt.Errorf("setupactions: %w", err)
	}
	return nil
}

// Tier is the op's floor raised to class (plan.TierOf). It does no I/O and
// reads no params: the class is the room's, resolved from the endpoint.
func (a *adapter) Tier(_ json.RawMessage, class string) (string, error) {
	t, err := plan.TierOf(a.spec.Tier, class)
	if err != nil {
		return "", fmt.Errorf("setupactions: %s: %w", a.spec.Name, err)
	}
	return t, nil
}

// Preview sends one T0 op_preview request naming this op and params; the
// runner validates them and asks the op what it would change. It never sends
// the op itself.
func (a *adapter) Preview(ctx context.Context, params json.RawMessage) (actions.PreviewResult, error) {
	if err := a.spec.Check(params); err != nil {
		return actions.PreviewResult{}, fmt.Errorf("setupactions: preview: %w", err)
	}
	roomID, err := a.sender.Room(ctx)
	if err != nil {
		return actions.PreviewResult{}, fmt.Errorf("setupactions: preview of %s: %w", a.spec.Name, err)
	}
	inner := bytes.TrimSpace(params)
	if len(inner) == 0 {
		inner = []byte(`{}`)
	}
	raw, err := json.Marshal(struct {
		Op     string          `json:"op"`
		Params json.RawMessage `json:"params"`
	}{a.spec.Name, inner})
	if err != nil {
		return actions.PreviewResult{}, fmt.Errorf("setupactions: preview of %s: encoding: %w", a.spec.Name, err)
	}
	class := remoteclients.ClassFrom(ctx)
	if class == "" {
		class = protocol.ClassLocal // a read; the runner raises it to its own floor
	}
	res, err := a.sender.Send(ctx, roomID, protocol.OpRequest{Op: PreviewOp, Params: raw, Tier: protocol.TierT0, Class: class}, previewTimeout)
	if err != nil {
		return actions.PreviewResult{}, fmt.Errorf("setupactions: preview of %s: %w", a.spec.Name, err)
	}
	var data struct {
		Preview *events.PreviewResult `json:"preview"`
	}
	if res.Truncated || len(res.Data) == 0 || json.Unmarshal(res.Data, &data) != nil || data.Preview == nil {
		return actions.PreviewResult{}, fmt.Errorf("setupactions: preview of %s: %w", a.spec.Name, ErrBadPreview)
	}
	return *data.Preview, nil
}

// Execute is ExecuteReceipt without the receipt, for a caller that does not
// know actions.Receipted.
func (a *adapter) Execute(ctx context.Context, params json.RawMessage) (string, error) {
	out, _, err := a.ExecuteReceipt(ctx, params)
	return out, err
}

// ExecuteReceipt sends the proposal step in ctx to the runner and returns the
// runner's summary and receipt. The op is this action, the params the frozen
// step's byte for byte; the call's params must be the same step (they are: the
// room passes step.Params). The receipt is returned with an error too, so a
// failed step still records how far the runner got.
func (a *adapter) ExecuteReceipt(ctx context.Context, params json.RawMessage) (string, events.SetupReceipt, error) {
	name := a.spec.Name
	step, ok := room.SetupStepFrom(ctx)
	if !ok || step.ProposalID == "" || step.ParamsHash == "" || step.StepIndex < 0 || !plan.ValidClass(step.Class) {
		return "", events.SetupReceipt{}, fmt.Errorf("setupactions: %s: %w", name, ErrNoProposal)
	}
	if step.Action != name {
		return "", events.SetupReceipt{}, fmt.Errorf("setupactions: %s: the proposal step runs %q: %w", name, clipName(step.Action), ErrStepMismatch)
	}
	if !sameJSON(step.Params, params) {
		return "", events.SetupReceipt{}, fmt.Errorf("setupactions: %s: the params differ from the proposal step's: %w", name, ErrStepMismatch)
	}
	if err := a.spec.Check(step.Params); err != nil {
		return "", events.SetupReceipt{}, fmt.Errorf("setupactions: %w", err)
	}
	tier, err := plan.TierOf(a.spec.Tier, step.Class)
	if err != nil {
		return "", events.SetupReceipt{}, fmt.Errorf("setupactions: %s: %w", name, err)
	}
	if plan.ValidTier(step.Tier) {
		if r, _ := plan.TierRank(step.Tier); r > rank(tier) {
			tier = step.Tier // the room routed it higher; never send it lower
		}
	}
	if tier == plan.TierT3 {
		return "", events.SetupReceipt{}, fmt.Errorf("setupactions: %s: %w", name, ErrT3Disabled)
	}
	roomID, err := a.sender.Room(ctx)
	if err != nil {
		return "", events.SetupReceipt{}, fmt.Errorf("setupactions: %s: %w", name, err)
	}
	proposal := remoteclients.ProposalFrom(ctx)
	if len(proposal) == 0 && a.proposals != nil {
		if proposal, err = a.proposals.FrozenProposal(ctx, step.ProposalID); err != nil {
			return "", events.SetupReceipt{}, fmt.Errorf("setupactions: %s: reading the frozen proposal: %w", name, err)
		}
	}
	idx := step.StepIndex
	res, err := a.sender.Send(ctx, roomID, protocol.OpRequest{
		Op:         name,
		Params:     append(json.RawMessage(nil), step.Params...),
		Tier:       tier,
		Class:      step.Class,
		ProposalID: step.ProposalID,
		StepIndex:  &idx,
		ParamsHash: step.ParamsHash,
		Proposal:   append(json.RawMessage(nil), proposal...),
	}, writeTimeout)
	summary, _ := tools.Truncate(res.Summary)
	if err != nil {
		return summary, res.Receipt, fmt.Errorf("setupactions: %s: %w", name, err)
	}
	if res.Receipt.OutMAC == "" {
		return summary, res.Receipt, fmt.Errorf("setupactions: %s: %w", name, ErrNoReceipt)
	}
	return summary, res.Receipt, nil
}

// rank orders a tier plan.ValidTier accepted.
func rank(tier string) int {
	r, _ := plan.TierRank(tier)
	return r
}

// sameJSON reports whether a and b are the same JSON value (object keys in
// any order, numbers compared digit for digit). Invalid JSON equals nothing.
func sameJSON(a, b json.RawMessage) bool {
	ca, okA := canonical(a)
	cb, okB := canonical(b)
	return okA && okB && bytes.Equal(ca, cb)
}

func canonical(raw json.RawMessage) ([]byte, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		return nil, false
	}
	out, err := json.Marshal(v)
	return out, err == nil
}

// CheckScope refuses a step outside p: no pipeline at all, a source op on a
// pipeline without a source database, a table that is not one of p's source
// tables, a host that is not one of p's connection hosts. It does no I/O.
func (a *adapter) CheckScope(p owners.Pipeline, params json.RawMessage) error {
	return checkScope(a.spec, p, params)
}

// scopeErr wraps a refusal in tools.ErrOutOfScope.
func scopeErr(name, pipelineID, format string, args ...any) error {
	return fmt.Errorf("setupactions: %s: %w: %s (pipeline %q)", name, tools.ErrOutOfScope, fmt.Sprintf(format, args...), pipelineID)
}
