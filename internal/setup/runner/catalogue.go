package runner

import (
	"context"
	"errors"

	"piperoom/internal/setup/protocol"
)

// Catalogue is the runner's view of the operations it can execute and of the
// guard table. internal/setup/ops provides the real one in a later wave; until
// it lands the runner refuses every op (nopCatalogue). Tests inject fakes.
type Catalogue interface {
	// Execute runs one OpRequest on the runner's machine. The returned
	// OpResult carries the outcome (OK, Summary, Receipt); a non-nil error
	// means the op could not run and the runner reports OK=false.
	Execute(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error)
	// Floor returns the op's tier floor (protocol.TierT0..TierT3), and false
	// when the op is not in the catalogue (the runner refuses with
	// RefuseUnknownOp).
	Floor(op string) (tier string, ok bool)
	// Target returns the SetupTarget kind the op reaches ("postgres", "kafka",
	// "connect", "host", ...), and "" when the op reaches no registered
	// target. The runner raises OpRequest.Class to the floor it confirmed for
	// that kind before the guard runs.
	Target(req protocol.OpRequest) string
	// GuardForbidden reports whether op must not run on the given class (the
	// guard table of docs/design/setup-agent.md §3.5). Class is already raised
	// to the runner's own floor.
	GuardForbidden(op, class string) bool
	// StepAction returns the proposal-step action name that op executes, and
	// false when op is not in the catalogue. events.ProposalStep.Action is an
	// actions.Action name and OpRequest.Op an ops.Registry name; the two are
	// usually the same string, but this is the explicit mapping for the cases
	// where they differ, so the runner can bind a request to the frozen step
	// it names.
	StepAction(op string) (action string, ok bool)
}

// nopCatalogue is the default catalogue until internal/setup/ops lands: it
// knows no op, so every OpRequest is refused with RefuseUnknownOp.
type nopCatalogue struct{}

func (nopCatalogue) Execute(context.Context, protocol.OpRequest) (protocol.OpResult, error) {
	return protocol.OpResult{}, errors.New("setup: no op catalogue")
}

func (nopCatalogue) Floor(string) (string, bool)        { return "", false }
func (nopCatalogue) Target(protocol.OpRequest) string   { return "" }
func (nopCatalogue) GuardForbidden(string, string) bool { return false }
func (nopCatalogue) StepAction(string) (string, bool)   { return "", false }
