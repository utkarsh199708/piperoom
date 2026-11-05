package setuptools

import (
	"context"
	"encoding/json"
	"fmt"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// TypeVerificationRequested is the room event setup_request_verification
// emits: a request for the server's ladder runner to verify the pipeline, and
// nothing more. The ladder runner (internal/room, U1-8b) is the only writer of
// setup.verified, so no model output can claim a pipeline verified
// (docs/design/setup-agent.md §3.4).
//
// It is not yet part of internal/events' type list: the contract change
// request in the U1-7 PR proposes adding
//
//	SetupVerificationRequested Type = "setup.verification_requested"
//
// beside SetupVerified, with an empty payload (see VerificationRequest), and
// amending the events.go block comment that lists the setup types the agent
// may emit ("except setup.plan and setup.blocked") to name it as well: the
// agent may emit exactly setup.plan and setup.verification_requested, and must
// never emit setup.verified. Until a lead decides, this local constant is the
// adapter the change request names. The room owns the rest of the CCR: its
// sink refuses the request while a ladder run is in flight (each run creates
// a T2 canary-insert proposal), and the human route
// POST /api/rooms/{id}/setup/verify emits the same event with the person as
// actor, so the ladder has one trigger and the log says who asked.
const TypeVerificationRequested events.Type = "setup.verification_requested"

// VerificationRequest is the payload TypeVerificationRequested carries. It is
// empty on purpose: the store stamps the event's time (Event.At), so a second
// timestamp here could only disagree with it, and the plan a request verifies
// is the room's current setup.plan when the sink accepts it, never a plan id
// the model chose. The room's sink stamps that plan id, so a setup.plan
// landing between the call and the ladder's start cannot make the request
// ambiguous.
type VerificationRequest struct{}

// requestVerificationTool is setup_request_verification (KindRoom, no
// parameters): the model asks for the ladder; it never reports the outcome.
type requestVerificationTool struct {
	sink Emitter
}

// Name is the tool name the model calls.
func (t *requestVerificationTool) Name() string { return "setup_request_verification" }

// Description tells the model to request verification and then wait, never to
// report a result.
func (t *requestVerificationTool) Description() string {
	return "Ask the server to run the pipeline's verification ladder now (connector, tasks, topic, snapshot, canary event, " +
		"schema subject, sink row, and a change the person makes). It takes no parameters and returns a request; " +
		"the server's ladder runner runs the checks and records the outcome itself. Never report a verification yourself."
}

// InputSchema is the empty object: the tool takes no parameters.
func (t *requestVerificationTool) InputSchema() map[string]any {
	return objectSchema(nil)
}

// Kind is KindRoom: the tool writes only to the room log, never to a system.
func (t *requestVerificationTool) Kind() tools.Kind { return tools.KindRoom }

// Run emits exactly one verification request and returns its acknowledgement.
// It cannot emit setup.verified, whatever the sink accepts.
func (t *requestVerificationTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	if len(input) > 0 {
		var in struct{}
		if err := decodeInput(input, &in); err != nil {
			return tools.Result{}, fmt.Errorf("%w: setup_request_verification: %v", tools.ErrBadInput, err)
		}
	}
	if err := t.sink.Emit(ctx, TypeVerificationRequested, VerificationRequest{}); err != nil {
		return tools.Result{}, fmt.Errorf("setuptools: requesting verification: %w", err)
	}
	return tools.Result{
		Summary: "verification requested. The server's ladder runner will run the checks and record the outcome " +
			"(setup.verified); wait for it and do not claim it. Rung 8 needs a change a person makes first.",
	}, nil
}
