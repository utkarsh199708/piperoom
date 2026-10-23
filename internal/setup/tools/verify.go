package setuptools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"piperoom/internal/room"
	"piperoom/internal/setup/catalog"
	"piperoom/internal/tools"
)

// VerificationRequester asks the server's ladder runner to verify a setup
// room now (design §3.4). The ladder runner (internal/room, U1-8b)
// implements it, as it implements POST /api/rooms/{id}/setup/verify for a
// person; it alone writes the outcome, setup.verified.
type VerificationRequester interface {
	RequestVerification(ctx context.Context, roomID string) error
}

// ErrNoRoom reports a verification request with no room to verify.
var ErrNoRoom = errors.New("setuptools: no room to verify")

// VerifyConfig configures setup_request_verification.
type VerifyConfig struct {
	// Requester reaches the ladder runner; required.
	Requester VerificationRequester
	// RoomID binds the tool to one room; "" takes it from the call's context
	// (room.RoomID).
	RoomID string
}

// NewVerificationTool returns setup_request_verification. It is given no
// Sink on purpose: the model requests verification and never reports it, so
// the tool has no way to append an event, and the room's agent Sink refuses
// setup.verified as well.
func NewVerificationTool(cfg VerifyConfig) (tools.Tool, error) {
	if cfg.Requester == nil {
		return nil, errors.New("setuptools: setup_request_verification needs the ladder runner")
	}
	return &verifyTool{req: cfg.Requester, roomID: cfg.RoomID}, nil
}

type verifyTool struct {
	req    VerificationRequester
	roomID string
}

// noParams is the schema of a tool that takes nothing.
var noParams = json.RawMessage(`{"type":"object","additionalProperties":false}`)

func (t *verifyTool) Name() string { return VerifyToolName }

func (t *verifyTool) Description() string {
	return "Ask the server to run the verification ladder now, after the last step of the plan. " +
		"Takes no parameters. The server inserts its own canary, reads every rung itself and records the outcome " +
		"(setup.verified); you never report the outcome yourself, you wait for it."
}

func (t *verifyTool) InputSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{}, "required": []string{}}
}

func (t *verifyTool) Kind() tools.Kind { return tools.KindRoom }

// Run sends one request to the ladder runner and nothing else.
func (t *verifyTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var schema map[string]any
	_ = json.Unmarshal(noParams, &schema)
	if err := catalog.Check(schema, input); err != nil {
		return tools.Result{}, fmt.Errorf("setuptools: %s takes no parameters: %w: %w", VerifyToolName, tools.ErrBadInput, err)
	}
	roomID := t.roomID
	if roomID == "" {
		roomID = room.RoomID(ctx)
	}
	if roomID == "" {
		return tools.Result{}, fmt.Errorf("setuptools: %s: %w", VerifyToolName, ErrNoRoom)
	}
	if err := t.req.RequestVerification(ctx, roomID); err != nil {
		return tools.Result{}, fmt.Errorf("setuptools: %s: %w", VerifyToolName, err)
	}
	return tools.Result{
		Summary: "Verification requested. The server's ladder runner now inserts its canary and checks every rung; " +
			"its outcome arrives as setup.verified. Do not state the outcome before it does.",
		Data: map[string]bool{"requested": true},
	}, nil
}
