// Package protocol is the wire contract between the Piperoom server and a
// setup runner (cmd/setup, "piperoom-setup"): the frames both sides exchange
// over the runner's one WebSocket to /api/runners/ws, the typed operation
// request and result, and the RunnerExecutor the server implements for the
// setup actions and remote clients (docs/design/setup-agent.md §2, §4 item 1).
//
// The runner executes only what the server sends as an OpRequest, against the
// catalogue it ships (internal/setup/ops); there is no generic exec and no
// shell string anywhere in this package. Every rule a doc comment states as
// "never" is a test in the runner (A9) or the room service (A5).
//
// CONTRACT FILE — owned by the leads. Do not edit; request changes in your PR.
// The design row U1-0 settles the final field list and the compatibility
// rule; until it lands, Version 1 is the only version and a different one is
// refused at Hello.
package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"piperoom/internal/events"
)

// Version is the protocol version both sides send in Hello and Welcome. A
// server refuses a Hello whose ProtocolVersion differs (RefuseProtocolVersion
// in the pairing outcome) until U1-0 defines a compatibility rule; a runner
// refuses a Welcome the same way. Bumped only by a leads PR to this file.
const Version = 1

// Limits of the transport (§2.1). The server closes a socket that sends a
// larger frame; the runner cuts an OpResult to fit (Truncated true).
const (
	// MaxFrameBytes is the largest JSON frame either side sends: 1 MiB.
	MaxFrameBytes = 1 << 20
	// MaxDataBytes caps OpResult.Data after redaction: 64 KiB.
	MaxDataBytes = 64 << 10
	// PingInterval is how often each side pings; LostAfter is how long a
	// silent runner stays attached before the server emits setup.runner_lost
	// and marks every in-flight step unknown (OpResult absent, OK nil on the
	// step, as an interrupted executor step is today).
	PingInterval = 20 * time.Second
	LostAfter    = 90 * time.Second
)

// Pairing codes (§2.1): CodeLength characters from CodeAlphabet (32 symbols,
// no 0/O or 1/I, so 40 bits), valid for CodeTTL, single use, bound to the
// (tenant, room, creator) that asked for it. The server compares a code and
// every later token with crypto/subtle.ConstantTimeCompare and never logs
// either; the audit event (setup.pairing) carries the outcome and the runner
// label only.
const (
	CodeLength   = 8
	CodeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	CodeTTL      = 10 * time.Minute
	// TokenIdle is the runner token's idle expiry; the token is rotated on
	// every reconnect, and a reconnect after TokenIdle needs a new code.
	TokenIdle = 30 * time.Minute
)

// Tiers of an operation (§3.5): the op's floor from the catalogue, raised to
// the class of the endpoint the server resolved from the params. The runner
// refuses a request whose Tier is below the op's floor.
const (
	TierT0 = "T0" // a read: a tool, never a proposal
	TierT1 = "T1" // a local write under the project directory; scheduled, cancellable
	TierT2 = "T2" // a write to a shared system; an approver of §3.6
	TierT3 = "T3" // a database setting or restart; an approver plus a maintenance window and a terminal "y" on the runner
)

// Classes of a target (§3.5). The server derives one per target from
// structured Hello and env_detect fields and the person's answers; a class is
// a floor that only rises. The runner keeps its own floors and raises
// OpRequest.Class to them before the guard runs, so a server cannot lower a
// production target to shared.
const (
	ClassLocal      = "local"
	ClassShared     = "shared"
	ClassProduction = "production"
)

// FrameType names what a Frame carries. Exactly one of the Frame's pointer
// fields is set, the one the type names; a frame with none or more than one
// is a protocol error and the socket is closed.
type FrameType string

// Frame types, runner → server (R) and server → runner (S).
const (
	// FramePair (R) is the runner's first frame: a pairing code, or a token
	// from an earlier pairing of the same room.
	FramePair FrameType = "pair"
	// FrameWelcome (S) answers a valid Pair with what the runner must show
	// the person before it sends Hello.
	FrameWelcome FrameType = "welcome"
	// FrameHello (R) is the runner's description of itself, sent only after
	// the person confirmed the Welcome on the runner's terminal.
	FrameHello FrameType = "hello"
	// FrameToken (S) hands the runner its token after Hello (and a fresh one
	// after every reconnect).
	FrameToken FrameType = "token"
	// FrameOp (S) asks the runner to execute one operation.
	FrameOp FrameType = "op"
	// FrameResult (R) is the outcome of one FrameOp.
	FrameResult FrameType = "result"
	// FrameRefused (R) says the runner did not execute a FrameOp and why; the
	// server emits setup.runner_refused from it.
	FrameRefused FrameType = "refused"
	// FrameResume (R) follows a reconnect with a token: what the runner was
	// doing when the socket dropped.
	FrameResume FrameType = "resume"
	// FrameCancel (S) asks the runner to stop one in-flight op.
	FrameCancel FrameType = "cancel"
	// FrameRegisterCredentials (S) asks the runner to send the read
	// credentials it generated for the pipeline (§3.7). The runner prints the
	// server origin and the secret names and waits for "y" on its terminal.
	FrameRegisterCredentials FrameType = "register_credentials"
	// FrameCredentials (R) carries those secret names and values, after the
	// terminal confirmation, straight to PIPEROOM_SECRETS_DIR on the server.
	FrameCredentials FrameType = "credentials"
	// FrameSecret (S) carries one secret value the driver typed in the UI
	// (POST /api/rooms/{id}/setup/secret) to the runner's secret store.
	FrameSecret FrameType = "secret"
	// FramePing and FramePong keep the socket alive (PingInterval).
	FramePing FrameType = "ping"
	FramePong FrameType = "pong"
)

// Frame is one JSON message on the runner socket.
type Frame struct {
	Type                FrameType            `json:"type"`
	Pair                *Pair                `json:"pair,omitempty"`
	Welcome             *Welcome             `json:"welcome,omitempty"`
	Hello               *Hello               `json:"hello,omitempty"`
	Token               *Token               `json:"token,omitempty"`
	Op                  *OpRequest           `json:"op,omitempty"`
	Result              *OpResult            `json:"result,omitempty"`
	Refused             *Refusal             `json:"refused,omitempty"`
	Resume              *Resume              `json:"resume,omitempty"`
	Cancel              *Cancel              `json:"cancel,omitempty"`
	RegisterCredentials *RegisterCredentials `json:"register_credentials,omitempty"`
	Credentials         *Credentials         `json:"credentials,omitempty"`
	Secret              *SecretValue         `json:"secret,omitempty"`
}

// Pair is the runner's first frame. Exactly one of Code and Token is set.
// Both are compared in constant time and appear in no log and no event.
type Pair struct {
	Code  string `json:"code,omitempty"`
	Token string `json:"token,omitempty"`
}

// Welcome is what the runner prints before it asks the person for "y/N":
// the server origin as the runner dialed it is already on the person's
// screen; this adds what the server claims. A runner started with --yes
// skips the question only when the server is loopback (standalone mode).
type Welcome struct {
	RoomID          string       `json:"room_id"`
	Title           string       `json:"title"`
	PipelineID      string       `json:"pipeline_id"`
	Tenant          string       `json:"tenant,omitempty"`
	Creator         events.Actor `json:"creator"`
	ProtocolVersion int          `json:"protocol_version"`
	// Targets are the targets the room knows (the SetupRequest's and every
	// setup.target_registered so far) with their classes: the runner's own
	// floor registry starts from them.
	Targets []events.SetupTarget `json:"targets,omitempty"`
}

// Hello describes the runner. Label is a name the person may change and is
// never the raw hostname; Tools maps a tool found on PATH ("docker",
// "docker compose", "kubectl", "helm", "aws", "confluent", "avn", "java",
// "systemctl") to its version string. The server stores it on
// setup.runner_attached. Nothing in Hello is a credential.
type Hello struct {
	OS               string            `json:"os"`
	Arch             string            `json:"arch"`
	Label            string            `json:"label"`
	Tools            map[string]string `json:"tools"`
	ProjectDir       string            `json:"project_dir"`
	CatalogueVersion string            `json:"catalogue_version"`
	ProtocolVersion  int               `json:"protocol_version"`
}

// Token is the runner's session token: 32 random bytes, in memory on both
// sides, bound to (tenant, room, creator, runner label), idle expiry
// TokenIdle, rotated on every reconnect. Never logged, never in an event.
type Token struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

// OpRequest is what the server sends; the runner executes exactly this and
// nothing else. Params is validated against the op's JSON Schema on both
// sides. For a T1 or higher op the request names the proposal step it
// executes: the runner refuses one without ProposalID, StepIndex and
// ParamsHash (RefuseNoProposal), recomputes ParamsHash over the frozen
// Proposal it received with the first request of that proposal (RFC 8785,
// docs/design/auth.md §3.1) and refuses a mismatch (RefuseParamsHash), and
// refuses a (ProposalID, StepIndex) it already started, from its op log that
// survives a restart (RefuseDuplicateStep). Class is the class of the resolved
// endpoint as the server decided it; the runner raises it to its own floor
// and checks the guard table again before anything runs.
type OpRequest struct {
	ID         string          `json:"id"`                    // unique per request; results, refusals and receipts carry it back
	Op         string          `json:"op"`                    // an ops.Registry name, e.g. "compose_up"
	Params     json.RawMessage `json:"params"`                // validated against the op's schema on both sides
	Tier       string          `json:"tier"`                  // TierT0..TierT3 as the server decided; never below the op's floor
	Class      string          `json:"class"`                 // ClassLocal | ClassShared | ClassProduction of the resolved target
	Deadline   time.Time       `json:"deadline"`              // the runner stops the op at this time and reports it
	ProposalID string          `json:"proposal_id,omitempty"` // T1+ only
	StepIndex  int             `json:"step_index"`            // T1+ only
	ParamsHash string          `json:"params_hash,omitempty"` // T1+ only: the proposal's hash the runner recomputes
	Proposal   json.RawMessage `json:"proposal,omitempty"`    // T1+ only, first request of a proposal: the frozen events.ProposalCreatedPayload
}

// OpResult is the outcome of one OpRequest. Summary and Data leave the runner
// only after literal redaction (every stored secret value replaced by
// ${secret:name}) and redact.Text; Summary is at most tools.MaxSummaryBytes
// and Data at most MaxDataBytes, Truncated saying when either was cut. A
// result never carries a secret value, a token or the raw output of an op
// (Receipt.OutMAC stands for it).
type OpResult struct {
	ID        string              `json:"id"`
	OK        bool                `json:"ok"`
	Summary   string              `json:"summary"`
	Data      json.RawMessage     `json:"data,omitempty"`
	Receipt   events.SetupReceipt `json:"receipt"`
	Truncated bool                `json:"truncated,omitempty"`
}

// Refusal says the runner did not run an OpRequest. Reason is one of the
// Refuse* codes, never free text; the server emits setup.runner_refused
// with the same fields and answers the executor with a *RefusedError. The
// runner also appends it to its local op log.
type Refusal struct {
	ID         string `json:"id"`
	Op         string `json:"op"`
	Reason     string `json:"reason"`
	ProposalID string `json:"proposal_id,omitempty"`
	StepIndex  *int   `json:"step_index,omitempty"`
}

// Refusal reasons (Refusal.Reason, setup.runner_refused.reason).
const (
	RefuseUnknownOp       = "unknown_op"       // not in the runner's catalogue
	RefuseSchema          = "schema"           // Params fail the op's schema or Validate
	RefuseTierBelowFloor  = "tier_below_floor" // Tier below the op's floor
	RefuseNoProposal      = "no_proposal"      // T1+ without ProposalID, StepIndex and ParamsHash
	RefuseParamsHash      = "params_hash"      // recomputed hash differs
	RefuseDuplicateStep   = "duplicate_step"   // (ProposalID, StepIndex) already started
	RefuseGuard           = "guard"            // the guard table forbids the op on this class
	RefuseTarget          = "target"           // the op would reach a host outside the registered targets, or a denylisted address
	RefuseNotConfirmed    = "not_confirmed"    // the person answered "N" on the runner's terminal (T3)
	RefuseProtocolVersion = "protocol_version" // Hello or Welcome carried another Version
	RefuseSecretsRead     = "secrets_read"     // a file op touched the secret store or a materialised path
	RefuseCredentials     = "credentials"      // RegisterCredentials declined on the terminal or the names are unknown
	RefuseDeadline        = "deadline"         // the op did not finish by OpRequest.Deadline
)

// OpStatus is one in-flight op as the runner reports it in Resume.
type OpStatus struct {
	ID         string    `json:"id"`
	ProposalID string    `json:"proposal_id,omitempty"`
	StepIndex  int       `json:"step_index"`
	State      string    `json:"state"`            // StateRunning | StateDone
	Result     *OpResult `json:"result,omitempty"` // set when State is StateDone
}

// States of OpStatus.State.
const (
	StateRunning = "running"
	StateDone    = "done"
)

// Resume is the runner's first frame after a reconnect with a token. The
// server reconciles: an op the runner reports running or done is never
// re-sent, a done result is taken as the step's result, and an op the server
// sent that the runner never saw is re-sent once. This keeps the exactly-once
// promise of docs/design/replicas.md §5 with the runner on the path.
type Resume struct {
	InFlight []OpStatus `json:"in_flight"`
}

// Cancel asks the runner to stop the op with ID. The runner answers with an
// OpResult (OK false, Receipt filled as far as it got) or, when the op had
// already finished, with its real result.
type Cancel struct {
	ID string `json:"id"`
}

// RegisterCredentials asks the runner to send the read credentials it
// generated during this setup (CreateReadRole and read-only API keys, §3.7).
// Names are the secret names the runner returned earlier; the runner prints
// the server origin and the names and waits for "y" on its terminal. Nothing
// is sent without that confirmation (RefuseCredentials otherwise).
type RegisterCredentials struct {
	PipelineID string   `json:"pipeline_id"`
	Names      []string `json:"names"`
}

// Credentials carries secret values from the runner to the server, once,
// after the terminal confirmation. The server writes each value to
// PIPEROOM_SECRETS_DIR/<pipeline>/<name> (0600) and emits
// setup.credentials_registered with the names only; the values are in no
// event, no log, no action result and no prompt. Values must never be in any
// other frame.
type Credentials struct {
	PipelineID string   `json:"pipeline_id"`
	Secrets    []Secret `json:"secrets"`
}

// Secret is one named value. Name matches ^[a-zA-Z_][a-zA-Z0-9_]*$ and is
// what ${secret:name} references; Value is at least 6 bytes (so literal
// redaction cannot mangle ordinary text) and is never written back by any op.
type Secret struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// SecretValue is one secret the driver typed in the UI, forwarded by the
// server in this one frame and stored nowhere on the server; the server emits
// setup.secret_set with the name only. The runner stores it in its secret
// store (PIPEROOM_SETUP_SECRETS_FILE) and refuses a value under 6 bytes.
type SecretValue struct {
	Secret
}

// RunnerExecutor is how a setup action or a remote client reaches the paired
// runner. The room service implements it over the runner registry; wiring
// hands it to the setup stack of a room. Execute sends one OpRequest and
// waits for its OpResult until ctx or req.Deadline ends. It returns
// ErrNoRunner when no runner is paired to roomID, a *RefusedError when the
// runner refused (ErrParamsHash and ErrDuplicateStep are the two refusals an
// executor retries never; both match errors.Is on the returned error), and
// ctx's error when the runner was lost first (the step's outcome is then
// unknown, as an interrupted executor step is today).
type RunnerExecutor interface {
	Execute(ctx context.Context, roomID string, req OpRequest) (OpResult, error)
}

// Sentinel errors of RunnerExecutor.Execute.
var (
	ErrNoRunner      = errors.New("setup: no runner paired to this room")
	ErrParamsHash    = errors.New("setup: params_hash mismatch")
	ErrDuplicateStep = errors.New("setup: step already executed on this runner")
)

// RefusedError is the error Execute returns for a Refusal. Is matches
// ErrParamsHash for RefuseParamsHash and ErrDuplicateStep for
// RefuseDuplicateStep, so callers branch with errors.Is, never on text.
type RefusedError struct {
	Refusal Refusal
}

// Error names the op and the reason code only.
func (e *RefusedError) Error() string {
	return "setup: runner refused " + e.Refusal.Op + ": " + e.Refusal.Reason
}

// Is maps the two retry-relevant reasons to their sentinels.
func (e *RefusedError) Is(target error) bool {
	switch target {
	case ErrParamsHash:
		return e.Refusal.Reason == RefuseParamsHash
	case ErrDuplicateStep:
		return e.Refusal.Reason == RefuseDuplicateStep
	}
	return false
}
