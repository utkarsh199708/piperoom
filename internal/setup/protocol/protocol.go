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
// Logging rule: a Frame is never logged whole, on either side. Pair, Token,
// Credentials and SecretValue carry credentials; a log line names the frame
// type, the op and the ids it needs, and nothing else. Secret prints its
// name only (String, LogValue), so a frame that slips into a log by value
// still shows no secret; that is a last line of defence, not permission.
//
// Versioning (U1-0): the protocol is Major.Minor (Version, Current). A runner
// and a server pair when the majors are equal and each side's minor is at
// most one behind the other's (ServerAccepts, RunnerAccepts); the session
// then speaks the lower minor (Negotiate). The rule for changing this file:
//
//   - A minor bump only adds: a new frame type, a new optional field, a new
//     op in the catalogue, a new Refuse* code. Each addition says "since
//     1.N" in its doc comment, and an absent field means exactly what it
//     meant before the field existed.
//   - Anything else is a major bump: removing or renaming a field, changing
//     a field's type or meaning, a field the receiver must understand to
//     stay safe (a new guard input, a new confinement rule), or tightening
//     what a peer may send.
//
// What each side does with a newer peer (each is a test in A5's or A9's
// PR): an unknown field is ignored (no decoder on either side uses
// DisallowUnknownFields on a Frame); an unknown frame type is dropped
// without acting on its payload (Frame.Check answers ErrUnknownFrame) and
// is never executed; an unknown op is refused (RefuseUnknownOp). The server
// never sends a frame type or an op that the negotiated minor lacks
// (Version.Has with FrameSince, or with the op's "since" minor in the
// catalogue), and the runner never sends a frame type the negotiated minor
// lacks.
//
// CONTRACT FILE — owned by the leads. Do not edit; request changes in your PR.
package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/events"
)

// The protocol version this build speaks. Bumped only by a leads PR to this
// file, under the rule in the package comment. 1.0 is the U0 shape (Pair
// without a version; Hello and Welcome with protocol_version only); 1.1
// (U1-0) adds protocol_version to Pair and protocol_minor to Pair, Welcome
// and Hello, and nothing else.
const (
	CurrentMajor = 1
	CurrentMinor = 1
)

// Version is a protocol version, Major.Minor. On the wire it is two integer
// fields, protocol_version (the major, the U0 field) and protocol_minor
// (since 1.1; absent means 0), so a 1.0 peer's frames decode unchanged.
type Version struct {
	Major int
	Minor int
}

// Current is the version this build speaks.
func Current() Version { return Version{Major: CurrentMajor, Minor: CurrentMinor} }

// String prints "1.1".
func (v Version) String() string {
	return strconv.Itoa(v.Major) + "." + strconv.Itoa(v.Minor)
}

// Has reports whether a session at v may use something added in minor since
// of the same major: a frame type (FrameSince) or an op (its catalogue
// "since" minor). A sender checks it against the negotiated version before
// it sends; the receiver's own checks (unknown frame dropped, unknown op
// refused) stay as the second line.
func (v Version) Has(since int) bool { return v.Minor >= since }

// ErrProtocolVersion is the error ServerAccepts and RunnerAccepts wrap; the
// message names both versions so the person knows which side to upgrade.
var ErrProtocolVersion = errors.New("setup: protocol version not compatible")

// valid is false for a version no build ever speaks: a major below 1 (also
// what an absent field decodes to) or a negative minor. Without it a peer
// claiming minor -1 would pass "minor >= own minor - 1" against a 1.0 side.
func (v Version) valid() bool { return v.Major >= 1 && v.Minor >= 0 }

// ServerAccepts is the server's rule: a runner pairs when its major equals
// the server's and its minor is at least the server's minor minus one. A
// runner newer than the server passes here; the runner's own rule
// (RunnerAccepts) bounds that side. The server checks it at Pair when the
// Pair carries a version (1.1 and later) and at Hello always; a refusal is
// setup.pairing {outcome: "refused"} and a close with CloseProtocolVersion.
func ServerAccepts(server, runner Version) error {
	if !runner.valid() || !server.valid() || runner.Major != server.Major || runner.Minor < server.Minor-1 {
		return fmt.Errorf("%w: server %s accepts runner %d.%d or later, runner is %s",
			ErrProtocolVersion, server, server.Major, max(server.Minor-1, 0), runner)
	}
	return nil
}

// RunnerAccepts is the runner's rule, the mirror image: a server pairs when
// its major equals the runner's and its minor is at least the runner's minor
// minus one. The runner checks it at Welcome, before it prints the room or
// asks the person for "y", and refuses with RefuseProtocolVersion.
func RunnerAccepts(runner, server Version) error {
	if !runner.valid() || !server.valid() || server.Major != runner.Major || server.Minor < runner.Minor-1 {
		return fmt.Errorf("%w: runner %s needs server %d.%d or later, server is %s",
			ErrProtocolVersion, runner, runner.Major, max(runner.Minor-1, 0), server)
	}
	return nil
}

// Negotiate is the version a paired session speaks: the shared major and
// the lower minor. Call it only after both Accepts checks passed.
func Negotiate(a, b Version) Version {
	return Version{Major: a.Major, Minor: min(a.Minor, b.Minor)}
}

// CloseProtocolVersion is the WebSocket close status (the private range of
// RFC 6455) the server closes with when ServerAccepts refuses. The close
// reason is the error text after "setup: protocol version not compatible: ",
// at most 123 bytes, e.g. "server 1.3 accepts runner 1.2 or later, runner
// is 1.0"; the runner prints it.
const CloseProtocolVersion = 4001

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

// FrameType names what a Frame carries. For a known type exactly one of the
// Frame's pointer fields is set, the one the type names (none for ping and
// pong); anything else is a protocol error and the socket is closed. A type
// this build does not know comes from a newer minor and is dropped
// (Frame.Check).
type FrameType string

// MaxLoggedTypeBytes caps how much of a peer's frame type reaches a log
// line or an error: the type of an unknown frame is peer text.
const MaxLoggedTypeBytes = 32

// Short is the type as a receiver may log it: at most MaxLoggedTypeBytes
// bytes, valid UTF-8, "…" appended when cut.
func (t FrameType) Short() string {
	s := string(t)
	if len(s) <= MaxLoggedTypeBytes {
		return strings.ToValidUTF8(s, "?")
	}
	return strings.ToValidUTF8(s[:MaxLoggedTypeBytes], "?") + "…"
}

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

// Errors of Frame.Check.
var (
	// ErrUnknownFrame: the type is not one this build knows (a newer
	// minor). The receiver drops the frame, logs its type only (Short, or
	// this error, which carries Short), and keeps the socket open; the
	// payload is never acted on. Check runs before any handler sees a frame.
	ErrUnknownFrame = errors.New("setup: unknown frame type")
	// ErrMalformedFrame: a known type whose payload field is missing, or
	// with another payload field set. The receiver closes the socket.
	ErrMalformedFrame = errors.New("setup: malformed frame")
)

// FrameSince reports the minor of CurrentMajor a frame type was added in,
// and false for a type this build does not know. A sender never sends a
// type whose minor the negotiated version lacks (Version.Has).
func FrameSince(t FrameType) (minor int, known bool) {
	switch t {
	case FramePair, FrameWelcome, FrameHello, FrameToken, FrameOp, FrameResult, FrameRefused,
		FrameResume, FrameCancel, FrameRegisterCredentials, FrameCredentials, FrameSecret,
		FramePing, FramePong:
		return 0, true
	}
	return 0, false
}

// Check classifies a decoded frame: nil when the type is known and exactly
// its own payload field is set (none for ping and pong), ErrUnknownFrame
// when the type is unknown (drop it), ErrMalformedFrame otherwise (close the
// socket). Which side may send which type is the receiver's check, not this
// one.
func (f *Frame) Check() error {
	if _, known := FrameSince(f.Type); !known {
		return fmt.Errorf("%w: %q", ErrUnknownFrame, f.Type.Short())
	}
	set := []struct {
		t     FrameType
		isSet bool
	}{
		{FramePair, f.Pair != nil}, {FrameWelcome, f.Welcome != nil}, {FrameHello, f.Hello != nil},
		{FrameToken, f.Token != nil}, {FrameOp, f.Op != nil}, {FrameResult, f.Result != nil},
		{FrameRefused, f.Refused != nil}, {FrameResume, f.Resume != nil}, {FrameCancel, f.Cancel != nil},
		{FrameRegisterCredentials, f.RegisterCredentials != nil}, {FrameCredentials, f.Credentials != nil},
		{FrameSecret, f.Secret != nil},
	}
	for _, p := range set {
		if p.isSet != (p.t == f.Type) {
			return fmt.Errorf("%w: type %q", ErrMalformedFrame, f.Type.Short())
		}
	}
	return nil
}

// Pair is the runner's first frame. Exactly one of Code and Token is set.
// Both are compared in constant time and appear in no log and no event.
//
// ProtocolVersion and ProtocolMinor (since 1.1) are the runner's Current, so
// the server can refuse an incompatible runner before it tells the runner
// anything about the room. A Pair without them is a 1.0 runner: the server
// defers the check to Hello. When a Pair carries a version, Hello must carry
// the same one, else the server closes the socket as malformed.
type Pair struct {
	Code            string `json:"code,omitempty"`
	Token           string `json:"token,omitempty"`
	ProtocolVersion int    `json:"protocol_version,omitempty"`
	ProtocolMinor   int    `json:"protocol_minor,omitempty"`
}

// Version is the runner's version as this Pair states it, and false for a
// 1.0 Pair that states none.
func (p Pair) Version() (Version, bool) {
	if p.ProtocolVersion == 0 {
		return Version{}, false
	}
	return Version{Major: p.ProtocolVersion, Minor: p.ProtocolMinor}, true
}

// Welcome is what the runner prints before it asks the person for "y/N":
// the server origin as the runner dialed it is already on the person's
// screen; this adds what the server claims. A runner started with --yes
// skips the question only when the server is loopback (standalone mode).
// ProtocolVersion and ProtocolMinor (since 1.1; absent is 0) are the
// server's Current; the runner checks RunnerAccepts before it prints
// anything else and refuses with RefuseProtocolVersion.
type Welcome struct {
	RoomID          string       `json:"room_id"`
	Title           string       `json:"title"`
	PipelineID      string       `json:"pipeline_id"`
	Tenant          string       `json:"tenant,omitempty"`
	Creator         events.Actor `json:"creator"`
	ProtocolVersion int          `json:"protocol_version"`
	ProtocolMinor   int          `json:"protocol_minor,omitempty"`
	// Targets are the targets the room knows (the SetupRequest's and every
	// setup.target_registered so far) with their classes: the runner's own
	// floor registry starts from them.
	Targets []events.SetupTarget `json:"targets,omitempty"`
}

// Version is the server's version as this Welcome states it.
func (w Welcome) Version() Version {
	return Version{Major: w.ProtocolVersion, Minor: w.ProtocolMinor}
}

// Hello describes the runner. Label is a name the person may change and is
// never the raw hostname; Tools maps a tool found on PATH ("docker",
// "docker compose", "kubectl", "helm", "aws", "confluent", "avn", "java",
// "systemctl") to its version string. The server stores it on
// setup.runner_attached. Nothing in Hello is a credential.
// ProtocolVersion and ProtocolMinor (since 1.1; absent is 0) are the
// runner's Current; the server always checks ServerAccepts on them and
// stores both on setup.runner_attached.
type Hello struct {
	OS               string            `json:"os"`
	Arch             string            `json:"arch"`
	Label            string            `json:"label"`
	Tools            map[string]string `json:"tools"`
	ProjectDir       string            `json:"project_dir"`
	CatalogueVersion string            `json:"catalogue_version"`
	ProtocolVersion  int               `json:"protocol_version"`
	ProtocolMinor    int               `json:"protocol_minor,omitempty"`
}

// Version is the runner's version as this Hello states it.
func (h Hello) Version() Version {
	return Version{Major: h.ProtocolVersion, Minor: h.ProtocolMinor}
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
// executes: the runner refuses one without ProposalID, StepIndex (nil, not
// zero: step 0 is a step) and ParamsHash (RefuseNoProposal), recomputes ParamsHash over the frozen
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
	StepIndex  *int            `json:"step_index,omitempty"`  // T1+ only; nil on a T0 request, and refused with ProposalID set
	ParamsHash string          `json:"params_hash,omitempty"` // T1+ only: the proposal's hash the runner recomputes
	Proposal   json.RawMessage `json:"proposal,omitempty"`    // T1+ only, first request of a proposal: the frozen events.ProposalCreatedPayload
}

// OpResult is the outcome of one OpRequest. Summary and Data leave the runner
// only after literal redaction (every stored secret value replaced by
// ${secret:name}) and redact.Text; Summary is at most tools.MaxSummaryBytes
// and Data at most MaxDataBytes, Truncated saying when either was cut. A
// result never carries a secret value, a token or the raw output of an op
// (Receipt.OutMAC stands for it). Redacted says that literal redaction
// replaced at least one stored value in Data, so a ${secret:NAME} there may
// stand for a value the endpoint holds in plain text: a caller that compares
// or acts on Data must not read it as the endpoint's value.
type OpResult struct {
	ID        string              `json:"id"`
	OK        bool                `json:"ok"`
	Summary   string              `json:"summary"`
	Data      json.RawMessage     `json:"data,omitempty"`
	Receipt   events.SetupReceipt `json:"receipt"`
	Truncated bool                `json:"truncated,omitempty"`
	Redacted  bool                `json:"redacted,omitempty"`
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
	RefuseNotConfirmed    = "not_confirmed"    // the person answered "N" on the runner's terminal (T3), or the server cancelled the op before it started
	RefuseProtocolVersion = "protocol_version" // RunnerAccepts refused the Welcome's version; the runner closes after it
	RefuseSecretsRead     = "secrets_read"     // a file op touched the secret store or a materialised path
	RefuseCredentials     = "credentials"      // RegisterCredentials declined on the terminal or the names are unknown
	RefuseDeadline        = "deadline"         // the op did not finish by OpRequest.Deadline
)

// OpStatus is one in-flight op as the runner reports it in Resume. ProposalID
// and StepIndex are those of the OpRequest (nil for a T0 op).
type OpStatus struct {
	ID         string    `json:"id"`
	ProposalID string    `json:"proposal_id,omitempty"`
	StepIndex  *int      `json:"step_index,omitempty"`
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
// generated during this setup (CreateReadRole and read-only API keys, §3.7),
// after POST /api/rooms/{id}/setup/credentials. RequestID is new per request
// and is what the runner's Credentials frame answers with; PipelineID is the
// room's pipeline, shown to the person, never taken from the runner. Names
// are the secret names the runner returned earlier; the runner prints the
// server origin, the pipeline id and the names and waits for "y" on its
// terminal. Nothing is sent without that confirmation (RefuseCredentials,
// carrying RequestID in Refusal.ID, otherwise). At most one request is
// pending per room; the server forgets it when answered, refused, or the
// runner is lost.
type RegisterCredentials struct {
	RequestID  string   `json:"request_id"`
	PipelineID string   `json:"pipeline_id"`
	Names      []string `json:"names"`
}

// Credentials carries secret values from the runner to the server, once,
// after the terminal confirmation. The server accepts it only while a
// RegisterCredentials with the same RequestID is pending for this runner's
// room, once per request, and refuses (closing nothing, logging the request
// id only) a frame with an unknown or already-answered RequestID or whose
// secret names are not exactly the requested set (RefuseCredentials is then
// emitted as setup.runner_refused by the server itself). The pipeline id is
// the room's, never a field of this frame, so a runner cannot name another
// pipeline's directory. The server validates every name against the Secret
// rule and the pipeline id against the owners id rule before it builds any
// path, then writes each value through an os.Root at PIPEROOM_SECRETS_DIR
// (no symlink or reparse point in <pipeline>/<name>, refused rather than
// followed) as <pipeline>/<name>, mode 0600, to a temporary file renamed
// into place, and emits setup.credentials_registered with the names only;
// the values are in no event, no log, no action result and no prompt. Values
// must never be in any other frame.
type Credentials struct {
	RequestID string   `json:"request_id"`
	Secrets   []Secret `json:"secrets"`
}

// Secret is one named value. Name matches ^[a-zA-Z_][a-zA-Z0-9_]*$ (so it is
// never a path segment with "/", "\" or ".") and is what ${secret:name}
// references; Value is at least 6 bytes (so literal redaction cannot mangle
// ordinary text) and is never written back by any op. String and LogValue
// print the name only, so a Secret in a log line or an error never shows the
// value; that is a last line of defence (see the package rule), not a licence
// to log frames.
type Secret struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// String prints the name only, never the value.
func (s Secret) String() string { return "secret:" + s.Name }

// LogValue prints the name only, never the value (log/slog).
func (s Secret) LogValue() slog.Value { return slog.StringValue(s.String()) }

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
