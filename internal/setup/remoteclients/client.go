package remoteclients

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

// ErrNoRunner reports that a client has no paired runner to execute an op:
// it was built without an Executor, or the call's context carries no room id.
// It is protocol.ErrNoRunner, so a caller that already checks the protocol's
// sentinel needs no second error to match. Every method answers it before it
// builds or sends anything.
var ErrNoRunner = protocol.ErrNoRunner

// ErrNoProposal reports a write method called without the proposal step the
// proposal path requires (room.SetupStep in ctx, with ProposalID, StepIndex,
// ParamsHash and a class). Nothing is sent.
var ErrNoProposal = errors.New("remoteclients: a write needs the proposal step in the call's context")

// ErrBadResult reports an op result this package cannot decode: Data over the
// protocol cap, invalid JSON, or a missing field the method needs.
var ErrBadResult = errors.New("remoteclients: unusable op result")

// ErrBadArgs reports a call with a missing or invalid argument (an empty
// connector name, a negative task id, an empty offset list). Nothing is sent.
var ErrBadArgs = errors.New("remoteclients: missing or invalid argument")

// Executor sends one op to the setup runner paired to a room and returns its
// result. protocol.RunnerExecutor (the room service) satisfies it; tests
// replace it with a fake that records requests and answers with JSON.
type Executor interface {
	Execute(ctx context.Context, roomID string, req protocol.OpRequest) (protocol.OpResult, error)
}

// Deadlines of one op, by tier. A read is bounded like every other read
// (10 s, AGENTS.md §4 rule 4); a T2 write gets a little longer because the
// runner may be talking to a slow Connect or database; a T3 write waits for a
// person's "y" on the runner's terminal, so it gets minutes, not seconds.
const (
	readTimeout = 10 * time.Second
	t2Timeout   = 30 * time.Second
	t3Timeout   = 5 * time.Minute

	// maxErrorText caps the runner's summary inside an error. The runner
	// already caps it at tools.MaxSummaryBytes; this is the backstop that keeps
	// a hand-made result from putting an unbounded string into a log line.
	maxErrorText = 512
)

// Option configures a Session; the zero value of each option is a no-op.
type Option func(*Session)

// WithTimeout replaces the per-tier deadline of every op (tests, a slow
// source). A value <= 0 keeps the tier defaults.
func WithTimeout(d time.Duration) Option {
	return func(s *Session) {
		if d > 0 {
			s.timeout = d
		}
	}
}

// Session is the remote view of one room's paired runner: the Executor, the
// room the clients belong to, and the deadline every client shares. A nil or
// zero Session has no runner, so every method answers ErrNoRunner.
type Session struct {
	ex      Executor
	roomID  string
	timeout time.Duration
}

// New returns the remote clients of roomID over ex. A nil ex or an empty
// roomID is allowed and makes every method answer ErrNoRunner, so a room
// without a runner can still be wired (the setup stack is built only for a
// paired runner, but a lost runner must not nil-panic a tool call).
//
// roomID is bound here rather than read from the call's context because the
// agent's read tools are scoped by pipeline id only
// (tools.WithPipelineID); only an action call carries the room
// (room.RoomID). The bound room wins over a carried one.
func New(ex Executor, roomID string, opts ...Option) *Session {
	s := &Session{ex: ex, roomID: roomID}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// Connect is the Kafka Connect client: clients.ConnectClient, the Q4a
// ConfigValidator, the Q5a ConnectLifecycleWriter, SourceOffsetReader and
// ConnectInfoReader, and the Q1c PluginReader.
func (s *Session) Connect() *Connect { return &Connect{s: s} }

// Source is the source Postgres client: clients.SourceDB, the Q2a
// SourceSettingsReader, the Q2b SourceAdminWriter and the setup-only
// clients.SetupSourceWriter.
func (s *Session) Source() *Source { return &Source{s: s} }

// Kafka is the Kafka reader: clients.KafkaAdmin plus KeyScanReader.
func (s *Session) Kafka() *Kafka { return &Kafka{s: s} }

// Registry is the Schema Registry reader: clients.SchemaRegistryReader.
func (s *Session) Registry() *Registry { return &Registry{s: s} }

// ---- call context ----

// classKey carries the class of the target a read reaches. A write takes its
// class from the proposal step instead (room.SetupStep).
type classKey struct{}

// WithClass returns ctx carrying the server's class of the target the call
// reaches (protocol.ClassLocal, ClassShared or ClassProduction). A read tool
// call is not a proposal step, so this is how the room tells a read which
// endpoint class it resolved; without it a read sends ClassLocal, the lowest
// class, which the runner raises to its own floor anyway. An empty or unknown
// class leaves ctx unchanged.
func WithClass(ctx context.Context, class string) context.Context {
	if !validClass(class) {
		return ctx
	}
	return context.WithValue(ctx, classKey{}, class)
}

// ClassFrom returns the class carried by ctx, or "".
func ClassFrom(ctx context.Context) string {
	c, _ := ctx.Value(classKey{}).(string)
	return c
}

// proposalKey carries the frozen proposal (events.ProposalCreatedPayload as
// JSON) the runner recomputes params_hash from.
type proposalKey struct{}

// WithProposal returns ctx carrying the frozen proposal the first write of a
// proposal executes. room.SetupStep has no room for it, so the adapter that
// fills the setup step adds it here; a later request of the same proposal may
// omit it (the runner caches it). Invalid or empty JSON leaves ctx unchanged.
func WithProposal(ctx context.Context, raw json.RawMessage) context.Context {
	if len(raw) == 0 || !json.Valid(raw) {
		return ctx
	}
	return context.WithValue(ctx, proposalKey{}, append(json.RawMessage(nil), raw...))
}

// ProposalFrom returns the frozen proposal carried by ctx, or nil.
func ProposalFrom(ctx context.Context) json.RawMessage {
	raw, _ := ctx.Value(proposalKey{}).(json.RawMessage)
	return raw
}

// ---- the one call path ----

// do builds one OpRequest for op and executes it. tier is the method's floor;
// params is a typed struct (never a map from the model) or nil for no params.
//
// A T0 op carries no proposal fields (protocol.OpRequest: nil on a T0
// request). A T1+ op requires the room.SetupStep in ctx and copies its
// proposal fields, class and (raised) tier; without one it returns
// ErrNoProposal before anything is sent.
func (s *Session) do(ctx context.Context, op, tier string, params any) (protocol.OpResult, error) {
	if s == nil || s.ex == nil {
		return protocol.OpResult{}, wrapOp(op, ErrNoRunner)
	}
	roomID := s.roomID
	if roomID == "" {
		// A session wired without a room (the U1-7 action adapter passes one
		// in ctx instead): the call's context may carry it.
		roomID = room.RoomID(ctx)
	}
	if roomID == "" {
		// No room, no runner: the executor is keyed by room.
		return protocol.OpResult{}, wrapOp(op, ErrNoRunner)
	}
	raw, err := marshalParams(params)
	if err != nil {
		return protocol.OpResult{}, wrapOp(op, err)
	}

	step, hasStep := room.SetupStepFrom(ctx)
	class := ClassFrom(ctx)
	if hasStep && validClass(step.Class) {
		class = step.Class
	}
	if !validClass(class) {
		// A read on an unknown endpoint: the lowest class never lowers the
		// runner's own floor for the target's kind.
		class = protocol.ClassLocal
	}

	req := protocol.OpRequest{Op: op, Params: raw, Tier: tier, Class: class}
	if tier != protocol.TierT0 {
		if !hasStep || step.ProposalID == "" || step.ParamsHash == "" || !validClass(step.Class) {
			return protocol.OpResult{}, wrapOp(op, ErrNoProposal)
		}
		idx := step.StepIndex
		req.ProposalID = step.ProposalID
		req.StepIndex = &idx
		req.ParamsHash = step.ParamsHash
		req.Tier = raiseTier(tier, step.Tier)
		if p := ProposalFrom(ctx); len(p) > 0 {
			req.Proposal = p
		}
	}

	// The deadline follows the tier the request actually runs at: a T2 step
	// routed T3 on a production target makes the runner ask for a person's
	// "y", which a T2 deadline would cut short.
	ctx, cancel := context.WithTimeout(ctx, s.timeoutFor(req.Tier))
	defer cancel()
	if dl, ok := ctx.Deadline(); ok {
		req.Deadline = dl
	}
	id, err := newOpID()
	if err != nil {
		return protocol.OpResult{}, wrapOp(op, err)
	}
	req.ID = id

	res, err := s.ex.Execute(ctx, roomID, req)
	if err != nil {
		if errors.Is(err, protocol.ErrNoRunner) {
			return res, wrapOp(op, ErrNoRunner)
		}
		return res, wrapOp(op, err)
	}
	if !res.OK {
		return res, wrapOp(op, refused(res))
	}
	if len(res.Data) > protocol.MaxDataBytes {
		return res, fmt.Errorf("remoteclients: %s: result data is %d bytes, over the %d cap: %w",
			op, len(res.Data), protocol.MaxDataBytes, ErrBadResult)
	}
	return res, nil
}

// decode unmarshals an op result's Data envelope into dst. data is the named
// field the op's result holds (see the method docs); a result without it is
// ErrBadResult, never a silent zero value.
func decode(res protocol.OpResult, data string, dst any) error {
	raw, err := field(res, data)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("remoteclients: decoding result %q: %w: %w", data, ErrBadResult, err)
	}
	return nil
}

// field returns one named raw field of a result's Data envelope.
func field(res protocol.OpResult, name string) (json.RawMessage, error) {
	if len(res.Data) == 0 {
		return nil, fmt.Errorf("remoteclients: result has no data, want %q: %w", name, ErrBadResult)
	}
	var env map[string]json.RawMessage
	if err := json.Unmarshal(res.Data, &env); err != nil {
		return nil, fmt.Errorf("remoteclients: decoding result data: %w: %w", ErrBadResult, err)
	}
	raw, ok := env[name]
	if !ok {
		return nil, fmt.Errorf("remoteclients: result data has no %q field: %w", name, ErrBadResult)
	}
	return raw, nil
}

// marshalParams encodes a typed params struct. nil is the empty object the
// catalogue validates (an op with no params has schema {}).
func marshalParams(params any) (json.RawMessage, error) {
	if params == nil {
		return json.RawMessage(`{}`), nil
	}
	b, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("remoteclients: encoding params: %w", err)
	}
	return b, nil
}

// refused turns a failed OpResult into an error: the summary (the runner
// redacted and capped it) plus the sentinel its Data's error_code names.
func refused(res protocol.OpResult) error {
	text := strings.TrimSpace(res.Summary)
	if text == "" {
		text = "the runner refused the op"
	}
	text = clipText(text)
	if sentinel := errorSentinel(res.Data); sentinel != nil {
		return fmt.Errorf("%s: %w", text, sentinel)
	}
	return errors.New(text)
}

// errorSentinel maps the result's error_code to the sentinel the Q actions
// branch on. Data without a code, or with an unknown one, maps to nothing.
func errorSentinel(data json.RawMessage) error {
	if len(data) == 0 {
		return nil
	}
	var env struct {
		Code string `json:"error_code"`
	}
	if json.Unmarshal(data, &env) != nil {
		return nil
	}
	switch env.Code {
	case ErrorCodeNotFound:
		return clients.ErrNotFound
	case ErrorCodeNotSupported:
		return clients.ErrNotSupported
	case ErrorCodeConnectorExists:
		return connect.ErrConnectorExists
	case ErrorCodeConnectorNotStopped:
		return connect.ErrConnectorNotStopped
	case ErrorCodeRebalancing:
		return connect.ErrRebalancing
	case ErrorCodeSessionChanged:
		return postgres.ErrSessionChanged
	case ErrorCodeSessionProtected:
		return postgres.ErrSessionProtected
	case ErrorCodeOutOfScope:
		return tools.ErrOutOfScope
	}
	return nil
}

// timeoutFor is the deadline of one op: the configured override, else the
// tier's default.
func (s *Session) timeoutFor(tier string) time.Duration {
	if s.timeout > 0 {
		return s.timeout
	}
	switch tier {
	case protocol.TierT3:
		return t3Timeout
	case protocol.TierT2, protocol.TierT1:
		return t2Timeout
	default:
		return readTimeout
	}
}

// newOpID returns one request id. The runner keys results, refusals and its
// duplicate-step ledger by it, so it must be unique per request; 12 random
// bytes are far beyond collision for one room's setup.
func newOpID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("remoteclients: generating an op id: %w", err)
	}
	return "rc_" + hex.EncodeToString(b[:]), nil
}

// wrapOp names the op in an error.
func wrapOp(op string, err error) error {
	return fmt.Errorf("remoteclients: %s: %w", op, err)
}

// validClass reports whether c is one of the protocol's classes.
func validClass(c string) bool {
	switch c {
	case protocol.ClassLocal, protocol.ClassShared, protocol.ClassProduction:
		return true
	}
	return false
}

// tierRank orders T0..T3; anything else ranks 0.
func tierRank(t string) int {
	switch t {
	case protocol.TierT3:
		return 3
	case protocol.TierT2:
		return 2
	case protocol.TierT1:
		return 1
	}
	return 0
}

// raiseTier returns the higher of a and b; a tier below the op's floor is
// never sent, and a step routed higher (a production target) is honoured.
func raiseTier(a, b string) string {
	if tierRank(b) > tierRank(a) {
		return b
	}
	return a
}

// clipText cuts untrusted text for an error on a rune boundary.
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
