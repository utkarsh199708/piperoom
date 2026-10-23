package remoteclients

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
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
// ParamsHash, a class and the frozen step's action and params). Nothing is
// sent.
var ErrNoProposal = errors.New("remoteclients: a write needs the proposal step in the call's context")

// ErrStepMismatch reports a write method whose op is not the frozen step's
// action, or whose arguments are not the step's params: the runner would
// refuse the request (checkStep), and sending another write under the step's
// approval is exactly what it must never do. Nothing is sent.
var ErrStepMismatch = errors.New("remoteclients: the write is not the proposal step's approved write")

// ErrStepSpent reports a second write for a proposal step this session already
// sent. The runner runs a step at most once (it records the step as started
// before it runs it, and refuses the step again as duplicate_step), so a retry
// inside one step can never run: the retry is a new proposal step. The error
// also wraps the first attempt's error when that attempt has finished.
// Nothing is sent.
var ErrStepSpent = errors.New("remoteclients: this proposal step's write was already sent to the runner; propose the step again to retry it")

// ErrBadResult reports an op result this package cannot decode: Data over the
// protocol cap, invalid JSON, or a missing field the method needs.
var ErrBadResult = errors.New("remoteclients: unusable op result")

// ErrTruncated reports a result the runner cut to fit the protocol
// (protocol.OpResult.Truncated): a cut list or a withheld payload is not the
// endpoint's answer, so it is never decoded as one. It matches ErrBadResult.
var ErrTruncated = fmt.Errorf("remoteclients: the runner cut the op result: %w", ErrBadResult)

// ErrRedacted reports a result whose Data the runner's literal redaction
// changed (protocol.OpResult.Redacted): a ${secret:NAME} there may stand for a
// value the endpoint holds in plain text, so a caller comparing it (a config
// read back after a create) or judging it (a plaintext-credential check) would
// read the placeholder as the endpoint's value. It matches ErrBadResult.
var ErrRedacted = fmt.Errorf("remoteclients: the runner redacted a stored secret value in the op result: %w", ErrBadResult)

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

	// maxSpentSteps caps the steps a session remembers as sent (ErrStepSpent).
	// The runner's own ledger is the boundary; this one only stops an
	// in-step retry before it reaches the wire, and the oldest entry goes
	// first.
	maxSpentSteps = 4096
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

	// mu guards spent and spentOrder: the proposal steps whose write this
	// session sent, with the outcome once it is known (ErrStepSpent).
	mu         sync.Mutex
	spent      map[stepKey]*spentStep
	spentOrder []stepKey
}

// stepKey names one proposal step of one room.
type stepKey struct {
	roomID     string
	proposalID string
	step       int
}

// spentStep is a sent step write; err is its outcome once the call returned.
type spentStep struct {
	done bool
	err  error
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

// do sends one T0 read. params is a typed struct (never a map from the model)
// or nil for no params. A read carries no proposal fields
// (protocol.OpRequest: nil on a T0 request).
func (s *Session) do(ctx context.Context, op, tier string, params any) (protocol.OpResult, error) {
	roomID, err := s.route(ctx, op)
	if err != nil {
		return protocol.OpResult{}, err
	}
	if tier != protocol.TierT0 {
		// A write goes through doStep, which binds it to the frozen step.
		return protocol.OpResult{}, wrapOp(op, ErrNoProposal)
	}
	raw, err := marshalParams(params)
	if err != nil {
		return protocol.OpResult{}, wrapOp(op, err)
	}
	class := ClassFrom(ctx)
	if step, ok := room.SetupStepFrom(ctx); ok && validClass(step.Class) {
		class = step.Class
	}
	if !validClass(class) {
		// A read on an unknown endpoint: the lowest class never lowers the
		// runner's own floor for the target's kind.
		class = protocol.ClassLocal
	}
	return s.send(ctx, roomID, protocol.OpRequest{Op: op, Params: raw, Tier: tier, Class: class})
}

// doStep sends the write of the proposal step in ctx (room.SetupStep): op at
// floor tier, with exactly the frozen step's params. args is the method's own
// arguments in the shape of the step's params (a struct whose fields carry the
// step's JSON names); they must be what the step approved, or the write is
// refused with ErrStepMismatch before anything is sent.
//
// The params on the wire are the frozen step's params, byte for byte, never a
// projection built from args: the runner refuses a request whose params are
// not canonically the step's (runner.checkStep), and a projection would drop
// the step's other fields (a connector the step is scoped by, a replace flag,
// a stop_first). The op is the step's action (StepAction is the identity for
// this package's writes), so a method that serves an action under another
// name (AlterSourceOffsets serves connect_reset_source_offsets) sends the
// action's op.
//
// A step's write is sent at most once per session (ErrStepSpent): the runner
// marks the step started before it runs it, so a second request for it could
// only be refused as duplicate_step. A retry of a failed or timed-out step is
// a new proposal step, never a second request under the same approval.
func (s *Session) doStep(ctx context.Context, op, tier string, args any) (protocol.OpResult, error) {
	roomID, err := s.route(ctx, op)
	if err != nil {
		return protocol.OpResult{}, err
	}
	step, ok := room.SetupStepFrom(ctx)
	if !ok || step.ProposalID == "" || step.ParamsHash == "" || !validClass(step.Class) || step.StepIndex < 0 {
		return protocol.OpResult{}, wrapOp(op, ErrNoProposal)
	}
	if step.Action != op {
		return protocol.OpResult{}, wrapOp(op, fmt.Errorf("the proposal step runs %q: %w", clipText(step.Action), ErrStepMismatch))
	}
	if err := matchStep(step.Params, args); err != nil {
		return protocol.OpResult{}, wrapOp(op, err)
	}
	idx := step.StepIndex
	req := protocol.OpRequest{
		Op:         op,
		Params:     append(json.RawMessage(nil), step.Params...),
		Tier:       raiseTier(tier, step.Tier),
		Class:      step.Class,
		ProposalID: step.ProposalID,
		StepIndex:  &idx,
		ParamsHash: step.ParamsHash,
	}
	if p := ProposalFrom(ctx); len(p) > 0 {
		req.Proposal = p
	}

	key := stepKey{roomID: roomID, proposalID: step.ProposalID, step: step.StepIndex}
	if err := s.spend(key); err != nil {
		return protocol.OpResult{}, wrapOp(op, err)
	}
	res, err := s.send(ctx, roomID, req)
	s.settle(key, err)
	return res, err
}

// route checks that the session can reach a runner for this call and returns
// the room it reaches: the bound room, or the call's when none is bound.
func (s *Session) route(ctx context.Context, op string) (string, error) {
	if s == nil || s.ex == nil {
		return "", wrapOp(op, ErrNoRunner)
	}
	roomID := s.roomID
	if ctxRoom := room.RoomID(ctx); roomID == "" {
		// A session wired without a room (the U1-7 action adapter passes one
		// in ctx instead): the call's context may carry it.
		roomID = ctxRoom
	} else if ctxRoom != "" && ctxRoom != roomID {
		// A session bound to one room used for a call in another would send
		// this room's reads (and writes) to the other room's runner.
		return "", wrapOp(op, fmt.Errorf("session is bound to another room than the call's: %w", ErrNoRunner))
	}
	if roomID == "" {
		// No room, no runner: the executor is keyed by room.
		return "", wrapOp(op, ErrNoRunner)
	}
	return roomID, nil
}

// send gives req its deadline and id, executes it and checks the result.
func (s *Session) send(ctx context.Context, roomID string, req protocol.OpRequest) (protocol.OpResult, error) {
	op := req.Op
	// The deadline follows the tier the request actually runs at: a T2 step
	// routed T3 on a production target makes the runner ask for a person's
	// "y", which a T2 deadline would cut short. A parent context with an
	// earlier deadline still wins (the room bounds a T3 step for this one,
	// room.setupT3StepTimeout).
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

// spend records key as sent, or refuses it with ErrStepSpent (and the first
// attempt's error, once that attempt has returned) when it already was.
func (s *Session) spend(key stepKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.spent[key]; ok {
		if prev.done && prev.err != nil {
			return fmt.Errorf("%w (first attempt: %w)", ErrStepSpent, prev.err)
		}
		return ErrStepSpent
	}
	if s.spent == nil {
		s.spent = map[stepKey]*spentStep{}
	}
	if len(s.spentOrder) >= maxSpentSteps {
		delete(s.spent, s.spentOrder[0])
		s.spentOrder = s.spentOrder[1:]
	}
	s.spent[key] = &spentStep{}
	s.spentOrder = append(s.spentOrder, key)
	return nil
}

// settle records the outcome of a sent step write.
func (s *Session) settle(key stepKey, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.spent[key]; ok {
		st.done, st.err = true, err
	}
}

// matchStep checks a write's arguments against the frozen step's params: the
// step's params are decoded into a value of args' type (fields the type does
// not name are the step's own and are ignored here; they still travel on the
// wire) and both are compared as canonical JSON. Numbers decode as
// json.Number, so a 64-bit LSN compares digit for digit. A step whose params
// do not decode, or differ in any field args names, is ErrStepMismatch; the
// error names no value.
func matchStep(stepParams json.RawMessage, args any) error {
	if len(bytes.TrimSpace(stepParams)) == 0 || !json.Valid(stepParams) {
		return fmt.Errorf("the proposal step has no params: %w", ErrStepMismatch)
	}
	want, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("remoteclients: encoding params: %w", err)
	}
	frozen := reflect.New(reflect.TypeOf(args))
	dec := json.NewDecoder(bytes.NewReader(stepParams))
	dec.UseNumber()
	if err := dec.Decode(frozen.Interface()); err != nil {
		return fmt.Errorf("the proposal step's params do not decode as this write's: %w", ErrStepMismatch)
	}
	got, err := json.Marshal(frozen.Elem().Interface())
	if err != nil {
		return fmt.Errorf("the proposal step's params do not encode as this write's: %w", ErrStepMismatch)
	}
	if !bytes.Equal(canonicalJSON(want), canonicalJSON(got)) {
		return fmt.Errorf("the arguments differ from the proposal step's params: %w", ErrStepMismatch)
	}
	return nil
}

// canonicalJSON re-encodes raw with sorted object keys and json.Number
// numbers, so two encodings of the same value compare equal byte for byte. Raw
// that does not decode is returned as is (it then equals nothing else).
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
//
// A result the runner cut (Truncated) or redacted (Redacted) is refused with
// ErrTruncated or ErrRedacted before any field is read: neither is the
// endpoint's answer, and a caller must not compare or act on it as one.
func field(res protocol.OpResult, name string) (json.RawMessage, error) {
	if res.Truncated {
		return nil, ErrTruncated
	}
	if res.Redacted {
		return nil, ErrRedacted
	}
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
