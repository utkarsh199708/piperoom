// Package setup replays a recorded setup runner session (U1-20): the op
// requests and results a paired runner answered during a setup run, each
// result's receipt, and the verification ladder's reads and events. The eval
// harness loads a recording instead of starting a real runner, so the six
// U1-19 setup scenarios can be exercised in replay mode.
//
// A recording is one JSON file per scenario (recordings/*.json). The JSON is
// the wire shape of internal/setup/protocol, so nothing here can drift from
// the contract: an Op loads into a protocol.OpRequest, its outcome into
// protocol.OpResult (the receipt included), and a refusal into
// protocol.Refusal. Loading is strict (unknown fields are an error), so a
// typo cannot silently drop a recorded result.
//
// The package has no runner and no network: Session implements
// protocol.RunnerExecutor and answers from the file, and FakeLadder
// (ladder.go) is the server's ladder runner for tests, issuing each rung's
// read as a request and writing setup.verified itself.
package setup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
)

// DefaultSourceKind is the source of a scenario that names none: the pipeline
// every incident scenario and the U1-19 setup scenarios are built on is
// Postgres. SourceKind and Target feed the report's per-source and
// per-target pass rates.
const DefaultSourceKind = "postgres"

// SinkNone is the warehouse kind of a setup that sets no sink up; it is
// plan.SinkNone, repeated here so a recording needs no plan import.
const SinkNone = "none"

// Source and target kinds a recording may name, as events.SetupRequest does.
var (
	sourceKinds = map[string]bool{"postgres": true, "mysql": true, "sqlserver": true, "mongodb": true, "oracle": true}
	targetKinds = map[string]bool{
		"compose": true, "host": true, "strimzi": true, "msk": true,
		"confluent": true, "aiven": true, "dbzserver": true, "embedded": true,
	}
)

// ValidSourceKind reports whether kind is a source kind a setup request
// accepts (events.SetupRequest.SourceKind).
func ValidSourceKind(kind string) bool { return sourceKinds[kind] }

// ValidTargetKind reports whether kind is a target kind a setup request
// accepts (events.SetupRequest.Target).
func ValidTargetKind(kind string) bool { return targetKinds[kind] }

// Errors of Load and Replay, matched with errors.Is.
var (
	// ErrRecording: the file is not a usable recording (bad JSON, an unknown
	// field, a missing scenario or op, an op with both or neither outcome).
	ErrRecording = errors.New("evals/setup: invalid recording")
	// ErrNoRecording: the replay has no unused op that matches the request.
	// It is what a setup run sees when the recording is short or the model
	// takes a step the session did not record; the harness reports it as a
	// miss, as it does for a tool call no recording answers.
	ErrNoRecording = errors.New("evals/setup: no recorded result for this op")
)

// Recording is one runner session's replay data.
type Recording struct {
	// Scenario is the eval scenario the session belongs to, e.g.
	// "setup/flat-plugin-dir".
	Scenario string `json:"scenario"`
	// Source is the setup request's source kind; empty means
	// DefaultSourceKind.
	Source string `json:"source,omitempty"`
	// Target is the setup request's target kind ("compose", "host", ...).
	Target string `json:"target,omitempty"`
	// Sink is the warehouse kind the setup fills; empty means SinkNone.
	Sink string `json:"sink,omitempty"`
	// RecordedAt is when the session was recorded. It is metadata only: the
	// replay never compares a clock.
	RecordedAt time.Time `json:"recorded_at,omitempty"`
	// Note says how the session was recorded (a real run, or composed from
	// the tools' own output when no stack was available), for a reader of the
	// file. It reaches no code path.
	Note string `json:"note,omitempty"`
	// Ops are the runner ops, in the order the server sent them. A replay
	// answers a request with the first unused op that matches by name and
	// params: the order breaks ties between identical ops, it does not
	// constrain the order of a caller's requests.
	Ops []Op `json:"ops"`
	// Ladder is the verification the session ended with; nil when the run
	// stopped before the ladder.
	Ladder *Ladder `json:"ladder,omitempty"`
}

// SourceKind is the recording's source kind with the default applied.
func (r Recording) SourceKind() string {
	if r.Source == "" {
		return DefaultSourceKind
	}
	return r.Source
}

// SinkKind is the recording's sink kind with the default applied.
func (r Recording) SinkKind() string {
	if r.Sink == "" {
		return SinkNone
	}
	return r.Sink
}

// Op is one recorded OpRequest and its outcome. Exactly one of Result and
// Refusal is set: Result for an op the runner ran (OK or not), Refusal for
// one it did not run (a guard, a tier, a schema check).
type Op struct {
	Op         string             `json:"op"`
	Tier       string             `json:"tier,omitempty"`
	Class      string             `json:"class,omitempty"`
	Params     json.RawMessage    `json:"params,omitempty"`
	ProposalID string             `json:"proposal_id,omitempty"`
	StepIndex  *int               `json:"step_index,omitempty"`
	ParamsHash string             `json:"params_hash,omitempty"`
	Result     *protocol.OpResult `json:"result,omitempty"`
	Refusal    *protocol.Refusal  `json:"refusal,omitempty"`
}

// Request is the OpRequest the recording states, without the per-call fields
// (ID and Deadline) the replay fills in.
func (o Op) Request() protocol.OpRequest {
	return protocol.OpRequest{
		Op:         o.Op,
		Params:     o.Params,
		Tier:       o.Tier,
		Class:      o.Class,
		ProposalID: o.ProposalID,
		StepIndex:  o.StepIndex,
		ParamsHash: o.ParamsHash,
	}
}

// Load reads one recording. It refuses an unknown field, a missing scenario
// or op, a malformed outcome and a source or target kind no setup request
// accepts, so a broken file costs no run.
func Load(path string) (Recording, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Recording{}, fmt.Errorf("evals/setup: read recording: %w", err)
	}
	return Parse(raw, path)
}

// Parse decodes a recording from raw. name is used in errors (the file path
// for Load).
func Parse(raw []byte, name string) (Recording, error) {
	var r Recording
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return Recording{}, fmt.Errorf("%w: %s: %v", ErrRecording, name, err)
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return Recording{}, fmt.Errorf("%w: %s: trailing JSON", ErrRecording, name)
	}
	if err := r.validate(); err != nil {
		return Recording{}, fmt.Errorf("%w: %s: %v", ErrRecording, name, err)
	}
	return r, nil
}

// validate checks the fields a replay depends on.
func (r *Recording) validate() error {
	if strings.TrimSpace(r.Scenario) == "" {
		return errors.New("scenario is required")
	}
	if r.Source != "" && !sourceKinds[r.Source] {
		return fmt.Errorf("source %q is not a setup source kind", r.Source)
	}
	if r.Target != "" && !targetKinds[r.Target] {
		return fmt.Errorf("target %q is not a setup target kind", r.Target)
	}
	if len(r.Ops) == 0 {
		return errors.New("ops is empty: a session records at least one op")
	}
	for i, op := range r.Ops {
		if err := op.validate(); err != nil {
			return fmt.Errorf("ops[%d]: %v", i, err)
		}
	}
	if r.Ladder != nil {
		if err := r.Ladder.validate(); err != nil {
			return fmt.Errorf("ladder: %v", err)
		}
	}
	if r.Target == "" {
		// The ladder a replay recomputes, and whether a missing one is
		// excused (PlanLadderUndefined), depend on the target: a session
		// must name it.
		return errors.New("target is required")
	}
	return nil
}

// digestRe is the shape of every digest a recording states: lowercase hex,
// 64 characters. out_mac is an HMAC-SHA256 over the result, params_hash a
// SHA-256 of the proposal, nonce_hash a SHA-256 of the canary nonce. The
// recordings were composed by hand once and carried 63- and 65-character
// values; the shape check keeps a synthetic value from standing in for a
// digest the protocol could have produced.
var digestRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// validDigest reports whether s has the shape of a protocol digest.
func validDigest(s string) bool { return digestRe.MatchString(s) }

// validate checks one op's outcome.
func (o *Op) validate() error {
	if strings.TrimSpace(o.Op) == "" {
		return errors.New("op is required")
	}
	switch o.Tier {
	case "", protocol.TierT0, protocol.TierT1, protocol.TierT2, protocol.TierT3:
	default:
		return fmt.Errorf("tier %q is not a protocol tier", o.Tier)
	}
	if o.ParamsHash != "" && !validDigest(o.ParamsHash) {
		return errors.New("params_hash is not a 64-character lowercase hex digest")
	}
	if o.Result != nil && o.Refusal != nil {
		return errors.New("an op records a result or a refusal, never both")
	}
	if o.Result == nil && o.Refusal == nil {
		return errors.New("an op records a result or a refusal")
	}
	if res := o.Result; res != nil {
		if res.ID != "" {
			return errors.New("result.id is filled by the replay, not the recording")
		}
		if strings.TrimSpace(res.Summary) == "" {
			return errors.New("result.summary is required")
		}
		if res.OK && res.Receipt.OutMAC == "" {
			return errors.New("an OK result carries a receipt.out_mac: it proves the result the runner sent")
		}
		if res.Receipt.OutMAC != "" && !validDigest(res.Receipt.OutMAC) {
			return errors.New("result.receipt.out_mac is not a 64-character lowercase hex digest")
		}
	}
	if ref := o.Refusal; ref != nil {
		if ref.ID != "" {
			return errors.New("refusal.id is filled by the replay, not the recording")
		}
		if ref.Op != "" && ref.Op != o.Op {
			return fmt.Errorf("refusal.op %q differs from op %q", ref.Op, o.Op)
		}
		if !knownReason(ref.Reason) {
			return fmt.Errorf("refusal.reason %q is not a protocol.Refuse* code", ref.Reason)
		}
	}
	return nil
}

// knownReason reports whether reason is one of the protocol's refusal codes.
func knownReason(reason string) bool {
	switch reason {
	case protocol.RefuseUnknownOp, protocol.RefuseSchema, protocol.RefuseTierBelowFloor,
		protocol.RefuseNoProposal, protocol.RefuseParamsHash, protocol.RefuseDuplicateStep,
		protocol.RefuseGuard, protocol.RefuseTarget, protocol.RefuseNotConfirmed,
		protocol.RefuseProtocolVersion, protocol.RefuseSecretsRead, protocol.RefuseCredentials,
		protocol.RefuseDeadline:
		return true
	}
	return false
}

// Ladder is the recorded verification of a session: the reads the ladder
// runner issued (tool name and structured Data, as the tools answered) and
// the setup.verified event it wrote. FakeLadder recomputes the event from the
// reads; a test compares the two, so the recording cannot claim a rung its
// own reads do not support.
type Ladder struct {
	PlanID    string    `json:"plan_id"`
	NonceHash string    `json:"nonce_hash"`
	Reads     []Read    `json:"reads,omitempty"`
	Verified  *Verified `json:"verified,omitempty"`
	// Table and ExampleSQL are rung 8's awaiting-change payload the room
	// wrote: the table the person changes and the statement the UI shows.
	Table      string `json:"table,omitempty"`
	ExampleSQL string `json:"example_sql,omitempty"`
}

// Verified is the setup.verified payload the recorded ladder runner wrote,
// without the step and receipt ids of the room it ran in (a replay has no
// room log; the rung's name, outcome and note are what is compared).
type Verified struct {
	Rungs []Rung `json:"rungs"`
}

// Rung is one row of a recorded verification.
type Rung struct {
	Name string `json:"name"`
	OK   bool   `json:"ok"`
	Note string `json:"note,omitempty"`
}

// validate checks a ladder's fields.
func (l *Ladder) validate() error {
	if strings.TrimSpace(l.PlanID) == "" {
		return errors.New("plan_id is required")
	}
	if l.NonceHash != "" && !validDigest(l.NonceHash) {
		return errors.New("nonce_hash is not a 64-character lowercase hex digest")
	}
	if len(l.Reads) == 0 && l.Verified == nil {
		return errors.New("a ladder records reads, a verified event, or both")
	}
	for i, read := range l.Reads {
		if strings.TrimSpace(read.Tool) == "" {
			return fmt.Errorf("reads[%d]: tool is required", i)
		}
		if len(read.Data) == 0 && read.Error == "" {
			return fmt.Errorf("reads[%d] (%s): data or error is required", i, read.Tool)
		}
	}
	if v := l.Verified; v != nil {
		if len(v.Rungs) == 0 {
			return errors.New("verified.rungs is empty")
		}
		seen := map[string]bool{}
		for i, rung := range v.Rungs {
			if strings.TrimSpace(rung.Name) == "" {
				return fmt.Errorf("verified.rungs[%d]: name is required", i)
			}
			if seen[rung.Name] {
				return fmt.Errorf("verified.rungs[%d]: duplicate rung %q", i, rung.Name)
			}
			seen[rung.Name] = true
		}
	}
	return nil
}

// Read is one ladder read as the tool answered: the tool name and its
// structured Data. The fake ladder's Reader gets the request; this is the
// recorded answer a recording-backed reader serves.
type Read struct {
	Tool  string          `json:"tool"`
	Input json.RawMessage `json:"input,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error string          `json:"error,omitempty"`
	At    time.Time       `json:"at,omitempty"`
}

// LoadDir reads every *.json recording in dir and indexes it by scenario.
func LoadDir(dir string) (map[string]*Recording, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, fmt.Errorf("evals/setup: list recordings: %w", err)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("evals/setup: no *.json recordings in %s", dir)
	}
	sort.Strings(paths)
	out := make(map[string]*Recording, len(paths))
	for _, p := range paths {
		r, err := Load(p)
		if err != nil {
			return nil, err
		}
		if _, dup := out[r.Scenario]; dup {
			return nil, fmt.Errorf("%w: duplicate scenario %q", ErrRecording, r.Scenario)
		}
		rec := r
		out[r.Scenario] = &rec
	}
	return out, nil
}

// Session replays one Recording as a protocol.RunnerExecutor. Every op is
// served at most once, and a request is answered by the first unused recorded
// op with the same name and exactly the same params (and a tier no lower than
// the recorded one). The recorded order breaks ties between identical ops; it
// does not constrain the order of the caller's requests. Per-run values the
// room mints for a live proposal (its ID and the proposal hash) are never
// compared, see match. A second identical request is a miss, as it would be
// for a real runner that already answered it. Session is safe for concurrent
// calls.
type Session struct {
	rec  Recording
	mu   sync.Mutex
	used []bool
	// misses are the requests no recording answered, for the run's report.
	misses []Miss
}

// Miss is one op request the recording had no unused result for.
type Miss struct {
	Op     string          `json:"op"`
	Params json.RawMessage `json:"params,omitempty"`
	Why    string          `json:"why,omitempty"`
}

// NewSession builds a replay over rec.
func NewSession(rec Recording) *Session {
	return &Session{rec: rec, used: make([]bool, len(rec.Ops))}
}

// Recording returns the session's data.
func (s *Session) Recording() Recording { return s.rec }

// Remaining is how many recorded ops no call has used yet.
func (s *Session) Remaining() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, used := range s.used {
		if !used {
			n++
		}
	}
	return n
}

// Unexecuted names the recorded ops no call used, in recording order.
func (s *Session) Unexecuted() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for i, op := range s.rec.Ops {
		if !s.used[i] {
			out = append(out, op.Op)
		}
	}
	return out
}

// Misses returns the requests the recording could not answer.
func (s *Session) Misses() []Miss {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Miss(nil), s.misses...)
}

// Execute implements protocol.RunnerExecutor: it finds the recorded op that
// answers req, marks it used and returns its result (with req.ID) or its
// refusal as a *protocol.RefusedError. With no match it returns an error
// wrapping ErrNoRecording and records a Miss.
func (s *Session) Execute(ctx context.Context, roomID string, req protocol.OpRequest) (protocol.OpResult, error) {
	if err := ctx.Err(); err != nil {
		return protocol.OpResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i, why := s.match(req)
	if i < 0 {
		s.misses = append(s.misses, Miss{Op: req.Op, Params: req.Params, Why: why})
		return protocol.OpResult{}, fmt.Errorf("%w: %s (session %s, room %s): %s", ErrNoRecording, req.Op, s.rec.Scenario, roomID, why)
	}
	s.used[i] = true
	op := s.rec.Ops[i]
	if op.Refusal != nil {
		ref := *op.Refusal
		ref.ID, ref.Op = req.ID, req.Op
		if ref.StepIndex == nil {
			ref.StepIndex = req.StepIndex
		}
		return protocol.OpResult{}, &protocol.RefusedError{Refusal: ref}
	}
	res := *op.Result
	res.ID = req.ID
	return res, nil
}

// match returns the index of the recording that answers req, or -1 and why.
// It matches by structure, never by a per-run value: the same op, exactly the
// same params, and a tier no lower than the recorded one. Proposal ID and
// params hash are ignored on purpose: the room mints a new ULID for every
// proposal and the hash covers that ID and the room (internal/room/
// params_hash.go), so a live run can never repeat the recorded values. The
// first unused op in recording order that matches answers the request.
// The caller holds s.mu.
func (s *Session) match(req protocol.OpRequest) (int, string) {
	why := ""
	for i, op := range s.rec.Ops {
		if s.used[i] || op.Op != req.Op {
			continue
		}
		switch {
		case !paramsEqual(op.Params, req.Params):
			why = "the recorded params differ from the request's"
		case tierRank(req.Tier) < tierRank(op.Tier):
			why = fmt.Sprintf("the request's tier %q is below the recorded %q", req.Tier, op.Tier)
		default:
			return i, ""
		}
	}
	if why != "" {
		return -1, why
	}
	return -1, "the session recorded no " + req.Op
}

// tierRank orders tiers T0..T3, as the runner does; an empty or unknown tier
// ranks as T0.
func tierRank(t string) int {
	switch t {
	case protocol.TierT3:
		return 3
	case protocol.TierT2:
		return 2
	case protocol.TierT1:
		return 1
	default:
		return 0
	}
}

// paramsEqual reports whether want and got decode to the same JSON value:
// every key, every array element, nothing extra. Absent params, null and {}
// are the same empty object.
func paramsEqual(want, got json.RawMessage) bool {
	w, ok := decodeParams(want)
	if !ok {
		return false
	}
	g, ok := decodeParams(got)
	if !ok {
		return false
	}
	return reflect.DeepEqual(w, g)
}

// decodeParams decodes raw op params, reading absent or null as {}.
func decodeParams(raw json.RawMessage) (any, bool) {
	var v any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, false
		}
	}
	if v == nil {
		v = map[string]any{}
	}
	return v, true
}

// paramsMatch reports whether every key of want is in got with an equal
// value. It matches a ladder read's input only (the ladder runner chooses
// its reads, not the model); a runner op is matched exactly by paramsEqual.
// An empty want matches any input.
func paramsMatch(want, got json.RawMessage) bool {
	if len(bytes.TrimSpace(want)) == 0 {
		return true
	}
	var w, g any
	if err := json.Unmarshal(want, &w); err != nil {
		return false
	}
	if len(bytes.TrimSpace(got)) > 0 {
		if err := json.Unmarshal(got, &g); err != nil {
			return false
		}
	}
	return subset(w, g)
}

// subset is paramsMatch's recursive comparison over decoded JSON.
func subset(want, got any) bool {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			if got != nil {
				return false
			}
			g = map[string]any{}
		}
		for k, v := range w {
			gv, ok := g[k]
			if !ok || !subset(v, gv) {
				return false
			}
		}
		return true
	case []any:
		g, ok := got.([]any)
		if !ok || len(w) > len(g) {
			return false
		}
		for i := range w {
			if !subset(w[i], g[i]) {
				return false
			}
		}
		return true
	default:
		return want == got
	}
}

// Check is the outcome of Verify: how many recorded ops and ladder rungs the
// replay used, and every way the recording failed to reproduce its own
// session.
type Check struct {
	Ops   int `json:"ops"`
	Rungs int `json:"rungs"`
	// NoLadder says why the session has no ladder to replay: plan.Ladder
	// defines none for its target (PlanLadderUndefined). Empty for every
	// session whose target has a ladder; such a session without one fails.
	NoLadder string   `json:"no_ladder,omitempty"`
	Failures []string `json:"failures,omitempty"`
}

// noLadderTargets are the targets whose setup has no verification ladder a
// replay can recompute: dbzserver's ladder reads server_status (U11b) and is
// not in plan.Ladder; embedded has plan.EmbeddedLadder (U6b), which judges a
// log file the person names and is not what ladderFor returns. Every other
// target must have a ladder (plan.Ladder or managed.Ladder): a new target is
// never excused by default.
var noLadderTargets = map[string]bool{"dbzserver": true, "embedded": true}

// PlanLadderUndefined says why no verification ladder is defined for rec's
// setup, or "" when one is: plan.Ladder for the Connect REST targets,
// managed.Ladder for the managed services (ladderFor, the server's choice).
// Only the targets of noLadderTargets are excused, and only when the same
// source and sink on a Connect target do have a ladder, so an unknown source
// or sink kind is never excused here. A session on such a target can only
// replay its ops; it must not claim a ladder nobody can recompute.
func PlanLadderUndefined(rec Recording) string {
	if !noLadderTargets[rec.Target] {
		return ""
	}
	if _, err := ladderFor(rec.SourceKind(), rec.Target, rec.SinkKind()); err == nil {
		return ""
	}
	if _, err := plan.Ladder(rec.SourceKind(), "compose", rec.SinkKind()); err != nil {
		return ""
	}
	return "no verification ladder is defined for target " + rec.Target + " (that target's ladder is not one a replay recomputes)"
}

// OK reports whether the recording replayed cleanly.
func (c Check) OK() bool { return len(c.Failures) == 0 }

// Verify replays rec the way a live setup run uses it and reports what did
// not reproduce.
//
// Every recorded op goes through a fresh Session as a live-shaped request: the
// same op, params, tier, class and step index, but a new request ID and, when
// the op belongs to a proposal, a proposal ID and params hash this run
// minted. Those are per-run values - the room mints a ULID per proposal and
// hashes that ID and the room - so a live run can never send the recorded
// ones; the session matches by structure. The answer must be the recorded
// outcome: a result with the same OK, summary, data and receipt, or a refusal
// with the same reason.
//
// The ladder then runs over the recorded reads through a fresh FakeLadder,
// and its rungs must equal the recorded setup.verified rung for rung. A
// recording with no ladder, no verified verdict, a read the ladder cannot
// use, or one whose recomputed verdict differs fails: a broken session or
// ladder must fail a setup scenario, never pass it silently. The one
// exception is a target plan.Ladder defines no ladder for
// (PlanLadderUndefined, the Debezium Server among them): its session records
// none, Check.NoLadder says why, and a session that records one fails.
func Verify(ctx context.Context, rec *Recording) Check {
	var c Check
	if rec == nil {
		c.Failures = append(c.Failures, "no recording")
		return c
	}
	s := NewSession(*rec)
	for i, op := range rec.Ops {
		res, err := s.Execute(ctx, "live-room", liveRequest(op, i))
		switch {
		case op.Refusal != nil:
			var refused *protocol.RefusedError
			if !errors.As(err, &refused) {
				c.Failures = append(c.Failures, fmt.Sprintf("ops[%d] %s: no recorded refusal: %v", i, op.Op, err))
				continue
			}
			if refused.Refusal.Reason != op.Refusal.Reason {
				c.Failures = append(c.Failures, fmt.Sprintf("ops[%d] %s: refused for %q, want %q", i, op.Op, refused.Refusal.Reason, op.Refusal.Reason))
				continue
			}
		default:
			if err != nil {
				c.Failures = append(c.Failures, fmt.Sprintf("ops[%d] %s: %v", i, op.Op, err))
				continue
			}
			if why := resultMismatch(*op.Result, res); why != "" {
				c.Failures = append(c.Failures, fmt.Sprintf("ops[%d] %s: %s", i, op.Op, why))
				continue
			}
		}
		c.Ops++
	}
	if left := s.Remaining(); left > 0 {
		c.Failures = append(c.Failures, fmt.Sprintf("%d recorded op(s) no live-shaped request used: %s", left, strings.Join(s.Unexecuted(), ", ")))
	}
	noLadder := PlanLadderUndefined(*rec)
	switch {
	case rec.Ladder == nil && noLadder != "":
		// Nothing to recompute: say so in the result rather than pass a
		// ladder nobody defined.
		c.NoLadder = noLadder
		return c
	case rec.Ladder == nil:
		c.Failures = append(c.Failures, "the session records no ladder")
		return c
	case noLadder != "":
		c.Failures = append(c.Failures, "the session records a ladder, but "+noLadder)
		return c
	}
	if rec.Ladder.Verified == nil {
		c.Failures = append(c.Failures, "the ladder records no setup.verified verdict")
		return c
	}
	sink := &MemorySink{}
	ladder := NewFakeLadder(LadderConfig{
		Source:     rec.SourceKind(),
		Target:     rec.Target,
		Sink:       rec.SinkKind(),
		PlanID:     rec.Ladder.PlanID,
		NonceHash:  rec.Ladder.NonceHash,
		Table:      rec.Ladder.Table,
		ExampleSQL: rec.Ladder.ExampleSQL,
	}, NewRecordingReader(rec), sink)
	got, err := ladder.Verify(ctx)
	if err != nil {
		c.Failures = append(c.Failures, "the ladder did not run: "+err.Error())
		return c
	}
	c.Rungs = len(got.Rungs)
	want := rec.Ladder.Verified.Rungs
	if len(got.Rungs) != len(want) {
		c.Failures = append(c.Failures, fmt.Sprintf("the ladder replayed %d rung(s), the recording says %d", len(got.Rungs), len(want)))
		return c
	}
	for i := range want {
		g := got.Rungs[i]
		if g.Name != want[i].Name || g.OK != want[i].OK || g.Note != want[i].Note {
			c.Failures = append(c.Failures, fmt.Sprintf("rung %d (%s) = ok %v %q, want ok %v %q",
				i, want[i].Name, g.OK, g.Note, want[i].OK, want[i].Note))
		}
	}
	return c
}

// liveRequest is the request a live run would send for o: the recorded op,
// params, tier, class and step index, with a fresh request ID and, for a
// proposal op, a fresh proposal ID and params hash. The recording's own
// per-run values are never reused; match ignores both.
func liveRequest(op Op, i int) protocol.OpRequest {
	req := op.Request()
	req.ID = fmt.Sprintf("op_live_%d", i)
	if op.ProposalID != "" {
		// A ULID-shaped id ("p_" + 26 characters) no recording can hold.
		req.ProposalID = fmt.Sprintf("p_live%022d", i)
	}
	if op.ParamsHash != "" {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s/live/%d", op.Op, i)))
		req.ParamsHash = hex.EncodeToString(sum[:])
	}
	return req
}

// resultMismatch says how got differs from the recorded want, "" when they
// agree. The request's ID is the only field the replay changes.
func resultMismatch(want, got protocol.OpResult) string {
	switch {
	case want.OK != got.OK:
		return fmt.Sprintf("ok = %v, want %v", got.OK, want.OK)
	case want.Summary != got.Summary:
		return fmt.Sprintf("summary = %q, want %q", got.Summary, want.Summary)
	case !paramsEqual(want.Data, got.Data):
		return "data differs from the recorded data"
	case want.Truncated != got.Truncated:
		return fmt.Sprintf("truncated = %v, want %v", got.Truncated, want.Truncated)
	case want.Redacted != got.Redacted:
		return fmt.Sprintf("redacted = %v, want %v", got.Redacted, want.Redacted)
	case want.Receipt.Kind != got.Receipt.Kind:
		return fmt.Sprintf("receipt.kind = %q, want %q", got.Receipt.Kind, want.Receipt.Kind)
	case want.Receipt.Target != got.Receipt.Target:
		return fmt.Sprintf("receipt.target = %q, want %q", got.Receipt.Target, want.Receipt.Target)
	case want.Receipt.ExitCode != got.Receipt.ExitCode:
		return fmt.Sprintf("receipt.exit_code = %d, want %d", got.Receipt.ExitCode, want.Receipt.ExitCode)
	case !slices.Equal(want.Receipt.Argv, got.Receipt.Argv):
		return "receipt.argv differs from the recorded argv"
	case want.Receipt.OutMAC != got.Receipt.OutMAC:
		return "receipt.out_mac differs from the recorded one"
	}
	return ""
}

// RecordingReader serves a Recording's ladder reads as a Reader: by tool
// name, each read once, in order. It is the replay a FakeLadder uses; a
// recording without ladder reads answers nothing.
type RecordingReader struct {
	rec  *Recording
	used []bool
	mu   sync.Mutex
}

// NewRecordingReader builds a reader over rec's ladder reads.
func NewRecordingReader(rec *Recording) *RecordingReader {
	n := 0
	if rec != nil && rec.Ladder != nil {
		n = len(rec.Ladder.Reads)
	}
	return &RecordingReader{rec: rec, used: make([]bool, n)}
}

// Read implements Reader.
func (r *RecordingReader) Read(ctx context.Context, req ReadRequest) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rec == nil || r.rec.Ladder == nil {
		return nil, fmt.Errorf("%w: the recording has no ladder reads", ErrNoRecording)
	}
	for i, read := range r.rec.Ladder.Reads {
		if r.used[i] || read.Tool != req.Tool {
			continue
		}
		if !paramsMatch(read.Input, req.Input) {
			continue
		}
		r.used[i] = true
		if read.Error != "" {
			return nil, errors.New(read.Error)
		}
		return read.Data, nil
	}
	return nil, fmt.Errorf("%w: the ladder read %s has no recorded answer", ErrNoRecording, req.Tool)
}
