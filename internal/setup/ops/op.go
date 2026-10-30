// Package ops is the setup runner's operation catalogue
// (docs/design/setup-agent.md §2.5): one Go value per typed operation, each
// with its tier floor, the target kind it reaches, its JSON Schema, a
// no-I/O Validate and an Execute. There is no generic exec and no shell
// string anywhere in this package: an op that runs a program builds an argv
// slice (file: exec.go), and every file op opens through an os.Root rooted at
// the project directory (§2.3, file: paths.go).
//
// The registry value implements the runner's Catalogue interface
// (internal/setup/runner/catalogue.go) structurally, so the runner can use it
// without importing this package: Execute, Floor, Target, GuardForbidden and
// StepAction, plus the optional FloorFor (a floor raised by the params, for an
// op that implements ParamTierer) and CheckTarget. Config.NetProbe is the
// seam Q6a's net_probe plugs into from the runner's position (net_probe.go).
//
// Invariants, each a test:
//   - every file path is checked lexically on every OS and then walked with
//     Root.Lstat, refusing any symlink, junction or reparse point;
//   - an op that reaches a host reaches only a host in Env.Targets, and every
//     dial passes a Control hook that refuses link-local and metadata
//     addresses against the address being dialed;
//   - an unknown op is an error, never a silent success;
//   - summaries stay at tools.MaxSummaryBytes and Data at
//     protocol.MaxDataBytes.
package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/redact"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

// CatalogueVersion names this catalogue build in Hello.CatalogueVersion.
const CatalogueVersion = "u1-2"

// defaultOpTimeout bounds one op; a nearer request deadline always wins
// (design §2.2, rule 4: 10 s for tools). An op that implements Deadliner
// may take longer, up to MaxOpTimeout.
const defaultOpTimeout = 10 * time.Second

// MaxOpTimeout caps a Deadliner's own deadline: no op holds the runner for
// longer, whatever it asks for.
const MaxOpTimeout = 20 * time.Minute

// Deadliner is implemented by an op that legitimately runs longer than the
// default (msk_wait polling a connector MSK provisions for 5 to 15
// minutes). Deadline is its own bound; Registry.Execute uses it clamped to
// [default, MaxOpTimeout], so it can raise the bound but never lower it,
// and the request's deadline still wins when it is nearer.
type Deadliner interface {
	Deadline() time.Duration
}

// opTimeout is the bound of one run of op before the request's deadline
// is applied.
func opTimeout(op Op) time.Duration {
	d := defaultOpTimeout
	if dl, ok := op.(Deadliner); ok {
		if own := dl.Deadline(); own > d {
			d = min(own, MaxOpTimeout)
		}
	}
	return d
}

// maxLoggedNameBytes caps an op name in an error or summary; the name is
// peer-supplied text.
const maxLoggedNameBytes = 64

// Errors callers branch on with errors.Is.
var (
	// ErrUnknownOp: the op is not in the registry.
	ErrUnknownOp = errors.New("ops: unknown op")
	// ErrParams: Params do not decode, carry an unknown field or fail Validate.
	ErrParams = errors.New("ops: invalid params")
	// ErrPath: a path was refused by the §2.3 rule.
	ErrPath = errors.New("ops: path refused")
	// ErrNotRegular: the path names something that is not a regular file.
	ErrNotRegular = errors.New("ops: not a regular file")
	// ErrNotText: content or a read target is not valid UTF-8.
	ErrNotText = errors.New("ops: not UTF-8 text")
	// ErrTooLarge: content or a value exceeds its cap.
	ErrTooLarge = errors.New("ops: too large")
	// ErrExecutedName: file_write names a file something executes or loads.
	ErrExecutedName = errors.New("ops: file is executed or loaded")
	// ErrTarget: the host or target label is not in Env.Targets.
	ErrTarget = errors.New("ops: target not in the room's targets")
	// ErrHTTPStatus: http_get got a non-2xx status.
	ErrHTTPStatus = errors.New("ops: http status")
	// ErrExec: java_version could not read a version.
	ErrExec = errors.New("ops: command failed")
	// ErrProbeFailed: a net_probe check failed; the checks in Data say which.
	ErrProbeFailed = errors.New("ops: probe check failed")
	// ErrSecretPath: the path is the runner's secret store or a materialised
	// secret file (design §2.4: no op can read a secret back).
	ErrSecretPath = errors.New("ops: secret store path")
)

// Op is one typed operation the runner can execute.
type Op interface {
	// Name is snake_case and unique in a Registry.
	Name() string
	// Target is the SetupTarget kind the op reaches ("connect", "postgres",
	// "host", ...), or "" when it reaches no registered target. An op that
	// takes the target from its params implements ParamTargeter instead.
	Target() string
	// Tier is the op's floor: protocol.TierT0..TierT3.
	Tier() string
	// Layer is the owners layer the op belongs to ("environment", ...).
	Layer() string
	// Describe is one line for the plan and the preview.
	Describe() string
	// ParamsSchema is the op's JSON Schema; both sides validate against it.
	ParamsSchema() map[string]any
	// Validate checks params without I/O: required fields, bounds, the path
	// rule and the artifact name rules. Execute re-runs it before acting.
	Validate(params json.RawMessage, env Env) error
	// Execute runs the op. It is never called with a params set Validate
	// rejected, but it checks again because the runner's contract promises it.
	Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error)
}

// ParamTargeter is implemented by an op whose target kind comes from its
// params (http_get, net_probe, sink_probe): the runner raises the request's class to its
// floor for that kind before the guard runs.
type ParamTargeter interface {
	ParamTarget(params json.RawMessage, env Env) string
}

// ParamTierer is implemented by an op whose tier for one step depends on its
// params (host properties_write: T1 for a worker file at the project root, T2
// for one outside the project directory). TierFor must do no I/O and must not
// consult the runner's OS, so the server can compute the same tier when it
// builds the step. It can only raise the op's floor (Registry.FloorFor takes
// the higher of the two); params it cannot classify are an error, never a
// silent floor.
type ParamTierer interface {
	TierFor(params json.RawMessage) (string, error)
}

// Result is what an Op.Execute returns before it becomes a protocol.OpResult.
type Result struct {
	Summary string
	Data    any
	Receipt events.SetupReceipt
	// Truncated says Summary or Data was cut to fit a cap.
	Truncated bool
}

// Config constructs a Registry. ProjectDir is required; every other field has
// a safe default.
type Config struct {
	ProjectDir string
	// Targets are the hosts the room may reach (Welcome.Targets and every
	// setup.target_registered); http_get, net_probe and sink_probe refuse anything else.
	Targets []events.SetupTarget
	// MountDirs are directories a rendered template mounts into a container
	// or builds from; file_write refuses every path under one (§2.5).
	MountDirs []string
	// Materialised lists project-relative paths whose content came from the
	// runner's secret store (the store also persists them in
	// .piperoom/materialised, which every file op reads); file_read,
	// dir_list, file_write and file_delete refuse them (§2.4).
	Materialised []string
	// Now, LookPath and Exec are injectable for tests.
	Now      func() time.Time
	LookPath func(string) (string, error)
	Exec     func(ctx context.Context, name string, args ...string) ([]byte, int, error)
	// Resolver overrides the system resolver for the guarded dialer (tests).
	Resolver *net.Resolver
	// HTTPClient overrides the guarded client (tests); nil builds one whose
	// dials pass the Control hook.
	HTTPClient *http.Client
	// NetProbe is Q6a's probe from the runner's position; nil uses the
	// built-in DNS/TCP/TLS probe in net_probe.go.
	NetProbe NetProbe
	// Source is the runner's read-only source database reader (a
	// *postgres.DB on its read-only pool). nil means this runner has no
	// source connection: the source read ops stay in the catalogue and fail
	// with ErrNoSource (error_code not_supported).
	Source SourceReader
}

// Env is what an op sees: the project root, the targets, and the injected
// clocks, resolver and probe. It holds no credential: Source is a reader
// over a connection the runner opened, never its DSN.
type Env struct {
	ProjectDir string
	Targets    []events.SetupTarget
	MountDirs  []string
	// Materialised lists project-relative paths the runner materialised from
	// its secret store; no file op may read, list, write or delete them.
	Materialised []string
	Now          func() time.Time
	LookPath     func(string) (string, error)
	Exec         func(ctx context.Context, name string, args ...string) ([]byte, int, error)
	Resolver     *net.Resolver
	Confine      *Confiner
	HTTP         *http.Client
	Dial         func(ctx context.Context, network, address string) (net.Conn, error)
	Probe        NetProbe
	Source       SourceReader
}

// Registry holds the catalogue in registration order and implements the
// runner's Catalogue interface.
type Registry struct {
	mu     sync.RWMutex
	ops    map[string]Op
	order  []Op
	env    Env
	mounts []string
	// embedded is set by RegisterEmbedded: every write op other than
	// reference_write is refused (embeddedRefusal).
	embedded bool
}

// New opens the project root and registers the T0 and T1 ops of this row.
func New(cfg Config) (*Registry, error) {
	dir := strings.TrimSpace(cfg.ProjectDir)
	if dir == "" {
		return nil, errors.New("ops: a project directory is required")
	}
	confine, err := OpenConfiner(dir)
	if err != nil {
		return nil, err
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	look := cfg.LookPath
	if look == nil {
		look = exec.LookPath
	}
	run := cfg.Exec
	if run == nil {
		run = runCommand
	}
	dialer := newGuardedDialer(cfg.Resolver)
	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = newGuardedHTTPClient(dialer)
	}
	probe := cfg.NetProbe
	if probe == nil {
		probe = builtinProbe{}
	}
	r := &Registry{
		ops:    make(map[string]Op),
		env:    Env{ProjectDir: confine.Dir(), Targets: cfg.Targets, MountDirs: cfg.MountDirs, Materialised: append([]string(nil), cfg.Materialised...), Now: now, LookPath: look, Exec: run, Resolver: cfg.Resolver, Confine: confine, HTTP: httpc, Dial: dialer.DialContext, Probe: probe, Source: cfg.Source},
		mounts: append([]string(nil), cfg.MountDirs...),
	}
	for _, op := range []Op{
		envDetectOp{}, fileReadOp{}, dirListOp{}, jarListOp{}, javaVersionOp{},
		httpGetOp{}, netProbeOp{}, sinkProbeOp{}, fileWriteOp{}, fileDeleteOp{},
		sourcePublicationTablesOp{},
	} {
		if err := r.Register(op); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Register adds op. It fails on an empty or duplicate name, an unknown tier
// or an unknown target kind.
func (r *Registry) Register(op Op) error {
	if op == nil {
		return errors.New("ops: nil op")
	}
	name := op.Name()
	if name == "" {
		return errors.New("ops: op with an empty name")
	}
	switch op.Tier() {
	case protocol.TierT0, protocol.TierT1, protocol.TierT2, protocol.TierT3:
	default:
		return fmt.Errorf("ops: %s: unknown tier %q", name, op.Tier())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.ops[name]; dup {
		return fmt.Errorf("ops: duplicate op %q", name)
	}
	r.ops[name] = op
	r.order = append(r.order, op)
	return nil
}

// RegisterMountDir adds a directory a template mounts or builds from;
// file_write refuses every path under it. The directory is a project-relative
// path.
func (r *Registry) RegisterMountDir(dir string) error {
	if why := lexicalRefusal(dir); why != "" {
		return fmt.Errorf("%w: mount dir %q: %s", ErrPath, clip(dir), why)
	}
	clean := strings.ReplaceAll(strings.TrimPrefix(dir, "./"), "\\", "/")
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, d := range r.mounts {
		if d == clean {
			return nil
		}
	}
	r.mounts = append(r.mounts, clean)
	return nil
}

// Get returns the op named name.
func (r *Registry) Get(name string) (Op, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	op, ok := r.ops[name]
	return op, ok
}

// Names lists the catalogue in registration order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.order))
	for _, op := range r.order {
		out = append(out, op.Name())
	}
	return out
}

// Floor returns the op's tier floor, and false for an op not in the
// catalogue (the runner refuses it with RefuseUnknownOp).
func (r *Registry) Floor(op string) (string, bool) {
	o, ok := r.Get(op)
	if !ok {
		return "", false
	}
	return o.Tier(), true
}

// FloorFor returns the tier floor of one request: the higher of the op's
// fixed floor and, for a ParamTierer, the tier its params need. ok is false
// for an op not in the catalogue. err is non-nil (wrapping ErrParams) when the
// op's TierFor cannot classify the params or answers something that is not a
// tier; the runner refuses such a request rather than fall back to the fixed
// floor, so a step whose tier cannot be computed never runs at the lower one.
// An op without TierFor answers exactly Floor.
func (r *Registry) FloorFor(op string, params json.RawMessage) (tier string, ok bool, err error) {
	o, ok := r.Get(op)
	if !ok {
		return "", false, nil
	}
	floor := o.Tier()
	pt, isTierer := o.(ParamTierer)
	if !isTierer {
		return floor, true, nil
	}
	need, err := pt.TierFor(params)
	if err != nil {
		return floor, true, fmt.Errorf("%w: %s: the step's tier: %w", ErrParams, clipName(op), err)
	}
	rank, valid := tierRank(need)
	if !valid {
		return floor, true, fmt.Errorf("%w: %s: the step's tier %q is not T0..T3", ErrParams, clipName(op), clipName(need))
	}
	if base, _ := tierRank(floor); rank > base {
		return need, true, nil
	}
	return floor, true, nil
}

// tierRank orders the four tiers and reports false for any other string.
func tierRank(t string) (int, bool) {
	switch t {
	case protocol.TierT0:
		return 0, true
	case protocol.TierT1:
		return 1, true
	case protocol.TierT2:
		return 2, true
	case protocol.TierT3:
		return 3, true
	}
	return 0, false
}

// LayerOf returns the owners layer of the op ("environment", ...), and false
// for an op not in the catalogue. setup_plan fills each stored step's Layer
// from it (setuptools.LayerCatalogue).
func (r *Registry) LayerOf(op string) (string, bool) {
	o, ok := r.Get(op)
	if !ok {
		return "", false
	}
	return o.Layer(), true
}

// Target returns the target kind the request reaches: the op's own kind, or
// the kind its params name for http_get, net_probe and sink_probe. "" means no registered
// target and lets the runner's class floor for "" apply (it never raises).
func (r *Registry) Target(req protocol.OpRequest) string {
	op, ok := r.Get(req.Op)
	if !ok {
		return ""
	}
	env := r.snapshotEnv()
	if pt, ok := op.(ParamTargeter); ok {
		return pt.ParamTarget(req.Params, env)
	}
	return op.Target()
}

// GuardForbidden reports whether the guard table forbids op on class. Class
// is already raised to the runner's own floor. The table is deliberately
// short here: this catalogue has no T2 or T3 op, and the one destructive
// class it has (local file writes) is refused on a production target.
func (r *Registry) GuardForbidden(op, class string) bool {
	for _, forbidden := range guardTable[op] {
		if class == forbidden {
			return true
		}
	}
	return false
}

// guardTable is the runner's side of design §3.5 / plan §3.6: op -> classes
// it must not run on. Every entry has a test.
var guardTable = map[string][]string{
	"file_write":  {protocol.ClassProduction},
	"file_delete": {protocol.ClassProduction},
}

// StepAction returns the proposal-step action name op executes. The runner's
// ops and actions share names in this catalogue; the method is the explicit
// mapping the runner binds a frozen step with.
func (r *Registry) StepAction(op string) (string, bool) {
	if _, ok := r.Get(op); !ok {
		return "", false
	}
	return op, true
}

// CheckTarget is the runner's optional TargetChecker: an op whose params name
// a host outside the room's targets is refused with RefuseTarget, and params
// that do not decode are refused with RefuseSchema, before the guard and
// before anything executes.
func (r *Registry) CheckTarget(req protocol.OpRequest) string {
	op, ok := r.Get(req.Op)
	if !ok {
		return protocol.RefuseUnknownOp
	}
	if r.embeddedRefusal(op) != nil {
		// Writing outside the reference project is reaching outside what an
		// embedded room may touch.
		return protocol.RefuseTarget
	}
	tc, ok := op.(targetChecker)
	if !ok {
		return ""
	}
	err := tc.checkTarget(req.Params, r.snapshotEnv())
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrTarget):
		return protocol.RefuseTarget
	case errors.Is(err, ErrParams):
		return protocol.RefuseSchema
	}
	return ""
}

// targetChecker is implemented by an op that allow-lists a host from its
// params (http_get, net_probe, sink_probe).
type targetChecker interface {
	checkTarget(params json.RawMessage, env Env) error
}

// CatalogueVersion reports the version the runner advertises in Hello.
func (r *Registry) CatalogueVersion() string { return CatalogueVersion }

// Execute satisfies runner.Catalogue: it validates, runs one op under the
// nearer of the request's deadline and the op's own bound (the default, or
// a Deadliner's clamped deadline), and caps and redacts the result.
func (r *Registry) Execute(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error) {
	op, ok := r.Get(req.Op)
	if !ok {
		out := protocol.OpResult{ID: req.ID, OK: false, Summary: "unknown op " + clipName(req.Op)}
		return out, fmt.Errorf("%w: %q", ErrUnknownOp, clipName(req.Op))
	}
	env := r.snapshotEnv()
	if err := r.embeddedRefusal(op); err != nil {
		out := Result{Summary: "refused " + op.Name() + ": " + err.Error()}
		return r.finish(req.ID, out, err), err
	}
	if err := op.Validate(req.Params, env); err != nil {
		out := Result{Summary: "refused " + op.Name() + ": " + err.Error()}
		return r.finish(req.ID, out, err), err
	}

	timeout := opTimeout(op)
	if !req.Deadline.IsZero() {
		if left := time.Until(req.Deadline); left < timeout {
			timeout = left
		}
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	started := env.Now()
	res, err := op.Execute(runCtx, req.Params, env)
	if res.Receipt.At.IsZero() {
		res.Receipt.At = started
	}
	if res.Receipt.TookMS == 0 {
		res.Receipt.TookMS = env.Now().Sub(started).Milliseconds()
	}
	if err == nil {
		if cerr := runCtx.Err(); cerr != nil {
			// The op returned just as the deadline fired: report the cut, not
			// a success the server would treat as complete.
			err = fmt.Errorf("ops: %s: %w", op.Name(), cerr)
		}
	}
	if err != nil {
		if res.Summary == "" {
			res.Summary = "failed " + op.Name() + ": " + err.Error()
		}
	} else {
		res.Receipt.ExitCode = 0
	}
	return r.finish(req.ID, res, err), err
}

// finish converts a Result into the wire shape: Summary and Data redacted and
// capped, Receipt kept whole. A nil err means OK. Ops that emit external text
// (http_get, file_read) redact it at the source too; the pass here is the
// backstop that makes protocol.go's "Data after redaction" true for every op.
func (r *Registry) finish(id string, res Result, err error) protocol.OpResult {
	out := protocol.OpResult{ID: id, OK: err == nil, Receipt: res.Receipt, Truncated: res.Truncated}
	summary, _ := redact.Text(res.Summary)
	summary, cut := tools.Truncate(summary)
	out.Summary = summary
	out.Truncated = out.Truncated || cut
	if res.Data != nil {
		data, st := redact.JSONStats(res.Data)
		// A string past redact.MaxScan is masked whole, never passed through
		// unscanned; report that as a cut.
		out.Truncated = out.Truncated || st.ByKind[redact.KindOverflow] > 0
		raw, merr := json.Marshal(data)
		if merr != nil {
			out.Truncated = true
		} else if len(raw) > protocol.MaxDataBytes {
			out.Truncated = true
			out.Data = json.RawMessage(`{"truncated":true,"note":"data exceeds the protocol cap"}`)
		} else {
			out.Data = raw
		}
	}
	return out
}

// snapshotEnv returns Env with the current mount list, so a dir a driver
// registered after New is seen by file_write.
func (r *Registry) snapshotEnv() Env {
	r.mu.RLock()
	defer r.mu.RUnlock()
	env := r.env
	env.MountDirs = append([]string(nil), r.mounts...)
	return env
}

// Close releases the project root.
func (r *Registry) Close() error {
	if r.env.Confine == nil {
		return nil
	}
	return r.env.Confine.Close()
}

// decodeParams fills dst from params, rejecting unknown fields, trailing JSON
// and keys that differ only in letter case (the decoder would fold them and
// let the last win, protocol.CheckParamKeys). A nil params value is the empty
// object.
func decodeParams(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if err := protocol.CheckParamKeys(raw); err != nil {
		return fmt.Errorf("%w: %v", ErrParams, err)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: %v", ErrParams, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: trailing JSON", ErrParams)
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return fmt.Errorf("%w: trailing JSON", ErrParams)
	}
	return nil
}

// clip cuts peer text for an error or summary, on a rune boundary.
func clip(s string) string { return clipTo(s, maxLoggedNameBytes*4) }

func clipName(s string) string { return clipTo(s, maxLoggedNameBytes) }

func clipTo(s string, max int) string {
	s = strings.ToValidUTF8(s, "?")
	// A clipped string can come from a hostile endpoint (a certificate CN, a
	// Content-Type); a control character in it could inject terminal escapes
	// or forged log lines into a summary an operator reads.
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, s)
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8Start(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// utf8Start reports whether b is not a UTF-8 continuation byte.
func utf8Start(b byte) bool { return b&0xC0 != 0x80 }
