// Package actions holds write operations on customer systems. An action runs
// only as a step of a proposal that every required layer owner approved.
// The agent can propose actions but never runs them.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
package actions

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
)

// Action is one write operation.
type Action interface {
	// Name is snake_case and unique, e.g. "connect_restart_task".
	Name() string
	// Layer is the pipeline layer whose owner must approve (see events.Layers).
	Layer() string
	// Describe explains the action for the model and the approval UI.
	Describe() string
	// ParamsSchema is a JSON Schema object for Params.
	ParamsSchema() map[string]any
	// Validate checks params without side effects. Called when a proposal is
	// created and again right before Execute. It may read from the target, so
	// it takes a context: the implementation bounds it with its own deadline
	// and stops when ctx ends.
	Validate(ctx context.Context, params json.RawMessage) error
	// Execute performs the write and returns a short human-readable result.
	Execute(ctx context.Context, params json.RawMessage) (string, error)
}

// Scoped is implemented by every action that writes to a named resource
// (connector, schema subject, table, consumer group). Approvers in a room only
// own and watch that room's pipeline, so a step aimed at another pipeline's
// resource must be refused even when the resource exists (security finding
// F10). The room service calls CheckScope with the room's pipeline after
// Validate, both when a proposal is created and right before Execute; an
// action that does not implement Scoped is refused.
type Scoped interface {
	// CheckScope returns an error when params target anything outside p.
	// It must not do I/O: decide from params and p alone.
	CheckScope(p owners.Pipeline, params json.RawMessage) error
}

// Preview is implemented by an action that can say, without writing, what
// Execute would do to the target: its current state, the expected state after,
// whether it can be undone, and the target system's own dry-run verdict where
// one exists. The room service calls it once per step after proposal.created
// (10 s, read-only, the pipeline scope in ctx) and stores the result on
// proposal.previewed; a Preview error is stored as the step's error and never
// blocks the proposal. Preview must call only *Reader client methods plus the
// documented dry-run endpoints; it never changes a system. (F4)
type Preview interface {
	Preview(ctx context.Context, params json.RawMessage) (PreviewResult, error)
}

// PreviewResult and DryRunResult are the event types, so one definition serves
// the action, the room log and the API.
type (
	PreviewResult = events.PreviewResult
	DryRunResult  = events.DryRunResult
)

// ---- X8: staging (try a step on the pipeline's staging stack first) ----

// StageResult is the event type, like PreviewResult. (X8)
type StageResult = events.StageResult

// Staging is the write-capable client set of one pipeline's staging stack,
// built by wiring (X8a) from that pipeline's owners.yaml staging:
// {connections: ...} block only, and only when write mode is on; never from
// connections: or the process-wide settings, so no field ever reaches
// production. Actions get it as Deps.Staging; nil means no staging (no
// staging: block, or write mode off). A field is nil when the block does
// not configure that system: Connect without connect_url, Registry without
// schema_registry_url, Kafka without kafka_brokers (optional: staging Connect
// may share the production Kafka cluster with its own internal topics, so it
// is read-only here and no X8 action writes Kafka). Only Stage may use it. (X8)
type Staging struct {
	Connect  clients.ConnectClient
	Registry clients.SchemaRegistryClient
	Kafka    clients.KafkaReader
}

// Timing of a stage. The room service bounds each Stage call with
// StageStepTimeout. Stage watches an applied change for StageObserveWindow
// (a connector must be RUNNING with every task RUNNING at its end; a FAILED
// task ends it early as unhealthy; never RUNNING within it is unhealthy),
// then restores staging on a context of its own,
// context.WithoutCancel(ctx) bounded by StageRestoreTimeout, so a cancelled
// stage still restores. (X8)
const (
	StageObserveWindow  = 60 * time.Second
	StageStepTimeout    = 2 * time.Minute
	StageRestoreTimeout = 10 * time.Second
)

// StageExpiry is proposal.stage_started's expires_at for a stage of steps
// steps begun at start: each step's StageStepTimeout and
// StageRestoreTimeout, plus 30 s for the event writes. (X8)
func StageExpiry(start time.Time, steps int) time.Time {
	if steps < 1 {
		steps = 1
	}
	return start.Add(time.Duration(steps)*(StageStepTimeout+StageRestoreTimeout) + 30*time.Second)
}

// Stager is implemented by an action whose change can be tried on the
// pipeline's staging stack before production (X8b: connect config,
// schema_register, schema_set_compatibility; X6c: connect_tune_throughput).
// Stage applies the change raw describes to the same-named object on
// staging, watches it for StageObserveWindow where the change has a running
// state, puts staging back, and reports. It writes ONLY through Deps.Staging
// and never calls a production client, reader or writer; it never creates
// the object on staging (a connector or subject that is not there is
// Applied false, Detail "connector not present in staging").
//
// The room service calls it from POST /api/proposals/{pid}/stage (driver
// only) once per step whose action implements it, in step order, after the
// step's Validate and CheckScope pass again, with the room's pipeline scope
// in ctx and a StageStepTimeout deadline; Stage must stop when ctx ends. The
// executor never calls it, and staging never changes what Execute does. An
// action without Stager is unaffected: its steps are simply not staged.
//
// Results:
//   - no staging client for this action (Deps.Staging nil, or the field it
//     needs nil): Applied false, Detail events.StageNoStaging, nil error;
//   - staging's own pre-check refuses (Registry compatibility, Connect
//     validate): DryRun set with OK false, Applied false, and no write made;
//   - applied and healthy for the window: Applied true, Healthy true;
//   - applied and unhealthy: Applied true, Healthy false, Detail the failed
//     task's or registry's error text (redacted by CleanStageResult).
//
// A non-nil error means no verdict (staging unreachable, ctx ended first);
// the room service stores it as the step's error. Restoring is part of
// Stage: a connector gets its previous whole config back
// (clients.ConnectConfigPutter); what cannot be undone (a registered schema
// version) or failed to restore is reported in Leftover. Detail, Leftover
// and errors carry names, states and traces only, never a config value, a
// record value, a staging URL or a credential. (X8)
type Stager interface {
	Stage(ctx context.Context, raw json.RawMessage) (StageResult, error)
}

// CleanStageResult is the form of a StageResult the room service writes to
// proposal.staged: Target, Detail, Leftover and DryRun.Text pass redact.Text
// and are cut on a rune boundary to events.MaxStageTargetBytes,
// MaxStageDetailBytes, MaxStageLeftoverBytes and 2 KiB (DryRunResult's
// cap); Healthy without Applied becomes false and a negative DurationMS
// becomes 0, so a buggy action cannot show a healthy stage that never ran.
// It never modifies r's DryRun in place. (X8)
func CleanStageResult(r StageResult) StageResult {
	clean := func(s string, n int) string {
		s, _ = redact.Text(s)
		return cutText(s, n)
	}
	r.Target = clean(r.Target, events.MaxStageTargetBytes)
	r.Detail = clean(r.Detail, events.MaxStageDetailBytes)
	r.Leftover = clean(r.Leftover, events.MaxStageLeftoverBytes)
	if r.DryRun != nil {
		d := *r.DryRun
		d.Text = clean(d.Text, maxDryRunTextBytes)
		r.DryRun = &d
	}
	if !r.Applied {
		r.Healthy = false
	}
	if r.DurationMS < 0 {
		r.DurationMS = 0
	}
	return r
}

// maxDryRunTextBytes is DryRunResult.Text's documented cap (2 KiB).
const maxDryRunTextBytes = 2048

// cutText cuts s to at most n bytes, ending in "…", without splitting a
// rune.
func cutText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	const ell = "…"
	m := n - len(ell)
	for m > 0 && !utf8.RuneStart(s[m]) {
		m--
	}
	return s[:m] + ell
}

// Receipted is implemented by an action whose Execute can hand back a receipt
// of what was done: the setup actions, which forward one typed op to the
// paired runner and get its events.SetupReceipt back. The executor prefers
// ExecuteReceipt over Execute when an action implements it and stores the
// receipt on action.executed (ActionExecutedPayload.Receipt). The string is
// the same short human-readable result Execute returns. A receipt never
// carries a secret value or raw output (docs/design/setup-agent.md §2.2). (U)
type Receipted interface {
	ExecuteReceipt(ctx context.Context, params json.RawMessage) (string, events.SetupReceipt, error)
}

// Tiered is implemented by an action whose approval route depends on where
// it writes (the setup actions and, from U1, the Q actions when they run in a
// setup room). Tier returns the step's tier, protocol.TierT0 to TierT3: the
// op's floor raised to class, the class of the endpoint the room service
// resolved from params (never a label the model supplied). It must not do
// I/O. An action that is not Tiered is T2 in a setup room. The room service
// routes by the result: T0 is never a proposal, T1 is scheduled under
// PolicySetupLocal, T2 and T3 wait for the approver of design §3.6, T3 also
// for a maintenance window where the guard table says so. (U)
type Tiered interface {
	Tier(params json.RawMessage, class string) (string, error)
}

// ---- X2: the recovery action class ----

// Action classes. Every action is a fix unless it implements Classed and says
// otherwise; the values are the event constants (events.ActionClass*). (X2,
// X6)
const (
	ClassFix      = events.ActionClassFix
	ClassRecovery = events.ActionClassRecovery
	// ClassPerformance is the class of an action that tunes throughput
	// (connect_tune_throughput, X6c). Registry.Register refuses one that
	// does not implement Verifier (throughput before and after) and Preview
	// (the TuningChange list whose Current values are the rollback the card
	// offers). It changes only keys of TunableKeys, checked with
	// CheckTunable in Validate and again in Execute. (X6)
	ClassPerformance = events.ActionClassPerformance
)

// Classed is implemented by an action that is not a plain fix. A recovery
// action re-emits data a fault lost (pg_resnapshot_rows, kafka_replay_range)
// and must also implement Verifier; a performance action (X6) must implement
// Verifier and Preview: Registry.Register refuses either otherwise.
// Class must not do I/O and returns the same value on every call. It is the
// action's kind, unrelated to the endpoint class Tiered.Tier takes (local |
// shared | production). (X2)
type Classed interface {
	Class() string
}

// ClassOf returns a's class: Class() when a implements Classed and says
// something, ClassFix otherwise (every action from before X2). (X2)
func ClassOf(a Action) string {
	if c, ok := a.(Classed); ok {
		if v := c.Class(); v != "" {
			return v
		}
	}
	return ClassFix
}

// SourceLayered marks an action on the pipeline's source database whose
// approving layer is the pipeline's source layer (events.SourceLayer of its
// source_kind: postgres or mysql), not Layer(). Layer() of such an action
// answers "postgres", the value a caller without a pipeline shows. The room
// and the agent take the layer from LayerFor. (Q8)
type SourceLayered interface {
	Action
	// SourceLayered has no behaviour; it only marks the action.
	SourceLayered()
}

// LayerFor returns the layer whose owner approves a step of a on a pipeline
// whose source_kind is sourceKind ("" = postgres): SourceLayer(sourceKind)
// for a SourceLayered action ("" for an unknown kind, which no owner owns),
// a.Layer() otherwise. (Q8)
//
// It does not know Layered (Q7) and answers "" for such an action: it stays
// for callers that only describe an action without params (builders, the
// action catalogue). The room service and the agent take a step's layer from
// StepLayer, the one entry point of both rules (tasks/Q-board.md, "7. MySQL
// source").
func LayerFor(a Action, sourceKind string) string {
	if _, ok := a.(SourceLayered); ok {
		return events.SourceLayer(sourceKind)
	}
	return a.Layer()
}

// ---- Q7: actions whose layer depends on params, and deferred actions ----

// Layered is implemented by an action whose approving layer depends on its
// params (repo_open_pull_request: the layer of the path globs the files
// match). Its Layer() returns "" (Registry.Register refuses "" from an action
// that is not Layered). The room service takes the step's layer from
// StepLayer, which asks LayerFor with the room's pipeline, at creation and
// again before Execute, and refuses a step whose Layer differs: the proposer
// never picks the approver. It must not do I/O. (Q7)
type Layered interface {
	LayerFor(p owners.Pipeline, params json.RawMessage) (string, error)
}

// StepLayer is the layer whose owner approves one step of a with params on
// pipeline p, the one rule the room service (Q7k, Q8o) and the agent call:
//   - a Layered action: LayerFor(p, params);
//   - a SourceLayered action: events.SourceLayer(p.SourceKindOrDefault());
//   - any other action: a.Layer().
//
// The source kind is p's owners.yaml source_kind (Q8d; "" = postgres).
// An empty answer is an error, never a layer: no owner owns "", so a step
// without a layer fails closed instead of reaching an approver by accident.
// It does no I/O. (Q7, Q8)
func StepLayer(a Action, p owners.Pipeline, params json.RawMessage) (string, error) {
	var layer string
	if l, ok := a.(Layered); ok {
		v, err := l.LayerFor(p, params)
		if err != nil {
			return "", fmt.Errorf("actions: %s: layer from params: %w", a.Name(), err)
		}
		layer = v
	} else if _, ok := a.(SourceLayered); ok {
		sourceKind := p.SourceKindOrDefault()
		layer = events.SourceLayer(sourceKind)
		if layer == "" {
			return "", fmt.Errorf("actions: %s: unknown source_kind %q", a.Name(), sourceKind)
		}
	} else {
		layer = a.Layer()
	}
	if layer == "" {
		return "", fmt.Errorf("actions: %s: no approving layer", a.Name())
	}
	return layer, nil
}

// Deferred is implemented by an action whose Execute only asks for a change
// that people apply later outside Piperoom (repo_open_pull_request: the
// repo's reviewers merge the PR, the customer's CD deploys it). Rules the
// room service and the agent apply to every step of such an action:
//   - never approved by a policy: not in owners.yaml auto_approve: or
//     autonomy: (Load refuses it), never scheduled, never a setup T1 step;
//   - a proposal holding a Deferred step holds only Deferred steps, at most
//     MaxDeferredSteps;
//   - the agent does not verify on action_result; it waits for the apply
//     signal (HumanInput "pr_merged") and the change to reach the system
//     (docs/design/fix-pr.md §8).
//
// Deferred has no method beyond the marker. Registry.Register refuses a
// Deferred action of class recovery or performance: a deferred change cannot
// be measured by Verify on the executor's timeline. (Q7)
type Deferred interface {
	Deferred()
}

// MaxDeferredSteps caps the Deferred steps of one proposal. (Q7)
const MaxDeferredSteps = 3

// PullRequestOpener is implemented by an action whose Execute opens a pull
// request. The executor prefers ExecutePullRequest over Execute when an
// action implements it and stores the reference on action.executed
// (ActionExecutedPayload.PullRequest), which starts the room's follower
// (Q7e). The string is Execute's short result. (Q7)
type PullRequestOpener interface {
	ExecutePullRequest(ctx context.Context, params json.RawMessage) (string, events.PullRequestRef, error)
}

// StepRef identifies the proposal step being executed. The executor puts it
// in ctx before Execute (WithStep); an action that names something after the
// step (the fix branch) reads it with StepFrom and refuses to run without it.
// Approvals are "layer: display name" of the approval.given events, in
// required_layers order. (Q7)
type StepRef struct {
	RoomID     string
	ProposalID string
	StepIndex  int
	Title      string // the proposal title, sanitized as stored
	Approvals  []string
}

// stepKey is the context key of a StepRef.
type stepKey struct{}

// WithStep returns ctx carrying s. Approvals is copied, so a later change to
// the caller's slice does not reach the action. (Q7)
func WithStep(ctx context.Context, s StepRef) context.Context {
	s.Approvals = slices.Clone(s.Approvals)
	return context.WithValue(ctx, stepKey{}, s)
}

// StepFrom returns the StepRef the executor put in ctx, and false when there
// is none (a call outside the executor: Validate at creation, Preview, a
// test). The Approvals slice is the caller's own copy. (Q7)
func StepFrom(ctx context.Context) (StepRef, bool) {
	s, ok := ctx.Value(stepKey{}).(StepRef)
	if !ok {
		return StepRef{}, false
	}
	s.Approvals = slices.Clone(s.Approvals)
	return s, true
}

// DiffPreview is the event type (PreviewResult.Diff). (Q7)
type DiffPreview = events.DiffPreview

// Default poll schedule of a recovery verification: the agent calls Verify
// first RecoveryVerifyFirstPoll after the step ran, doubles the wait up to
// RecoveryVerifyMaxPoll, and stops when After is 0 or RecoveryVerifyTimeout
// after the step ran (A4 may make them settings). (X2)
const (
	RecoveryVerifyFirstPoll = 15 * time.Second
	RecoveryVerifyMaxPoll   = 60 * time.Second
	RecoveryVerifyTimeout   = 10 * time.Minute
)

// Verifier is implemented by every recovery action (and every performance
// action, with the X6 measure below): it measures, without
// writing, how much of the gap the action set out to repair is still open.
// The agent (X2c) calls it after the step's action.executed with ok: true,
// once per poll of the schedule above, with the step's params and the time
// the step ran (the action.executed event time), and writes one
// agent.recovery_check per poll and one agent.verification with the
// outcome (events.RecoveryOutcomeFor) at the end. The agent runs it, not the
// room service, on the same action instance the executor runs (the agent's
// Deps.Actions is the pipeline's registry, built over write clients when
// write mode is on), so the rule is Verify's own: it calls only *Reader
// client methods (the Reconciler counts of X1, CountReader, Kafka offsets)
// and never a writer, whatever write mode says.
//
// Each call is bounded by the caller with a 10 s deadline and must stop when
// ctx ends; the room's pipeline scope is in ctx, so a read outside it is
// refused (tools.ErrOutOfScope). It reads counts and timestamps only and
// returns counts only: never a row value or key, in VerifyResult or in an
// error. An error wrapping clients.ErrNotSupported (the source or sink cannot
// count) makes the outcome events.RecoveryUnverifiable; any other error is
// one failed poll and the agent keeps polling until its timeout. (X2)
type Verifier interface {
	Verify(ctx context.Context, raw json.RawMessage, executedAt time.Time) (VerifyResult, error)
}

// VerifyResult is one Verify measurement. Before is the size of the gap the
// action set out to repair, measured for the window or range in params (for
// pg_resnapshot_rows the source rows the condition matches, the preview's
// Count; for kafka_replay_range the records in the range) and the same on
// every call. After is the part of Before not yet found at the target, 0 <=
// After <= Before. Unit is events.RecoveryUnitRows or RecoveryUnitRecords.
// Detail is one optional line of counts and names (table, topic, window), at
// most 200 bytes, never values. (X2) A performance action fills it with
// throughput instead (X6 block below): there After may exceed Before.
type VerifyResult struct {
	Before int64  `json:"before"`
	After  int64  `json:"after"`
	Unit   string `json:"unit"`
	Detail string `json:"detail,omitempty"`
	// X6: set by a performance action only (omitted otherwise, so a recovery
	// result marshals as before); see the X6 block below.
	LagSlopeBefore *int64 `json:"lag_slope_before,omitempty"`
	LagSlopeAfter  *int64 `json:"lag_slope_after,omitempty"`
}

// ---- X6: the performance action class (ClassPerformance, above) ----

// Verify schedule of a performance action. Verify derives both windows from
// executedAt, never from the clock: Before is the throughput over
// [executedAt-PerformanceVerifyWindow, executedAt), After over
// [executedAt+PerformanceVerifySettle, executedAt+PerformanceVerifySettle+
// PerformanceVerifyWindow) (the settle time skips the task restart and
// rebalance a config change causes). The agent makes its first call once
// the After window has closed, retries a failed call every
// RecoveryVerifyMaxPoll, and stops at PerformanceVerifyTimeout after the
// step ran (then the outcome is events.PerformanceUnverifiable). (X6)
const (
	PerformanceVerifyWindow  = 15 * time.Minute
	PerformanceVerifySettle  = 2 * time.Minute
	PerformanceVerifyTimeout = 30 * time.Minute
)

// A performance action's Verify follows the Verifier rules above (read
// methods only, 10 s per call, the room's pipeline scope in ctx, counts
// only) with this VerifyResult:
//   - Before and After are the throughput of the step's connector, in
//     events.PerformanceUnitRecordsPerMinute (Unit): for a source connector
//     the records it wrote to its topics, for a sink connector the records
//     its consumer group consumed, the same measure the kafka_throughput
//     tool (X6a) reports. Both are >= 0.
//   - LagSlopeBefore and LagSlopeAfter are the change of the sink's
//     consumer lag over each window in records per minute (positive: lag
//     grows), nil for a source connector or when not measured.
//   - Detail names the connector and topic and the two windows, never a
//     value.
//
// Verify keeps no state between calls: it reads only history the call can
// reach at that moment (record counts by offset timestamp,
// clients.RateReader; the pipeline's group_lag watch samples,
// clients.SampleReader). A sink's consumed records over a window are the
// records written to its topics less the change of group_lag (the
// pipeline's total lag, one sample per 15 min) between the window's ends.
// When a window cannot be measured from it (no sample near an end: the
// watcher is off or young), the error wraps clients.ErrNotSupported and the
// outcome is unverifiable. The agent
// applies events.PerformanceOutcomeFor to the result. (X6)

// TunableKey is one connector config key a performance action may change,
// with the static bounds of its value: a base-10 integer v, Min <= v <= Max.
// Connector is the connector type the key applies to ("source", "sink"), or
// "" for both. Bounds that depend on the target (tasks.max at most the
// topic's partitions for a sink, max.queue.size above max.batch.size) are
// the action's to check on top. (X6)
type TunableKey struct {
	Key       string `json:"key"`
	Connector string `json:"connector,omitempty"`
	Min       int64  `json:"min"`
	Max       int64  `json:"max"`
}

// TunableKeys returns the allow-list of the performance class, in a stable
// order, as a fresh slice (like clients.ChangeKinds). It is the leads': a
// key is added only by a contract change (a test pins the list). Any other
// key, including every LooksSecret key, is out of the class. The consumer's
// max.poll.records is set per sink connector through Connect's client
// override, consumer.override.max.poll.records (the worker must allow
// overrides). (X6)
func TunableKeys() []TunableKey {
	return []TunableKey{
		{Key: "tasks.max", Min: 1, Max: 128},
		{Key: "max.batch.size", Connector: "source", Min: 1, Max: 20480},
		{Key: "max.queue.size", Connector: "source", Min: 2, Max: 163840},
		{Key: "poll.interval.ms", Connector: "source", Min: 10, Max: 60000},
		{Key: "snapshot.fetch.size", Connector: "source", Min: 1, Max: 100000},
		{Key: "snapshot.max.threads", Connector: "source", Min: 1, Max: 16},
		{Key: "consumer.override.max.poll.records", Connector: "sink", Min: 1, Max: 10000},
	}
}

// TuningChange is the event type (PreviewResult.Tuning), so one definition
// serves the action, the room log and the API. (X6)
type TuningChange = events.TuningChange

// ErrNotTunable: the key is outside TunableKeys, looks secret, or does not
// apply to the connector's type. (X6)
var ErrNotTunable = errors.New("actions: key is not tunable")

// ErrTuningBounds: the value is not a base-10 integer or is outside the
// key's static bounds; the message names the bounds. (X6)
var ErrTuningBounds = errors.New("actions: tuning value out of bounds")

// CheckTunable checks one change of a performance step against the
// allow-list: connectorType is the connector's type from
// clients.ConnectorStatus.Type ("source" or "sink"; anything else is
// refused), key must be in TunableKeys for that type and value a base-10
// integer within its bounds. It does no I/O. The error wraps ErrNotTunable
// or ErrTuningBounds and never echoes a value of a key outside the
// allow-list. (X6)
func CheckTunable(connectorType, key, value string) error {
	if connectorType != "source" && connectorType != "sink" {
		return fmt.Errorf("%w: connector type %q is not source or sink", ErrNotTunable, connectorType)
	}
	if LooksSecret(key) {
		return fmt.Errorf("%w: %q looks like a credential", ErrNotTunable, key)
	}
	for _, k := range TunableKeys() {
		if k.Key != key {
			continue
		}
		if k.Connector != "" && k.Connector != connectorType {
			return fmt.Errorf("%w: %s applies to %s connectors only", ErrNotTunable, key, k.Connector)
		}
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil || v < k.Min || v > k.Max {
			return fmt.Errorf("%w: %s must be an integer from %d to %d", ErrTuningBounds, key, k.Min, k.Max)
		}
		return nil
	}
	return fmt.Errorf("%w: %q is not in the performance allow-list", ErrNotTunable, key)
}

// secretKeySubstrings are the fragments that mark a config key as a
// credential. The rule is shared, not configurable: a runner op, a validator
// and a connector-config action must agree on what is secret.
var secretKeySubstrings = []string{"password", "passwd", "secret", "token", "credential", "jaas", "apikey", "private"}

// LooksSecret reports whether a config key probably names a credential: the
// key contains one of password, passwd, secret, token, credential, jaas,
// apikey or private (any case), or has a bare "key" segment (api.key,
// ssl.key.password, key.converter credentials). It is the one rule the
// connector-config actions, the config validator (Q4a) and the runner's ops
// and artifact validators share, so a value for such a key is refused in
// plain text everywhere and allowed only as a ${secret:name} or
// ${file:...:key} reference. It is a second line of defence behind each
// caller's allow-list, so it errs towards true. (U; A2 switches its private
// copy to it in U1-23)
func LooksSecret(key string) bool {
	k := strings.ToLower(key)
	for _, s := range secretKeySubstrings {
		if strings.Contains(k, s) {
			return true
		}
	}
	for _, seg := range strings.FieldsFunc(k, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	}) {
		if seg == "key" {
			return true
		}
	}
	return false
}

// ---- Connector config: redaction, hash and changed keys (X3-0, X5-0) ----
//
// One scheme for the watcher's config_drift (X3b), the accept-config route
// (X3b) and the stored config versions (X5c):
//   - RedactConfig is the only form of a config that is stored or shown.
//   - ConfigHash of that form is the config's public id: config_drift's
//     old_hash/new_hash, accept-config's hash and a config version's hash.
//     No secret value enters it, so it is never a guessing oracle.
//   - DigestConfig is what the watcher keeps between polls: the hash and the
//     redacted form, plus a keyed fingerprint per key that stays in the
//     process, so ChangedConfigKeys also sees a change the redacted form
//     hides (a secret value, or a password inside a JDBC URL).
//   - events.SecretKeyChanged is the one placeholder for a changed credential
//     key's name, in config_drift and in a config version's changed_keys.

// RedactedValue replaces the value of a LooksSecret key in a stored config.
// (X5)
const RedactedValue = "<redacted>"

// MaxConfigValueBytes cuts one redacted config value; a longer one ends in
// "…" within the limit. (X5)
const MaxConfigValueBytes = 4096

// RedactConfig returns the form of a connector config that may be stored and
// shown (X5c config versions): a LooksSecret key's value becomes
// RedactedValue, every other value passes redact.Text (so a password inside a
// JDBC URL is masked) and is cut at MaxConfigValueBytes on a rune boundary.
// It never modifies cfg and returns a non-nil map. Apply it once, to the raw
// config: redact.Text is not idempotent (a masked "[redacted 16]" may be
// masked again), so the watcher (X3b) redacts (DigestConfig does) and hands
// ConfigDigest.Redacted on, and the store (X5c) keeps it as given. (X5)
func RedactConfig(cfg map[string]string) map[string]string {
	out := make(map[string]string, len(cfg))
	for k, v := range cfg {
		if LooksSecret(k) {
			out[k] = RedactedValue
			continue
		}
		v, _ = redact.Text(v)
		out[k] = cutValue(v)
	}
	return out
}

// cutValue cuts v to at most MaxConfigValueBytes bytes, ending in "…",
// without splitting a rune.
func cutValue(v string) string {
	if len(v) <= MaxConfigValueBytes {
		return v
	}
	const ell = "…"
	n := MaxConfigValueBytes - len(ell)
	for n > 0 && !utf8.RuneStart(v[n]) {
		n--
	}
	return v[:n] + ell
}

// ConfigHash is the id of a redacted config: the lowercase hex SHA-256 of its
// JSON encoding (encoding/json writes map keys sorted, so key order never
// matters; nil and empty are the same). Pass RedactConfig's result: hashing a
// raw config would make the hash a guessing oracle for a secret value. Two
// configs that differ only where RedactConfig masks (a secret value) have the
// same hash. (X3, X5)
func ConfigHash(redacted map[string]string) string {
	if redacted == nil {
		redacted = map[string]string{}
	}
	// json.Marshal of a map[string]string cannot fail: strings always encode
	// (invalid UTF-8 becomes U+FFFD), so there is no error to report.
	b, _ := json.Marshal(redacted)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ConfigDigest is what the watcher keeps of a connector config between two
// polls (X3b), in memory only: Hash is ConfigHash(Redacted), the config's
// public id; Redacted is RedactConfig of the raw config, the form a config
// version stores (X5c). The per-key fingerprints are unexported, keyed with a
// random per-process key (HMAC-SHA256), never marshalled, printed or logged
// (String, GoString and LogValue show Hash only), and comparable only between
// digests of the same process. Build one with DigestConfig. (X3, X5)
type ConfigDigest struct {
	Hash     string            `json:"hash"`
	Redacted map[string]string `json:"-"`
	keys     map[string][sha256.Size]byte
}

// String returns the hash only, so a digest in a format verb or a log line
// never shows a fingerprint.
func (d ConfigDigest) String() string { return d.Hash }

// GoString is String, for %#v.
func (d ConfigDigest) GoString() string { return d.Hash }

// LogValue is the hash only, for log/slog.
func (d ConfigDigest) LogValue() slog.Value { return slog.StringValue(d.Hash) }

// fingerprintKey keys ConfigDigest's per-key fingerprints. It is random per
// process and never leaves it, so a fingerprint cannot be checked against a
// guessed value elsewhere. Set once, never changed.
var fingerprintKey = sync.OnceValue(func() []byte {
	k := make([]byte, 32)
	_, _ = rand.Read(k) // never fails (crypto/rand: it crashes the program instead)
	return k
})

// DigestConfig reduces cfg (clients.ConnectReader.Config) to a ConfigDigest.
// It keeps no raw value. (X3, X5)
func DigestConfig(cfg map[string]string) ConfigDigest {
	red := RedactConfig(cfg)
	d := ConfigDigest{Hash: ConfigHash(red), Redacted: red, keys: make(map[string][sha256.Size]byte, len(cfg))}
	key := fingerprintKey()
	for k, v := range cfg {
		m := hmac.New(sha256.New, key)
		// Length prefixes keep "a"+"bc" and "ab"+"c" apart.
		fmt.Fprintf(m, "%d:%s%d:%s", len(k), k, len(v), v)
		var fp [sha256.Size]byte
		copy(fp[:], m.Sum(nil))
		d.keys[k] = fp
	}
	return d
}

// ChangedConfigKeys returns the names of the keys added, removed or changed
// from old to cur, sorted and without repeats, with every name LooksSecret
// matches replaced by events.SecretKeyChanged (once, however many). Names
// only, never a value; not nil, so none encodes as []. It compares the raw
// values' fingerprints, so it also reports a change that keeps Hash (a
// secret value: then old.Hash == cur.Hash and the result holds
// events.SecretKeyChanged). The config_drift signal keeps the first
// events.MaxChangedKeys and counts the rest in KeysOmitted; a config version
// keeps up to clients.MaxConfigKeys. (X3, X5)
func ChangedConfigKeys(old, cur ConfigDigest) []string {
	var names []string
	for k, fp := range old.keys {
		if c, ok := cur.keys[k]; !ok || c != fp {
			names = append(names, k)
		}
	}
	for k := range cur.keys {
		if _, ok := old.keys[k]; !ok {
			names = append(names, k)
		}
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		if LooksSecret(n) {
			n = events.SecretKeyChanged
		}
		out = append(out, n)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// Registry holds actions in registration order.
type Registry struct {
	mu    sync.RWMutex
	byKey map[string]Action
	order []Action
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{byKey: map[string]Action{}}
}

// Register adds a. It fails on an empty or duplicate name, an unknown class,
// and a recovery action that does not implement Verifier (X2): a repair that
// cannot be counted afterwards is never registered. A performance action
// needs Verifier and Preview (X6). An empty Layer() needs Layered, so no
// step is ever without an approving layer, and a Deferred action must be a
// fix: a recovery or performance verdict cannot wait for a merge (Q7).
func (r *Registry) Register(a Action) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	name := a.Name()
	if name == "" {
		return errors.New("actions: empty action name")
	}
	if _, layered := a.(Layered); !layered && a.Layer() == "" {
		return fmt.Errorf("actions: action %q has an empty Layer() and does not implement Layered", name)
	}
	if _, deferred := a.(Deferred); deferred {
		if class := ClassOf(a); class != ClassFix {
			return fmt.Errorf("actions: deferred action %q has class %q; a deferred action is a fix", name, class)
		}
	}
	switch class := ClassOf(a); class {
	case ClassFix:
	case ClassRecovery:
		if _, ok := a.(Verifier); !ok {
			return fmt.Errorf("actions: recovery action %q does not implement Verifier", name)
		}
	case ClassPerformance: // X6: a tuning is measured after it ran and can be rolled back from its preview
		if _, ok := a.(Verifier); !ok {
			return fmt.Errorf("actions: performance action %q does not implement Verifier", name)
		}
		if _, ok := a.(Preview); !ok {
			return fmt.Errorf("actions: performance action %q does not implement Preview", name)
		}
	default:
		return fmt.Errorf("actions: action %q has unknown class %q", name, class)
	}
	if _, dup := r.byKey[name]; dup {
		return fmt.Errorf("actions: duplicate action %q", name)
	}
	r.byKey[name] = a
	r.order = append(r.order, a)
	return nil
}

// Get returns the action with the given name.
func (r *Registry) Get(name string) (Action, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.byKey[name]
	return a, ok
}

// All returns every action in registration order.
func (r *Registry) All() []Action {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]Action(nil), r.order...)
}
