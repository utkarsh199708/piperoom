// Package events defines the incident room event log: every change in a room
// is one append-only event. The UI, Slack, hand-off and audit all read it.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"
)

// Type names an event. Values are stable strings stored in the database and
// sent to the browser.
type Type string

const (
	RoomCreated Type = "room.created"
	RoomClosed  Type = "room.closed"

	// Room limits (docs/design/retention.md). The first three are stored in
	// the room log; room.deleted never is (see RoomDeletedPayload).
	RoomRetentionWarning Type = "room.retention_warning"
	RoomExtended         Type = "room.extended"
	RoomLogCapWarning    Type = "room.log_cap_warning"
	RoomDeleted          Type = "room.deleted"

	// RoomEscalated raises the severity of an open room: a forecast room whose
	// metric reached its limit, so the watcher's normal signal joined it.
	// System actor; the snapshot's severity is the newest one. (X3)
	RoomEscalated Type = "room.escalated"

	AgentStatus     Type = "agent.status"
	AgentStepStart  Type = "agent.step.started"
	AgentToolCall   Type = "agent.tool_call"
	AgentToolResult Type = "agent.tool_result"
	AgentMessage    Type = "agent.message"
	AgentHypothesis Type = "agent.hypothesis"
	// AgentUsage reports the tokens, latency and estimated cost of one model
	// call (docs/design/cost.md). Counts only, never text.
	AgentUsage Type = "agent.usage"
	// AgentVerification is the agent's verdict after the last step of a
	// proposal ran: the layer checks and the data-flow check it made. It
	// replaces the plain-text "[verification]" agent.message. (F4)
	AgentVerification Type = "agent.verification"
	// AgentRecoveryCheck is one poll of a recovery action's Verify while the
	// agent waits for the repair to land: the counts so far, so a client can
	// show "checking (2 of N)". Progress only: the verdict is the one
	// agent.verification that follows the last poll. (X2)
	AgentRecoveryCheck Type = "agent.recovery_check"
	// AgentTriage is the bounded first look written by the triage pass before
	// the full investigation (docs/design: tasks/F6-proactive-triage.md). One
	// per run of the pass; a room may hold several (a re-fire re-runs it). (F6)
	AgentTriage Type = "agent.triage"

	CommentAdded   Type = "comment.added"
	SteerSuggested Type = "steer.suggested"
	SteerSent      Type = "steer.sent"
	// SteerDeclined closes a pending suggestion without sending it
	// (POST /api/rooms/{id}/suggestions/{sid}/decline, driver only).
	SteerDeclined Type = "steer.declined"

	ControlRequested Type = "control.requested"
	ControlGranted   Type = "control.granted"
	// ControlLeaseExpired says the driver's lease ran out with no renewal and
	// no new driver: the room has nobody actively steering. System actor.
	ControlLeaseExpired Type = "control.lease_expired"

	ProposalCreated  Type = "proposal.created"
	ApprovalGiven    Type = "approval.given"
	ApprovalRejected Type = "approval.rejected"
	ActionExecuted   Type = "action.executed"
	// ProposalPreviewed is written by the system actor once per proposal,
	// after proposal.created, with what each step would do (actions.Preview).
	// Informational: it is not part of params_hash and can be absent (no
	// preview support, a restart mid-preview). (F4)
	ProposalPreviewed Type = "proposal.previewed"
	// ApprovalNudged records that a member asked a pending layer's owners to
	// look (POST /api/rooms/{id}/nudge); at most one per proposal and layer
	// per 10 minutes. The Slack mirror renders it as a thread reply. (F9)
	ApprovalNudged Type = "approval.nudged"
	// ProposalScheduled says the pipeline's auto-approve policy will approve
	// and run the proposal at RunAt unless someone cancels it first. Written
	// by the system actor after proposal.previewed, at most once per proposal.
	// Like approval.given it counts against the room log caps (system events
	// may use the 2% reserve; it is not exempt like room.closed). (P2)
	ProposalScheduled Type = "proposal.scheduled"
	// ProposalCancelled stops a scheduled proposal: the policy will not run
	// it, and it stays open for manual approval. Actor: the person who
	// cancelled (POST /api/proposals/{pid}/cancel). Counted like
	// proposal.scheduled. (P2)
	ProposalCancelled Type = "proposal.cancelled"
	// X8: ProposalStageStarted says the driver started trying a proposal's
	// steps on the pipeline's staging stack (POST /api/proposals/{pid}/stage).
	// Actor: the driver. It ends with proposal.staged for the same proposal,
	// or at its expires_at; while one is pending a second stage answers 409.
	// ProposalStaged carries what staging said (actions.Stager), written by the
	// system actor. Both are informational like proposal.previewed: not part
	// of params_hash, never run by the executor, and the agent's Sink refuses
	// them. (X8)
	ProposalStageStarted Type = "proposal.stage_started"
	ProposalStaged       Type = "proposal.staged"

	// AutonomyGranted and AutonomyRevoked are the trust ledger of one
	// (pipeline, action): the action earned policy approval (PolicyEarned)
	// after a streak of verified recoveries, or lost it. System actor, except
	// a revoke by a person (reason "human"), whose actor is that person.
	// Written to the room whose verification or click caused them. (AU1)
	AutonomyGranted Type = "autonomy.granted"
	AutonomyRevoked Type = "autonomy.revoked"

	// HypothesisVerdict is a person's confirm or dispute of the agent's
	// current hypothesis (POST /api/rooms/{id}/hypothesis). HypothesisSeq is
	// the seq of the agent.hypothesis it answers; a later agent.hypothesis
	// starts over with no verdicts. Any member may write one; the snapshot
	// keeps the latest per person. AgentHypothesisPayload.Confirmed is never
	// set by the agent: clients and the snapshot derive it from these events. (F9)
	HypothesisVerdict Type = "hypothesis.verdict"

	// X4: RoomTeamPaged records that the driver paged the team that owns an
	// upstream cause (POST /api/rooms/{id}/page-team): one Slack post to the
	// team's owners.yaml slack_channel, never automatic, at most one per team
	// per room per hour. Actor: the driver. The team's reply is not ingested.
	// The room's agent Sink refuses it. (X4)
	RoomTeamPaged Type = "room.team_paged"

	// HandoffRequested is written when the driver asks for a hand-off, before
	// the agent writes the summary (which can take minutes). It ends with
	// handoff.created or handoff.failed for the same request_id, or at its
	// expires_at. While one is in flight a second request answers 409.
	HandoffRequested Type = "handoff.requested"
	HandoffFailed    Type = "handoff.failed"
	HandoffCreated   Type = "handoff.created"
	HandoffAccepted  Type = "handoff.accepted"

	TracePinned Type = "trace.pinned"

	// PostmortemDrafted says a closing draft exists for a closed room; the
	// Markdown itself is rendered on request by GET /api/rooms/{id}/postmortem.md
	// and never stored. Written once by the system actor after room.closed (not
	// in shadow rooms). PostmortemFailed says the model pass failed; the
	// events-only draft still renders. Never written for an open room. (F2)
	PostmortemDrafted Type = "postmortem.drafted"
	PostmortemFailed  Type = "postmortem.failed"

	// RoomLabelled is the human label of a closed room (POST /api/rooms/{id}/label):
	// what the cause was and what fixed it, for similar_incidents and evals.
	// Any room, shadow or not; re-labelling appends another and the last wins. (F8)
	RoomLabelled Type = "room.labelled"
	// ChecklistStepDone records that a person did (or undid) one item of a
	// runbook section shown in the room (POST /api/rooms/{id}/checklist). (F8)
	ChecklistStepDone Type = "checklist.step_done"

	// Shadow mode (docs/design/shadow.md). All three are written only to a
	// closed shadow room, after room.closed.
	ShadowRevealed Type = "shadow.revealed"
	ShadowLabelled Type = "shadow.labelled"
	ShadowGraded   Type = "shadow.graded"

	// Setup rooms (docs/design/setup-agent.md; U wave). Every setup.* event
	// is audit: it names what happened, who did it and the runner label, and
	// never a secret value, a pairing code or a runner token. All are written
	// by the room service (system actor, or the person who clicked) except
	// setup.plan and setup.blocked, which the agent emits through its Sink.
	// The Sink refuses setup.verified from the agent: only the server's
	// ladder runner writes it.
	SetupPairing               Type = "setup.pairing"                // every pairing attempt, success or not, and a replacement of a lost runner
	SetupRunnerAttached        Type = "setup.runner_attached"        // the runner's Hello, after the person confirmed on its terminal
	SetupRunnerLost            Type = "setup.runner_lost"            // silent for protocol.LostAfter; in-flight steps become unknown
	SetupRunnerRefused         Type = "setup.runner_refused"         // the runner did not run an op; reason is a protocol.Refuse* code
	SetupPlan                  Type = "setup.plan"                   // the agent's plan (setup_plan); tiers filled by the server
	SetupTargetRegistered      Type = "setup.target_registered"      // a driver click added a target (POST /api/rooms/{id}/setup/targets)
	SetupTargetClassed         Type = "setup.target_classed"         // a target's class was set or raised, by the server or the person
	SetupPipelineExtended      Type = "setup.pipeline_extended"      // a driver click grew the provisional pipeline
	SetupSecretSet             Type = "setup.secret_set"             // a secret name entered the runner's store; the value is nowhere
	SetupCredentialsRegistered Type = "setup.credentials_registered" // read credentials reached PIPEROOM_SECRETS_DIR; names only
	SetupMaintenanceWindow     Type = "setup.maintenance_window"     // the driver recorded a window for T3 steps
	SetupVerified              Type = "setup.verified"               // the ladder's outcome; emitted by the ladder runner only
	SetupAwaitingChange        Type = "setup.awaiting_change"        // rung 8: the ladder waits for a change the person makes
	SetupBlocked               Type = "setup.blocked"                // a bound stopped the run; it asks the person a question
	// PipelineRegistered says the owners entry of a setup room's pipeline was
	// written (or its pull request opened) and the server reloaded. The room
	// then closes with resolution "pipeline live".
	PipelineRegistered Type = "pipeline.registered"
)

// ActorKind says who caused an event.
type ActorKind string

const (
	ActorHuman  ActorKind = "human"
	ActorAgent  ActorKind = "agent"
	ActorSystem ActorKind = "system"
)

// Actor is who caused an event.
type Actor struct {
	Kind ActorKind `json:"kind"`
	ID   string    `json:"id"`
	Name string    `json:"name"`
	// Verified is true when the server vouches for ID and Name: a person in
	// users.yaml (kind human), or the server itself (kinds agent and system).
	// It is false for a person known only to the IdP: a derived "u_" id that
	// got in through an owners.yaml group or user: principal, auditors: or a
	// viewer group. That person's Name comes from the IdP name claim, which
	// they can set, so clients must mark it (docs/design/auth.md §2.1). It is
	// decided when the event is written, from the users.yaml loaded then.
	// omitempty: false is written as no key, so an event from before this
	// field serialises exactly as before and an export of an old room keeps
	// its hash; a missing key reads as false.
	Verified bool `json:"verified,omitempty"`
}

// AgentActor is the actor used for every event the agent emits.
var AgentActor = Actor{Kind: ActorAgent, ID: "agent", Name: "Agent", Verified: true}

// Event is one entry in a room's log. Seq starts at 1 and has no gaps
// within a room.
type Event struct {
	RoomID  string          `json:"room_id"`
	Seq     int64           `json:"seq"`
	Type    Type            `json:"type"`
	Actor   Actor           `json:"actor"`
	Payload json.RawMessage `json:"payload"`
	At      time.Time       `json:"at"`
}

// Store is the append-only event log.
type Store interface {
	// Append stores one event and assigns the next Seq for the room atomically.
	// payload is marshalled to JSON.
	Append(ctx context.Context, roomID string, typ Type, actor Actor, payload any) (Event, error)
	// Since returns events with Seq > afterSeq in order, at most limit. A room
	// that does not exist (never created, or deleted) is an error matching the
	// store's ErrNotFound; limit <= 0 is an empty page.
	Since(ctx context.Context, roomID string, afterSeq int64, limit int) ([]Event, error)
	// Subscribe replays events after afterSeq, then streams new ones live.
	// The channel closes when ctx ends, and when the room is deleted. No event
	// may be skipped or repeated.
	Subscribe(ctx context.Context, roomID string, afterSeq int64) (<-chan Event, error)
}

// Decode unmarshals an event payload into T.
func Decode[T any](e Event) (T, error) {
	var v T
	if err := json.Unmarshal(e.Payload, &v); err != nil {
		return v, fmt.Errorf("events: decode %s payload: %w", e.Type, err)
	}
	return v, nil
}

// ---- Payloads (one struct per Type) ----

// RoomCreatedPayload starts a room.
type RoomCreatedPayload struct {
	Title      string          `json:"title"`
	Severity   string          `json:"severity"` // "sev1".."sev4"
	PipelineID string          `json:"pipeline_id"`
	Alert      json.RawMessage `json:"alert,omitempty"` // raw alert from the webhook
	// AlertSource is the route that opened the room: "webhook" (dev X-User
	// route), a signed source ("alertmanager", "grafana", "pagerduty",
	// "datadog", "generic") or the built-in watcher ("watcher"; dedup key
	// "watcher:<pipeline>", no HTTP route). Empty for rooms not opened by an alert.
	AlertSource string `json:"alert_source,omitempty"`
	// DedupKey is the exact index key the room was opened under, so a restart
	// rebuilds the same alert index instead of re-deriving it from Alert.
	DedupKey string `json:"dedup_key,omitempty"`
	// Shadow is true when the room's pipeline has shadow enabled in owners.yaml
	// at open time: the agent runs silently, proposes nothing, and its output
	// is revealed after the room closes. Frozen here so a restart keeps it.
	// Never set by CreateRoom (POST /api/rooms).
	Shadow bool `json:"shadow,omitempty"`
	// Mode is RoomModeSetup for a room POST /api/setup opened: the agent runs
	// in setup mode (agent.ModeSetup) and Alert holds the SetupRequest.
	// Empty (omitted) for every incident room, so events from before the U
	// wave serialise exactly as before. Never set together with Shadow. (U)
	Mode string `json:"mode,omitempty"`
	// Origin is RoomOriginForecast for a room a forecast signal opened: it
	// opens quietly, sev4, with the trend summary as its first system message
	// and no agent run (zero tokens) until the first steer, the first @agent
	// comment or a room.escalated. Empty (omitted) for every other room, so
	// events from before the X wave serialise exactly as before. (X3)
	Origin string `json:"origin,omitempty"`
}

// RoomModeSetup is RoomCreatedPayload.Mode of a setup room. (U)
const RoomModeSetup = "setup"

// RoomOriginForecast is RoomCreatedPayload.Origin of a room a forecast
// signal opened. (X3)
const RoomOriginForecast = "forecast"

// RoomEscalatedPayload raises an open room's severity. Severity is the new
// level, always more severe than From; Signal is the watcher signal that
// caused it (e.g. "slot_wal"). Escalating a forecast room also starts its
// agent when none has run yet. (X3)
type RoomEscalatedPayload struct {
	Severity string `json:"severity"` // "sev1".."sev3"
	From     string `json:"from"`
	Signal   string `json:"signal"`
}

// ---- X wave: watcher signals (X1 drift, X3 forecast and config_drift) ----
//
// The watcher (internal/watch) puts one flat object per firing signal in the
// "signals" list of its evidence JSON (room.created.alert and its joins),
// like every older signal: "signal" and "severity" first, then the signal's
// own keys. DriftSignal, ForecastSignal and ConfigDriftSignal are those
// objects for the X-wave signals, so the watcher, the room service, the
// snapshot, the web and the agent's evidence renderer
// (agent.AlertEvidenceText) agree on one set of keys. They are not event
// types. Like all watcher evidence they carry names, counts, hashes and times
// only: never a key value, a row value, record content or a connector config
// value. The names are also the owners.yaml watch.signals names and
// clients.WatchSignal.Signal values.

// X-wave watcher signal names (the evidence's signals[].signal).
const (
	// SignalCountDrift (X1): source and sink row counts over the same closed
	// window (shifted by the sink's lag) differ by more than the tolerance.
	SignalCountDrift = "count_drift"
	// SignalRowDrift (X1): sampled source keys are missing on the sink after
	// the lag, or a timestamp column differs.
	SignalRowDrift = "row_drift"
	// SignalDupKeys (X1): the CDC topic holds the same (key, source
	// position) more than once in the window.
	SignalDupKeys = "dup_keys"
	// SignalOrderSkew (X1): the source position goes backwards for one key
	// on the CDC topic.
	SignalOrderSkew = "order_skew"
	// SignalForecast (X3): a pipeline metric will reach its limit within the
	// forecast horizon.
	SignalForecast = "forecast"
	// SignalConfigDrift (X3): a connector's config changed outside an
	// approved fix.
	SignalConfigDrift = "config_drift"
)

// SignalSeverity is the default severity of an X-wave watcher signal: "sev2"
// for count_drift and row_drift, "sev3" for dup_keys, order_skew and
// config_drift, "sev4" for forecast; "" for any other name (the older
// signals' severities are the watcher's and owners.yaml's). It is the one
// table docs/api.md (Watcher alerts) shows.
func SignalSeverity(signal string) string {
	switch signal {
	case SignalCountDrift, SignalRowDrift:
		return "sev2"
	case SignalDupKeys, SignalOrderSkew, SignalConfigDrift:
		return "sev3"
	case SignalForecast:
		return "sev4"
	}
	return ""
}

// DriftWindow is the half-open time window [From, To) a drift check read.
type DriftWindow struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// DriftSignal is the evidence entry of one drift signal (X1). It carries
// names, counts and a window only: never a key value, a row value or record
// content. The numbers are always present (0 is evidence too); Table, Topic
// and Detail are omitted when empty. Window is an object here, while an
// older signal's "window" (dlq_growth) is a word like "15m": a reader goes by
// "signal" first.
type DriftSignal struct {
	Kind     string      `json:"signal"`          // SignalCountDrift, SignalRowDrift, SignalDupKeys or SignalOrderSkew
	Severity string      `json:"severity"`        // SignalSeverity(Kind) unless owners.yaml sets another
	Table    string      `json:"table,omitempty"` // "schema.table" of the source table (count_drift, row_drift)
	Topic    string      `json:"topic,omitempty"` // the CDC topic (dup_keys, order_skew; the table's topic otherwise when known)
	Window   DriftWindow `json:"window"`
	// Source and Sink are the row counts on each side over Window
	// (count_drift); for dup_keys and order_skew Source is the records
	// scanned and Sink is 0.
	Source int64 `json:"source"`
	Sink   int64 `json:"sink"`
	// Missing is rows (count_drift: Source - Sink when positive) or sampled
	// keys (row_drift) not on the sink; for dup_keys the duplicate events.
	Missing int `json:"missing"`
	// Mismatched is sampled keys whose timestamp columns differ (row_drift);
	// for order_skew the per-key position regressions.
	Mismatched int `json:"mismatched"`
	// Sample is the keys compared (row_drift, at most
	// clients.ReconcileMaxSample) or the distinct keys affected (dup_keys,
	// order_skew).
	Sample int `json:"sample"`
	// Detail is one line of watcher-written text (fixed wording plus names
	// and numbers, at most 200 bytes), shown as text and never as HTML.
	Detail string `json:"detail,omitempty"`
}

// ForecastSignal is the evidence entry of a forecast signal (X3): a pipeline
// metric will reach its limit within the forecast horizon, by a
// least-squares line over the last config.WatchForecastFitWindow of watch
// samples (clients.SampleReader), fired when the ETA is inside
// config.WatchForecast.Horizon and the fit's R² is at least
// config.WatchForecastMinR2. Metric is one of clients.WatchMetrics();
// Current, Limit and Slope are in that metric's unit
// (clients.WatchMetricUnit; Slope per hour). Object names the slot, topic,
// group or database the series follows when it is one object, "" for a
// pipeline total. Numbers and names only.
type ForecastSignal struct {
	Signal   string    `json:"signal"`   // SignalForecast
	Severity string    `json:"severity"` // SignalSeverity(SignalForecast), "sev4"
	Metric   string    `json:"metric"`
	Object   string    `json:"object,omitempty"`
	Current  float64   `json:"current"`
	Limit    float64   `json:"limit"`
	ETA      time.Time `json:"eta"`            // when the line reaches Limit, UTC
	Slope    float64   `json:"slope_per_hour"` // > 0: a flat or shrinking series never fires
	R2       float64   `json:"r2"`             // goodness of fit, 0..1
	Samples  int       `json:"samples"`        // points in the fit
}

// ConfigDriftSignal is the evidence entry of a config_drift signal (X3): a
// connector's config changed outside an approved fix, that is
// actions.ChangedConfigKeys between the last approved digest and the current
// one is not empty. The approved digest moves at server start, on every
// action.executed that touches the connector, and on POST
// /api/pipelines/{pid}/connectors/{name}/accept-config. OldHash and NewHash
// are actions.ConfigHash of the two redacted configs (ConfigDigest.Hash, the
// same id as a config version's hash, X5): no secret value enters them, so
// they are equal when only a secret value changed.
type ConfigDriftSignal struct {
	Signal    string `json:"signal"`   // SignalConfigDrift
	Severity  string `json:"severity"` // SignalSeverity(SignalConfigDrift), "sev3"
	Connector string `json:"connector"`
	OldHash   string `json:"old_hash"`
	NewHash   string `json:"new_hash"`
	// ChangedKeys are the NAMES of the keys added, removed or changed, sorted,
	// the first MaxChangedKeys of actions.ChangedConfigKeys. Values, old or
	// new, are never stored, logged or sent: not here, not in the room log,
	// and not in the watcher's memory beyond a keyed per-key fingerprint. A
	// secret-looking key (actions.LooksSecret) is reported as
	// SecretKeyChanged instead of its name, once however many such keys
	// changed.
	ChangedKeys []string `json:"changed_keys"`
	// KeysOmitted counts the changed names past MaxChangedKeys.
	KeysOmitted int `json:"keys_omitted,omitempty"`
}

// SecretKeyChanged stands for the name of a changed credential key
// (actions.LooksSecret): in ConfigDriftSignal.ChangedKeys and in
// clients.ConnectorConfigVersion.ChangedKeys (X5). It is the only placeholder
// for it; not even the key's name is shown. (X3)
const SecretKeyChanged = "<secret key changed>"

// MaxChangedKeys caps ConfigDriftSignal.ChangedKeys. A connector config has a
// few dozen keys, so more than this means the config was replaced. (X3)
const MaxChangedKeys = 50

// ---- X7: retention evidence in config_risk (watcher X7d) ----
//
// Retention findings are entries of the existing config_risk signal's
// "risks" list (internal/watch), next to the connector risks
// ({connector, code, sev, text, table?}); they are not a new signal. Each
// compares a pipeline topic's (or, when a clients.TableRetentionReader
// answers, a sink table's) configured retention with its owners.yaml
// declared_retention, in both directions: keeping data shorter than declared
// loses it, keeping it longer than declared breaks the retention promise.
// A topic with no declaration is never judged; an unreadable topic config
// (no DescribeConfigs ACL) gives one INFO log and no finding.

// Retention risk codes (RetentionRisk.Code).
const (
	// RiskRetentionShorter: the configured retention is shorter than
	// declared ("pg.public.orders retains 1d; declared 7d").
	RiskRetentionShorter = "retention_shorter_than_declared"
	// RiskRetentionLonger: the configured retention is longer than declared,
	// infinite (retention.ms=-1) included.
	RiskRetentionLonger = "retention_longer_than_declared"
	// RiskCompactCDCTopic: a CDC topic of the pipeline has cleanup.policy
	// compact without delete, so delete events are compacted away after
	// delete.retention.ms and a re-read cannot replay them.
	RiskCompactCDCTopic = "compact_cdc_topic"
)

// RetentionRiskSeverity is the severity of every retention finding: it fires
// config_risk (it is listed in /api/fleet signals_firing and logged) but opens
// no room on its own, whatever config_risk's action; when another signal of
// the pipeline opens or joins a room, the finding rides along in the
// evidence.
const RetentionRiskSeverity = "sev4"

// RetentionRisk is one retention entry of config_risk's "risks" list (X7d).
// Configured and Declared are topology.FormatRetention text ("1d", "36h",
// "infinite"); Configured is "" for RiskCompactCDCTopic. Text is a fixed
// sentence per code with names and these durations filled in, at most 200
// bytes, rendered as text. Names, durations and the policy word only: never
// a record, a row or any other config value.
type RetentionRisk struct {
	Code       string `json:"code"`            // RiskRetentionShorter, RiskRetentionLonger or RiskCompactCDCTopic
	Sev        string `json:"sev"`             // RetentionRiskSeverity; "sev" like the connector risks
	Topic      string `json:"topic,omitempty"` // the pipeline topic judged
	Table      string `json:"table,omitempty"` // or the sink table ("schema.table")
	Configured string `json:"configured,omitempty"`
	Declared   string `json:"declared,omitempty"`
	// RetentionBytes is the topic's retention.bytes when set (> 0): a size
	// cap can cut retention short of retention.ms, so the finding names it.
	RetentionBytes int64  `json:"retention_bytes,omitempty"`
	CleanupPolicy  string `json:"cleanup_policy,omitempty"` // the topic's cleanup.policy word(s)
	Text           string `json:"text"`
}

// RoomClosedPayload ends a room. A room closed by the open-room limit
// (PIPEROOM_MAX_OPEN_DAYS) is closed by the system actor with the resolution
// "retention: open longer than N days".
type RoomClosedPayload struct {
	Resolution string `json:"resolution"`
}

// RoomRetentionWarningPayload says the open-room limit will close the room at
// CloseAt. Written by the system actor once per deadline, 24 h before it. A
// room that is past its deadline when the limit first sees it (the limit was
// just turned on or lowered) is warned at once with CloseAt 24 h later: no
// room is closed less than 24 h after its warning. An extension moves the
// deadline and gets its own warning.
type RoomRetentionWarningPayload struct {
	CloseAt  time.Time `json:"close_at"`
	OpenDays int       `json:"open_days"` // days from opened_at to CloseAt
}

// RoomExtendedPayload records the room's one extension of the open-room limit
// (POST /api/rooms/{id}/extend, driver only). CloseAt is the new deadline:
// the close time the room showed (snapshot auto_close_at) plus Days.
type RoomExtendedPayload struct {
	Days    int       `json:"days"`
	CloseAt time.Time `json:"close_at"`
}

// RoomLogCapWarningPayload says the room log reached 90% of a cap
// (PIPEROOM_ROOM_MAX_EVENTS or PIPEROOM_ROOM_MAX_BYTES, bytes counted on the
// stored payloads). Written once per room by the system actor. At 100% agent
// events are refused and the agent stops (agent.status "log_full").
type RoomLogCapWarningPayload struct {
	Events    int64 `json:"events"`
	MaxEvents int64 `json:"max_events"`
	Bytes     int64 `json:"bytes"`
	MaxBytes  int64 `json:"max_bytes"`
}

// RoomDeletedPayload says an admin deleted the room (DELETE /api/rooms/{id}).
// It is never stored: the log is deleted with the room. The WebSocket sends it
// as the last "event" frame of each open connection to the room, with Seq =
// the room's last seq + 1, the admin as actor, and then closes with the close
// reason "room deleted".
type RoomDeletedPayload struct {
	Reason string `json:"reason"`
}

// AgentStatusPayload reports the agent's state. "log_full" is written by the
// system actor when the room log reached its cap: the agent has stopped and
// cannot be woken in this room.
type AgentStatusPayload struct {
	State  string `json:"state"` // "working" | "idle" | "waiting_approval" | "error" | "done" | "queued" | "log_full"
	Detail string `json:"detail,omitempty"`
}

// AgentStepStartPayload opens one visible investigation step.
type AgentStepStartPayload struct {
	StepID string `json:"step_id"`
	Title  string `json:"title"`
}

// AgentToolCallPayload records a tool call inside a step.
type AgentToolCallPayload struct {
	StepID    string          `json:"step_id"`
	ToolUseID string          `json:"tool_use_id"`
	Tool      string          `json:"tool"`
	Input     json.RawMessage `json:"input"`
}

// AgentToolResultPayload records a tool result inside a step. A Data larger
// than PIPEROOM_EVENT_MAX_DATA_BYTES is stored as a TruncatedData object
// instead, and Truncated is then true. Truncated therefore means "something
// in this result was cut": the Summary (tools.MaxSummaryBytes), the Data, or
// both. A UI tells cut Data apart by its shape: an object with "truncated":
// true, "original_bytes" and "preview" (TruncatedData).
type AgentToolResultPayload struct {
	StepID    string          `json:"step_id"`
	ToolUseID string          `json:"tool_use_id"`
	Tool      string          `json:"tool"`
	Summary   string          `json:"summary"`
	Data      json.RawMessage `json:"data,omitempty"`
	IsError   bool            `json:"is_error"`
	Truncated bool            `json:"truncated,omitempty"` // Summary or Data (or both) was cut; see above
	TookMS    int64           `json:"took_ms"`
	// Provenance is the list of reads behind Summary (the receipt), redacted
	// and capped by the agent (tools.MaxProvenanceCalls). It is outside Data:
	// a cut Data keeps its receipt. Empty for room tools and replayed tools. (F3)
	Provenance []ToolCall `json:"provenance,omitempty"`
	// Rejected is set when the agent refused the call before running it: one
	// of the ToolRejected* values. IsError is then true and Summary says why.
	// Empty for a tool that ran, even one that failed. Counted as
	// invalid_tool_calls by the evals gate (V0; set by A4 in V1t,
	// docs/design/model-providers.md 5.1).
	Rejected string `json:"rejected,omitempty"`
}

// Values of AgentToolResultPayload.Rejected (V0).
const (
	ToolRejectedUnknownTool  = "unknown_tool"  // the call named no registered tool
	ToolRejectedInvalidJSON  = "invalid_json"  // the arguments are not valid JSON
	ToolRejectedInvalidInput = "invalid_input" // the arguments failed schema or input validation
)

// ToolCall mirrors tools.Call (same fields and JSON tags) so this package stays
// a leaf. The agent converts and applies tools' caps before emitting. (F3)
type ToolCall struct {
	Kind           string    `json:"kind"`
	Target         string    `json:"target"`
	Text           string    `json:"text"`
	ParamsRedacted []string  `json:"params_redacted,omitempty"`
	At             time.Time `json:"at"`
	TookMS         int64     `json:"took_ms"`
}

// TruncatedData replaces a tool_result.data over PIPEROOM_EVENT_MAX_DATA_BYTES.
// Preview is the start of the original JSON text, cut at a UTF-8 boundary so
// the whole object stays under the limit; it is not valid JSON on its own.
type TruncatedData struct {
	Truncated     bool   `json:"truncated"` // always true
	OriginalBytes int64  `json:"original_bytes"`
	Preview       string `json:"preview"`
}

// TruncateData is the one implementation of the Data cap for the room
// (internal/room, A5 follow-up) and the agent (R11 #136), so both produce the
// same TruncatedData bytes. It returns raw unchanged and false when len(raw)
// <= max or max <= 0. Otherwise it returns the encoded TruncatedData (the
// start of raw as Preview, cut at a UTF-8 boundary; JSON escaping can grow
// the preview, so it is halved until the whole object fits in max bytes) and
// true. An empty preview is returned when nothing fits.
func TruncateData(raw json.RawMessage, max int) (json.RawMessage, bool) {
	if max <= 0 || len(raw) <= max {
		return raw, false
	}
	td := TruncatedData{Truncated: true, OriginalBytes: int64(len(raw))}
	n := max
	for {
		td.Preview = cutUTF8(string(raw), n)
		out, err := json.Marshal(td)
		if err != nil {
			// Unreachable: TruncatedData holds only a bool, an int64 and a
			// string, which encoding/json always encodes (invalid UTF-8 in
			// the preview becomes U+FFFD).
			return json.RawMessage(`{"truncated":true,"original_bytes":` + strconv.Itoa(len(raw)) + `,"preview":""}`), true
		}
		if len(out) <= max || n == 0 {
			return json.RawMessage(out), true
		}
		n /= 2
	}
}

// cutUTF8 returns at most n bytes of s, never splitting a rune.
func cutUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// AgentMessagePayload is plain text the agent says to the room.
// ReplyTo is set when it answers a comment.
type AgentMessagePayload struct {
	Text    string `json:"text"`
	StepID  string `json:"step_id,omitempty"`
	ReplyTo string `json:"reply_to,omitempty"` // CommentID
}

// AgentHypothesisPayload is the agent's current best guess.
type AgentHypothesisPayload struct {
	Text         string   `json:"text"`
	BasedOnSteps []string `json:"based_on_steps"`
	Confidence   string   `json:"confidence"` // "low" | "medium" | "high"
	// Confirmed is always false when written by the agent; derived state
	// (snapshot and clients) sets it from hypothesis.verdict events. Kept for
	// wire compatibility. (F9)
	Confirmed bool `json:"confirmed"`
	// UnbackedTerms are identifier-like names and numbers in Text that none of
	// the cited steps' summaries contain (agent grounding, F3). The UI marks
	// them; they never block the hypothesis. At most 20, each ≤ 80 bytes.
	UnbackedTerms []string `json:"unbacked_terms,omitempty"`
	// X4: Scope says where the cause is (HypothesisScope); omitted on the
	// wire when unknown, and an event written before X4 decodes as
	// ScopeUnknown. Team is an owners.yaml teams[].id, set only with
	// ScopeUpstreamApp or ScopeVendor, else "". ChangeID is the
	// clients.PipelineChange.ID the scope rests on (the deploy, migration or
	// vendor incident the agent cites), 0 when none. (X4)
	Scope    HypothesisScope `json:"scope,omitzero"`
	Team     string          `json:"team,omitempty"`
	ChangeID int64           `json:"change_id,omitempty"`
}

// UnmarshalJSON defaults Scope to ScopeUnknown, so a hypothesis written
// before X4 (no "scope" key) replays as unknown, not as "". (X4)
func (p *AgentHypothesisPayload) UnmarshalJSON(b []byte) error {
	type plain AgentHypothesisPayload
	v := plain{Scope: ScopeUnknown}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*p = AgentHypothesisPayload(v)
	return nil
}

// ---- X4: hypothesis scope and team pages ----

// HypothesisScope is where the agent places the cause of the incident. Only
// the four constants below are ever held after decoding: an absent, null,
// empty or unrecognised value reads as ScopeUnknown, so an old or a newer
// writer never breaks replay. (X4)
type HypothesisScope string

const (
	// ScopePipeline: the cause is in the pipeline (source DB settings,
	// connector, registry, Kafka, consumer, sink); a pipeline fix is the root
	// fix.
	ScopePipeline HypothesisScope = "pipeline"
	// ScopeUpstreamApp: an application change outside the pipeline (a deploy
	// or a migration) caused it. The agent may propose only a mitigation,
	// labelled as such, never a pipeline fix as the root fix.
	ScopeUpstreamApp HypothesisScope = "upstream_app"
	// ScopeVendor: a vendor's incident (managed Kafka, Connect, registry).
	ScopeVendor HypothesisScope = "vendor"
	// ScopeUnknown: not established, or written before X4.
	ScopeUnknown HypothesisScope = "unknown"
)

// Normalize returns s when it is one of the three known scopes, else
// ScopeUnknown.
func (s HypothesisScope) Normalize() HypothesisScope {
	switch s {
	case ScopePipeline, ScopeUpstreamApp, ScopeVendor:
		return s
	}
	return ScopeUnknown
}

// IsZero makes omitzero drop an unknown scope, so a hypothesis without a
// scope marshals byte for byte as before X4 (export hashes).
func (s HypothesisScope) IsZero() bool { return s.Normalize() == ScopeUnknown }

// MarshalJSON writes the normalised value.
func (s HypothesisScope) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(s.Normalize()))
}

// UnmarshalJSON accepts any JSON string or null; a value that is not one of
// the scopes becomes ScopeUnknown. A non-string is an error.
func (s *HypothesisScope) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*s = ScopeUnknown
		return nil
	}
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return fmt.Errorf("events: hypothesis scope: %w", err)
	}
	*s = HypothesisScope(v).Normalize()
	return nil
}

// TeamPagedPayload is room.team_paged. Team is the owners.yaml teams[].id;
// HypothesisSeq the seq of the agent.hypothesis the page carried; ChangeID
// that hypothesis's change_id (0 when none). Ids only: the Slack message is
// rendered from the room log when sent and never stored. (X4)
type TeamPagedPayload struct {
	Team          string `json:"team"`
	HypothesisSeq int64  `json:"hypothesis_seq"`
	ChangeID      int64  `json:"change_id,omitempty"`
}

// HypothesisVerdictPayload: Verdict is "confirmed" or "disputed"; Note is the
// person's optional reason (typed text, at most 500 characters). (F9)
type HypothesisVerdictPayload struct {
	Verdict       string `json:"verdict"`
	Note          string `json:"note,omitempty"`
	HypothesisSeq int64  `json:"hypothesis_seq"`
}

// AgentTriagePayload is the triage pass's one-line verdict. It is labelled
// "triage, unverified" wherever it is shown and never carries a proposal.
// Status "ok" has a Headline; "failed" has none and Reason says why (fixed
// codes: "timeout", "model_error", "budget", "refused", "no_tools", "off"). (F6)
type AgentTriagePayload struct {
	Status         string   `json:"status"`                    // "ok" | "failed"
	Reason         string   `json:"reason,omitempty"`          // failed only
	Headline       string   `json:"headline,omitempty"`        // one sentence, ≤ 200 chars, plain text
	SuspectedLayer string   `json:"suspected_layer,omitempty"` // one of Layers, or "" when unsure
	Confidence     string   `json:"confidence,omitempty"`      // "low" | "medium" | "high"
	EvidenceRefs   []string `json:"evidence_refs"`             // step ids of the pass's reads; never null
	NextCheck      string   `json:"next_check,omitempty"`      // the one read the full run should do first, ≤ 120 chars
	Model          string   `json:"model"`
	TookMS         int64    `json:"took_ms"`
}

// Roles of a model call in AgentUsagePayload.Role.
const (
	UsageRoleTriage     = "triage"     // an investigation turn on PIPEROOM_TRIAGE_MODEL
	UsageRoleMain       = "main"       // an investigation turn on the main model (Options.Model)
	UsageRoleHandoff    = "handoff"    // the hand-off summary request
	UsageRoleCompaction = "compaction" // the server-side compaction pass inside a triage or main request
	// UsageRoleJudge is a model call that grades agent output (an LLM judge of
	// a hand-off or an eval run). Reserved: nothing emits it until the judges
	// of #93 and the evals are wired to usage and spend (docs/design/cost.md 2).
	UsageRoleJudge = "judge"
	// UsageRolePostmortem is the closing-summary request of a closed room (F2c).
	UsageRolePostmortem = "postmortem"
	// UsageRoleFirstLook is a model call of the triage pass (agent.triage, F6).
	// Not to be confused with UsageRoleTriage, a turn on PIPEROOM_TRIAGE_MODEL.
	UsageRoleFirstLook = "first_look"
)

// AgentUsagePayload is the cost of one model call (docs/design/cost.md). The
// agent emits one per request it sends, failed requests included; a request
// refused by a budget is never sent and gets none. When a request ran
// server-side compaction, a second event with Role "compaction" carries the
// compaction pass, whose tokens the API reports outside the request's own
// usage fields; its LatencyMS is 0 because its time is part of the request's.
//
// It carries counts only: never prompt text, tool input or output, model text
// or error text. Model is the model id the request named. CostUSD is
// estimated from the PIPEROOM_MODEL_PRICES table; it is omitted (nil) when no
// table is set or the table has no entry for Model, and it is never guessed.
type AgentUsagePayload struct {
	Model                    string   `json:"model"`
	Role                     string   `json:"role"` // see UsageRole*
	InputTokens              int64    `json:"input_tokens"`
	CacheCreationInputTokens int64    `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64    `json:"cache_read_input_tokens"`
	OutputTokens             int64    `json:"output_tokens"`
	LatencyMS                int64    `json:"latency_ms"`
	CostUSD                  *float64 `json:"cost_usd,omitempty"`
	// Provider is the PIPEROOM_MODEL_PROVIDER value (llm.Provider*) of the
	// endpoint that served the call; omitted on events written before V0 (or
	// before the agent sets it), which all came from a Claude provider. (V0)
	Provider string `json:"provider,omitempty"`
	// Estimated is true when the provider reported no usage and the counts
	// were estimated from request and response sizes; they are charged like
	// real usage (docs/design/model-providers.md 8.3). (V0)
	Estimated bool `json:"estimated,omitempty"`
}

// CommentAddedPayload is a human comment, optionally anchored to a step.
type CommentAddedPayload struct {
	CommentID     string `json:"comment_id"`
	StepID        string `json:"step_id,omitempty"`
	Text          string `json:"text"`
	MentionsAgent bool   `json:"mentions_agent"`
	ClientID      string `json:"client_id,omitempty"` // the request's idempotency key (docs/api.md, Idempotent posts; #459)
}

// SteerSuggestedPayload is a non-driver's suggestion waiting for the driver.
type SteerSuggestedPayload struct {
	SuggestionID string `json:"suggestion_id"`
	Text         string `json:"text"`
	ClientID     string `json:"client_id,omitempty"` // the steer request's idempotency key (#459)
}

// SteerSentPayload is an instruction delivered to the agent by the driver.
// When the driver forwards a suggestion, SuggestionID names it and From is
// the person who wrote it; the event's Actor stays the forwarding driver.
// The agent treats a forwarded suggestion's text as that person's words
// (untrusted data), not as the driver's instruction.
type SteerSentPayload struct {
	SuggestionID string `json:"suggestion_id,omitempty"` // set when forwarding a suggestion
	From         *Actor `json:"from,omitempty"`          // the suggester; set only with SuggestionID
	Text         string `json:"text"`
	ClientID     string `json:"client_id,omitempty"` // the steer request's idempotency key (#459); none on a forward: the forward is the driver's action, not the suggester's request
}

// SteerDeclinedPayload closes a pending suggestion (steer.suggested) without
// sending it to the agent. Only the driver may decline (403 not_driver), only a
// suggestion that is still pending (a sent or declined one answers
// 409 already_decided, exactly like a second send), and at most once: the
// suggestion then leaves snapshot.pending_suggestions and can never be sent.
// Reason is the driver's optional note to the person who suggested it; text a
// person types, so it follows the NUL / invalid UTF-8 rule of docs/api.md and
// is at most 500 characters. The agent never sees a declined suggestion.
type SteerDeclinedPayload struct {
	SuggestionID string `json:"suggestion_id"`
	Reason       string `json:"reason,omitempty"`
}

// ControlRequestedPayload asks the driver for control.
type ControlRequestedPayload struct{}

// ControlLeaseExpiredPayload is written by the system actor once per lease:
// when Driver's lease (LeaseUntil, the value of the control.granted that set
// it) has passed and nobody renewed it or took control. It is the room's
// "no active driver" signal for clients and for paging (Slack). It does not
// move the driver role: as before (docs/api.md, Driver lease), the driver's
// next action renews the lease (a control.granted to the same driver, which
// clears the signal), and anyone may now take control with control/grant.
// One event per (room, LeaseUntil): with several replicas only the replica
// holding the room's lease writes it, and a restart does not repeat it. Not
// written for a closed room or a room that never had a driver.
type ControlLeaseExpiredPayload struct {
	Driver     Actor     `json:"driver"`
	LeaseUntil time.Time `json:"lease_until"`
}

// ControlGrantedPayload moves the driver role. Reason is empty for a grant a
// person made (control/grant, hand-off accept, a lease renewal) and
// ControlReasonSoleMember when the server made the room's first member the
// driver without a request (P2). omitempty: events before P2 hash as before.
type ControlGrantedPayload struct {
	To         Actor     `json:"to"`
	LeaseUntil time.Time `json:"lease_until"`
	Reason     string    `json:"reason,omitempty"`
}

// ControlReasonSoleMember is ControlGrantedPayload.Reason when the first
// member to join a driverless alert room (not a viewer, not a shadow room,
// no control.requested pending) became its driver. System actor. (P2)
const ControlReasonSoleMember = "sole_member"

// ProposalStep is one write action in a proposed fix.
type ProposalStep struct {
	Action      string          `json:"action"` // an actions.Action name
	Layer       string          `json:"layer"`  // see Layers
	Params      json.RawMessage `json:"params"`
	Description string          `json:"description"`
	// Verify says what the agent will check after this step runs, in one
	// sentence ("task 0 RUNNING within 60 s"). Optional; agent text, ≤ 200
	// chars after the room's sanitizing. Part of the hashed proposal; omitted
	// when empty, so a proposal without it hashes as before. (F4)
	Verify string `json:"verify,omitempty"`
}

// PreviewResult is what one write action would do, read before approval.
// Current and Expected are short human sentences; Current names the read it
// came from. Downstream is filled by the room service from the topology (node
// ids, data edges only, ≤ 12); actions leave it nil. (F4)
type PreviewResult struct {
	Target     string        `json:"target"`
	Current    string        `json:"current"`
	Expected   string        `json:"expected"`
	Downstream []string      `json:"downstream,omitempty"`
	Reversible bool          `json:"reversible"`
	Rollback   string        `json:"rollback,omitempty"` // how to undo, as an action and params, when Reversible
	DryRun     *DryRunResult `json:"dry_run,omitempty"`  // nil when the target system has no dry-run
	// X2: a recovery action says how much Execute would re-emit ("will
	// re-emit 412 rows"), read from the target, never estimated by the
	// model. Count is nil for every other action (and omitted, so a fix
	// preview marshals as before); Unit is RecoveryUnitRows or
	// RecoveryUnitRecords whenever Count is set.
	Count *int64 `json:"count,omitempty"`
	Unit  string `json:"unit,omitempty"`
	// X6: a performance action (connect_tune_throughput) lists every key it
	// would change, with the value now and the value it sets, read from the
	// connector's config by Preview; Current is the rollback value the card
	// offers when the verdict is PerformanceWorse. Nil for every other
	// action (and omitted, so other previews marshal as before).
	Tuning []TuningChange `json:"tuning,omitempty"`
}

// TuningChange is one allow-listed key of a performance step
// (actions.TunableKeys): the key, its value now and the value the step sets.
// Values are base-10 integers as Connect stores them, never another key's
// value; Current is "" when the key is unset (Connect's default applies; the
// card says "unset"). (X6)
type TuningChange struct {
	Key      string `json:"key"`
	Current  string `json:"current"`
	Proposed string `json:"proposed"`
}

// DryRunResult is the target system's own answer to "would this be accepted"
// (Registry compatibility check, Connect config validate). Skipped is set,
// with OK false, when the check exists but was not run (the reason says why). (F4)
type DryRunResult struct {
	OK      bool   `json:"ok"`
	Source  string `json:"source"`            // "registry compatibility check" | "connect config validate"
	Text    string `json:"text"`              // the system's messages, redacted, ≤ 2 KiB
	Skipped string `json:"skipped,omitempty"` // why the check was not run
}

// StepPreview is one step's preview or the reason there is none. (F4)
type StepPreview struct {
	StepIndex int            `json:"step_index"`
	Preview   *PreviewResult `json:"preview,omitempty"`
	Error     string         `json:"error,omitempty"` // redacted, ≤ 500 bytes
}

// ProposalPreviewedPayload carries every step's preview, in step order. (F4)
type ProposalPreviewedPayload struct {
	ProposalID string        `json:"proposal_id"`
	Steps      []StepPreview `json:"steps"`
	At         time.Time     `json:"at"`
}

// ---- X8: staging (try a proposal's steps on a staging stack first) ----
//
// A pipeline may declare a staging stack in owners.yaml (staging:
// {connections: ...}, docs/api.md "X8 staging"). The driver can then try a
// proposal's steps there before anyone approves them: the room service calls
// each step's actions.Stager, which applies the same change to the
// same-named object on staging, watches it, and puts staging back. The
// result is evidence for the approvers, like the preview; it never changes
// what the executor does in production. Everything here is names, states,
// counts, times and redacted traces: never a record value, a config value, a
// staging URL or a credential.

// StageResult is what staging said about one step (actions.Stager). Applied
// is true when the change was written to staging; Healthy is true only when
// it was applied and staging stayed healthy for the whole observation window
// (actions.StageObserveWindow), so Healthy implies Applied. Both are always
// present on the wire: false is evidence too. Detail is one redacted text of
// at most MaxStageDetailBytes: why it was not applied (StageNoStaging,
// "connector not present in staging"), the failed task's trace, or what
// healthy means here ("RUNNING, 3 of 3 tasks RUNNING for 60 s"). DryRun is
// staging's own pre-check where the action makes one (the Registry
// compatibility check of a schema step); a refusal there means nothing was
// written. Leftover says what the stage could not put back on staging (a
// registered schema version, a failed restore), at most MaxStageLeftoverBytes;
// empty when staging is as it was. DurationMS is the stage's wall time,
// apply to verdict, restore excluded. (X8)
type StageResult struct {
	Target     string        `json:"target,omitempty"` // the staging object, e.g. "connector orders-src"; a name, never a URL
	Applied    bool          `json:"applied"`
	Healthy    bool          `json:"healthy"`
	Detail     string        `json:"detail,omitempty"`
	DryRun     *DryRunResult `json:"dry_run,omitempty"`
	Leftover   string        `json:"leftover,omitempty"`
	DurationMS int64         `json:"duration_ms"`
}

// Caps of a StageResult's text, enforced by actions.CleanStageResult before
// the room service writes proposal.staged. (X8)
const (
	MaxStageTargetBytes   = 200
	MaxStageDetailBytes   = 2048
	MaxStageLeftoverBytes = 500
	MaxStageErrorBytes    = 500 // StepStage.Error, like StepPreview.Error
)

// StageNoStaging is StageResult.Detail when the action has no staging client
// to write through (the pipeline declares no staging:, the staging block has
// no client the action needs, or write mode is off): Applied false, nil
// error. (X8)
const StageNoStaging = "no staging declared"

// Stage outcomes, StageResult.Outcome: the three states a client shows. (X8)
const (
	// StageHealthy: applied and healthy for the whole window.
	StageHealthy = "healthy"
	// StageFailed: applied and unhealthy, or refused by staging's own
	// pre-check (DryRun ran and said no), so the change would fail.
	StageFailed = "failed"
	// StageNotApplied: nothing was tried (no staging, the object is not on
	// staging, a pre-check that was skipped); Detail says why.
	StageNotApplied = "not_applied"
)

// Outcome is the one rule that turns a StageResult into StageHealthy,
// StageFailed or StageNotApplied, shared by the room service, the agent, the
// Slack mirror and the web.
func (r StageResult) Outcome() string {
	switch {
	case r.Applied && r.Healthy:
		return StageHealthy
	case r.Applied:
		return StageFailed
	case r.DryRun != nil && !r.DryRun.OK && r.DryRun.Skipped == "":
		return StageFailed
	default:
		return StageNotApplied
	}
}

// ProposalStageStartedPayload is proposal.stage_started (actor: the driver).
// ParamsHash is the proposal's params_hash when the stage started; Steps are
// the indexes of the steps being staged, in order: every step whose action
// implements actions.Stager (never empty: a proposal with none answers 409
// and writes nothing). ExpiresAt is actions.StageExpiry of the start and
// len(Steps): after it, a stage with no proposal.staged is lost (the server
// stopped) and a new one may start. Times are UTC. (X8)
type ProposalStageStartedPayload struct {
	ProposalID string    `json:"proposal_id"`
	ParamsHash string    `json:"params_hash"`
	Steps      []int     `json:"steps"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// StepStage is one staged step: its result, or the error that stopped Stage
// (staging unreachable, ctx ended before a verdict), redacted, at most
// MaxStageErrorBytes. Exactly one of Result and Error is set. (X8)
type StepStage struct {
	StepIndex int          `json:"step_index"`
	Result    *StageResult `json:"result,omitempty"`
	Error     string       `json:"error,omitempty"`
}

// ProposalStagedPayload is proposal.staged (system actor): one entry per
// index of the matching proposal.stage_started, in the same order. Each step
// is staged on its own against staging as it was (each Stage restores before
// the next starts), and a failed step does not stop the next. ParamsHash is
// copied from proposal.stage_started: a stage counts for a proposal only while
// it equals the proposal's current params_hash. (X8)
type ProposalStagedPayload struct {
	ProposalID string      `json:"proposal_id"`
	ParamsHash string      `json:"params_hash"`
	Steps      []StepStage `json:"steps"`
	At         time.Time   `json:"at"`
}

// StepHealthy reports whether step index i was staged with outcome
// StageHealthy. A step that is not in Steps, or has an Error, is not.
// owners.yaml require_staging: needs it for each step whose action it names.
func (p ProposalStagedPayload) StepHealthy(i int) bool {
	for _, s := range p.Steps {
		if s.StepIndex == i {
			return s.Error == "" && s.Result != nil && s.Result.Outcome() == StageHealthy
		}
	}
	return false
}

// Healthy reports whether at least one step was staged and every step in
// Steps is StepHealthy: the stage's one-word verdict.
func (p ProposalStagedPayload) Healthy() bool {
	if len(p.Steps) == 0 {
		return false
	}
	for _, s := range p.Steps {
		if !p.StepHealthy(s.StepIndex) {
			return false
		}
	}
	return true
}

// VerificationCheck is one read-tool check of a layer after a fix. (F4)
type VerificationCheck struct {
	Layer  string `json:"layer"` // one of Layers
	Name   string `json:"name"`  // the read tool, e.g. "connect_status"
	OK     bool   `json:"ok"`
	Detail string `json:"detail"` // the tool summary line, clipped (agent.clipLine)
}

// SinkCheck is one warehouse table in the data-flow check. (F4)
type SinkCheck struct {
	Table    string    `json:"table"`
	Newest   time.Time `json:"newest,omitzero"`   // newest row seen; zero when unread
	Baseline time.Time `json:"baseline,omitzero"` // proposal-time newest row; zero when unknown
	Passed   bool      `json:"passed"`
	PassedBy string    `json:"passed_by,omitempty"` // "rows since proposal" | "rows during check"
}

// DataFlowCheck says whether new rows reached every sink after the fix.
// Checked is false when the check could not run (Detail says why). (F4)
type DataFlowCheck struct {
	Checked bool        `json:"checked"`
	Fresh   bool        `json:"fresh"`
	Sinks   []SinkCheck `json:"sinks,omitempty"`
	Detail  string      `json:"detail,omitempty"`
}

// AgentVerificationPayload is the verdict on one proposal's fix. Repeat is
// true for a repeated last-step result: no check ran again, Checks is empty
// and Recovered repeats the first verdict. (F4)
type AgentVerificationPayload struct {
	ProposalID string              `json:"proposal_id"`
	Checks     []VerificationCheck `json:"checks"`
	DataFlow   *DataFlowCheck      `json:"data_flow,omitempty"`
	Recovered  bool                `json:"recovered"`
	Verdict    string              `json:"verdict"` // one line, e.g. "recovered: every layer check passed and data is flowing again"
	Repeat     bool                `json:"repeat,omitempty"`
	// X2: set only for a proposal of class recovery (ProposalCreatedPayload.Class),
	// from the action's Verify polls; absent on every other verification, which
	// marshals as before. Outcome is one of the Recovery* outcomes below.
	// Before is the gap the action set out to repair and After what is still
	// missing at the last poll (counts only, never row values); pointers so a
	// recovered After of 0 is kept and "not measured" (unverifiable) is
	// absent. Recovered is true only when Outcome is RecoveryRecovered.
	Outcome string `json:"outcome,omitempty"`
	Before  *int64 `json:"before,omitempty"`
	After   *int64 `json:"after,omitempty"`
	Unit    string `json:"unit,omitempty"` // RecoveryUnitRows | RecoveryUnitRecords, with Before/After
	// X6: a proposal of class performance sets Outcome to one of the
	// Performance* outcomes, Before and After to the step's throughput in
	// the window before and after it ran, and Unit to
	// PerformanceUnitRecordsPerMinute (actions.Verifier). LagSlopeBefore and
	// LagSlopeAfter are the consumer lag's change over the same windows, in
	// records per minute (positive: lag grows), absent when not measured (a
	// source connector, no lag history). Recovered keeps its F4 meaning on a
	// performance proposal (every layer check passed: a tuning restarts
	// tasks, which can fail them); the throughput verdict is Outcome. Absent
	// on every other verification, which marshals as before.
	LagSlopeBefore *int64 `json:"lag_slope_before,omitempty"`
	LagSlopeAfter  *int64 `json:"lag_slope_after,omitempty"`
}

// ---- X2: recovery actions (class, outcomes, verify progress) ----

// Action classes, ProposalCreatedPayload.Class and actions.Class. A fix
// changes a system's state (restart, config, offsets); a recovery re-emits
// data a fault lost or corrupted and must be verifiable by counts
// (actions.Verifier). The constants live here because package actions
// imports events, so one definition serves the action, the log and the API.
const (
	ActionClassFix      = "fix"
	ActionClassRecovery = "recovery"
	// ActionClassPerformance is the class of an action that tunes
	// throughput (connect_tune_throughput, X6c): it changes only
	// allow-listed keys (actions.TunableKeys) and, like a recovery, must be
	// verifiable (actions.Verifier, measuring throughput before and after it
	// ran). (X6)
	ActionClassPerformance = "performance"
)

// Recovery outcomes, AgentVerificationPayload.Outcome. The agent polls the
// action's Verify until After is 0 or its verify timeout: recovered (After
// is 0), partial (0 < After < Before at the timeout), not_recovered (After ≥
// Before at the timeout), unverifiable (Verify could not count, e.g. the
// reader answered clients.ErrNotSupported; Before/After absent, and the
// proposal is never shown as recovered).
const (
	RecoveryRecovered    = "recovered"
	RecoveryPartial      = "partial"
	RecoveryNotRecovered = "not_recovered"
	RecoveryUnverifiable = "unverifiable"
)

// Units of a recovery count: source rows (pg_resnapshot_rows) or Kafka
// records (kafka_replay_range).
const (
	RecoveryUnitRows    = "rows"
	RecoveryUnitRecords = "records"
)

// RecoveryOutcomeFor is the outcome of a finished poll loop from its last
// counts: RecoveryRecovered when after ≤ 0, RecoveryPartial when after <
// before, RecoveryNotRecovered otherwise. It is the one rule the agent, the
// post-mortem and the web share; a loop that could not count reports
// RecoveryUnverifiable instead of calling it.
func RecoveryOutcomeFor(before, after int64) string {
	switch {
	case after <= 0:
		return RecoveryRecovered
	case after < before:
		return RecoveryPartial
	default:
		return RecoveryNotRecovered
	}
}

// AgentRecoveryCheckPayload is one Verify poll (agent.recovery_check, agent
// actor). Poll counts from 1; MaxPolls is the most the agent will make in
// this verification (its timeout and backoff), so Poll ≤ MaxPolls. Counts
// only, never row values or keys; StepIndex is the recovery step polled. (X2)
type AgentRecoveryCheckPayload struct {
	ProposalID string `json:"proposal_id"`
	StepIndex  int    `json:"step_index"`
	Poll       int    `json:"poll"`
	MaxPolls   int    `json:"max_polls"`
	Before     int64  `json:"before"`
	After      int64  `json:"after"`
	Unit       string `json:"unit"` // RecoveryUnitRows | RecoveryUnitRecords
}

// ---- X6: performance actions (ActionClassPerformance above; throughput verdict) ----

// Performance outcomes, AgentVerificationPayload.Outcome of a performance
// proposal; PerformanceOutcomeFor is the rule. Unverifiable is the recovery
// value: Verify could not measure a window (the reader answered
// clients.ErrNotSupported, or no history covers it), and Before/After are
// then absent. (X6)
const (
	PerformanceImproved     = "improved"
	PerformanceNoChange     = "no_change"
	PerformanceWorse        = "worse"
	PerformanceUnverifiable = RecoveryUnverifiable
)

// PerformanceUnitRecordsPerMinute is the unit of a performance Verify's
// Before/After and of the lag slopes: records per minute averaged over the
// window, so a consumer under 1 record/s still compares in whole numbers.
// Clients may show it per second. (X6)
const PerformanceUnitRecordsPerMinute = "records_per_min"

// PerformanceChangePercent is the least relative change that counts: a
// throughput or lag slope that moves less is no_change. (X6)
const PerformanceChangePercent = 10

// PerformanceOutcomeFor is the verdict on a performance step from its
// throughput before and after (records per minute, >= 0) and, when both are
// measured, the lag slopes (records per minute; positive: lag grows). It is
// the one rule the agent, the post-mortem and the web share; a verification
// that could not measure reports PerformanceUnverifiable instead of calling
// it.
//
//   - throughput decides first: improved when after >= before + 10 % (any
//     after > 0 when before is 0), worse when after <= before - 10 %;
//   - only when it moved less, the lag decides: improved when a growing lag
//     (slope before > 0) has a slope at least 10 % lower (also when it now
//     shrinks), worse when a lag that was not growing (slope before <= 0)
//     now grows or a growing lag's slope rose by at least 10 %;
//   - else no_change.
//
// Throughput comes first in both directions: a sink with a backlog consumes
// at its capacity, so its rate is the tuning's effect, while the lag slope
// also moves with the producer. A consume rate up 20 % while the producer
// sped up more is improved; one down 30 % while the producer slowed even
// more (the lag now shrinks) is worse. (X6)
func PerformanceOutcomeFor(before, after int64, lagSlopeBefore, lagSlopeAfter *int64) string {
	const p = PerformanceChangePercent
	lag := lagSlopeBefore != nil && lagSlopeAfter != nil
	var lb, la int64
	if lag {
		lb, la = *lagSlopeBefore, *lagSlopeAfter
	}
	switch {
	case after > before && after*100 >= before*(100+p):
		return PerformanceImproved
	case after < before && after*100 <= before*(100-p):
		return PerformanceWorse
	case lag && lb > 0 && la*100 <= lb*(100-p):
		return PerformanceImproved
	case lag && lb <= 0 && la > 0:
		return PerformanceWorse
	case lag && lb > 0 && la*100 >= lb*(100+p):
		return PerformanceWorse
	}
	return PerformanceNoChange
}

// ProposalCreatedPayload is a fix waiting for approvals.
type ProposalCreatedPayload struct {
	ProposalID     string         `json:"proposal_id"`
	Title          string         `json:"title"`
	Steps          []ProposalStep `json:"steps"`
	Risk           string         `json:"risk"`
	Rollback       string         `json:"rollback"`
	RequiredLayers []string       `json:"required_layers"`
	// ParamsHash is hex SHA-256 of the RFC 8785 (JCS) form of the frozen
	// proposal (docs/design/auth.md §3.1). Empty on events written before auth PR 4.
	ParamsHash string `json:"params_hash,omitempty"`
	// Origin says who wrote the proposal: ProposalOriginAgent or
	// ProposalOriginDriver (POST /api/rooms/{id}/proposals). Empty reads as
	// "agent": every proposal before P3 came from the agent. Not part of
	// params_hash. (P3)
	Origin string `json:"origin,omitempty"`
	// FallbackLayers are the RequiredLayers nobody owns in owners.yaml (no
	// layers: entry and no pipeline owners: entry names a principal): the
	// room's driver approves them, with grant "fallback". Fixed at creation
	// like RequiredLayers; empty when every layer has an owner. Not part of
	// params_hash. (P2)
	FallbackLayers []string `json:"fallback_layers,omitempty"`
	// Class is ActionClassRecovery when any step's action is a recovery
	// action (actions.ClassOf), set by the room service from the action
	// registry, never from the proposer's text. Empty reads as
	// ActionClassFix, so a fix proposal marshals as before. Not part of
	// params_hash: it follows from the steps' action names, which are. (X2)
	// X6: ActionClassPerformance when any step's action is a performance
	// action. A proposal has one verdict, so the room service refuses one
	// that mixes recovery and performance steps (400 validation); fix steps
	// may join either.
	Class string `json:"class,omitempty"`
}

// Proposal origins in ProposalCreatedPayload.Origin. (P3)
const (
	ProposalOriginAgent  = "agent"
	ProposalOriginDriver = "driver"
)

// ProposalScheduledPayload: the policy Policy (PolicyAutoApprove, or
// PolicySetupLocal for a T1 setup step) runs the proposal at RunAt. The driver or
// any required-layer owner may cancel it until the policy starts (its first
// approval is logged); CancelUntil (equal to RunAt in this wave) is the
// advertised deadline, and a cancel after it still wins if it lands first.
// Times are UTC. (P2)
type ProposalScheduledPayload struct {
	ProposalID  string    `json:"proposal_id"`
	RunAt       time.Time `json:"run_at"`
	Policy      string    `json:"policy"`
	CancelUntil time.Time `json:"cancel_until"`
}

// ProposalCancelledPayload stops a scheduled proposal. By is the person who
// cancelled (also the event actor, kept here so a mirror reads one object);
// Reason is their optional typed text, at most 500 characters, under the
// typed-text rule of docs/api.md. (P2)
type ProposalCancelledPayload struct {
	ProposalID string `json:"proposal_id"`
	By         Actor  `json:"by"`
	Reason     string `json:"reason"`
}

// Identity says who made a decision and how they logged in
// (docs/design/auth.md §3.2). SessionID is the public session id, never the token.
type Identity struct {
	Iss       string    `json:"iss,omitempty"`
	Sub       string    `json:"sub,omitempty"`
	SessionID string    `json:"session_id,omitempty"`
	Method    string    `json:"method"`          // "oidc" | "dev"
	Grant     string    `json:"grant,omitempty"` // "user:…" | "group:…" | "backup:…" | "solo" | "fallback" (P2: the driver on an unowned layer)
	AuthTime  time.Time `json:"auth_time,omitzero"`
	// Email ties the decision to a mailbox as well as to the actor id, so the
	// audit trail never rests on a display name (docs/design/auth.md §2.1):
	// the session's IdP email when the token said email_verified, else the
	// users.yaml email: of the user. Empty when the server knows neither.
	Email string `json:"email,omitempty"`
}

// ApprovalGivenPayload is one layer's approval. Grant says what gave the
// right to approve (Grant* below); Identity.Grant keeps the principal detail
// ("user:priya", "backup:user:meera"). A policy approval has the system actor,
// Grant GrantPolicy, Policy naming the owners.yaml list that allowed it
// (PolicyAutoApprove) and no Identity. Grant and Policy are empty on events
// written before P2: read "solo" from Identity.Grant, anything else as owner.
type ApprovalGivenPayload struct {
	ProposalID string    `json:"proposal_id"`
	Layer      string    `json:"layer"`
	ParamsHash string    `json:"params_hash,omitempty"`
	Identity   *Identity `json:"identity,omitempty"`
	Grant      string    `json:"grant,omitempty"`  // (P2) GrantOwner | GrantSolo | GrantFallback | GrantPolicy
	Policy     string    `json:"policy,omitempty"` // (P2) set only with GrantPolicy
}

// Grants in ApprovalGivenPayload.Grant. (P2)
const (
	// GrantOwner: the actor owns the layer (owners.yaml principal or backup,
	// or the users.yaml layers list in dev mode).
	GrantOwner = "owner"
	// GrantSolo: the actor is PIPEROOM_SOLO_USER.
	GrantSolo = "solo"
	// GrantFallback: nobody owns the layer (ProposalCreatedPayload.FallbackLayers)
	// and the actor was the room's driver when the approval was appended.
	GrantFallback = "fallback"
	// GrantPolicy: the pipeline's auto-approve policy, after the
	// proposal.scheduled countdown ran out uncancelled. System actor.
	GrantPolicy = "policy"
	// GrantAdmin: the actor is a tenant admin (owners.yaml tenants.<t>.admins)
	// approving a T2 or T3 step of a setup room whose provisional pipeline
	// nobody owns yet (docs/design/setup-agent.md §3.6). On a team server it
	// is the only grant for such a step besides an overlapping pipeline's
	// owners; the driver's fallback applies in solo mode only. (U)
	GrantAdmin = "admin"
)

// Policies in ApprovalGivenPayload.Policy and ProposalScheduledPayload.Policy.
const (
	// PolicyAutoApprove names the owners.yaml auto_approve: list. (P2)
	PolicyAutoApprove = "auto_approve"
	// PolicySetupLocal schedules a T1 step of a setup room (a write under the
	// project directory on a local-class target) after
	// PIPEROOM_SETUP_LOCAL_DELAY, cancellable like any scheduled proposal;
	// with the delay "0" every T1 step waits for a click instead. Never for
	// T2 or T3, never on a shared or production target. (U)
	PolicySetupLocal = "setup_local"
	// PolicyEarned names the owners.yaml autonomy: block: the action earned
	// policy approval on this pipeline (autonomy.granted) and has not lost it.
	// Scheduled and approved exactly like PolicyAutoApprove (countdown,
	// cancel, system actor); never for a step above the block's max_tier,
	// never with write mode off or PIPEROOM_AUTONOMY=off, and never by the
	// agent: the system actor approves, the agent only proposed. (AU1)
	PolicyEarned = "earned_autonomy"
)

// AutonomyGrantedPayload: Action earned policy approval on the room's
// pipeline after Streak consecutive verified recoveries inside Window, the
// owners.yaml value as written ("30d"). A verified recovery is an
// agent.verification with recovered true (for a recovery-class proposal,
// outcome RecoveryRecovered; X2) on an agent proposal of the action that no
// member disputed. Evidence are the proposal ids of the streak, oldest
// first. (AU1)
type AutonomyGrantedPayload struct {
	Action   string   `json:"action"`
	Streak   int      `json:"streak"`
	Window   string   `json:"window"`
	Evidence []string `json:"evidence"`
}

// Reasons of AutonomyRevokedPayload.Reason. (AU1)
const (
	AutonomyRevokedVerifyFailed  = "verify_failed"  // a verification of the action was not a recovery (recovered false: partial, not_recovered or unverifiable fail closed)
	AutonomyRevokedHuman         = "human"          // a layer owner or tenant admin revoked it (By set)
	AutonomyRevokedConfigChanged = "config_changed" // the autonomy: entry changed or went away in owners.yaml
	AutonomyRevokedExpired       = "expired"        // the streak fell out of its window
)

// AutonomyRevokedPayload: Action lost policy approval on the room's
// pipeline and starts earning again from a zero streak. By is the person
// for AutonomyRevokedHuman (also the event actor), nil otherwise. (AU1)
type AutonomyRevokedPayload struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
	By     *Actor `json:"by,omitempty"`
}

// ApprovalRejectedPayload is one layer owner's rejection.
type ApprovalRejectedPayload struct {
	ProposalID string    `json:"proposal_id"`
	Layer      string    `json:"layer"`
	Reason     string    `json:"reason"`
	ParamsHash string    `json:"params_hash,omitempty"`
	Identity   *Identity `json:"identity,omitempty"`
}

// ApprovalNudgedPayload names the proposal and the layer still pending on it. (F9)
type ApprovalNudgedPayload struct {
	ProposalID string `json:"proposal_id"`
	Layer      string `json:"layer"`
}

// ActionExecutedPayload is the result of running one approved step. An Output
// over PIPEROOM_EVENT_MAX_DATA_BYTES is cut at a UTF-8 boundary and ends with
// "… [truncated: N bytes]", N being the original size.
type ActionExecutedPayload struct {
	ProposalID string `json:"proposal_id"`
	StepIndex  int    `json:"step_index"`
	OK         bool   `json:"ok"`
	Output     string `json:"output"`
	// ParamsHash is the proposal's params_hash the executor checked before Execute.
	ParamsHash string `json:"params_hash,omitempty"`
	// OutcomeUnknown is true when the step may or may not have run: its
	// executor stopped after it claimed the step and before it recorded the
	// result (docs/design/replicas.md section 5). OK is then false, the step is
	// never run again, and the agent is told the outcome is unknown
	// (HumanInput.OK nil): the target must be checked before a retry.
	OutcomeUnknown bool `json:"outcome_unknown,omitempty"`
	// PlanStepIndex is the setup plan step this step consumed
	// (events.SetupPlanStep.Index), set by the executor from the route it
	// resolved right before the write; nil for a step outside a setup plan
	// (an incident room, a driver's own fix) and on events from before U1-6.
	// StepIndex above is the step's index inside its PROPOSAL, so the plan
	// fold must never key on it. (U1-6)
	PlanStepIndex *int `json:"plan_step_index,omitempty"`
	// Receipt is what the runner did for a setup step (actions.Receipted):
	// the exact argv, target and output MAC, never credentials or raw output.
	// Nil for every action that is not Receipted, so events from before the
	// U wave serialise exactly as before. (U)
	Receipt *SetupReceipt `json:"receipt,omitempty"`
}

// ---- Setup rooms (U wave; docs/design/setup-agent.md §2.2, §4 item 2) ----

// SetupReceipt is the setup equivalent of ToolCall: proof of what the runner
// executed for one step. Argv is the exact argv of an exec op with every
// secret as ${secret:name}; Target is the host, URL, file path or database
// the op reached, never credentials; OutMAC is HMAC-SHA256, under a key only
// the runner holds, over the redacted result exactly as the server receives
// it (the OK flag and the redacted Summary and Data), so it proves the
// transport integrity of the redacted result: whoever alters it in flight or
// at rest cannot mint a matching MAC, and the runner's store re-verifies it
// on a dispute. The raw output itself is never MAC'd and never leaves the
// runner. A receipt never holds a secret value, a token or raw output.
type SetupReceipt struct {
	Kind     string    `json:"kind"`           // "exec" | "http" | "sql" | "file"
	Argv     []string  `json:"argv,omitempty"` // exec only
	Target   string    `json:"target"`
	ExitCode int       `json:"exit_code"`
	At       time.Time `json:"at"`
	TookMS   int64     `json:"took_ms"`
	OutMAC   string    `json:"out_mac"`
}

// SetupRequest opens a setup room (POST /api/setup); RoomCreatedPayload.Alert
// holds it and the agent reads it from Alert.Raw. Everything in it is typed
// by the person: hosts become the room's first targets (SetupTarget), tables
// and connectors the provisional pipeline's scope (owners.Provisional). The
// model never extends any of it; only a driver click does
// (setup.target_registered, setup.pipeline_extended).
type SetupRequest struct {
	PipelineID  string        `json:"pipeline_id"`
	Tenant      string        `json:"tenant,omitempty"` // "" without a tenants: block; else a tenant the creator belongs to
	Title       string        `json:"title"`
	SourceKind  string        `json:"source_kind"` // postgres | mysql | sqlserver | mongodb | oracle
	Target      string        `json:"target"`      // compose | host | strimzi | msk | confluent | aiven | dbzserver | embedded
	TopicPrefix string        `json:"topic_prefix"`
	Tables      []string      `json:"tables"`
	Connectors  []string      `json:"connectors,omitempty"`
	Targets     []SetupTarget `json:"targets"`
	Repos       []string      `json:"repos,omitempty"` // repo ids of owners.yaml repos:, or the tenant default
}

// SetupTarget is one host or account a setup room may reach. Kind says what
// it is ("postgres", "kafka", "connect", "registry", "kubernetes", "aws",
// "confluent", "aiven", "host"); Address is a host:port, a URL, a kubectl
// context and namespace, or an account id. Class is a floor: the person may
// raise a class above what the server derives, never lower it, and a
// registered target is never below "shared" (docs/design/setup-agent.md
// §2.6, §3.5). Empty means "let the server derive it".
type SetupTarget struct {
	Kind    string `json:"kind"`
	Address string `json:"address"`
	Class   string `json:"class,omitempty"` // "local" | "shared" | "production"
}

// Outcomes of SetupPairingPayload.Outcome.
const (
	PairingOK          = "ok"
	PairingBadCode     = "bad_code"
	PairingExpired     = "expired"
	PairingRateLimited = "rate_limited"
	PairingReplaced    = "replaced" // a lost runner was replaced by a new pairing
	PairingRefused     = "refused"  // the person answered N on the runner, or the protocol versions are not compatible (protocol.ServerAccepts, RunnerAccepts)
)

// SetupPairingPayload records one pairing attempt. Source is the client
// address as the server saw it (through the trusted-proxy rule); RunnerLabel
// is set on success. The code and the token are never in it.
type SetupPairingPayload struct {
	Outcome     string `json:"outcome"` // see Pairing*
	Source      string `json:"source"`
	RunnerLabel string `json:"runner_label,omitempty"`
}

// SetupRunnerAttachedPayload is the runner's Hello as stored. Tools maps a
// tool name to its version. ProjectDir is a path on the runner's machine.
// ProtocolVersion and ProtocolMinor are the runner's version from Hello
// (U1-0; the minor is absent, so 0, for a 1.0 runner); the session speaks
// the lower of that minor and the server's.
type SetupRunnerAttachedPayload struct {
	RunnerLabel      string            `json:"runner_label"`
	OS               string            `json:"os"`
	Arch             string            `json:"arch"`
	Tools            map[string]string `json:"tools"`
	ProjectDir       string            `json:"project_dir"`
	CatalogueVersion string            `json:"catalogue_version"`
	ProtocolVersion  int               `json:"protocol_version"`
	ProtocolMinor    int               `json:"protocol_minor,omitempty"`
}

// SetupRunnerLostPayload says the runner went silent (protocol.LostAfter).
// Every in-flight step is then recorded with OutcomeUnknown.
type SetupRunnerLostPayload struct {
	RunnerLabel string    `json:"runner_label"`
	LastSeen    time.Time `json:"last_seen"`
}

// SetupRunnerRefusedPayload mirrors protocol.Refusal: Reason is a
// protocol.Refuse* code, never text from the runner.
type SetupRunnerRefusedPayload struct {
	Op         string `json:"op"`
	Reason     string `json:"reason"`
	ProposalID string `json:"proposal_id,omitempty"`
	StepIndex  *int   `json:"step_index,omitempty"`
}

// SetupPlanStep is one step of a setup plan. Op and Verify are the model's;
// Target, Tier and Layer are filled by the server from the resolved endpoint
// (docs/design/setup-agent.md §3.3) and never from the model's text.
type SetupPlanStep struct {
	Index  int    `json:"index"`
	Op     string `json:"op"`
	Target string `json:"target"`
	Tier   string `json:"tier"` // protocol.TierT0..TierT3
	Layer  string `json:"layer"`
	Verify string `json:"verify,omitempty"` // one sentence, agent text, ≤ 200 chars after sanitizing
}

// SetupPlanPhase groups steps under a name ("prerequisites", "stack",
// "connector", "verify").
type SetupPlanPhase struct {
	Name  string          `json:"name"`
	Steps []SetupPlanStep `json:"steps"`
}

// SetupPlanPayload is the current plan. A new plan after a failed step names
// the one it replaces in Supersedes; a proposal whose step is not in the
// current plan is refused (agent.ErrNotInPlan).
type SetupPlanPayload struct {
	PlanID     string           `json:"plan_id"`
	Supersedes string           `json:"supersedes,omitempty"`
	Phases     []SetupPlanPhase `json:"phases"`
}

// SetupTargetRegisteredPayload: a driver click added a target. Class is the
// class the server derived combined with the person's floor, never below
// "shared". By is also the event actor.
type SetupTargetRegisteredPayload struct {
	Kind    string `json:"kind"`
	Address string `json:"address"`
	Class   string `json:"class"`
	By      Actor  `json:"by"`
}

// SetupTargetClassedPayload: a target's class was set or raised. By is
// "server" (derived from Hello and env_detect) or "person" (the raise route
// or the SetupRequest floor); Reason is a fixed code ("loopback",
// "created_by_room", "prod_pattern", "person", "default").
type SetupTargetClassedPayload struct {
	Target string `json:"target"`
	Class  string `json:"class"`
	Reason string `json:"reason"`
	By     string `json:"by"` // "server" | "person"
}

// SetupPipelineExtendedPayload: a driver click grew the provisional
// pipeline (POST /api/rooms/{id}/setup/pipeline). Hosts are targets, not
// here. By is also the event actor.
type SetupPipelineExtendedPayload struct {
	AddConnectors     []string `json:"add_connectors,omitempty"`
	AddTables         []string `json:"add_tables,omitempty"`
	AddConsumerGroups []string `json:"add_consumer_groups,omitempty"`
	AddSinkTables     []string `json:"add_sink_tables,omitempty"`
	By                Actor    `json:"by"`
}

// SetupSecretSetPayload: a secret entered the runner's store under Name,
// from the runner's terminal (Actor "runner") or the UI route (Actor the
// driver). The value is in no event, no log and no prompt.
type SetupSecretSetPayload struct {
	Name  string `json:"name"`
	Actor Actor  `json:"actor"`
}

// SetupCredentialsRegisteredPayload: the runner's Credentials frame was
// written to PIPEROOM_SECRETS_DIR/<pipeline>/. Names only; values nowhere.
type SetupCredentialsRegisteredPayload struct {
	PipelineID  string   `json:"pipeline_id"`
	Names       []string `json:"names"`
	RunnerLabel string   `json:"runner_label"`
}

// SetupMaintenanceWindowPayload: the driver recorded the window inside which
// a T3 step of the guard table may be created and executed (at most 4 h, at
// most 24 h ahead). By is also the event actor.
type SetupMaintenanceWindowPayload struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	By    Actor     `json:"by"`
}

// SetupRung is one rung of the verification ladder as the ladder runner
// evaluated it on structured Data, never on a summary. StepID and ReceiptID
// name the read it issued in this room's log.
type SetupRung struct {
	Name      string `json:"name"`
	OK        bool   `json:"ok"`
	StepID    string `json:"step_id,omitempty"`
	ReceiptID string `json:"receipt_id,omitempty"`
	Note      string `json:"note,omitempty"`
}

// SetupVerifiedPayload is the ladder's outcome for the plan PlanID. It is
// written by the server's ladder runner only: the agent's Sink refuses the
// type, so no model output can claim a pipeline verified. NonceHash is
// SHA-256 of the canary nonce the runner inserted; the nonce itself is in
// the proposal params and the receipt, never here. Rungs are in ladder
// order (design §3.4); pipeline_register needs rungs 1 to 7 OK with no
// executed step after this event.
type SetupVerifiedPayload struct {
	PlanID              string      `json:"plan_id"`
	NonceHash           string      `json:"nonce_hash"`
	Rungs               []SetupRung `json:"rungs"`
	FirstEventLatencyMS *int64      `json:"first_event_latency_ms,omitempty"`
}

// SetupAwaitingChangePayload: rung 8 waits for a change the person makes to
// Table; ExampleSQL is the statement the UI shows (from the source driver,
// never from the model).
type SetupAwaitingChangePayload struct {
	Table      string `json:"table"`
	ExampleSQL string `json:"example_sql"`
}

// Reasons of SetupBlockedPayload.Reason.
const (
	SetupBlockedAttempts   = "attempts"    // SetupBounds.AttemptsPerStep reached on one step
	SetupBlockedSteps      = "steps"       // SetupBounds.StepsPerPlan reached
	SetupBlockedWallClock  = "wall_clock"  // SetupBounds.WallClock reached
	SetupBlockedBudget     = "budget"      // the setup token budget reached
	SetupBlockedRunnerLost = "runner_lost" // the runner was lost and did not return
	SetupBlockedRefused    = "refused"     // the runner refused a step twice
)

// SetupBlockedPayload: a bound stopped the run (agent.SetupBounds); the agent
// goes idle as after a budget stop and asks Question (agent text, ≤ 500
// chars after sanitizing).
type SetupBlockedPayload struct {
	Reason    string `json:"reason"`
	StepIndex *int   `json:"step_index,omitempty"`
	Question  string `json:"question"`
}

// Who watches a registered pipeline (PipelineRegisteredPayload.WatchedBy and
// the owners.yaml setup: block; docs/design/setup-agent.md §3.7).
const (
	WatchedByServer = "server" // the team server reads it through the registered read credentials
	WatchedByLocal  = "local"  // a local target: the standalone server on the same machine
)

// PipelineRegisteredPayload: the owners entry was written to Path
// (OWNERS_DIR/<id>.yaml, or the file next to the project in standalone
// mode) or opened as PullRequestURL (repos: set), and the server reloaded.
// ApprovedBy are the approvers of the pipeline_register proposal; on a team
// server at least one is not By.
type PipelineRegisteredPayload struct {
	PipelineID     string  `json:"pipeline_id"`
	Tenant         string  `json:"tenant,omitempty"`
	Path           string  `json:"path,omitempty"`
	PullRequestURL string  `json:"pull_request_url,omitempty"`
	WatchedBy      string  `json:"watched_by"` // WatchedByServer | WatchedByLocal
	By             Actor   `json:"by"`
	ApprovedBy     []Actor `json:"approved_by"`
}

// HandoffSummary is the agent-written shift hand-off.
type HandoffSummary struct {
	WhatBroke string   `json:"what_broke"`
	Why       string   `json:"why"`
	Done      []string `json:"done"`
	RuledOut  []string `json:"ruled_out"`
	Open      []string `json:"open"`
	Risks     []string `json:"risks"`
	// Violations are the checks the summary still fails after the agent's
	// one self-correction pass (agent.CheckHandoff rules: unknown_reference,
	// unknown_identifier, unknown_number, open_proposal_missing, …). Shown to
	// the recipient before Accept; they never block a hand-off. nil on
	// summaries written before F3; [] when checked and clean. (F3)
	Violations []HandoffViolation `json:"violations,omitempty"`
}

// MarshalJSON keeps "checked and clean" on the wire: omitempty alone drops a
// non-nil empty Violations, so [] would read as "not checked". A nil
// Violations is still omitted, so a summary written before F3 marshals byte
// for byte as before (export hashes).
func (h HandoffSummary) MarshalJSON() ([]byte, error) {
	type plain HandoffSummary
	if h.Violations == nil || len(h.Violations) > 0 {
		return json.Marshal(plain(h))
	}
	return json.Marshal(struct {
		plain
		Violations []HandoffViolation `json:"violations"`
	}{plain(h), h.Violations})
}

// HandoffViolation is one failed hand-off check (agent.HandoffViolation's
// wire form). Detail may quote the summary; it is agent text, ≤ 200 bytes. (F3)
type HandoffViolation struct {
	Rule   string `json:"rule"`
	Field  string `json:"field"`
	Detail string `json:"detail"`
}

// HandoffRequestedPayload is written, with the driver as actor, when
// POST /api/rooms/{id}/handoffs passes its checks and before the agent writes
// the summary. The request is "in flight" from this event until the first of:
// a handoff.created or handoff.failed carrying the same RequestID, or
// ExpiresAt (the server's summary deadline, now + 3 min; it covers a server
// that stopped mid-request and wrote neither). While a request is in flight
// another POST to the route answers 409 handoff_in_progress, whoever sends
// it. Clients fold the same rule to show "writing hand-off…".
type HandoffRequestedPayload struct {
	RequestID     string    `json:"request_id"` // "hr_" + ULID
	To            Actor     `json:"to"`
	StayAsWatcher bool      `json:"stay_as_watcher"`
	ExpiresAt     time.Time `json:"expires_at"`
}

// Reasons in HandoffFailedPayload.Reason. Fixed codes, never error text: a
// model or store error can quote room content.
const (
	HandoffFailedSummary   = "summary_failed" // the summary call failed or timed out
	HandoffFailedNotDriver = "not_driver"     // the requester stopped being the driver meanwhile
	HandoffFailedClosed    = "room_closed"    // the room closed meanwhile
	HandoffFailedInternal  = "internal_error" // anything else; details only in the server log
)

// HandoffFailedPayload ends an in-flight hand-off request without a hand-off.
// Written by the system actor; the route answers the error it maps to.
type HandoffFailedPayload struct {
	RequestID string `json:"request_id"`
	Reason    string `json:"reason"` // see HandoffFailed*
}

// HandoffCreatedPayload proposes moving the driver role to another person.
// RequestID is the handoff.requested it completes; empty on events written
// before handoff.requested existed.
type HandoffCreatedPayload struct {
	HandoffID     string         `json:"handoff_id"`
	RequestID     string         `json:"request_id,omitempty"`
	To            Actor          `json:"to"`
	Summary       HandoffSummary `json:"summary"`
	StayAsWatcher bool           `json:"stay_as_watcher"`
}

// HandoffAcceptedPayload completes a hand-off.
type HandoffAcceptedPayload struct {
	HandoffID string `json:"handoff_id"`
}

// TraceStage is one stage of a record trace.
type TraceStage struct {
	Layer  string            `json:"layer"`
	Name   string            `json:"name"`
	Status string            `json:"status"` // "seen" | "not_seen" | "stopped_here" | "unknown"
	Facts  map[string]string `json:"facts,omitempty"`
}

// TracePinnedPayload pins a record trace to the room.
type TracePinnedPayload struct {
	Table  string       `json:"table"`
	Key    string       `json:"key"`
	Stages []TraceStage `json:"stages"`
}

// ---- Post-mortem (F2) ----

// PostmortemDraftedPayload names where the draft's prose sections come from.
// Source "handoff": the summary of hand-off SummaryRef, written after the last
// proposal, action and hypothesis. "model": Summary holds a fresh closing
// summary (HandoffSummary shape, sanitized like a hand-off) and Violations the
// grounding rules it failed (rule names of internal/agent CheckHandoff, no
// text). "events": no prose; the render uses the final hypothesis and the
// resolution only.
type PostmortemDraftedPayload struct {
	Source     string          `json:"source"` // "handoff" | "model" | "events"
	SummaryRef string          `json:"summary_ref,omitempty"`
	Summary    *HandoffSummary `json:"summary,omitempty"`
	Violations []string        `json:"violations,omitempty"`
}

// PostmortemFailedPayload: Reason is a fixed code (HandoffFailedSummary or
// HandoffFailedInternal), never error text.
type PostmortemFailedPayload struct {
	Reason string `json:"reason"`
}

// ---- Labels and checklists (F8) ----

// RoomLabelledPayload generalises ShadowLabelledPayload to every closed room.
type RoomLabelledPayload struct {
	ActualLayer   string    `json:"actual_layer,omitempty"`    // one of Layers, "" = unknown
	Cause         string    `json:"cause"`                     // 1..500 chars, typed text
	FixThatWorked string    `json:"fix_that_worked,omitempty"` // ≤ 500 chars; "" = none / not fixed by a Piperoom action
	FixProposalID string    `json:"fix_proposal_id,omitempty"` // the executed proposal, when the fix was one
	AgentHelped   *bool     `json:"agent_helped,omitempty"`
	Identity      *Identity `json:"identity,omitempty"`
}

// ChecklistStepDonePayload ticks or unticks item Item (0-based) of the runbook
// section that tool result ToolUseID (an agent.tool_result of runbook_lookup)
// returned as hits[Hit].items. By is the event actor; Note is optional typed
// text ≤ 300 chars.
type ChecklistStepDonePayload struct {
	StepID    string `json:"step_id"`
	ToolUseID string `json:"tool_use_id"`
	Hit       int    `json:"hit"`
	Item      int    `json:"item"`
	Done      bool   `json:"done"`
	Note      string `json:"note,omitempty"`
}

// ---- Shadow mode ----

// ShadowRevealedPayload says a shadow run's result exists. Written once per
// room, by the system actor, when both the room is closed and the run has
// ended. It carries numbers only: the hypothesis itself is read from
// GET /api/rooms/{id}/shadow after the blind label, so this event cannot
// un-blind a labeller.
type ShadowRevealedPayload struct {
	Status string `json:"status"` // "hypothesised" | "no_hypothesis" | "error" | "budget" | "interrupted"
	// Seconds from rooms.opened_at; -1 when there was no hypothesis.
	TimeToFirstHypothesisS int64 `json:"time_to_first_hypothesis_s"`
	TimeToFinalHypothesisS int64 `json:"time_to_final_hypothesis_s"`
	// HumanMTTRS is closed_at - opened_at in seconds.
	HumanMTTRS         int64 `json:"human_mttr_s"`
	FinishedAfterClose bool  `json:"finished_after_close"`
	ToolCalls          int   `json:"tool_calls"`
	Tokens             int64 `json:"tokens"` // 0 when the runner counts no tokens (demo, plain Run)
}

// ShadowLabelledPayload is the blind label a pipeline member gives after the
// room is closed and before they may read the run. One per room.
type ShadowLabelledPayload struct {
	ActualLayer    string    `json:"actual_layer"`     // one of Layers
	RootCauseTerms []string  `json:"root_cause_terms"` // 1..20, each 1..200 chars
	Identity       *Identity `json:"identity,omitempty"`
}

// ShadowGradedPayload is a pipeline member's grade of the revealed run against
// the resolution and the label. Re-grading appends another; the last wins.
type ShadowGradedPayload struct {
	LayerCorrect bool      `json:"layer_correct"`
	RootCause    string    `json:"root_cause"` // "yes" | "partial" | "no"
	Notes        string    `json:"notes,omitempty"`
	Identity     *Identity `json:"identity,omitempty"`
}

// Layers are the pipeline layers that own nodes and approve fixes, of every
// pipeline kind: the six CDC layers first, then the batch-only ones (P4),
// then the setup layer (U), then the MySQL source layer (Q8: appended after
// LayerEnvironment so no existing position moves). A check of "is this a
// known layer" uses Layers; the layers of one pipeline come from
// LayersOfSource(its kind, its source_kind).
var Layers = []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse", "orchestrator", "transform", LayerEnvironment, LayerMySQL}

// Source kinds of a CDC pipeline: owners.yaml source_kind (Q8). The same
// strings as SetupRequest.SourceKind.
const (
	SourceKindPostgres = "postgres"
	SourceKindMySQL    = "mysql"
)

// LayerMySQL is the source layer of a pipeline whose source_kind is mysql: it
// takes the place of "postgres" in LayersOfSource. (Q8)
const LayerMySQL = "mysql"

// SourceLayer returns the layer of a source database of the given source kind
// ("" = postgres), or "" for an unknown kind, which fails closed: no owner
// owns "". (Q8)
func SourceLayer(sourceKind string) string {
	switch sourceKind {
	case "", SourceKindPostgres:
		return "postgres"
	case SourceKindMySQL:
		return LayerMySQL
	}
	return ""
}

// LayersOfSource returns the layers of a pipeline of kind and sourceKind,
// upstream first: LayersOf(kind) with its source layer (index 0 of a cdc
// pipeline) replaced by SourceLayer(sourceKind). A batch pipeline ignores
// sourceKind. An unknown sourceKind on a cdc pipeline answers nil. New slice
// per call. (Q8)
func LayersOfSource(kind, sourceKind string) []string {
	l := LayersOf(kind)
	if kind != "" && kind != PipelineKindCDC {
		return l
	}
	sl := SourceLayer(sourceKind)
	if sl == "" || len(l) == 0 {
		return nil
	}
	l[0] = sl
	return l
}

// IncidentLayers are the layers an incident can be in: Layers without the
// setup layer. The triage schema's suspected_layer enum, the room label,
// shadow label, scenario, learned-example, agreement and similar_incidents
// validators mean this set (a cause is never "environment"); they keep
// checking Layers until their rows switch, which only widens what they
// accept. Approvals, required and fallback layers, and owners entries use
// Layers and LayersOf, where environment is valid. New slice per call. (U)
func IncidentLayers() []string {
	out := make([]string, 0, len(Layers)-1)
	for _, l := range Layers {
		if l != LayerEnvironment {
			out = append(out, l)
		}
	}
	return out
}

// LayerEnvironment is the layer of the setup actions that change the user's
// environment rather than one pipeline system: compose files, services,
// manifests, plugin installs and pipeline_register (docs/design/setup-agent.md
// §2.5, §3.9). It is the last layer of a CDC pipeline. An owners.yaml that
// does not name it leaves it unowned: the driver approves it with
// GrantFallback in solo mode, a tenant admin with GrantAdmin otherwise. (U)
const LayerEnvironment = "environment"

// Pipeline kinds (owners.yaml kind:). (P4)
const (
	// PipelineKindCDC is Postgres -> Debezium -> Kafka -> consumers ->
	// warehouse; the default when owners.yaml names no kind.
	PipelineKindCDC = "cdc"
	// PipelineKindBatch is an orchestrated batch pipeline: an orchestrator
	// (Airflow DAGs), a transform step (dbt models) and the warehouse.
	PipelineKindBatch = "batch"
)

// LayersOf returns the layers of a pipeline of the given kind, upstream
// first: "cdc" (and "", the default kind) gives the six CDC layers and then
// LayerEnvironment (U: at the end, so code that indexes the six by position
// is unchanged), "batch" gives orchestrator, transform, warehouse. An unknown
// kind gives nil. The slice is new on every call, so a caller may keep or
// change it. (P4)
func LayersOf(kind string) []string {
	switch kind {
	case "", PipelineKindCDC:
		return []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse", LayerEnvironment}
	case PipelineKindBatch:
		return []string{"orchestrator", "transform", "warehouse"}
	}
	return nil
}
