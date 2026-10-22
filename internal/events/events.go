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

	// HypothesisVerdict is a person's confirm or dispute of the agent's
	// current hypothesis (POST /api/rooms/{id}/hypothesis). HypothesisSeq is
	// the seq of the agent.hypothesis it answers; a later agent.hypothesis
	// starts over with no verdicts. Any member may write one; the snapshot
	// keeps the latest per person. AgentHypothesisPayload.Confirmed is never
	// set by the agent: clients and the snapshot derive it from these events. (F9)
	HypothesisVerdict Type = "hypothesis.verdict"

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
}

// RoomModeSetup is RoomCreatedPayload.Mode of a setup room. (U)
const RoomModeSetup = "setup"

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
}

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
)

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
// the op reached, never credentials; OutMAC is HMAC-SHA256 of the raw output
// under a key only the runner holds, so the output itself never leaves the
// runner and a dispute is settled by re-running the receipt there. A receipt
// never holds a secret value, a token or raw output.
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

// ---- Watcher signals (X wave) ----
//
// A watcher signal is a kind of evidence the built-in watcher (internal/watch)
// puts in room.created.alert of a watcher room, or posts as a join into an
// open one. The kinds of before the X wave are named in internal/owners
// (owners.Signal*); the X-wave kinds are named here so the watcher, the agent
// and the web app share one spelling. owners.yaml watch: settings (A3) must
// accept these names before the watcher emits them.

// Drift signal kinds (X1): "green but wrong" faults that raise no symptom.
const (
	SignalCountDrift = "count_drift" // source and sink row counts differ for a closed window
	SignalRowDrift   = "row_drift"   // sampled keys are missing in the sink or carry other timestamps
	SignalDupKeys    = "dup_keys"    // one key landed more than once where it must be unique
	SignalOrderSkew  = "order_skew"  // events of one key arrived out of source order
)

// DefaultSignalSeverity is the room severity a signal opens with when
// owners.yaml sets none: sev2 for a count or row drift (data is missing or
// wrong), sev3 for duplicates and order skew (data is there, but needs a
// look). "" for a kind it does not know: the caller keeps its own default.
func DefaultSignalSeverity(kind string) string {
	switch kind {
	case SignalCountDrift, SignalRowDrift:
		return "sev2"
	case SignalDupKeys, SignalOrderSkew:
		return "sev3"
	}
	return ""
}

// DriftWindow is the closed time window a drift signal compared, UTC.
type DriftWindow struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// DriftSignal is the evidence of one drift signal (X1). It holds counts and
// names only, never a key or a record value. Table is the source table
// ("schema.table"); Topic is the CDC topic between them, "" when the kind did
// not read one. Source and Sink are row counts in Window (count_drift; 0 for
// the other kinds). Missing counts the sampled keys absent in the sink
// (row_drift). Mismatched counts the sampled keys whose timestamps differ
// (row_drift), the keys found more than once (dup_keys) or the keys found out
// of order (order_skew). Sample is how many keys were checked (0 for
// count_drift). Detail is one line written by the watcher, ≤ 300 runes.
type DriftSignal struct {
	Kind       string      `json:"kind"` // Signal* above
	Table      string      `json:"table"`
	Topic      string      `json:"topic,omitempty"`
	Window     DriftWindow `json:"window"`
	Source     int64       `json:"source"`
	Sink       int64       `json:"sink"`
	Missing    int         `json:"missing"`
	Mismatched int         `json:"mismatched"`
	Sample     int         `json:"sample"`
	Detail     string      `json:"detail,omitempty"`
}

// Layers are the pipeline layers that own nodes and approve fixes, of every
// pipeline kind: the six CDC layers first, then the batch-only ones (P4),
// then the setup layer (U). A check of "is this a known layer" uses Layers;
// the layers of one pipeline come from LayersOf(its kind).
var Layers = []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse", "orchestrator", "transform", LayerEnvironment}

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
