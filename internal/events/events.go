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
	// ProposalWithdrawn ends an open proposal without running it: the agent
	// withdrew its own proposal (withdraw_proposal room tool), the driver
	// withdrew it (POST /api/rooms/{id}/proposals/{pid}/withdraw), or the room
	// closed while it was still open (system actor, reason
	// ProposalWithdrawnRoomClosed, written right after room.closed). Only a
	// proposal with no step claimed by the executor can be withdrawn. A
	// withdrawn proposal takes no approval or rejection (409
	// proposal_withdrawn) and leaves open_proposals. (Q0, QA G8/G9)
	ProposalWithdrawn Type = "proposal.withdrawn"
	// OwnerPaged records that the owners of one layer were asked to look at the
	// room: by the agent (page_owner room tool), by a member (POST
	// /api/rooms/{id}/page), or by the server itself when a watcher or alert
	// room still has no driver PIPEROOM_PAGE_UNDRIVEN_AFTER after it opened
	// (system actor, Auto). At most one per room and layer per 10 minutes. The
	// web app shows it to the paged people; the notify webhook and the Slack
	// bridge deliver it outside the app. (Q0, QA G2)
	OwnerPaged Type = "owner.paged"

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
//
// A new-incident note of the watcher join (system actor, Q2 F21) also carries
// "new_incident": true and "agent_text" (the evidence as the agent reads it):
// the replica holding the room's agent lease turns it into the waking
// alert_update, so the log is the cross-replica inbox. Clients ignore both.
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
	// Kind is "" for a plain comment or CommentKindFixNote: a person logs what
	// they did outside Piperoom ("restarted order-sync by hand"). The agent
	// reads it as a comment; the post-mortem lists fix notes under "What was
	// done" and treats the room as fixed by a person. (Q0, QA G10)
	Kind     string `json:"kind,omitempty"`
	ClientID string `json:"client_id,omitempty"` // the request's idempotency key (docs/api.md, Idempotent posts; #459)
}

// CommentKindFixNote is CommentAddedPayload.Kind for a logged manual fix. (Q0)
const CommentKindFixNote = "fix_note"

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

// ControlGrantedPayload moves the driver role.
type ControlGrantedPayload struct {
	To         Actor     `json:"to"`
	LeaseUntil time.Time `json:"lease_until"`
}

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
	// ImpactedLayers are layers the fix does not write to but whose data it
	// changes (a decimal precision change on the connector reaches the consumer
	// and the warehouse). The proposer names them and an action may add more
	// (actions.Impacting). The server keeps known layers only, drops the step
	// layers, sorts them and adds them to RequiredLayers, so their owners must
	// approve too; params_hash covers them through RequiredLayers. Omitted when
	// empty. (Q0, QA G5)
	ImpactedLayers []string `json:"impacted_layers,omitempty"`
	// RiskLevel is "low" | "medium" | "high": the proposer's value, raised by the
	// server to the highest actions.Impact level of the steps, never lowered;
	// an unknown or empty proposer value counts as "medium". Derived from the
	// hashed steps, so not hashed itself. Empty on events written before Q0. (Q0)
	RiskLevel string `json:"risk_level,omitempty"`
	// ImpactReasons are the actions.Impact reasons of the steps (≤ 8, each ≤ 300
	// chars), in step order, for approvers. Server-written; not hashed. (Q0)
	ImpactReasons []string `json:"impact_reasons,omitempty"`
}

// Risk levels of ProposalCreatedPayload.RiskLevel and actions.Impact. (Q0)
const (
	RiskLow    = "low"
	RiskMedium = "medium"
	RiskHigh   = "high"
)

// ProposalWithdrawnRoomClosed is the Reason of the system actor's withdrawal of
// a proposal still open when its room closed. (Q0)
const ProposalWithdrawnRoomClosed = "room closed"

// ProposalWithdrawnPayload ends an open proposal without running it. Reason is
// the withdrawer's one line (≤ 500 chars, sanitized like other agent text) or
// ProposalWithdrawnRoomClosed. (Q0, QA G8/G9)
type ProposalWithdrawnPayload struct {
	ProposalID string `json:"proposal_id"`
	Reason     string `json:"reason"`
}

// OwnerPagedPayload asks the owners of Layer to look at the room. Owners are
// the principals the server resolved for Layer from owners.yaml (approvers,
// then backups), never chosen by the caller: user ids, plus "group:<name>" for
// a group whose members the server cannot list (Q2 F6); [] when the layer has
// nobody, never null. Reason is one line (≤ 300 chars, sanitized); agent text is untrusted
// and clients render it as text. ProposalID names the proposal waiting on the
// layer, when there is one. Auto is true when the server paged on its own (no
// driver PIPEROOM_PAGE_UNDRIVEN_AFTER after a watcher or alert room opened).
// (Q0, QA G2)
type OwnerPagedPayload struct {
	Layer      string   `json:"layer"`
	Owners     []string `json:"owners"`
	Reason     string   `json:"reason"`
	ProposalID string   `json:"proposal_id,omitempty"`
	Auto       bool     `json:"auto,omitempty"`
}

// Identity says who made a decision and how they logged in
// (docs/design/auth.md §3.2). SessionID is the public session id, never the token.
type Identity struct {
	Iss       string    `json:"iss,omitempty"`
	Sub       string    `json:"sub,omitempty"`
	SessionID string    `json:"session_id,omitempty"`
	Method    string    `json:"method"`          // "oidc" | "dev"
	Grant     string    `json:"grant,omitempty"` // "user:…" | "group:…" | "backup:…" | "solo"
	AuthTime  time.Time `json:"auth_time,omitzero"`
	// Email ties the decision to a mailbox as well as to the actor id, so the
	// audit trail never rests on a display name (docs/design/auth.md §2.1):
	// the session's IdP email when the token said email_verified, else the
	// users.yaml email: of the user. Empty when the server knows neither.
	Email string `json:"email,omitempty"`
}

// ApprovalGivenPayload is one layer owner's approval.
type ApprovalGivenPayload struct {
	ProposalID string    `json:"proposal_id"`
	Layer      string    `json:"layer"`
	ParamsHash string    `json:"params_hash,omitempty"`
	Identity   *Identity `json:"identity,omitempty"`
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
	HandoffFailedSummary   = "summary_failed" // the summary call failed or timed out, or its summary stayed invalid after one repair (Q0: route 503 summary_failed, never 500)
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
	// Manual is true when the driver wrote Summary (POST
	// /api/rooms/{id}/handoffs with a summary, typically after the agent's
	// failed): no model call was made and Violations are not checked. (Q0, QA G4)
	Manual bool `json:"manual,omitempty"`
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

// Layers are the pipeline layers that own nodes and approve fixes.
var Layers = []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse"}
