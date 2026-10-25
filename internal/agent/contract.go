// Package agent runs the investigation loop with Claude.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
// The implementation (run.go and others) is owned by agent A4.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// HumanInput is something a person said that the agent must read.
type HumanInput struct {
	Seq int64 `json:"seq"` // room event seq it came from
	// At is the time of the room event the input came from (events.Event.At).
	// The room service sets it for "action_result": the action.executed time
	// a recovery step's Verify counts from (actions.Verifier, X2c). Zero when
	// the sender does not know it (the CLI, a demo inbox): the runner then
	// uses the time it first read the input, which is never earlier.
	At        time.Time    `json:"at,omitzero"`
	Actor     events.Actor `json:"actor"`
	Kind      string       `json:"kind"` // "steer" | "suggestion" | "comment" | "approval_result" | "action_result" | "alert_update" | "proposal_created" | "proposal_executed" | "proposal_staged" | "pr_merged" | "pr_closed" | "setup_verified"
	StepID    string       `json:"step_id,omitempty"`
	CommentID string       `json:"comment_id,omitempty"`
	// Text of an "alert_update" is the server's rendering of a re-fired or
	// joined alert's evidence (AlertEvidenceText). It is untrusted data, it
	// never wakes an idle agent and never resets the turn guard. (F6)
	Text string `json:"text"`
	// OK is set only for Kind "action_result": true when the step succeeded,
	// false when it failed. Nil means the outcome is unknown, and the agent must
	// then never claim the pipeline recovered. (X8: also for
	// "proposal_staged", below.)
	OK *bool `json:"ok,omitempty"`
	// ProposalID, Layer and StepIndex identify what an "approval_result" or
	// "action_result" is about, so the runner never parses Text for them.
	// Layer is set for approval_result; StepIndex for action_result.
	ProposalID string `json:"proposal_id,omitempty"`
	Layer      string `json:"layer,omitempty"`
	StepIndex  *int   `json:"step_index,omitempty"`
	// Approved is set only for Kind "approval_result": true for an approval,
	// false for a rejection.
	Approved *bool `json:"approved,omitempty"`
	// Origin is set for "proposal_created" and "proposal_executed": the
	// proposal's events.ProposalCreatedPayload.Origin ("driver" in P3; the
	// runner already hears about its own proposals through approval_result
	// and action_result). For "proposal_created", ProposalID names the
	// proposal and Text is the server's rendering of it (title, steps, risk,
	// rollback): the driver typed it, so it is that person's words, untrusted
	// data like a forwarded suggestion, never an instruction. (P3)
	Origin string `json:"origin,omitempty"`
	// StepsOK is set only for Kind "proposal_executed", sent once after the
	// last step of the proposal ProposalID ran: one entry per step in step
	// order, true when it succeeded, false when it failed, nil when the outcome
	// is unknown (as OK). A step that never ran (an earlier one failed) is
	// false. The runner verifies the fix as after its own. (P3)
	StepsOK []*bool `json:"steps_ok,omitempty"`
	// X8: Kind "proposal_staged" is sent once per proposal.staged. ProposalID
	// names the proposal, Text is the server's rendering of the result (each
	// staged step's outcome and detail: staging's own words, untrusted data,
	// never an instruction), and OK is events.ProposalStagedPayload.Healthy:
	// true when every staged step was healthy, false otherwise. A healthy
	// stage is evidence, never approval: the agent must not claim the fix
	// worked in production. (X8)
	//
	// Q7: Kinds "pr_merged" and "pr_closed" are sent once per fix PR (a step
	// of a Deferred action, actions.Deferred), from its pull_request.updated
	// with state merged or closed. ProposalID and StepIndex identify the
	// step, At is the merge (or close) time, and Text is the server's
	// rendering ("PR #12 in connect-config merged at 14:02 (merge 1a2b3c4);
	// edited after approval: no"). Both wake an idle agent. "pr_closed" is
	// final for that step: the outcome is events.DeferredNotApplied. After
	// "pr_merged" the agent waits for the change to reach the system before
	// it verifies (docs/design/fix-pr.md §8.2). (Q7)
	//
	// U: Kind "setup_verified" carries the ladder's outcome to the agent. It
	// is sent once per setup.verified, only in a setup room (Alert.Mode ==
	// ModeSetup) and only when its plan_id is the room's current setup.plan;
	// the outcome of a superseded plan is never delivered. Seq is the
	// setup.verified event. OK is true when every rung required for
	// registration is green (rungs 1 to 7, docs/design/setup-agent.md §3.9),
	// false otherwise; only then may the agent propose pipeline_register.
	// Text is the server's rendering of the rungs (name, ok, note): the
	// ladder's own words about the person's systems, untrusted data that the
	// agent wraps, never an instruction. (U1-26)
}

// Inbox delivers human input to the agent between model turns.
type Inbox interface {
	// Drain returns all pending inputs (possibly none) without blocking.
	Drain(ctx context.Context) ([]HumanInput, error)
	// Wait blocks until at least one input is pending or ctx ends.
	Wait(ctx context.Context) error
}

// Sink receives every event the agent produces. The room service implements
// it by appending to the room log; the CLI prints to the terminal.
type Sink interface {
	Emit(ctx context.Context, typ events.Type, payload any) error
}

// Proposals creates fix proposals that need human approval.
type Proposals interface {
	// Create validates every step and stores the proposal. It returns the
	// proposal ID. It must not execute anything.
	Create(ctx context.Context, p events.ProposalCreatedPayload) (string, error)
}

// Alert is what starts an investigation.
type Alert struct {
	RoomID     string          `json:"room_id"`
	Title      string          `json:"title"`
	PipelineID string          `json:"pipeline_id"`
	Raw        json.RawMessage `json:"raw,omitempty"`
	// PipelineMap is a text rendering of the topology, given to the model up front.
	PipelineMap string `json:"pipeline_map"`
	// Description is the alert's human description (annotation, summary or
	// message), selected and filled once by internal/alertin from the first
	// firing alert of the payload. It is untrusted text: the agent masks it
	// (redact, untrusted-data wrapper) before it enters any prompt.
	// Consumed by R5b (#140). Empty for rooms not opened by an alert.
	Description string `json:"description,omitempty"`
	// Labels are the alert's labels (Alertmanager labels, Grafana/Datadog tags,
	// PagerDuty custom details), same source and trust level as Description.
	// Consumed by R5b (#140).
	Labels map[string]string `json:"labels,omitempty"`
	// Triage is the triage pass's result when it ran before this run (nil
	// otherwise). Its steps are already in the room log; the runner lists
	// them as known step ids so a hypothesis may cite them. (F6)
	Triage *TriageResult `json:"triage,omitempty"`
	// PipelineKind is the room pipeline's owners.yaml kind:
	// events.PipelineKindCDC or events.PipelineKindBatch, filled by the room
	// service from owners. Empty reads as "cdc" (rooms before P4, a pipeline
	// owners.yaml does not know). It decides which tools the room offers and
	// the prompt's per-kind section (P4b). (P4)
	PipelineKind string `json:"pipeline_kind,omitempty"`
	// X4: Teams are owners.yaml's teams visible to the room's pipeline (its
	// tenant's teams with a tenants: block), filled by the room service. A
	// hypothesis's team must be one of their ids; the agent maps a cited
	// change's ExternalChange.Service to the team whose Owns lists it. Nil
	// when owners.yaml has no teams: the hypothesis then names no team. (X4)
	Teams []Team `json:"teams,omitempty"`
	// X4: At is when the alert opened the room (the room's room.created
	// time), filled by the room service. The agent measures a change's
	// distance to the alert from it (X4c ranking), so a run started later
	// (a quiet forecast room's first steer, X3c; a steer after a restart)
	// still ranks against the alert, not against its own start. Zero when
	// the caller has no room (cmd/investigate, evals): the run's start
	// stands for it. (X4)
	At time.Time `json:"at,omitzero"`
	// X8: Staging is true when the room's pipeline declares a staging stack
	// (owners.yaml staging:) and write mode is on, so the driver can try a
	// proposal there (POST /api/proposals/{pid}/stage); filled by the room
	// service. The agent then recommends staging for a proposal whose action
	// implements actions.Stager, and reads the result back from HumanInput
	// kind "proposal_staged". False (omitted) means no staging: the agent
	// does not mention it. (X8)
	Staging bool `json:"staging,omitempty"`
	// Mode is "" for an incident room and ModeSetup for a setup room
	// (room.created.mode). In setup mode Raw holds the events.SetupRequest,
	// PipelineID the pipeline being set up (no owners.yaml entry yet: the
	// room scopes tools and actions to owners.Provisional(request)), and Run
	// uses the setup prompt frame, the registries wiring.SetupStack built
	// over the paired runner, and Options.Setup as its bounds
	// (docs/design/setup-agent.md §3.1). The untrusted-data wrapper, receipts
	// and hypothesis grounding apply unchanged. (U)
	Mode string `json:"mode,omitempty"`
}

// Team is one owners.yaml teams[] entry as the agent sees it: the id and the
// service names it owns. The Slack channel is the room service's business
// (room.team_paged), never the model's. (X4)
type Team struct {
	ID   string   `json:"id"`
	Owns []string `json:"owns"`
}

// ModeSetup is Alert.Mode of a setup room; it equals events.RoomModeSetup. (U)
const ModeSetup = "setup"

// SetupBounds are the stops of a setup run (Options.Setup). On any of them
// Run emits setup.blocked with the reason and a question, goes idle, and
// returns an error wrapping *BudgetError, which callers treat as a stop, not
// a failure (as a token-budget stop today). Zero values take the defaults
// of design §3.1: 3 attempts per step, 12 executed steps per plan, 45 min,
// PIPEROOM_SETUP_TOKEN_BUDGET tokens. (U)
type SetupBounds struct {
	AttemptsPerStep int           // failed executions of one plan step before the run stops
	StepsPerPlan    int           // executed steps (T1 and above) before the run stops
	WallClock       time.Duration // from Run's start, over every turn and wait
	TokenBudget     int64         // model tokens for this setup room, cache included; counted like Options.TokenBudget
}

// ErrNotInPlan is returned (wrapped) by Proposals.Create in a setup room when
// a step's op is not in the current setup.plan: the model must emit a new
// plan (setup_plan) before it proposes again (design §3.3). The agent treats
// it as a tool error of propose_fix, not as a stop. (U)
var ErrNotInPlan = errors.New("agent: proposal step is not in the current setup plan")

// TriageResult is what the triage pass returns to its caller. (F6)
type TriageResult struct {
	Payload events.AgentTriagePayload `json:"payload"`
	Steps   []TriageStep              `json:"steps"`
}

// TriageStep is one read the pass made, for the main run's first message. (F6)
type TriageStep struct {
	StepID  string `json:"step_id"`
	Tool    string `json:"tool"`
	Summary string `json:"summary"` // the tool summary, redacted; ≤ tools.MaxSummaryBytes
	IsError bool   `json:"is_error"`
}

// Deps are the agent's collaborators.
type Deps struct {
	// Tools are the read-only tools (A2, A3 and the other tool packages). It
	// must be built with tools.NewReadOnlyRegistry; from R5b the runner
	// refuses a registry whose ReadOnly() is false.
	Tools     *tools.Registry
	Actions   *actions.Registry // write actions the agent may PROPOSE (never run)
	Inbox     Inbox
	Sink      Sink
	Proposals Proposals
	Logger    *slog.Logger
	// Usage, when set, is called exactly once when Run ends, however it ends
	// (nil, error or panic), with the model tokens the room has used in this
	// process (input + output, cache included; the count behind
	// PIPEROOM_ROOM_TOKEN_BUDGET). Only an Endpoint run counts tokens; the
	// plain Run and the demo runner never call it. Shadow mode records the
	// value. Nil = not wanted.
	Usage func(used int64)
	// Watch, when set, reads the watcher's status row of the room's pipeline.
	// The hypothesis guard (rule 1) refuses a healthy verdict while a symptom
	// signal fires and is not clearing. The room service passes a reader that
	// returns no row older than 3 x PIPEROOM_WATCH_INTERVAL. No row, a stale row
	// or a read error never refuses. Nil = rule 1 reads no row. (H1)
	Watch clients.WatchStatusReader
}

// Hypothesis guard modes (Options.HypothesisGuard). (H1)
const (
	GuardOff     = "off"     // no check
	GuardLog     = "log"     // check and log what would be refused, never refuse
	GuardEnforce = "enforce" // refuse, then accept after two refusals with confidence low
)

// Options tune the model call.
type Options struct {
	Model    string // default "claude-opus-5-5"
	Effort   string // default "high"
	MaxTurns int    // safety stop; default 40
	// HypothesisGuard is GuardOff, GuardLog or GuardEnforce; "" = GuardLog.
	// Filled from PIPEROOM_HYPOTHESIS_GUARD (config.ParseHypothesisGuard);
	// shadow rooms use the same value. (H1)
	HypothesisGuard string
	// TokenBudget caps model tokens for this run's room. 0 = the Endpoint's
	// default (PIPEROOM_ROOM_TOKEN_BUDGET). A value > 0 becomes the room's
	// limit when Run starts, even if the room's count already exists (a
	// hand-off may have created it); the count so far is kept. Shadow rooms
	// set it lower.
	TokenBudget int64
	// MaxToolDataBytes caps a tool Result.Data, as encoded JSON bytes, before
	// it enters an event (events.TruncateData); 0 means 64 KiB. cmd/server
	// and internal/wiring pass config.Limits.EventMaxDataBytes so the agent
	// and the room agree on one limit. Consumed by R11 (#136).
	MaxToolDataBytes int64
	// Setup are the bounds of a setup run (Alert.Mode == ModeSetup). Nil
	// outside setup mode; in setup mode nil means the SetupBounds defaults.
	// The server fills it from PIPEROOM_SETUP_TOKEN_BUDGET and the design's
	// fixed attempt, step and wall-clock bounds. (U)
	Setup *SetupBounds
}

// Runner is implemented by A4 in run.go:
//
//	func Run(ctx context.Context, alert Alert, deps Deps, opts Options) error
//
// Run investigates until ctx ends or the room is closed. When the model
// finishes a turn it emits AgentStatus "idle" and calls Inbox.Wait.
// An Inbox whose Wait returns ErrInboxClosed ends the run cleanly (nil) after
// the current turn. When the room's token budget stops the run, Run returns an
// error wrapping *BudgetError after it has emitted the idle status and the
// message; a caller must treat it as a stop, not a failure (no retry, no
// non-zero exit).
type Runner func(ctx context.Context, alert Alert, deps Deps, opts Options) error

// HandoffWriter is implemented by A4 in handoff.go:
//
//	func WriteHandoff(ctx context.Context, roomEvents []events.Event, opts Options) (events.HandoffSummary, error)
//
// It makes one model call that turns the room log into a hand-off summary.
type HandoffWriter func(ctx context.Context, roomEvents []events.Event, opts Options) (events.HandoffSummary, error)

// Triager is implemented by A4 (Endpoint.Triage): one bounded first look on a
// cheap model, at most 2 read tools, outside the agent pool. It emits its own
// steps and one agent.triage through deps.Sink and returns the same payload.
// It returns an error only when nothing was emitted (ctx ended first). (F6)
type Triager func(ctx context.Context, alert Alert, deps Deps, opts Options) (TriageResult, error)

// PostmortemDraft is a closing summary plus the grounding rules it failed. (F2)
type PostmortemDraft struct {
	Summary    events.HandoffSummary
	Violations []string // CheckHandoff rule names, e.g. "unknown_number"
}

// PostmortemWriter is implemented by A4 in postmortem.go:
//
//	func WritePostmortem(ctx context.Context, roomEvents []events.Event, opts Options) (PostmortemDraft, error)
//
// One model call over a closed room's log, grounded like a hand-off. (F2)
type PostmortemWriter func(ctx context.Context, roomEvents []events.Event, opts Options) (PostmortemDraft, error)
