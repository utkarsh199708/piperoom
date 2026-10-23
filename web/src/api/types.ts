// Mirror of internal/events/events.go (contract) + docs/api.md objects.
// Field names and JSON match the Go structs exactly.

import type { NodeKind } from './nodeKinds'

// ---- actors ----

export type ActorKind = 'human' | 'agent' | 'system'

export interface Actor {
  kind: ActorKind
  id: string
  name: string
  /**
   * Server-set (events.Actor.Verified): true when the server vouches for id and name.
   * Omitted when false; missing means false. The only input to the verified badge.
   */
  verified?: boolean
}

// ---- event log ----

export type EventType =
  | 'room.created'
  | 'room.closed'
  | 'agent.status'
  | 'agent.step.started'
  | 'agent.tool_call'
  | 'agent.tool_result'
  | 'agent.message'
  | 'agent.hypothesis'
  | 'agent.usage'
  | 'agent.triage'
  | 'comment.added'
  | 'steer.suggested'
  | 'steer.sent'
  | 'steer.declined'
  | 'control.requested'
  | 'control.granted'
  | 'control.lease_expired'
  | 'proposal.created'
  | 'approval.given'
  | 'approval.rejected'
  | 'action.executed'
  | 'handoff.requested'
  | 'handoff.failed'
  | 'handoff.created'
  | 'handoff.accepted'
  | 'trace.pinned'
  | 'shadow.revealed'
  | 'shadow.labelled'
  | 'shadow.graded'
  | 'hypothesis.verdict'
  | 'approval.nudged'
  | 'room.labelled'
  | 'checklist.step_done'
  | 'proposal.previewed'
  | 'proposal.stage_started'
  | 'proposal.staged'
  | 'agent.verification'
  | 'agent.recovery_check'
  | 'postmortem.drafted'
  | 'postmortem.failed'
  | 'proposal.scheduled'
  | 'proposal.cancelled'
  | 'room.escalated'
  // U wave: setup rooms (docs/design/setup-agent.md, docs/api.md "U-wave events").
  | 'setup.pairing'
  | 'setup.runner_attached'
  | 'setup.runner_lost'
  | 'setup.runner_refused'
  | 'setup.plan'
  | 'setup.blocked'
  | 'setup.target_registered'
  | 'setup.target_classed'
  | 'setup.pipeline_extended'
  | 'setup.maintenance_window'
  | 'setup.verified'
  | 'setup.awaiting_change'
  | 'pipeline.registered'

export interface Event {
  room_id: string
  seq: number
  type: EventType
  actor: Actor
  payload: unknown
  at: string
}

// ---- payloads (one per Type, matching events.go) ----

/** room.labelled: the human label of a closed room (POST /api/rooms/{id}/label). */
export interface RoomLabelledPayload {
  actual_layer?: string
  cause: string
  fix_that_worked?: string
  fix_proposal_id?: string
  agent_helped?: boolean
}

/** checklist.step_done: item `item` of hits[`hit`].items of tool result `tool_use_id` ticked or unticked. */
export interface ChecklistStepDonePayload {
  step_id: string
  tool_use_id: string
  hit: number
  item: number
  done: boolean
  note?: string
}

/** Body of POST /api/rooms/{id}/label. */
export interface LabelInput {
  actual_layer?: string
  cause: string
  fix_that_worked?: string
  fix_proposal_id?: string
  agent_helped?: boolean
}

export interface RoomCreatedPayload {
  title: string
  severity: string // "sev1".."sev4"
  pipeline_id: string
  alert?: unknown
  /** Server-set route that opened the room: "webhook", a signed source name, or "watcher". */
  alert_source?: string
  dedup_key?: string
  /** The pipeline had shadow mode on when the room opened (docs/design/shadow.md); the agent runs silently and its numbers are revealed after close. */
  shadow?: boolean
  /** X3: "forecast" for a room a forecast signal opened (sev4, no agent until someone asks); absent otherwise. */
  origin?: string
  /** U0: "setup" for a room POST /api/setup opened; absent otherwise. Never set with `shadow`. */
  mode?: 'setup'
}

/** room.escalated (X3, system actor): a watcher signal raised an open room's severity; a room is never lowered. */
export interface RoomEscalatedPayload {
  severity: string // the new level, "sev1".."sev3"
  from: string
  signal: string
}

// ---- watcher evidence (room.created.alert when alert_source is "watcher"; docs/api.md "Watcher alerts") ----

/** The four X1 drift signal names (events.SignalCountDrift etc.). */
export type DriftKind = 'count_drift' | 'row_drift' | 'dup_keys' | 'order_skew'

/** events.DriftWindow: half-open [from, to), RFC 3339 UTC. */
export interface DriftWindow {
  from: string
  to: string
}

/**
 * events.DriftSignal: one flat entry of the watcher evidence's `signals` list. Names, counts and a window only;
 * `detail` and `table` are untrusted text, rendered as text. `window` is an object here, while dlq_growth's is a
 * word, so a reader goes by `signal` first.
 */
export interface DriftSignal {
  signal: DriftKind
  severity: string
  table?: string
  topic?: string
  window: DriftWindow
  /** count_drift: source rows; dup_keys/order_skew: records scanned. */
  source: number
  /** count_drift: sink rows; 0 otherwise. */
  sink: number
  /** count_drift: source - sink when positive; row_drift: sampled keys not on the sink; dup_keys: duplicate events. */
  missing: number
  /** row_drift: sampled keys whose timestamp differs; order_skew: position regressions. */
  mismatched: number
  /** row_drift: keys compared; dup_keys/order_skew: distinct keys affected. */
  sample: number
  detail?: string
}

/** events.ForecastSignal (X3): a metric's trend reaches its limit within the forecast horizon. */
export interface ForecastSignal {
  signal: 'forecast'
  severity: string
  metric: string
  object?: string
  current: number
  limit: number
  eta: string
  slope_per_hour: number
  r2: number
  samples: number
}

/** events.ConfigDriftSignal (X3): a connector's config changed outside an approved fix. Key names only. */
export interface ConfigDriftSignal {
  signal: 'config_drift'
  severity: string
  connector: string
  old_hash: string
  new_hash: string
  changed_keys: string[]
  keys_omitted?: number
}

export interface RoomClosedPayload {
  resolution: string
}

export interface AgentStatusPayload {
  state: string // "working" | "idle" | "waiting_approval" | "error" | "done" | "queued"
  detail?: string
}

export interface AgentStepStartPayload {
  step_id: string
  title: string
}

export interface AgentToolCallPayload {
  step_id: string
  tool_use_id: string
  tool: string
  input: unknown
}

/** One read a tool made against a customer system (F3): the receipt behind the summary. */
export interface ToolCall {
  /** "sql" | "http" | "kafka" | "registry" */
  kind: string
  /** The system read (host, database, brokers), never credentials. */
  target: string
  /** What a person would re-run: SQL with placeholders, "GET /connectors/x/status". */
  text: string
  params_redacted?: string[]
  at: string
  took_ms: number
}

export interface AgentToolResultPayload {
  step_id: string
  tool_use_id: string
  tool: string
  summary: string
  data?: unknown
  is_error: boolean
  truncated?: boolean
  took_ms: number
  /** F3: the reads behind `summary`, at most 16. Absent for replayed tools and old events. */
  provenance?: ToolCall[]
}

export interface AgentMessagePayload {
  text: string
  step_id?: string
  reply_to?: string // CommentID
}

export interface AgentHypothesisPayload {
  text: string
  based_on_steps: string[]
  confidence: string // "low" | "medium" | "high"
  confirmed: boolean
  /** F3: names and numbers in `text` that none of the cited steps show; at most 20. */
  unbacked_terms?: string[]
}

/**
 * The triage pass's first look (F6): one line shown as "triage, unverified". `failed` has no
 * headline and a fixed `reason` (timeout | model_error | budget | refused | no_tools | off).
 */
export interface AgentTriagePayload {
  status: 'ok' | 'failed'
  reason?: string
  headline?: string
  suspected_layer?: string
  confidence?: string
  evidence_refs: string[]
  next_check?: string
  model: string
  took_ms: number
}

// One model request's tokens, latency and estimated cost (docs/design/cost.md).
// Cost telemetry: the reducer keeps it out of the activity feed and the steps.
export interface AgentUsagePayload {
  model: string
  role: string // triage | main | handoff | compaction | postmortem | first_look | judge
  input_tokens: number
  cache_creation_input_tokens: number
  cache_read_input_tokens: number
  output_tokens: number
  latency_ms: number
  cost_usd?: number // omitted when no price is configured for the model
}

export interface CommentAddedPayload {
  comment_id: string
  step_id?: string
  text: string
  mentions_agent: boolean
  client_id?: string // the request's idempotency key (#459)
}

export interface SteerSuggestedPayload {
  suggestion_id: string
  text: string
  client_id?: string // the steer request's idempotency key (#459)
}

export interface SteerSentPayload {
  suggestion_id?: string // set when forwarding a suggestion
  text: string
  client_id?: string // the steer request's idempotency key (#459); none on a forward
}

export interface SteerDeclinedPayload {
  suggestion_id: string
  reason?: string
}

/** System actor; the driver's lease ran out with no renewal and no new driver. */
export interface ControlLeaseExpiredPayload {
  driver: Actor
  lease_until: string
}

export type ControlRequestedPayload = Record<string, never>

export interface ControlGrantedPayload {
  to: Actor
  lease_until: string
  /**
   * P2b: "sole_member" when the first member to join a driverless alert room became its driver
   * without asking (system actor). Absent for every other grant.
   */
  reason?: 'sole_member' | string
}

export interface ProposalStep {
  action: string
  layer: string
  params: unknown
  description: string
  /** What the agent will check after this step runs, e.g. "task 0 RUNNING within 60 s". */
  verify?: string
}

/** The target system's own answer to "would this be accepted" (registry check, connect validate). */
export interface DryRunResult {
  ok: boolean
  source: string
  text: string
  skipped?: string
}

/** What one write step would do, read before approval (proposal.previewed). */
export interface PreviewResult {
  target: string
  current: string
  expected: string
  /** Topology node ids reachable over data edges, at most 12; absent when unknown. */
  downstream?: string[]
  reversible: boolean
  rollback?: string
  dry_run?: DryRunResult
  /** X2: how much a recovery step would re-emit, read from the target by the action's Preview. Absent for a fix. */
  count?: number
  /** X2: what `count` counts. Anything else is shown without a unit word. */
  unit?: RecoveryUnit
}

export interface StepPreview {
  step_index: number
  preview?: PreviewResult
  error?: string
}

export interface ProposalPreviewedPayload {
  proposal_id: string
  steps: StepPreview[]
  at: string
}

/** The three states a staged step shows (events.StageResult.Outcome). */
export type StageOutcome = 'healthy' | 'failed' | 'not_applied'

/**
 * What one step did on the staging stack (X8, docs/api.md "X8 staging"). `detail`, `leftover` and
 * `target` are staging's own words, untrusted: render them as text.
 */
export interface StageResult {
  target?: string
  applied: boolean
  healthy: boolean
  detail?: string
  dry_run?: DryRunResult
  /** What the stage could not put back on staging; absent when staging is as it was. */
  leftover?: string
  duration_ms: number
}

/** One staged step: exactly one of `result` (staging gave a verdict) and `error` (it did not). */
export interface StageStepResult {
  step_index: number
  result?: StageResult
  error?: string
}

/** proposal.stage_started (actor: the driver): `steps` are the indexes of the stageable steps. */
export interface ProposalStageStartedPayload {
  proposal_id: string
  params_hash: string
  steps: number[]
  expires_at: string
}

/** proposal.staged (system actor): one entry per index of the matching stage_started. */
export interface ProposalStagedPayload {
  proposal_id: string
  params_hash: string
  steps: StageStepResult[]
  at: string
}

export interface VerificationCheck {
  layer: string
  name: string
  ok: boolean
  detail: string
}

export interface SinkCheck {
  table: string
  newest?: string
  baseline?: string
  passed: boolean
  passed_by?: string
}

export interface DataFlowCheck {
  checked: boolean
  fresh: boolean
  sinks?: SinkCheck[]
  detail?: string
}

/** X2: what a recovery count counts (docs/api.md, X2 recovery actions). */
export type RecoveryUnit = 'rows' | 'records'

/** X2: how a recovery proposal ended (events.RecoveryOutcomeFor). */
export type RecoveryOutcome = 'recovered' | 'partial' | 'not_recovered' | 'unverifiable'

/** X2: proposal.created.class. Absent reads as "fix". */
export type ProposalClass = 'fix' | 'recovery' | 'performance'

/**
 * agent.recovery_check (agent actor): one poll of a recovery step's Verify after it ran. Progress only,
 * never a verdict. `poll` counts from 1 and never exceeds `max_polls`.
 */
export interface AgentRecoveryCheckPayload {
  proposal_id: string
  step_index: number
  poll: number
  max_polls: number
  /** The gap the action set out to repair. */
  before: number
  /** What is still missing at this poll. */
  after: number
  unit: RecoveryUnit
}

/** The agent's verdict on a fix (agent.verification). `repeat` means "already verified", no checks. */
export interface AgentVerificationPayload {
  proposal_id: string
  checks: VerificationCheck[]
  data_flow?: DataFlowCheck
  recovered: boolean
  verdict: string
  repeat?: boolean
  /**
   * X2, recovery proposals only: the counted verdict. `before`/`after` are nullable pointers on the
   * wire (absent for `unverifiable`); `after: 0` is a real count, never "missing". `recovered` is true
   * only with outcome "recovered".
   */
  outcome?: RecoveryOutcome
  before?: number | null
  after?: number | null
  unit?: RecoveryUnit
}

export interface ProposalCreatedPayload {
  proposal_id: string
  title: string
  steps: ProposalStep[]
  risk: string
  rollback: string
  required_layers: string[]
  /** Hex SHA-256 of the frozen proposal (auth PR 4). Absent on events written before it. */
  params_hash?: string
  /**
   * P2b: the required_layers nobody owns; the room's driver approves them (`grant: "fallback"`).
   * Absent when every layer has an owner.
   */
  fallback_layers?: string[]
  /** X2/X6: set by the room service from the action registry. Absent reads as "fix". */
  class?: ProposalClass
  /** P3c: who wrote it. "driver" for POST /api/rooms/{id}/proposals (the event's actor is the driver); absent reads as "agent". */
  origin?: ProposalOrigin
}

/** P3c: proposal.created.origin. */
export type ProposalOrigin = 'agent' | 'driver'

/** One registered write action (GET /api/actions, P3c). Never carries client state or credentials. */
export interface ActionInfo {
  name: string
  layer: string
  /** The action's Describe(). */
  describe: string
  /** The action's ParamsSchema(): a JSON Schema object (lib/paramsForm reads it). */
  params_schema: Record<string, unknown>
  /** Whether the action implements actions.Preview. */
  preview: boolean
}

/** POST /api/rooms/{id}/proposals (P3c): a driver-authored fix. Only registered actions with their params. */
export interface CreateProposalBody {
  title: string
  risk: string
  rollback: string
  steps: { action: string; layer: string; params: Record<string, unknown>; description?: string }[]
}

/**
 * Who made a decision and how they logged in (auth PR 4, docs/design/auth.md 3.2).
 * `grant` is "user:…", "group:…", "backup:…" or "solo"; `method` is "oidc" or "dev".
 */
export interface Identity {
  iss?: string
  sub?: string
  session_id?: string
  method: string
  grant?: string
  auth_time?: string
}

export interface ApprovalGivenPayload {
  proposal_id: string
  layer: string
  params_hash?: string
  identity?: Identity
  /** P2b: what gave the right to approve. Absent before P2: read `identity.grant` instead. */
  grant?: 'owner' | 'solo' | 'fallback' | 'policy' | string
  /** P2b: "auto_approve" with `grant: "policy"`. */
  policy?: string
}

/**
 * proposal.scheduled (P2b): system actor, after proposal.previewed, at most once per proposal. Unless
 * cancelled, the policy approves every required layer at `run_at` and the steps run. `cancel_until`
 * equals `run_at` in this wave and is advertised only: a cancel after it still wins if it lands first.
 */
export interface ProposalScheduledPayload {
  proposal_id: string
  run_at: string
  policy: 'auto_approve' | string
  cancel_until: string
}

/** proposal.cancelled (P2b): `by` (also the actor) stopped the countdown; the proposal stays open. */
export interface ProposalCancelledPayload {
  proposal_id: string
  by: Actor
  reason: string
}

export interface ApprovalRejectedPayload {
  proposal_id: string
  layer: string
  reason: string
  params_hash?: string
  identity?: Identity
}

export interface ActionExecutedPayload {
  proposal_id: string
  step_index: number
  ok: boolean
  output: string
  /** The proposal's params_hash the executor checked before this step ran (auth PR 4). */
  params_hash?: string
  /** The step may or may not have run (its executor stopped mid-step); never retried. */
  outcome_unknown?: boolean
}

export interface HandoffSummary {
  what_broke: string
  why: string
  done: string[]
  ruled_out: string[]
  open: string[]
  risks: string[]
  /**
   * F3: checks the summary still fails after the agent's one correction pass.
   * Absent on a summary written before F3, or when the server omits an empty list.
   */
  violations?: HandoffViolation[]
}

/** One failed hand-off check; `detail` is agent text and may quote the summary. */
export interface HandoffViolation {
  rule: string
  field: string
  detail: string
}

/** handoff.requested (R0): a hand-off is being written; in flight until created / failed / expires_at. */
export interface HandoffRequestedPayload {
  request_id: string
  to: Actor
  stay_as_watcher: boolean
  expires_at: string
}

/** handoff.failed (R0): ends a request without a hand-off. `reason` is a fixed code, never error text. */
export interface HandoffFailedPayload {
  request_id: string
  reason: 'summary_failed' | 'not_driver' | 'room_closed' | 'internal_error' | string
}

export interface HandoffCreatedPayload {
  handoff_id: string
  /** The handoff.requested it completes; absent on events from before R0. */
  request_id?: string
  to: Actor
  summary: HandoffSummary
  stay_as_watcher: boolean
}

export interface HandoffAcceptedPayload {
  handoff_id: string
}

export interface TraceStage {
  layer: string
  name: string
  status: string // "seen" | "not_seen" | "stopped_here" | "unknown"
  facts?: Record<string, string>
}

export interface TracePinnedPayload {
  table: string
  key: string
  stages: TraceStage[]
}

export const LAYERS = [
  'postgres',
  'kafka-connect',
  'schema-registry',
  'kafka',
  'consumer',
  'warehouse',
  // Batch pipelines (P4): the orchestration and transform steps own their own
  // incident layers, alongside the shared warehouse layer.
  'orchestrator',
  'transform',
] as const

export type Layer = (typeof LAYERS)[number]

// ---- REST / WS objects (docs/api.md) ----

export interface ApiError {
  error: { code: string; message: string }
}

export type ApprovalValue = 'approved' | 'rejected' | 'pending'

export interface ProposalSummary {
  proposal_id: string
  title: string
  steps: ProposalStep[]
  risk: string
  rollback: string
  required_layers: string[]
  approvals: Record<string, ApprovalValue>
  /** After auth PR 4. "" only for a proposal that cannot be hashed. */
  params_hash?: string
  /**
   * P1a: every required layer approved and nothing ran because write mode is off. Absent
   * otherwise, and on a server from before P1a. The only value in the P wave.
   */
  blocked?: 'write_mode_off'
  /** P2b: copied from proposal.created.fallback_layers; [] when every layer has an owner, absent before P2b. */
  fallback_layers?: string[]
  /**
   * P2b: while a proposal.scheduled is pending. The room GET leaves it out once run_at is more than
   * 45 s in the past (#652); the web folds proposal.scheduled itself and never reads this field.
   */
  scheduled?: { run_at: string; cancel_until: string }
  /** X2: copied from proposal.created.class; absent reads as "fix". */
  class?: ProposalClass
  /** P3c: copied from proposal.created.origin; absent reads as "agent". */
  origin?: ProposalOrigin
  /** X8: step indexes whose action implements actions.Stager; [] or absent when none (and before X8c). */
  stageable?: number[]
  /** X8: a proposal.stage_started with no proposal.staged yet, before its expires_at. */
  stage_running?: { by: Actor; started_at: string; expires_at: string; steps: number[] }
  /** X8: the newest proposal.staged payload, kept while its params_hash differs from the proposal's. */
  staged?: ProposalStagedPayload
  /** X8: a step's action is in require_staging: and no healthy stage for the current params_hash exists. */
  staging_required?: boolean
}

/**
 * RoomSnapshot.mode (docs/api.md, P-wave snapshot fields; P1a, P2b). It arrives with the room GET
 * and does not change over the socket. A server without it (before P1a) has write mode on.
 */
export interface RoomMode {
  /** The server's PIPEROOM_WRITE_MODE: false means approved fixes are recorded and never run. */
  write: boolean
  /** True when the caller may approve every layer of the room's pipeline. */
  solo: boolean
  /** The pipeline's auto_approve: list from owners.yaml. */
  auto_approve: string[]
  /** X8: the pipeline declares staging: and write mode is on, so "Try in staging" may exist. Absent before X8c. */
  staging?: boolean
}

export interface SuggestionSummary {
  suggestion_id: string
  from: { id: string; name: string; verified?: boolean }
  text: string
}

export interface MemberSummary {
  id: string
  name: string
  verified?: boolean
  role: string
}

export interface DriverSummary {
  id: string
  name: string
  verified?: boolean
  lease_until?: string
}

export interface AgentStatusSummary {
  state: string
  detail?: string
}

export interface HypothesisSummary {
  text: string
  based_on_steps: string[]
  confidence: string
  confirmed: boolean
  /** F3: see AgentHypothesisPayload. */
  unbacked_terms?: string[]
  /** F9: seq of the agent.hypothesis event. */
  seq?: number
  /** F9: latest verdict per person on `seq`. */
  verdicts?: { verdict: HypothesisVerdict; by: { id: string; name: string; verified?: boolean }; note?: string; at: string }[]
}

export type HypothesisVerdict = 'confirmed' | 'disputed'

/** hypothesis.verdict (F9). hypothesis_seq is the agent.hypothesis it answers. */
export interface HypothesisVerdictPayload {
  verdict: HypothesisVerdict
  note?: string
  hypothesis_seq: number
}

/** approval.nudged (F9): ids only. */
export interface ApprovalNudgedPayload {
  proposal_id: string
  layer: string
}

/** The newest agent.triage of a room (snapshot `triage`, F6). */
export interface TriageSummary {
  status: 'ok' | 'failed'
  reason?: string
  headline?: string
  suspected_layer?: string
  confidence?: string
  evidence_refs: string[]
  next_check?: string
  at: string
}

// ---- setup rooms (U wave; mirror of internal/events/events.go and the snapshot docs) ----

/** The body of POST /api/setup (events.SetupRequest). `tenant` is required only with a tenants: block. */
export interface SetupRequest {
  pipeline_id: string
  tenant?: string
  title: string
  source_kind: string // postgres | mysql | sqlserver | mongodb | oracle
  target: string // compose | host | strimzi | msk | confluent | aiven | dbzserver | embedded
  topic_prefix: string
  tables: string[]
  connectors?: string[]
  targets: SetupTargetInput[]
  repos?: string[]
}

/** One host or account a setup room may reach (events.SetupTarget). Class is a floor; "" lets the server decide. */
export interface SetupTargetInput {
  kind: string // postgres | kafka | connect | registry | kubernetes | aws | confluent | aiven | host
  address: string
  class?: string // local | shared | production
}

/** POST /api/setup's answer: the room, its single-use pairing code and the line to paste. */
export interface CreateSetupResponse {
  room: RoomSnapshot
  code: string
  expires_at: string
  command: string
}

/** The document's snapshot shape of a setup room (`setup` on the room GET; absent on list rows). */
export interface SetupSnapshot {
  mode: boolean
  /** null before pairing; a lost runner keeps its record with connected false. */
  runner: SetupRunnerSummary | null
  plan_id?: string
  phase?: string
  step_index?: number
  bounds?: SetupBounds
  targets?: SetupTargetSummary[]
  provisional?: { topic_prefix?: string; tables?: string[]; connectors?: string[] }
  maintenance_window?: { start: string; end: string }
  verified?: SetupRung[]
  blocked?: { reason: string; question: string }
  watched_by?: string
}

export interface SetupRunnerSummary {
  label: string
  connected: boolean
  tools?: Record<string, string>
}

export interface SetupBounds {
  attempts_per_step: number
  steps_per_plan: number
  wall_clock_s: number
  token_budget: number
}

export interface SetupTargetSummary {
  kind: string
  address: string
  class: string
}

/** setup.plan: the current plan; a new plan names the one it replaces in `supersedes`. */
export interface SetupPlanStep {
  index: number
  op: string
  target: string
  tier: string // T0..T3
  layer: string
  verify?: string
}

export interface SetupPlanPhase {
  name: string
  steps: SetupPlanStep[]
}

export interface SetupPlanPayload {
  plan_id: string
  supersedes?: string
  phases: SetupPlanPhase[]
}

export interface SetupRunnerAttachedPayload {
  runner_label: string
  os: string
  arch: string
  tools?: Record<string, string>
  project_dir?: string
  catalogue_version?: string
  protocol_version?: number
  protocol_minor?: number
}

export interface SetupRunnerLostPayload {
  runner_label: string
  last_seen: string
}

/** setup.blocked: a bound stopped the run; `question` is agent text (untrusted). */
export interface SetupBlockedPayload {
  reason: string
  step_index?: number
  question: string
}

export interface SetupRung {
  name: string
  ok: boolean
  step_id?: string
  receipt_id?: string
  note?: string
}

export interface SetupVerifiedPayload {
  plan_id: string
  nonce_hash: string
  rungs: SetupRung[]
  first_event_latency_ms?: number
}

/** setup.awaiting_change (rung 8): the person must change the table; example_sql comes from the driver. */
export interface SetupAwaitingChangePayload {
  table: string
  example_sql: string
}

export interface SetupTargetRegisteredPayload {
  kind: string
  address: string
  class: string
}

export interface SetupTargetClassedPayload {
  target: string
  class: string
  reason: string
  by: string // "server" | "person"
}

export interface SetupPipelineExtendedPayload {
  add_connectors?: string[]
  add_tables?: string[]
  add_consumer_groups?: string[]
  add_sink_tables?: string[]
}

export interface SetupMaintenanceWindowPayload {
  start: string
  end: string
}

/** pipeline.registered: the owners entry was written (or opened as a PR); `watched_by` is server | local. */
export interface PipelineRegisteredPayload {
  pipeline_id: string
  tenant?: string
  path?: string
  pull_request_url?: string
  watched_by: string
}

export interface RoomSnapshot {
  id: string
  title: string
  severity: string
  pipeline_id: string
  status: string // "open" | "closed"
  opened_at: string
  closed_at?: string
  resolution?: string
  last_seq: number
  driver: DriverSummary | null
  agent_status: AgentStatusSummary
  hypothesis: HypothesisSummary | null
  /** F6: absent on an older server; null until the first agent.triage. */
  triage?: TriageSummary | null
  open_proposals: ProposalSummary[]
  pending_suggestions: SuggestionSummary[]
  members: MemberSummary[]
  /** F8: a room.labelled exists. */
  labelled?: boolean
  /** Shadow room (docs/api.md): the agent ran silently, no proposals; its insight is zeros until revealed. */
  shadow?: boolean
  /** F11: cost and clock facts folded server-side; absent on an older server. */
  insight?: RoomInsight
  /** F10: the actor of room.closed; closed rooms only. */
  closed_by?: Actor
  /** F2: the post-mortem draft's state; absent on an older server. */
  postmortem?: PostmortemSummary
  /** P1a/P2b: on the room GET only (list rows have none); absent on an older server, which means write mode on. */
  mode?: RoomMode
  /** X3: room.created.origin ("forecast" for a forecast room); absent for every other room. */
  origin?: string
  /** X3: the newest forecast signal of the room's evidence; absent when none. */
  forecast?: ForecastSummary
  /** U wave: present on a setup room only (docs/api.md "U-wave snapshot fields"). */
  setup?: SetupSnapshot
}

/**
 * The trend metrics the watcher samples (clients.WatchMetrics, docs/api.md "Trends"). Units: bytes for
 * slot_wal_bytes and db_size_bytes, records for dlq_records and group_lag, seconds for topic_oldest_unread_age.
 */
export const WATCH_METRICS = [
  'slot_wal_bytes',
  'dlq_records',
  'group_lag',
  'db_size_bytes',
  'topic_oldest_unread_age',
] as const
export type WatchMetric = (typeof WATCH_METRICS)[number]

/** One sample of GET /api/pipelines/{pid}/trends (clients.SamplePoint): oldest first, `value` in the metric's unit. */
export interface TrendPoint {
  at: string
  value: number
}

/**
 * RoomSnapshot.forecast (X3, events.ForecastSignal minus signal/severity, plus `at`): `current`, `limit` and
 * `slope_per_hour` in the metric's unit; `eta` is when the fitted line reaches `limit`; `object` names the slot,
 * topic, group or database the series follows (absent for a pipeline total).
 */
export interface ForecastSummary {
  metric: string
  object?: string
  current: number
  limit: number
  eta: string
  slope_per_hour: number
  r2: number
  at?: string
}

/** RoomSnapshot.postmortem (docs/api.md, F2). */
export interface PostmortemSummary {
  status: 'none' | 'drafting' | 'drafted' | 'failed'
  source?: PostmortemSource
  reason?: string
}

export type PostmortemSource = 'handoff' | 'model' | 'events'

/** postmortem.drafted (F2): system actor, after room.closed. `violations` are rule names only. */
export interface PostmortemDraftedPayload {
  source: PostmortemSource
  summary_ref?: string
  summary?: HandoffSummary
  violations?: string[]
}

/** postmortem.failed (F2): reason is summary_failed | internal_error, never error text. */
export interface PostmortemFailedPayload {
  reason: string
}

/** RoomSnapshot.insight (docs/api.md, F11). */
export interface RoomInsight {
  first_hypothesis_at: string | null
  /** Whole seconds from opened_at to the first hypothesis; -1 before one exists. */
  time_to_first_hypothesis_s: number
  final_confidence: string
  steps: number
  tool_calls: number
  tool_errors: number
  usage: {
    calls: number
    unpriced_calls: number
    tokens: number
    cost_usd: number | null
    latency_ms_total: number
  }
}

/** GET /api/pipelines/{pid}/agent-stats (F11): the agent's track record on one pipeline. */
export interface AgentStats {
  pipeline_id: string
  window_days: number
  generated_at: string
  rooms: number
  hypothesised: number
  labelled: number
  graded: number
  layer_correct: number
  root_cause: { yes: number; partial: number; no: number }
  by_confidence: { confidence: string; runs: number; correct: number; accuracy: number }[]
  median_time_to_first_hypothesis_s: number
  median_human_mttr_s: number
  median_cost_usd: number
  cost_available: boolean
  enough_data: boolean
  shadow: { rooms: number; graded: number; layer_correct: number }
  /** X6 infrastructure cost hints, last 7 days of watch samples; null without history, absent before X6d. */
  infra?: AgentStatsInfra | null
}

/**
 * X6 `infra` of agent-stats (docs/api.md): units only, never a currency. Each metric is null when the
 * pipeline has no sample of it in the window; `from`/`to` are the first and newest sample used.
 */
export interface AgentStatsInfra {
  from: string
  to: string
  samples: number
  wal_retained_gb_days: number | null
  dlq_records: number | null
  oldest_unread_age_s: number | null
}

export interface TopologyNode {
  id: string
  /** The exhaustive node-kind union (nodeKinds.ts); docs/api.md "Topology". */
  kind: NodeKind
  name: string
  layer: string
  owner: string
  health: 'ok' | 'warn' | 'fail' | 'unknown'
  facts?: Record<string, string>
}

export interface TopologyEdge {
  from: string
  to: string
  /** Absent means 'data'. 'depends_on': `from` needs `to` to work; no records flow (docs/api.md "Topology"). */
  kind?: 'data' | 'depends_on'
  /** Absent means 'mapped' (proven). 'inferred': assumed from owners.yaml, shown as unconfirmed. */
  mapping?: 'mapped' | 'inferred'
}

export interface Topology {
  pipeline_id: string
  nodes: TopologyNode[]
  edges: TopologyEdge[]
  /** P4: a reader the pipeline's kind needs is missing or not supported, so some nodes are left out. Absent when false. */
  partial?: boolean
}

export interface Me {
  id: string
  name: string
  verified?: boolean
  layers: string[]
  /** After auth PR 3; absent on an older server (treated as member). */
  role?: 'member' | 'viewer'
  /** The IdP sent no groups claim (docs/design/auth.md 2.2). */
  groups_missing?: boolean
  /** This user is PIPEROOM_SOLO_USER (omitted otherwise): approvals need a typed confirm. */
  solo?: boolean
  /** `admin: true` in users.yaml (omitted otherwise): may open the Settings pages (/api/admin/*). */
  admin?: boolean
}

/** One row of GET /api/people: a person a driver may grant control or hand off to. */
/** One row of GET /auth/users (dev mode only): who the dev login screen may pick. */
export interface AuthUser {
  id: string
  name: string
  layers: string[]
}

/** One row of GET /api/pipelines. */
export interface PipelineInfo {
  id: string
  title: string
  shadow: boolean
  /** P0 item 8 (server P4a-4): `cdc` or `batch`. Missing (an older server) means `cdc`. */
  kind?: 'cdc' | 'batch'
}

/** FleetPipeline.forecasts[] (X5): one firing forecast; `metric` is an X3 sample metric name. */
export interface FleetForecast {
  metric: string
  eta: string
}

/** FleetPipeline.last_verdict (X5): the human judgement of the pipeline's last judged room. */
export interface FleetVerdict {
  room_id: string
  /** `confirmed` | `disputed`, or '' when the room has only a grade. */
  verdict: string
  /** null when the room was not graded. */
  layer_correct: boolean | null
  at: string
}

export type FleetHealth = 'ok' | 'warn' | 'fail' | 'unknown'

/** One row of GET /api/fleet (docs/api.md "Fleet view", X5-0). */
export interface FleetPipeline {
  pipeline_id: string
  title: string
  kind: string
  tenant: string
  team: string
  health: FleetHealth
  /** A topology node id as GET .../topology names it (`connect:<connector>`, `pg:<table>`); '' when nothing fires. */
  worst_node: string
  open_rooms: number
  /** Watcher signal names now firing or acked, worst first. */
  signals_firing: string[]
  changes_24h: number
  /** Soonest ETA first. */
  forecasts: FleetForecast[]
  last_verdict: FleetVerdict | null
  /** Omitted when the watcher never checked the pipeline. */
  checked_at?: string
}

/** One F8 `pipeline_changes` row as GET /api/fleet/changes returns it (already masked; untrusted text). */
export interface PipelineChange {
  id: number
  pipeline_id: string
  kind: string
  object: string
  at: string
  actor?: string
  source: string
  fingerprint: string
  before?: unknown
  after?: unknown
  summary: string
}

/** GET /api/fleet/changes: one page, newest first; `next` is '' on the last page. */
export interface FleetChangesPage {
  changes: PipelineChange[]
  next: string
}

export interface Person {
  id: string
  name: string
  verified?: boolean
  /** Only for IdP users with a verified email. */
  email?: string
  /** F9: the layers the person may approve; absent from an old server. */
  layers?: string[]
}

export interface PresenceMember {
  id: string
  name: string
  verified?: boolean
  state: 'viewing' | 'away'
  /** F9: when the member entered `state`, RFC 3339 UTC; absent from an old server. */
  since?: string
}

export interface EventsPage {
  events: Event[]
  last_seq: number
}

/** Frames of the room list socket (GET /api/rooms/ws, F6). */
export type ListWsServerMessage =
  | { type: 'rooms'; rooms: RoomSnapshot[] }
  | { type: 'room'; room: RoomSnapshot }
  | { type: 'room_deleted'; room_id: string }
  | { type: 'error'; error: { code: string; message: string } }

export type WsServerMessage =
  | { type: 'event'; event: Event }
  | { type: 'presence'; members: PresenceMember[] }
  | { type: 'error'; error: { code: string; message: string } }

// ---- Shadow mode (docs/api.md "Shadow mode", docs/design/shadow.md 5.2) ----

/** shadow.revealed: numbers only, never the hypothesis text. */
export interface ShadowRevealedPayload {
  status: ShadowStatus
  time_to_first_hypothesis_s: number // -1 when there was no hypothesis
  time_to_final_hypothesis_s: number
  human_mttr_s: number
  finished_after_close: boolean
  tool_calls: number
  tokens: number
}

export interface ShadowLabelledPayload {
  actual_layer: string
  root_cause_terms: string[]
  identity?: Identity
}

export type ShadowRootCause = 'yes' | 'partial' | 'no'

export interface ShadowGradedPayload {
  layer_correct: boolean
  root_cause: ShadowRootCause
  notes?: string
  identity?: Identity
}

export type ShadowStatus =
  | 'queued'
  | 'running'
  | 'hypothesised'
  | 'no_hypothesis'
  | 'error'
  | 'budget'
  | 'interrupted'

/** GET /api/rooms/{id}/shadow -> run. Times are null when unset. */
export interface ShadowRun {
  room_id: string
  status: ShadowStatus
  model: string
  pipeline_map: string
  started_at: string | null
  first_hypothesis_at: string | null
  final_hypothesis_at: string | null
  finished_at: string | null
  finished_after_close: boolean
  final_hypothesis: AgentHypothesisPayload | null
  tool_calls: number
  tokens: number
  revealed_at: string | null
}

/** An event of the shadow log: its own seq, never fed to the room reducer. */
export type ShadowEvent = Event & { shadow: true }

export interface ShadowRoomContext {
  created?: Event | null
  labelled?: Event | null
  graded?: Event | null
}

export interface ShadowResponse {
  run: ShadowRun
  events: ShadowEvent[]
  room?: ShadowRoomContext
}

export interface ShadowBlindBody {
  actual_layer: string
  root_cause_terms: string[]
}

export interface ShadowGradeBody {
  layer_correct: boolean
  root_cause: ShadowRootCause
  notes?: string
}

/** One closed shadow room in GET /api/shadow/metrics. */
export interface ShadowMetricsRoom {
  room_id: string
  pipeline_id: string
  alert_source: string
  status: ShadowStatus
  opened_at: string
  closed_at: string
  labelled: boolean
  graded: boolean
  layer_correct: boolean | null
  root_cause: ShadowRootCause | ''
  time_to_first_s: number // -1 when there was no value
  time_to_final_s: number
  human_mttr_s: number
  tool_calls: number
  tokens: number
  finished_after_close: boolean
}

export interface ShadowMetrics {
  rooms: number
  labelled: number
  graded: number
  layer_correct: number
  root_cause: { yes: number; partial: number; no: number }
  by_status: Partial<Record<ShadowStatus, number>>
  median_time_to_first_s: number // -1 when no room has the value
  median_time_to_final_s: number
  median_human_mttr_s: number
  total_tokens: number
  tokens_available: boolean
  per_room: ShadowMetricsRoom[]
}

// Admin settings and connection tests (docs/api.md, "Admin: settings and connection tests").
// Field names are the server's JSON names. A Go server may send `null` for an empty list, so
// the list fields are read through `lines()` in lib/settings.ts, never indexed directly.

/** Where a connection applies. */
export type ConnectionScope = 'process-wide' | 'pipeline'

/** The kinds of connection the server probes; an unknown kind from a newer server is still shown. */
export type ConnectionKind = 'source_db' | 'kafka' | 'connect' | 'schema_registry' | 'warehouse'

/** One row of a connection's settings table. A secret never carries a value: `set` says whether one is present. */
export interface ConnectionSetting {
  key: string
  /** Empty for a secret. URLs and brokers come without user info, query or fragment. */
  display: string
  secret: boolean
  /** Only for a secret: whether a value is present. */
  set?: boolean
  /** `env NAME`, `file /path` or `value`. */
  from: string
}

/** One connection the server runs with. `id` is what POST /api/admin/connections/test takes. */
export interface AdminConnection {
  id: string
  scope: ConnectionScope
  /** Set when `scope` is `pipeline`, otherwise empty. */
  pipeline_id: string
  kind: ConnectionKind | (string & {})
  used_by: string[]
  settings: ConnectionSetting[]
}

/** The principals that own one layer of a pipeline, as written in owners.yaml (`user:<id>`, `group:<name>`). */
export interface AdminLayerOwners {
  layer: string
  principals: string[]
}

/** One pipeline of owners.yaml. */
export interface AdminPipeline {
  id: string
  title: string
  tenant: string
  /** `own` or `process-wide`. */
  connections: string
  connectors: string[]
  consumer_groups: string[]
  source_tables: string[]
  sink_tables: string[]
  owners: AdminLayerOwners[]
  shadow: boolean
}

/** One person of users.yaml. */
export interface AdminPerson {
  id: string
  name: string
  layers: string[]
  admin: boolean
}

/** GET /api/admin/config: what the server runs with now. */
export interface AdminConfig {
  /** `file` in phase 1. */
  source: string
  /** Base name only. */
  owners_file: string
  /** Base name only. */
  users_file: string
  connections: AdminConnection[]
  pipelines: AdminPipeline[]
  people: AdminPerson[]
}

/** `ok`, `fail`, `warn` (works, but should change) or `skip` (an earlier check failed). */
export type ProbeStatus = 'ok' | 'fail' | 'warn' | 'skip'

/** One check of a connection test. `problem`, `fix` and `detail` are fixed text, never a server's raw error. */
export interface ProbeCheck {
  id: string
  status: ProbeStatus
  ms: number
  problem?: string
  fix?: string
  detail?: string
}

/** POST /api/admin/connections/test: `ok` is false when any check failed. */
export interface ProbeResult {
  kind: string
  ok: boolean
  took_ms: number
  checks: ProbeCheck[]
}
