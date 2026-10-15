// Mirror of internal/events/events.go (contract) + docs/api.md objects.
// Field names and JSON match the Go structs exactly.

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
  | 'owner.paged'
  | 'proposal.created'
  | 'proposal.withdrawn'
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
  | 'agent.verification'
  | 'postmortem.drafted'
  | 'postmortem.failed'

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

/** comment.added `kind` (Q0): absent or "" is a plain comment; "fix_note" logs a fix made outside Piperoom. */
export type CommentKind = 'fix_note'

export interface CommentAddedPayload {
  comment_id: string
  step_id?: string
  text: string
  mentions_agent: boolean
  kind?: CommentKind | ''
  client_id?: string // the request's idempotency key (#459)
}

/**
 * owner.paged (Q0, QA G2): the owners of `layer` were asked to look at the room. `owners` are user
 * ids the server resolved (never chosen by the caller), `[]` when the layer has nobody. `reason`
 * can be agent text: render it as text. `auto` is true when the server paged on its own.
 */
export interface OwnerPagedPayload {
  layer: string
  owners: string[]
  reason: string
  proposal_id?: string
  auto?: boolean
}

/** Body of POST /api/rooms/{id}/page. */
export interface PageInput {
  layer: string
  reason: string
  proposal_id?: string
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

/** The agent's verdict on a fix (agent.verification). `repeat` means "already verified", no checks. */
export interface AgentVerificationPayload {
  proposal_id: string
  checks: VerificationCheck[]
  data_flow?: DataFlowCheck
  recovered: boolean
  verdict: string
  repeat?: boolean
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
   * Q0 (QA G5): layers the fix does not write to but whose data it changes. They are inside
   * `required_layers` already, so their owners must approve too.
   */
  impacted_layers?: string[]
  /** Q0: "low" | "medium" | "high". Absent on events written before Q0. */
  risk_level?: string
  /** Q0: why the server raised the risk or added impacted layers (at most 8, each at most 300 characters). */
  impact_reasons?: string[]
}

/**
 * proposal.withdrawn (Q0, QA G8/G9): the proposal ended without running. By the agent, the driver,
 * or the system actor right after room.closed with reason "room closed".
 */
export interface ProposalWithdrawnPayload {
  proposal_id: string
  reason: string
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
  /** Q0: the driver wrote the summary (the event's actor), no model was called. */
  manual?: boolean
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
  /** Q0: copied from proposal.created. */
  impacted_layers?: string[]
  risk_level?: string
  impact_reasons?: string[]
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

/** One entry of the snapshot's `pages` (Q0): the newest owner.paged of a layer, open rooms only. */
export interface PageSummary {
  layer: string
  /** User ids; compare with `/api/me` id. */
  owners: string[]
  reason: string
  proposal_id?: string
  by: Actor
  auto?: boolean
  at: string
}

export interface RoomSnapshot {
  id: string
  title: string
  severity: string
  pipeline_id: string
  status: string // "open" | "closed"
  opened_at: string
  closed_at?: string
  /** Who closed the room (F10); absent while open. */
  closed_by?: Actor
  resolution?: string
  last_seq: number
  driver: DriverSummary | null
  agent_status: AgentStatusSummary
  hypothesis: HypothesisSummary | null
  /** F6: absent on an older server; null until the first agent.triage. */
  triage?: TriageSummary | null
  /** Q0: the newest owner.paged per layer, in seq order; absent on an older server, [] on a closed room. */
  pages?: PageSummary[]
  open_proposals: ProposalSummary[]
  pending_suggestions: SuggestionSummary[]
  members: MemberSummary[]
  /** F8: a room.labelled exists. */
  labelled?: boolean
  /** Shadow room (docs/api.md): the agent ran silently, no proposals; its insight is zeros until revealed. */
  shadow?: boolean
  /** F11: cost and clock facts folded server-side; absent on an older server. */
  insight?: RoomInsight
  /** Q0: injection kinds found in the title (`[]` when clean). The title is still shown, as text. */
  title_flags?: string[]
  /** F2: the post-mortem draft's state; absent on an older server. */
  postmortem?: PostmortemSummary
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
}

export interface TopologyNode {
  id: string
  kind: 'source_table' | 'connector' | 'topic' | 'dlq_topic' | 'consumer_group' | 'sink_table'
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
  /** Group names of the session, so `group:<name>` page owners can match. Not sent by the server yet (docs/api.md /api/me has none). */
  groups?: string[]
  /** This user is PIPEROOM_SOLO_USER (omitted otherwise): approvals need a typed confirm. */
  solo?: boolean
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
