// Mirror of internal/events/events.go (contract) + docs/api.md objects.
// Field names and JSON match the Go structs exactly.

// ---- actors ----

export type ActorKind = 'human' | 'agent' | 'system'

export interface Actor {
  kind: ActorKind
  id: string
  name: string
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
  /** The pipeline had shadow mode on when the room opened (docs/design/shadow.md). */
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

export interface AgentToolResultPayload {
  step_id: string
  tool_use_id: string
  tool: string
  summary: string
  data?: unknown
  is_error: boolean
  truncated?: boolean
  took_ms: number
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
}

// One model request's tokens, latency and estimated cost (docs/design/cost.md).
// Cost telemetry: the reducer keeps it out of the activity feed and the steps.
export interface AgentUsagePayload {
  model: string
  role: 'triage' | 'main' | 'handoff' | 'compaction'
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
}

export interface SteerSuggestedPayload {
  suggestion_id: string
  text: string
}

export interface SteerSentPayload {
  suggestion_id?: string // set when forwarding a suggestion
  text: string
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
}

export interface HandoffSummary {
  what_broke: string
  why: string
  done: string[]
  ruled_out: string[]
  open: string[]
  risks: string[]
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
}

export interface SuggestionSummary {
  suggestion_id: string
  from: { id: string; name: string }
  text: string
}

export interface MemberSummary {
  id: string
  name: string
  role: string
}

export interface DriverSummary {
  id: string
  name: string
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
  open_proposals: ProposalSummary[]
  pending_suggestions: SuggestionSummary[]
  members: MemberSummary[]
  /** True when the room opened as a shadow room: the agent ran silently, no proposals. */
  shadow?: boolean
  /** F8: a room.labelled exists. */
  labelled?: boolean
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
  layers: string[]
  /** After auth PR 3; absent on an older server (treated as member). */
  role?: 'member' | 'viewer'
  /** The IdP sent no groups claim (docs/design/auth.md 2.2). */
  groups_missing?: boolean
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
  /** Only for IdP users with a verified email. */
  email?: string
  /** F9: the layers the person may approve; absent from an old server. */
  layers?: string[]
}

export interface PresenceMember {
  id: string
  name: string
  state: 'viewing' | 'away'
  /** F9: when the member entered `state`, RFC 3339 UTC; absent from an old server. */
  since?: string
}

export interface EventsPage {
  events: Event[]
  last_seq: number
}

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
  layer_correct: boolean
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
