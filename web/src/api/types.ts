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
  | 'control.requested'
  | 'control.granted'
  | 'proposal.created'
  | 'approval.given'
  | 'approval.rejected'
  | 'action.executed'
  | 'handoff.requested'
  | 'handoff.failed'
  | 'handoff.created'
  | 'handoff.accepted'
  | 'trace.pinned'

export interface Event {
  room_id: string
  seq: number
  type: EventType
  actor: Actor
  payload: unknown
  at: string
}

// ---- payloads (one per Type, matching events.go) ----

export interface RoomCreatedPayload {
  title: string
  severity: string // "sev1".."sev4"
  pipeline_id: string
  alert?: unknown
}

export interface RoomClosedPayload {
  resolution: string
}

export interface AgentStatusPayload {
  state: string // "working" | "idle" | "waiting_approval" | "error" | "done"
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
}

export interface PresenceMember {
  id: string
  name: string
  state: 'viewing' | 'away'
}

export interface EventsPage {
  events: Event[]
  last_seq: number
}

export type WsServerMessage =
  | { type: 'event'; event: Event }
  | { type: 'presence'; members: PresenceMember[] }
  | { type: 'error'; error: { code: string; message: string } }
