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
  | 'comment.added'
  | 'steer.suggested'
  | 'steer.sent'
  | 'control.requested'
  | 'control.granted'
  | 'proposal.created'
  | 'approval.given'
  | 'approval.rejected'
  | 'action.executed'
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
}

export interface ApprovalGivenPayload {
  proposal_id: string
  layer: string
}

export interface ApprovalRejectedPayload {
  proposal_id: string
  layer: string
  reason: string
}

export interface ActionExecutedPayload {
  proposal_id: string
  step_index: number
  ok: boolean
  output: string
}

export interface HandoffSummary {
  what_broke: string
  why: string
  done: string[]
  ruled_out: string[]
  open: string[]
  risks: string[]
}

export interface HandoffCreatedPayload {
  handoff_id: string
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
  kind: 'source_table' | 'connector' | 'topic' | 'consumer_group' | 'sink_table'
  name: string
  layer: string
  owner: string
  health: 'ok' | 'warn' | 'fail' | 'unknown'
  facts?: Record<string, string>
}

export interface TopologyEdge {
  from: string
  to: string
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
}

/** One row of GET /api/people: a person a driver may grant control or hand off to. */
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
