// UI state derived from the room snapshot + the event log. This is the shape
// the reducer (state/reducer.ts) folds events into.
import type {
  ActionExecutedPayload,
  Actor,
  AgentStatusPayload,
  HandoffSummary,
  HypothesisSummary,
  Identity,
  ProposalStep,
  RoomSnapshot,
  TraceStage,
} from '../api/types'

export type Severity = 'sev1' | 'sev2' | 'sev3' | 'sev4'
export type RoomStatus = 'open' | 'closed'
export type ApprovalValue = 'approved' | 'rejected' | 'pending'

export interface DriverInfo {
  id: string
  name: string
  lease_until?: string
}

export interface AgentStatusState {
  state: AgentStatusPayload['state']
  detail?: string
}

export interface HypothesisState {
  text: string
  based_on_steps: string[]
  confidence: HypothesisSummary['confidence']
  confirmed: boolean
}

export interface ToolCallState {
  tool_use_id: string
  tool: string
  input: unknown
  summary?: string
  data?: unknown
  is_error?: boolean
  truncated?: boolean
  took_ms?: number
}

export interface AgentMessageState {
  text: string
  at: string
}

export interface StepState {
  id: string
  title: string
  tool_calls: ToolCallState[]
  messages: AgentMessageState[]
}

export interface CommentState {
  id: string
  step_id?: string
  text: string
  mentions_agent: boolean
  author: Actor
  at: string
  replies: AgentMessageState[]
}

/**
 * pending: waiting in the driver's queue. accepted: the driver sent it (steer.sent with its id).
 * declined: the driver closed it without sending (steer.declined).
 */
export type SuggestionStatus = 'pending' | 'accepted' | 'declined'

export interface SuggestionState {
  suggestion_id: string
  from: Actor
  text: string
  status: SuggestionStatus
  /** When the suggestion was made. */
  at?: string
  /** The driver's note when declining (steer.declined `reason`). */
  decline_reason?: string
}

/** Who decided a layer, how they logged in, and the hash they decided on. */
export interface DecisionState {
  outcome: 'approved' | 'rejected'
  by: Actor
  identity?: Identity
  params_hash?: string
  reason?: string
  /** When the decision was made (the event's time); absent on a decision built without an event. */
  at?: string
}

export interface ProposalState {
  proposal_id: string
  title: string
  steps: ProposalStep[]
  risk: string
  rollback: string
  required_layers: string[]
  approvals: Record<string, ApprovalValue>
  /** Per layer, for the layers that have been decided. */
  decisions: Record<string, DecisionState>
  /**
   * The params_hash the UI shows and sends back with approve and reject. Taken from
   * proposal.created (or the snapshot for an older event); "" when there is none.
   */
  params_hash: string
  status: 'open' | 'approved' | 'rejected'
  executed: ActionExecutedPayload[]
}

export interface MemberState {
  id: string
  name: string
  role: string
}

export interface ControlRequestState {
  from: Actor
}

export interface TraceState {
  table: string
  key: string
  stages: TraceStage[]
}

export interface HandoffState {
  handoff_id: string
  to: Actor
  summary: HandoffSummary
  stay_as_watcher: boolean
  status: 'pending' | 'accepted'
}

/** A hand-off the agent is still writing the summary for (handoff.requested, R0). */
export interface HandoffRequestState {
  request_id: string
  to: Actor
  stay_as_watcher: boolean
  expires_at: string
}

/** The last hand-off request that ended without a hand-off (handoff.failed). */
export interface HandoffFailureState {
  request_id: string
  reason: string
}

/** The alert that opened the room, from room.created's `alert` webhook body. */
export interface AlertInfo {
  /** Best-effort source inferred from the raw body shape; null when unknown. */
  source: string | null
}

export interface ActivityEntry {
  seq: number
  at: string
  actor: Actor
  text: string
}

export interface RoomState {
  id: string
  title: string
  severity: Severity
  pipeline_id: string
  status: RoomStatus
  opened_at: string
  closed_at?: string
  resolution?: string
  last_seq: number
  /**
   * The snapshot's last_seq: how many events the first replay (after=0) has to fold. 0 when
   * unknown (fixtures, empty room). Drives the replay progress bar.
   */
  replay_target: number
  driver: DriverInfo | null
  /** Set when room.created carried an alert webhook body (alert-opened room). */
  alert: AlertInfo | null
  agent_status: AgentStatusState
  hypothesis: HypothesisState | null
  steps: StepState[]
  messages: AgentMessageState[]
  comments: CommentState[]
  suggestions: SuggestionState[]
  proposals: ProposalState[]
  /**
   * proposal_id -> params_hash from the snapshot's open_proposals. The server computes
   * it for proposals whose proposal.created event predates the hash, so the reducer
   * falls back to it when the event has none.
   */
  snapshot_hashes: Record<string, string>
  members: MemberState[]
  control_requests: ControlRequestState[]
  trace: TraceState | null
  handoffs: HandoffState[]
  /** In flight until handoff.created / handoff.failed with its id, or `expires_at`. */
  handoff_request: HandoffRequestState | null
  handoff_failure: HandoffFailureState | null
  activity: ActivityEntry[]
}

export function emptyRoomState(id: string): RoomState {
  return {
    id,
    title: '',
    severity: 'sev3',
    pipeline_id: '',
    status: 'open',
    opened_at: '',
    last_seq: 0,
    replay_target: 0,
    driver: null,
    alert: null,
    agent_status: { state: 'idle' },
    hypothesis: null,
    steps: [],
    messages: [],
    comments: [],
    suggestions: [],
    proposals: [],
    snapshot_hashes: {},
    members: [],
    control_requests: [],
    trace: null,
    handoffs: [],
    handoff_request: null,
    handoff_failure: null,
    activity: [],
  }
}

export function fromSnapshot(snapshot: RoomSnapshot): RoomState {
  // The snapshot only carries header/derived state for a fast first paint; the
  // timeline, comments, proposals, suggestions, members, trace and hand-offs are
  // rebuilt from the full event log (WebSocket after=0). So we seed last_seq at 0
  // (letting the replay fold every event) and leave the event-derived collections
  // empty — seeding them here would duplicate on replay.
  return {
    ...emptyRoomState(snapshot.id),
    id: snapshot.id,
    title: snapshot.title,
    severity: snapshot.severity as Severity,
    pipeline_id: snapshot.pipeline_id,
    status: snapshot.status as RoomStatus,
    opened_at: snapshot.opened_at,
    closed_at: snapshot.closed_at,
    resolution: snapshot.resolution,
    last_seq: 0,
    replay_target: snapshot.last_seq ?? 0,
    snapshot_hashes: Object.fromEntries(
      (snapshot.open_proposals ?? [])
        .filter((p) => p.params_hash)
        .map((p) => [p.proposal_id, p.params_hash as string]),
    ),
    driver: snapshot.driver
      ? { id: snapshot.driver.id, name: snapshot.driver.name, lease_until: snapshot.driver.lease_until }
      : null,
    agent_status: {
      state: snapshot.agent_status.state,
      detail: snapshot.agent_status.detail,
    },
    hypothesis: snapshot.hypothesis
      ? {
          text: snapshot.hypothesis.text,
          based_on_steps: snapshot.hypothesis.based_on_steps ?? [],
          confidence: snapshot.hypothesis.confidence,
          confirmed: snapshot.hypothesis.confirmed,
        }
      : null,
  }
}
