// The single reducer that folds one room event into UI state, mirroring A5's
// Apply(snapshot, event) semantics. Dedupe (seq <= last_seq) happens here; gap
// filling (seq > last_seq + 1) is the transport's job (see hooks/useRoom.ts).
import type {
  ActionExecutedPayload,
  Actor,
  AgentHypothesisPayload,
  AgentMessagePayload,
  AgentStatusPayload,
  AgentStepStartPayload,
  AgentToolCallPayload,
  AgentToolResultPayload,
  ApprovalGivenPayload,
  ApprovalRejectedPayload,
  CommentAddedPayload,
  ControlGrantedPayload,
  Event,
  HandoffAcceptedPayload,
  HandoffCreatedPayload,
  HandoffFailedPayload,
  HandoffRequestedPayload,
  ProposalCreatedPayload,
  RoomClosedPayload,
  RoomCreatedPayload,
  SteerDeclinedPayload,
  SteerSentPayload,
  SteerSuggestedPayload,
  TracePinnedPayload,
} from '../api/types'
import { describeEvent } from './describe'
import { alertSource } from '../lib/alerts'
import type { MemberState, RoomState, StepState } from './types'

function as<T>(e: Event): T {
  return e.payload as T
}

// True when the event's seq skips ahead — the transport must fetch the missing
// events (/events?after=lastSeq) before applying.
export function detectGap(lastSeq: number, event: Event): boolean {
  return event.seq > lastSeq + 1
}

function updateStep(
  steps: StepState[],
  stepId: string,
  fn: (s: StepState) => StepState,
): StepState[] {
  return steps.map((s) => (s.id === stepId ? fn(s) : s))
}

// upsertMember mirrors the server's snapshot.Apply: every human actor is a room
// member. New actors join as "watching"; an actor already in the list is never
// downgraded (a driver who acts stays the driver).
function upsertMember(members: MemberState[], actor: Actor): MemberState[] {
  const existing = members.find((m) => m.id === actor.id)
  if (!existing) return [...members, { id: actor.id, name: actor.name, role: 'watching' }]
  if (existing.name === actor.name) return members
  return members.map((m) => (m.id === actor.id ? { ...m, name: actor.name } : m))
}

export function applyEvent(state: RoomState, event: Event): RoomState {
  if (event.seq <= state.last_seq) return state

  // agent.usage is cost telemetry, one or two per model request. It moves
  // last_seq on (so the next event is not a gap) and nothing else: it must not
  // take a slot of the 20-entry activity window or show up as a raw line.
  if (event.type === 'agent.usage') return { ...state, last_seq: event.seq }

  let next: RoomState = state

  switch (event.type) {
    case 'room.created': {
      const p = as<RoomCreatedPayload>(event)
      const driver =
        event.actor.kind === 'human'
          ? { id: event.actor.id, name: event.actor.name }
          : state.driver
      // Webhook-opened rooms carry the raw alert body here; a room created via
      // POST /api/rooms has no alert at all.
      const alert =
        p.alert === undefined || p.alert === null
          ? null
          : { source: alertSource(p.alert) }
      next = {
        ...state,
        title: p.title,
        severity: p.severity as RoomState['severity'],
        pipeline_id: p.pipeline_id,
        status: 'open',
        opened_at: event.at,
        driver,
        alert,
        members:
          event.actor.kind === 'human'
            ? [{ id: event.actor.id, name: event.actor.name, role: 'driver' }]
            : state.members,
      }
      break
    }

    case 'room.closed': {
      const p = as<RoomClosedPayload>(event)
      next = {
        ...state,
        status: 'closed',
        closed_at: event.at,
        resolution: p.resolution,
      }
      break
    }

    case 'agent.status': {
      const p = as<AgentStatusPayload>(event)
      next = { ...state, agent_status: { state: p.state, detail: p.detail } }
      break
    }

    case 'agent.step.started': {
      const p = as<AgentStepStartPayload>(event)
      if (state.steps.some((s) => s.id === p.step_id)) break
      next = {
        ...state,
        steps: [
          ...state.steps,
          { id: p.step_id, title: p.title, tool_calls: [], messages: [] },
        ],
      }
      break
    }

    case 'agent.tool_call': {
      const p = as<AgentToolCallPayload>(event)
      next = {
        ...state,
        steps: updateStep(state.steps, p.step_id, (s) => {
          if (s.tool_calls.some((t) => t.tool_use_id === p.tool_use_id)) return s
          return {
            ...s,
            tool_calls: [
              ...s.tool_calls,
              { tool_use_id: p.tool_use_id, tool: p.tool, input: p.input },
            ],
          }
        }),
      }
      break
    }

    case 'agent.tool_result': {
      const p = as<AgentToolResultPayload>(event)
      next = {
        ...state,
        steps: updateStep(state.steps, p.step_id, (s) => ({
          ...s,
          tool_calls: s.tool_calls.map((t) =>
            t.tool_use_id === p.tool_use_id
              ? {
                  ...t,
                  tool: p.tool,
                  summary: p.summary,
                  data: p.data,
                  is_error: p.is_error,
                  truncated: p.truncated,
                  took_ms: p.took_ms,
                }
              : t,
          ),
        })),
      }
      break
    }

    case 'agent.message': {
      const p = as<AgentMessagePayload>(event)
      if (p.reply_to) {
        next = {
          ...state,
          comments: state.comments.map((c) =>
            c.id === p.reply_to
              ? { ...c, replies: [...c.replies, { text: p.text, at: event.at }] }
              : c,
          ),
        }
      } else if (p.step_id) {
        next = {
          ...state,
          steps: updateStep(state.steps, p.step_id, (s) => ({
            ...s,
            messages: [...s.messages, { text: p.text, at: event.at }],
          })),
        }
      } else {
        next = {
          ...state,
          messages: [...state.messages, { text: p.text, at: event.at }],
        }
      }
      break
    }

    case 'agent.hypothesis': {
      const p = as<AgentHypothesisPayload>(event)
      next = {
        ...state,
        hypothesis: {
          text: p.text,
          based_on_steps: p.based_on_steps ?? [],
          confidence: p.confidence,
          confirmed: p.confirmed,
        },
      }
      break
    }

    case 'comment.added': {
      const p = as<CommentAddedPayload>(event)
      next = {
        ...state,
        comments: [
          ...state.comments,
          {
            id: p.comment_id,
            step_id: p.step_id,
            text: p.text,
            mentions_agent: p.mentions_agent,
            author: event.actor,
            at: event.at,
            replies: [],
          },
        ],
      }
      break
    }

    case 'steer.suggested': {
      const p = as<SteerSuggestedPayload>(event)
      next = {
        ...state,
        suggestions: [
          ...state.suggestions,
          {
            suggestion_id: p.suggestion_id,
            from: event.actor,
            text: p.text,
            status: 'pending' as const,
            at: event.at,
          },
        ],
      }
      break
    }

    case 'steer.sent': {
      const p = as<SteerSentPayload>(event)
      if (p.suggestion_id) {
        next = {
          ...state,
          // Kept, not dropped: the room shows what became of each suggestion.
          suggestions: state.suggestions.map((s) =>
            s.suggestion_id === p.suggestion_id ? { ...s, status: 'accepted' as const } : s,
          ),
        }
      }
      break
    }

    case 'steer.declined': {
      const p = as<SteerDeclinedPayload>(event)
      next = {
        ...state,
        suggestions: state.suggestions.map((s) =>
          s.suggestion_id === p.suggestion_id
            ? { ...s, status: 'declined' as const, decline_reason: p.reason || undefined }
            : s,
        ),
      }
      break
    }

    case 'control.requested': {
      if (state.control_requests.some((r) => r.from.id === event.actor.id)) break
      next = {
        ...state,
        control_requests: [...state.control_requests, { from: event.actor }],
      }
      break
    }

    case 'control.granted': {
      const p = as<ControlGrantedPayload>(event)
      const members = state.members.some((m) => m.id === p.to.id)
        ? state.members.map((m) =>
            m.id === p.to.id
              ? { ...m, role: 'driver' }
              : m.role === 'driver'
                ? { ...m, role: 'watching' }
                : m,
          )
        : [
            ...state.members.map((m) =>
              m.role === 'driver' ? { ...m, role: 'watching' } : m,
            ),
            { id: p.to.id, name: p.to.name, role: 'driver' },
          ]
      next = {
        ...state,
        driver: { id: p.to.id, name: p.to.name, lease_until: p.lease_until },
        members,
        control_requests: state.control_requests.filter((r) => r.from.id !== p.to.id),
      }
      break
    }

    case 'proposal.created': {
      const p = as<ProposalCreatedPayload>(event)
      // A second proposal.created with the same id (a replay over a snapshot, a retry) is
      // ignored: the first one stays, with the decisions it already collected.
      if (state.proposals.some((x) => x.proposal_id === p.proposal_id)) break
      const approvals: Record<string, 'pending'> = {}
      for (const layer of p.required_layers ?? []) approvals[layer] = 'pending'
      next = {
        ...state,
        proposals: [
          ...state.proposals,
          {
            proposal_id: p.proposal_id,
            title: p.title,
            steps: p.steps ?? [],
            risk: p.risk,
            rollback: p.rollback,
            required_layers: p.required_layers ?? [],
            approvals,
            decisions: {},
            // The event's hash, else the snapshot's (an event written before the
            // hash existed has none; the server computed one for the snapshot).
            params_hash: p.params_hash || state.snapshot_hashes[p.proposal_id] || '',
            status: 'open',
            executed: [],
          },
        ],
      }
      break
    }

    case 'approval.given': {
      const p = as<ApprovalGivenPayload>(event)
      next = {
        ...state,
        proposals: state.proposals.map((prop) => {
          if (prop.proposal_id !== p.proposal_id) return prop
          const approvals = { ...prop.approvals, [p.layer]: 'approved' as const }
          const allApproved = (prop.required_layers ?? []).every(
            (l) => approvals[l] === 'approved',
          )
          return {
            ...prop,
            approvals,
            decisions: {
              ...prop.decisions,
              [p.layer]: {
                outcome: 'approved' as const,
                by: event.actor,
                identity: p.identity,
                params_hash: p.params_hash,
                at: event.at,
              },
            },
            status: allApproved ? 'approved' : 'open',
          }
        }),
      }
      break
    }

    case 'approval.rejected': {
      const p = as<ApprovalRejectedPayload>(event)
      next = {
        ...state,
        proposals: state.proposals.map((prop) =>
          prop.proposal_id === p.proposal_id
            ? {
                ...prop,
                approvals: { ...prop.approvals, [p.layer]: 'rejected' as const },
                decisions: {
                  ...prop.decisions,
                  [p.layer]: {
                    outcome: 'rejected' as const,
                    by: event.actor,
                    identity: p.identity,
                    params_hash: p.params_hash,
                    reason: p.reason,
                    at: event.at,
                  },
                },
                status: 'rejected',
              }
            : prop,
        ),
      }
      break
    }

    case 'action.executed': {
      const p = as<ActionExecutedPayload>(event)
      next = {
        ...state,
        proposals: state.proposals.map((prop) =>
          prop.proposal_id === p.proposal_id
            ? { ...prop, executed: [...prop.executed, p] }
            : prop,
        ),
      }
      break
    }

    case 'handoff.requested': {
      const p = as<HandoffRequestedPayload>(event)
      next = {
        ...state,
        handoff_request: {
          request_id: p.request_id,
          to: p.to,
          stay_as_watcher: p.stay_as_watcher,
          expires_at: p.expires_at,
        },
        handoff_failure: null,
      }
      break
    }

    case 'handoff.failed': {
      const p = as<HandoffFailedPayload>(event)
      next = {
        ...state,
        handoff_request: state.handoff_request?.request_id === p.request_id ? null : state.handoff_request,
        handoff_failure: { request_id: p.request_id, reason: p.reason },
      }
      break
    }

    case 'handoff.created': {
      const p = as<HandoffCreatedPayload>(event)
      next = {
        ...state,
        // Ends the request it completes; an event from before R0 carries no request_id and ends any.
        handoff_request:
          !p.request_id || state.handoff_request?.request_id === p.request_id ? null : state.handoff_request,
        handoff_failure: null,
        handoffs: [
          ...state.handoffs,
          {
            handoff_id: p.handoff_id,
            to: p.to,
            summary: p.summary,
            stay_as_watcher: p.stay_as_watcher,
            status: 'pending' as const,
          },
        ],
      }
      break
    }

    case 'handoff.accepted': {
      const p = as<HandoffAcceptedPayload>(event)
      const accepted = state.handoffs.find((h) => h.handoff_id === p.handoff_id)
      next = {
        ...state,
        handoffs: state.handoffs.map((h) =>
          h.handoff_id === p.handoff_id ? { ...h, status: 'accepted' as const } : h,
        ),
        // The server emits control.granted (driver + lease) immediately before
        // handoff.accepted; keep that lease rather than replacing it with a
        // lease-less driver object. Only move the driver here as a fallback for
        // a log that never saw the control.granted.
        driver: accepted
          ? state.driver?.id === accepted.to.id
            ? state.driver
            : { id: accepted.to.id, name: accepted.to.name }
          : state.driver,
        members: accepted
          ? state.members.some((m) => m.id === accepted.to.id)
            ? state.members.map((m) =>
                m.id === accepted.to.id
                  ? { ...m, role: 'driver' }
                  : m.role === 'driver'
                    ? { ...m, role: 'watching' }
                    : m,
              )
            : [
                ...state.members.map((m) =>
                  m.role === 'driver' ? { ...m, role: 'watching' } : m,
                ),
                { id: accepted.to.id, name: accepted.to.name, role: 'driver' },
              ]
          : state.members,
      }
      break
    }

    case 'trace.pinned': {
      const p = as<TracePinnedPayload>(event)
      next = {
        ...state,
        trace: { table: p.table, key: p.key, stages: p.stages ?? [] },
      }
      break
    }

    default:
      break
  }

  // Every human actor is a room member, mirroring the server's snapshot.Apply
  // (which upserts the actor as "watching" before handling the event). Applied
  // after the switch so no case's members rebuild can discard it.
  const members =
    event.actor.kind === 'human' ? upsertMember(next.members, event.actor) : next.members

  return {
    ...next,
    members,
    last_seq: event.seq,
    activity: [
      {
        seq: event.seq,
        at: event.at,
        actor: event.actor,
        // state is the pre-event state: describeEvent needs the driver before
        // this event to tell a lease renewal from a take-over.
        text: describeEvent(event, state.driver?.id ?? null),
      },
      ...next.activity,
    ].slice(0, 20),
  }
}
