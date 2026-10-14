// Turns an event into a short, human-readable sentence for the activity feed.
import type {
  ActionExecutedPayload,
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
  HandoffCreatedPayload,
  ProposalCreatedPayload,
  RoomClosedPayload,
  RoomCreatedPayload,
  SteerSentPayload,
  SteerSuggestedPayload,
  TracePinnedPayload,
} from '../api/types'
import { isolate } from '../lib/bidi'
import { humanStatus } from '../lib/status'

function as<T>(e: Event): T {
  return e.payload as T
}

// One short sentence for the activity feed. `driverBefore` is the driver id just
// before this event was applied; it distinguishes a lease renewal from a take-over.
export function describeEvent(e: Event, driverBefore: string | null = null): string {
  // Every value that came from a person or the agent is a bidi isolate (lib/bidi.ts).
  const who = e.actor.kind === 'agent' ? 'Agent' : isolate(e.actor.name || e.actor.id)
  switch (e.type) {
    case 'room.created': {
      const p = as<RoomCreatedPayload>(e)
      return `${who} opened the room — ${isolate(p.title)}`
    }
    case 'room.closed': {
      const p = as<RoomClosedPayload>(e)
      return `${who} closed the room: ${isolate(p.resolution)}`
    }
    case 'agent.status': {
      const p = as<AgentStatusPayload>(e)
      return `Agent is ${humanStatus(p.state)}${p.detail ? ` (${isolate(p.detail)})` : ''}`
    }
    case 'agent.step.started': {
      const p = as<AgentStepStartPayload>(e)
      return `Agent started step: ${isolate(p.title)}`
    }
    case 'agent.tool_call': {
      const p = as<AgentToolCallPayload>(e)
      return `Agent called ${p.tool}`
    }
    case 'agent.tool_result': {
      const p = as<AgentToolResultPayload>(e)
      if (p.is_error) return `${p.tool} failed`
      return `${p.tool} returned ${isolate(p.summary)}`
    }
    case 'agent.message': {
      const p = as<AgentMessagePayload>(e)
      return p.reply_to ? `Agent replied to a comment` : `Agent: ${isolate(p.text)}`
    }
    case 'agent.hypothesis': {
      const p = as<AgentHypothesisPayload>(e)
      return `Agent hypothesis (${p.confidence}): ${isolate(p.text)}`
    }
    case 'comment.added': {
      const p = as<CommentAddedPayload>(e)
      return `${who} commented: ${isolate(p.text)}`
    }
    case 'steer.suggested': {
      const p = as<SteerSuggestedPayload>(e)
      return `${who} suggested: ${isolate(p.text)}`
    }
    case 'steer.sent': {
      const p = as<SteerSentPayload>(e)
      return `${who} steered the agent: ${isolate(p.text)}`
    }
    case 'control.requested':
      return `${who} requested control`
    case 'control.granted': {
      const p = as<ControlGrantedPayload>(e)
      if (e.actor.id === p.to.id) {
        // A self-grant is either the sitting driver renewing their lease or
        // someone taking over (no driver, or an expired lease) — never a hand-over.
        return e.actor.id === driverBefore
          ? `${who} renewed the lease`
          : `${who} took control`
      }
      return `${who} handed control to ${isolate(p.to.name)}`
    }
    case 'proposal.created': {
      const p = as<ProposalCreatedPayload>(e)
      return `${who} proposed: ${isolate(p.title)}`
    }
    case 'approval.given': {
      const p = as<ApprovalGivenPayload>(e)
      return `${who} approved ${p.layer}`
    }
    case 'approval.rejected': {
      const p = as<ApprovalRejectedPayload>(e)
      return `${who} rejected ${p.layer}: ${isolate(p.reason)}`
    }
    case 'action.executed': {
      const p = as<ActionExecutedPayload>(e)
      return `Step ${p.step_index + 1} ${p.ok ? 'succeeded' : 'failed'}`
    }
    case 'handoff.created': {
      const p = as<HandoffCreatedPayload>(e)
      return `${who} started a hand-off to ${isolate(p.to.name)}`
    }
    case 'handoff.accepted':
      return `${who} accepted the hand-off`
    case 'trace.pinned': {
      const p = as<TracePinnedPayload>(e)
      return `Trace pinned for ${isolate(p.table)} ${isolate(p.key)}`
    }
    default:
      return e.type
  }
}
