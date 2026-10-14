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
  ChecklistStepDonePayload,
  CommentAddedPayload,
  ControlGrantedPayload,
  Event,
  HandoffCreatedPayload,
  HandoffRequestedPayload,
  HypothesisVerdictPayload,
  ApprovalNudgedPayload,
  ProposalCreatedPayload,
  RoomClosedPayload,
  RoomCreatedPayload,
  RoomLabelledPayload,
  SteerSentPayload,
  SteerSuggestedPayload,
  TracePinnedPayload,
} from '../api/types'

function as<T>(e: Event): T {
  return e.payload as T
}

// One short sentence for the activity feed. `driverBefore` is the driver id just
// before this event was applied; it distinguishes a lease renewal from a take-over.
export function describeEvent(
  e: Event,
  driverBefore: string | null = null,
  runbookItem?: (toolUseId: string, hit: number, item: number) => { section: string; text: string } | null,
): string {
  const who = e.actor.kind === 'agent' ? 'Agent' : e.actor.name || e.actor.id
  switch (e.type) {
    case 'room.created': {
      const p = as<RoomCreatedPayload>(e)
      return `${who} opened the room — ${p.title}`
    }
    case 'room.closed': {
      const p = as<RoomClosedPayload>(e)
      return `${who} closed the room: ${p.resolution}`
    }
    case 'agent.status': {
      const p = as<AgentStatusPayload>(e)
      return `Agent is ${p.state}${p.detail ? ` (${p.detail})` : ''}`
    }
    case 'agent.step.started': {
      const p = as<AgentStepStartPayload>(e)
      return `Agent started step: ${p.title}`
    }
    case 'agent.tool_call': {
      const p = as<AgentToolCallPayload>(e)
      return `Agent called ${p.tool}`
    }
    case 'agent.tool_result': {
      const p = as<AgentToolResultPayload>(e)
      if (p.is_error) return `${p.tool} failed`
      return `${p.tool} returned ${p.summary}`
    }
    case 'agent.message': {
      const p = as<AgentMessagePayload>(e)
      return p.reply_to ? `Agent replied to a comment` : `Agent: ${p.text}`
    }
    case 'agent.hypothesis': {
      const p = as<AgentHypothesisPayload>(e)
      return `Agent hypothesis (${p.confidence}): ${p.text}`
    }
    case 'hypothesis.verdict': {
      const p = as<HypothesisVerdictPayload>(e)
      return `${who} ${p.verdict} the hypothesis${p.note ? `: ${p.note}` : ''}`
    }
    case 'approval.nudged': {
      const p = as<ApprovalNudgedPayload>(e)
      return `${who} nudged ${p.layer} owners`
    }
    case 'comment.added': {
      const p = as<CommentAddedPayload>(e)
      return `${who} commented: ${p.text}`
    }
    case 'steer.suggested': {
      const p = as<SteerSuggestedPayload>(e)
      return `${who} suggested: ${p.text}`
    }
    case 'steer.sent': {
      const p = as<SteerSentPayload>(e)
      return `${who} steered the agent: ${p.text}`
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
      return `${who} handed control to ${p.to.name}`
    }
    case 'proposal.created': {
      const p = as<ProposalCreatedPayload>(e)
      return `${who} proposed: ${p.title}`
    }
    case 'approval.given': {
      const p = as<ApprovalGivenPayload>(e)
      return `${who} approved ${p.layer}`
    }
    case 'approval.rejected': {
      const p = as<ApprovalRejectedPayload>(e)
      return `${who} rejected ${p.layer}: ${p.reason}`
    }
    case 'action.executed': {
      const p = as<ActionExecutedPayload>(e)
      return `Step ${p.step_index + 1} ${p.ok ? 'succeeded' : 'failed'}`
    }
    case 'handoff.requested': {
      const p = as<HandoffRequestedPayload>(e)
      return `${who} asked for a hand-off to ${p.to.name}`
    }
    case 'handoff.failed':
      return 'The hand-off could not be written'
    case 'handoff.created': {
      const p = as<HandoffCreatedPayload>(e)
      return `${who} started a hand-off to ${p.to.name}`
    }
    case 'handoff.accepted':
      return `${who} accepted the hand-off`
    case 'room.labelled': {
      const p = as<RoomLabelledPayload>(e)
      return `${who} labelled the room: cause ${p.cause}${p.fix_that_worked ? ` · fix ${p.fix_that_worked}` : ''}`
    }
    case 'checklist.step_done': {
      const p = as<ChecklistStepDonePayload>(e)
      const found = runbookItem?.(p.tool_use_id, p.hit, p.item)
      const verb = p.done ? 'ticked' : 'unticked'
      if (!found) return `${who} ${verb} step ${p.item + 1} of a runbook`
      return `${who} ${verb} "${found.text}" (step ${p.item + 1}${found.section ? ` of ${found.section}` : ''})`
    }
    case 'trace.pinned': {
      const p = as<TracePinnedPayload>(e)
      return `Trace pinned for ${p.table} ${p.key}`
    }
    default:
      return e.type
  }
}
