// Turns an event into one curated line for the activity feed. Pure: no ids ever reach the text,
// and nothing here builds HTML (the feed renders `text` as text nodes, `{tool}` as <code>).
import { neutralizeBadgeText } from '../lib/verification'
import type {
  ActionExecutedPayload,
  AgentVerificationPayload,
  AgentHypothesisPayload,
  AgentMessagePayload,
  AgentStatusPayload,
  AgentStepStartPayload,
  AgentToolCallPayload,
  AgentToolResultPayload,
  AgentTriagePayload,
  ApprovalGivenPayload,
  ApprovalRejectedPayload,
  ChecklistStepDonePayload,
  CommentAddedPayload,
  ControlGrantedPayload,
  ControlLeaseExpiredPayload,
  Event,
  HandoffCreatedPayload,
  HandoffRequestedPayload,
  HypothesisVerdictPayload,
  ApprovalNudgedPayload,
  ProposalCreatedPayload,
  RoomClosedPayload,
  RoomCreatedPayload,
  SteerDeclinedPayload,
  RoomLabelledPayload,
  SteerSentPayload,
  SteerSuggestedPayload,
  TracePinnedPayload,
} from '../api/types'
import { argTags } from '../lib/toolLayers'

export type ActivityKind = 'step' | 'human' | 'decision' | 'control' | 'system'
export interface Described {
  /** May contain one `{tool}` placeholder, which the feed renders as <code>. */
  text: string
  detail?: string
  kind: ActivityKind
  /** The tool name the `{tool}` placeholder stands for (already validated). */
  tool?: string
}

export interface DescribeContext {
  driverBefore: string | null
  stepTitles: Record<string, string>
  proposalTitles: Record<string, string>
  /** step_id -> 1-based position in the room's steps. */
  stepIndex: Record<string, number>
  /** suggestion_id -> name of who suggested it. */
  suggesters?: Record<string, string>
  /** The hypothesis confidence before this event, to say raised or lowered. */
  confidenceBefore?: string | null
  /** Finds a runbook item's section and text for a checklist tick, in the room's state. */
  runbookItem?: (toolUseId: string, hit: number, item: number) => { section: string; text: string } | null
}

export const EMPTY_CONTEXT: DescribeContext = { driverBefore: null, stepTitles: {}, proposalTitles: {}, stepIndex: {} }

/** A person's display name, safe to embed in a sentence (see neutralizeBadgeText). */
function nm(name: string | undefined): string {
  return name ? neutralizeBadgeText(name) : ''
}

function as<T>(e: Event): T {
  return e.payload as T
}

/** Clip to `max` code points (never splits a surrogate pair), with an ellipsis. */
export function clip(s: string | undefined, max: number): string {
  // Server ids (ULID-shaped) in free text are noise to a reader; drop them.
  const plain = (s ?? '').replace(ID_IN_TEXT, '…')
  const chars = Array.from(plain.replace(/\s+/g, ' ').trim())
  return chars.length <= max ? chars.join('') : `${chars.slice(0, max - 1).join('')}…`
}

const ID_IN_TEXT = /\b[pgsh]_[0-9A-Z]{26}\b/g
const TOOL_NAME = /^[a-z0-9_]{1,64}$/

const STATE_WORDS: Record<string, string> = {
  working: 'working',
  idle: 'idle',
  waiting_approval: 'waiting for approval',
  error: 'error',
}

const RANK: Record<string, number> = { low: 0, medium: 1, high: 2 }

function took(ms: number): string {
  return ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${Math.round(ms)}ms`
}

export function describeEvent(e: Event, ctx: DescribeContext = EMPTY_CONTEXT): Described {
  // Every person name below is untrusted, so it goes in neutralized: a name cannot read as a badge or a verdict.
  const who = e.actor.kind === 'agent' ? 'Agent' : nm(e.actor.name || e.actor.id)
  const step = (id?: string) => {
    const n = id ? ctx.stepIndex[id] : undefined
    return n ? `step ${n}` : 'a step'
  }
  const stepCap = (id?: string) => {
    const s = step(id)
    return s.charAt(0).toUpperCase() + s.slice(1)
  }
  // A tool name goes in <code> only when it is a plain identifier; anything else stays plain text.
  const withTool = (tool: string, make: (t: string) => string): Pick<Described, 'text' | 'tool'> =>
    TOOL_NAME.test(tool) ? { text: make('{tool}'), tool } : { text: make(clip(tool, 40)) }

  switch (e.type) {
    case 'room.created': {
      const p = as<RoomCreatedPayload>(e)
      return {
        text: e.actor.kind === 'human' ? `Room opened by ${who}` : `Room opened — ${clip(p.title, 80)}`,
        kind: 'system',
      }
    }
    case 'room.closed': {
      const p = as<RoomClosedPayload>(e)
      return { text: `${who} closed the room`, detail: clip(p.resolution, 200) || undefined, kind: 'decision' }
    }
    case 'agent.status': {
      const p = as<AgentStatusPayload>(e)
      return {
        text: `Agent ${STATE_WORDS[p.state] ?? clip(String(p.state).replace(/_/g, ' '), 30)}`,
        detail: p.detail ? clip(p.detail, 200) : undefined,
        kind: 'system',
      }
    }
    case 'agent.step.started': {
      const p = as<AgentStepStartPayload>(e)
      return { text: `Agent started ${step(p.step_id)} · ${clip(p.title, 80)}`, kind: 'step' }
    }
    case 'agent.tool_call': {
      const p = as<AgentToolCallPayload>(e)
      const tags = argTags(p.input).join(' ')
      return { ...withTool(p.tool, (t) => `${stepCap(p.step_id)} · ran ${t}`), detail: tags ? clip(tags, 200) : undefined, kind: 'system' }
    }
    case 'agent.tool_result': {
      const p = as<AgentToolResultPayload>(e)
      if (p.is_error) {
        return { ...withTool(p.tool, (t) => `${stepCap(p.step_id)} · ${t} failed`), detail: clip(p.summary, 200) || undefined, kind: 'step' }
      }
      return { text: `${stepCap(p.step_id)} done · ${took(p.took_ms)}`, detail: clip(p.summary, 200) || undefined, kind: 'step' }
    }
    case 'agent.message': {
      const p = as<AgentMessagePayload>(e)
      if (p.reply_to) {
        return { text: p.step_id ? `Agent replied on ${step(p.step_id)}` : 'Agent replied to a comment', detail: clip(p.text, 200), kind: 'step' }
      }
      return { text: `Agent: ${clip(p.text, 120)}`, detail: p.text.length > 120 ? clip(p.text, 200) : undefined, kind: 'step' }
    }
    case 'agent.triage': {
      const p = as<AgentTriagePayload>(e)
      return p.status === 'ok' && p.headline
        ? { text: `First look: ${clip(p.headline, 120)}`, detail: p.headline.length > 120 ? clip(p.headline, 200) : undefined, kind: 'step' }
        : { text: `First look failed (${clip(p.reason || 'unknown', 40)})`, kind: 'step' }
    }
    case 'agent.hypothesis': {
      const p = as<AgentHypothesisPayload>(e)
      const before = ctx.confidenceBefore ? RANK[ctx.confidenceBefore] : undefined
      const now = RANK[p.confidence]
      const verb = before === undefined || now === undefined || before === now ? 'at' : now > before ? 'raised to' : 'lowered to'
      return { text: `Hypothesis ${verb} ${clip(p.confidence, 20)} confidence`, detail: clip(p.text, 200), kind: 'step' }
    }
    case 'hypothesis.verdict': {
      const p = as<HypothesisVerdictPayload>(e)
      return { text: `${who} ${clip(p.verdict, 20)} the hypothesis${p.note ? `: ${clip(p.note, 80)}` : ''}`, kind: 'human' }
    }
    case 'approval.nudged': {
      const p = as<ApprovalNudgedPayload>(e)
      return { text: `${who} nudged ${clip(p.layer, 40)} owners`, kind: 'human' }
    }
    case 'comment.added': {
      const p = as<CommentAddedPayload>(e)
      return {
        text: p.step_id ? `${who} commented on ${step(p.step_id)}` : `${who} commented`,
        detail: clip(p.text, 200),
        kind: 'human',
      }
    }
    case 'steer.suggested': {
      const p = as<SteerSuggestedPayload>(e)
      return { text: `${who} suggested: "${clip(p.text, 80)}"`, kind: 'human' }
    }
    case 'steer.sent': {
      const p = as<SteerSentPayload>(e)
      if (p.suggestion_id) {
        const from = nm(ctx.suggesters?.[p.suggestion_id])
        return { text: from ? `${who} sent ${from}'s suggestion` : `${who} sent a suggestion`, detail: clip(p.text, 200), kind: 'human' }
      }
      return { text: `${who} steered: "${clip(p.text, 80)}"`, kind: 'human' }
    }
    case 'steer.declined': {
      const p = as<SteerDeclinedPayload>(e)
      const from = nm(ctx.suggesters?.[p.suggestion_id])
      return {
        text: from ? `${who} declined ${from}'s suggestion` : `${who} declined a suggestion`,
        detail: p.reason ? clip(p.reason, 200) : undefined,
        kind: 'human',
      }
    }
    case 'control.requested':
      return { text: `${who} asked for control`, kind: 'control' }
    case 'control.granted': {
      const p = as<ControlGrantedPayload>(e)
      if (e.actor.id === p.to.id) {
        // A self-grant is either the sitting driver renewing their lease or
        // someone taking over (no driver, or an expired lease), never a hand-over.
        return e.actor.id === ctx.driverBefore
          ? { text: `${who} renewed the lease`, kind: 'system' }
          : { text: `${who} took control`, kind: 'control' }
      }
      return { text: `${who} handed control to ${nm(p.to.name)}`, kind: 'control' }
    }
    case 'control.lease_expired': {
      const p = as<ControlLeaseExpiredPayload>(e)
      return { text: `${nm(p.driver?.name) || 'The driver'}'s lease expired · no one is driving`, kind: 'control' }
    }
    case 'proposal.created': {
      const p = as<ProposalCreatedPayload>(e)
      return { text: `Agent proposed a fix · ${clip(p.title, 80)}`, kind: 'decision' }
    }
    case 'proposal.previewed':
      return { text: 'Previewed what the fix will do', kind: 'decision' }
    case 'agent.verification': {
      const p = as<AgentVerificationPayload>(e)
      return { text: `Verification: ${p.recovered ? 'recovered' : 'not recovered'}`, kind: 'decision' }
    }
    case 'approval.given': {
      const p = as<ApprovalGivenPayload>(e)
      return { text: `${who} approved ${clip(p.layer, 40)}`, kind: 'decision' }
    }
    case 'approval.rejected': {
      const p = as<ApprovalRejectedPayload>(e)
      return { text: `${who} rejected ${clip(p.layer, 40)}`, detail: p.reason ? clip(p.reason, 200) : undefined, kind: 'decision' }
    }
    case 'action.executed': {
      const p = as<ActionExecutedPayload>(e)
      return { text: `Step ${p.step_index + 1} ${p.ok ? 'ran' : 'failed'}`, detail: p.output ? clip(p.output, 200) : undefined, kind: 'decision' }
    }
    case 'handoff.requested': {
      const p = as<HandoffRequestedPayload>(e)
      return { text: `${who} asked for a hand-off to ${nm(p.to.name)}`, kind: 'control' }
    }
    case 'handoff.failed':
      return { text: 'The hand-off could not be written', kind: 'control' }
    case 'handoff.created': {
      const p = as<HandoffCreatedPayload>(e)
      return { text: `${who} started a hand-off to ${nm(p.to.name)}`, kind: 'control' }
    }
    case 'handoff.accepted':
      return { text: `${who} accepted the hand-off`, kind: 'control' }
    case 'room.labelled': {
      const p = as<RoomLabelledPayload>(e)
      return {
        text: `${who} labelled the room: cause ${clip(p.cause, 40)}${p.fix_that_worked ? ` · fix ${clip(p.fix_that_worked, 40)}` : ''}`,
        kind: 'human',
      }
    }
    case 'checklist.step_done': {
      const p = as<ChecklistStepDonePayload>(e)
      const found = ctx.runbookItem?.(p.tool_use_id, p.hit, p.item)
      const verb = p.done ? 'ticked' : 'unticked'
      if (!found) return { text: `${who} ${verb} step ${p.item + 1} of a runbook`, kind: 'human' }
      return {
        text: `${who} ${verb} "${clip(found.text, 80)}" (step ${p.item + 1}${found.section ? ` of ${clip(found.section, 40)}` : ''})`,
        kind: 'human',
      }
    }
    case 'trace.pinned': {
      const p = as<TracePinnedPayload>(e)
      return { text: `${who} traced ${clip(p.table, 40)} · ${clip(p.key, 40)}`, kind: 'human' }
    }
    // Shadow events carry no agent text, so these lines cannot un-blind anyone.
    case 'shadow.revealed':
      return { text: 'Shadow run finished: its result is ready to read after you label the room', kind: 'system' }
    case 'shadow.labelled':
      return { text: `${who} recorded a blind label`, kind: 'human' }
    case 'shadow.graded':
      return { text: `${who} graded the shadow run`, kind: 'human' }
    default:
      return { text: clip(e.type, 60), kind: 'system' }
  }
}
