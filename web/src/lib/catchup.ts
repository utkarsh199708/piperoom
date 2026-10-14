// "While you were away": a pure diff of two folded room states (like lib/attention.ts), so it is
// tested without timers or a browser. It never reads the activity window (capped at 20).
import type { CurrentUser } from '../state/permissions'
import type { ProposalState, RoomState } from '../state/types'

/** Where "Show me" scrolls: the proposal that needs you, else the hypothesis, else the investigation. */
export type CatchUpTarget = 'proposal' | 'hypothesis' | 'steps'

export interface CatchUpSummary {
  items: string[]
  target: CatchUpTarget
}

function mine(p: ProposalState, user: CurrentUser): boolean {
  return p.required_layers.some((l) => user.layers.includes(l) && p.approvals[l] === 'pending')
}

function executedCount(proposals: ProposalState[], ok: boolean): number {
  return proposals.reduce((n, p) => n + p.executed.filter((e) => e.ok === ok).length, 0)
}

/** What changed from `prev` to `next` that a returning person should hear about; null when nothing. */
export function catchUpSummary(prev: RoomState, next: RoomState, user: CurrentUser): CatchUpSummary | null {
  const items: string[] = []
  let target: CatchUpTarget | null = null

  const steps = next.steps.length - prev.steps.length
  if (steps > 0) {
    items.push(`${steps} ${steps === 1 ? 'step' : 'steps'}`)
    target ??= 'steps'
  }

  const before = prev.hypothesis
  const after = next.hypothesis
  if (after && (!before || before.text !== after.text || before.confidence !== after.confidence)) {
    const moved = !before || before.confidence !== after.confidence
    items.push(moved ? `hypothesis → ${after.confidence}` : 'hypothesis changed')
    target = 'hypothesis'
  }

  const known = new Set(prev.proposals.map((p) => p.proposal_id))
  const fresh = next.proposals.filter((p) => !known.has(p.proposal_id))
  if (fresh.some((p) => mine(p, user))) {
    items.push('a proposal needs you')
    target = 'proposal'
  } else if (fresh.length > 0) {
    items.push('a fix was proposed')
    target = 'proposal'
  }
  if (executedCount(next.proposals, false) > executedCount(prev.proposals, false)) {
    items.push('fix failed')
    target = 'proposal'
  } else if (executedCount(next.proposals, true) > executedCount(prev.proposals, true)) {
    items.push('fix ran')
    target = 'proposal'
  }

  const knownHandoffs = new Set(prev.handoffs.map((h) => h.handoff_id))
  if (next.handoffs.some((h) => !knownHandoffs.has(h.handoff_id) && h.to.id === user.id)) {
    items.push('hand-off for you')
  }

  if (next.driver && next.driver.id !== prev.driver?.id) items.push(`driver → ${next.driver.name}`)
  if (prev.status === 'open' && next.status === 'closed') items.push('room closed')
  if (next.agent_status.state === 'error' && prev.agent_status.state !== 'error') items.push('agent error')

  if (items.length === 0) return null
  return { items, target: target ?? 'steps' }
}

/** "12m" for the "(12m)" in the strip; whole minutes, at least 1. */
export function awayLabel(ms: number): string {
  return `${Math.max(1, Math.round(ms / 60_000))}m`
}
