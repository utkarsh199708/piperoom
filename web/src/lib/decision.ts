// What the room asks of this person right now, as one value the decision bar draws.
// Pure: the bar owns the 3 s "done" timer and the dismissed-rejection storage.
import type { Actor } from '../api/types'
import type { ProposalState, RoomState } from '../state/types'
import { neutralizeBadgeText } from './verification'
import type { CurrentUser } from '../state/permissions'
import { canDecideLayer, isViewer } from '../state/permissions'
import { waitingForWriteMode } from './writeMode'
import { approveAllFor } from './approveAll'

export type Decision =
  /** `all`: the card offers one "Approve and run" for these layers (P2c); `layer` is the first of them. */
  | { kind: 'needs-you'; proposal: ProposalState; layer: string; completing: boolean; all?: string[] }
  | { kind: 'waiting'; proposal: ProposalState; pending: string[]; approved: number; total: number }
  | { kind: 'executing'; proposal: ProposalState; step: number; total: number; action: string }
  | { kind: 'done'; proposal: ProposalState; ok: boolean; failedStep?: number }
  | { kind: 'rejected'; proposal: ProposalState; by: string; byActor?: Actor; layer: string }

function newest(proposals: ProposalState[], status: ProposalState['status']): ProposalState | undefined {
  return [...proposals].reverse().find((p) => p.status === status)
}

/** Every layer except `layer` has approved: this person's approval completes the set. */
export function completesSet(p: ProposalState, layer: string): boolean {
  return p.required_layers.every((l) => l === layer || p.approvals[l] === 'approved')
}

export function decisionFor(state: RoomState, user: CurrentUser): Decision | null {
  if (state.status !== 'open') return null
  const proposals = state.proposals

  // The same place ProposalsPanel gives the A and R keys: the first proposal with its "Approve and
  // run", or with a layer you can decide.
  if (!isViewer(user)) {
    for (const p of proposals) {
      const all = approveAllFor(state, p, user)
      if (all) return { kind: 'needs-you', proposal: p, layer: all[0], completing: true, all }
      const layer = p.required_layers.find((l) => canDecideLayer(p, user, l))
      if (layer) return { kind: 'needs-you', proposal: p, layer, completing: completesSet(p, layer) }
    }
  }

  const approved = newest(proposals, 'approved')
  if (approved) {
    const total = approved.steps.length
    // A finished fix is news only until the agent proposes the next one: then the bar follows that one.
    const superseded = proposals.indexOf(approved) < proposals.length - 1
    const failed = approved.executed.findIndex((e) => !e.ok)
    if (failed >= 0) {
      if (!superseded) return { kind: 'done', proposal: approved, ok: false, failedStep: failed + 1 }
    } else if (waitingForWriteMode(approved)) {
      // Write mode is off: the set is complete and nothing will run, so no "Running step 1" bar.
      // The card and the rail say so; the bar has no question to ask.
    } else if (approved.executed.length < total) {
      const i = approved.executed.length
      return { kind: 'executing', proposal: approved, step: i + 1, total, action: approved.steps[i].action }
    } else if (approved.executed.length > 0 && !superseded) {
      return { kind: 'done', proposal: approved, ok: true }
    }
  }

  const waiting = proposals.find((p) => p.status === 'open' && p.required_layers.some((l) => p.approvals[l] === 'pending'))
  if (waiting) {
    return {
      kind: 'waiting',
      proposal: waiting,
      pending: waiting.required_layers.filter((l) => waiting.approvals[l] === 'pending'),
      approved: waiting.required_layers.filter((l) => waiting.approvals[l] === 'approved').length,
      total: waiting.required_layers.length,
    }
  }

  // A rejection stays until a newer proposal exists (the bar also lets the person dismiss it).
  const rejected = newest(proposals, 'rejected')
  if (rejected && proposals[proposals.length - 1] === rejected) {
    const layer = rejected.required_layers.find((l) => rejected.decisions?.[l]?.outcome === 'rejected') ?? ''
    const d = layer ? rejected.decisions[layer] : undefined
    return { kind: 'rejected', proposal: rejected, by: d ? neutralizeBadgeText(d.by.name || d.by.id) : 'a reviewer', byActor: d?.by, layer }
  }
  return null
}
