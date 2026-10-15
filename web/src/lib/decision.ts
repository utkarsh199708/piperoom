// What the room asks of this person right now, as one value the decision bar draws.
// Pure: the bar owns the 3 s "done" timer and the dismissed-rejection storage.
import type { ProposalState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canDecideLayer, isViewer } from '../state/permissions'

export type Decision =
  | { kind: 'needs-you'; proposal: ProposalState; layer: string; completing: boolean }
  | { kind: 'waiting'; proposal: ProposalState; pending: string[]; approved: number; total: number }
  | { kind: 'executing'; proposal: ProposalState; step: number; total: number; action: string }
  | { kind: 'done'; proposal: ProposalState; ok: boolean; failedStep?: number }
  | { kind: 'rejected'; proposal: ProposalState; by: string; layer: string }

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

  // The same row ProposalsPanel gives the A and R keys: first proposal, first layer you can decide.
  if (!isViewer(user)) {
    for (const p of proposals) {
      const layer = p.required_layers.find((l) => canDecideLayer(p, user, l))
      if (layer) return { kind: 'needs-you', proposal: p, layer, completing: completesSet(p, layer) }
    }
  }

  const approved = newest(proposals, 'approved')
  if (approved) {
    const total = approved.steps.length
    const failed = approved.executed.findIndex((e) => !e.ok)
    if (failed >= 0) return { kind: 'done', proposal: approved, ok: false, failedStep: failed + 1 }
    if (approved.executed.length < total) {
      const i = approved.executed.length
      return { kind: 'executing', proposal: approved, step: i + 1, total, action: approved.steps[i].action }
    }
    if (approved.executed.length > 0) return { kind: 'done', proposal: approved, ok: true }
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
    return { kind: 'rejected', proposal: rejected, by: d ? d.by.name || d.by.id : 'a reviewer', layer }
  }
  return null
}
