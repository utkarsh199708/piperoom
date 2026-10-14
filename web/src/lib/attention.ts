// Which room changes deserve a person's attention while the tab is in the background.
// Pure: compares two folded states, so it is tested without a browser. Only what the
// room state already holds (docs/api.md events folded by state/reducer.ts).
import type { CurrentUser } from '../state/permissions'
import type { RoomState } from '../state/types'

export type AttentionKind = 'approval_needed' | 'proposal' | 'handoff' | 'driver_changed'

export interface AttentionItem {
  kind: AttentionKind
  title: string
  body: string
  /** Same tag replaces an earlier notification for the same thing. */
  tag: string
}

export function attentionItems(prev: RoomState, next: RoomState, user: CurrentUser): AttentionItem[] {
  const items: AttentionItem[] = []
  const room = next.title || next.id

  const knownProposals = new Set(prev.proposals.map((p) => p.proposal_id))
  for (const p of next.proposals) {
    if (knownProposals.has(p.proposal_id)) continue
    const mine = p.required_layers.some((l) => user.layers.includes(l) && p.approvals[l] === 'pending')
    items.push({
      kind: mine ? 'approval_needed' : 'proposal',
      title: mine ? `Your approval is needed: ${room}` : `Fix proposed: ${room}`,
      body: p.title,
      tag: `proposal:${p.proposal_id}`,
    })
  }

  const knownHandoffs = new Set(prev.handoffs.map((h) => h.handoff_id))
  for (const h of next.handoffs) {
    if (knownHandoffs.has(h.handoff_id)) continue
    const toMe = h.to.id === user.id
    // The driver who wrote the hand-off does not need to be told about it.
    if (!toMe && next.driver?.id === user.id) continue
    items.push({
      kind: 'handoff',
      title: toMe ? `Hand-off for you to accept: ${room}` : `Hand-off: ${room}`,
      body: `To ${h.to.name}`,
      tag: `handoff:${h.handoff_id}`,
    })
  }

  const before = prev.driver?.id
  const after = next.driver?.id
  // The lease renewals the server logs keep the same driver and are not a change.
  if (after && after !== before && after !== user.id) {
    items.push({
      kind: 'driver_changed',
      title: `New driver: ${room}`,
      body: `${next.driver?.name ?? after} is driving`,
      tag: `driver:${room}`,
    })
  }
  return items
}
