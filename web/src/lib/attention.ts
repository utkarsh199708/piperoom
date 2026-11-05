// Which room changes deserve a person's attention while the tab is in the background.
// Pure: compares two folded states, so it is tested without a browser. Only what the
// room state already holds (docs/api.md events folded by state/reducer.ts).
import { canDecideLayer } from '../state/permissions'
import type { CurrentUser } from '../state/permissions'
import type { RoomState } from '../state/types'
import { plainText } from './title'
import { neutralizeBadgeText } from './verification'

export type AttentionKind =
  | 'approval_needed'
  | 'proposal'
  | 'handoff'
  | 'driver_changed'
  | 'control_granted'
  | 'agent_error'
  | 'resolved'

/** Kinds that also raise an in-app toast (whether or not the tab is hidden). The rest are notification-only. */
export const TOAST_KINDS: ReadonlySet<AttentionKind> = new Set([
  'approval_needed',
  'handoff',
  'control_granted',
  'agent_error',
  'resolved',
])

export interface AttentionItem {
  kind: AttentionKind
  title: string
  body: string
  /** Same tag replaces an earlier notification for the same thing. */
  tag: string
  /** approval_needed: the layer of mine that waits. */
  layer?: string
  /** handoff to me: who is handing the room over. */
  from?: string
  /** True when the item is for this person to act on (a hand-off to someone else is not). */
  forMe?: boolean
}

// Notification and toast text never carries tool data, record values or the agent's error detail:
// only names and titles people typed, cleaned of control and bidi characters (lib/title plainText).
const body = (t: string, max = 120) => plainText(t, max)

export function attentionItems(prev: RoomState, next: RoomState, user: CurrentUser): AttentionItem[] {
  const items: AttentionItem[] = []
  const room = next.title || next.id

  // Nobody decides a layer in a room that is not open, owner or driver (the same rule as the panel's buttons).
  const isOpen = next.status === 'open'
  const driverId = isOpen ? next.driver?.id : undefined
  const knownProposals = new Set(prev.proposals.map((p) => p.proposal_id))
  for (const p of next.proposals) {
    if (knownProposals.has(p.proposal_id)) continue
    const layer = isOpen ? p.required_layers.find((l) => canDecideLayer(p, user, l, driverId)) : undefined
    const mine = layer !== undefined
    items.push({
      kind: mine ? 'approval_needed' : 'proposal',
      title: mine ? `Your approval is needed: ${room}` : `Fix proposed: ${room}`,
      body: body(p.title),
      tag: `proposal:${p.proposal_id}`,
      layer,
      forMe: mine,
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
      body: `To ${body(neutralizeBadgeText(h.to.name), 60)}`,
      tag: `handoff:${h.handoff_id}`,
      from: body(neutralizeBadgeText(next.driver?.name ?? 'Someone'), 60),
      forMe: toMe,
    })
  }

  const before = prev.driver?.id
  const after = next.driver?.id
  // The lease renewals the server logs keep the same driver and are not a change.
  if (after && after !== before && after !== user.id) {
    items.push({
      kind: 'driver_changed',
      title: `New driver: ${room}`,
      body: `${body(neutralizeBadgeText(next.driver?.name ?? after), 60)} is driving`,
      tag: `driver:${room}`,
    })
  }
  if (after === user.id && before !== user.id) {
    items.push({
      kind: 'control_granted',
      title: `You now drive "${body(room, 80)}"`,
      body: 'Control was given to you.',
      tag: `control:${next.id}`,
      forMe: true,
    })
  }

  if (next.agent_status.state === 'error' && prev.agent_status.state !== 'error') {
    items.push({
      kind: 'agent_error',
      title: `Agent error: ${room}`,
      // Never the status detail: it can carry tool or record text.
      body: 'The agent stopped. Open the room to steer it.',
      tag: `agent-error:${next.id}`,
      forMe: true,
    })
  }

  if (next.status === 'closed' && prev.status !== 'closed') {
    const closer = [...next.activity].reverse().find((a) => /clos/i.test(a.text))?.actor.name
    items.push({
      kind: 'resolved',
      title: `Resolved: ${room}`,
      body: closer ? `Closed by ${body(neutralizeBadgeText(closer), 60)}` : 'Closed',
      tag: `resolved:${next.id}`,
      forMe: true,
    })
  }
  return items
}
