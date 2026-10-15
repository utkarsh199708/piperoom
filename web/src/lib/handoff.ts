import type { HandoffState, RoomState } from '../state/types'

/** The hand-off request still being written at `now`: handoff.requested not yet ended and not past expires_at. */
export function handoffInFlight(state: RoomState, now: number) {
  const r = state.handoff_request
  if (!r) return null
  const end = Date.parse(r.expires_at)
  return Number.isNaN(end) || end > now ? r : null
}

// handoff.failed `reason` codes (docs/api.md, Hand-off requests) in words. Never the server's error text.
const FAILURE_TEXT: Record<string, string> = {
  summary_failed: 'The agent could not write the summary. Try again.',
  not_driver: 'You stopped being the driver while the summary was written.',
  room_closed: 'The room closed while the summary was written.',
  internal_error: 'The hand-off failed on the server. Try again.',
}

export function handoffFailureText(reason: string): string {
  return FAILURE_TEXT[reason] ?? FAILURE_TEXT.internal_error
}

/** "Shift N" counts the hand-offs that were real shifts or still may be: superseded ones are not. */
export function shiftIndex(handoffs: HandoffState[], h: HandoffState): number {
  const live = handoffs.filter((x) => x.status !== 'voided' || x === h)
  return Math.max(0, live.indexOf(h))
}
