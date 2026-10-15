import type { HandoffSummary } from '../api/types'
import type { RoomState } from '../state/types'

/** The hand-off request still being written at `now`: handoff.requested not yet ended and not past expires_at. */
export function handoffInFlight(state: RoomState, now: number) {
  const r = state.handoff_request
  if (!r) return null
  const end = Date.parse(r.expires_at)
  return Number.isNaN(end) || end > now ? r : null
}

// handoff.failed `reason` codes (docs/api.md, Hand-off requests) in words. Never the server's error text.
const FAILURE_TEXT: Record<string, string> = {
  summary_failed: 'The agent could not write the summary. Try again, or write it yourself.',
  not_driver: 'You stopped being the driver while the summary was written.',
  room_closed: 'The room closed while the summary was written.',
  internal_error: 'The hand-off failed on the server. Try again.',
}

export function handoffFailureText(reason: string): string {
  return FAILURE_TEXT[reason] ?? FAILURE_TEXT.internal_error
}

// ---- the driver's own summary (Q0, QA G4) ----
// POST /api/rooms/{id}/handoffs takes `summary` when the agent could not write one. Limits are the server's
// (docs/api.md): what_broke and why 1-1000 characters, list items at most 500, at most 20 per list.

export const MAX_SUMMARY_TEXT = 1000
export const MAX_SUMMARY_ITEM = 500
export const MAX_SUMMARY_ITEMS = 20

/** What the form holds: the two texts, and one list per field as text with one item per line. */
export interface SummaryDraft {
  what_broke: string
  why: string
  done: string
  ruled_out: string
  open: string
  risks: string
}

export function emptySummaryDraft(): SummaryDraft {
  return { what_broke: '', why: '', done: '', ruled_out: '', open: '', risks: '' }
}

const LIST_FIELDS = [
  ['done', 'Done'],
  ['ruled_out', 'Ruled out'],
  ['open', 'Still open'],
  ['risks', 'Risks'],
] as const

const chars = (s: string) => Array.from(s).length

/** The items of a list field: one per line, blank lines dropped, a leading "-", "*" or bullet removed. */
export function listItems(text: string): string[] {
  return text
    .split('\n')
    .map((l) => l.trim().replace(/^[-*\u2022]\s+/, '').trim())
    .filter(Boolean)
}

/**
 * The summary to send, or the reasons it cannot be sent yet. Checked here so the person sees a problem
 * next to their words; the server checks the same limits again and its answer is shown too.
 */
export function parseSummaryDraft(d: SummaryDraft): { summary: HandoffSummary | null; errors: string[] } {
  const errors: string[] = []
  const what_broke = d.what_broke.trim()
  const why = d.why.trim()
  if (!what_broke) errors.push('Say what broke.')
  else if (chars(what_broke) > MAX_SUMMARY_TEXT) errors.push(`"What broke" is at most ${MAX_SUMMARY_TEXT} characters.`)
  if (!why) errors.push('Say why it broke, or what you think the cause is.')
  else if (chars(why) > MAX_SUMMARY_TEXT) errors.push(`"Why" is at most ${MAX_SUMMARY_TEXT} characters.`)
  const lists: Record<string, string[]> = {}
  for (const [key, label] of LIST_FIELDS) {
    const items = listItems(d[key])
    if (items.length > MAX_SUMMARY_ITEMS) errors.push(`"${label}" has at most ${MAX_SUMMARY_ITEMS} items.`)
    if (items.some((i) => chars(i) > MAX_SUMMARY_ITEM)) errors.push(`Each "${label}" item is at most ${MAX_SUMMARY_ITEM} characters.`)
    lists[key] = items
  }
  if (errors.length > 0) return { summary: null, errors }
  return {
    summary: { what_broke, why, done: lists.done, ruled_out: lists.ruled_out, open: lists.open, risks: lists.risks },
    errors,
  }
}

/** The failure reasons after which the form offers "Write the summary yourself". */
export function canWriteByHand(reason: string | undefined): boolean {
  return reason === 'summary_failed'
}
