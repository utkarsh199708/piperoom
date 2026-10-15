// "Waiting for Arjun · away 12m": who can unblock a pending layer, and whether they are looking.
import type { Person, PresenceMember } from '../api/types'
import { ageLabel } from './time'

export interface Owner {
  id: string
  name: string
  /** Server-set, from GET /api/people. */
  verified?: boolean
  /** here | away 12m | not here */
  presence: string
}

export const NUDGE_WINDOW_MS = 10 * 60_000
const MAX_TITLE = 120

/** Presence as a word: from the member's server-side `since`, never a client timer. */
export function presenceWord(m: PresenceMember | undefined, now: number): string {
  if (!m) return 'not here'
  if (m.state === 'viewing') return 'here'
  return m.since ? `away ${ageLabel(m.since, now)}` : 'away'
}

/** People whose `layers` include `layer` (group grants are not in the list), with their presence. */
export function ownersOf(layer: string, people: Person[] | undefined, presence: PresenceMember[], now: number): Owner[] {
  return (people ?? [])
    .filter((p) => p.layers?.includes(layer))
    .map((p) => ({ id: p.id, name: p.name, verified: p.verified, presence: presenceWord(presence.find((m) => m.id === p.id), now) }))
}

/** The title is untrusted text: no quotes, no newlines, a bounded length. */
export function safeTitle(title: string): string {
  const t = title.replace(/["“”]/g, "'").replace(/\s+/g, ' ').trim()
  return t.length > MAX_TITLE ? `${t.slice(0, MAX_TITLE - 1)}…` : t
}

/** The deep link a nudge copies (#141): origin + /rooms/ + the encoded id. */
export function roomLink(roomId: string): string {
  return `${window.location.origin}/rooms/${encodeURIComponent(roomId)}`
}

export function nudgeText(layer: string, title: string, roomId: string): string {
  return `Your approval is needed for ${layer} on "${safeTitle(title)}": ${roomLink(roomId)}`
}
