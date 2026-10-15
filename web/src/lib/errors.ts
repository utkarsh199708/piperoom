// Short user-facing text for the room error banner. A few server error codes
// (docs/api.md) have a friendlier phrasing; every other error keeps the raw
// server message.
import { ApiClientError } from '../api/client'

const FRIENDLY_TEXT: Record<string, string> = {
  lease_held: 'someone else holds control',
  stale_handoff: 'this hand-off is out of date',
  no_hypothesis: 'there is no hypothesis to decide on yet',
  already_nudged: 'already nudged a few minutes ago',
  handoff_in_progress: 'a hand-off is already being written for this room. Wait for it to finish.',
  summary_failed: 'the agent could not write the hand-off summary. Try again, or write the summary yourself.',
}

/** Friendly text for a server error code, or the server message unchanged. */
export function friendlyError(code: string, message: string): string {
  return FRIENDLY_TEXT[code] ?? message
}

/** Shown when the server cannot be reached; a steer or comment typed meanwhile is kept and retried. */
export const OFFLINE_TEXT = "Can't reach Piperoom \u2014 your message is kept. Retrying\u2026"

/** True for a fetch that never reached the server (a TypeError, or the browsers' own wording). */
export function isNetworkError(e: unknown): boolean {
  if (e instanceof ApiClientError) return false
  if (e instanceof TypeError) return true
  return e instanceof Error && /failed to fetch|networkerror|load failed/i.test(e.message)
}

/** Friendly text for a value thrown by the API layer (fetch rejects). */
export function errorText(e: unknown): string {
  if (isNetworkError(e)) return OFFLINE_TEXT
  if (e instanceof ApiClientError) return friendlyError(e.code, e.message)
  if (e instanceof Error) return e.message
  return String(e)
}
