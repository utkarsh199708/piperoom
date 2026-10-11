// Short user-facing text for the room error banner. A few server error codes
// (docs/api.md) have a friendlier phrasing; every other error keeps the raw
// server message.
import { ApiClientError } from '../api/client'

const FRIENDLY_TEXT: Record<string, string> = {
  lease_held: 'someone else holds control',
  stale_handoff: 'this hand-off is out of date',
}

/** Friendly text for a server error code, or the server message unchanged. */
export function friendlyError(code: string, message: string): string {
  return FRIENDLY_TEXT[code] ?? message
}

/** Friendly text for a value thrown by the API layer (fetch rejects). */
export function errorText(e: unknown): string {
  if (e instanceof ApiClientError) return friendlyError(e.code, e.message)
  if (e instanceof Error) return e.message
  return String(e)
}
