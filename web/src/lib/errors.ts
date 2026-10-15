// Short user-facing text for the room error banner. A few server error codes
// (docs/api.md) have a friendlier phrasing; every other error keeps the raw
// server message.
import { ApiClientError } from '../api/client'

const FRIENDLY_TEXT: Record<string, string> = {
  // Q0: approve, reject and withdraw all answer these for a proposal that is no longer open.
  proposal_withdrawn: 'this fix was already withdrawn, so it can no longer be decided on',
  proposal_running: 'this fix is already approved or running, so it can no longer be withdrawn',
  lease_held: 'someone else holds control',
  stale_handoff: 'this hand-off is out of date',
  // Shadow mode (docs/api.md). already_decided keeps the server's text: it also means "this
  // layer was decided" for approvals. room_open is shared with the F8 label, the shadow export
  // and a shadow room's usage, so its text names no single action.
  not_revealed: 'the agent is still finishing its run; grading opens when it ends',
  not_labelled: 'record your blind label first',
  room_open: 'the room is still open; this is available once it is closed',
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

// The friendly texts above are lowercase fragments for the room banner; an inline message is a sentence.
const sentence = (t: string): string => t.charAt(0).toUpperCase() + t.slice(1)

/** What a failed withdraw (POST .../proposals/{pid}/withdraw) says. Nothing is kept or retried. */
export function withdrawErrorText(e: unknown): string {
  if (isNetworkError(e)) return "Can't reach Piperoom. The fix was not withdrawn; try again."
  if (e instanceof ApiClientError && e.code === 'not_driver') return 'Only the driver can withdraw a fix.'
  return sentence(errorText(e))
}

/** What a failed close says. A layer owner who is not the driver gets 403 not_driver when the server disagrees. */
export function closeErrorText(e: unknown): string {
  if (e instanceof ApiClientError && e.code === 'not_driver') {
    return "Only the driver or an approver of one of this pipeline's layers can close the room."
  }
  return errorText(e)
}
