// Short user-facing text for the room error banner. A few server error codes
// (docs/api.md) have a friendlier phrasing; every other error keeps the raw
// server message.
import { ApiClientError } from '../api/client'

const FRIENDLY_TEXT: Record<string, string> = {
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
  // #459: the client sent one idempotency key with two different messages, a client bug.
  client_id_reused: 'this message could not be sent (its id was already used). Reload the room and send it again.',
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

/** A steer or comment got no answer in time and the server is not known to dedupe retries. */
export const UNCONFIRMED_TEXT =
  'No answer from Piperoom yet \u2014 waiting a minute before your message can be sent again\u2026'

/** The check above found no sign of the message: only the person's own Retry sends it again. */
export const NOT_CONFIRMED_TEXT = "Piperoom never confirmed your message. Press Retry to send it again."

/** 409 client_id_reused (docs/api.md, Idempotent posts): one key sent with two different bodies. */
export function isClientIdReused(e: unknown): boolean {
  return e instanceof ApiClientError && e.code === 'client_id_reused'
}

/**
 * True for api.steer/api.comment's SendUnconfirmedError. Checked by name so this module keeps
 * working where tests replace '../api/client' with a partial mock.
 */
export function isUnconfirmed(e: unknown): boolean {
  return e instanceof Error && e.name === 'SendUnconfirmedError'
}

/** Friendly text for a value thrown by the API layer (fetch rejects). */
export function errorText(e: unknown): string {
  if (isUnconfirmed(e)) return UNCONFIRMED_TEXT
  if (isNetworkError(e)) return OFFLINE_TEXT
  if (e instanceof ApiClientError) return friendlyError(e.code, e.message)
  if (e instanceof Error) return e.message
  return String(e)
}

/**
 * Shown when the server cannot be reached during a room action other than a steer or comment (approve, reject,
 * nudge, close, label, hand-off...). Nothing keeps or retries these. A fetch that rejects can also mean the server
 * got the request and only the reply was lost, so this says the action may not have happened.
 */
export const ACTION_OFFLINE_TEXT = "Can't reach Piperoom, so that may not have gone through. Try again."

/** errorText for a room action the send ledger does not keep: a network error never promises a retry. */
export function actionErrorText(e: unknown): string {
  if (isNetworkError(e)) return ACTION_OFFLINE_TEXT
  return errorText(e)
}
