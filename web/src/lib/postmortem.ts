import { ApiClientError } from '../api/client'
import type { RoomState } from '../state/types'
import { isNetworkError } from './errors'

// The server ends `drafting` within 3 minutes. A room closed before F2 replays room.closed and never
// gets a draft event, so after this long the "writing" line would be a lie: the draft is just the log.
const DRAFTING_MAX_MS = 10 * 60_000

/** True while the agent may still be writing the closing summary of this room. */
export function postmortemDrafting(state: Pick<RoomState, 'postmortem' | 'closed_at'>, now: number): boolean {
  if (state.postmortem?.status !== 'drafting') return false
  const closed = state.closed_at ? Date.parse(state.closed_at) : NaN
  return Number.isNaN(closed) || now - closed < DRAFTING_MAX_MS
}

/** Why the draft did not load, in words. Never the server's message. */
export function postmortemErrorText(e: unknown): string {
  if (isNetworkError(e)) return "can't reach Piperoom. Check your connection and retry."
  if (e instanceof ApiClientError) {
    if (e.status === 404) return 'this room was not found.'
    if (e.status === 401) return 'your session ended. Sign in again.'
    if (e.status === 403) return 'you do not have access to this room.'
    if (e.status === 504 || e.code === 'timeout') return 'the server took too long. Retry in a moment.'
  }
  return 'something went wrong on the server. Retry in a moment.'
}

/** The export toast's text: a refused export names the role it needs, never the raw message. */
export function exportErrorText(e: unknown, pipelineId: string): string {
  if (e instanceof ApiClientError && e.status === 403 && e.code === 'not_allowed') {
    return `Export needs an approver or auditor role for ${pipelineId || 'this pipeline'}`
  }
  if (isNetworkError(e)) return "Could not export the room: can't reach Piperoom"
  return 'Could not export the room. Try again.'
}

/** Save `text` as a file through a temporary link. */
export function downloadText(text: string, filename: string, type: string): void {
  const url = URL.createObjectURL(new Blob([text], { type }))
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(url)
}

/** Same name the server's `?download=1` uses. */
export function postmortemFilename(roomId: string): string {
  return `piperoom-${roomId}-postmortem.md`
}
