// "Log a fix" (Q0, QA G10): a comment of kind fix_note, for a fix a person made outside Piperoom.
import type { CurrentUser } from '../state/permissions'
import { isViewer } from '../state/permissions'
import type { RoomState } from '../state/types'

/** How long after the close a fix may still be logged (docs/api.md, POST /api/rooms/{id}/comments). */
export const FIX_NOTE_WINDOW_MS = 24 * 60 * 60 * 1000

/** Longest comment text the server takes (R4). */
export const MAX_FIX_NOTE = 4000

/**
 * Open rooms always; a closed room for 24 h after it closed. The server decides who may log one on
 * a closed room (its closer or a member of the pipeline) and answers 403 otherwise, so the dialog
 * shows that answer instead of the client guessing at it. Viewers never write.
 */
export function canLogFix(state: RoomState, user: CurrentUser, now = Date.now()): boolean {
  if (isViewer(user)) return false
  if (state.status === 'open') return true
  const closed = state.closed_at ? Date.parse(state.closed_at) : NaN
  return Number.isFinite(closed) && now - closed < FIX_NOTE_WINDOW_MS
}
