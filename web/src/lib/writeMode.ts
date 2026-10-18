// Write mode (P wave, item 1; docs/api.md "P-wave snapshot fields"). With it off the server records
// approvals as usual and runs no step, so the UI must never say a fix is running or has run.
import type { RoomMode } from '../api/types'
import type { ProposalState } from '../state/types'

/** The repo path of the page that says how to turn write mode on (written by P1d-1). */
export const QUICKSTART_DOC_PATH = 'docs/pilot/quickstart.md'

/** The one phrase for an approved fix that waits for write mode: card, rail, header and pill all use it. */
export const WAITING_FOR_WRITE_MODE = 'Approved · waiting for write mode'

/**
 * Where "How to turn it on" points, or null when the build has no docs URL. The doc lives in the
 * repo, not in the app: a path relative to the app would 404 here, so without `VITE_DOCS_BASE_URL`
 * (e.g. the repo's `blob/main` URL) there is no link and the banner shows the path as text.
 */
export function quickstartHref(): string | null {
  const base = (import.meta.env.VITE_DOCS_BASE_URL as string | undefined)?.trim()
  return base ? `${base.replace(/\/+$/, '')}/${QUICKSTART_DOC_PATH}` : null
}

/** The server said write mode is off. No `mode` (an older server) means on. */
export function writeModeOff(mode: RoomMode | undefined | null): boolean {
  return mode?.write === false
}

/**
 * Every required layer approved and the executor held the steps back because write mode is off.
 * Nothing may have run: once an action.executed exists (the server was restarted with write mode
 * on, or the proposal ran before it was turned off) the execution card tells the real story.
 */
export function waitingForWriteMode(p: ProposalState): boolean {
  return p.status === 'approved' && p.blocked === 'write_mode_off' && p.executed.length === 0
}
