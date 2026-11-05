// "Propose a fix yourself" (P3d-2): who may, what the dialog sends, and how the server's refusals
// read. The server checks all of it again (docs/api.md POST /api/rooms/{id}/proposals); this only
// hides the button where the call would be refused and turns a refusal into a plain sentence.
import { ApiClientError } from '../api/client'
import type { ActionInfo, CreateProposalBody } from '../api/types'
import type { RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { isViewer } from '../state/permissions'
import { errorText } from './errors'

/** The risk words the proposal card colours (low, medium, high). */
export const RISKS = ['low', 'medium', 'high'] as const

/** The server's cap on open driver-authored proposals in one room. */
export const MAX_DRIVER_PROPOSALS = 5

/**
 * The driver of an open incident room. Never a viewer, a shadow room (it has no proposals) or a
 * setup room (its proposals are the plan's typed ops, not the action registry).
 */
export function canProposeFix(state: RoomState, user: CurrentUser): boolean {
  return (
    state.status === 'open' &&
    !state.shadow &&
    !state.setup &&
    !isViewer(user) &&
    !!state.driver &&
    state.driver.id === user.id
  )
}

export interface ProposeFixDraft {
  title: string
  risk: string
  rollback: string
  /** The step's description; optional. */
  description: string
}

export const EMPTY_DRAFT: ProposeFixDraft = { title: '', risk: 'medium', rollback: '', description: '' }

/** The request body, or a message naming what is missing. One step: the chosen action and its params. */
export function proposalBody(
  action: ActionInfo | undefined,
  params: Record<string, unknown>,
  draft: ProposeFixDraft,
): { ok: true; body: CreateProposalBody } | { ok: false; field: 'action' | 'title'; error: string } {
  if (!action) return { ok: false, field: 'action', error: 'Choose an action.' }
  const title = draft.title.trim()
  if (!title) return { ok: false, field: 'title', error: 'Give the fix a title.' }
  const description = draft.description.trim()
  return {
    ok: true,
    body: {
      title,
      risk: draft.risk.trim(),
      rollback: draft.rollback.trim(),
      steps: [{ action: action.name, layer: action.layer, params, ...(description ? { description } : {}) }],
    },
  }
}

/**
 * A refusal of POST /api/rooms/{id}/proposals as one plain sentence. A 400 keeps the server's own
 * words: they name the step and the param the action refused.
 */
export function proposeErrorText(e: unknown): string {
  if (e instanceof ApiClientError) {
    switch (e.code) {
      case 'not_driver':
        return 'Only the driver can propose a fix. Take control of the room first.'
      case 'viewer':
        return 'Viewers cannot propose fixes.'
      case 'too_many_proposals':
        return `This room already has ${MAX_DRIVER_PROPOSALS} open fixes you proposed. Approve, reject or wait for one before proposing another.`
      case 'room_closed':
        return 'The room is closed; no more fixes can be proposed.'
      case 'validation':
        return `The server refused the fix: ${e.message}`
    }
  }
  return errorText(e)
}
