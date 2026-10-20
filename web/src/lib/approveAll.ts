// Approve and run (P2, tasks/P2-solo-ceremony.md P2c; docs/api.md POST /api/proposals/{pid}/approve-all).
// When one person may approve every layer a proposal needs, the card offers one button that records
// all of them in one call, in place of a click and a confirm per layer. Pure: the card, the decision
// bar and the A key all ask this one function, so they never disagree about which flow is on screen.
import type { RoomMode } from '../api/types'
import type { ProposalState, RoomState } from '../state/types'
import type { CurrentUser, ProposalLike } from '../state/permissions'
import { canDecideLayer, isFallbackLayer, isViewer } from '../state/permissions'

/**
 * The words of the one button, on the card, in its confirm and on the decision bar. With write mode
 * off nothing runs, so they never say "run" (the run confirm's pair since P1e).
 */
export function approveAllLabel(writeOff: boolean): string {
  return writeOff ? 'Approve (will not run)' : 'Approve and run'
}

/** The typed confirm of the solo user for an approve-all: one string for every layer of the call. */
export function soloAllConfirmText(pipelineId: string): string {
  return `${pipelineId}/*`
}

/**
 * The layers one "Approve and run" would record (the pending ones, in `required_layers` order), or
 * null when the card keeps its per-layer rows.
 *
 * The caller must cover every required layer, an approved one included, because the server checks
 * all of them (403 not_layer_owner otherwise): `mode.solo` (the server's own answer for this room
 * and caller) or `/api/me` layers that hold each one. With a single pending layer the row's own
 * Approve is already one click behind the same run confirm, so the card keeps it, unless the row
 * cannot offer it (mode.solo for a layer `/api/me` does not list). `driverId` is the room's driver:
 * they cover the proposal's `fallback_layers` (pass it only for an open room).
 */
export function approveAllLayers(
  proposal: ProposalLike,
  user: CurrentUser,
  mode: RoomMode | null | undefined,
  driverId?: string | null,
): string[] | null {
  if (isViewer(user) || proposal.status !== 'open') return null
  if (proposal.required_layers.some((l) => proposal.approvals[l] === 'rejected')) return null
  const pending = proposal.required_layers.filter((l) => (proposal.approvals[l] ?? 'pending') === 'pending')
  if (pending.length === 0) return null
  // The room's driver covers a layer nobody owns (P2b `fallback_layers`), as the server's approve-all
  // does: one owned layer plus one unowned layer is one "Approve and run" for a lone driver.
  const asDriver = driverId != null && driverId === user.id
  const covers =
    mode?.solo === true ||
    proposal.required_layers.every((l) => user.layers.includes(l) || (asDriver && isFallbackLayer(proposal, l)))
  if (!covers) return null
  if (pending.length === 1 && canDecideLayer(proposal, user, pending[0], driverId)) return null
  return pending
}

/**
 * `approveAllLayers` for a proposal of the room on screen: a closed room decides nothing, and a
 * proposal whose approve-all the server refused (403 not_layer_owner, kept in
 * `approve_all_refused`) goes back to the per-layer rows, so the layers the person does own can
 * still be approved one at a time.
 */
export function approveAllFor(state: RoomState, proposal: ProposalState, user: CurrentUser): string[] | null {
  if (state.status !== 'open') return null
  if (state.approve_all_refused?.[proposal.proposal_id] !== undefined) return null
  return approveAllLayers(proposal, user, state.mode, state.driver?.id)
}
