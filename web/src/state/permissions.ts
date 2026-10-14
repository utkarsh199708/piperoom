// Pure permission logic. Rules come from docs/api.md: only the driver steers,
// only a layer owner can approve/reject a pending layer, etc.
import type { ApprovalValue } from '../api/types'
import type { HandoffState, ProposalState, RoomState, SuggestionState } from './types'

/** `/api/me.role`: a viewer can read rooms but every write answers 403 viewer. */
export type Role = 'member' | 'viewer'

export interface CurrentUser {
  id: string
  name: string
  layers: string[]
  /** Absent in fixture mode and old servers: treated as a member. */
  role?: Role
  /** The IdP sent no groups claim, so group-based rights are not available. */
  groupsMissing?: boolean
  /** `/api/me.solo`: show the solo banner and ask for the typed confirm up front. */
  solo?: boolean
  /** `/api/me.admin`: may change integrations. Absent = not an admin (the page is read-only). */
  admin?: boolean
}

/** A viewer sees everything and changes nothing; the server enforces it, the UI hides the controls. */
export function isViewer(user: CurrentUser): boolean {
  return user.role === 'viewer'
}

export function isDriver(state: RoomState, user: CurrentUser): boolean {
  return state.driver?.id === user.id
}

export function isOpen(state: RoomState): boolean {
  return state.status === 'open'
}

// A driver lease expires after 30 minutes (docs/api.md). When it has expired
// (or there is no driver at all, e.g. a webhook room), anyone may take control.
export function isLeaseExpired(state: RoomState): boolean {
  const until = state.driver?.lease_until
  if (!until) return false
  return Date.parse(until) <= Date.now()
}

export function canSteer(state: RoomState, user: CurrentUser): boolean {
  return !isViewer(user) && isOpen(state) && isDriver(state, user)
}

export function canSuggest(state: RoomState, user: CurrentUser): boolean {
  return !isViewer(user) && isOpen(state) && !isDriver(state, user) && state.driver != null
}

export function canRequestControl(state: RoomState, user: CurrentUser): boolean {
  return (
    !isViewer(user) &&
    isOpen(state) &&
    !isDriver(state, user) &&
    state.driver != null &&
    !isLeaseExpired(state) &&
    !state.control_requests.some((r) => r.from.id === user.id)
  )
}

export function canTakeControl(state: RoomState, user: CurrentUser): boolean {
  return (
    !isViewer(user) &&
    isOpen(state) &&
    !isDriver(state, user) &&
    (state.driver == null || isLeaseExpired(state))
  )
}

export function canGrantControl(state: RoomState, user: CurrentUser): boolean {
  return !isViewer(user) && isOpen(state) && isDriver(state, user)
}

export function canHandoff(state: RoomState, user: CurrentUser): boolean {
  return !isViewer(user) && isOpen(state) && isDriver(state, user)
}

export function canClose(state: RoomState, user: CurrentUser): boolean {
  return !isViewer(user) && isOpen(state) && isDriver(state, user)
}

export function canSendSuggestionToAgent(
  state: RoomState,
  user: CurrentUser,
  suggestion: SuggestionState,
): boolean {
  return !isViewer(user) && isOpen(state) && isDriver(state, user) && suggestion.suggestion_id != null
}

/**
 * What the layer rules need from a proposal: the full ProposalState, or a room snapshot's
 * open proposal (the room list) with `status: 'open'`.
 */
export type ProposalLike = Pick<ProposalState, 'required_layers' | 'approvals' | 'status'>

export function pendingLayersFor(
  proposal: ProposalLike,
  user: CurrentUser,
): string[] {
  return proposal.required_layers.filter(
    (layer) => proposal.approvals[layer] === 'pending' && user.layers.includes(layer),
  )
}

export function canDecideLayer(
  proposal: ProposalLike,
  user: CurrentUser,
  layer: string,
): boolean {
  return (
    !isViewer(user) &&
    proposal.status === 'open' &&
    proposal.approvals[layer] === 'pending' &&
    user.layers.includes(layer)
  )
}

export function canApprove(
  proposal: ProposalState,
  user: CurrentUser,
  layer: string,
): boolean {
  return canDecideLayer(proposal, user, layer)
}

export function canReject(
  proposal: ProposalState,
  user: CurrentUser,
  layer: string,
): boolean {
  return canDecideLayer(proposal, user, layer)
}

export function canAcceptHandoff(user: CurrentUser, handoff: HandoffState): boolean {
  return !isViewer(user) && handoff.status === 'pending' && handoff.to.id === user.id
}

export function proposalAllApproved(proposal: ProposalState): boolean {
  return proposal.status === 'approved'
}

export function proposalApprovalState(
  proposal: ProposalState,
  layer: string,
): ApprovalValue {
  return proposal.approvals[layer] ?? 'pending'
}
