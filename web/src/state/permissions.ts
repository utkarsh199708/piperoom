// Pure permission logic. Rules come from docs/api.md: only the driver steers,
// only a layer owner can approve/reject a pending layer, etc.
import type { ApprovalValue } from '../api/types'
import type { HandoffState, ProposalState, RoomState, SuggestionState } from './types'

export interface CurrentUser {
  id: string
  name: string
  layers: string[]
}

export function isDriver(state: RoomState, user: CurrentUser): boolean {
  return state.driver?.id === user.id
}

export function isOpen(state: RoomState): boolean {
  return state.status === 'open'
}

export function canSteer(state: RoomState, user: CurrentUser): boolean {
  return isOpen(state) && isDriver(state, user)
}

export function canSuggest(state: RoomState, user: CurrentUser): boolean {
  return isOpen(state) && !isDriver(state, user) && state.driver != null
}

export function canRequestControl(state: RoomState, user: CurrentUser): boolean {
  return (
    isOpen(state) &&
    !isDriver(state, user) &&
    !state.control_requests.some((r) => r.from.id === user.id)
  )
}

export function canGrantControl(state: RoomState, user: CurrentUser): boolean {
  return isOpen(state) && isDriver(state, user)
}

export function canHandoff(state: RoomState, user: CurrentUser): boolean {
  return isOpen(state) && isDriver(state, user)
}

export function canClose(state: RoomState, user: CurrentUser): boolean {
  return isOpen(state) && isDriver(state, user)
}

export function canSendSuggestionToAgent(
  state: RoomState,
  user: CurrentUser,
  suggestion: SuggestionState,
): boolean {
  return isOpen(state) && isDriver(state, user) && suggestion.suggestion_id != null
}

export function pendingLayersFor(
  proposal: ProposalState,
  user: CurrentUser,
): string[] {
  return proposal.required_layers.filter(
    (layer) => proposal.approvals[layer] === 'pending' && user.layers.includes(layer),
  )
}

export function canDecideLayer(
  proposal: ProposalState,
  user: CurrentUser,
  layer: string,
): boolean {
  return proposal.approvals[layer] === 'pending' && user.layers.includes(layer)
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
  return handoff.status === 'pending' && handoff.to.id === user.id
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
