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
  /** `/api/me.verified` (server-set); absent in fixture mode, which counts as unverified. */
  verified?: boolean
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

/**
 * How many people are in the room: everyone the presence frame or the event log names, the driver,
 * and you. The sole-member rules (P2) read it: nobody to ask for control, nobody to hand off to.
 */
export function roomPeopleCount(state: RoomState, presenceIds: readonly string[], userId: string): number {
  const ids = [...presenceIds, ...state.members.map((m) => m.id), userId]
  if (state.driver) ids.push(state.driver.id)
  return new Set(ids).size
}

/**
 * How many people a hand-off could go to, you included: once `GET /api/people` (the documented
 * hand-off targets the dialog picks from) has loaded, those plus the log's members, the driver and
 * you. Presence is left out then: a viewer watching is no one to hand off to, and a member with a live
 * session is already in /api/people. Until it loads (or when it fails), roomPeopleCount.
 */
export function handoffTargetCount(
  state: RoomState,
  presenceIds: readonly string[],
  userId: string,
  people: readonly { id: string }[] | undefined,
): number {
  if (!people) return roomPeopleCount(state, presenceIds, userId)
  return roomPeopleCount(state, people.map((p) => p.id), userId)
}

/**
 * `people` is roomPeopleCount: the room's sole member never asks for control (P2: the server makes
 * the first member of a driverless alert room its driver).
 */
export function canRequestControl(state: RoomState, user: CurrentUser, people: number): boolean {
  return (
    people > 1 &&
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

/** A hand-off needs someone to take it: offered only when `people` (handoffTargetCount) is above one (P2). */
export function canHandoff(state: RoomState, user: CurrentUser, people: number): boolean {
  return people > 1 && !isViewer(user) && isOpen(state) && isDriver(state, user)
}

export function canClose(state: RoomState, user: CurrentUser): boolean {
  return !isViewer(user) && isOpen(state) && isDriver(state, user)
}

export function canSendSuggestionToAgent(
  state: RoomState,
  user: CurrentUser,
  suggestion: SuggestionState,
): boolean {
  return !isViewer(user) && isOpen(state) && isDriver(state, user) && suggestion.suggestion_id != null && suggestion.status === 'pending'
}

/**
 * What the layer rules need from a proposal: the full ProposalState, or a room snapshot's
 * open proposal (the room list) with `status: 'open'`.
 */
export type ProposalLike = Pick<ProposalState, 'required_layers' | 'approvals' | 'status' | 'fallback_layers'>

/** A layer nobody owns (P2b `fallback_layers`): the room's driver approves it. */
export function isFallbackLayer(proposal: Pick<ProposalLike, 'fallback_layers'>, layer: string): boolean {
  return proposal.fallback_layers?.includes(layer) === true
}

export function pendingLayersFor(
  proposal: ProposalLike,
  user: CurrentUser,
): string[] {
  return proposal.required_layers.filter(
    (layer) => proposal.approvals[layer] === 'pending' && user.layers.includes(layer),
  )
}

/**
 * `driverId` is the room's driver: they may decide a layer in `fallback_layers` (docs/api.md, P2b).
 * Pass it only for an open room; the server checks the driver again when it appends the event.
 */
export function canDecideLayer(
  proposal: ProposalLike,
  user: CurrentUser,
  layer: string,
  driverId?: string | null,
): boolean {
  return (
    !isViewer(user) &&
    proposal.status === 'open' &&
    proposal.approvals[layer] === 'pending' &&
    (user.layers.includes(layer) || (driverId != null && driverId === user.id && isFallbackLayer(proposal, layer)))
  )
}

export function canApprove(
  proposal: ProposalState,
  user: CurrentUser,
  layer: string,
  driverId?: string | null,
): boolean {
  return canDecideLayer(proposal, user, layer, driverId)
}

export function canReject(
  proposal: ProposalState,
  user: CurrentUser,
  layer: string,
  driverId?: string | null,
): boolean {
  return canDecideLayer(proposal, user, layer, driverId)
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
