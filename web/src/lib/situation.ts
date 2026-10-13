import type { RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canDecideLayer } from '../state/permissions'

/** The first layer of a still-open proposal that this user may decide. Never for a viewer. */
export function firstPendingLayer(state: RoomState, user: CurrentUser): string | null {
  for (const p of state.proposals) {
    for (const layer of p.required_layers) {
      if (canDecideLayer(p, user, layer)) return layer
    }
  }
  return null
}
