// Room list ordering: open rooms first, then newest opened first. The server
// already sorts open-first (docs/api.md), but fixture mode and any client-side
// refresh can return arbitrary order, so the sorting rule lives here too.
import type { RoomSnapshot } from '../api/types'
import type { CurrentUser } from '../state/permissions'
import { isViewer } from '../state/permissions'
import { agentStatusChip } from './tones'

function openedAtMs(room: RoomSnapshot): number {
  const t = Date.parse(room.opened_at)
  return Number.isNaN(t) ? 0 : t
}

/** Comparator: open rooms before closed; within each group, newest opened first. */
export function compareRooms(a: RoomSnapshot, b: RoomSnapshot): number {
  const aRank = a.status === 'open' ? 0 : 1
  const bRank = b.status === 'open' ? 0 : 1
  if (aRank !== bRank) return aRank - bRank
  return openedAtMs(b) - openedAtMs(a)
}

/** Copy of `rooms` in list order (Array.sort is stable, so ties keep input order). */
export function sortRoomsForList(rooms: RoomSnapshot[]): RoomSnapshot[] {
  return [...rooms].sort(compareRooms)
}

// ---- home page helpers (UI refresh §7.2) ----

/**
 * Layers of mine that have a pending approval in this room. Only open rooms count, and a
 * viewer never has any: they cannot approve, so nothing is "waiting for them".
 */
export function pendingLayersForUser(room: RoomSnapshot, user: CurrentUser): string[] {
  if (room.status !== 'open' || isViewer(user)) return []
  const layers: string[] = []
  for (const p of room.open_proposals) {
    for (const layer of p.required_layers) {
      if (p.approvals[layer] === 'pending' && user.layers.includes(layer) && !layers.includes(layer)) {
        layers.push(layer)
      }
    }
  }
  return layers
}

/** Why a room needs this person right now. */
export type NeedsYou = { reason: 'approval'; layers: string[] } | { reason: 'no_driver' }

/**
 * Whether a room needs this person, or null. A pending approval on my layer wins over
 * "no driver". Never set for a viewer or a closed room. Drives the amber `room-card--mine` row.
 */
export function needsYou(room: RoomSnapshot, user: CurrentUser): NeedsYou | null {
  if (room.status !== 'open' || isViewer(user)) return null
  const layers = pendingLayersForUser(room, user)
  if (layers.length > 0) return { reason: 'approval', layers }
  if (room.driver === null) return { reason: 'no_driver' }
  return null
}

export type LiveKind = 'working' | 'attn' | 'done' | 'idle' | 'error'

/** The agent badge on a room row. `kind` maps to `.room-card__live--<kind>`. */
export function roomLiveBadge(room: RoomSnapshot): { kind: LiveKind; label: string } {
  const { state, detail } = room.agent_status
  if (state === 'error') return { kind: 'error', label: 'error' }
  if (state === 'done') return { kind: 'done', label: 'resolved' }
  // A closed room has no live agent, whatever its last status said.
  if (room.status !== 'open') return { kind: 'done', label: 'closed' }
  if (state === 'waiting_approval') return { kind: 'attn', label: 'needs approval' }
  if (state === 'working') return { kind: 'working', label: agentStatusChip(state, detail).label }
  if (state === 'idle') return { kind: 'idle', label: 'idle' }
  return { kind: 'idle', label: detail ? `${state} · ${detail}` : state }
}

export interface RoomStats {
  open: number
  closed: number
  /** Open rooms with no driver. */
  noDriver: RoomSnapshot[]
  /** Open rooms with a pending approval on one of my layers. */
  needsYou: RoomSnapshot[]
  /** Open rooms whose agent is working or waiting for an approval. */
  working: RoomSnapshot[]
}

/** Numbers for the stat strip on the home page, from the room list alone. */
export function roomStats(rooms: RoomSnapshot[], user: CurrentUser): RoomStats {
  const open = rooms.filter((r) => r.status === 'open')
  return {
    open: open.length,
    closed: rooms.length - open.length,
    noDriver: open.filter((r) => r.driver === null),
    needsYou: open.filter((r) => pendingLayersForUser(r, user).length > 0),
    working: open.filter(
      (r) => r.agent_status.state === 'working' || r.agent_status.state === 'waiting_approval',
    ),
  }
}
