// Room list ordering: open rooms first, then newest opened first. The server
// already sorts open-first (docs/api.md), but fixture mode and any client-side
// refresh can return arbitrary order, so the sorting rule lives here too.
import type { RoomSnapshot } from '../api/types'
import { canDecideLayer, isViewer } from '../state/permissions'
import type { CurrentUser } from '../state/permissions'
import { roomStatus, statusInputFromSnapshot } from './roomStatus'
import type { RoomStatusInfo } from './roomStatus'

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

// ---- Room list (the home screen) ----
// Everything the list needs to decide is here as pure functions, so the screen only draws.

export type RoomTab = 'open' | 'mine' | 'resolved' | 'all'

export const ROOM_TABS: RoomTab[] = ['open', 'mine', 'resolved', 'all']

/** Layers of this room's open proposals that wait on a decision (any person). */
function pendingLayers(room: RoomSnapshot): string[] {
  if (room.status !== 'open') return []
  const out: string[] = []
  for (const p of room.open_proposals ?? []) {
    for (const layer of p.required_layers) {
      if (p.approvals[layer] === 'pending' && !out.includes(layer)) out.push(layer)
    }
  }
  return out
}

/** True when an open proposal in this room waits on someone's approval. */
export function needsApproval(room: RoomSnapshot): boolean {
  return pendingLayers(room).length > 0
}

/**
 * The pending layers this person owns. "Needs you" is only ever about these: a layer
 * owned by someone else never tints a row. A viewer owns nothing and can decide nothing.
 */
export function layersNeedingYou(room: RoomSnapshot, user: CurrentUser): string[] {
  if (room.status !== 'open') return []
  const out: string[] = []
  for (const p of room.open_proposals ?? []) {
    // The same rule the proposals panel uses to show Approve/Reject (state/permissions.ts).
    const proposal = { required_layers: p.required_layers, approvals: p.approvals, status: 'open' as const }
    for (const layer of p.required_layers) {
      if (canDecideLayer(proposal, user, layer) && !out.includes(layer)) out.push(layer)
    }
  }
  return out
}

export function needsYou(room: RoomSnapshot, user: CurrentUser): boolean {
  return layersNeedingYou(room, user).length > 0
}

/**
 * "Take control" on a list row: same rule as canTakeControl in state/permissions.ts
 * (open room, not the driver, and no driver or an expired lease), from the snapshot fields.
 */
export function canTakeControlOf(room: RoomSnapshot, user: CurrentUser): boolean {
  if (isViewer(user) || room.status !== 'open') return false
  if (room.driver?.id === user.id) return false
  return !hasLiveDriver(room)
}

/** A driver whose lease has not run out. No lease time means a live driver; an expired lease means none. */
export function hasLiveDriver(room: RoomSnapshot, now = Date.now()): boolean {
  if (room.driver == null) return false
  const until = room.driver.lease_until
  return until == null || !(Date.parse(until) <= now)
}

/** Rooms that need me: I drive an open room, or one of my layers has to decide. */
export function isMine(room: RoomSnapshot, user: CurrentUser): boolean {
  if (room.status !== 'open') return false
  return room.driver?.id === user.id || needsYou(room, user)
}

export function roomsForTab(rooms: RoomSnapshot[], tab: RoomTab, user: CurrentUser): RoomSnapshot[] {
  switch (tab) {
    case 'open':
      return rooms.filter((r) => r.status === 'open')
    case 'mine':
      return rooms.filter((r) => isMine(r, user))
    case 'resolved':
      return rooms.filter((r) => r.status !== 'open')
    default:
      return rooms
  }
}

export function roomsForPipeline(rooms: RoomSnapshot[], pipelineId: string | null): RoomSnapshot[] {
  return pipelineId ? rooms.filter((r) => r.pipeline_id === pipelineId) : rooms
}

/** Distinct pipeline ids in the rooms, sorted. */
export function pipelineIds(rooms: RoomSnapshot[]): string[] {
  return [...new Set(rooms.map((r) => r.pipeline_id).filter(Boolean))].sort()
}

export interface RoomStats {
  open: number
  openWithDriver: number
  needsYou: number
  /** Layers waiting on this person, across rooms (for the stat's caption). */
  needsYouLayers: string[]
  agentWorking: number
  /** Caption of the first working room, e.g. "customers · step 2". */
  agentCaption: string
  resolvedToday: number
  /** Median open-to-close minutes of the rooms resolved today, or null. */
  medianMinutes: number | null
}

function sameLocalDay(a: number, b: number): boolean {
  const x = new Date(a)
  const y = new Date(b)
  return x.getFullYear() === y.getFullYear() && x.getMonth() === y.getMonth() && x.getDate() === y.getDate()
}

export function roomStats(rooms: RoomSnapshot[], user: CurrentUser, now = Date.now()): RoomStats {
  const open = rooms.filter((r) => r.status === 'open')
  const working = open.filter((r) => r.agent_status.state === 'working')
  const layers: string[] = []
  let mine = 0
  for (const r of open) {
    const l = layersNeedingYou(r, user)
    if (l.length > 0) mine++
    for (const x of l) if (!layers.includes(x)) layers.push(x)
  }
  const minutes = rooms
    .filter((r) => r.status !== 'open' && r.closed_at && sameLocalDay(Date.parse(r.closed_at), now))
    .map((r) => (Date.parse(r.closed_at as string) - Date.parse(r.opened_at)) / 60000)
    .filter((m) => Number.isFinite(m) && m >= 0)
    .sort((a, b) => a - b)
  const mid = Math.floor(minutes.length / 2)
  const median =
    minutes.length === 0 ? null : minutes.length % 2 ? minutes[mid] : (minutes[mid - 1] + minutes[mid]) / 2
  const first = working[0]
  return {
    open: open.length,
    openWithDriver: open.filter((r) => hasLiveDriver(r, now)).length,
    needsYou: mine,
    needsYouLayers: layers,
    agentWorking: working.length,
    agentCaption: first ? [first.pipeline_id, first.agent_status.detail].filter(Boolean).join(' · ') : 'idle',
    resolvedToday: minutes.length,
    medianMinutes: median === null ? null : Math.round(median),
  }
}

export type Dot = 'ok' | 'warn' | 'fail' | 'info' | 'agent' | 'idle'

/** The room's status column: a dot colour and a word. The word always carries the meaning. */
export function roomStatusCell(room: RoomSnapshot, now = Date.now()): RoomStatusInfo {
  return roomStatus(statusInputFromSnapshot(room), now)
}

/** The agent column ("working · step 2", "waiting · 1 of 2", "idle"). */
export function agentCell(room: RoomSnapshot): { dot: Dot; label: string; agent: boolean } {
  const { state, detail } = room.agent_status
  // A closed room's agent is finished, whatever its last status event said.
  if (room.status !== 'open') return { dot: 'ok', label: 'resolved', agent: false }
  switch (state) {
    case 'working':
      return { dot: 'agent', label: detail ? `working · ${detail}` : 'working', agent: true }
    case 'waiting_approval': {
      const p = room.open_proposals?.[0]
      const done = p ? p.required_layers.filter((l) => p.approvals[l] === 'approved').length : 0
      const label = p ? `waiting · ${done} of ${p.required_layers.length}` : 'waiting'
      return { dot: 'idle', label, agent: false }
    }
    case 'done':
      return { dot: 'ok', label: 'done', agent: false }
    case 'error':
      return { dot: 'fail', label: detail ? `error · ${detail}` : 'error', agent: false }
    default:
      return { dot: 'idle', label: state || 'idle', agent: false }
  }
}
