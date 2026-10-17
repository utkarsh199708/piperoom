// Owner pages (Q0, QA G2): who was asked to look at a room, and which pages this person has not seen.
// Pure functions plus a small localStorage store; the hook (hooks/useOwnerPages.ts) wires them to the room list.
import { LAYERS } from '../api/types'
import type { PageSummary, RoomSnapshot } from '../api/types'
import type { CurrentUser } from '../state/permissions'
import { isViewer } from '../state/permissions'
import type { PageState, RoomState } from '../state/types'
import { plainText } from './title'

/** Longest reason the server takes (docs/api.md, POST /api/rooms/{id}/page). */
export const MAX_PAGE_REASON = 300

/** A page of one room, as the room list carries it. */
export interface PagedRoom {
  room: RoomSnapshot
  page: PageSummary
  /** `${room id}|${layer}`: one slot per room and layer, which is how the server keeps them too. */
  key: string
}

export function pageKey(roomId: string, layer: string): string {
  return `${roomId}|${layer}`
}

/**
 * Any member may page owners of an open room. The server refuses viewers (403 viewer), closed rooms
 * (409 room_closed), and a second page of the same layer within 10 minutes (409 already_paged).
 */
export function canPageOwner(state: RoomState, user: CurrentUser): boolean {
  return !isViewer(user) && state.status === 'open'
}

/**
 * True when an owner entry names this person: their id (also as `user:<id>`), or `group:<name>` for a
 * group they belong to. `groups` comes from the session; an empty list matches no group.
 */
export function ownsPage(owners: string[], userId: string, groups: readonly string[] = []): boolean {
  return owners.some((o) => o === userId || o === `user:${userId}` || (o.startsWith('group:') && groups.includes(o.slice(6))))
}

/** Every page, in open rooms, that names `userId` (or one of `groups`) among its owners, newest first. */
export function pagesFor(rooms: RoomSnapshot[], userId: string, groups: readonly string[] = []): PagedRoom[] {
  const out: PagedRoom[] = []
  for (const room of rooms) {
    if (room.status !== 'open') continue
    for (const page of room.pages ?? []) {
      if (Array.isArray(page.owners) && ownsPage(page.owners, userId, groups)) {
        out.push({ room, page, key: pageKey(room.id, page.layer) })
      }
    }
  }
  return out.sort((a, b) => (Date.parse(b.page.at) || 0) - (Date.parse(a.page.at) || 0))
}

/** What the person has seen: page key -> the `at` of the newest page of that slot they saw. */
export type SeenPages = Record<string, string>

/** True when `at` is later than `seenAt`; an unreadable time counts as new unless it is the very same text. */
function newer(at: string, seenAt: string | undefined): boolean {
  if (seenAt === undefined) return true
  const a = Date.parse(at)
  const b = Date.parse(seenAt)
  if (Number.isNaN(a) || Number.isNaN(b)) return at !== seenAt
  return a > b
}

/** The pages for `userId` that are newer than what `seen` records. */
export function unseenPages(rooms: RoomSnapshot[], userId: string, seen: SeenPages, groups: readonly string[] = []): PagedRoom[] {
  return pagesFor(rooms, userId, groups).filter((p) => newer(p.page.at, seen[p.key]))
}

/** `seen` with `items` marked as seen (never moves a slot back to an older time). */
export function markSeen(seen: SeenPages, items: PagedRoom[]): SeenPages {
  let next = seen
  for (const { key, page } of items) {
    if (newer(page.at, next[key])) {
      if (next === seen) next = { ...seen }
      next[key] = page.at
    }
  }
  return next === seen ? seen : trim(next)
}

const MAX_SEEN = 200

// The store would otherwise grow with every room ever paged; keep the newest entries.
function trim(seen: SeenPages): SeenPages {
  const keys = Object.keys(seen)
  if (keys.length <= MAX_SEEN) return seen
  const kept = keys.sort((a, b) => (Date.parse(seen[b]) || 0) - (Date.parse(seen[a]) || 0)).slice(0, MAX_SEEN)
  return Object.fromEntries(kept.map((k) => [k, seen[k]]))
}

const seenKey = (userId: string) => `piperoom.pages.seen.${userId}`

/** The seen pages of this person in this browser; {} when the store is empty, blocked or damaged. */
export function readSeen(userId: string): SeenPages {
  try {
    const raw = localStorage.getItem(seenKey(userId))
    if (!raw) return {}
    const parsed: unknown = JSON.parse(raw)
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return {}
    const out: SeenPages = {}
    for (const [k, v] of Object.entries(parsed)) if (typeof v === 'string') out[k] = v
    return out
  } catch {
    return {}
  }
}

/** Best effort: a blocked store only means the banner can come back after a reload. */
export function writeSeen(userId: string, seen: SeenPages): void {
  try {
    localStorage.setItem(seenKey(userId), JSON.stringify(seen))
  } catch {
    /* storage blocked: nothing to keep */
  }
}

// The layer a page form starts from: the first look's suspect, else a layer a fix is waiting on. A person
// can change it; an empty answer stays empty so a page is never aimed by accident.
export function suggestedPageLayer(state: RoomState): string {
  const guess = state.triage?.status === 'ok' ? state.triage.suspected_layer : undefined
  if (guess && (LAYERS as readonly string[]).includes(guess)) return guess
  for (const p of state.proposals) {
    if (p.status !== 'open') continue
    const waiting = p.required_layers.find((l) => p.approvals[l] === 'pending')
    if (waiting) return waiting
  }
  return ''
}

/** "Kabir, Meera" for owner ids; an id the room has no name for stays as it is. */
export function ownerNames(owners: string[], names: Record<string, string> = {}): string {
  return owners.map((id) => plainText(names[id] ?? id, 40)).join(', ')
}

/** Who paged: "Agent", the person's name, or the server for an automatic page. */
export function pagedBy(page: Pick<PageState, 'by' | 'auto'>): string {
  if (page.auto) return 'Piperoom (auto)'
  if (page.by.kind === 'agent') return 'Agent'
  return plainText(page.by.name || page.by.id, 40)
}
