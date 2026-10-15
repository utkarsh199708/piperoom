// Readers for the F8 tool results (what_changed, similar_incidents, runbook_lookup). Their `data`
// comes from tool output the agent got back, so every field is checked and anything unexpected is
// dropped: the UI renders these as plain text only, never as HTML or links.

import type { StepState } from '../state/types'

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v)
const str = (v: unknown): string => (typeof v === 'string' ? v : '')

/** Cap of one before/after document shown in the lane (the server stores at most 4 KiB). */
export const MAX_DIFF_CHARS = 4096

export type ChangeKind = 'ddl' | 'connector_config' | 'connector_state' | 'schema_version'

export interface ChangeRow {
  id: number
  kind: string
  object: string
  at: string
  actor: string
  summary: string
  before: Record<string, string> | null
  after: Record<string, string> | null
}

function flat(v: unknown): Record<string, string> | null {
  if (!isObj(v)) return null
  const out: Record<string, string> = {}
  for (const [k, val] of Object.entries(v)) {
    out[k] = typeof val === 'string' ? val : JSON.stringify(val)
  }
  return out
}

/** The changes of a what_changed result, newest first; [] for anything else. */
export function parseChanges(data: unknown): ChangeRow[] {
  if (!isObj(data) || !Array.isArray(data.changes)) return []
  const rows: ChangeRow[] = []
  for (const c of data.changes) {
    if (!isObj(c) || !str(c.kind) || !str(c.object) || Number.isNaN(Date.parse(str(c.at)))) continue
    rows.push({
      id: typeof c.id === 'number' ? c.id : 0,
      kind: str(c.kind),
      object: str(c.object),
      at: str(c.at),
      actor: str(c.actor),
      summary: str(c.summary),
      before: flat(c.before),
      after: flat(c.after),
    })
  }
  return rows.sort((a, b) => Date.parse(b.at) - Date.parse(a.at))
}

/** The layer a change belongs to, for its colour (docs: ddl → postgres, connector_* → kafka-connect). */
export function changeLayer(kind: string): string {
  switch (kind) {
    case 'ddl':
      return 'postgres'
    case 'connector_config':
    case 'connector_state':
      return 'kafka-connect'
    case 'schema_version':
      return 'schema-registry'
    default:
      return 'kafka'
  }
}

/** One "key: value" line per key of a before/after document, cut at MAX_DIFF_CHARS. */
export function diffText(doc: Record<string, string> | null): string {
  if (!doc) return ''
  const text = Object.entries(doc)
    .map(([k, v]) => `${k}: ${v}`)
    .join('\n')
  return text.length > MAX_DIFF_CHARS ? `${text.slice(0, MAX_DIFF_CHARS)}…` : text
}

export interface SimilarHit {
  room_id: string
  title: string
  date: string
  fix_that_worked: string
  fix_actions: string[]
  /** The layer a person labelled as the cause; '' when the room has no label. */
  layer: string
  duration_s: number
  closed_by: string
  resolution: string
}

function readHit(h: unknown): SimilarHit | null {
  if (!isObj(h) || !str(h.room_id)) return null
  return {
    room_id: str(h.room_id),
    title: str(h.title),
    date: str(h.date) || str(h.closed_at) || str(h.opened_at),
    fix_that_worked: str(h.fix_that_worked),
    fix_actions: Array.isArray(h.fix_actions) ? h.fix_actions.filter((a): a is string => typeof a === 'string') : [],
    layer: str(h.layer),
    duration_s: typeof h.duration_s === 'number' ? h.duration_s : 0,
    closed_by: str(h.closed_by),
    resolution: str(h.resolution),
  }
}

/**
 * Whether a past room is worth offering as history (QA G7: a room titled "test", closed with a one-line
 * note, was shown as "Seen before"). It needs a resolution and something the room learned: a fix that
 * worked or ran, or a label naming the layer. The hit carries no event count and no hypothesis, so a
 * short room that has both is still shown; the server-side similar_incidents filter is the other half.
 */
export function isUsableHit(h: SimilarHit): boolean {
  if (!h.resolution.trim()) return false
  return h.fix_that_worked.trim() !== '' || h.fix_actions.length > 0 || h.layer.trim() !== ''
}

/**
 * The best usable hit of a similar_incidents result (hits come best first), or null. Hits that are not
 * worth showing are skipped, not just the first one. `closed_by` is only ever the server's verified name.
 */
export function topSimilarHit(data: unknown): SimilarHit | null {
  if (!isObj(data) || !Array.isArray(data.hits)) return null
  for (const raw of data.hits) {
    const hit = readHit(raw)
    if (hit && isUsableHit(hit)) return hit
  }
  return null
}

export interface RunbookHit {
  section: string
  items: string[]
}

/** The hits of a runbook_lookup result that carry checklist items. */
export function runbookHits(data: unknown): RunbookHit[] {
  if (!isObj(data) || !Array.isArray(data.hits)) return []
  return data.hits.map((h) => {
    if (!isObj(h)) return { section: '', items: [] }
    const items = Array.isArray(h.items) ? h.items.filter((i): i is string => typeof i === 'string') : []
    // Keep indexes stable: an item that is not text still takes its place.
    const raw = Array.isArray(h.items) ? h.items : []
    return { section: str(h.section) || str(h.title) || str(h.heading), items: raw.length === items.length ? items : raw.map((i) => str(i)) }
  })
}

/** "12 min" / "45 s" / "2 h 5 min" for a duration in seconds. */
export function durationLabel(s: number): string {
  if (s < 60) return `${Math.max(0, Math.round(s))} s`
  const m = Math.round(s / 60)
  if (m < 60) return `${m} min`
  const h = Math.floor(m / 60)
  return m % 60 ? `${h} h ${m % 60} min` : `${h} h`
}

/** The newest what_changed result that holds at least one change; [] when there is none. */
export function latestChanges(steps: StepState[]): { changes: ChangeRow[]; seen: boolean } {
  let seen = false
  for (let i = steps.length - 1; i >= 0; i--) {
    const calls = steps[i].tool_calls
    for (let j = calls.length - 1; j >= 0; j--) {
      if (calls[j].tool !== 'what_changed' || calls[j].is_error || calls[j].data === undefined) continue
      seen = true
      const changes = parseChanges(calls[j].data)
      if (changes.length > 0) return { changes, seen }
    }
  }
  return { changes: [], seen }
}


/** The top usable hit of the newest similar_incidents result that has one. */
export function latestSimilarHit(steps: StepState[]): SimilarHit | null {
  for (let i = steps.length - 1; i >= 0; i--) {
    const calls = steps[i].tool_calls
    for (let j = calls.length - 1; j >= 0; j--) {
      if (calls[j].tool !== 'similar_incidents' || calls[j].is_error) continue
      const hit = topSimilarHit(calls[j].data)
      if (hit) return hit
    }
  }
  return null
}

