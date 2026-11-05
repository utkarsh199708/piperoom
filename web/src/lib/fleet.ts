// The fleet view (X5, docs/api.md "Fleet view"): GET /api/fleet rows and GET /api/fleet/changes pages, checked
// field by field, plus the browser-side sort and filter. Titles, teams, node ids and change summaries come from
// owners.yaml and from changes Piperoom recorded (some sent by outside systems), so they are rendered as text only.
import { ApiClientError } from '../api/client'
import type { FleetChangesPage, FleetHealth, FleetPipeline, FleetVerdict, PipelineChange } from '../api/types'
import { errorText } from './errors'
import { plainText, stripBidi } from './title'

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v)
const str = (v: unknown): string => (typeof v === 'string' ? v : '')
const count = (v: unknown): number => (typeof v === 'number' && Number.isFinite(v) && v > 0 ? Math.floor(v) : 0)
const isTime = (v: unknown): v is string => typeof v === 'string' && !Number.isNaN(Date.parse(v))

/** Health words, worst first: the server's sort order. */
export const HEALTH_ORDER: readonly FleetHealth[] = ['fail', 'warn', 'unknown', 'ok']

export const FLEET_HEALTH_LABEL: Record<FleetHealth, string> = {
  fail: 'Failing',
  warn: 'Degraded',
  unknown: 'Unknown',
  ok: 'Healthy',
}

// The server caps signals at 32 and forecasts at 8; a longer list is cut, not trusted.
const MAX_SIGNALS = 32
const MAX_FORECASTS = 8
const MAX_NAME = 200
const MAX_SUMMARY = 300

const name = (v: unknown, max = MAX_NAME) => {
  const s = stripBidi(str(v)).trim()
  return s.length > max ? `${s.slice(0, max - 1)}…` : s
}

function cleanVerdict(v: unknown): FleetVerdict | null {
  if (!isObj(v) || !str(v.room_id)) return null
  return {
    room_id: str(v.room_id),
    verdict: v.verdict === 'confirmed' || v.verdict === 'disputed' ? v.verdict : '',
    layer_correct: typeof v.layer_correct === 'boolean' ? v.layer_correct : null,
    at: isTime(v.at) ? v.at : '',
  }
}

/** The GET /api/fleet answer as rows the screen can draw: a non-array is [], a row without an id is dropped. */
export function cleanFleet(raw: unknown): FleetPipeline[] {
  if (!Array.isArray(raw)) return []
  const out: FleetPipeline[] = []
  for (const r of raw) {
    if (!isObj(r) || !str(r.pipeline_id)) continue
    const health = HEALTH_ORDER.includes(r.health as FleetHealth) ? (r.health as FleetHealth) : 'unknown'
    const signals = Array.isArray(r.signals_firing) ? r.signals_firing.filter((s): s is string => typeof s === 'string' && s !== '') : []
    const forecasts = Array.isArray(r.forecasts)
      ? r.forecasts.filter((f) => isObj(f) && str(f.metric) && isTime(f.eta)).map((f) => ({ metric: str(f.metric), eta: str(f.eta) }))
      : []
    out.push({
      pipeline_id: str(r.pipeline_id),
      title: plainText(str(r.title)),
      kind: r.kind === 'batch' ? 'batch' : 'cdc',
      tenant: name(r.tenant),
      team: name(r.team),
      health,
      worst_node: name(r.worst_node),
      open_rooms: count(r.open_rooms),
      signals_firing: signals.slice(0, MAX_SIGNALS).map((s) => name(s)),
      changes_24h: count(r.changes_24h),
      forecasts: forecasts.slice(0, MAX_FORECASTS),
      last_verdict: cleanVerdict(r.last_verdict),
      ...(isTime(r.checked_at) ? { checked_at: r.checked_at } : {}),
    })
  }
  return out
}

/** One GET /api/fleet/changes page, checked: rows without a kind, object or time are dropped. */
export function cleanChangesPage(raw: unknown): FleetChangesPage {
  if (!isObj(raw)) return { changes: [], next: '' }
  const changes: PipelineChange[] = []
  for (const c of Array.isArray(raw.changes) ? raw.changes : []) {
    if (!isObj(c) || !str(c.kind) || !str(c.object) || !isTime(c.at)) continue
    changes.push({
      id: typeof c.id === 'number' ? c.id : 0,
      pipeline_id: str(c.pipeline_id),
      kind: str(c.kind),
      object: name(c.object),
      at: c.at,
      ...(str(c.actor) ? { actor: name(c.actor) } : {}),
      source: str(c.source),
      fingerprint: str(c.fingerprint),
      before: c.before,
      after: c.after,
      summary: name(c.summary, MAX_SUMMARY),
    })
  }
  return { changes, next: str(raw.next) }
}

/** An external change's link (X4 `after.url`) when it is an absolute http(s) URL; '' otherwise. */
export function changeLink(c: PipelineChange): string {
  if (!isObj(c.after)) return ''
  const raw = str(c.after.url)
  if (!raw) return ''
  try {
    const u = new URL(raw)
    return (u.protocol === 'http:' || u.protocol === 'https:') && u.host && !u.username && !u.password ? u.href : ''
  } catch {
    return ''
  }
}

/** The soonest forecast ETA in ms, Infinity when the row has none. */
export function soonestEta(row: FleetPipeline): number {
  let best = Infinity
  for (const f of row.forecasts) {
    const t = Date.parse(f.eta)
    if (!Number.isNaN(t) && t < best) best = t
  }
  return best
}

export type FleetSortKey = 'health' | 'eta' | 'name' | 'changes'
export type SortDir = 'asc' | 'desc'

export const FLEET_SORT_KEYS: readonly FleetSortKey[] = ['health', 'eta', 'name', 'changes']

/** Each key's natural first direction: worst health, soonest ETA, A to Z, most changes. */
export const DEFAULT_DIR: Record<FleetSortKey, SortDir> = { health: 'asc', eta: 'asc', name: 'asc', changes: 'desc' }

const byId = (a: FleetPipeline, b: FleetPipeline) => a.pipeline_id.localeCompare(b.pipeline_id)

function compare(key: FleetSortKey, a: FleetPipeline, b: FleetPipeline): number {
  switch (key) {
    case 'health':
      return HEALTH_ORDER.indexOf(a.health) - HEALTH_ORDER.indexOf(b.health)
    case 'eta': {
      const x = soonestEta(a)
      const y = soonestEta(b)
      return x === y ? 0 : x < y ? -1 : 1
    }
    case 'changes':
      return a.changes_24h - b.changes_24h
    default:
      return 0
  }
}

/**
 * The rows sorted by `key` in `dir`, ties by pipeline id A to Z. A pipeline with no forecast stays last in either
 * ETA direction: "no forecast" is not a time.
 */
export function sortFleet(rows: FleetPipeline[], key: FleetSortKey, dir: SortDir): FleetPipeline[] {
  const sign = dir === 'asc' ? 1 : -1
  return [...rows].sort((a, b) => {
    if (key === 'eta') {
      const none = Number(soonestEta(a) === Infinity) - Number(soonestEta(b) === Infinity)
      if (none !== 0) return none
    }
    if (key === 'name') return sign * byId(a, b)
    return sign * compare(key, a, b) || byId(a, b)
  })
}

export interface FleetFilter {
  tenant: string
  team: string
  kind: string
}

/** Exact match on each filter that is set ('' = any), as the server's `?tenant=&team=&kind=` would. */
export function filterFleet(rows: FleetPipeline[], f: FleetFilter): FleetPipeline[] {
  return rows.filter(
    (r) => (!f.tenant || r.tenant === f.tenant) && (!f.team || r.team === f.team) && (!f.kind || r.kind === f.kind),
  )
}

/** The distinct non-empty values of one filter field, A to Z: the options its menu offers. */
export function facetValues(rows: FleetPipeline[], field: keyof FleetFilter): string[] {
  return [...new Set(rows.map((r) => r[field]).filter((v) => v !== ''))].sort((a, b) => a.localeCompare(b))
}

/** The last human judgement in words; '' when there is none. */
export function verdictLabel(v: FleetVerdict | null): string {
  if (!v) return ''
  if (v.verdict === 'confirmed') return 'Confirmed'
  if (v.verdict === 'disputed') return 'Disputed'
  if (v.layer_correct === true) return 'Graded: right layer'
  if (v.layer_correct === false) return 'Graded: wrong layer'
  return ''
}

/**
 * The lane page of a pipeline, optionally pointing at one node. The node is the id exactly as the API gave it
 * (`worst_node`): the fleet never resolves it against the topology, the lane page does.
 */
export function laneHref(pipelineId: string, node?: string): string {
  const base = `/pipelines/${encodeURIComponent(pipelineId)}`
  return node ? `${base}?node=${encodeURIComponent(node)}` : base
}

/** The landing route of a bare app URL (no `#/...` route at all); null when the URL already names a screen. */
export function landingHash(hash: string): string | null {
  return hash === '' || hash === '#' ? '#/fleet' : null
}

/** The error panel's text:a fleet route this server does not serve reads as such, anything else as the API said. */
export function fleetErrorText(e: unknown): string {
  if (e instanceof ApiClientError && e.status === 404) return 'This server does not serve the fleet view.'
  return errorText(e)
}
