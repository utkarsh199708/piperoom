// Drift facts (X1): the watcher's count_drift, row_drift, dup_keys and order_skew entries, read from the evidence
// JSON of a watcher-opened room (room.created.alert, docs/api.md "Watcher alerts"). The evidence is untrusted
// input: every string is cleaned and capped here, and callers render it as text, never as HTML.
import type { DriftKind, Topology, TopologyNode } from '../api/types'
import { durationLabel } from './time'
import { stripBidi } from './title'

export const DRIFT_KINDS: readonly DriftKind[] = ['count_drift', 'row_drift', 'dup_keys', 'order_skew']

// The other signals share the evidence's `signals` list but are not drift; dlq_growth's `window` is a word.
const NOT_DRIFT = new Set([
  'connector_down',
  'slot_inactive',
  'slot_wal',
  'dlq_growth',
  'config_risk',
  'consumer_lag',
  'group_unstable',
  'offset_below_log_start',
  'forecast',
  'config_drift',
])

const MAX_FACTS = 16
const MAX_NAME_CHARS = 200
// docs/api.md caps detail at 200 bytes; a longer one is cut, not trusted.
const MAX_DETAIL_CHARS = 200

/** One drift entry, cleaned. Numbers are null when the entry did not carry a usable one. */
export interface DriftFact {
  /** The known kind, or null for a missing or unknown one, which is shown as a neutral fact. */
  kind: DriftKind | null
  /** The entry's `signal` as sent (cleaned); '' when missing. */
  signal: string
  severity: string
  table: string
  topic: string
  /** RFC 3339 window ends; '' when missing or not a time. */
  from: string
  to: string
  source: number | null
  sink: number | null
  missing: number | null
  mismatched: number | null
  sample: number | null
  detail: string
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

// Control characters (C0, DEL, C1) become spaces; bidi controls go (they would reorder what people read).
// eslint-disable-next-line no-control-regex
const CONTROL = /[\u0000-\u001f\u007f-\u009f]+/g

/** Untrusted text cleaned for one line: no bidi or control characters, at most `max` characters. */
export function cleanText(v: unknown, max: number): string {
  if (typeof v !== 'string') return ''
  const s = stripBidi(v).replace(CONTROL, ' ').replace(/\s+/g, ' ').trim()
  const chars = Array.from(s)
  return chars.length > max ? chars.slice(0, max - 1).join('') + '…' : s
}

function count(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) && v >= 0 ? Math.floor(v) : null
}

function time(v: unknown): string {
  return typeof v === 'string' && v.length <= 64 && !Number.isNaN(Date.parse(v)) ? v : ''
}

/**
 * The drift facts of a room's alert evidence. Only a watcher-opened room has them: the caller passes the
 * server-set alert_source, so a webhook body shaped like watcher evidence never draws facts. An entry whose
 * `signal` is one of the four kinds is a known fact; an entry with a missing or unknown `signal` and a window
 * object (the drift shape) is a neutral fact. Never throws.
 */
export function driftFacts(alert: unknown, alertSource: string | undefined): DriftFact[] {
  if (alertSource !== 'watcher' || !isRecord(alert) || !Array.isArray(alert.signals)) return []
  const out: DriftFact[] = []
  for (const e of alert.signals) {
    if (out.length >= MAX_FACTS) break
    if (!isRecord(e)) continue
    const signal = cleanText(e.signal, 64)
    if (NOT_DRIFT.has(signal)) continue
    const kind = (DRIFT_KINDS as readonly string[]).includes(signal) ? (signal as DriftKind) : null
    const w = e.window
    if (kind === null && !isRecord(w)) continue
    out.push({
      kind,
      signal,
      severity: cleanText(e.severity, 16),
      table: cleanText(e.table, MAX_NAME_CHARS),
      topic: cleanText(e.topic, MAX_NAME_CHARS),
      from: isRecord(w) ? time(w.from) : '',
      to: isRecord(w) ? time(w.to) : '',
      source: count(e.source),
      sink: count(e.sink),
      missing: count(e.missing),
      mismatched: count(e.mismatched),
      sample: count(e.sample),
      detail: cleanText(e.detail, MAX_DETAIL_CHARS),
    })
  }
  return out
}

const KIND_LABEL: Record<DriftKind, string> = {
  count_drift: 'count drift',
  row_drift: 'row drift',
  dup_keys: 'duplicate keys',
  order_skew: 'order skew',
}

/** "count drift"; an unknown kind reads with spaces ("null_drift" → "null drift"), a missing one "drift". */
export function driftLabel(f: DriftFact): string {
  if (f.kind) return KIND_LABEL[f.kind]
  return f.signal ? f.signal.replace(/_/g, ' ') : 'drift'
}

const NUM = new Intl.NumberFormat('en-US')

function n(v: number): string {
  return NUM.format(v)
}

function plural(v: number, one: string, many: string): string {
  return `${n(v)} ${v === 1 ? one : many}`
}

/** "2.1", "<0.1", "100": a share of a whole, one decimal at most. */
export function percent(part: number, whole: number): string {
  const p = (part / whole) * 100
  if (p > 0 && p < 0.1) return '<0.1'
  return p.toFixed(1).replace(/\.0$/, '')
}

/** "1 h" for the window, '' when it has no valid ends. */
export function windowSpan(f: DriftFact): string {
  return f.from && f.to ? durationLabel(f.from, f.to) : ''
}

// UTC clock for the window, so every reader sees the same ends: "10:00–11:00 UTC", or with dates across a day.
function utcClock(iso: string): { day: string; clock: string } {
  const s = new Date(Date.parse(iso)).toISOString()
  return { day: s.slice(0, 10), clock: s.slice(11, 16) }
}

/** "10:00–11:00 UTC"; "2026-10-21 23:00–2026-10-22 00:00 UTC" across a day; '' without a valid window. */
export function windowClock(f: DriftFact): string {
  if (!f.from || !f.to) return ''
  const a = utcClock(f.from)
  const b = utcClock(f.to)
  return a.day === b.day ? `${a.clock}–${b.clock} UTC` : `${a.day} ${a.clock}–${b.day} ${b.clock} UTC`
}

/** What the signal measured, without the window: "2.1 % rows missing", "3 duplicate events on 2 keys". */
export function driftMeasure(f: DriftFact): string {
  switch (f.kind) {
    case 'count_drift': {
      const { source, sink, missing } = f
      if (source && missing) return `${percent(missing, source)} % rows missing`
      if (source && sink !== null && sink > source) return `${percent(sink - source, source)} % extra rows on the sink`
      return 'row counts differ'
    }
    case 'row_drift': {
      const parts: string[] = []
      const of = f.sample ? ` of ${n(f.sample)}` : ''
      if (f.missing) parts.push(`${n(f.missing)}${of} sampled rows missing`)
      if (f.mismatched) parts.push(`${n(f.mismatched)}${of} sampled rows differ`)
      return parts.length ? parts.join(', ') : 'sampled rows differ'
    }
    case 'dup_keys': {
      const head = f.missing ? plural(f.missing, 'duplicate event', 'duplicate events') : 'duplicate events'
      return f.sample ? `${head} on ${plural(f.sample, 'key', 'keys')}` : head
    }
    case 'order_skew': {
      const head = f.mismatched ? plural(f.mismatched, 'out-of-order event', 'out-of-order events') : 'events out of order'
      return f.sample ? `${head} on ${plural(f.sample, 'key', 'keys')}` : head
    }
    default:
      return driftLabel(f)
  }
}

/** The lane's line: "2.1 % rows missing over 1 h". */
export function driftPhrase(f: DriftFact): string {
  const span = windowSpan(f)
  const measure = driftMeasure(f)
  return span ? `${measure} over ${span}` : measure
}

function isTableKind(f: DriftFact): boolean {
  return f.kind === 'count_drift' || f.kind === 'row_drift' || (f.kind === null && f.table !== '')
}

function isTopicKind(f: DriftFact): boolean {
  return f.kind === 'dup_keys' || f.kind === 'order_skew' || (f.kind === null && f.table === '' && f.topic !== '')
}

// The sink node of a table: the node named `table` when it is a sink, else the first sink downstream of it along
// data edges. With no table named, the pipeline's only sink. null when it cannot be told: a fact never lands on a
// node it might not belong to.
function sinkOf(topology: Topology, table: string): TopologyNode | null {
  const sinks = topology.nodes.filter((n) => n.kind === 'sink_table')
  if (table === '') return sinks.length === 1 ? sinks[0] : null
  const start = topology.nodes.find((n) => n.name === table && (n.kind === 'source_table' || n.kind === 'sink_table'))
  if (!start) return null
  if (start.kind === 'sink_table') return start
  const byId = new Map(topology.nodes.map((n) => [n.id, n]))
  const seen = new Set([start.id])
  let frontier = [start.id]
  while (frontier.length > 0) {
    const next: string[] = []
    for (const e of topology.edges ?? []) {
      if (e.kind === 'depends_on' || !frontier.includes(e.from) || seen.has(e.to)) continue
      const node = byId.get(e.to)
      if (!node) continue
      if (node.kind === 'sink_table') return node
      seen.add(e.to)
      next.push(e.to)
    }
    frontier = next
  }
  return null
}

function topicNode(topology: Topology, topic: string): TopologyNode | null {
  const topics = topology.nodes.filter((n) => n.kind === 'topic')
  if (topic === '') return topics.length === 1 ? topics[0] : null
  return topics.find((n) => n.name === topic) ?? null
}

/**
 * Node id → the drift facts drawn on it: count_drift and row_drift on the sink node of `table`, dup_keys and
 * order_skew on the node of `topic`, an unknown kind by whichever name it carries. Facts with no node stay in the
 * room header only.
 */
export function placeDrift(topology: Topology, facts: readonly DriftFact[]): Map<string, DriftFact[]> {
  const out = new Map<string, DriftFact[]>()
  for (const f of facts) {
    const node = isTableKind(f) ? sinkOf(topology, f.table) : isTopicKind(f) ? topicNode(topology, f.topic) : null
    if (!node) continue
    out.set(node.id, [...(out.get(node.id) ?? []), f])
  }
  return out
}
