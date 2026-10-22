// X3 trends on the lane: which node shows which watcher metric, the sparkline geometry and the forecast
// ETA wording. Pure functions only; the hook (useTrends) fetches and the components draw.
import type { ForecastSummary, TopologyNode, TrendPoint, WatchMetric } from '../api/types'
import { WATCH_METRICS } from '../api/types'
import { humanBytes } from './evidenceShapes'
import { utcLabel } from './time'

/** clients.MaxSamplesPerSeries: one sample per 15 min for 7 days. A longer answer is cut to its newest points. */
export const MAX_TREND_POINTS = 672

/** No badge for an ETA further out than this: a week away is not news on a lane. */
const BADGE_HORIZON_MS = 7 * 24 * 3_600_000

interface MetricInfo {
  /** What the series measures, in words (sparkline label, chart title). */
  label: string
  /** The lane node kind the series belongs to; the first node of that kind carries it. */
  kind: TopologyNode['kind']
  unit: 'bytes' | 'records' | 'seconds'
  /**
   * What reaching the limit means. A slot or database that reaches its limit fills the disk; a topic whose
   * oldest unread record reaches retention.ms loses data (the broker deletes it unread).
   */
  outcome: 'fills' | 'data_loss'
}

const METRICS: Record<WatchMetric, MetricInfo> = {
  slot_wal_bytes: { label: 'WAL retained by the slot', kind: 'source_table', unit: 'bytes', outcome: 'fills' },
  dlq_records: { label: 'Dead-letter records', kind: 'dlq_topic', unit: 'records', outcome: 'fills' },
  group_lag: { label: 'Consumer lag', kind: 'consumer_group', unit: 'records', outcome: 'fills' },
  db_size_bytes: { label: 'Source database size', kind: 'source_table', unit: 'bytes', outcome: 'fills' },
  topic_oldest_unread_age: { label: 'Oldest unread record', kind: 'topic', unit: 'seconds', outcome: 'data_loss' },
}

export function isWatchMetric(m: unknown): m is WatchMetric {
  return typeof m === 'string' && (WATCH_METRICS as readonly string[]).includes(m)
}

export function metricLabel(metric: WatchMetric): string {
  return METRICS[metric].label
}

/** A metric value in its unit: "12 GiB", "1,204 records", "5 h". */
export function formatMetricValue(metric: WatchMetric, v: number): string {
  if (!Number.isFinite(v)) return '–'
  switch (METRICS[metric].unit) {
    case 'bytes':
      return humanBytes(v)
    case 'seconds':
      return durationLabel(v * 1000) ?? '0 min'
    default:
      return `${Math.round(v).toLocaleString('en-US')} records`
  }
}

/**
 * The trends answer as points the chart can draw: a non-array body gives [], a point without a finite value or
 * a parseable time is dropped (a hostile or broken answer never reaches the SVG), and at most MAX_TREND_POINTS of
 * the newest are kept.
 */
export function cleanSeries(raw: unknown): TrendPoint[] {
  if (!Array.isArray(raw)) return []
  const out: TrendPoint[] = []
  for (const p of raw) {
    if (typeof p !== 'object' || p === null) continue
    const { at, value } = p as { at?: unknown; value?: unknown }
    if (typeof at !== 'string' || typeof value !== 'number' || !Number.isFinite(value)) continue
    if (Number.isNaN(Date.parse(at))) continue
    out.push({ at, value })
  }
  return out.length > MAX_TREND_POINTS ? out.slice(out.length - MAX_TREND_POINTS) : out
}

/** The box mapping of a series: its points scaled into w×h (y grows down) and the y of any value. */
function scale(points: TrendPoint[], w: number, h: number, limit?: number) {
  const ts = points.map((p) => Date.parse(p.at))
  const t0 = Math.min(...ts)
  const t1 = Math.max(...ts)
  const vs = points.map((p) => p.value)
  let lo = Math.min(...vs)
  let hi = Math.max(...vs)
  if (limit !== undefined && Number.isFinite(limit)) {
    lo = Math.min(lo, limit)
    hi = Math.max(hi, limit)
  }
  // A 1px inset keeps a 2px stroke inside the box at the extremes.
  const pad = 1
  const x = (i: number) => {
    if (t1 !== t0) return pad + ((w - 2 * pad) * (ts[i] - t0)) / (t1 - t0)
    return points.length === 1 ? w / 2 : pad + ((w - 2 * pad) * i) / (points.length - 1)
  }
  const y = (v: number) => round(hi === lo ? h / 2 : pad + (h - 2 * pad) * (1 - (v - lo) / (hi - lo)))
  return { xy: points.map((p, i): [number, number] => [round(x(i)), y(p.value)]), y }
}

/**
 * The series scaled into a w×h box (y grows down), x by sample time so a gap in sampling shows as a long
 * segment. `limit` widens the y range so the line can be read against it. An empty series gives [].
 */
export function sparklineXY(points: TrendPoint[], w: number, h: number, limit?: number): [number, number][] {
  return points.length === 0 ? [] : scale(points, w, h, limit).xy
}

/** The y of `v` in the same box as sparklineXY (where the chart draws the limit line). */
export function sparklineY(points: TrendPoint[], h: number, v: number, limit?: number): number {
  return points.length === 0 ? h / 2 : scale(points, 1, h, limit).y(v)
}

/** SVG path data for the series: "M x y L x y …", one command per point; '' for an empty series. */
export function sparklinePath(points: TrendPoint[], w: number, h: number, limit?: number): string {
  return sparklineXY(points, w, h, limit)
    .map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x} ${y}`)
    .join(' ')
}

function round(n: number): number {
  return Math.round(n * 100) / 100
}

/**
 * A time span for a badge: "30 min" under an hour, "18 h" under two days, "3 d" up to a week; null beyond a
 * week (no badge). Rounded down, so the badge never promises more time than the fit gives.
 */
export function durationLabel(ms: number): string | null {
  if (!Number.isFinite(ms) || ms > BADGE_HORIZON_MS) return null
  const min = ms / 60_000
  if (min < 60) return `${Math.max(1, Math.floor(min))} min`
  const h = min / 60
  if (h < 48) return `${Math.floor(h)} h`
  return `${Math.floor(h / 24)} d`
}

export interface EtaBadge {
  /** What the badge shows: "fills in 3 d", "data loss in 18 h". */
  short: string
  /** The accessible text and hover: the metric, the object, the ETA as a UTC time. */
  long: string
  /** The ETA has passed: the limit is reached (or the fit says so). */
  due: boolean
}

/** The badge for a forecast at `now`, or null when there is none to show (unknown metric, bad time, > 7 d). */
export function etaBadge(forecast: Pick<ForecastSummary, 'metric' | 'eta' | 'object'>, now: number): EtaBadge | null {
  if (!isWatchMetric(forecast.metric)) return null
  const eta = Date.parse(forecast.eta)
  if (Number.isNaN(eta)) return null
  const info = METRICS[forecast.metric]
  const what = forecast.object ? `${info.label} (${forecast.object})` : info.label
  const when = utcLabel(forecast.eta)
  const left = eta - now
  if (left <= 0) {
    const short = info.outcome === 'data_loss' ? 'data loss now' : 'limit reached'
    return { short, long: `Forecast: ${what} reached its limit at ${when}`, due: true }
  }
  const span = durationLabel(left)
  if (span === null) return null
  const short = info.outcome === 'data_loss' ? `data loss in ${span}` : `fills in ${span}`
  const consequence = info.outcome === 'data_loss' ? 'unread records are deleted' : 'it reaches its limit'
  return { short, long: `Forecast: ${what}: ${consequence} in about ${span}, at ${when}`, due: false }
}

/** What one lane node draws: a metric's series (may be empty) and the forecast that falls on it, if any. */
export interface NodeTrend {
  metric: WatchMetric
  points: TrendPoint[]
  forecast?: ForecastSummary
}

/**
 * Which node carries which metric. Each metric goes to the first node of its kind; a forecast goes to the node
 * of that kind named like its `object` when there is one. A node carries one metric: the forecast's first, then
 * the metrics in WATCH_METRICS order (so a source shows slot WAL before database size). Metrics with no
 * samples and no forecast draw nothing.
 */
export function laneTrends(
  nodes: TopologyNode[],
  series: Partial<Record<WatchMetric, TrendPoint[]>>,
  forecast?: ForecastSummary | null,
): Map<string, NodeTrend> {
  const out = new Map<string, NodeTrend>()
  const firstOf = (kind: TopologyNode['kind']) => nodes.find((n) => n.kind === kind)
  if (forecast && isWatchMetric(forecast.metric)) {
    const kind = METRICS[forecast.metric].kind
    const node = nodes.find((n) => n.kind === kind && forecast.object && n.name === forecast.object) ?? firstOf(kind)
    if (node) out.set(node.id, { metric: forecast.metric, points: series[forecast.metric] ?? [], forecast })
  }
  for (const metric of WATCH_METRICS) {
    const points = series[metric]
    if (!points || points.length === 0) continue
    if (forecast?.metric === metric) continue
    const node = firstOf(METRICS[metric].kind)
    if (node && !out.has(node.id)) out.set(node.id, { metric, points })
  }
  return out
}

/** The accessible name of a sparkline of `metric`: what it measures and its newest value. */
export function sparklineLabel(metric: WatchMetric, points: TrendPoint[]): string {
  const last = points[points.length - 1]
  const name = metricLabel(metric)
  return last ? `${name}, last 24 h, now ${formatMetricValue(metric, last.value)}` : `${name}, last 24 h`
}

/**
 * The snapshot's `forecast` as the UI uses it, or undefined when it is absent or malformed (a metric or eta that
 * is not a string, a number that is not finite). The text fields are names only; they render as text.
 */
export function cleanForecast(raw: unknown): ForecastSummary | undefined {
  if (typeof raw !== 'object' || raw === null) return undefined
  const f = raw as Record<string, unknown>
  const num = (v: unknown) => typeof v === 'number' && Number.isFinite(v)
  if (typeof f.metric !== 'string' || typeof f.eta !== 'string') return undefined
  if (!num(f.current) || !num(f.limit) || !num(f.slope_per_hour) || !num(f.r2)) return undefined
  return {
    metric: f.metric,
    ...(typeof f.object === 'string' && f.object !== '' ? { object: f.object } : {}),
    current: f.current as number,
    limit: f.limit as number,
    eta: f.eta,
    slope_per_hour: f.slope_per_hour as number,
    r2: f.r2 as number,
    ...(typeof f.at === 'string' ? { at: f.at } : {}),
  }
}
