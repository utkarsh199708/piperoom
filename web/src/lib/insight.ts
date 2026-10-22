// Formatting for the cost chip and the first-insight line (F11), and the pipeline cost view (X6). Numbers only.
import type { AgentStats, AgentStatsInfra } from '../api/types'
import type { InsightState } from '../state/types'

/** "12s", "2m 10s", "1 h 2 min", "6 d 2 h": whole seconds, no rounding up. */
export function elapsedLabel(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000))
  const d = Math.floor(total / 86400)
  const h = Math.floor((total % 86400) / 3600)
  const m = Math.floor((total % 3600) / 60)
  const s = total % 60
  // Same words as durationLabel past an hour ("1 h 2 min", "6 d 2 h"); a stopwatch below it.
  if (d > 0) return h ? `${d} d ${h} h` : `${d} d`
  if (h > 0) return m ? `${h} h ${m} min` : `${h} h`
  if (m > 0) return `${m}m ${s}s`
  return `${s}s`
}

/** "812k tokens", "1.2M tokens", "640 tokens". */
export function tokensLabel(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1).replace(/\.0$/, '')}M tokens`
  if (n >= 1000) return `${Math.round(n / 1000)}k tokens`
  return `${n} tokens`
}

function usd(v: number): string {
  return v > 0 && v < 0.01 ? '<$0.01' : `$${v.toFixed(2)}`
}

/**
 * The cost part of the chip: "$0.42", "≥ $0.42" (some calls unpriced: a lower bound), or the
 * token count when nothing is priced. An unknown cost is never shown as $0. "" before any call.
 */
export function costLabel(u: InsightState['usage']): string {
  if (u.calls === 0) return ''
  if (u.cost_usd === null) return tokensLabel(u.tokens)
  return u.unpriced_calls > 0 ? `≥ ${usd(u.cost_usd)}` : usd(u.cost_usd)
}

/** "$0.42 · 11 steps · 2m 10s"; the cost part is left out before the first model call. */
export function chipText(i: InsightState, durationMs: number): string {
  const parts = [
    costLabel(i.usage),
    `${i.steps} ${i.steps === 1 ? 'step' : 'steps'}`,
    elapsedLabel(durationMs),
  ].filter(Boolean)
  return parts.join(' · ')
}

/** The chip tooltip: calls, tokens, models, first hypothesis, tool errors. */
export function chipTooltip(i: InsightState): string {
  const u = i.usage
  const parts: string[] = []
  if (u.calls > 0) {
    parts.push(`${u.calls} model ${u.calls === 1 ? 'call' : 'calls'}`)
    let tok = `${u.tokens.toLocaleString('en-US')} tokens`
    if (u.cache_read_tokens > 0) tok += ` (${Math.round(u.cache_read_tokens / 1000)}k cache reads)`
    parts.push(tok)
    const models = Object.entries(u.models).map(([m, n]) => `${m} ×${n}`)
    if (models.length > 0) parts.push(models.join(', '))
  }
  if (i.time_to_first_hypothesis_s >= 0) parts.push(`first hypothesis in ${i.time_to_first_hypothesis_s} s`)
  if (i.tool_errors > 0) parts.push(`${i.tool_errors} tool ${i.tool_errors === 1 ? 'error' : 'errors'}`)
  return parts.length > 0 ? parts.join(' · ') : 'No model calls yet'
}

// The agent's own record at this confidence on this pipeline (F11). Small-N honesty: a rate is
// only worded when the server says there is enough data; a partial grade is not "right".
export function recordText(stats: AgentStats, confidence: string): string | null {
  if (stats.graded === 0) return null
  const pipeline = stats.pipeline_id
  if (!stats.enough_data) {
    return `${stats.graded} labelled ${stats.graded === 1 ? 'room' : 'rooms'} on ${pipeline} · not enough data yet`
  }
  const b = stats.by_confidence.find((x) => x.confidence === confidence && x.runs > 0)
  if (!b) return `no record at ${confidence} yet on ${pipeline}`
  return `right ${b.correct} of ${b.runs} times at ${confidence} on ${pipeline} (${stats.window_days} d)`
}

// The model side of the pipeline cost view (X6): the median priced room. An unpriced record is never $0.
export function modelCostText(stats: AgentStats): string {
  const rooms = `${stats.rooms} closed ${stats.rooms === 1 ? 'room' : 'rooms'}`
  if (stats.rooms === 0) return `no closed rooms in ${stats.window_days} d`
  if (!stats.cost_available || stats.median_cost_usd < 0) return `${rooms} · not priced`
  return `median ${usd(stats.median_cost_usd)} per room · ${rooms} · ${stats.window_days} d`
}

/** "30 GB-days", "1.25 GB-days", "<0.01 GB-days": decimal GB (10⁹ bytes) held per day. */
export function gbDaysLabel(n: number): string {
  if (n > 0 && n < 0.01) return '<0.01 GB-days'
  return `${n.toLocaleString('en-US', { maximumFractionDigits: n < 10 ? 2 : 1 })} GB-days`
}

/** "45 s", "12 min", "1 h 30 min", "2 d 3 h": an age in whole seconds. */
export function unreadAgeLabel(s: number): string {
  if (s >= 86400) return elapsedLabel(s * 1000)
  if (s < 60) return `${Math.max(0, Math.round(s))} s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m} min`
  return m % 60 ? `${Math.floor(m / 60)} h ${m % 60} min` : `${m / 60} h`
}

export interface InfraFact {
  label: string
  /** null: the watcher has no sample of this metric in the window. */
  value: string | null
  hint: string
}

// The infrastructure side of the pipeline cost view (X6): units only, the customer prices them.
export function infraFacts(infra: AgentStatsInfra): InfraFact[] {
  const n = (v: number | null, f: (x: number) => string) => (v === null || v === undefined ? null : f(v))
  return [
    {
      label: 'WAL retained',
      value: n(infra.wal_retained_gb_days, gbDaysLabel),
      hint: 'WAL the replication slot held, summed over the window',
    },
    {
      label: 'Dead-letter queue',
      value: n(infra.dlq_records, (x) => `${x.toLocaleString('en-US')} ${x === 1 ? 'record' : 'records'}`),
      hint: 'Records in the dead-letter topics at the newest sample',
    },
    {
      label: 'Oldest unread',
      value: n(infra.oldest_unread_age_s, unreadAgeLabel),
      hint: 'Age of the oldest record not yet consumed, at the newest sample',
    },
  ]
}
