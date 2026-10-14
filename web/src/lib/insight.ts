// Formatting for the cost chip and the first-insight line (F11). Numbers only.
import type { AgentStats } from '../api/types'
import type { InsightState } from '../state/types'

/** "12s", "2m 10s", "1 h 02 min": whole seconds, no rounding up. */
export function elapsedLabel(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000))
  const h = Math.floor(total / 3600)
  const m = Math.floor((total % 3600) / 60)
  const s = total % 60
  if (h > 0) return `${h} h ${String(m).padStart(2, '0')} min`
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
