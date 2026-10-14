// Pure helpers for shadow mode (docs/design/shadow.md): number formatting, the metrics
// headline and the blind-label form rules. No React here so they are easy to test.
import type { ShadowMetrics, ShadowStatus } from '../api/types'

export const MAX_TERMS = 20
export const MAX_TERM_LEN = 200
export const MAX_NOTES = 2000

/** "n/a" for the server's -1 ("no room has the value"), else s / min / h. */
export function formatSeconds(s: number | null | undefined): string {
  if (s == null || s < 0 || !Number.isFinite(s)) return 'n/a'
  if (s < 60) return `${Math.round(s)} s`
  if (Math.round(s / 60) < 60) return `${Math.round(s / 60)} min`
  return `${(s / 3600).toFixed(1)} h`
}

export function formatTokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1000) return `${Math.round(n / 1000)}k`
  return String(n)
}

/**
 * The line a pilot copies: "14 of 16 matched, median 4 min vs 52 min, 380k tokens".
 * "Matched" is layer_correct of every closed shadow room, ungraded and failed runs
 * included (the server's denominator), so the number is honest and low. Agent time is
 * the final hypothesis, the one the grade judges.
 */
export function metricsHeadline(m: ShadowMetrics): string {
  const parts = [
    `${m.layer_correct} of ${m.rooms} matched`,
    `median ${formatSeconds(m.median_time_to_final_s)} vs ${formatSeconds(m.median_human_mttr_s)}`,
    m.tokens_available ? `${formatTokens(m.total_tokens)} tokens` : 'tokens not counted',
  ]
  return parts.join(', ')
}

export const STATUS_LABEL: Record<ShadowStatus, string> = {
  queued: 'queued',
  running: 'running',
  hypothesised: 'hypothesis',
  no_hypothesis: 'no hypothesis',
  error: 'error',
  budget: 'budget spent',
  interrupted: 'interrupted',
}

/** Label for a run status; a status this UI does not know yet shows as the raw value, never blank. */
export function statusLabel(s: string): string {
  return (STATUS_LABEL as Record<string, string | undefined>)[s] ?? s
}

/** One term per line (commas stay inside a term); blank lines are dropped, terms trimmed. */
export function parseTerms(text: string): string[] {
  return text
    .split('\n')
    .map((t) => t.trim())
    .filter((t) => t !== '')
}

/** A reason the terms cannot be sent, or null. Mirrors the server's 400 rules so the form says it first. */
export function termsProblem(terms: string[]): string | null {
  if (terms.length === 0) return 'Add at least one root-cause term.'
  if (terms.length > MAX_TERMS) return `At most ${MAX_TERMS} terms.`
  if (terms.some((t) => [...t].length > MAX_TERM_LEN)) return `Each term is at most ${MAX_TERM_LEN} characters.`
  return null
}

/** The form's date as RFC 3339 for `since` (closed_at >= start of that day, UTC); '' for no filter. */
export function sinceParam(date: string): string {
  return /^\d{4}-\d{2}-\d{2}$/.test(date) ? `${date}T00:00:00Z` : ''
}
