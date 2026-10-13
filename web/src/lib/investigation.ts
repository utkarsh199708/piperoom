import type { StepState } from '../state/types'

/** "412 ms", "3.2 s", "2m 10s". */
export function formatDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`
  if (ms < 60_000) return `${Math.round(ms / 100) / 10} s`
  const whole = Math.round(ms / 1000)
  const m = Math.floor(whole / 60)
  const s = whole % 60
  return `${m}m ${String(s).padStart(2, '0')}s`
}

/** Sum of took_ms over a step's tool calls; null when no call reported a time. */
export function stepTookMs(step: StepState): number | null {
  let total = 0
  let seen = false
  for (const t of step.tool_calls) {
    if (typeof t.took_ms === 'number') {
      total += t.took_ms
      seen = true
    }
  }
  return seen ? total : null
}

/** "5 steps · 2m 10s" for the Investigation panel title. */
export function investigationSummary(steps: StepState[]): string {
  const n = `${steps.length} ${steps.length === 1 ? 'step' : 'steps'}`
  let total = 0
  let seen = false
  for (const s of steps) {
    const t = stepTookMs(s)
    if (t !== null) {
      total += t
      seen = true
    }
  }
  return seen ? `${n} · ${formatDuration(total)}` : n
}

/** "lease 24m" from lease_until; null when there is no lease or it has run out. */
export function leaseLabel(leaseUntil: string | undefined, now = Date.now()): string | null {
  if (!leaseUntil) return null
  const t = Date.parse(leaseUntil)
  if (Number.isNaN(t)) return null
  const diff = t - now
  if (diff <= 0) return null
  const minutes = Math.max(1, Math.round(diff / 60_000))
  return `lease ${minutes}m`
}
