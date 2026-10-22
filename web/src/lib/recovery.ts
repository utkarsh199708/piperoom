import type { AgentRecoveryCheckPayload, AgentVerificationPayload, RecoveryOutcome, RecoveryUnit } from '../api/types'

// X2d: the recovery card's wording and number handling. Everything here reads counts off the wire,
// which is not trusted to be a number: a value that is not a whole, non-negative, safe integer is
// "no count" (never a guess), and a unit outside the contract's two words shows no unit at all.

/** A count the card may print: `0` is a real count, anything that is not a whole number is not. */
export function asCount(v: unknown): number | undefined {
  return typeof v === 'number' && Number.isSafeInteger(v) && v >= 0 ? v : undefined
}

function asUnit(u: unknown): RecoveryUnit | undefined {
  return u === 'rows' || u === 'records' ? u : undefined
}

/** "1 row", "412 rows", "0 records"; a unit the contract does not name is left out ("412"). */
export function countWithUnit(n: number, unit: unknown): string {
  const u = asUnit(unit)
  const num = n.toLocaleString('en-US')
  if (!u) return num
  return `${num} ${n === 1 ? u.slice(0, -1) : u}`
}

/** The preview's "Will re-emit 412 rows"; undefined when the step carries no usable count. */
export function previewLine(count: unknown, unit: unknown): string | undefined {
  const n = asCount(count)
  return n === undefined ? undefined : `Will re-emit ${countWithUnit(n, unit)}`
}

const OUTCOMES: readonly RecoveryOutcome[] = ['recovered', 'partial', 'not_recovered', 'unverifiable']

/** The verdict's outcome, or undefined for a fix (no outcome) or a word the contract does not define. */
export function recoveryOutcome(v: Pick<AgentVerificationPayload, 'outcome'>): RecoveryOutcome | undefined {
  return OUTCOMES.find((o) => o === v.outcome)
}

export type RecoveryTone = 'success' | 'warning' | 'danger' | 'muted'

export interface OutcomeView {
  word: string
  tone: RecoveryTone
}

/** The heading word and colour of each outcome. partial is a warning, not_recovered is danger. */
export function outcomeView(o: RecoveryOutcome): OutcomeView {
  switch (o) {
    case 'recovered':
      return { word: 'Recovered', tone: 'success' }
    case 'partial':
      return { word: 'Partially recovered', tone: 'warning' }
    case 'not_recovered':
      return { word: 'Not recovered', tone: 'danger' }
    case 'unverifiable':
      return { word: 'Could not verify', tone: 'muted' }
  }
}

/**
 * The heading of a counted verdict. A count that reached 0 while a layer check failed is not a recovery: the
 * agent's verdict says "NOT recovered" (`recovered: false` next to outcome "recovered"), so the card does too.
 */
export function verdictView(o: RecoveryOutcome, recovered: boolean): OutcomeView {
  if (o === 'recovered' && !recovered) return { word: 'Not recovered: a check failed', tone: 'danger' }
  return outcomeView(o)
}

/** "412 missing → 0 rows": the gap before, then what is still missing. Undefined without a `before`. */
export function gapLine(before: unknown, after: unknown, unit: unknown): string | undefined {
  const b = asCount(before)
  if (b === undefined) return undefined
  const a = asCount(after)
  return `${b.toLocaleString('en-US')} missing → ${a === undefined ? '?' : countWithUnit(a, unit)}`
}

/** "3 min", "45 s": how long a recovery took. Empty for a missing, invalid or negative span. */
export function tookLabel(fromIso: string | undefined, toIso: string | undefined): string {
  if (!fromIso || !toIso) return ''
  const a = Date.parse(fromIso)
  const b = Date.parse(toIso)
  if (Number.isNaN(a) || Number.isNaN(b) || b < a) return ''
  const secs = Math.round((b - a) / 1000)
  if (secs < 60) return `${secs} s`
  return `${Math.round(secs / 60)} min`
}

/** "checking (2 of 8)"; the poll is clamped into 1..max so a bad payload never prints "9 of 8". */
export function checkingLabel(c: Pick<AgentRecoveryCheckPayload, 'poll' | 'max_polls'>): string {
  const max = asCount(c.max_polls)
  const poll = asCount(c.poll)
  if (poll === undefined || max === undefined || max < 1) return 'checking'
  return `checking (${Math.min(Math.max(poll, 1), max)} of ${max})`
}
