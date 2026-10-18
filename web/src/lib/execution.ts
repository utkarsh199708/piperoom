import type { ProposalState } from '../state/types'
import { waitingForWriteMode } from './writeMode'

/** blocked (P1a): approved while write mode is off, so no step will run. */
export type ExecutionPhase = 'pending' | 'blocked' | 'running' | 'done' | 'failed' | 'unknown'

export interface ExecutionInfo {
  phase: ExecutionPhase
  /** 1-based step now running, or the step that stopped the run; 0 before anything ran. */
  step: number
  of: number
}

/**
 * Where a proposal's execution stands, from its steps and the action.executed events so far.
 * Failure and unknown outcome beat progress: the executor stops at the first of either.
 */
export function executionPhase(p: ProposalState): ExecutionInfo {
  const of = p.steps.length
  const unknown = p.executed.find((x) => x.outcome_unknown)
  if (unknown) return { phase: 'unknown', step: unknown.step_index + 1, of }
  const failed = p.executed.find((x) => !x.ok)
  if (failed) return { phase: 'failed', step: failed.step_index + 1, of }
  const okSteps = new Set(p.executed.filter((x) => x.ok).map((x) => x.step_index))
  if (of > 0 && okSteps.size >= of) return { phase: 'done', step: of, of }
  if (waitingForWriteMode(p)) return { phase: 'blocked', step: 0, of }
  if (p.status === 'approved') return { phase: 'running', step: Math.min(okSteps.size + 1, of), of }
  return { phase: 'pending', step: 0, of }
}

/** What a verdict says. `recovered: false` covers three of them. */
export type VerdictKind = 'recovered' | 'not-recovered' | 'not-verified' | 'inconclusive'

/**
 * The payload has one boolean for three outcomes: the agent also sends `recovered: false` when it
 * could not check at all ("not verified: …", e.g. a step's outcome is unknown) or the checks were
 * inconclusive. Only the verdict's first words tell them apart (internal/agent/verify.go).
 */
export function verdictKind(v: { recovered: boolean; verdict?: string }): VerdictKind {
  if (v.recovered) return 'recovered'
  const head = (v.verdict ?? '').trimStart().toLowerCase()
  if (head.startsWith('not verified')) return 'not-verified'
  if (head.startsWith('inconclusive')) return 'inconclusive'
  return 'not-recovered'
}

/** The verdict in two or three words: "recovered", "not recovered", "not verified", "inconclusive". */
export function verdictWords(kind: VerdictKind): string {
  return kind.replace('-', ' ')
}
