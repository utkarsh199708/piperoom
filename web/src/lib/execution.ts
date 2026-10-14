import type { ProposalState } from '../state/types'

export type ExecutionPhase = 'pending' | 'running' | 'done' | 'failed' | 'unknown'

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
  if (p.status === 'approved') return { phase: 'running', step: Math.min(okSteps.size + 1, of), of }
  return { phase: 'pending', step: 0, of }
}
