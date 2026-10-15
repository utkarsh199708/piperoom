import { describe, expect, it } from 'vitest'
import type { ProposalState } from '../state/types'
import { executionPhase } from './execution'

function proposal(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p_1',
    title: 'fix',
    steps: [
      { action: 'a', layer: 'kafka-connect', params: {}, description: '' },
      { action: 'b', layer: 'schema-registry', params: {}, description: '' },
    ],
    risk: 'low',
    rollback: 'undo',
    required_layers: ['kafka-connect'],
    approvals: {},
    decisions: {},
    params_hash: 'h',
    status: 'approved',
    executed: [],
    ...over,
  }
}
const ran = (i: number, ok = true, extra = {}) => ({ proposal_id: 'p_1', step_index: i, ok, output: 'x', ...extra })

describe('executionPhase', () => {
  it('is pending until the proposal is approved', () => {
    expect(executionPhase(proposal({ status: 'open' }))).toEqual({ phase: 'pending', step: 0, of: 2 })
  })
  it('runs step 1, then step 2, while approved and not finished', () => {
    expect(executionPhase(proposal())).toEqual({ phase: 'running', step: 1, of: 2 })
    expect(executionPhase(proposal({ executed: [ran(0)] }))).toEqual({ phase: 'running', step: 2, of: 2 })
  })
  it('is done when every step has an ok result', () => {
    expect(executionPhase(proposal({ executed: [ran(0), ran(1)] }))).toEqual({ phase: 'done', step: 2, of: 2 })
  })
  it('failed beats progress and names the step that stopped the run', () => {
    expect(executionPhase(proposal({ executed: [ran(0), ran(1, false)] }))).toEqual({ phase: 'failed', step: 2, of: 2 })
  })
  it('an unknown outcome is its own phase, not a failure', () => {
    expect(executionPhase(proposal({ executed: [ran(0, false, { outcome_unknown: true })] }))).toEqual({
      phase: 'unknown',
      step: 1,
      of: 2,
    })
  })
})
