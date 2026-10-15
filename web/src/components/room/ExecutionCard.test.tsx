import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import type { ProposalState } from '../../state/types'
import { ExecutionCard } from './ExecutionCard'

function proposal(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p_1',
    title: 'fix',
    steps: [
      { action: 'schema_set_compatibility', layer: 'schema-registry', params: {}, description: '' },
      { action: 'connect_restart_task', layer: 'kafka-connect', params: {}, description: '' },
    ],
    risk: 'low',
    rollback: 'undo',
    required_layers: ['schema-registry'],
    approvals: {},
    decisions: {},
    params_hash: 'hash-a',
    status: 'approved',
    executed: [],
    ...over,
  }
}
const ran = (i: number, ok = true, extra = {}) => ({ proposal_id: 'p_1', step_index: i, ok, output: `out ${i}`, ...extra })

describe('ExecutionCard', () => {
  it('shows nothing before approval', () => {
    const { container } = render(<ExecutionCard proposal={proposal({ status: 'open' })} />)
    expect(container).toBeEmptyDOMElement()
  })
  it('names the running step in a polite live header', () => {
    render(<ExecutionCard proposal={proposal({ executed: [ran(0)] })} />)
    const header = screen.getByText('Running · step 2 of 2 · connect_restart_task')
    expect(header).toHaveAttribute('aria-live', 'polite')
    expect(screen.getByLabelText('running')).toBeInTheDocument()
    expect(screen.getByText('out 0')).toBeInTheDocument()
  })
  it('says verifying after the last step until a verdict arrives', () => {
    render(<ExecutionCard proposal={proposal({ executed: [ran(0), ran(1)] })} />)
    expect(screen.getByText('Ran 2 of 2 · verifying…')).toBeInTheDocument()
  })
  it('stops at the failed step', () => {
    render(<ExecutionCard proposal={proposal({ executed: [ran(0), ran(1, false)] })} />)
    expect(screen.getByText('Stopped at step 2 of 2')).toBeInTheDocument()
    expect(screen.getByLabelText('failed')).toBeInTheDocument()
  })
  it('flags an unknown outcome in amber text', () => {
    render(<ExecutionCard proposal={proposal({ executed: [ran(0, false, { outcome_unknown: true, output: '' })] })} />)
    expect(screen.getByText('Result unknown — check the target before retrying')).toBeInTheDocument()
  })
  it('keeps the exec-hash test ids and the hash-differs badge', () => {
    render(<ExecutionCard proposal={proposal({ executed: [ran(0, true, { params_hash: 'hash-b' })] })} />)
    expect(screen.getByTestId('exec-hash-0')).toBeInTheDocument()
    expect(screen.getByText('hash differs from proposal')).toBeInTheDocument()
  })
})

describe('ExecutionCard roll back', () => {
  const rb = { asDriver: true, onSteer: async () => true }
  it('is offered once the run is done, failed or unknown, not while running', () => {
    const running = render(<ExecutionCard proposal={proposal({ executed: [ran(0)] })} rollback={rb} />)
    expect(screen.queryByRole('button', { name: 'Roll back' })).not.toBeInTheDocument()
    running.unmount()
    const done = render(<ExecutionCard proposal={proposal({ executed: [ran(0), ran(1)] })} rollback={rb} />)
    expect(screen.getByRole('button', { name: 'Roll back' })).toBeInTheDocument()
    done.unmount()
    render(<ExecutionCard proposal={proposal({ executed: [ran(0, false)] })} rollback={rb} />)
    expect(screen.getByRole('button', { name: 'Roll back' })).toBeInTheDocument()
  })
  it('is not offered to a viewer (no rollback prop)', () => {
    render(<ExecutionCard proposal={proposal({ executed: [ran(0), ran(1)] })} />)
    expect(screen.queryByRole('button', { name: /roll back/i })).not.toBeInTheDocument()
  })
})
