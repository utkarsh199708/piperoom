import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ProposalsSummary } from './ProposalsSummary'
import type { ProposalState } from '../../state/types'

// KAN-560: room.closed leaves a proposal "open" in the log and in the snapshot's open_proposals, but a
// closed room takes no decision (409 room_closed), so that proposal never ran. The rail must say so,
// not "Waiting for <layer>" with an approval count that can no longer move.

const NOT_RUN = 'Not run · room closed'

function proposal(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p1',
    title: 'Set subject compatibility to NONE, then restart the connector task',
    steps: [],
    risk: 'low',
    rollback: 'set compatibility back to BACKWARD',
    required_layers: ['schema-registry', 'kafka-connect'],
    approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' },
    decisions: {},
    params_hash: 'abc123',
    status: 'open',
    executed: [],
    ...over,
  }
}

const ran = (over: Partial<ProposalState> = {}) =>
  proposal({
    proposal_id: 'p_ran',
    title: 'Restart the connector task',
    status: 'approved',
    steps: [{ action: 'connect_restart_task', layer: 'kafka-connect', params: {}, description: 'restart' }],
    approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' },
    executed: [{ proposal_id: 'p_ran', step_index: 0, ok: true, output: 'ok' }],
    verification: { proposal_id: 'p_ran', checks: [], recovered: true, verdict: 'recovered: all good' },
    ...over,
  })

describe('ProposalsSummary on a closed room', () => {
  it('reads a proposal that never got its approvals as not run: no waiting text, no counter, not open', () => {
    render(<ProposalsSummary proposals={[proposal()]} closed />)
    expect(screen.getByText(NOT_RUN)).toBeInTheDocument()
    expect(screen.queryByText(/waiting for/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/\d+ of \d+/)).not.toBeInTheDocument()
    expect(screen.queryByText(/\bopen\b/i)).not.toBeInTheDocument()
    // The heading counts it, but no longer calls it open.
    expect(screen.getByText('1')).toBeInTheDocument()
    // It stays one button that jumps to the full card.
    expect(screen.getByRole('button')).toHaveTextContent('1. Set subject compatibility to NONE')
  })

  it('keeps no counter when some layers had approved before the room closed', () => {
    render(
      <ProposalsSummary
        proposals={[proposal({ approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' } })]}
        closed
      />,
    )
    expect(screen.getByText(NOT_RUN)).toBeInTheDocument()
    expect(screen.queryByText(/waiting for/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/\d+ of \d+/)).not.toBeInTheDocument()
  })

  it('goes from waiting to not run when the room closes under it', () => {
    const { rerender } = render(<ProposalsSummary proposals={[proposal()]} />)
    expect(screen.getByText('Waiting for schema-registry')).toBeInTheDocument()
    rerender(<ProposalsSummary proposals={[proposal()]} closed />)
    expect(screen.getByText(NOT_RUN)).toBeInTheDocument()
    expect(screen.queryByText(/waiting for/i)).not.toBeInTheDocument()
  })

  it('tells a proposal that ran from one that did not, in the same room', () => {
    render(<ProposalsSummary proposals={[ran(), proposal()]} closed />)
    expect(screen.getByText('Ran · recovered')).toBeInTheDocument()
    expect(screen.getByText('2 of 2')).toBeInTheDocument()
    expect(screen.getByText(NOT_RUN)).toBeInTheDocument()
    expect(screen.getAllByText(NOT_RUN)).toHaveLength(1)
    expect(screen.queryByText(/waiting for/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/\bopen\b/i)).not.toBeInTheDocument()
  })

  // Anything that is not an unrun "open" proposal reads exactly as it does in an open room.
  it.each([
    ['a proposal that ran', ran(), 'Ran · recovered', '2 of 2'],
    [
      'an approved proposal with no steps to run',
      proposal({ status: 'approved', approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' } }),
      'Approved',
      '2 of 2',
    ],
    [
      'a rejected proposal',
      proposal({ status: 'rejected', approvals: { 'schema-registry': 'rejected', 'kafka-connect': 'pending' } }),
      'Rejected',
      undefined,
    ],
  ])('reads %s as it does in an open room', (_name, p, word, count) => {
    const open = render(<ProposalsSummary proposals={[p]} />)
    expect(screen.getByText(word)).toBeInTheDocument()
    open.unmount()
    render(<ProposalsSummary proposals={[p]} closed />)
    expect(screen.getByText(word)).toBeInTheDocument()
    if (count) expect(screen.getByText(count)).toBeInTheDocument()
    expect(screen.queryByText(NOT_RUN)).not.toBeInTheDocument()
  })
})

describe('ProposalsSummary on an open room', () => {
  it.each([
    ['no closed prop', undefined],
    ['closed={false}', false],
  ])('is unchanged with %s: open, waiting for the layer, with the count', (_name, closed) => {
    render(<ProposalsSummary proposals={[proposal()]} closed={closed} />)
    expect(screen.getByText('1 open')).toBeInTheDocument()
    expect(screen.getByText('Waiting for schema-registry')).toBeInTheDocument()
    expect(screen.getByText('0 of 2')).toBeInTheDocument()
    expect(screen.queryByText(/not run/i)).not.toBeInTheDocument()
  })
})
