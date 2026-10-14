import { describe, expect, it } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { ClampedText } from './ClampedText'
import { ActivityFeed } from '../ActivityFeed'
import { ProposalsPanel } from '../ProposalsPanel'
import { emptyRoomState } from '../../state/types'
import type { RoomActions } from '../../hooks/useRoom'

const noop = () => undefined
const actions = {
  comment: noop,
  steer: () => Promise.resolve(true),
  sendSuggestion: noop,
  requestControl: noop,
  grantControl: noop,
  close: () => Promise.resolve({ ok: true }),
  approve: () => Promise.resolve({ ok: true }),
  reject: noop,
  handoff: noop,
  acceptHandoff: noop,
  trace: noop,
} satisfies RoomActions

describe('ClampedText', () => {
  it('short text has no toggle', () => {
    render(<ClampedText text="short" />)
    expect(screen.getByText('short')).toBeInTheDocument()
    expect(screen.queryByRole('button')).not.toBeInTheDocument()
  })

  it('long text starts folded and opens with Show more', () => {
    render(<ClampedText text={'A'.repeat(4000)} />)
    const text = screen.getByText('A'.repeat(4000))
    expect(text).toHaveAttribute('data-clamped', 'true')
    const toggle = screen.getByRole('button', { name: 'Show more' })
    expect(toggle).toHaveAttribute('aria-expanded', 'false')
    fireEvent.click(toggle)
    expect(text).not.toHaveAttribute('data-clamped')
    expect(screen.getByRole('button', { name: 'Show less' })).toHaveAttribute('aria-expanded', 'true')
  })

  it('folds a long activity line', () => {
    render(
      <ActivityFeed
        activity={[{ seq: 1, at: '2026-10-14T10:00:00Z', actor: { kind: 'agent', id: 'agent', name: 'Agent' }, text: 'B'.repeat(3000) }]}
      />,
    )
    expect(screen.getByRole('button', { name: 'Show more' })).toBeInTheDocument()
  })

  it('folds long executor output, and two reports of one step both render (unique keys)', () => {
    const proposal = {
      proposal_id: 'p1',
      title: 'Fix',
      steps: [],
      risk: 'low',
      rollback: '',
      required_layers: [],
      approvals: {},
      decisions: {},
      params_hash: 'h',
      status: 'approved' as const,
      executed: [
        { proposal_id: 'p1', step_index: 0, ok: false, output: 'first try' },
        { proposal_id: 'p1', step_index: 0, ok: true, output: 'C'.repeat(2000) },
      ],
    }
    render(
      <ProposalsPanel
        state={{ ...emptyRoomState('r1'), proposals: [proposal] }}
        user={{ id: 'u', name: 'U', layers: [] }}
        actions={actions}
      />,
    )
    expect(screen.getByText('first try')).toBeInTheDocument()
    expect(screen.getAllByText('step 1')).toHaveLength(2)
    expect(screen.getByRole('button', { name: 'Show more' })).toBeInTheDocument()
  })
})
