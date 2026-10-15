import { afterEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen } from '@testing-library/react'
import type { ProposalState } from '../../state/types'
import type { Topology } from '../../api/types'
import { BlastRadius, PREVIEW_WAIT_MS } from './BlastRadius'

const topology: Topology = {
  pipeline_id: 'orders',
  nodes: [{ id: 'topic:pg.orders', kind: 'topic', name: 'pg.orders', layer: 'kafka', owner: 'a', health: 'ok' }],
  edges: [],
}

function proposal(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p_1',
    title: 'fix',
    steps: [{ action: 'connect_restart_task', layer: 'kafka-connect', params: {}, description: '' }],
    risk: 'low',
    rollback: 'undo',
    required_layers: ['kafka-connect'],
    approvals: {},
    decisions: {},
    params_hash: 'h',
    status: 'open',
    executed: [],
    created_at: new Date().toISOString(),
    ...over,
  }
}

afterEach(() => vi.useRealTimers())

describe('BlastRadius', () => {
  it('shows the skeleton, then "Preview unavailable" after 20 s, never an error', () => {
    vi.useFakeTimers()
    render(<BlastRadius proposal={proposal()} topology={null} />)
    expect(screen.getByText('Previewing what this will do…')).toBeInTheDocument()
    act(() => {
      vi.advanceTimersByTime(PREVIEW_WAIT_MS + 200)
    })
    expect(screen.getByText('Preview unavailable')).toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })

  it('lists target, current and expected, downstream names from the topology, and reversibility', () => {
    const p = proposal({
      preview: {
        proposal_id: 'p_1',
        at: '2026-10-11T10:04:00Z',
        steps: [
          {
            step_index: 0,
            preview: {
              target: 'pg-orders-connector',
              current: 'task 0 FAILED',
              expected: 'task 0 RUNNING',
              downstream: ['topic:pg.orders', 'group:unknown'],
              reversible: true,
              rollback: 'restart again',
              dry_run: { ok: false, source: 'registry compatibility check', text: 'incompatible' },
            },
          },
        ],
      },
    })
    render(<BlastRadius proposal={p} topology={topology} />)
    expect(screen.getByText('This touches')).toBeInTheDocument()
    expect(screen.getByText('pg-orders-connector')).toBeInTheDocument()
    expect(screen.getByText(/task 0 FAILED/)).toBeInTheDocument()
    expect(screen.getByText(/pg\.orders → group:unknown/)).toBeInTheDocument()
    expect(screen.getByText('reversible')).toHaveAttribute('title', 'restart again')
    expect(screen.getByText('Would be rejected')).toBeInTheDocument()
    expect(screen.getByText('incompatible')).toBeInTheDocument()
  })

  it('says downstream unknown, shows Skipped, and a failed step as a warning', () => {
    const p = proposal({
      preview: {
        proposal_id: 'p_1',
        at: '2026-10-11T10:04:00Z',
        steps: [
          {
            step_index: 0,
            preview: { target: 't', current: 'c', expected: 'e', reversible: false, dry_run: { ok: false, source: 's', text: '', skipped: 'no baseline' } },
          },
          { step_index: 1, error: 'dial failed' },
        ],
      },
    })
    render(<BlastRadius proposal={p} topology={null} />)
    expect(screen.getByText(/downstream unknown/)).toBeInTheDocument()
    expect(screen.getByText('not reversible')).toBeInTheDocument()
    expect(screen.getByText('Skipped: no baseline')).toBeInTheDocument()
    expect(screen.getByText(/Preview failed: dial failed/)).toBeInTheDocument()
  })
})
