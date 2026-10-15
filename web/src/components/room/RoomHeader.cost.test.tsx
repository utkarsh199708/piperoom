import { describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { RoomHeader } from './RoomHeader'
import { emptyInsight, emptyRoomState } from '../../state/types'
import type { RoomState } from '../../state/types'
import type { CurrentUser } from '../../state/permissions'

// Q0 (QA G14, G16) as the header shows them.
const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: [], role: 'member' }
const priya: CurrentUser = { id: 'priya', name: 'Priya', layers: ['schema-registry'], role: 'member' }
const sam: CurrentUser = { id: 'sam', name: 'Sam', layers: [], role: 'member' }

function room(usage: Partial<RoomState['insight']['usage']>, over: Partial<RoomState> = {}): RoomState {
  const i = emptyInsight()
  return {
    ...emptyRoomState('r_1'),
    title: 'Warehouse orders stale',
    pipeline_id: 'orders',
    opened_at: '2026-10-15T10:00:00Z',
    driver: { id: 'kabir', name: 'Kabir' },
    insight: { ...i, steps: 4, usage: { ...i.usage, ...usage } },
    ...over,
  }
}

function show(state: RoomState, user: CurrentUser) {
  return render(
    <RoomHeader
      state={state}
      user={user}
      members={[{ id: 'kabir', name: 'Kabir' }]}
      conn="live"
      copied={false}
      onHandoff={vi.fn()}
      onCloseRoom={vi.fn()}
      onInvite={vi.fn()}
    />,
  )
}

describe('RoomHeader cost', () => {
  it('shows the estimated cost next to the agent pill, with the tokens in its description', () => {
    show(room({ calls: 9, tokens: 812_200, cost_usd: 1.42 }), kabir)
    const header = screen.getByRole('banner')
    const badge = within(header).getByTestId('cost-badge')
    expect(badge).toHaveTextContent('$1.42 est.')
    expect(document.getElementById(badge.getAttribute('aria-describedby') as string)).toHaveTextContent('812,200 tokens')
    // The facts-line chip keeps the steps and the clock, and does not repeat the cost.
    expect(within(header).getByTestId('cost-chip')).not.toHaveTextContent('$')
  })

  it('a lower bound says so', () => {
    show(room({ calls: 9, unpriced_calls: 3, tokens: 10, cost_usd: 1.42 }), kabir)
    expect(screen.getByTestId('cost-badge')).toHaveTextContent('≥ $1.42 est.')
  })

  it('shows no badge while the cost is unknown, and the chip names the tokens instead', () => {
    show(room({ calls: 9, unpriced_calls: 9, tokens: 812_200, cost_usd: null }), kabir)
    expect(screen.queryByTestId('cost-badge')).not.toBeInTheDocument()
    expect(screen.getByTestId('cost-chip')).toHaveTextContent('812k tokens')
  })

  it('shows the snapshot\'s figure on the first paint, before the log has replayed', () => {
    const snapshot = { calls: 40, unpriced_calls: 0, tokens: 2_000_000, cost_usd: 3.1, latency_ms_total: 0 }
    show(room({}, { replay_target: 300, last_seq: 0, snapshot_usage: snapshot }), kabir)
    expect(screen.getByTestId('cost-badge')).toHaveTextContent('$3.10 est.')
  })
})

describe('RoomHeader Close (QA G16)', () => {
  it('the driver and a layer owner see Close room, anyone else does not', () => {
    const driver = show(room({}), kabir)
    expect(screen.getByRole('button', { name: /Close room/ })).toBeInTheDocument()
    driver.unmount()
    const owner = show(room({}), priya)
    expect(screen.getByRole('button', { name: /Close room/ })).toBeInTheDocument()
    owner.unmount()
    show(room({}), sam)
    expect(screen.queryByRole('button', { name: /Close room/ })).not.toBeInTheDocument()
  })
})

// Q3 S9: a closed room has no driver; the header says who closed it.
describe('RoomHeader closed room', () => {
  it('shows who closed it and not "is driving"', () => {
    show(
      room({}, { status: 'closed', closed_at: '2026-10-15T10:05:00Z', closed_by: { kind: 'human', id: 'kabir', name: 'Kabir' } }),
      priya,
    )
    expect(screen.queryByText(/is driving/)).toBeNull()
    // The name is a PersonName (with its verified badge), so "Closed by" and "Kabir" are separate nodes.
    expect(within(screen.getByText(/^\s*Closed by$/)).getByText('Kabir')).toBeInTheDocument()
  })

  it('still says "is driving" while open', () => {
    show(room({}), priya)
    expect(within(screen.getByText(/^\s*is driving$/)).getByText('Kabir')).toBeInTheDocument()
  })
})
