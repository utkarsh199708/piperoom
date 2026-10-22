import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { Topology } from '../api/types'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { RoomState } from '../state/types'

// X3: a forecast room opens on the chart of the trend that opened it, above the lane.
const user: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['postgres'], role: 'member', verified: true }

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0, busy: false }),
}))
vi.mock('../hooks/useRoom', () => ({ useRoom: vi.fn() }))
vi.mock('../hooks/useTopology', () => ({ useTopology: vi.fn() }))
vi.mock('../hooks/useTrends', () => ({ useTrends: vi.fn() }))

import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { useTrends } from '../hooks/useTrends'
import { RoomScreen } from './RoomScreen'

const topology: Topology = {
  pipeline_id: 'orders',
  nodes: [{ id: 'pg:public.orders', kind: 'source_table', name: 'public.orders', layer: 'postgres', owner: 'priya', health: 'ok' }],
  edges: [],
}

const forecast = {
  metric: 'slot_wal_bytes',
  object: 'orders_slot',
  current: 64424509440,
  limit: 107374182400,
  eta: new Date(Date.now() + 3 * 86_400_000 + 60_000).toISOString(),
  slope_per_hour: 1073741824,
  r2: 0.97,
}

const points = [
  { at: new Date(Date.now() - 900_000).toISOString(), value: 60e9 },
  { at: new Date().toISOString(), value: 64e9 },
]

function show(over: Partial<RoomState>) {
  vi.mocked(useRoom).mockReturnValue({
    state: {
      ...emptyRoomState('r_wal'),
      title: 'Slot WAL will fill',
      severity: 'sev4',
      pipeline_id: 'orders',
      opened_at: new Date().toISOString(),
      ...over,
    },
    presence: [],
    conn: 'live',
    error: null,
    actions: {} as RoomActions,
    stale: null,
    dismissStale: vi.fn(),
    reload: vi.fn(),
  })
  vi.mocked(useTopology).mockReturnValue({ topology, error: null, updatedAt: null, notFound: false, retry: () => undefined })
  vi.mocked(useTrends).mockReturnValue({ status: 'ready', series: { slot_wal_bytes: points } })
  return render(
    <MemoryRouter initialEntries={['/rooms/r_wal']}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
      </Routes>
    </MemoryRouter>,
  )
}

beforeEach(() => vi.clearAllMocks())

describe('RoomScreen — forecast room', () => {
  it('shows the forecast chart before the lane, and the badge on the source node', () => {
    show({ origin: 'forecast', forecast })
    const chart = document.querySelector('[data-forecast-chart]') as HTMLElement
    const lane = screen.getAllByRole('group', { name: 'Pipeline, source to sink' })[0]
    expect(chart).not.toBeNull()
    expect(chart.compareDocumentPosition(lane) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    expect(lane.querySelector('[data-eta]')).toHaveTextContent('fills in 3 d')
    expect(lane.querySelector('[data-sparkline]')).not.toBeNull()
    expect(vi.mocked(useTrends)).toHaveBeenCalledWith('orders', true)
  })

  it('another room has no chart, even with a forecast in its evidence', () => {
    show({ forecast })
    expect(document.querySelector('[data-forecast-chart]')).toBeNull()
    expect(document.querySelector('[data-eta]')).not.toBeNull()
  })
})
