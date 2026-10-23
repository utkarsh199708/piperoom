import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { Topology } from '../../api/types'

vi.mock('../../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: [] },
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
  }),
}))

vi.mock('../../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../api/client')>()
  return { ...actual, isFixtures: () => false, api: { ...actual.api, topology: vi.fn(), trends: vi.fn() } }
})

import { api } from '../../api/client'
import { PipelineLanePage } from './PipelineLanePage'

const topology: Topology = {
  pipeline_id: 'orders',
  nodes: [
    { id: 'pg:public.orders', kind: 'source_table', name: 'public.orders', layer: 'postgres', owner: 'db', health: 'warn' },
    { id: 'connect:pg-orders-connector', kind: 'connector', name: 'pg-orders-connector', layer: 'kafka-connect', owner: 'ops', health: 'fail' },
  ],
  edges: [{ from: 'pg:public.orders', to: 'connect:pg-orders-connector' }],
}

const renderAt = (path: string) =>
  render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/pipelines/:pipelineId" element={<PipelineLanePage />} />
      </Routes>
    </MemoryRouter>,
  )

beforeEach(() => {
  vi.mocked(api.topology).mockResolvedValue(topology)
  vi.mocked(api.trends).mockResolvedValue([])
})

afterEach(() => vi.resetAllMocks())

describe('PipelineLanePage', () => {
  it('draws the lane and names the worst node the fleet linked', async () => {
    renderAt('/pipelines/orders?node=connect%3Apg-orders-connector')
    expect(await screen.findByTestId('worst-node')).toHaveTextContent('Worst node: connect:pg-orders-connector (kafka-connect)')
    expect(api.topology).toHaveBeenCalledWith('orders')
    expect(screen.getByRole('link', { name: 'Rooms on this pipeline' })).toHaveAttribute('href', '/?pipeline=orders')
    expect(screen.getByRole('link', { name: 'Back to fleet' })).toHaveAttribute('href', '/fleet')
  })

  it('says so when the linked node is not on the lane', async () => {
    renderAt('/pipelines/orders?node=sink%3Agone')
    expect(await screen.findByTestId('worst-node')).toHaveTextContent('sink:gone (not on the lane right now)')
  })
})
