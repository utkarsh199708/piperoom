import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

vi.mock('@/api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../api/client')>()
  return { ...actual, isFixtures: () => false, api: { ...actual.api, setupPipeline: vi.fn() } }
})

import { api } from '@/api/client'
import { PipelineExtend } from './PipelineExtend'
import { setupState } from '../../test/setupRoom'
import { SETUP_ROOM_ID, pipelineExtended } from '../../fixtures/setup'
import type { CurrentUser } from '../../state/permissions'

const setupPipeline = vi.mocked(api.setupPipeline)

const driver: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['environment'], role: 'member' }
const watcher: CurrentUser = { id: 'priya', name: 'Priya', layers: [], role: 'member' }
const runner = { label: 'arjun-laptop', connected: true, tools: {} }

function room() {
  return setupState({ snapshot: { runner }, events: [] })
}

describe('PipelineExtend (U1-12b)', () => {
  it('shows the provisional pipeline the room is scoped to', () => {
    render(<PipelineExtend state={room()} user={driver} />)
    const card = screen.getByRole('region', { name: 'Pipeline' })
    expect(card).toHaveTextContent('public.orders')
    expect(card).toHaveTextContent('public.order_items')
    expect(card).toHaveTextContent('Connectors: none')
  })

  it('posts only the lists that name something', async () => {
    setupPipeline.mockResolvedValue({} as never)
    render(<PipelineExtend state={room()} user={driver} />)
    fireEvent.change(screen.getByLabelText('Add connectors'), { target: { value: 'orders-src\norders-outbox' } })
    fireEvent.change(screen.getByLabelText('Add tables'), { target: { value: 'public.shipments' } })
    fireEvent.click(screen.getByRole('button', { name: 'Extend the pipeline' }))
    await waitFor(() =>
      expect(setupPipeline).toHaveBeenCalledWith(SETUP_ROOM_ID, {
        add_connectors: ['orders-src', 'orders-outbox'],
        add_tables: ['public.shipments'],
      }),
    )
    expect(screen.getByLabelText('Add connectors')).toHaveValue('')
  })

  it('refuses an empty extension before any request', () => {
    render(<PipelineExtend state={room()} user={driver} />)
    fireEvent.click(screen.getByRole('button', { name: 'Extend the pipeline' }))
    expect(screen.getByText(/Name at least one connector/)).toBeInTheDocument()
    expect(setupPipeline).not.toHaveBeenCalled()
  })

  it('shows the server’s validation error', async () => {
    setupPipeline.mockRejectedValue(new Error('validation: public.orders is not in this source'))
    render(<PipelineExtend state={room()} user={driver} />)
    fireEvent.change(screen.getByLabelText('Add tables'), { target: { value: 'public.orders' } })
    fireEvent.click(screen.getByRole('button', { name: 'Extend the pipeline' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('validation')
  })

  it('folds setup.pipeline_extended into the names it shows', () => {
    const state = setupState({
      snapshot: { runner },
      events: [pipelineExtended({ add_connectors: ['orders-src'], add_tables: ['public.shipments'] }, 3)],
    })
    render(<PipelineExtend state={state} user={driver} />)
    const card = screen.getByRole('region', { name: 'Pipeline' })
    expect(card).toHaveTextContent('orders-src')
    expect(card).toHaveTextContent('public.shipments')
  })

  it('is read-only for someone who is not the driver', () => {
    render(<PipelineExtend state={room()} user={watcher} />)
    expect(screen.getByText('Only the driver can extend the pipeline.')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Extend the pipeline' })).not.toBeInTheDocument()
  })
})
