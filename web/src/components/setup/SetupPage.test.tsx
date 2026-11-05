import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'

vi.mock('../../context/user', () => ({
  useCurrentUser: () => ({ user: { id: 'arjun', name: 'Arjun', layers: ['environment'], role: 'member' } }),
}))
vi.mock('../../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../api/client')>()
  return { ...actual, isFixtures: () => false, api: { ...actual.api, createSetup: vi.fn() } }
})
vi.mock('../../fixtures/player', () => ({ addFixtureRoom: vi.fn(), fixtureRooms: [] }))

import { api } from '../../api/client'
import { SetupPage } from './SetupPage'
import { setupPairing } from '../../lib/setup'

const createSetup = vi.mocked(api.createSetup)

function show() {
  return render(
    <MemoryRouter initialEntries={['/setup']}>
      <Routes>
        <Route path="/setup" element={<SetupPage />} />
        <Route path="/rooms/:roomId" element={<div>room opened</div>} />
      </Routes>
    </MemoryRouter>,
  )
}

function fillValid() {
  fireEvent.change(screen.getByLabelText('Pipeline id'), { target: { value: 'orders' } })
  fireEvent.change(screen.getByLabelText('Title'), { target: { value: 'Watch orders' } })
  fireEvent.change(screen.getByLabelText('Topic prefix'), { target: { value: 'orders' } })
  fireEvent.change(screen.getByLabelText('Source tables'), { target: { value: 'public.orders\npublic.order_items' } })
  fireEvent.change(screen.getByLabelText('Address 1'), { target: { value: 'db.acme:5432' } })
  fireEvent.change(screen.getByLabelText('Class 1'), { target: { value: 'production' } })
}

beforeEach(() => {
  sessionStorage.clear()
  createSetup.mockReset()
})

describe('Set up page (U1-12a)', () => {
  it('labels every field the task names, including a class per target', () => {
    show()
    for (const label of ['Tenant', 'Pipeline id', 'Title', 'Source', 'Target', 'Topic prefix', 'Source tables', 'Kind 1', 'Address 1', 'Class 1']) {
      expect(screen.getByLabelText(label)).toBeInTheDocument()
    }
    const classes = screen.getByLabelText('Class 1') as HTMLSelectElement
    expect([...classes.options].map((o) => o.value)).toEqual(['', 'local', 'shared', 'production'])
    expect(screen.getByRole('group', { name: 'Targets and classes' })).toBeInTheDocument()
  })

  it('a second target can be added and removed', () => {
    show()
    fireEvent.click(screen.getByRole('button', { name: 'Add target' }))
    expect(screen.getByLabelText('Address 2')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Remove target 2' }))
    expect(screen.queryByLabelText('Address 2')).not.toBeInTheDocument()
  })

  it('refuses a missing title before any request', () => {
    show()
    fillValid()
    fireEvent.change(screen.getByLabelText('Title'), { target: { value: '  ' } })
    fireEvent.click(screen.getByRole('button', { name: 'Open the setup room' }))
    expect(screen.getByText('Title is required.')).toBeInTheDocument()
    expect(screen.getByLabelText('Title')).toHaveAttribute('aria-invalid', 'true')
    expect(createSetup).not.toHaveBeenCalled()
  })

  it('posts the request the server documents, remembers the command and opens the room', async () => {
    createSetup.mockResolvedValue({
      room: { id: 'r_new' } as never,
      code: 'ABCD2345',
      expires_at: '2026-10-22T10:10:00Z',
      command: 'piperoom-setup --server http://127.0.0.1:8090 --code ABCD2345',
    })
    show()
    fillValid()
    fireEvent.click(screen.getByRole('button', { name: 'Open the setup room' }))

    await waitFor(() => expect(createSetup).toHaveBeenCalledTimes(1))
    expect(createSetup.mock.calls[0][0]).toEqual({
      pipeline_id: 'orders',
      title: 'Watch orders',
      source_kind: 'postgres',
      target: 'compose',
      topic_prefix: 'orders',
      tables: ['public.orders', 'public.order_items'],
      targets: [{ kind: 'postgres', address: 'db.acme:5432', class: 'production' }],
    })
    expect(await screen.findByText('room opened')).toBeInTheDocument()
    expect(setupPairing('r_new')?.command).toContain('--code ABCD2345')
  })

  it('shows the server error above the form', async () => {
    createSetup.mockRejectedValue(new Error('pipeline_exists'))
    show()
    fillValid()
    fireEvent.click(screen.getByRole('button', { name: 'Open the setup room' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('pipeline_exists')
  })
})
