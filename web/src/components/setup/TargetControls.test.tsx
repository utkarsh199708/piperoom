import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

vi.mock('@/api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../api/client')>()
  return {
    ...actual,
    isFixtures: () => false,
    api: { ...actual.api, setupTarget: vi.fn(), setupTargetClass: vi.fn() },
  }
})

import { api } from '@/api/client'
import { TargetControls } from './TargetControls'
import { setupState } from '../../test/setupRoom'
import { SETUP_ROOM_ID } from '../../fixtures/setup'
import type { CurrentUser } from '../../state/permissions'

const setupTarget = vi.mocked(api.setupTarget)
const setupTargetClass = vi.mocked(api.setupTargetClass)

const driver: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['environment'], role: 'member' }
const watcher: CurrentUser = { id: 'priya', name: 'Priya', layers: [], role: 'member' }
const runner = { label: 'arjun-laptop', connected: true, tools: {} }

function room() {
  return setupState({
    snapshot: {
      runner,
      targets: [
        { kind: 'postgres', address: 'db.acme:5432', class: 'shared' },
        { kind: 'kafka', address: 'kafka.acme:9092', class: 'production' },
      ],
    },
  })
}

beforeEach(() => {
  setupTarget.mockReset()
  setupTargetClass.mockReset()
})

describe('TargetControls (U1-12b)', () => {
  it('lists every target with its class', () => {
    render(<TargetControls state={room()} user={driver} />)
    const card = screen.getByRole('region', { name: 'Targets and classes' })
    expect(card).toHaveTextContent('db.acme:5432')
    expect(card).toHaveTextContent('shared')
    expect(screen.getAllByRole('listitem')).toHaveLength(2)
    expect(screen.getByLabelText('Raise class for db.acme:5432')).toBeInTheDocument()
  })

  it('offers only classes above the target’s current floor', () => {
    render(<TargetControls state={room()} user={driver} />)
    const shared = screen.getByLabelText('New class for db.acme:5432') as HTMLSelectElement
    expect([...shared.options].map((o) => o.value)).toEqual(['production'])
    // production is the top: no select, just the fact that it cannot rise further
    expect(screen.queryByLabelText('New class for kafka.acme:9092')).not.toBeInTheDocument()
    expect(screen.getByText('Highest class')).toBeInTheDocument()
  })

  it('raises a class through its own route', async () => {
    setupTargetClass.mockResolvedValue({} as never)
    render(<TargetControls state={room()} user={driver} />)
    fireEvent.click(screen.getByRole('button', { name: 'Raise class for db.acme:5432' }))
    await waitFor(() =>
      expect(setupTargetClass).toHaveBeenCalledWith(SETUP_ROOM_ID, 'db.acme:5432', { class: 'production' }),
    )
    expect(await screen.findByText('Class raised to production for db.acme:5432.')).toBeInTheDocument()
  })

  it('adds a target with the kind, address and an optional class', async () => {
    setupTarget.mockResolvedValue({} as never)
    render(<TargetControls state={room()} user={driver} />)
    fireEvent.change(screen.getByLabelText('Kind'), { target: { value: 'connect' } })
    fireEvent.change(screen.getByLabelText('New target address'), { target: { value: 'connect.acme:8083' } })
    fireEvent.click(screen.getByRole('button', { name: 'Add target' }))
    await waitFor(() =>
      expect(setupTarget).toHaveBeenCalledWith(SETUP_ROOM_ID, {
        kind: 'connect',
        address: 'connect.acme:8083',
      }),
    )
    expect(await screen.findByText('Target connect.acme:8083 added.')).toBeInTheDocument()
    expect(screen.getByLabelText('New target address')).toHaveValue('')
  })

  it('refuses an empty address before any request', () => {
    render(<TargetControls state={room()} user={driver} />)
    fireEvent.click(screen.getByRole('button', { name: 'Add target' }))
    expect(screen.getByText('Target address is required.')).toBeInTheDocument()
    expect(setupTarget).not.toHaveBeenCalled()
  })

  it('shows the server’s refusal above the form', async () => {
    setupTarget.mockRejectedValue(new Error('address_refused'))
    render(<TargetControls state={room()} user={driver} />)
    fireEvent.change(screen.getByLabelText('New target address'), { target: { value: '169.254.169.254:80' } })
    fireEvent.click(screen.getByRole('button', { name: 'Add target' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('address_refused')
  })

  it('cannot change anything for someone who is not the driver', () => {
    render(<TargetControls state={room()} user={watcher} />)
    expect(screen.getByText(/Only the driver can add targets or raise a class/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Add target' })).not.toBeInTheDocument()
    expect(screen.queryByLabelText('New class for db.acme:5432')).not.toBeInTheDocument()
  })
})
