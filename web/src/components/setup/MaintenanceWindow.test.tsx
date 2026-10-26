import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

vi.mock('@/api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../api/client')>()
  return { ...actual, isFixtures: () => false, api: { ...actual.api, setupMaintenanceWindow: vi.fn() } }
})

import { api } from '@/api/client'
import { MaintenanceWindow } from './MaintenanceWindow'
import { setupState } from '../../test/setupRoom'
import { SETUP_ROOM_ID, maintenanceWindow } from '../../fixtures/setup'
import type { CurrentUser } from '../../state/permissions'

const setupWindow = vi.mocked(api.setupMaintenanceWindow)

const driver: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['environment'], role: 'member' }
const watcher: CurrentUser = { id: 'priya', name: 'Priya', layers: [], role: 'member' }
const runner = { label: 'arjun-laptop', connected: true, tools: {} }

const pad = (n: number) => String(n).padStart(2, '0')
function inputValue(offsetHours: number): string {
  const date = new Date(Date.now() + offsetHours * 60 * 60 * 1000)
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`
}
function instant(value: string): string {
  return new Date(value).toISOString()
}

function room() {
  return setupState({ snapshot: { runner } })
}

describe('MaintenanceWindow (U1-12b)', () => {
  it('posts RFC 3339 times for a window inside the rules', async () => {
    setupWindow.mockResolvedValue({} as never)
    const start = inputValue(2)
    const end = inputValue(4)
    render(<MaintenanceWindow state={room()} user={driver} />)
    fireEvent.change(screen.getByLabelText('Window starts'), { target: { value: start } })
    fireEvent.change(screen.getByLabelText('Window ends'), { target: { value: end } })
    fireEvent.click(screen.getByRole('button', { name: 'Set maintenance window' }))
    await waitFor(() =>
      expect(setupWindow).toHaveBeenCalledWith(SETUP_ROOM_ID, {
        start: instant(start),
        end: instant(end),
      }),
    )
    expect(await screen.findByText(/Maintenance window set/)).toBeInTheDocument()
  })

  it('refuses a window longer than four hours before any request', () => {
    render(<MaintenanceWindow state={room()} user={driver} />)
    fireEvent.change(screen.getByLabelText('Window starts'), { target: { value: inputValue(1) } })
    fireEvent.change(screen.getByLabelText('Window ends'), { target: { value: inputValue(6) } })
    fireEvent.click(screen.getByRole('button', { name: 'Set maintenance window' }))
    expect(screen.getByText('A window is at most four hours long.')).toBeInTheDocument()
    expect(screen.getByLabelText('Window ends')).toHaveAttribute('aria-invalid', 'true')
    expect(setupWindow).not.toHaveBeenCalled()
  })

  it('refuses an end before the start', () => {
    render(<MaintenanceWindow state={room()} user={driver} />)
    fireEvent.change(screen.getByLabelText('Window starts'), { target: { value: inputValue(3) } })
    fireEvent.change(screen.getByLabelText('Window ends'), { target: { value: inputValue(2) } })
    fireEvent.click(screen.getByRole('button', { name: 'Set maintenance window' }))
    expect(screen.getByText('The window must end after it starts.')).toBeInTheDocument()
    expect(setupWindow).not.toHaveBeenCalled()
  })

  it('shows the window already set', () => {
    const state = setupState({
      snapshot: { runner },
      events: [maintenanceWindow('2026-10-22T18:00:00Z', '2026-10-22T20:00:00Z', 3)],
    })
    render(<MaintenanceWindow state={state} user={driver} />)
    expect(screen.getByText(/Current window:/)).toBeInTheDocument()
  })

  it('shows the server refusal and clears nothing', async () => {
    setupWindow.mockRejectedValue(new Error('window_too_long'))
    render(<MaintenanceWindow state={room()} user={driver} />)
    fireEvent.change(screen.getByLabelText('Window starts'), { target: { value: inputValue(1) } })
    fireEvent.change(screen.getByLabelText('Window ends'), { target: { value: inputValue(2) } })
    fireEvent.click(screen.getByRole('button', { name: 'Set maintenance window' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('window_too_long')
  })

  it('is read-only for someone who is not the driver', () => {
    render(<MaintenanceWindow state={room()} user={watcher} />)
    expect(screen.getByText('Only the driver can set a maintenance window.')).toBeInTheDocument()
    expect(screen.queryByLabelText('Window starts')).not.toBeInTheDocument()
  })
})
