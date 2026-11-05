import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return { ...actual, isFixtures: () => false, api: { ...actual.api, people: vi.fn() } }
})

import { api } from '../api/client'
import { SteerBox } from './SteerBox'

const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve: vi.fn(),
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
}

// /api/people lists everyone in users.yaml, including people who cannot sign in on a solo server.
const people = [
  { id: 'arjun', name: 'Arjun', verified: true },
  { id: 'priya', name: 'Priya', verified: true },
  { id: 'meera', name: 'Meera', verified: true },
  { id: 'kabir', name: 'Kabir', verified: true },
]
const driving = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }

beforeEach(() => {
  vi.mocked(api.people).mockReset().mockResolvedValue(people)
  vi.mocked(actions.grantControl).mockReset()
})

describe('SteerBox "Give control to" on a solo server (KAN-570)', () => {
  const solo: CurrentUser = { id: 'arjun', name: 'Arjun', layers: [], solo: true }

  it('offers nobody who cannot sign in, and says why', async () => {
    render(<SteerBox state={driving} user={solo} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: /Give control to/ }))

    expect(await screen.findByText('No other person can sign in on this server (solo mode).')).toBeInTheDocument()
    expect(screen.queryByLabelText('Give control to')).not.toBeInTheDocument()
    expect(screen.queryByRole('option')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Give control' })).toBeDisabled()
    expect(actions.grantControl).not.toHaveBeenCalled()
  })

  it('a normal server lists the other people and grants as before', async () => {
    const user: CurrentUser = { id: 'arjun', name: 'Arjun', layers: [] }
    render(<SteerBox state={driving} user={user} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: /Give control to/ }))

    const select = await screen.findByLabelText('Give control to')
    expect(Array.from(select.querySelectorAll('option')).map((o) => o.textContent)).toEqual([
      'Priya (priya)',
      'Meera (meera)',
      'Kabir (kabir)',
    ])
    fireEvent.change(select, { target: { value: 'meera' } })
    fireEvent.click(screen.getByRole('button', { name: 'Give control' }))
    await waitFor(() => expect(actions.grantControl).toHaveBeenCalledWith('meera'))
  })
})
