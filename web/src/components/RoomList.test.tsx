import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import type { RoomSnapshot } from '../api/types'

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] },
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
  }),
}))
vi.mock('../hooks/useRoomList', () => ({ useRoomList: vi.fn() }))
vi.mock('../api/client', () => {
  class ApiClientError extends Error {
    code: string
    status: number
    constructor(status: number, code: string, message: string) {
      super(message)
      this.name = 'ApiClientError'
      this.code = code
      this.status = status
    }
  }
  return { api: { createRoom: vi.fn() }, isFixtures: () => false, ApiClientError }
})

import { useRoomList } from '../hooks/useRoomList'
import { api } from '../api/client'
import { RoomList } from './RoomList'

function room(over: Partial<RoomSnapshot> & { id: string }): RoomSnapshot {
  return {
    title: over.id,
    severity: 'sev3',
    pipeline_id: 'orders',
    status: 'open',
    opened_at: '2026-10-11T10:00:00Z',
    last_seq: 0,
    driver: null,
    agent_status: { state: 'idle' },
    hypothesis: null,
    open_proposals: [],
    pending_suggestions: [],
    members: [],
    ...over,
  }
}

describe('RoomList', () => {
  it('lists open rooms before closed (newest first) and shows severity, pipeline, opened time and driver', () => {
    vi.mocked(useRoomList).mockReturnValue({
      rooms: [
        room({ id: 'c1', title: 'Closed one', severity: 'sev4', status: 'closed', opened_at: '2026-10-11T12:00:00Z' }),
        room({ id: 'o_old', title: 'Older open', opened_at: '2026-10-11T09:00:00Z' }),
        room({
          id: 'o_new',
          title: 'Newest open',
          severity: 'sev1',
          opened_at: '2026-10-11T11:00:00Z',
          driver: { id: 'kabir', name: 'Kabir' },
        }),
      ],
      loading: false,
      error: null,
      reload: vi.fn(),
    })

    const { container } = render(
      <MemoryRouter>
        <RoomList />
      </MemoryRouter>,
    )

    // Open rooms first, newest opened first; the closed room comes last.
    const titles = Array.from(container.querySelectorAll('.room-card__title')).map(
      (el) => el.textContent,
    )
    expect(titles).toEqual(['Newest open', 'Older open', 'Closed one'])

    const newest = screen.getByRole('link', { name: /Newest open/ })
    expect(within(newest).getByText('sev1')).toBeInTheDocument()
    expect(within(newest).getByText(/opened/)).toBeInTheDocument()
    expect(within(newest).getByText(/driver Kabir/)).toBeInTheDocument()

    const older = screen.getByRole('link', { name: /Older open/ })
    expect(within(older).getByText(/no driver/)).toBeInTheDocument()
  })

  it('shows an error when creating a room fails', async () => {
    vi.mocked(useRoomList).mockReturnValue({
      rooms: [],
      loading: false,
      error: null,
      reload: vi.fn(),
    })
    vi.mocked(api.createRoom).mockRejectedValueOnce(new Error('validation: unknown pipeline'))

    render(
      <MemoryRouter>
        <RoomList />
      </MemoryRouter>,
    )

    fireEvent.change(screen.getByLabelText('Title'), { target: { value: 'Broken room' } })
    fireEvent.click(screen.getByRole('button', { name: 'Open room' }))

    expect(await screen.findByText(/validation: unknown pipeline/)).toBeInTheDocument()
  })
})
