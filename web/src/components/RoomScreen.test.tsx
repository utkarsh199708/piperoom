import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { RoomState } from '../state/types'
import { emptyRoomState } from '../state/types'

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] },
    setUserId: vi.fn(),
  }),
}))
vi.mock('../hooks/useRoom', () => ({ useRoom: vi.fn() }))
vi.mock('../hooks/useTopology', () => ({ useTopology: () => ({ topology: null, error: null }) }))

import { useRoom } from '../hooks/useRoom'
import { RoomScreen } from './RoomScreen'

const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve: vi.fn(),
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  trace: vi.fn(),
}

function webhookRoom(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r_webhook'),
    title: 'ConsumerLag firing on orders-pipeline',
    severity: 'sev1',
    pipeline_id: 'orders',
    opened_at: '2026-10-11T10:05:00Z',
    alert: { source: 'Alertmanager' },
    ...over,
  }
}

function show(state: RoomState) {
  vi.mocked(useRoom).mockReturnValue({
    state,
    presence: [],
    conn: 'live',
    error: null,
    actions,
  })
  return render(
    <MemoryRouter initialEntries={['/rooms/r_webhook']}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
      </Routes>
    </MemoryRouter>,
  )
}

beforeEach(() => {
  vi.resetAllMocks()
})

describe('RoomScreen — alert room header and no-driver banner', () => {
  it('shows the alert source and severity from room.created in the header', () => {
    show(webhookRoom())
    expect(screen.getByText('ConsumerLag firing on orders-pipeline')).toBeInTheDocument()
    expect(screen.getByText('sev1')).toBeInTheDocument()
    expect(screen.getByText('alert · Alertmanager')).toBeInTheDocument()
  })

  it('shows the no-driver banner and grants control to the current user', () => {
    show(webhookRoom())
    // The banner's button takes control (the SteerBox has an inline one too).
    const banner = screen.getByRole('status')
    expect(within(banner).getByText(/No one is driving/)).toBeInTheDocument()
    fireEvent.click(within(banner).getByRole('button', { name: 'Take control' }))
    expect(actions.grantControl).toHaveBeenCalledWith('arjun')
  })

  it('hides the banner once someone is driving', () => {
    show(webhookRoom({ driver: { id: 'kabir', name: 'Kabir' } }))
    expect(screen.queryByText(/No one is driving/)).not.toBeInTheDocument()
    expect(screen.getByText('alert · Alertmanager')).toBeInTheDocument()
  })

  it('shows a plain alert chip when the source cannot be detected', () => {
    show(webhookRoom({ alert: { source: null } }))
    expect(screen.getByText('alert')).toBeInTheDocument()
  })

  it('has no alert chip for rooms created without an alert', () => {
    show(webhookRoom({ alert: null }))
    expect(screen.queryByText('alert')).not.toBeInTheDocument()
    expect(screen.queryByText(/alert ·/)).not.toBeInTheDocument()
  })
})
