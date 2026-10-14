import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions, StaleInfo } from '../hooks/useRoom'
import type { RoomState } from '../state/types'
import { emptyRoomState } from '../state/types'

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] },
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
  }),
}))
vi.mock('../hooks/useRoom', () => ({ useRoom: vi.fn() }))
vi.mock('../hooks/useTopology', () => ({ useTopology: vi.fn() }))

import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { RoomScreen } from './RoomScreen'

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
}
const staleOff = { stale: null, dismissStale: vi.fn(), reload: vi.fn() }

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

function show(state: RoomState, stale: StaleInfo | null = null) {
  vi.mocked(useRoom).mockReturnValue({
    state,
    presence: [],
    conn: 'live',
    error: null,
    actions,
    ...staleOff,
    stale,
  })
  vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null, notFound: false, retry: () => undefined })
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

describe('RoomScreen — agent done status and resolution', () => {
  it('shows a green "resolved — verified" chip when the agent posts done', () => {
    show(webhookRoom({ agent_status: { state: 'done', detail: 'fix applied' } }))
    const chip = screen.getByText('resolved — verified')
    expect(chip).toBeInTheDocument()
    // The pill's tone is a data attribute (was the chip--ok class).
    const pill = chip.closest('[data-tone]')
    expect(pill).toHaveAttribute('data-tone', 'ok')
  })

  it('shows the resolution once the room is closed', () => {
    show(webhookRoom({ status: 'closed', resolution: 'Compatibility set to NONE' }))
    expect(screen.getAllByText('Resolved').length).toBeGreaterThan(0)
    expect(screen.getByText('Compatibility set to NONE')).toBeInTheDocument()
  })
})

describe('RoomScreen — topology refresh wiring', () => {
  it('passes one refresh key per executed action and active only while open', () => {
    show(
      webhookRoom({
        status: 'open',
        proposals: [
          {
            proposal_id: 'p1',
            title: 'fix',
            steps: [],
            risk: 'low',
            rollback: 'undo',
            required_layers: [],
            approvals: {},
            decisions: {},
            params_hash: '',
            status: 'approved',
            executed: [
              { proposal_id: 'p1', step_index: 0, ok: true, output: 'a' },
              { proposal_id: 'p1', step_index: 1, ok: true, output: 'b' },
            ],
          },
        ],
      }),
    )
    expect(useTopology).toHaveBeenLastCalledWith('orders', { refreshKey: 2, active: true })

    show(webhookRoom({ status: 'closed', resolution: 'done' }))
    expect(useTopology).toHaveBeenLastCalledWith('orders', { refreshKey: 0, active: false })
  })
})

describe('RoomScreen — stale fix banner (params_mismatch)', () => {
  it('shows the banner when the hook says the fix changed, and Reload/Dismiss call the hook', () => {
    show(webhookRoom(), { proposalId: "p1", sentHash: "h1" })
    const banner = screen.getByRole('alert')
    expect(banner).toHaveTextContent('This fix changed since you loaded it. Reload to see the new version.')
    fireEvent.click(within(banner).getByRole('button', { name: 'Reload' }))
    fireEvent.click(within(banner).getByRole('button', { name: 'Dismiss' }))
    expect(staleOff.reload).toHaveBeenCalledTimes(1)
    expect(staleOff.dismissStale).toHaveBeenCalledTimes(1)
  })

  it('shows no banner by default', () => {
    show(webhookRoom())
    expect(screen.queryByText(/This fix changed since you loaded it/)).not.toBeInTheDocument()
  })
})
