import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { ConnState } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { RoomState } from '../state/types'

// The U3 layout of the room screen: header, pill, rail, loading and error, and the keys
// printed on the buttons.
const ctx = vi.hoisted(() => ({ user: null as unknown as CurrentUser }))

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user: ctx.user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0, busy: false }),
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
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
}
const reload = vi.fn()

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'], role: 'member', verified: true }

function room(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r_schema_change'),
    title: 'Warehouse orders stale',
    severity: 'sev2',
    pipeline_id: 'orders',
    opened_at: '2026-10-11T10:00:00Z',
    driver: { id: 'kabir', name: 'Kabir', verified: true, lease_until: new Date(Date.now() + 3_600_000).toISOString() },
    agent_status: { state: 'working', detail: 'step 2' },
    steps: [
      { id: 's1', title: 'Check the connector', tool_calls: [], messages: [] },
      { id: 's2', title: 'Check the DDL log', tool_calls: [], messages: [] },
    ],
    members: [
      { id: 'kabir', name: 'Kabir', role: 'driver', verified: true },
      { id: 'arjun', name: 'Arjun', role: 'watching', verified: true },
    ],
    ...over,
  }
}

function show(user: CurrentUser, state: RoomState | null, conn: ConnState = 'live', error: string | null = null) {
  ctx.user = user
  vi.mocked(useRoom).mockReturnValue({
    state,
    presence: [],
    conn,
    error,
    actions,
    stale: null,
    dismissStale: vi.fn(),
    reload,
  })
  vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null, notFound: false, retry: () => undefined })
  return render(
    <MemoryRouter initialEntries={['/rooms/r_schema_change']}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
        <Route path="/" element={<p>rooms home</p>} />
      </Routes>
    </MemoryRouter>,
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  Element.prototype.scrollIntoView = vi.fn()
})

const withHypothesis = (cited: string[]) =>
  room({
    hypothesis: {
      text: 'The schema change broke the connector.',
      based_on_steps: cited,
      confidence: 'high',
      confirmed: false,
      seq: 3,
      verdicts: [],
    },
  })

describe('RoomScreen citation chips (F3)', () => {
  it('opens, scrolls to and flashes the cited step, then stops flashing', () => {
    vi.useFakeTimers()
    try {
      show(arjun, withHypothesis(['s1']))
      const row = document.getElementById('step-s1') as HTMLElement
      expect(row).not.toHaveAttribute('data-cited')
      vi.mocked(Element.prototype.scrollIntoView).mockClear()
      fireEvent.click(screen.getByRole('button', { name: 'Go to step 1: Check the connector' }))
      expect(Element.prototype.scrollIntoView).toHaveBeenCalledWith(expect.objectContaining({ block: 'center' }))
      expect(row).toHaveAttribute('data-cited')
      expect(within(row).getByRole('button', { name: /Check the connector/ })).toHaveAttribute('aria-expanded', 'true')
      act(() => {
        vi.advanceTimersByTime(650)
      })
      expect(row).not.toHaveAttribute('data-cited')
    } finally {
      vi.useRealTimers()
    }
  })

  it('shows step ? for an id the log does not hold, and it does nothing', () => {
    show(arjun, withHypothesis(['s_gone']))
    const chip = screen.getByRole('button', { name: 'step ?' })
    vi.mocked(Element.prototype.scrollIntoView).mockClear()
    expect(chip).toBeDisabled()
    expect(chip).toHaveAttribute('title', 'This step is not in the room log')
    expect(screen.queryByText('s_gone')).not.toBeInTheDocument()
    fireEvent.click(chip)
    expect(Element.prototype.scrollIntoView).not.toHaveBeenCalled()
  })

  it('hovering a chip marks the row without scrolling', () => {
    show(arjun, withHypothesis(['s1']))
    const row = document.getElementById('step-s1') as HTMLElement
    const chip = screen.getByRole('button', { name: /Go to step 1/ })
    vi.mocked(Element.prototype.scrollIntoView).mockClear()
    fireEvent.mouseEnter(chip)
    expect(row).toHaveAttribute('data-cited')
    expect(Element.prototype.scrollIntoView).not.toHaveBeenCalled()
    fireEvent.mouseLeave(chip)
    expect(row).not.toHaveAttribute('data-cited')
  })
})
