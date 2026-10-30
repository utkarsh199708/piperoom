import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { ProposalState, RoomState } from '../state/types'
import { emptyRoomState } from '../state/types'

// KAN-560: a room that was closed with a proposal still "open" in the log. RoomScreen hands the
// rail the room's closed state (one prop), so rail, card and decision bar all say the same thing.

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] },
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

const actions = {
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
} as unknown as RoomActions

const unrun: ProposalState = {
  proposal_id: 'p1',
  title: 'Set subject compatibility to NONE',
  steps: [],
  risk: 'low',
  rollback: 'set it back to BACKWARD',
  required_layers: ['schema-registry', 'kafka-connect'],
  approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' },
  decisions: {},
  params_hash: 'abc123abc123',
  status: 'open',
  executed: [],
}

function room(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r1'),
    title: 'Orders connector stopped',
    severity: 'sev2',
    pipeline_id: 'orders',
    opened_at: '2026-10-11T10:05:00Z',
    driver: { id: 'kabir', name: 'Kabir' },
    proposals: [unrun],
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
    stale: null,
    dismissStale: vi.fn(),
    reload: vi.fn(),
  })
  vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null, notFound: false, retry: () => undefined })
  return render(
    <MemoryRouter initialEntries={['/rooms/r1']}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
      </Routes>
    </MemoryRouter>,
  )
}

beforeEach(() => {
  vi.resetAllMocks()
  window.sessionStorage.clear()
})

describe('RoomScreen with a proposal that never got all its approvals', () => {
  it('on a closed room says "not run" in the rail and on the card, and offers nothing to decide', () => {
    show(room({ status: 'closed', closed_at: '2026-10-11T10:30:00Z', resolution: 'gave up' }))

    const rail = screen.getByRole('complementary', { name: 'Room details' })
    expect(within(rail).getByText('Not run · room closed')).toBeInTheDocument()
    expect(within(rail).queryByText(/waiting for/i)).not.toBeInTheDocument()
    expect(within(rail).queryByText(/\d+ of \d+/)).not.toBeInTheDocument()
    expect(within(rail).queryByText(/\bopen\b/i)).not.toBeInTheDocument()

    const card = screen.getByRole('article', { name: 'Set subject compatibility to NONE' })
    expect(within(card).getByText('not run · room closed')).toBeInTheDocument()

    expect(screen.queryByText(/waiting for/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/\d+ of \d+ approved/)).not.toBeInTheDocument()
    expect(screen.queryByRole('region', { name: 'Decision' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^(Approve|Reject|Nudge)/ })).not.toBeInTheDocument()
  })

  it('on an open room still waits for the layer, in the rail and on the card', () => {
    show(room())

    const rail = screen.getByRole('complementary', { name: 'Room details' })
    expect(within(rail).getByText('1 open')).toBeInTheDocument()
    expect(within(rail).getByText('Waiting for schema-registry')).toBeInTheDocument()
    expect(within(rail).getByText('0 of 2')).toBeInTheDocument()
    expect(within(rail).queryByText(/not run/i)).not.toBeInTheDocument()

    const card = screen.getByRole('article', { name: 'Set subject compatibility to NONE' })
    expect(within(card).queryByText(/not run/i)).not.toBeInTheDocument()
    expect(screen.getByRole('region', { name: 'Decision' })).toHaveTextContent('Needs your approval · kafka-connect')
  })
})

describe('RoomScreen on a phone-width layout (no side rail)', () => {
  const original = window.matchMedia

  afterEach(() => {
    window.matchMedia = original
  })

  it('shows the Fix tab summary as "not run" on a closed room, with no layer or count', () => {
    // jsdom has no matchMedia, so useMediaQuery falls back to the wide layout; force the narrow one.
    window.matchMedia = ((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      addListener: vi.fn(),
      removeListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })) as unknown as typeof window.matchMedia

    show(room({ status: 'closed', closed_at: '2026-10-11T10:30:00Z', resolution: 'gave up' }))

    expect(screen.queryByRole('complementary', { name: 'Room details' })).not.toBeInTheDocument()
    const fix = document.querySelector('[role="tabpanel"][id$="content-fix"]') as HTMLElement
    expect(fix).not.toBeNull()
    expect(within(fix).getByText('Not run · room closed')).toBeInTheDocument()
    expect(within(fix).queryByText(/waiting for/i)).not.toBeInTheDocument()
    expect(within(fix).queryByText(/\d+ of \d+/)).not.toBeInTheDocument()
  })

  // KAN-579: the tab badge counted the proposal that KAN-560 says was never run.
  function narrow() {
    window.matchMedia = ((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      addListener: vi.fn(),
      removeListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })) as unknown as typeof window.matchMedia
  }

  it('shows no open count on the Fix tab of a closed room', () => {
    narrow()
    show(room({ status: 'closed', closed_at: '2026-10-11T10:30:00Z', resolution: 'gave up' }))

    expect(screen.getByRole('tab', { name: /^Fix/ })).toHaveTextContent(/^Fix$/)
  })

  it('still shows the open count on the Fix tab of an open room', () => {
    narrow()
    show(room())

    expect(screen.getByRole('tab', { name: /^Fix/ })).toHaveTextContent(/^Fix1$/)
  })
})
