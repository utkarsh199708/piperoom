import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
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
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve: vi.fn(),
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  trace: vi.fn(),
}
const reload = vi.fn()

const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['consumer'], role: 'member' }
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'], role: 'member' }
const vera: CurrentUser = { id: 'vera', name: 'Vera', layers: [], role: 'viewer' }

function room(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r_schema_change'),
    title: 'Warehouse orders stale',
    severity: 'sev2',
    pipeline_id: 'orders',
    opened_at: '2026-10-11T10:00:00Z',
    driver: { id: 'kabir', name: 'Kabir', lease_until: new Date(Date.now() + 3_600_000).toISOString() },
    agent_status: { state: 'working', detail: 'step 2' },
    steps: [
      { id: 's1', title: 'Check the connector', tool_calls: [], messages: [] },
      { id: 's2', title: 'Check the DDL log', tool_calls: [], messages: [] },
    ],
    members: [
      { id: 'kabir', name: 'Kabir', role: 'driver' },
      { id: 'arjun', name: 'Arjun', role: 'watching' },
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
    stale: false,
    dismissStale: vi.fn(),
    reload,
  })
  vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null })
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
})

describe('RoomScreen header', () => {
  it('shows title, severity, a mono facts line and a polite status pill', () => {
    show(arjun, room())
    expect(screen.getByRole('heading', { level: 1, name: 'Warehouse orders stale' })).toBeInTheDocument()
    expect(screen.getByText('sev2')).toBeInTheDocument()
    expect(screen.getByText('#r_schema_change')).toBeInTheDocument()
    // The facts line carries the pipeline, the id and when it opened.
    const facts = screen.getByText('#r_schema_change').closest('p') as HTMLElement
    expect(facts).toHaveTextContent('orders')
    expect(facts).toHaveTextContent(/opened/)
    const pill = screen.getByText('Agent working · step 2').closest('[aria-live]')
    expect(pill).toHaveAttribute('aria-live', 'polite')
  })

  it('shows the people stack with the driver and "Kabir is driving"', () => {
    show(arjun, room())
    const people = screen.getByRole('list', { name: 'People in the room' })
    expect(within(people).getByRole('img', { name: 'Kabir · driving' })).toBeInTheDocument()
    expect(screen.getByText('Kabir is driving')).toBeInTheDocument()
    expect(screen.getByText('2 here')).toBeInTheDocument()
  })

  it('the driver sees Hand off with its key; others do not', () => {
    show(kabir, room())
    const btn = screen.getByRole('button', { name: 'Hand off' })
    expect(btn).toHaveAttribute('aria-keyshortcuts', 'H')
  })

  it('others get no Hand off button', () => {
    show(arjun, room())
    expect(screen.queryByRole('button', { name: 'Hand off' })).not.toBeInTheDocument()
  })

  it('Invite copies the link', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    show(arjun, room())
    fireEvent.click(screen.getByRole('button', { name: 'Invite (copy link)' }))
    expect(writeText).toHaveBeenCalledTimes(1)
    expect(await screen.findByRole('button', { name: 'Copied' })).toBeInTheDocument()
  })
})

describe('RoomScreen rail and column', () => {
  it('has a Proposals summary, Who’s here and Activity in the rail', () => {
    show(arjun, room())
    const rail = screen.getByRole('complementary', { name: 'Room details' })
    expect(within(rail).getByRole('heading', { name: /Proposals/ })).toBeInTheDocument()
    expect(within(rail).getByText(/No fix proposed yet/)).toBeInTheDocument()
    expect(within(rail).getByRole('heading', { name: /here/ })).toBeInTheDocument()
    expect(within(rail).getByRole('heading', { name: 'Activity' })).toBeInTheDocument()
  })

  it('puts the proposal in the column and links the rail summary to it', () => {
    const proposal = {
      proposal_id: 'p1',
      title: 'Restart the task',
      steps: [],
      risk: 'low',
      rollback: 'none',
      required_layers: ['kafka-connect'],
      approvals: { 'kafka-connect': 'pending' as const },
      decisions: {},
      params_hash: 'abc123abc123',
      status: 'open' as const,
      executed: [],
    }
    show(arjun, room({ proposals: [proposal] }))
    const anchor = document.getElementById('proposal') as HTMLElement
    expect(anchor).not.toBeNull()
    expect(within(anchor).getByText('Restart the task')).toBeInTheDocument()
    const rail = screen.getByRole('complementary', { name: 'Room details' })
    expect(rail.contains(anchor)).toBe(false)
    expect(within(rail).getByText('Waiting for kafka-connect')).toBeInTheDocument()
  })

  it('shows the hypothesis with a confidence badge and step numbers', () => {
    show(
      arjun,
      room({
        hypothesis: {
          text: 'A schema change broke the connector.',
          based_on_steps: ['s2', 's1'],
          confidence: 'high',
          confirmed: false,
        },
      }),
    )
    expect(screen.getByText('high confidence')).toBeInTheDocument()
    expect(screen.getByText('A schema change broke the connector.')).toBeInTheDocument()
    expect(screen.getByText('based on steps 2, 1')).toBeInTheDocument()
  })

  it('the viewer view is read-only: no composer, Hand off, Trace or Take control', () => {
    show(vera, room({ driver: null }))
    expect(screen.getByText('View only')).toBeInTheDocument()
    expect(screen.queryByLabelText('Steer the agent')).not.toBeInTheDocument()
    for (const name of ['Hand off', 'Trace one record', 'Take control', 'Take control and steer', 'Request control']) {
      expect(screen.queryByRole('button', { name })).not.toBeInTheDocument()
    }
    expect(screen.getByText(/you can read this room but not steer it/)).toBeInTheDocument()
  })

  it('a room with no driver shows the banner, the idle pill and a disabled composer', () => {
    show(arjun, room({ driver: null, steps: [], agent_status: { state: 'idle' } }))
    expect(screen.getByText('Agent idle · waiting for a driver')).toBeInTheDocument()
    expect(screen.getByLabelText('Steer the agent')).toBeDisabled()
    expect(screen.getByText('The agent has not started yet.')).toBeInTheDocument()
    // Banner, plus the empty state's own button.
    fireEvent.click(screen.getByRole('button', { name: 'Take control and steer' }))
    expect(actions.grantControl).toHaveBeenCalledWith('arjun')
  })
})

describe('RoomScreen loading and error', () => {
  it('shows the skeleton while the room loads', () => {
    const { container } = show(arjun, null, 'loading')
    expect(container.querySelector('[aria-busy="true"]')).not.toBeNull()
    expect(screen.getByRole('status')).toHaveTextContent('Loading room…')
    expect(screen.queryByLabelText('Steer the agent')).not.toBeInTheDocument()
  })

  it('shows an error state with Try again and Back to rooms', () => {
    show(arjun, null, 'error', 'boom')
    expect(screen.getByText('Could not load room: boom')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }))
    expect(reload).toHaveBeenCalledTimes(1)
    fireEvent.click(screen.getByRole('link', { name: 'Back to rooms' }))
    expect(screen.getByText('rooms home')).toBeInTheDocument()
  })
})

describe('RoomScreen keys', () => {
  const press = (key: string, init: KeyboardEventInit = {}) => fireEvent.keyDown(window, { key, ...init })

  it('H opens the hand-off dialog for the driver', () => {
    show(kabir, room())
    press('h')
    expect(screen.getByRole('dialog', { name: 'Hand off the room' })).toBeInTheDocument()
  })

  it('H does nothing for someone who cannot hand off', () => {
    show(arjun, room())
    press('h')
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })

  it('Shift+T opens the trace dialog; a viewer gets nothing', () => {
    const { unmount } = show(arjun, room())
    press('T', { shiftKey: true })
    expect(screen.getByRole('dialog', { name: 'Trace a record' })).toBeInTheDocument()
    unmount()
    show(vera, room())
    press('T', { shiftKey: true })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })

  it('T takes control when there is no driver, and requests it otherwise', () => {
    const { unmount } = show(arjun, room({ driver: null }))
    press('t')
    expect(actions.grantControl).toHaveBeenCalledWith('arjun')
    unmount()
    show(arjun, room())
    press('t')
    expect(actions.requestControl).toHaveBeenCalledTimes(1)
  })

  it('keys are ignored while typing and with a modifier held', () => {
    show(kabir, room())
    const box = screen.getByLabelText('Steer the agent')
    fireEvent.keyDown(box, { key: 'h' })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    press('h', { metaKey: true })
    press('h', { ctrlKey: true })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })

  it('Esc closes the hand-off dialog', () => {
    show(kabir, room())
    press('h')
    fireEvent.keyDown(document.activeElement ?? document.body, { key: 'Escape' })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })
})
