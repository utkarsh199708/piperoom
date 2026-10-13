import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import type { RoomState } from '../state/types'
import { emptyRoomState } from '../state/types'

const ctx = vi.hoisted(() => ({
  user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] } as CurrentUser,
}))

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: ctx.user,
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
    busy: false,
  }),
}))
vi.mock('../hooks/useRoom', () => ({ useRoom: vi.fn() }))
vi.mock('../hooks/useTopology', () => ({ useTopology: vi.fn() }))
vi.mock('../hooks/usePeople', async (importOriginal) => {
  const real = await importOriginal<typeof import('../hooks/usePeople')>()
  return {
    ...real,
    usePeople: () => ({
      status: 'ready' as const,
      people: [
        { id: 'kabir', name: 'Kabir' },
        { id: 'priya', name: 'Priya' },
      ],
      reload: vi.fn(),
    }),
  }
})

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

const member: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'], role: 'member' }
const viewer: CurrentUser = { id: 'vera', name: 'Vera', layers: ['kafka-connect'], role: 'viewer' }

function room(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r1'),
    title: 'Orders stale',
    severity: 'sev2',
    pipeline_id: 'orders',
    opened_at: '2026-10-11T10:05:00Z',
    driver: { id: 'kabir', name: 'Kabir' },
    ...over,
  }
}

function pendingProposal() {
  return {
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
}

function show(state: RoomState) {
  vi.mocked(useRoom).mockReturnValue({
    state,
    presence: [],
    conn: 'live',
    error: null,
    actions,
    stale: false,
    dismissStale: vi.fn(),
    reload: vi.fn(),
  })
  vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null })
  return render(
    <MemoryRouter initialEntries={['/rooms/r1']}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
      </Routes>
    </MemoryRouter>,
  )
}

function todo(): HTMLElement {
  return screen.getByRole('region', { name: 'What you need to do' })
}

beforeEach(() => {
  vi.resetAllMocks()
  ctx.user = member
})

describe('RoomScreen — "What you need to do" card, one per predicate', () => {
  it('a pending approval of mine says Approve or reject and offers Go to proposal', () => {
    show(room({ proposals: [pendingProposal()] }))
    const card = todo()
    expect(card).toHaveTextContent('Approve or reject the kafka-connect step.')
    expect(card).toHaveAttribute('data-todo', 'approve')
    expect(within(card).getByRole('button', { name: 'Go to proposal' })).toBeInTheDocument()
  })

  it('a hand-off to me says who is handing over and Accept calls acceptHandoff', () => {
    show(
      room({
        handoffs: [
          {
            handoff_id: 'h1',
            to: { kind: 'human', id: 'arjun', name: 'Arjun' },
            summary: { what_broke: '', why: '', done: [], ruled_out: [], open: [], risks: [] },
            stay_as_watcher: false,
            status: 'pending',
          },
        ],
      }),
    )
    const card = todo()
    expect(card).toHaveTextContent('Kabir is handing the room to you.')
    fireEvent.click(within(card).getByRole('button', { name: 'Accept' }))
    expect(actions.acceptHandoff).toHaveBeenCalledWith('h1')
  })

  it('no driver says take control and the button grants control to me', () => {
    show(room({ driver: null }))
    const card = todo()
    expect(card).toHaveTextContent('No one is driving')
    fireEvent.click(within(card).getByRole('button', { name: 'Take control' }))
    expect(actions.grantControl).toHaveBeenCalledWith('arjun')
  })

  it('the driver is told "You are driving" with the lease', () => {
    const lease = new Date(Date.now() + 24 * 60_000 + 10_000).toISOString()
    show(room({ driver: { id: 'arjun', name: 'Arjun', lease_until: lease } }))
    const card = todo()
    expect(card).toHaveTextContent('You are driving')
    expect(card).toHaveTextContent('lease 24m')
  })

  it('a viewer is told "You are viewing", even with a pending layer of their own', () => {
    ctx.user = viewer
    show(room({ proposals: [pendingProposal()] }))
    const card = todo()
    expect(card).toHaveTextContent('You are viewing')
    expect(card).toHaveAttribute('data-todo', 'viewer')
    expect(within(card).queryAllByRole('button')).toHaveLength(0)
  })

  it('otherwise it is quiet and names the driver', () => {
    show(room())
    const card = todo()
    expect(card).toHaveAttribute('data-todo', 'quiet')
    expect(card).toHaveTextContent('Nothing right now — Kabir is driving.')
    expect(card).toHaveTextContent('You can request control.')
  })

  it('a closed room reads Resolved with the resolution', () => {
    show(room({ status: 'closed', resolution: 'Compatibility set to NONE' }))
    expect(todo()).toHaveTextContent('Resolved · Compatibility set to NONE')
  })
})

describe('RoomScreen — "What is happening" card', () => {
  it('shows the hypothesis with confidence, confirmed and the steps it is based on', () => {
    show(
      room({
        steps: [
          { id: 's_a', title: 'First', tool_calls: [], messages: [] },
          { id: 's_b', title: 'Second', tool_calls: [], messages: [] },
        ],
        hypothesis: {
          text: 'A schema change broke the connector.',
          based_on_steps: ['s_b'],
          confidence: 'high',
          confirmed: true,
        },
      }),
    )
    const card = screen.getByRole('region', { name: 'What is happening' })
    expect(card).toHaveTextContent('A schema change broke the connector.')
    expect(card).toHaveTextContent('high confidence')
    expect(card).toHaveTextContent('confirmed')
    expect(within(card).getByRole('button', { name: '#2' })).toBeInTheDocument()
  })

  it('without a hypothesis it names the step the agent is on', () => {
    show(
      room({
        agent_status: { state: 'working', detail: 'step 1' },
        steps: [{ id: 's_a', title: 'Check the DDL log', tool_calls: [], messages: [] }],
      }),
    )
    expect(screen.getByRole('region', { name: 'What is happening' })).toHaveTextContent(
      'Working on: Check the DDL log…',
    )
  })

  it('before anything happens it says the agent has not started', () => {
    show(room())
    expect(screen.getByRole('region', { name: 'What is happening' })).toHaveTextContent(
      'The agent has not started.',
    )
  })
})

describe('RoomScreen — topbar status pill', () => {
  it('is a polite status region with the working label and step', () => {
    show(room({ agent_status: { state: 'working', detail: 'step 5' } }))
    const pill = screen.getByRole('status')
    expect(pill).toHaveTextContent('working · step 5')
    expect(pill).toHaveAttribute('aria-live', 'polite')
    expect(pill).toHaveClass('status-pill')
  })
})

describe('RoomScreen — layout', () => {
  it('puts Proposals first in the main column, before the Investigation', () => {
    show(room({ proposals: [pendingProposal()] }))
    const main = document.querySelector('.room-col--main') as HTMLElement
    const kids = Array.from(main.children)
    expect(kids[0]).toHaveAttribute('id', 'proposals')
    expect(kids[1]).toHaveClass('investigation')
    expect(kids[2]).toHaveClass('steer')
    expect(document.querySelector('.room-col--side .presence')).not.toBeNull()
  })

  it('titles the investigation with the step count', () => {
    show(room({ steps: [{ id: 's1', title: 'Check', tool_calls: [], messages: [] }] }))
    expect(screen.getByRole('heading', { name: /Investigation/ })).toHaveTextContent('1 step')
  })

  it('shows an error card with a way back when the room cannot load', () => {
    vi.mocked(useRoom).mockReturnValue({
      state: null,
      presence: [],
      conn: 'error',
      error: 'boom',
      actions,
      stale: false,
      dismissStale: vi.fn(),
      reload: vi.fn(),
    })
    vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null })
    render(
      <MemoryRouter initialEntries={['/rooms/r1']}>
        <Routes>
          <Route path="/rooms/:roomId" element={<RoomScreen />} />
        </Routes>
      </MemoryRouter>,
    )
    expect(screen.getByRole('alert')).toHaveTextContent('Could not load room: boom')
  })
})

describe('RoomScreen — hand-off dialog focus', () => {
  it('Esc closes the dialog and focus goes back to Hand off', () => {
    show(room({ driver: { id: 'arjun', name: 'Arjun' } }))
    const opener = screen.getByRole('button', { name: 'Hand off' })
    act(() => opener.focus())
    fireEvent.click(opener)

    const dialog = screen.getByRole('dialog', { name: 'Hand off the room' })
    // First field is focused on open.
    expect(dialog.contains(document.activeElement)).toBe(true)
    expect(document.activeElement).toHaveAttribute('id', 'handoff-to')

    fireEvent.keyDown(document.activeElement as Element, { key: 'Escape' })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(opener).toHaveFocus()
  })

  it('the close button also returns focus, even when the click did not focus the opener', () => {
    show(room({ driver: { id: 'arjun', name: 'Arjun' } }))
    const opener = screen.getByRole('button', { name: 'Hand off' })
    fireEvent.click(opener)
    fireEvent.click(screen.getByRole('button', { name: 'Close hand-off dialog' }))
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(opener).toHaveFocus()
  })
})
