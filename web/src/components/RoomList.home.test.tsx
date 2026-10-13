// Home page states and the "needs you" treatment (UI refresh §7.2, §7.13).
// Separate from RoomList.test.tsx because these tests switch the signed-in user per test.
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, within, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import type { ProposalSummary, RoomSnapshot } from '../api/types'
import type { CurrentUser } from '../state/permissions'
import fixtureRooms from '../fixtures/rooms.json'

const ctx = vi.hoisted(() => ({
  user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] } as CurrentUser,
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
vi.mock('../hooks/useRoomList', () => ({ useRoomList: vi.fn() }))
vi.mock('../api/client', () => ({ api: { createRoom: vi.fn() }, isFixtures: () => false }))

import { useRoomList } from '../hooks/useRoomList'
import { RoomList } from './RoomList'

const member: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'], role: 'member' }
const viewer: CurrentUser = { id: 'vera', name: 'Vera', layers: ['kafka-connect'], role: 'viewer' }

function room(over: Partial<RoomSnapshot> & { id: string }): RoomSnapshot {
  return {
    title: over.id,
    severity: 'sev3',
    pipeline_id: 'orders',
    status: 'open',
    opened_at: '2026-10-11T10:00:00Z',
    last_seq: 0,
    driver: { id: 'kabir', name: 'Kabir' },
    agent_status: { state: 'idle' },
    hypothesis: null,
    open_proposals: [],
    pending_suggestions: [],
    members: [],
    ...over,
  }
}

const pendingProposal: ProposalSummary = {
  proposal_id: 'p1',
  title: 'Restart the connector',
  steps: [],
  risk: 'low',
  rollback: 'none',
  required_layers: ['kafka-connect', 'schema-registry'],
  approvals: { 'kafka-connect': 'pending', 'schema-registry': 'pending' },
}

function show(rooms: RoomSnapshot[], extra: Partial<ReturnType<typeof useRoomList>> = {}) {
  vi.mocked(useRoomList).mockReturnValue({
    rooms,
    loading: false,
    error: null,
    reload: vi.fn(),
    ...extra,
  })
  return render(
    <MemoryRouter>
      <RoomList />
    </MemoryRouter>,
  )
}

function stat(label: string): HTMLElement {
  const summary = screen.getByRole('group', { name: 'Summary' })
  const l = within(summary).getByText(label)
  return l.closest('.stat') as HTMLElement
}

beforeEach(() => {
  ctx.user = member
})

describe('RoomList stat strip', () => {
  it('counts open rooms, rooms with no driver and rooms that need me from the room list', () => {
    show(fixtureRooms as unknown as RoomSnapshot[])
    expect(within(stat('Open rooms')).getByText('2')).toBeInTheDocument()
    expect(within(stat('No driver')).getByText('1')).toBeInTheDocument()
    expect(within(stat('Needs you')).getByText('0')).toBeInTheDocument()
    expect(within(stat('Agent working')).getByText('0')).toBeInTheDocument()
    // The "no driver" tile is warn-tinted only while it is above zero; "needs you" stays plain at 0.
    expect(stat('No driver')).toHaveClass('stat--warn')
    expect(stat('Needs you')).not.toHaveClass('stat--accent')
  })

  it('tints "Needs you" and counts a working agent when there is something to show', () => {
    show([
      room({ id: 'a', open_proposals: [pendingProposal], agent_status: { state: 'waiting_approval' } }),
      room({ id: 'b', agent_status: { state: 'working', detail: 'step 3' } }),
    ])
    expect(within(stat('Needs you')).getByText('1')).toBeInTheDocument()
    expect(within(stat('Needs you')).getByText('kafka-connect')).toBeInTheDocument()
    expect(stat('Needs you')).toHaveClass('stat--accent')
    expect(within(stat('Agent working')).getByText('2')).toBeInTheDocument()
  })

  it('is hidden when there are no rooms', () => {
    show([])
    expect(screen.queryByRole('group', { name: 'Summary' })).not.toBeInTheDocument()
  })

  it('shows a viewer no "Needs you" tile and no new-room form', () => {
    ctx.user = viewer
    show([room({ id: 'a', open_proposals: [pendingProposal] })])
    expect(screen.queryByText('Needs you')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Title')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Open room' })).not.toBeInTheDocument()
    expect(screen.getByText('Open rooms')).toBeInTheDocument()
  })
})

describe('RoomList rows', () => {
  it('marks a room with a pending layer I own as mine, with the layer in the tag', () => {
    const { container } = show([room({ id: 'a', title: 'Needs approval', open_proposals: [pendingProposal] })])
    const card = container.querySelector('.room-card') as HTMLElement
    expect(card).toHaveClass('room-card--mine')
    expect(within(card).getByText('Needs you · kafka-connect')).toBeInTheDocument()
  })

  it('does not mark a room mine when the pending layer belongs to someone else', () => {
    ctx.user = { ...member, layers: ['warehouse'] }
    const { container } = show([room({ id: 'a', open_proposals: [pendingProposal] })])
    expect(container.querySelector('.room-card')).not.toHaveClass('room-card--mine')
  })

  it('never marks a room mine for a viewer, even with a pending layer and no driver', () => {
    ctx.user = viewer
    const { container } = show([room({ id: 'a', driver: null, open_proposals: [pendingProposal] })])
    const card = container.querySelector('.room-card') as HTMLElement
    expect(card).not.toHaveClass('room-card--mine')
    expect(within(card).queryByText(/Needs you/)).not.toBeInTheDocument()
    expect(within(card).queryByText('No driver')).not.toBeInTheDocument()
  })

  it('marks an open room without a driver as mine for a member', () => {
    const { container } = show([room({ id: 'a', driver: null })])
    const card = container.querySelector('.room-card') as HTMLElement
    expect(card).toHaveClass('room-card--mine')
    expect(within(card).getByText('No driver')).toBeInTheDocument()
  })

  it('gives each row a severity rail class, a live badge and the driver-first people stack', () => {
    const { container } = show([
      room({
        id: 'a',
        severity: 'sev1',
        agent_status: { state: 'working', detail: 'step 3' },
        members: [
          { id: 'arjun', name: 'Arjun', role: 'watching' },
          { id: 'kabir', name: 'Kabir', role: 'driver' },
          { id: 'priya', name: 'Priya', role: 'watching' },
        ],
      }),
    ])
    const card = container.querySelector('.room-card') as HTMLElement
    expect(card).toHaveClass('room-card--sev1')
    expect(card.querySelector('.room-card__live--working')).toHaveTextContent('working · step 3')
    const avatars = Array.from(card.querySelectorAll('.room-card__people .avatar'))
    expect(avatars.map((a) => a.textContent)).toEqual(['KA', 'AR', 'PR'])
    expect(avatars[0]).toHaveClass('avatar--driver')
    expect(avatars[1]).not.toHaveClass('avatar--driver')
  })

  it('shows "needs approval" in amber while the agent waits for an approval', () => {
    const { container } = show([room({ id: 'a', agent_status: { state: 'waiting_approval' } })])
    expect(container.querySelector('.room-card__live--attn')).toHaveTextContent('needs approval')
  })

  it('dims closed rooms, shows the resolution and never marks them mine', () => {
    const { container } = show([
      room({ id: 'c', status: 'closed', driver: null, resolution: 'Restarted the consumer', agent_status: { state: 'done' } }),
    ])
    const card = container.querySelector('.room-card') as HTMLElement
    expect(card).toHaveClass('room-card--closed')
    expect(card).not.toHaveClass('room-card--mine')
    expect(within(card).getByText(/Restarted the consumer/)).toBeInTheDocument()
    expect(card.querySelector('.room-card__live--done')).toHaveTextContent('resolved')
  })

  it('renders the page head with the open and closed counts', () => {
    show(fixtureRooms as unknown as RoomSnapshot[])
    expect(screen.getByRole('heading', { level: 1, name: 'Rooms' })).toBeInTheDocument()
    expect(screen.getByText('2 open · 1 closed')).toBeInTheDocument()
  })
})

describe('RoomList states', () => {
  it('shows three skeleton rows while the first load runs', () => {
    const { container } = show([], { loading: true })
    expect(container.querySelectorAll('.room-skeletons .skeleton')).toHaveLength(3)
    expect(screen.getByText('Loading rooms…')).toBeInTheDocument()
    expect(container.querySelector('.group')).toHaveAttribute('aria-busy', 'true')
  })

  it('shows the error strip with a Retry that reloads the list', () => {
    const reload = vi.fn()
    show([], { error: 'network down', reload })
    const alert = screen.getByRole('alert')
    expect(alert).toHaveClass('banner--error')
    expect(within(alert).getByText('network down')).toBeInTheDocument()
    fireEvent.click(within(alert).getByRole('button', { name: 'Retry' }))
    expect(reload).toHaveBeenCalledTimes(1)
  })

  it('shows the empty state, pointing a member to the form and a viewer to alerts', () => {
    const first = show([])
    expect(screen.getByText('No open rooms')).toBeInTheDocument()
    expect(screen.getByText('Rooms open from alerts or from the form above.')).toBeInTheDocument()
    first.unmount()

    ctx.user = viewer
    show([])
    expect(screen.getByText('Rooms open from alerts.')).toBeInTheDocument()
  })
})
