import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { Link, MemoryRouter, Route, Routes, useLocation } from 'react-router-dom'
import { useState } from 'react'
import type { RoomSnapshot } from '../api/types'

const ctx = vi.hoisted(() => ({
  user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] } as {
    id: string
    name: string
    layers: string[]
    role?: 'viewer'
  },
}))

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: ctx.user,
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
  }),
}))
vi.mock('../hooks/useRoomList', () => ({ useRoomList: vi.fn() }))
// What GET /api/pipelines answers; tests swap it to see the empty and error states.
const pipelinesState = vi.hoisted(() => ({
  value: { status: 'loading', reload: () => undefined } as Record<string, unknown>,
}))
vi.mock('../hooks/usePipelines', () => ({ usePipelines: () => pipelinesState.value }))
vi.mock('sonner', () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
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
  return { api: { createRoom: vi.fn(), grantControl: vi.fn() }, isFixtures: () => false, ApiClientError }
})

import { toast } from 'sonner'
import { useRoomList } from '../hooks/useRoomList'
import { api } from '../api/client'
import { InShellContext, RoomListContext, TopBarSlotContext } from './shell/context'
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

function proposal(approvals: Record<string, 'pending' | 'approved'>) {
  return {
    proposal_id: 'p1',
    title: 'Restart the task',
    steps: [],
    risk: 'low',
    rollback: 'none',
    required_layers: Object.keys(approvals),
    approvals,
  }
}

const reload = vi.fn()
function mockList(rooms: RoomSnapshot[], extra: Partial<ReturnType<typeof useRoomList>> = {}) {
  vi.mocked(useRoomList).mockReturnValue({ rooms, loading: false, error: null, reload, mode: 'push', updatedAt: null, ...extra })
}

function Where() {
  const l = useLocation()
  return <span data-testid="where">{l.pathname + l.search}</span>
}

function renderList(path = '/') {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/" element={<RoomList />} />
        <Route path="/rooms/:id" element={<p>a room</p>} />
      </Routes>
      <Where />
    </MemoryRouter>,
  )
}

const titlesInOrder = () =>
  within(screen.getByRole('table', { name: 'Rooms' }))
    .getAllByRole('row')
    .slice(1)
    .map((r) => r.querySelector('a')?.textContent)

beforeEach(() => {
  ctx.user = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
  pipelinesState.value = {
    status: 'ready',
    reload: vi.fn(),
    data: [
      { id: 'payments', title: 'Payments', shadow: false },
      { id: 'orders', title: '', shadow: false },
    ],
  }
  vi.clearAllMocks()
  vi.mocked(api.grantControl).mockReset()
  vi.mocked(api.createRoom).mockReset()
  mockList([])
})

describe('RoomList table', () => {
  const rooms = [
    room({ id: 'c1', title: 'Closed one', severity: 'sev4', status: 'closed', opened_at: '2026-10-11T12:00:00Z' }),
    room({ id: 'o_old', title: 'Older open', opened_at: '2026-10-11T09:00:00Z' }),
    room({
      id: 'o_new',
      title: 'Newest open',
      severity: 'sev1',
      opened_at: '2026-10-11T11:00:00Z',
      driver: { id: 'kabir', name: 'Kabir' },
    }),
  ]

  it('lists open rooms before closed (newest first) and shows severity, pipeline, opened time and driver', () => {
    mockList(rooms)
    renderList()

    // The Open tab holds the open rooms only; All adds the closed one, last.
    expect(titlesInOrder()).toEqual(['Newest open', 'Older open'])
    fireEvent.mouseDown(screen.getByRole('tab', { name: /All/ }))
    expect(titlesInOrder()).toEqual(['Newest open', 'Older open', 'Closed one'])

    const newest = screen.getByRole('row', { name: /Newest open/ })
    expect(within(newest).getByText('sev1')).toBeInTheDocument()
    expect(within(newest).getAllByText('orders').length).toBeGreaterThan(0)
    expect(within(newest).getByText(/opened/)).toBeInTheDocument()
    expect(within(newest).getByText('Kabir')).toBeInTheDocument()
    expect(within(newest).queryByRole('button', { name: /Take control/ })).not.toBeInTheDocument()

    // No driver: a member is offered Take control instead.
    const older = screen.getByRole('row', { name: /Older open/ })
    expect(within(older).getByRole('button', { name: /Take control/ })).toBeInTheDocument()
  })

  it('has the Open / Mine / Resolved / All tabs with counts', () => {
    mockList(rooms)
    renderList()
    expect(screen.getByRole('tab', { name: /Open\s*2/ })).toHaveAttribute('aria-selected', 'true')
    expect(screen.getByRole('tab', { name: /Mine\s*0/ })).toBeInTheDocument()
    fireEvent.mouseDown(screen.getByRole('tab', { name: /Resolved\s*1/ }))
    expect(titlesInOrder()).toEqual(['Closed one'])
  })

  it('shows the stat strip', () => {
    mockList([
      ...rooms,
      room({
        id: 'work',
        title: 'Working',
        agent_status: { state: 'working', detail: 'step 2' },
        driver: { id: 'priya', name: 'Priya' },
      }),
    ])
    renderList()
    const strip = screen.getByRole('group', { name: 'Summary' })
    expect(within(strip).getByText('Open').nextSibling).toHaveTextContent('3')
    expect(within(strip).getByText('Agent working').parentElement).toHaveTextContent('orders · step 2')
  })

  it('a Mine room is one I drive or one waiting on my layer', () => {
    mockList([
      room({ id: 'mine_drive', title: 'I drive', driver: { id: 'arjun', name: 'Arjun' } }),
      room({ id: 'mine_layer', title: 'My layer', open_proposals: [proposal({ 'kafka-connect': 'pending' })] }),
      room({ id: 'other', title: 'Not mine', driver: { id: 'kabir', name: 'Kabir' } }),
    ])
    renderList()
    fireEvent.mouseDown(screen.getByRole('tab', { name: /Mine/ }))
    expect(titlesInOrder().sort()).toEqual(['I drive', 'My layer'])
  })
})

describe('first-insight span', () => {
  const insight = (s: number) => ({
    first_hypothesis_at: null,
    time_to_first_hypothesis_s: s,
    final_confidence: '',
    steps: 0,
    tool_calls: 0,
    tool_errors: 0,
    usage: { calls: 0, unpriced_calls: 0, tokens: 0, cost_usd: null, latency_ms_total: 0 },
  })

  it('shows "insight 48 s" for an open room with a hypothesis, nothing for -1, closed or shadow rooms', () => {
    mockList([
      room({ id: 'with', insight: insight(48) }),
      room({ id: 'without', insight: insight(-1) }),
      room({ id: 'old' }),
      room({ id: 'done', status: 'closed', closed_at: '2026-10-11T11:00:00Z', insight: insight(30) }),
      room({ id: 'shadowed', shadow: true, insight: insight(30) }),
    ])
    renderList('/?tab=all')
    expect(screen.getAllByText(/insight \d+ s/)).toHaveLength(1)
    expect(screen.getByText(/insight 48 s/)).toBeInTheDocument()
  })
})

describe('Needs you', () => {
  it('tints and badges only the rooms waiting on a layer the person owns', () => {
    mockList([
      room({ id: 'a', title: 'Mine to approve', open_proposals: [proposal({ 'kafka-connect': 'pending' })] }),
      room({
        id: 'b',
        title: 'Someone elses layer',
        open_proposals: [proposal({ warehouse: 'pending' })],
        driver: { id: 'kabir', name: 'Kabir' },
      }),
      room({
        id: 'c',
        title: 'Already approved by me',
        open_proposals: [proposal({ 'kafka-connect': 'approved', warehouse: 'pending' })],
        driver: { id: 'kabir', name: 'Kabir' },
      }),
    ])
    renderList()
    const a = screen.getByRole('row', { name: /Mine to approve/ })
    expect(within(a).getByText('Needs you')).toBeInTheDocument()
    expect(a.className).toMatch(/bg-warning-subtle/)
    for (const name of [/Someone elses layer/, /Already approved by me/]) {
      const r = screen.getByRole('row', { name })
      expect(within(r).queryByText('Needs you')).not.toBeInTheDocument()
      expect(r.className).not.toMatch(/bg-warning-subtle/)
      // Still readable as a status: someone has to approve.
      expect(within(r).getByText(/^Needs approval/)).toBeInTheDocument()
    }
    // The stat counts the rooms that need this person, with the layer in the caption.
    const strip = screen.getByRole('group', { name: 'Summary' })
    expect(within(strip).getByText('Needs you').parentElement).toHaveTextContent('1')
    expect(within(strip).getByText('Needs you').parentElement).toHaveTextContent('kafka-connect approval')
  })

  it('a viewer is never "needed", even when they hold the layer', () => {
    ctx.user = { id: 'vera', name: 'Vera', layers: ['kafka-connect'], role: 'viewer' }
    mockList([room({ id: 'a', title: 'Mine to approve', open_proposals: [proposal({ 'kafka-connect': 'pending' })] })])
    renderList()
    expect(within(screen.getByRole('row', { name: /Mine to approve/ })).queryByText('Needs you')).not.toBeInTheDocument()
  })
})

describe('pipeline filter', () => {
  const rooms = [
    room({ id: 'o1', title: 'Orders one', pipeline_id: 'orders' }),
    room({ id: 'p1', title: 'Payments one', pipeline_id: 'payments' }),
  ]

  it('reads ?pipeline=<id> and shows only that pipeline', () => {
    mockList(rooms)
    renderList('/?pipeline=payments')
    expect(titlesInOrder()).toEqual(['Payments one'])
    expect(screen.getByRole('button', { name: /Pipeline: payments/ })).toBeInTheDocument()
  })

  it('Clear removes the filter from the URL', () => {
    mockList(rooms)
    renderList('/?pipeline=payments')
    fireEvent.click(screen.getByRole('button', { name: /Clear pipeline filter/ }))
    expect(screen.getByTestId('where')).toHaveTextContent(/^\/$/)
    expect(titlesInOrder().sort()).toEqual(['Orders one', 'Payments one'])
  })

  it('says so when the filtered pipeline has no rooms, and offers to clear it', () => {
    mockList(rooms)
    renderList('/?pipeline=billing')
    expect(screen.getByText(/No open rooms in billing/)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Clear pipeline filter' }))
    expect(titlesInOrder().length).toBe(2)
  })
})

describe('states', () => {
  it('shows the empty state with New room and a way to resolved rooms', () => {
    mockList([room({ id: 'c1', title: 'Old', status: 'closed' })])
    renderList()
    expect(screen.getByText('No open rooms')).toBeInTheDocument()
    expect(screen.getByText(/You can also open one by hand/)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'See resolved rooms' }))
    expect(titlesInOrder()).toEqual(['Old'])
  })

  it('the empty state for a viewer has no New room', () => {
    ctx.user = { id: 'vera', name: 'Vera', layers: [], role: 'viewer' }
    mockList([])
    renderList()
    expect(screen.getByText('No open rooms')).toBeInTheDocument()
    expect(screen.queryByText(/open one by hand/)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /New room/ })).not.toBeInTheDocument()
  })

  it('shows a skeleton while the first load runs', () => {
    mockList([], { loading: true })
    renderList()
    expect(screen.getByRole('status', { name: 'Loading rooms' })).toBeInTheDocument()
    expect(screen.queryByText('No open rooms')).not.toBeInTheDocument()
  })

  it('shows an error with Retry, and Retry reloads', () => {
    mockList([], { error: 'boom: 500' })
    renderList()
    const alert = screen.getByRole('alert')
    expect(alert).toHaveTextContent('Could not load rooms')
    expect(alert).toHaveTextContent('boom: 500')
    fireEvent.click(within(alert).getByRole('button', { name: 'Retry' }))
    expect(reload).toHaveBeenCalledTimes(1)
  })

  it('keeps the table when a refresh fails, with a warning and Retry', () => {
    mockList([room({ id: 'o1', title: 'Kept' })], { error: 'net down' })
    renderList()
    expect(titlesInOrder()).toEqual(['Kept'])
    expect(screen.getByRole('alert')).toHaveTextContent('Could not refresh the list: net down')
  })
})

describe('New room', () => {
  it('shows an error when creating a room fails', async () => {
    vi.mocked(api.createRoom).mockRejectedValueOnce(new Error('validation: unknown pipeline'))
    renderList()

    fireEvent.click(screen.getAllByRole('button', { name: 'New room' })[0])
    fireEvent.change(screen.getByLabelText('Title'), { target: { value: 'Broken room' } })
    fireEvent.click(screen.getByRole('button', { name: 'Open room' }))

    expect(await screen.findByText(/validation: unknown pipeline/)).toBeInTheDocument()
  })

  it('creates the room with title, severity and pipeline, then opens it', async () => {
    vi.mocked(api.createRoom).mockResolvedValueOnce(room({ id: 'r_new' }))
    mockList([room({ id: 'o1', pipeline_id: 'payments' })])
    renderList()

    fireEvent.click(screen.getByRole('button', { name: 'New room' }))
    fireEvent.change(screen.getByLabelText('Title'), { target: { value: '  Lag on payments ' } })
    fireEvent.change(screen.getByLabelText('Severity'), { target: { value: 'sev1' } })
    // The pipeline defaults to a known one.
    expect(screen.getByLabelText('Pipeline')).toHaveValue('payments')
    fireEvent.click(screen.getByRole('button', { name: 'Open room' }))

    await waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent('/rooms/r_new'))
    expect(api.createRoom).toHaveBeenCalledWith({ title: 'Lag on payments', severity: 'sev1', pipeline_id: 'payments' })
  })

  it('reopening starts fresh and takes the pipeline filter that is on screen now', () => {
    mockList([room({ id: 'o1', pipeline_id: 'orders' }), room({ id: 'p1', pipeline_id: 'payments' })])
    render(
      <MemoryRouter initialEntries={['/?pipeline=orders']}>
        <Link to="/?pipeline=payments">sidebar link</Link>
        <Routes>
          <Route path="/" element={<RoomList />} />
        </Routes>
      </MemoryRouter>,
    )
    fireEvent.click(screen.getByRole('button', { name: 'New room' }))
    expect(screen.getByLabelText('Pipeline')).toHaveValue('orders')
    fireEvent.change(screen.getByLabelText('Title'), { target: { value: 'half typed' } })
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()

    // The same screen under another filter (the sidebar link): the new dialog uses it.
    fireEvent.click(screen.getByRole('link', { name: 'sidebar link' }))
    fireEvent.click(screen.getByRole('button', { name: 'New room' }))
    expect(screen.getByLabelText('Pipeline')).toHaveValue('payments')
    expect(screen.getByLabelText('Title')).toHaveValue('')
  })

  it('offers the server\'s pipelines and nothing else', () => {
    renderList()
    fireEvent.click(screen.getAllByRole('button', { name: 'New room' })[0])
    const options = within(screen.getByLabelText('Pipeline')).getAllByRole('option')
    expect(options.map((o) => o.textContent)).toEqual(['payments · Payments', 'orders'])
  })

  it('shows the selected pipeline kind next to the pipeline picker', () => {
    pipelinesState.value = {
      status: 'ready',
      reload: vi.fn(),
      data: [
        { id: 'payments', title: 'Payments', shadow: false, kind: 'cdc' },
        { id: 'nightly', title: 'Nightly', shadow: false, kind: 'batch' },
      ],
    }
    renderList()
    fireEvent.click(screen.getAllByRole('button', { name: 'New room' })[0])
    const dialog = screen.getByRole('dialog', { name: 'New room' })
    // The first pipeline is the default; its kind is a cdc badge.
    expect(within(dialog).getByText('cdc')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Pipeline'), { target: { value: 'nightly' } })
    expect(within(dialog).getByText('batch')).toBeInTheDocument()
    expect(within(dialog).queryByText('cdc')).not.toBeInTheDocument()
  })

  it('with no pipelines it says so and cannot open a room (no made-up "orders")', () => {
    pipelinesState.value = { status: 'ready', reload: vi.fn(), data: [] }
    renderList()
    fireEvent.click(screen.getAllByRole('button', { name: 'New room' })[0])
    expect(screen.getByText(/no pipelines configured/)).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Title'), { target: { value: 'x' } })
    expect(screen.getByRole('button', { name: 'Open room' })).toBeDisabled()
    expect(api.createRoom).not.toHaveBeenCalled()
  })

  it('shows the load error with Try again', () => {
    const reloadPipelines = vi.fn()
    pipelinesState.value = { status: 'error', message: 'boom: 500', reload: reloadPipelines }
    renderList()
    fireEvent.click(screen.getAllByRole('button', { name: 'New room' })[0])
    expect(screen.getByRole('alert')).toHaveTextContent('Could not load the pipelines: boom: 500')
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }))
    expect(reloadPipelines).toHaveBeenCalledTimes(1)
    expect(screen.getByRole('button', { name: 'Open room' })).toBeDisabled()
  })

  it('does not submit an empty title', () => {
    renderList()
    fireEvent.click(screen.getAllByRole('button', { name: 'New room' })[0])
    expect(screen.getByRole('button', { name: 'Open room' })).toBeDisabled()
  })
})

describe('Take control', () => {
  it('takes control of a room nobody drives, then reloads', async () => {
    vi.mocked(api.grantControl).mockResolvedValueOnce({} as never)
    mockList([room({ id: 'nd', title: 'Nobody drives' })])
    renderList()
    fireEvent.click(screen.getByRole('button', { name: /Take control/ }))
    await waitFor(() => expect(api.grantControl).toHaveBeenCalledWith('nd', 'arjun'))
    await waitFor(() => expect(reload).toHaveBeenCalled())
    expect(toast.success).toHaveBeenCalled()
    // The click does not also open the room.
    expect(screen.getByTestId('where')).toHaveTextContent(/^\/$/)
  })

  it('shows the server error when it fails', async () => {
    vi.mocked(api.grantControl).mockRejectedValueOnce(new Error('lease_held'))
    mockList([room({ id: 'nd', title: 'Nobody drives' })])
    renderList()
    fireEvent.click(screen.getByRole('button', { name: /Take control/ }))
    await waitFor(() => expect(toast.error).toHaveBeenCalledWith('lease_held'))
  })

  it('is offered when the driver lease has expired, but not for a live lease or my own room', () => {
    mockList([
      room({
        id: 'expired',
        title: 'Expired lease',
        driver: { id: 'kabir', name: 'Kabir', lease_until: '2020-01-01T00:00:00Z' },
      }),
      room({
        id: 'live',
        title: 'Live lease',
        driver: { id: 'kabir', name: 'Kabir', lease_until: '2999-01-01T00:00:00Z' },
      }),
    ])
    renderList()
    expect(within(screen.getByRole('row', { name: /Expired lease/ })).getByRole('button', { name: /Take control/ })).toBeInTheDocument()
    const live = screen.getByRole('row', { name: /Live lease/ })
    expect(within(live).queryByRole('button')).not.toBeInTheDocument()
    expect(within(live).getByText('Kabir')).toBeInTheDocument()
  })

  it('is not offered on a room I already drive', () => {
    mockList([room({ id: 'mine', title: 'Mine already', driver: { id: 'arjun', name: 'Arjun', lease_until: '2020-01-01T00:00:00Z' } })])
    renderList()
    expect(within(screen.getByRole('row', { name: /Mine already/ })).queryByRole('button')).not.toBeInTheDocument()
  })
})

describe('keyboard', () => {
  const rooms = [
    room({ id: 'a', title: 'Alpha', opened_at: '2026-10-11T12:00:00Z', driver: { id: 'kabir', name: 'Kabir' } }),
    room({ id: 'b', title: 'Bravo', opened_at: '2026-10-11T11:00:00Z' }),
    room({ id: 'c', title: 'Charlie', opened_at: '2026-10-11T10:00:00Z', driver: { id: 'kabir', name: 'Kabir' } }),
  ]

  const activeId = () => document.querySelector('tr[data-active]')?.getAttribute('data-room-id')

  it('J and K move the row, and Enter opens it', () => {
    mockList(rooms)
    renderList()
    fireEvent.keyDown(document.body, { key: 'j' })
    expect(activeId()).toBe('a')
    fireEvent.keyDown(document.body, { key: 'j' })
    fireEvent.keyDown(document.body, { key: 'j' })
    expect(activeId()).toBe('c')
    fireEvent.keyDown(document.body, { key: 'j' }) // stays on the last row
    expect(activeId()).toBe('c')
    fireEvent.keyDown(document.body, { key: 'k' })
    expect(activeId()).toBe('b')
    // The row's link has focus, so it is the keyboard target of Enter.
    expect(document.activeElement).toBe(screen.getByRole('link', { name: 'Bravo' }))
    fireEvent.keyDown(document.body, { key: 'Enter' })
    expect(screen.getByTestId('where')).toHaveTextContent('/rooms/b')
  })

  it('T takes control of the active room when it has no driver', async () => {
    vi.mocked(api.grantControl).mockResolvedValueOnce({} as never)
    mockList(rooms)
    renderList()
    fireEvent.keyDown(document.body, { key: 'j' })
    fireEvent.keyDown(document.body, { key: 't' }) // Alpha has a driver: nothing
    expect(api.grantControl).not.toHaveBeenCalled()
    fireEvent.keyDown(document.body, { key: 'j' })
    fireEvent.keyDown(document.body, { key: 't' })
    await waitFor(() => expect(api.grantControl).toHaveBeenCalledWith('b', 'arjun'))
  })

  it('the picked row is a room, not a position: a refresh that inserts a row above does not move T', async () => {
    vi.mocked(api.grantControl).mockResolvedValue({} as never)
    mockList(rooms)
    const view = renderList()
    fireEvent.keyDown(document.body, { key: 'j' })
    fireEvent.keyDown(document.body, { key: 'j' }) // Bravo
    expect(activeId()).toBe('b')

    // The poll brings a newer room, which sorts to the top. Bravo stays the picked row.
    mockList([room({ id: 'z', title: 'Zulu', opened_at: '2026-10-11T13:00:00Z' }), ...rooms])
    view.rerender(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<RoomList />} />
        </Routes>
        <Where />
      </MemoryRouter>,
    )
    expect(titlesInOrder()[0]).toBe('Zulu')
    expect(activeId()).toBe('b')
    fireEvent.keyDown(document.body, { key: 't' })
    await waitFor(() => expect(api.grantControl).toHaveBeenCalledWith('b', 'arjun'))
    expect(api.grantControl).not.toHaveBeenCalledWith('z', 'arjun')
  })

  it('when the picked room leaves the list nothing is picked, and T does nothing', () => {
    mockList(rooms)
    const view = renderList()
    fireEvent.keyDown(document.body, { key: 'j' })
    fireEvent.keyDown(document.body, { key: 'j' })
    mockList(rooms.filter((r) => r.id !== 'b'))
    view.rerender(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<RoomList />} />
        </Routes>
        <Where />
      </MemoryRouter>,
    )
    expect(activeId()).toBeUndefined()
    fireEvent.keyDown(document.body, { key: 't' })
    expect(api.grantControl).not.toHaveBeenCalled()
  })

  it('ignores T while a take is in flight', async () => {
    vi.mocked(api.grantControl).mockReturnValue(new Promise(() => {}))
    mockList([room({ id: 'x', title: 'X' }), room({ id: 'y', title: 'Y', opened_at: '2026-10-11T09:00:00Z' })])
    renderList()
    fireEvent.keyDown(document.body, { key: 'j' })
    fireEvent.keyDown(document.body, { key: 't' })
    await waitFor(() => expect(api.grantControl).toHaveBeenCalledTimes(1))
    fireEvent.keyDown(document.body, { key: 'j' })
    fireEvent.keyDown(document.body, { key: 't' })
    expect(api.grantControl).toHaveBeenCalledTimes(1)
  })

  it('C opens the new-room dialog', () => {
    renderList()
    fireEvent.keyDown(document.body, { key: 'c' })
    expect(screen.getByRole('dialog', { name: 'New room' })).toBeInTheDocument()
  })

  it('does nothing inside a text field, with a modifier, or while a dialog is open', () => {
    mockList(rooms)
    renderList()
    const field = document.createElement('input')
    document.body.appendChild(field)
    fireEvent.keyDown(field, { key: 'j' })
    fireEvent.keyDown(field, { key: 'c' })
    expect(activeId()).toBeUndefined()
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    field.remove()

    fireEvent.keyDown(document.body, { key: 'j', ctrlKey: true })
    fireEvent.keyDown(document.body, { key: 'c', metaKey: true })
    expect(activeId()).toBeUndefined()
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()

    fireEvent.keyDown(document.body, { key: 'c' })
    const dialog = screen.getByRole('dialog', { name: 'New room' })
    fireEvent.keyDown(dialog, { key: 'j' })
    expect(activeId()).toBeUndefined()
  })
})

describe('refresh', () => {
  // F6: the list hook keeps the rows current (push, or its own poll fallback); the screen
  // no longer runs a timer or reloads on visibility.
  it('the screen itself does not poll or reload on visibility', () => {
    vi.useFakeTimers()
    try {
      renderList()
      document.dispatchEvent(new Event('visibilitychange'))
      vi.advanceTimersByTime(60_000)
      expect(reload).not.toHaveBeenCalled()
    } finally {
      vi.useRealTimers()
    }
  })

  it('in the shell it reads the shell list and does not load its own', () => {
    const shellReload = vi.fn()
    mockList([room({ id: 'own', title: 'From the hook' })])
    render(
      <MemoryRouter>
        <RoomListContext.Provider
          value={{ rooms: [room({ id: 'sh', title: 'From the shell' })], loading: false, error: null, reload: shellReload }}
        >
          <RoomList />
        </RoomListContext.Provider>
      </MemoryRouter>,
    )
    expect(titlesInOrder()).toEqual(['From the shell'])
    expect(vi.mocked(useRoomList)).toHaveBeenCalledWith({ enabled: false })
    expect(reload).not.toHaveBeenCalled()
  })

  it('a polled list shows how old it is; a pushed one just says live', () => {
    mockList([], { mode: 'poll', updatedAt: Date.now() - 5_000 })
    const { unmount } = renderList()
    expect(screen.getByText(/^live · updated/)).toBeInTheDocument()
    unmount()
    mockList([], { mode: 'push', updatedAt: null })
    renderList()
    expect(screen.getByText(/^live$/)).toBeInTheDocument()
  })
})

describe('top bar', () => {
  function InShell({ children }: { children: React.ReactNode }) {
    const [el, setEl] = useState<HTMLElement | null>(null)
    return (
      <InShellContext.Provider value>
        <TopBarSlotContext.Provider value={el}>
          <div data-testid="slot" ref={setEl} />
          {children}
        </TopBarSlotContext.Provider>
      </InShellContext.Provider>
    )
  }

  it('in the shell the live pill and New room go to the top bar, and the list draws no header', () => {
    mockList([])
    render(
      <MemoryRouter>
        <InShell>
          <RoomList />
        </InShell>
      </MemoryRouter>,
    )
    const slot = screen.getByTestId('slot')
    expect(within(slot).getByRole('button', { name: 'New room' })).toBeInTheDocument()
    expect(within(slot).getByText(/^live/)).toBeInTheDocument()
    expect(screen.queryByRole('heading', { level: 1 })).not.toBeInTheDocument()
  })

  it('outside the shell the list draws its own title and controls', () => {
    renderList()
    expect(screen.getByRole('heading', { level: 1, name: 'Rooms' })).toBeInTheDocument()
    expect(screen.getByText(/^live/)).toBeInTheDocument()
  })
})

describe('driver column verification', () => {
  it('draws the server badge, never one from the name, and shows an unverified driver’s id', () => {
    mockList([
      room({ id: 'a', title: 'Verified driver', driver: { id: 'kabir', name: 'Kabir', verified: true } }),
      room({ id: 'b', title: 'Spoofed driver', driver: { id: 'u_9', name: 'Kabir ✓ verified' } }),
    ])
    renderList()
    const badge = (row: HTMLElement) =>
      Array.from(row.querySelectorAll<HTMLElement>('[data-slot="verified-badge"]')).map((b) => b.dataset.verification)
    const a = screen.getByRole('row', { name: /Verified driver/ })
    expect(badge(a)).toEqual(['verified'])
    const b = screen.getByRole('row', { name: /Spoofed driver/ })
    expect(badge(b)).toEqual(['unverified'])
    expect(b).toHaveTextContent('(u_9)')
    expect(b).not.toHaveTextContent('✓')
  })
})

describe('New room — Ctrl+Enter', () => {
  it('submits the form from the keyboard like the close and hand-off dialogs', async () => {
    vi.mocked(api.createRoom).mockResolvedValueOnce(room({ id: 'r_kbd' }))
    renderList()
    fireEvent.click(screen.getAllByRole('button', { name: 'New room' })[0])
    const title = screen.getByLabelText('Title')
    fireEvent.change(title, { target: { value: 'From the keyboard' } })
    fireEvent.keyDown(title, { key: 'Enter', ctrlKey: true })
    await waitFor(() => expect(api.createRoom).toHaveBeenCalledTimes(1))
  })

  it('shows the shortcut on the button', () => {
    renderList()
    fireEvent.click(screen.getAllByRole('button', { name: 'New room' })[0])
    expect(screen.getByRole('button', { name: 'Open room' })).toHaveAttribute('aria-keyshortcuts')
  })
})

describe('resolved rows', () => {
  it('read Resolved · duration, no driver, agent resolved, closed ago with a UTC title, and the resolution', () => {
    mockList([
      room({
        id: 'c1',
        title: 'Old one',
        status: 'closed',
        opened_at: '2026-10-11T10:00:00Z',
        closed_at: '2026-10-11T10:06:00Z',
        resolution: 'Restarted the connector task after the schema fix.',
      }),
    ])
    renderList()
    fireEvent.click(screen.getByRole('button', { name: 'See resolved rooms' }))
    const row = screen.getByText('Old one').closest('tr')!
    expect(within(row).getByText('Resolved · 6 min')).toBeInTheDocument()
    expect(within(row).getByText('resolved')).toBeInTheDocument()
    expect(within(row).getByText('—')).toBeInTheDocument()
    expect(within(row).getByText(/^closed .* ago$/)).toHaveAttribute('title', '2026-10-11 10:06:00 UTC')
    expect(within(row).getByText('Restarted the connector task after the schema fix.')).toBeInTheDocument()
  })
})
