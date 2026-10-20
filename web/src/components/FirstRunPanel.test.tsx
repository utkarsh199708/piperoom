import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
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
  useCurrentUser: () => ({ user: ctx.user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0 }),
}))
vi.mock('../hooks/useRoomList', () => ({ useRoomList: vi.fn() }))
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
  return {
    api: { createRoom: vi.fn(), grantControl: vi.fn(), authUsers: vi.fn() },
    isFixtures: () => false,
    ApiClientError,
  }
})

import { toast } from 'sonner'
import { useRoomList } from '../hooks/useRoomList'
import { api, ApiClientError } from '../api/client'
import { RoomList } from './RoomList'
import { FIRST_RUN_TITLE } from './FirstRunPanel'

function room(id: string): RoomSnapshot {
  return {
    id,
    title: id,
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
  }
}

function mockList(rooms: RoomSnapshot[]) {
  vi.mocked(useRoomList).mockReturnValue({
    rooms,
    loading: false,
    error: null,
    reload: vi.fn(),
    mode: 'push',
    updatedAt: null,
  })
}

function pipelines(data: unknown[]) {
  pipelinesState.value = { status: 'ready', reload: vi.fn(), data }
}

const renderList = () =>
  render(
    <MemoryRouter>
      <RoomList />
    </MemoryRouter>,
  )

const panel = () => screen.findByRole('region', { name: FIRST_RUN_TITLE })
const command = async () => within(await panel()).findByLabelText('Test alert command')
const origin = () => window.location.origin

beforeEach(() => {
  vi.clearAllMocks()
  ctx.user = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
  pipelines([{ id: 'orders', title: 'Orders', shadow: false, kind: 'cdc' }])
  vi.mocked(api.authUsers).mockResolvedValue([])
})

describe('FirstRunPanel on the room list', () => {
  it('shows the dev curl with this origin, the user and the first pipeline when there is no room', async () => {
    mockList([])
    renderList()
    expect((await command()).textContent).toBe(
      `curl -X POST ${origin()}/api/alerts/webhook -H 'Content-Type: application/json' \\\n` +
        `  -H 'X-User: arjun' -d '{"title":"test","pipeline_id":"orders"}'`,
    )
    expect(screen.getByText(/Opens a room on/)).toHaveTextContent('Opens a room on orders')
    // no credential of any kind: dev login has no password, oidc sessions are cookies
    expect((await command()).textContent).not.toMatch(/cookie|authorization|bearer|token|secret/i)
  })

  it('is not there once a room exists', async () => {
    mockList([room('r1')])
    renderList()
    await screen.findByRole('table', { name: 'Rooms' })
    expect(screen.queryByRole('region', { name: FIRST_RUN_TITLE })).toBeNull()
    expect(api.authUsers).not.toHaveBeenCalled()
  })

  it('goes away when the first room arrives', async () => {
    mockList([])
    const view = renderList()
    await panel()
    mockList([room('r1')])
    view.rerender(
      <MemoryRouter>
        <RoomList />
      </MemoryRouter>,
    )
    expect(screen.queryByRole('region', { name: FIRST_RUN_TITLE })).toBeNull()
  })

  it('is not shown to a viewer, who cannot post an alert', async () => {
    ctx.user = { id: 'vik', name: 'Vik', layers: [], role: 'viewer' }
    mockList([])
    renderList()
    await screen.findByText('No open rooms')
    expect(screen.queryByRole('region', { name: FIRST_RUN_TITLE })).toBeNull()
  })

  it('names dag_id for a batch pipeline', async () => {
    pipelines([{ id: 'nightly', title: 'Nightly', shadow: false, kind: 'batch' }])
    mockList([])
    renderList()
    expect((await command()).textContent).toContain(`'{"title":"test","pipeline_id":"nightly","dag_id":"<DAG_ID>"}'`)
    expect(screen.getByText(/A batch pipeline: put one of its DAG ids/)).toBeInTheDocument()
  })

  it('uses the first of several pipelines and says so', async () => {
    pipelines([
      { id: 'payments', title: '', shadow: false, kind: 'cdc' },
      { id: 'orders', title: '', shadow: false, kind: 'cdc' },
    ])
    mockList([])
    renderList()
    expect((await command()).textContent).toContain('"pipeline_id":"payments"')
    expect(screen.getByText(/the first of 2 pipelines/)).toBeInTheDocument()
  })

  it('keeps the placeholder and says how to get a pipeline when the server knows none', async () => {
    pipelines([])
    mockList([])
    renderList()
    expect((await command()).textContent).toContain('"pipeline_id":"<PIPELINE>"')
    expect(screen.getByText(/knows no pipeline yet/)).toBeInTheDocument()
  })

  it('keeps the placeholder when the pipelines cannot be loaded', async () => {
    pipelinesState.value = { status: 'error', message: 'boom', reload: vi.fn() }
    mockList([])
    renderList()
    expect((await command()).textContent).toContain('"pipeline_id":"<PIPELINE>"')
    expect(screen.getByText(/Could not load the pipelines \(boom\)/)).toBeInTheDocument()
  })

  it('shows no curl on an oidc server, where the dev webhook is not mounted', async () => {
    vi.mocked(api.authUsers).mockRejectedValue(new ApiClientError(404, 'not_found', 'not found'))
    mockList([])
    renderList()
    const region = await panel()
    await within(region).findByText(/unsigned test webhook is off/)
    expect(within(region).queryByLabelText('Test alert command')).toBeNull()
    expect(within(region).queryByRole('button', { name: /copy/i })).toBeNull()
  })

  it('keeps the dev command when the mode probe fails for another reason', async () => {
    vi.mocked(api.authUsers).mockRejectedValue(new Error('network down'))
    mockList([])
    renderList()
    expect((await command()).textContent).toContain("-H 'X-User: arjun'")
  })

  it('copies the exact command with a labelled button', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    mockList([])
    renderList()
    const text = (await command()).textContent
    fireEvent.click(screen.getByRole('button', { name: 'Copy test alert command' }))
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(text))
    expect(await screen.findByRole('button', { name: 'Copied test alert command' })).toBeInTheDocument()
    expect(toast.success).toHaveBeenCalledWith('Test alert command copied')
  })

  it('says so when copying fails', async () => {
    const writeText = vi.fn().mockRejectedValue(new Error('denied'))
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    mockList([])
    renderList()
    await command()
    fireEvent.click(screen.getByRole('button', { name: 'Copy test alert command' }))
    await waitFor(() => expect(toast.error).toHaveBeenCalledWith('Could not copy. Select the command instead.'))
    expect(screen.getByRole('button', { name: 'Copy test alert command' })).toBeInTheDocument()
  })

  it('scrolls the command inside its block on a narrow screen instead of widening the page', async () => {
    mockList([])
    renderList()
    const pre = await command()
    expect(pre.className).toContain('overflow-x-auto')
    expect(pre.className).toContain('whitespace-pre')
    expect(pre.className).toContain('min-w-0')
  })
})
