import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import type { RoomSnapshot } from '../api/types'

// #536 (review of 3d17a65): the real UserProvider and the real useRoom. Between dev-login setting
// the new cookie and GET /api/me answering, the previous user's send ledger must post nothing:
// not on a reconnect, not by the live retry, not by a Retry of the kept action.

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }) }))
vi.mock('../lib/env', () => ({ isDev: true }))
vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    isFixtures: () => false,
    probeSession: vi.fn(),
    openRoomWs: vi.fn(),
    api: { ...actual.api, me: vi.fn(), devLogin: vi.fn(), room: vi.fn(), events: vi.fn() },
  }
})

import { api, openRoomWs, probeSession } from '../api/client'
import { useRoom } from '../hooks/useRoom'
import type { RoomActions } from '../hooks/useRoom'
import { LIVE_RETRY_MS } from '../hooks/sendLedger'
import { UserProvider } from './UserContext'
import { useCurrentUser } from './user'

class FakeWebSocket {
  static CONNECTING = 0
  static OPEN = 1
  readyState = FakeWebSocket.OPEN
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: ((ev: { reason: string }) => void) | null = null
  onerror: (() => void) | null = null
  send() {}
  close() {}
}

const snap: RoomSnapshot = {
  id: 'r1',
  title: 'T',
  severity: 'sev2',
  pipeline_id: 'orders',
  status: 'open',
  opened_at: '2026-10-11T10:00:00Z',
  last_seq: 0,
  driver: { id: 'arjun', name: 'Arjun' },
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [],
  pending_suggestions: [],
  members: [],
}

let actionsNow: RoomActions | null = null

function Room() {
  const { user, setUserId } = useCurrentUser()
  const { actions } = useRoom('r1')
  actionsNow = actions
  return (
    <div>
      <span data-testid="who">{user.id}</span>
      <button onClick={() => setUserId('priya')}>as priya</button>
    </div>
  )
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
  vi.useFakeTimers({ shouldAdvanceTime: true })
  vi.mocked(probeSession).mockResolvedValue({ valid: false })
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

async function pass(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

describe('dev user switch: the window before /api/me answers (#536)', () => {
  it("posts nothing of the previous user's on a reconnect, the live retry or a Retry", async () => {
    let cookie = 'arjun'
    let offline = true
    const posts: { as: string; body: string }[] = []
    vi.stubGlobal(
      'fetch',
      vi.fn((_path: string, init: RequestInit) => {
        posts.push({ as: cookie, body: String(init.body) })
        if (offline) return Promise.reject(new TypeError('Failed to fetch'))
        return Promise.resolve(new Response(JSON.stringify({}), { status: 200 }))
      }),
    )
    const sockets: FakeWebSocket[] = []
    vi.mocked(api.room).mockResolvedValue(snap)
    vi.mocked(api.events).mockResolvedValue({ events: [], next_after: 0 } as never)
    vi.mocked(openRoomWs).mockImplementation(() => {
      const s = new FakeWebSocket()
      sockets.push(s)
      return s as unknown as WebSocket
    })
    vi.mocked(api.me).mockResolvedValueOnce({ id: 'arjun', name: 'Arjun', layers: ['kafka'] })
    render(
      <MemoryRouter>
        <UserProvider>
          <Room />
        </UserProvider>
      </MemoryRouter>,
    )
    expect(await screen.findByTestId('who')).toHaveTextContent('arjun')
    await waitFor(() => expect(sockets).toHaveLength(1))
    act(() => sockets[0].onopen?.())

    // arjun's steer fails while the socket is still open: kept, and the live retry is armed.
    act(() => {
      void actionsNow!.steer('arjun says stop', { clientId: 'kA' })
    })
    await pass(10)
    expect(actionsNow!.sends?.isPending('kA')).toBe(true)

    // Switch: dev-login sets priya's cookie at once; /api/me has not answered yet.
    let answerMe: (v: { id: string; name: string; layers: string[] }) => void = () => {}
    vi.mocked(api.me).mockReturnValueOnce(new Promise((r) => (answerMe = r)))
    vi.mocked(api.devLogin).mockImplementation(async () => {
      cookie = 'priya'
    })
    offline = false
    const before = posts.length
    fireEvent.click(screen.getByRole('button', { name: 'as priya' }))
    await pass(10)
    expect(screen.getByTestId('who')).toHaveTextContent('arjun') // still in the window

    await pass(LIVE_RETRY_MS + 1_000) // the live retry
    act(() => {
      sockets[0].readyState = 3 // CLOSED
      sockets[0].onclose?.({ reason: '' })
    })
    await pass(2_000) // the reconnect timer opens a socket with priya's cookie
    expect(sockets).toHaveLength(2)
    act(() => sockets[1].onopen?.()) // and drains
    await pass(100)
    act(() => {
      void actionsNow!.steer('arjun says stop', { clientId: 'kA' }) // a Retry click
    })
    await pass(100)
    expect(posts.slice(before)).toEqual([])

    // /api/me answers: priya's room, priya's own ledger. Still nothing of arjun's.
    await act(async () => {
      answerMe({ id: 'priya', name: 'Priya', layers: ['postgres'] })
    })
    expect(await screen.findByTestId('who')).toHaveTextContent('priya')
    await waitFor(() => expect(sockets).toHaveLength(3))
    act(() => sockets[sockets.length - 1].onopen?.())
    await pass(LIVE_RETRY_MS + 1_000)
    expect(posts.slice(before)).toEqual([])
    expect(actionsNow!.sends?.isPending('kA')).toBe(false)
  })
})
