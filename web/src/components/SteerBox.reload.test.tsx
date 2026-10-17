import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen } from '@testing-library/react'
import type { Event, RoomSnapshot } from '../api/types'
import type { CurrentUser } from '../state/permissions'

// #550: a silent stale_position reload (#464) passes through a null room state, so a screen that
// renders `state ? <SteerBox/> : skeleton` unmounts the composer and mounts a new one. The new one
// must still know the steer the old one sent: never re-offer a delivered steer, never store it twice.
// The real useRoom, ledger and api.steer run over a fake server: POSTs follow docs/api.md
// "Idempotent posts" (stored on arrival, keyed on client_id, the event echoes it) and are answered
// after `answerAfter` ms; the socket follows "Stale position" (the upgrade is accepted, then a
// position past the stored log gets a stale_position frame and a close with that reason).

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }) }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user: ARJUN_USER, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0 }),
}))
vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    isFixtures: () => false,
    openRoomWs: vi.fn(),
    api: { ...actual.api, me: vi.fn(), room: vi.fn(), events: vi.fn() },
  }
})

import { api, openRoomWs } from '../api/client'
import { useRoom } from '../hooks/useRoom'
import { SteerBox } from './SteerBox'

const ARJUN_USER: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka'] }
const ARJUN = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }

class FakeWebSocket {
  static CONNECTING = 0
  static OPEN = 1
  static CLOSING = 2
  static CLOSED = 3
  readyState = FakeWebSocket.CONNECTING
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: ((ev: { reason: string }) => void) | null = null
  onerror: (() => void) | null = null
  send() {}
  close() {
    this.readyState = FakeWebSocket.CLOSED
  }
  drop(reason = '') {
    this.readyState = FakeWebSocket.CLOSED
    this.onclose?.({ reason })
  }
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

const status1: Event = {
  room_id: 'r1',
  seq: 1,
  type: 'agent.status',
  actor: { kind: 'agent', id: 'agent', name: 'Agent' },
  payload: { state: 'working' },
  at: '2026-10-11T10:00:01Z',
}

function fakeServer() {
  const server = {
    /** The room log the socket replays. */
    log: [status1] as Event[],
    /** The steer texts the server stored, in order. */
    steers: [] as string[],
    opens: [] as number[],
    sockets: [] as FakeWebSocket[],
    answerAfter: 500,
    /** How long GET /api/rooms/{id} takes. */
    roomAfter: 30,
    offline: false,
  }
  const byKey = new Map<string, Event>()
  vi.stubGlobal(
    'fetch',
    vi.fn((_path: string, init: RequestInit) => {
      if (server.offline) return Promise.reject(new TypeError('Failed to fetch'))
      const body = JSON.parse(String(init.body)) as { text: string; client_id?: string }
      let ev = body.client_id ? byKey.get(body.client_id) : undefined
      if (!ev) {
        server.steers.push(body.text)
        ev = {
          room_id: 'r1',
          seq: 100 + server.steers.length,
          type: 'steer.sent',
          actor: ARJUN,
          payload: { text: body.text, client_id: body.client_id },
          at: '',
        }
        if (body.client_id) byKey.set(body.client_id, ev)
      }
      const answer = ev
      return new Promise<Response>((resolve) =>
        setTimeout(
          () => resolve(new Response(JSON.stringify(answer), { status: 200, headers: { 'Content-Type': 'application/json' } })),
          server.answerAfter,
        ),
      )
    }),
  )
  vi.mocked(openRoomWs).mockImplementation((_room: string, after: number) => {
    const sock = new FakeWebSocket()
    server.opens.push(after)
    server.sockets.push(sock)
    queueMicrotask(() => {
      if (sock.readyState === FakeWebSocket.CLOSED) return
      sock.readyState = FakeWebSocket.OPEN
      sock.onopen?.() // the upgrade is accepted before the position is checked
      const last = server.log.length ? server.log[server.log.length - 1].seq : 0
      if (after > last) {
        const message = `after ${after} is past the room's last event ${last}`
        sock.onmessage?.({ data: JSON.stringify({ type: 'error', error: { code: 'stale_position', message } }) })
        sock.drop('stale_position')
        return
      }
      for (const e of server.log) {
        if (e.seq > after) sock.onmessage?.({ data: JSON.stringify({ type: 'event', event: e }) })
      }
    })
    return sock as unknown as WebSocket
  })
  vi.mocked(api.room).mockImplementation(
    () => new Promise((resolve) => setTimeout(() => resolve(snap), server.roomAfter)),
  )
  vi.mocked(api.events).mockResolvedValue({ events: [], next_after: 0 } as never)
  vi.mocked(api.me).mockResolvedValue(undefined as never)
  return server
}

// The screen the ticket describes: RoomScreen renders the skeleton while the state is null.
// `hidden` unmounts the composer by hand, for any other remount.
function Room({ hidden = false }: { hidden?: boolean }) {
  const { state, actions, conn } = useRoom('r1')
  return state && !hidden ? <SteerBox state={state} user={ARJUN_USER} actions={actions} conn={conn} /> : <p>loading</p>
}

/** Advances the clock in 50 ms steps, so each answer and render runs in its own step. */
async function pass(ms: number) {
  for (let t = 0; t < ms; t += 50) {
    await act(async () => {
      await vi.advanceTimersByTimeAsync(50)
    })
  }
}

const field = () => screen.getByLabelText<HTMLTextAreaElement>('Steer the agent')
const retry = () => screen.queryByRole('button', { name: 'Retry' })

async function typeAndSend(text: string) {
  fireEvent.focus(field())
  fireEvent.change(field(), { target: { value: text } })
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: /Send to agent/ }))
    await Promise.resolve()
  })
}

/**
 * One earlier steer by this client, answered with its client_id echoed: the ledger then knows the
 * server dedupes from a real answer (docs/api.md, Idempotent posts), so a TypeError keeps the next
 * steer for the reconnect (#551). The warm-up is not counted in `server.steers`.
 */
async function warmUp(server: ReturnType<typeof fakeServer>) {
  const after = server.answerAfter
  server.answerAfter = 50
  await typeAndSend('hello')
  await pass(200)
  expect(field().value).toBe('')
  server.answerAfter = after
  server.steers.length = 0
}

/** The person presses Send on whatever the field holds, if anything. */
async function sendWhatIsThere() {
  if (!field().value.trim()) return
  fireEvent.focus(field())
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: /Send to agent/ }))
    await Promise.resolve()
  })
  await pass(5_000)
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
  vi.useFakeTimers({ shouldAdvanceTime: true })
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('SteerBox across a silent stale_position reload (#550)', () => {
  it('scenario A: a kept steer drained by the stale socket is not re-offered, and Send does not store it again', async () => {
    const server = fakeServer()
    render(<Room />)
    await pass(200)
    expect(server.opens).toEqual([0])
    await warmUp(server)

    // The socket drops; the steer fails with a TypeError, so it is kept and the Retry strip shows.
    act(() => server.sockets[0].drop())
    server.offline = true
    await typeAndSend('stop the connector')
    await pass(100)
    expect(retry()).toBeInTheDocument()
    expect(server.steers).toEqual([])

    // The log behind the client's position is gone. The reconnect with after=1 is accepted, its
    // onopen drains the kept steer (stored once), then stale_position reloads the room silently.
    server.offline = false
    server.log = []
    await pass(3_000)
    expect(server.opens.slice(0, 3)).toEqual([0, 1, 0])
    expect(server.steers).toEqual(['stop the connector'])

    expect(field().value).toBe('')
    expect(retry()).not.toBeInTheDocument()
    await sendWhatIsThere()
    expect(server.steers).toEqual(['stop the connector'])
  })

  it('a kept steer whose drained copy lands while no composer is mounted: the next composer starts empty', async () => {
    const server = fakeServer()
    const view = render(<Room />)
    await pass(200)
    await warmUp(server)
    act(() => server.sockets[0].drop())
    server.offline = true
    await typeAndSend('stop the connector')
    await pass(400) // past the draft's 300 ms debounce: the words are in localStorage
    expect(retry()).toBeInTheDocument()

    // The composer goes away (a reload's skeleton, or any remount) before the reconnect drains it.
    view.rerender(<Room hidden />)
    expect(screen.queryByLabelText('Steer the agent')).toBeNull()
    server.offline = false
    server.answerAfter = 100
    for (let t = 0; t < 5_000 && server.steers.length === 0; t += 50) await pass(50)
    await pass(300)
    expect(server.steers).toEqual(['stop the connector']) // the drained copy was answered

    view.rerender(<Room />)
    expect(field().value).toBe('')
    expect(retry()).not.toBeInTheDocument()
    await sendWhatIsThere()
    expect(server.steers).toEqual(['stop the connector'])
  })

  it('a kept steer whose drained copy is still out when the composer remounts: the Retry strip is back, and goes when it lands', async () => {
    const server = fakeServer()
    render(<Room />)
    await pass(200)
    await warmUp(server)
    act(() => server.sockets[0].drop())
    server.offline = true
    await typeAndSend('stop the connector')
    await pass(100)
    server.offline = false
    server.log = []
    server.answerAfter = 3_000
    await pass(1_500)
    // Remounted, the drained copy still on the wire: the same action is offered, not a fresh draft.
    expect(field().value).toBe('stop the connector')
    expect(retry()).toBeInTheDocument()
    // A Retry click is that action: it waits for the copy on the wire and posts nothing new.
    await act(async () => {
      fireEvent.click(retry()!)
      await Promise.resolve()
    })
    await pass(3_000)
    expect(server.steers).toEqual(['stop the connector'])
    expect(field().value).toBe('')
    expect(retry()).not.toBeInTheDocument()
  })

  it('scenario B: a steer in flight for 4 s across the reload is not left in the field, and Send does not store it again', async () => {
    const server = fakeServer()
    server.answerAfter = 4_000
    render(<Room />)
    await pass(200)

    await typeAndSend('pause the consumer')
    expect(server.steers).toEqual(['pause the consumer'])
    // Mid-flight, the socket drops and the reconnect gets stale_position: a silent reload.
    server.log = []
    act(() => server.sockets[0].drop())
    await pass(2_000)
    expect(server.opens.slice(0, 3)).toEqual([0, 1, 0])
    // The new composer knows the steer is still on the wire: no second copy from a Send press.
    expect(screen.queryByRole('button', { name: /Send to agent/ })).toBeNull()
    await pass(3_000)

    expect(field().value).toBe('')
    expect(retry()).not.toBeInTheDocument()
    await sendWhatIsThere()
    expect(server.steers).toEqual(['pause the consumer'])
  })

  it('scenario B, pressed mid-flight: Send on the restored words waits for the same action', async () => {
    const server = fakeServer()
    server.answerAfter = 4_000
    render(<Room />)
    await pass(200)
    await typeAndSend('pause the consumer')
    server.log = []
    act(() => server.sockets[0].drop())
    await pass(2_000)
    // Even a submit by keyboard while the first copy is on the wire posts nothing new.
    await act(async () => {
      fireEvent.keyDown(field(), { key: 'Enter', ctrlKey: true })
      await Promise.resolve()
    })
    await pass(4_000)
    expect(server.steers).toEqual(['pause the consumer'])
    expect(field().value).toBe('')
  })

  it('words typed after the reload are a new action: sent and stored as their own steer', async () => {
    const server = fakeServer()
    server.answerAfter = 4_000
    render(<Room />)
    await pass(200)
    await typeAndSend('pause the consumer')
    server.log = []
    act(() => server.sockets[0].drop())
    await pass(5_000)
    expect(field().value).toBe('')
    server.answerAfter = 100
    await typeAndSend('pause the consumer')
    await pass(500)
    expect(server.steers).toEqual(['pause the consumer', 'pause the consumer'])
    expect(field().value).toBe('')
  })
})
