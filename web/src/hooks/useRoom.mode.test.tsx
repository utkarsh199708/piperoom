import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { Event, RoomMode, RoomSnapshot } from '../api/types'
import { ApiClientError } from '../api/client'
import { waitingForWriteMode } from '../lib/writeMode'

// P1: write mode changes only when the server restarts, and a restart drops the socket. So the hook
// asks for GET /api/rooms/{id} again on every reconnect and takes its `mode` and `blocked` marks, in
// both directions. The first connect does not (its snapshot is fresh).

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }) }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka'] },
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
  }),
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
import { useRoom } from './useRoom'

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

const off: RoomMode = { write: false, solo: false, auto_approve: [] }
const on: RoomMode = { write: true, solo: false, auto_approve: [] }

function snap(mode: RoomMode | undefined, blocked = false): RoomSnapshot {
  return {
    id: 'r1',
    title: 'T',
    severity: 'sev2',
    pipeline_id: 'orders',
    status: 'open',
    opened_at: '2026-10-11T10:00:00Z',
    last_seq: 0,
    driver: null,
    agent_status: { state: 'idle' },
    hypothesis: null,
    open_proposals: blocked
      ? [
          {
            proposal_id: 'p1',
            title: 'Restart the task',
            steps: [],
            risk: 'low',
            rollback: '',
            required_layers: ['schema-registry', 'kafka-connect'],
            approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' },
            blocked: 'write_mode_off',
          },
        ]
      : [],
    pending_suggestions: [],
    members: [],
    ...(mode ? { mode } : {}),
  }
}

const human = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }
function ev(seq: number, type: Event['type'], payload: unknown): Event {
  return {
    room_id: 'r1',
    seq,
    type,
    actor: human,
    payload,
    at: `2026-10-11T10:00:${String(seq).padStart(2, '0')}Z`,
  }
}
const created = ev(1, 'proposal.created', {
  proposal_id: 'p1',
  title: 'Restart the task',
  steps: [{ action: 'restart_connector_task', layer: 'kafka-connect', params: {}, description: '' }],
  risk: 'low',
  rollback: '',
  required_layers: ['schema-registry', 'kafka-connect'],
})
const approval = (seq: number, layer: string) => ev(seq, 'approval.given', { proposal_id: 'p1', layer })

function fakeServer() {
  const server = { log: [] as Event[], opens: [] as number[], sockets: [] as FakeWebSocket[] }
  vi.mocked(openRoomWs).mockImplementation((_room: string, after: number) => {
    const sock = new FakeWebSocket()
    server.opens.push(after)
    server.sockets.push(sock)
    queueMicrotask(() => {
      if (sock.readyState === FakeWebSocket.CLOSED) return
      sock.readyState = FakeWebSocket.OPEN
      sock.onopen?.()
      for (const e of server.log) {
        if (e.seq > after) sock.onmessage?.({ data: JSON.stringify({ type: 'event', event: e }) })
      }
    })
    return sock as unknown as WebSocket
  })
  vi.mocked(api.me).mockResolvedValue(undefined as never)
  return server
}

async function pass(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
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

describe('useRoom — write mode on reconnect', () => {
  it('does not ask again on the first connect', async () => {
    const server = fakeServer()
    server.log = [created]
    vi.mocked(api.room).mockResolvedValue(snap(off))
    const hook = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(hook.result.current.state?.last_seq).toBe(1))
    expect(api.room).toHaveBeenCalledTimes(1)
    expect(hook.result.current.state?.mode).toEqual(off)
  })

  it('on to off: the approval that completes the set after the restart is marked blocked', async () => {
    const server = fakeServer()
    server.log = [created, approval(2, 'schema-registry')]
    vi.mocked(api.room).mockResolvedValue(snap(on))
    const hook = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(hook.result.current.state?.last_seq).toBe(2))
    expect(hook.result.current.state?.mode?.write).toBe(true)

    // The server restarts with write mode off; the last approval lands under it.
    server.log = [...server.log, approval(3, 'kafka-connect')]
    vi.mocked(api.room).mockResolvedValue(snap(off, true))
    act(() => server.sockets[0].drop())
    await pass(15_000)

    expect(server.opens).toEqual([0, 2])
    expect(api.room).toHaveBeenCalledTimes(2)
    const state = hook.result.current.state
    expect(state?.mode).toEqual(off)
    expect(state?.proposals[0].status).toBe('approved')
    expect(state?.proposals[0].blocked).toBe('write_mode_off')
    expect(waitingForWriteMode(state!.proposals[0])).toBe(true)
    expect(hook.result.current.conn).toBe('live')
  })

  it('on to off: a proposal approved earlier and not run is marked blocked too', async () => {
    const server = fakeServer()
    server.log = [created, approval(2, 'schema-registry'), approval(3, 'kafka-connect')]
    vi.mocked(api.room).mockResolvedValue(snap(on))
    const hook = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(hook.result.current.state?.last_seq).toBe(3))
    expect(hook.result.current.state?.proposals[0].blocked).toBeUndefined()

    vi.mocked(api.room).mockResolvedValue(snap(off)) // the snapshot carries no blocked mark: mode.write is enough
    act(() => server.sockets[0].drop())
    await pass(15_000)

    expect(hook.result.current.state?.proposals[0].blocked).toBe('write_mode_off')
  })

  it('off to on: the waiting mark and the mode are dropped', async () => {
    const server = fakeServer()
    server.log = [created, approval(2, 'schema-registry'), approval(3, 'kafka-connect')]
    vi.mocked(api.room).mockResolvedValue(snap(off, true))
    const hook = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(hook.result.current.state?.last_seq).toBe(3))
    const before = hook.result.current.state
    expect(before?.mode?.write).toBe(false)
    expect(waitingForWriteMode(before!.proposals[0])).toBe(true)

    vi.mocked(api.room).mockResolvedValue(snap(on))
    act(() => server.sockets[0].drop())
    await pass(15_000)

    expect(server.opens).toEqual([0, 3])
    const state = hook.result.current.state
    expect(state?.mode).toEqual(on)
    expect(state?.snapshot_blocked).toBeUndefined()
    expect(state?.proposals[0].blocked).toBeUndefined()
    expect(waitingForWriteMode(state!.proposals[0])).toBe(false)
    expect(state?.proposals[0].status).toBe('approved') // the decision itself is untouched
  })

  it('a server that stops sending mode (before P1a) reads as write mode on', async () => {
    const server = fakeServer()
    server.log = [created, approval(2, 'schema-registry'), approval(3, 'kafka-connect')]
    vi.mocked(api.room).mockResolvedValue(snap(off, true))
    const hook = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(hook.result.current.state?.last_seq).toBe(3))

    vi.mocked(api.room).mockResolvedValue(snap(undefined))
    act(() => server.sockets[0].drop())
    await pass(15_000)

    expect('mode' in hook.result.current.state!).toBe(false)
    expect(waitingForWriteMode(hook.result.current.state!.proposals[0])).toBe(false)
  })

  it('a failed refresh does not hold the reconnect back or change what is known', async () => {
    const server = fakeServer()
    server.log = [created]
    vi.mocked(api.room).mockResolvedValueOnce(snap(off))
    vi.mocked(api.room).mockRejectedValue(new ApiClientError(503, 'unavailable', 'starting'))
    const hook = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(hook.result.current.state?.last_seq).toBe(1))

    server.log = [created, approval(2, 'schema-registry')]
    act(() => server.sockets[0].drop())
    await pass(15_000)

    expect(server.opens).toEqual([0, 1])
    expect(hook.result.current.conn).toBe('live')
    expect(hook.result.current.state?.last_seq).toBe(2)
    expect(hook.result.current.state?.mode).toEqual(off)
    expect(hook.result.current.error).toBeNull()
  })

  it('a slow refresh does not hold the replay back, and its late answer is still applied', async () => {
    const server = fakeServer()
    server.log = [created, approval(2, 'schema-registry')]
    vi.mocked(api.room).mockResolvedValueOnce(snap(on))
    const hook = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(hook.result.current.state?.last_seq).toBe(2))

    let answer: (s: RoomSnapshot) => void = () => undefined
    vi.mocked(api.room).mockImplementation(() => new Promise((resolve) => (answer = resolve)))
    server.log = [...server.log, approval(3, 'kafka-connect')]
    act(() => server.sockets[0].drop())
    await pass(15_000)

    // The replay went on without the answer: the approval is in, judged by the old mode.
    expect(server.opens).toEqual([0, 2])
    expect(hook.result.current.state?.last_seq).toBe(3)
    expect(hook.result.current.state?.proposals[0].status).toBe('approved')
    expect(hook.result.current.state?.proposals[0].blocked).toBeUndefined()

    // Then the answer comes: the proposal is re-judged against the new mode.
    await act(async () => {
      answer(snap(off, true))
    })
    expect(hook.result.current.state?.mode).toEqual(off)
    expect(hook.result.current.state?.proposals[0].blocked).toBe('write_mode_off')
  })
})
