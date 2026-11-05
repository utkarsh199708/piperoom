import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, render, renderHook, screen, waitFor } from '@testing-library/react'
import type { Actor, Event, RoomSnapshot } from '../api/types'
import { ApiClientError } from '../api/client'
import { ProposalsPanel } from '../components/ProposalsPanel'

// P2c-3: useRoom's cancelSchedule is POST /api/proposals/{pid}/cancel, and the countdown in the fold
// survives a reconnect the way the log says (the fresh room GET is read for the mode only).

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }) }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['consumer'] },
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
    api: { ...actual.api, me: vi.fn(), room: vi.fn(), events: vi.fn(), cancelProposal: vi.fn() },
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

const NOW = Date.parse('2026-10-19T10:00:00Z')
const RUN_AT = '2026-10-19T10:01:00Z'
const system: Actor = { kind: 'system', id: 'system', name: 'system' }
const arjun: Actor = { kind: 'human', id: 'arjun', name: 'Arjun' }

function ev(seq: number, type: Event['type'], payload: unknown, actor: Actor = system): Event {
  return { room_id: 'r1', seq, type, actor, payload, at: `2026-10-19T09:59:${String(seq).padStart(2, '0')}Z` }
}
const created = ev(1, 'proposal.created', {
  proposal_id: 'p1',
  title: 'Restart the connector task',
  steps: [{ action: 'connect_restart_task', layer: 'kafka-connect', params: {}, description: '' }],
  risk: 'low',
  rollback: '',
  required_layers: ['kafka-connect', 'consumer'],
  params_hash: 'h',
})
const scheduled = ev(2, 'proposal.scheduled', { proposal_id: 'p1', run_at: RUN_AT, policy: 'auto_approve', cancel_until: RUN_AT })
const cancelled = ev(3, 'proposal.cancelled', { proposal_id: 'p1', by: arjun, reason: '' }, arjun)

function snap(withScheduled: boolean): RoomSnapshot {
  return {
    id: 'r1',
    title: 'T',
    severity: 'sev2',
    pipeline_id: 'orders',
    status: 'open',
    opened_at: '2026-10-19T09:58:00Z',
    last_seq: 2,
    driver: null,
    agent_status: { state: 'idle' },
    hypothesis: null,
    open_proposals: [
      {
        proposal_id: 'p1',
        title: 'Restart the connector task',
        steps: [],
        risk: 'low',
        rollback: '',
        required_layers: ['kafka-connect', 'consumer'],
        approvals: { 'kafka-connect': 'pending', consumer: 'pending' },
        params_hash: 'h',
        ...(withScheduled ? { scheduled: { run_at: RUN_AT, cancel_until: RUN_AT } } : {}),
      },
    ],
    pending_suggestions: [],
    members: [],
    mode: { write: true, solo: false, auto_approve: ['connect_restart_task'] },
  }
}

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

async function live(log: Event[] = [created, scheduled]) {
  const server = fakeServer()
  server.log = log
  vi.mocked(api.room).mockResolvedValue(snap(true))
  const hook = renderHook(() => useRoom('r1'))
  await waitFor(() => expect(hook.result.current.state?.last_seq).toBe(log.length))
  return { server, hook }
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
  vi.useFakeTimers({ shouldAdvanceTime: true })
  vi.setSystemTime(NOW)
})
afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('useRoom cancelSchedule', () => {
  it('posts to the cancel route and answers ok; the event clears the countdown', async () => {
    const { server, hook } = await live()
    expect(hook.result.current.state?.proposals[0].scheduled?.run_at).toBe(RUN_AT)
    vi.mocked(api.cancelProposal).mockResolvedValue(cancelled)
    let res: unknown
    await act(async () => {
      res = await hook.result.current.actions.cancelSchedule?.('p1')
    })
    expect(api.cancelProposal).toHaveBeenCalledWith('p1', undefined)
    expect(res).toEqual({ ok: true })
    expect(hook.result.current.error).toBeNull()

    act(() => server.sockets[0].onmessage?.({ data: JSON.stringify({ type: 'event', event: cancelled }) }))
    await waitFor(() => expect(hook.result.current.state?.last_seq).toBe(3))
    expect(hook.result.current.state?.proposals[0].scheduled).toBeUndefined()
    expect(hook.result.current.state?.proposals[0].status).toBe('open')
  })

  it('409 not_scheduled is answered to the card, not the banner', async () => {
    const { hook } = await live()
    vi.mocked(api.cancelProposal).mockRejectedValue(
      new ApiClientError(409, 'not_scheduled', 'no auto-approve countdown of this proposal is pending'),
    )
    let res: unknown
    await act(async () => {
      res = await hook.result.current.actions.cancelSchedule?.('p1')
    })
    expect(res).toEqual({ ok: false, notScheduled: true })
    expect(hook.result.current.error).toBeNull()
  })

  it('any other failure goes to the banner', async () => {
    const { hook } = await live()
    vi.mocked(api.cancelProposal).mockRejectedValue(new ApiClientError(409, 'room_closed', 'room is closed'))
    let res: { ok: boolean; error?: string } | undefined
    await act(async () => {
      res = await hook.result.current.actions.cancelSchedule?.('p1')
    })
    expect(res?.ok).toBe(false)
    expect(res?.error).toBeTruthy()
    expect(hook.result.current.error).toBe(res?.error)
  })
})

describe('reconnect with a fresh snapshot', () => {
  it('a cancel logged while away clears the countdown when the replay resumes', async () => {
    const { server, hook } = await live()
    server.log = [created, scheduled, cancelled]
    vi.mocked(api.room).mockResolvedValue(snap(false))
    act(() => server.sockets[0].drop())
    await pass(15_000)
    expect(server.opens).toEqual([0, 2])
    expect(api.room).toHaveBeenCalledTimes(2)
    expect(hook.result.current.state?.last_seq).toBe(3)
    expect(hook.result.current.state?.proposals[0].scheduled).toBeUndefined()
    expect(hook.result.current.conn).toBe('live')
  })

  it('nothing new: the countdown stays, once, whether or not the fresh room GET still carries it', async () => {
    for (const withScheduled of [true, false]) {
      const { server, hook } = await live()
      // false: the room view hid it as stale (#652). The fold still has it, so it stays cancellable.
      vi.mocked(api.room).mockResolvedValue(snap(withScheduled))
      act(() => server.sockets[0].drop())
      await pass(15_000)
      expect(server.opens).toEqual([0, 2])
      const state = hook.result.current.state
      expect(state?.proposals).toHaveLength(1)
      expect(state?.proposals[0].scheduled).toEqual({ run_at: RUN_AT, cancel_until: RUN_AT, policy: 'auto_approve' })
      hook.unmount()
      vi.mocked(openRoomWs).mockReset()
      vi.mocked(api.room).mockReset()
    }
  })

  it('the card after a reconnect counts from the server time it was given, not from the reconnect', async () => {
    const { server, hook } = await live()
    vi.mocked(api.room).mockResolvedValue(snap(true))
    act(() => server.sockets[0].drop())
    await pass(15_000)
    const state = hook.result.current.state!
    render(
      <ProposalsPanel
        state={state}
        user={{ id: 'arjun', name: 'Arjun', layers: ['consumer'] }}
        actions={hook.result.current.actions}
      />,
    )
    // run_at is 60 s after NOW and about 15 s have passed in the reconnect.
    const shown = screen.getByTestId('countdown-time').textContent ?? ''
    const m = /^Runs in 0:(\d\d)$/.exec(shown)
    expect(m).not.toBeNull()
    expect(Number(m![1])).toBeGreaterThanOrEqual(40)
    expect(Number(m![1])).toBeLessThanOrEqual(45)
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument()
  })
})
