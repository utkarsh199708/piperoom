import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, render, renderHook, screen, waitFor, fireEvent } from '@testing-library/react'
import { toast } from 'sonner'
import type { Event, EventsPage, RoomSnapshot } from '../api/types'

vi.mock('sonner', () => ({
  toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }),
}))

// The hook reads the current user from context; provide a fixed user so it can
// render without a router/provider.
const session = vi.hoisted(() => ({ epoch: 0 }))

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] },
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: session.epoch,
  }),
}))

// Mock the transport surface (api.room, api.events, openRoomWs) but keep the
// rest of the client module intact.
vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    isFixtures: () => false,
    openRoomWs: vi.fn(),
    api: {
      ...actual.api,
      me: vi.fn(),
      room: vi.fn(),
      events: vi.fn(),
      grantControl: vi.fn(),
      declineSuggestion: vi.fn(),
      approve: vi.fn(),
      reject: vi.fn(),
    },
  }
})

import { ApiClientError, api, openRoomWs } from '../api/client'
import { onAuthProblem } from '../lib/auth'
import type { AuthProblem } from '../lib/auth'
import { useRoom } from './useRoom'
import { ProposalsPanel } from '../components/ProposalsPanel'

class FakeWebSocket {
  static CONNECTING = 0
  static OPEN = 1
  static CLOSING = 2
  static CLOSED = 3
  readyState = FakeWebSocket.OPEN
  sent: string[] = []
  closed = false
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: ((ev: { reason: string }) => void) | null = null
  onerror: (() => void) | null = null
  send(data: string) {
    this.sent.push(data)
  }
  close() {
    this.closed = true
    this.readyState = FakeWebSocket.CLOSED
  }
}

function snapshot(id: string): RoomSnapshot {
  return {
    id,
    title: 'T',
    severity: 'sev2',
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

function ev(
  seq: number,
  type: Event['type'] = 'agent.status',
  payload: unknown = { state: 'working' },
): Event {
  return {
    room_id: 'r1',
    seq,
    type,
    actor: { kind: 'agent', id: 'agent', name: 'Agent' },
    payload,
    at: `2026-10-11T10:00:${String(seq).padStart(2, '0')}Z`,
  }
}

function wsMessage(event: Event) {
  return { data: JSON.stringify({ type: 'event', event }) }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
  session.epoch = 0
})

describe('useRoom transport', () => {
  it('fills a gap from /events before applying the event, in order', async () => {
    const socket = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(api.events).mockResolvedValue({
      events: [
        ev(1, 'comment.added', { comment_id: 'c1', text: 'hi', mentions_agent: false }),
        ev(2, 'agent.status', { state: 'working' }),
      ],
      last_seq: 2,
    } satisfies EventsPage)
    vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 0))
    socket.onopen?.()

    await act(async () => {
      socket.onmessage?.(
        wsMessage(ev(3, 'comment.added', { comment_id: 'c2', text: 'again', mentions_agent: false })),
      )
    })

    await waitFor(() => expect(result.current.state?.last_seq).toBe(3))
    expect(api.events).toHaveBeenCalledWith('r1', 0)
    expect(result.current.state?.comments).toHaveLength(2)
  })

  it('closes the socket when the gap fetch fails, without applying the event', async () => {
    const socket = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(api.events).mockRejectedValue(new Error('boom'))
    vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 0))
    socket.onopen?.()

    await act(async () => {
      socket.onmessage?.(wsMessage(ev(3)))
    })

    await waitFor(() => expect(socket.closed).toBe(true))
    expect(result.current.state?.last_seq).toBe(0)
  })

  it('replaces the socket a failed gap fetch closed with exactly one fresh socket, resuming from lastSeq', async () => {
    const s1 = new FakeWebSocket()
    const s2 = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(api.events).mockRejectedValue(new Error('boom'))
    vi.mocked(openRoomWs)
      .mockReturnValueOnce(s1 as unknown as WebSocket)
      .mockReturnValueOnce(s2 as unknown as WebSocket)

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(1))
    s1.onopen?.()
    await act(async () => {
      s1.onmessage?.(wsMessage(ev(1)))
      // Two events behind a hole: both gap fetches fail, only one reconnect may follow.
      s1.onmessage?.(wsMessage(ev(3)))
      s1.onmessage?.(wsMessage(ev(4)))
    })
    await waitFor(() => expect(s1.closed).toBe(true))
    expect(result.current.state?.last_seq).toBe(1)

    act(() => {
      s1.onclose?.({ reason: '' }) // the browser reports the close
    })
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(2), { timeout: 3000 })
    expect(openRoomWs).toHaveBeenNthCalledWith(2, 'r1', 1)
    s2.onopen?.()
    await sleep(100)
    expect(s2.closed).toBe(false)
    expect(openRoomWs).toHaveBeenCalledTimes(2)
  })

  it('a gap fetch that fails after a reconnect closes the socket the event came from, not the new one', async () => {
    const s1 = new FakeWebSocket()
    const s2 = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    let failFetch!: (e: Error) => void
    vi.mocked(api.events).mockReturnValue(
      new Promise<EventsPage>((_, reject) => {
        failFetch = reject
      }),
    )
    vi.mocked(openRoomWs)
      .mockReturnValueOnce(s1 as unknown as WebSocket)
      .mockReturnValueOnce(s2 as unknown as WebSocket)

    renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(1))
    s1.onopen?.()
    await act(async () => {
      s1.onmessage?.(wsMessage(ev(3)))
    })
    await waitFor(() => expect(api.events).toHaveBeenCalledTimes(1))

    // The server drops s1 while its gap fetch is still pending; the hook opens s2.
    act(() => {
      s1.readyState = FakeWebSocket.CLOSED
      s1.onclose?.({ reason: '' })
    })
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(2), { timeout: 3000 })
    act(() => {
      s2.onopen?.()
    })

    await act(async () => {
      failFetch(new Error('boom'))
    })
    expect(s1.closed).toBe(true)
    expect(s2.closed).toBe(false) // the healthy new connection is left alone
    await sleep(100)
    expect(openRoomWs).toHaveBeenCalledTimes(2)
  })

  it('serializes two messages that arrive during one gap fetch', async () => {
    const socket = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))

    let resolveFirst!: (page: EventsPage) => void
    const firstFetch = new Promise<EventsPage>((resolve) => {
      resolveFirst = resolve
    })
    vi.mocked(api.events)
      .mockImplementationOnce(() => firstFetch)
      .mockResolvedValueOnce({ events: [ev(3)], last_seq: 3 })

    vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 0))
    socket.onopen?.()

    // Two messages while the first gap fetch is still pending.
    await act(async () => {
      socket.onmessage?.(wsMessage(ev(2)))
      socket.onmessage?.(wsMessage(ev(4)))
    })
    await waitFor(() => expect(api.events).toHaveBeenCalledTimes(1))

    await act(async () => {
      resolveFirst({ events: [ev(1)], last_seq: 1 })
    })

    await waitFor(() => expect(result.current.state?.last_seq).toBe(4))
    expect(api.events).toHaveBeenCalledTimes(2)
    expect(api.events).toHaveBeenNthCalledWith(1, 'r1', 0)
    expect(api.events).toHaveBeenNthCalledWith(2, 'r1', 2)
  })

  it('shows short user text for a lease_held error instead of the raw message', async () => {
    const socket = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 0))
    socket.onopen?.()

    await act(async () => {
      socket.onmessage?.({
        data: JSON.stringify({
          type: 'error',
          error: { code: 'lease_held', message: 'control/grant: someone holds the lease' },
        }),
      })
    })

    expect(result.current.error).toBe('someone else holds control')
  })

  it('a 409 already_decided on a suggestion decline gets friendly text', async () => {
    const socket = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)
    vi.mocked(api.declineSuggestion).mockRejectedValue(new ApiClientError(409, 'already_decided', 'raw'))

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 0))
    socket.onopen?.()
    await act(async () => {
      result.current.actions.declineSuggestion('g1', 'no')
    })
    await waitFor(() => expect(result.current.error).toBe('this suggestion was already sent or declined'))
    expect(api.declineSuggestion).toHaveBeenCalledWith('r1', 'g1', 'no')
  })

  it('maps a lease_held rejection from control/grant (self-grant race) to the friendly text', async () => {
    const socket = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)
    vi.mocked(api.grantControl).mockRejectedValue(
      new ApiClientError(409, 'lease_held', 'control/grant: kabir holds the lease until 10:35'),
    )

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 0))
    socket.onopen?.()

    // The no-driver banner grants control to the current user.
    await act(async () => {
      result.current.actions.grantControl('arjun')
    })

    await waitFor(() => expect(result.current.error).toBe('someone else holds control'))
    expect(api.grantControl).toHaveBeenCalledWith('r1', 'arjun')
  })
})

describe('useRoom approve', () => {
  async function setup() {
    const socket = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)
    const hook = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 0))
    socket.onopen?.()
    return hook
  }

  it('the real solo-mode server error becomes needsConfirm, not an error banner', async () => {
    vi.mocked(api.approve).mockRejectedValue(
      new ApiClientError(400, 'validation', 'solo mode: type confirm "orders/kafka" to approve'),
    )
    const { result } = await setup()
    let res: unknown
    await act(async () => {
      res = await result.current.actions.approve('p1', 'kafka', { params_hash: 'h1' })
    })
    expect(res).toEqual({ ok: false, needsConfirm: true })
    expect(result.current.error).toBeNull()
  })

  it('passes confirm through and resolves ok', async () => {
    vi.mocked(api.approve).mockResolvedValue(ev(1))
    const { result } = await setup()
    let res: unknown
    await act(async () => {
      res = await result.current.actions.approve('p1', 'kafka', { params_hash: 'h1', confirm: 'orders/kafka' })
    })
    expect(res).toEqual({ ok: true })
    expect(api.approve).toHaveBeenCalledWith('p1', 'kafka', { params_hash: 'h1', confirm: 'orders/kafka' })
  })

  it('any other error is shown in the banner and is not needsConfirm', async () => {
    vi.mocked(api.approve).mockRejectedValue(
      new ApiClientError(403, 'not_layer_owner', 'user arjun does not own layer "kafka"'),
    )
    const { result } = await setup()
    let res: unknown
    await act(async () => {
      res = await result.current.actions.approve('p1', 'kafka', { params_hash: 'h1' })
    })
    expect(res).toEqual({ ok: false })
    expect(result.current.error).toBe('user arjun does not own layer "kafka"')
  })

  it('409 params_mismatch on approve raises the stale flag, reloads the room and does not retry', async () => {
    vi.mocked(api.approve).mockRejectedValue(
      new ApiClientError(409, 'params_mismatch', 'params_hash does not match the proposal'),
    )
    const { result } = await setup()
    expect(api.room).toHaveBeenCalledTimes(1)
    let res: unknown
    await act(async () => {
      res = await result.current.actions.approve('p1', 'kafka', { params_hash: 'h1' })
    })
    expect(res).toEqual({ ok: false, mismatch: true })
    expect(result.current.stale).toEqual({ proposalId: 'p1', sentHash: 'h1' })
    // Not the generic error banner, and no second approve call.
    expect(result.current.error).toBeNull()
    // The room state is fetched again (snapshot), which is the reload.
    await waitFor(() => expect(api.room).toHaveBeenCalledTimes(2))
    expect(api.approve).toHaveBeenCalledTimes(1)
    // The flag survives the reload until it is dismissed.
    expect(result.current.stale).toEqual({ proposalId: 'p1', sentHash: 'h1' })
    act(() => result.current.dismissStale())
    expect(result.current.stale).toBeNull()
  })

  it('409 params_mismatch on reject sends the rendered hash, raises the stale flag and does not retry', async () => {
    vi.mocked(api.reject).mockRejectedValue(
      new ApiClientError(409, 'params_mismatch', 'params_hash does not match the proposal'),
    )
    const { result } = await setup()
    act(() => result.current.actions.reject('p1', 'kafka', 'wrong', 'h1'))
    await waitFor(() => expect(result.current.stale).toEqual({ proposalId: 'p1', sentHash: 'h1' }))
    expect(api.reject).toHaveBeenCalledWith('p1', 'kafka', 'wrong', 'h1')
    await waitFor(() => expect(api.room).toHaveBeenCalledTimes(2))
    expect(api.reject).toHaveBeenCalledTimes(1)
    expect(result.current.error).toBeNull()
  })

  it('a 409 that is not params_mismatch (already_decided) stays an ordinary error', async () => {
    vi.mocked(api.approve).mockRejectedValue(
      new ApiClientError(409, 'already_decided', 'layer "kafka" already decided'),
    )
    const { result } = await setup()
    await act(async () => {
      await result.current.actions.approve('p1', 'kafka', { params_hash: 'h1' })
    })
    expect(result.current.stale).toBeNull()
    expect(result.current.error).toBe('layer "kafka" already decided')
    expect(api.room).toHaveBeenCalledTimes(1)
    expect(toast.warning).not.toHaveBeenCalled()
  })

  it('raises the stale toast once, where the 409 happens, and Reload closes it for good', async () => {
    vi.mocked(api.approve).mockRejectedValue(
      new ApiClientError(409, 'params_mismatch', 'params_hash does not match the proposal'),
    )
    const { result } = await setup()
    expect(toast.warning).not.toHaveBeenCalled()
    await act(async () => {
      await result.current.actions.approve('p1', 'kafka', { params_hash: 'h1' })
    })
    expect(toast.warning).toHaveBeenCalledTimes(1)
    const [title, options] = vi.mocked(toast.warning).mock.calls[0]
    expect(title).toBe('Proposal changed since you looked')
    expect(options?.duration).toBe(Infinity)
    expect(options?.action).toMatchObject({ label: 'Reload' })
    await waitFor(() => expect(api.room).toHaveBeenCalledTimes(2))
    vi.mocked(toast.dismiss).mockClear()

    // Reload (the banner button, or the toast's own action) closes it ...
    act(() => result.current.reload())
    expect(toast.dismiss).toHaveBeenCalledWith('stale-params-mismatch')
    await waitFor(() => expect(api.room).toHaveBeenCalledTimes(3))
    await act(async () => {
      await sleep(20)
    })
    // ... and nothing raises it again, although the stale state (the banner) is still there.
    expect(toast.warning).toHaveBeenCalledTimes(1)
    expect(result.current.stale).not.toBeNull()

    // The toast action is the same reload.
    const action = options?.action as unknown as { onClick: () => void }
    vi.mocked(toast.dismiss).mockClear()
    act(() => action.onClick())
    expect(toast.dismiss).toHaveBeenCalledWith('stale-params-mismatch')
    expect(toast.warning).toHaveBeenCalledTimes(1)
  })

  it('dismissing the stale state, or a decision that goes through, takes the toast down', async () => {
    vi.mocked(api.approve).mockRejectedValueOnce(
      new ApiClientError(409, 'params_mismatch', 'params_hash does not match the proposal'),
    )
    const { result } = await setup()
    await act(async () => {
      await result.current.actions.approve('p1', 'kafka', { params_hash: 'h1' })
    })
    vi.mocked(toast.dismiss).mockClear()
    act(() => result.current.dismissStale())
    expect(toast.dismiss).toHaveBeenCalledWith('stale-params-mismatch')

    vi.mocked(api.approve).mockResolvedValueOnce(ev(1))
    await act(async () => {
      await result.current.actions.approve('p1', 'kafka', { params_hash: 'h2' })
    })
    expect(result.current.stale).toBeNull()
  })

  it('leaving the room takes the toast down', async () => {
    const { unmount } = await setup()
    vi.mocked(toast.dismiss).mockClear()
    unmount()
    expect(toast.dismiss).toHaveBeenCalledWith('stale-params-mismatch')
  })
})

describe('the stale note through a real room reload', () => {
  const H1 = 'a'.repeat(64)
  const H2 = 'b'.repeat(64)
  const created = (hash: string) =>
    ev(1, 'proposal.created', {
      proposal_id: 'p1',
      title: 'Restart the task',
      steps: [],
      risk: 'low',
      rollback: 'none',
      required_layers: ['kafka'],
      params_hash: hash,
    })
  const user = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] }

  // The room as RoomScreen uses it: no state means "Loading room…" and the panel is unmounted.
  function Harness() {
    const { state, actions, stale } = useRoom('r1')
    if (!state) return <p>Loading room…</p>
    return <ProposalsPanel state={state} user={user} actions={actions} stale={stale} />
  }

  it('the panel is unmounted by the reload, and the note is back when the new hash arrives', async () => {
    const s1 = new FakeWebSocket()
    const s2 = new FakeWebSocket()
    // The second snapshot is held back, so "Loading room…" can be seen.
    let releaseSnapshot: (s: RoomSnapshot) => void = () => undefined
    vi.mocked(api.room)
      .mockResolvedValueOnce(snapshot('r1'))
      .mockReturnValueOnce(new Promise<RoomSnapshot>((r) => (releaseSnapshot = r)))
    vi.mocked(openRoomWs)
      .mockReturnValueOnce(s1 as unknown as WebSocket)
      .mockReturnValueOnce(s2 as unknown as WebSocket)
    vi.mocked(api.approve).mockRejectedValue(
      new ApiClientError(409, 'params_mismatch', 'params_hash does not match the proposal'),
    )

    render(<Harness />)
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(1))
    act(() => s1.onopen?.())
    await act(async () => {
      s1.onmessage?.(wsMessage(created(H1)))
    })
    const approveButton = await screen.findByRole('button', { name: 'Approve' })
    expect(screen.queryByText('You looked at')).not.toBeInTheDocument()

    fireEvent.click(approveButton)
    fireEvent.click(await screen.findByRole('button', { name: 'Approve and run' })) // completes the set
    // The reload takes the panel away ...
    await waitFor(() => expect(screen.getByText('Loading room…')).toBeInTheDocument())
    expect(screen.queryByRole('region', { name: 'Proposals' })).not.toBeInTheDocument()
    // ... and brings it back with the server's hash, plus what the person looked at.
    await act(async () => releaseSnapshot(snapshot('r1')))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(2))
    act(() => s2.onopen?.())
    await act(async () => {
      s2.onmessage?.(wsMessage(created(H2)))
    })
    expect(await screen.findByText('You looked at')).toBeInTheDocument()
    expect(screen.getByText('aaaa aaaa … aaaa').closest('span[title]')).toHaveClass('line-through')
    expect(screen.getAllByText('bbbb bbbb … bbbb')).toHaveLength(2)
    expect(api.approve).toHaveBeenCalledTimes(1)
    expect(api.approve).toHaveBeenCalledWith('p1', 'kafka', { params_hash: H1 })
  })
})

describe('useRoom session epochs and reconnects', () => {
  it('an epoch bump closes the old socket and opens exactly one new socket with after=0', async () => {
    const s1 = new FakeWebSocket()
    const s2 = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(openRoomWs)
      .mockReturnValueOnce(s1 as unknown as WebSocket)
      .mockReturnValueOnce(s2 as unknown as WebSocket)

    const { rerender } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(1))
    s1.onopen?.()
    await act(async () => {
      s1.onmessage?.(wsMessage(ev(1)))
    })

    session.epoch = 1 // a login or logout happened
    rerender()

    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(2))
    expect(s1.closed).toBe(true)
    expect(openRoomWs).toHaveBeenNthCalledWith(2, 'r1', 0) // full replay, not lastSeq
    await sleep(50)
    expect(openRoomWs).toHaveBeenCalledTimes(2)

    // The old socket closing later (server side) must not reconnect or report.
    const seen: AuthProblem[] = []
    const off = onAuthProblem((p) => seen.push(p))
    s1.onclose?.({ reason: 'session_expired' })
    s1.onmessage?.({ data: JSON.stringify({ type: 'error', error: { code: 'session_expired', message: 'x' } }) })
    off()
    expect(seen).toEqual([])
    await sleep(50)
    expect(openRoomWs).toHaveBeenCalledTimes(2)
  })

  it('a close with reason session_expired reports the epoch and stops reconnects', async () => {
    const s1 = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(openRoomWs).mockReturnValue(s1 as unknown as WebSocket)
    const seen: AuthProblem[] = []
    const off = onAuthProblem((p) => seen.push(p))

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(1))
    s1.onopen?.()
    act(() => {
      s1.onclose?.({ reason: 'session_expired' })
    })
    off()

    expect(seen).toEqual([{ kind: 'session_expired', message: 'session_expired', epoch: 0 }])
    expect(result.current.conn).toBe('error')
    await sleep(1300) // longer than the first backoff
    expect(openRoomWs).toHaveBeenCalledTimes(1)
  })

  it('error frames viewer and proxy_identity become banners', async () => {
    const s1 = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(openRoomWs).mockReturnValue(s1 as unknown as WebSocket)
    const seen: AuthProblem[] = []
    const off = onAuthProblem((p) => seen.push(p))

    renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(1))
    s1.onopen?.()
    for (const code of ['viewer', 'proxy_identity']) {
      act(() => {
        s1.onmessage?.({ data: JSON.stringify({ type: 'error', error: { code, message: 'm' } }) })
      })
    }
    off()
    expect(seen.map((p) => p.kind)).toEqual(['viewer', 'proxy_identity'])
  })

  it('an upgrade refused before open asks /api/me, but only once per outage', async () => {
    const s1 = new FakeWebSocket()
    const s2 = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(api.me).mockResolvedValue({ id: 'arjun', name: 'Arjun', layers: [] })
    vi.mocked(openRoomWs)
      .mockReturnValueOnce(s1 as unknown as WebSocket)
      .mockReturnValueOnce(s2 as unknown as WebSocket)

    renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(1))
    act(() => {
      s1.readyState = FakeWebSocket.CLOSED
      s1.onclose?.({ reason: '' }) // never opened
    })
    expect(api.me).toHaveBeenCalledTimes(1)

    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(2), { timeout: 3000 })
    act(() => {
      s2.readyState = FakeWebSocket.CLOSED
      s2.onclose?.({ reason: '' }) // refused again
    })
    expect(api.me).toHaveBeenCalledTimes(1) // not re-probed on every retry
  })

  it('a socket that opened and dropped does not probe /api/me', async () => {
    const s1 = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(openRoomWs).mockReturnValue(s1 as unknown as WebSocket)
    renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(1))
    s1.onopen?.()
    act(() => {
      s1.readyState = FakeWebSocket.CLOSED
      s1.onclose?.({ reason: '' })
    })
    expect(api.me).not.toHaveBeenCalled()
  })

  it('a reconnect in the same epoch resumes from lastSeq', async () => {
    const s1 = new FakeWebSocket()
    const s2 = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(openRoomWs)
      .mockReturnValueOnce(s1 as unknown as WebSocket)
      .mockReturnValueOnce(s2 as unknown as WebSocket)

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(1))
    expect(openRoomWs).toHaveBeenNthCalledWith(1, 'r1', 0)
    s1.onopen?.()
    await act(async () => {
      s1.onmessage?.(wsMessage(ev(1)))
    })
    await waitFor(() => expect(result.current.state?.last_seq).toBe(1))

    act(() => {
      s1.readyState = FakeWebSocket.CLOSED
      s1.onclose?.({ reason: '' })
    })
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledTimes(2), { timeout: 3000 })
    expect(openRoomWs).toHaveBeenNthCalledWith(2, 'r1', 1)
  })
})

describe('useRoom reconnect jitter', () => {
  let sockets: FakeWebSocket[]

  beforeEach(async () => {
    // Only the timers the hook uses: waitFor and React keep their own.
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] })
    sockets = []
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(api.me).mockResolvedValue({ id: 'arjun', name: 'Arjun', layers: [] })
    vi.mocked(openRoomWs).mockImplementation(() => {
      const s = new FakeWebSocket()
      sockets.push(s)
      return s as unknown as WebSocket
    })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  async function mount(random: () => number) {
    renderHook(() => useRoom('r1', random))
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0) // the snapshot loads and the first socket opens
    })
    expect(sockets).toHaveLength(1)
  }

  // Never-opened sockets, so the backoff keeps growing instead of resetting on open.
  function dropLatest() {
    const s = sockets[sockets.length - 1]
    act(() => {
      s.readyState = FakeWebSocket.CLOSED
      s.onclose?.({ reason: '' })
    })
  }

  async function advance(ms: number) {
    await act(async () => {
      await vi.advanceTimersByTimeAsync(ms)
    })
  }

  it.each([
    [0, 500],
    [0.5, 750],
    [1, 1000],
  ])('with random %s the first retry waits %s ms', async (r, wait) => {
    await mount(() => r)
    dropLatest()
    await advance(wait - 1)
    expect(sockets).toHaveLength(1)
    await advance(1)
    expect(sockets).toHaveLength(2)
  })

  it('draws a new delay for every retry, from a ceiling that doubles up to 10 s', async () => {
    // Retry n waits between ceiling/2 and ceiling; the ceilings are 1, 2, 4, 8, 10, 10 s.
    const draws = [1, 0, 1, 0, 1, 0]
    const waits = [1000, 1000, 4000, 4000, 10000, 5000]
    let i = 0
    await mount(() => draws[i++])
    for (let n = 0; n < waits.length; n++) {
      dropLatest()
      await advance(waits[n] - 1)
      expect(sockets).toHaveLength(n + 1)
      await advance(1)
      expect(sockets).toHaveLength(n + 2)
    }
  })
})
