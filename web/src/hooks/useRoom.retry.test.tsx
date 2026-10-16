import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, renderHook, screen, waitFor } from '@testing-library/react'
import type { Event, RoomSnapshot } from '../api/types'
import type { StepState } from '../state/types'

// #390: one Retry is one steer or comment, whatever the reconnect timing and whichever
// component sends it. RollbackButton and the Timeline reply boxes retry with a plain
// steer/comment call (no `retry` flag), so the hook must cover them on its own.

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
    api: { ...actual.api, me: vi.fn(), room: vi.fn(), events: vi.fn(), steer: vi.fn(), comment: vi.fn() },
  }
})

import { ApiClientError, api, openRoomWs } from '../api/client'
import { NOT_CONFIRMED_TEXT, UNCONFIRMED_TEXT } from '../lib/errors'
import { rollbackSteer } from '../lib/rollback'
import { RollbackButton } from '../components/room/RollbackButton'
import { Timeline } from '../components/Timeline'
import { useRoom } from './useRoom'

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
  driver: null,
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [],
  pending_suggestions: [],
  members: [],
}

const step: StepState = { id: 's1', title: 'Check the Debezium connector', tool_calls: [], messages: [] }
const offline = () => new TypeError('Failed to fetch')

// Only a server known to dedupe keeps a failed send for the reconnect (#545, #551): the room's
// log shows one, a keyed post of another member, before the first send.
const KEYED: Event = {
  room_id: 'r1',
  seq: 1,
  type: 'comment.added',
  actor: { kind: 'human', id: 'kabir', name: 'Kabir' },
  payload: { comment_id: 'c0', text: 'earlier', mentions_agent: false, client_id: 'theirs' },
  at: '2026-10-11T10:00:01Z',
}

async function knownToDedupe(socket: FakeWebSocket) {
  await act(async () => {
    socket.onmessage?.({ data: JSON.stringify({ type: 'event', event: KEYED }) })
    await Promise.resolve()
  })
}

function mountSocket() {
  const socket = new FakeWebSocket()
  vi.mocked(api.room).mockResolvedValue(snap)
  vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)
  return socket
}

/** A drain request that settles only when the test says so. */
function hangingCall() {
  let settle: { resolve: () => void; reject: (e: unknown) => void } = { resolve: () => {}, reject: () => {} }
  const p = new Promise<never>((resolve, reject) => {
    settle = { resolve: resolve as unknown as () => void, reject }
  })
  return { p, settle: () => settle }
}

function RollbackRoom() {
  const { actions } = useRoom('r1')
  return <RollbackButton title="Restart" proposalId="p1" rollback="set it back" asDriver onSteer={actions.steer} />
}

function TimelineRoom() {
  const { actions } = useRoom('r1')
  return <Timeline steps={[step]} messages={[]} comments={[]} onComment={actions.comment} />
}

const ROLLBACK = rollbackSteer('Restart', 'p1', 'set it back')

async function rollbackFailsOffline() {
  const socket = mountSocket()
  render(<RollbackRoom />)
  await waitFor(() => expect(openRoomWs).toHaveBeenCalled())
  await knownToDedupe(socket)
  vi.mocked(api.steer).mockRejectedValueOnce(offline())
  fireEvent.click(screen.getByRole('button', { name: 'Roll back' }))
  fireEvent.click(screen.getByRole('button', { name: 'Ask the agent to roll back' }))
  await screen.findByRole('button', { name: 'Retry' })
  return socket
}

async function replyFailsOffline() {
  const socket = mountSocket()
  render(<TimelineRoom />)
  await waitFor(() => expect(openRoomWs).toHaveBeenCalled())
  await knownToDedupe(socket)
  vi.mocked(api.comment).mockRejectedValueOnce(offline())
  fireEvent.click(screen.getByRole('button', { name: /Check the Debezium connector/ }))
  fireEvent.change(screen.getByLabelText('Comment on this step'), { target: { value: 'looks right' } })
  fireEvent.click(screen.getByRole('button', { name: 'Comment' }))
  await screen.findByRole('button', { name: 'Retry' })
  return socket
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
})

describe('RollbackButton Retry with the real hook', () => {
  it('Retry while the reconnect is sending the rollback waits for it: one more steer, not two', async () => {
    const socket = await rollbackFailsOffline()
    const drain = hangingCall()
    vi.mocked(api.steer).mockImplementationOnce(() => drain.p)
    vi.mocked(api.steer).mockResolvedValue(undefined as never)
    act(() => socket.onopen?.())
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    await act(async () => drain.settle().resolve())
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    expect(api.steer).toHaveBeenCalledTimes(2)
    expect(api.steer).toHaveBeenLastCalledWith('r1', ROLLBACK, expect.any(String))
  })

  it('Retry after the reconnect delivered the rollback does not steer again', async () => {
    const socket = await rollbackFailsOffline()
    vi.mocked(api.steer).mockResolvedValue(undefined as never)
    act(() => socket.onopen?.())
    await waitFor(() => expect(api.steer).toHaveBeenCalledTimes(2))
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    expect(api.steer).toHaveBeenCalledTimes(2)
  })
})

describe('Timeline step reply Retry with the real hook', () => {
  it('Retry while the reconnect is sending the comment waits for it', async () => {
    const socket = await replyFailsOffline()
    const drain = hangingCall()
    vi.mocked(api.comment).mockImplementationOnce(() => drain.p)
    vi.mocked(api.comment).mockResolvedValue(undefined as never)
    act(() => socket.onopen?.())
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    await act(async () => drain.settle().resolve())
    await waitFor(() => expect(screen.getByLabelText('Comment on this step')).toHaveValue(''))
    expect(api.comment).toHaveBeenCalledTimes(2)
    expect(api.comment).toHaveBeenLastCalledWith('r1', { step_id: 's1', text: 'looks right', client_id: expect.any(String) })
  })

  it('Retry after the reconnect delivered the comment does not post it again', async () => {
    const socket = await replyFailsOffline()
    vi.mocked(api.comment).mockResolvedValue(undefined as never)
    act(() => socket.onopen?.())
    await waitFor(() => expect(api.comment).toHaveBeenCalledTimes(2))
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    await waitFor(() => expect(screen.getByLabelText('Comment on this step')).toHaveValue(''))
    expect(api.comment).toHaveBeenCalledTimes(2)
  })
})

describe('useRoom: actions are told apart by client_id, never by their words', () => {
  /** `dedupes` false: no answer and no keyed event seen yet, so nothing is re-sent on its own. */
  async function openRoom(dedupes = true) {
    const socket = mountSocket()
    const hook = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalled())
    if (dedupes) await knownToDedupe(socket)
    return { hook, socket }
  }

  // Review QA-D1: an old delivery must not answer the Retry of a newer action's 503.
  it('a 503 and then Retry posts the steer, even with an older delivered action of the same words', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const { hook, socket } = await openRoom()
      vi.mocked(api.steer).mockRejectedValueOnce(offline())
      await act(async () => {
        await hook.result.current.actions.steer('pause the consumer', { clientId: 'k1' })
      })
      vi.mocked(api.steer).mockResolvedValueOnce(undefined as never)
      act(() => socket.onopen?.())
      await waitFor(() => expect(api.steer).toHaveBeenCalledTimes(2))
      await act(async () => {
        vi.advanceTimersByTime(3 * 60_000)
      })

      // Later the same words again (a quick-steer chip): a new action, answered 503.
      vi.mocked(api.steer).mockRejectedValueOnce(new ApiClientError(503, 'unavailable', 'try again'))
      let ok = true
      await act(async () => {
        ok = await hook.result.current.actions.steer('pause the consumer', { clientId: 'k2' })
      })
      expect(ok).toBe(false)
      expect(api.steer).toHaveBeenCalledTimes(3)

      vi.mocked(api.steer).mockResolvedValueOnce(undefined as never)
      await act(async () => {
        ok = await hook.result.current.actions.steer('pause the consumer', { clientId: 'k2' })
      })
      expect(ok).toBe(true)
      expect(api.steer).toHaveBeenCalledTimes(4)
    } finally {
      vi.useRealTimers()
    }
  })

  it('a deliberate repeat of delivered words is a new action and posts', async () => {
    const { hook, socket } = await openRoom()
    vi.mocked(api.steer).mockRejectedValueOnce(offline())
    await act(async () => {
      await hook.result.current.actions.steer('pause the consumer', { clientId: 'k1' })
    })
    vi.mocked(api.steer).mockResolvedValue(undefined as never)
    act(() => socket.onopen?.())
    await waitFor(() => expect(api.steer).toHaveBeenCalledTimes(2))
    // Within seconds, the same words: round 2 swallowed this send for 2 minutes; now it posts.
    await act(async () => {
      await hook.result.current.actions.steer('pause the consumer', { clientId: 'k2' })
    })
    expect(api.steer).toHaveBeenCalledTimes(3)
    expect(api.steer).toHaveBeenLastCalledWith('r1', 'pause the consumer', 'k2')
  })

  // Round 3: a copy that never settles must not block those words until reload. Before any answer
  // the server is not known to dedupe (#545, #551), so the copy that hangs is the first send itself
  // (a failed one is not re-sent by the reconnect), and a Retry waits on it.
  describe('a send that never settles, before the server is known to dedupe', () => {
    async function hungAfterRetry() {
      const opened = await openRoom(false)
      const drain = hangingCall()
      vi.mocked(api.steer).mockImplementationOnce(() => drain.p)
      act(() => {
        void opened.hook.result.current.actions.steer('stop', { clientId: 'k1' })
      })
      let ok: boolean | undefined
      act(() => {
        void opened.hook.result.current.actions.steer('stop', { clientId: 'k1' }).then((v) => (ok = v))
      })
      await act(async () => {
        vi.advanceTimersByTime(15_000)
      })
      await waitFor(() => expect(ok).toBe(false))
      expect(api.steer).toHaveBeenCalledTimes(1)
      return { ...opened, drain }
    }

    // Round 4: a copy that never answered may be stored, so it is never posted again on its own.
    it('later Retries and reconnects do not post while the event stream is checked', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true })
      try {
        const { hook, socket } = await hungAfterRetry()
        expect(hook.result.current.error).toBe(UNCONFIRMED_TEXT)
        vi.mocked(api.steer).mockResolvedValue(undefined as never)
        let ok = true
        await act(async () => {
          ok = await hook.result.current.actions.steer('stop', { clientId: 'k1' })
        })
        expect(ok).toBe(false)
        act(() => socket.onopen?.())
        await act(async () => {
          await Promise.resolve()
        })
        expect(api.steer).toHaveBeenCalledTimes(1)
      } finally {
        vi.useRealTimers()
      }
    })

    it('with no sign of it in the stream, the words are "not confirmed" and only a click posts them, once', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true })
      try {
        const { hook } = await hungAfterRetry()
        await act(async () => {
          vi.advanceTimersByTime(65_000)
        })
        expect(hook.result.current.error).toBe(NOT_CONFIRMED_TEXT)
        expect(api.steer).toHaveBeenCalledTimes(1)
        vi.mocked(api.steer).mockResolvedValue(undefined as never)
        let ok = false
        await act(async () => {
          ok = await hook.result.current.actions.steer('stop', { clientId: 'k1' })
        })
        expect(ok).toBe(true)
        expect(api.steer).toHaveBeenCalledTimes(2)
        expect(hook.result.current.error).toBeNull()
      } finally {
        vi.useRealTimers()
      }
    })

    it('a late success of the old copy settles it: no second post', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true })
      try {
        const { hook, socket, drain } = await hungAfterRetry()
        await act(async () => drain.settle().resolve())
        expect(hook.result.current.error).toBeNull()
        vi.mocked(api.steer).mockResolvedValue(undefined as never)
        act(() => socket.onopen?.())
        let ok = false
        await act(async () => {
          ok = await hook.result.current.actions.steer('stop', { clientId: 'k1' })
        })
        expect(ok).toBe(true)
        expect(api.steer).toHaveBeenCalledTimes(1)
      } finally {
        vi.useRealTimers()
      }
    })
  })

  it('a Retry waits at most 15 s for a hanging send, then reports it unconfirmed without posting', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const { hook } = await openRoom(false)
      const drain = hangingCall()
      vi.mocked(api.steer).mockImplementationOnce(() => drain.p)
      act(() => {
        void hook.result.current.actions.steer('pause the consumer', { clientId: 'k1' })
      })

      let ok: boolean | undefined
      act(() => {
        void hook.result.current.actions.steer('pause the consumer', { clientId: 'k1' }).then((v) => (ok = v))
      })
      await act(async () => {
        vi.advanceTimersByTime(15_000)
      })
      await waitFor(() => expect(ok).toBe(false))
      expect(hook.result.current.error).toBe(UNCONFIRMED_TEXT)
      expect(api.steer).toHaveBeenCalledTimes(1)
    } finally {
      vi.useRealTimers()
    }
  })
})
